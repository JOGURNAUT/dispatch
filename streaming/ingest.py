"""Spark Structured Streaming: Kafka -> bronze Parquet.

The distributed path. The batch runner in `transforms/run_pipeline.py` and this
job apply the same functions from the `dispatch` package; what changes is who
executes them and over how much data. That is the reason the transformation
logic has no Spark import in it -- a rule written in Spark SQL can only be
tested by starting Spark, so in practice it does not get tested.

Three decisions here that are the actual content of the job:

CHECKPOINTS, NOT OFFSETS IN CODE. The checkpoint directory is what makes a
restart resume rather than replay from the beginning or skip to the end.
Committed with the output in the same atomic step, so the exactly-once claim is
about the *pair*, not about Kafka.

FOREACHBATCH, NOT A PLAIN SINK. Structured Streaming's file sink cannot do a
partition replace, and the late-arrival rule needs one: an event for Tuesday
arriving on Friday has to be merged into Tuesday. `foreachBatch` gives a normal
batch DataFrame per micro-batch, which can be written with the overwrite
semantics that needs.

WATERMARK ON EVENT TIME, NOT PROCESSING TIME. A watermark on processing time
discards data for arriving late, which is the opposite of handling it. The
watermark here bounds state, and the bronze layer still keeps every record --
dropping is a decision for silver, where it is visible and quarantined.

    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
        streaming/ingest.py --bootstrap localhost:9092 --bronze data/bronze
"""

from __future__ import annotations

import argparse
import json
import sys

TOPIC = "trip_events"
# Bounds how long Spark keeps per-key state for late events. Larger than the
# producer lag the pipeline tolerates, smaller than anything that would make
# state grow without limit.
WATERMARK = "6 hours"


def build_session(app_name: str = "dispatch-ingest"):
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName(app_name)
        # Dynamic overwrite is what makes a micro-batch replace only the
        # partitions it actually touched. The default, STATIC, would drop every
        # partition in the table on each write -- which is a full-table wipe on
        # a job whose intent is to update one day.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def event_schema():
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    # Declared, never inferred. Schema inference on a stream samples the first
    # micro-batch, so a column absent from the first minute of traffic is absent
    # from the schema for the life of the job, and every later value of it is
    # dropped without a word.
    return StructType([
        StructField("event_id", StringType(), False),
        StructField("trip_id", StringType(), False),
        StructField("order_id", StringType(), True),
        StructField("driver_id", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("event_type", StringType(), False),
        # Read as a string and parsed by the shared contract. Letting Spark cast
        # to timestamp silently nulls whatever spelling it does not recognise,
        # and a null timestamp is indistinguishable from an absent one.
        StructField("event_ts", StringType(), False),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("distance_m", IntegerType(), True),
        StructField("promised_minutes", IntegerType(), True),
        StructField("producer_version", StringType(), True),
    ])


# The OUTPUT shape, declared for the same reason the input one is.
#
# createDataFrame was left to infer this, and it cannot: `_reason` and
# `_payload` are null on every valid row, so there is no value anywhere in the
# batch to infer a type from, and Spark fails the whole micro-batch with
# CANNOT_DETERMINE_TYPE. Inference also makes the schema depend on which rows
# happen to arrive first, which is the thing the input schema exists to prevent.
NORMALISED_COLUMNS = [
    ("event_id", "string"), ("trip_id", "string"), ("order_id", "string"),
    ("driver_id", "string"), ("store_id", "string"), ("event_type", "string"),
    ("event_ts", "timestamp"), ("ingested_at", "timestamp"),
    ("lat", "double"), ("lon", "double"),
    ("distance_m", "int"), ("promised_minutes", "int"),
    ("producer_version", "string"), ("schema_version", "string"),
    ("extra", "string"), ("issues", "string"),
    ("_valid", "boolean"), ("_reason", "string"), ("_payload", "string"),
    ("dt", "string"),
]


def normalised_schema():
    from pyspark.sql.types import (
        BooleanType, DoubleType, IntegerType, StringType, StructField,
        StructType, TimestampType,
    )

    kinds = {"string": StringType(), "timestamp": TimestampType(),
             "double": DoubleType(), "int": IntegerType(),
             "boolean": BooleanType()}
    return StructType([StructField(name, kinds[kind], True)
                       for name, kind in NORMALISED_COLUMNS])


def normalise_partition(rows):
    """Apply the shared contract across one Spark partition.

    Runs on the executors. Imports `dispatch` inside the function because the
    module has to resolve there, not on the driver, and a top-level import would
    succeed locally and fail on a real cluster.
    """
    from dispatch.contracts import ContractViolation, normalise_event

    def blank() -> dict:
        """Every output column, present and null.

        Both branches below must yield the SAME KEYS. They did not: the
        violation path emitted six fields and the valid path about twenty, so
        the rows in one partition had different shapes depending on how clean
        the data happened to be.
        """
        return {name: None for name, _ in NORMALISED_COLUMNS}

    for row in rows:
        raw = row.asDict()
        out = blank()
        try:
            event = normalise_event(raw, ingested_at=row["_ingested_at"])
        except ContractViolation as exc:
            # Not dropped. Carried forward with its reason so the silver stage
            # can quarantine it and someone can read what the producer sent.
            out.update({"_valid": False, "_reason": str(exc),
                        "_payload": json.dumps(raw, default=str),
                        "trip_id": raw.get("trip_id"), "dt": "unknown"})
            yield out
            continue

        out.update(event.as_row())
        out["issues"] = ",".join(event.issues) or None
        out["_valid"] = True
        # Partition by the day the event describes, not the day it arrived.
        # Arrival-date partitioning splits one trip across two days.
        out["dt"] = event.event_ts.strftime("%Y-%m-%d")
        yield out


def write_batch(batch_df, batch_id: int, bronze_path: str) -> None:
    """One micro-batch to bronze, replacing only the partitions it touches.

    `foreachBatch` can be called more than once for the same batch_id when a
    driver fails between the write and the offset commit. The write is therefore
    an overwrite of named partitions rather than an append: a second execution
    of the same batch produces the same bronze.
    """
    if batch_df.rdd.isEmpty():
        return
    (batch_df.write
        .mode("overwrite")
        .partitionBy("dt")
        .parquet(bronze_path))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--bronze", default="data/bronze")
    parser.add_argument("--checkpoint", default="data/_checkpoints/ingest")
    parser.add_argument("--starting-offsets", default="earliest")
    parser.add_argument("--trigger-seconds", type=int, default=30)
    parser.add_argument("--once", action="store_true",
                        help="drain available data and stop; the batch-mode backfill")
    args = parser.parse_args(argv)

    from pyspark.sql import functions as F

    spark = build_session()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        # Bounds how much one micro-batch may pull. Without it, a job restarted
        # after a long outage tries to read the whole backlog in one batch and
        # dies on memory, which is the point at which people start deleting
        # checkpoints and losing their exactly-once guarantee.
        .option("maxOffsetsPerTrigger", 200_000)
        .load()
    )

    parsed = (
        raw.select(F.from_json(F.col("value").cast("string"), event_schema()).alias("e"),
                   F.col("timestamp").alias("_ingested_at"))
        .select("e.*", "_ingested_at")
    )

    def process(batch_df, batch_id):
        rows = batch_df.rdd.mapPartitions(normalise_partition)
        if rows.isEmpty():
            return
        out = spark.createDataFrame(rows, schema=normalised_schema())
        write_batch(out, batch_id, args.bronze)
        invalid = out.filter(~F.col("_valid")).count()
        print(f"batch {batch_id}: {out.count()} rows, {invalid} contract violations")

    trigger = ({"availableNow": True} if args.once
               else {"processingTime": f"{args.trigger_seconds} seconds"})

    query = (
        parsed.writeStream
        .foreachBatch(process)
        # The checkpoint and the output commit together. This is what the
        # exactly-once claim actually rests on; Kafka alone gives at-least-once.
        .option("checkpointLocation", args.checkpoint)
        .trigger(**trigger)
        .start()
    )
    query.awaitTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
