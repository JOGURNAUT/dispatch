"""Tests for the lake's storage layer.

The point of this layer is that a local directory and a bucket behave the same
way. So the tests are about the behaviours that differ between a filesystem and
object storage, which is where a pipeline written against one breaks on the
other:

  no append        bronze used to open a file in "a" mode; GCS has no such thing
  replace is explicit   a derived partition must be rebuilt, not added to
  listing is a query    partition discovery cannot glob a directory tree

The GCS backend is exercised against a fake bucket rather than a real one. That
is deliberate and it is also a limit worth naming: it proves the key layout, the
delete-before-write ordering and the client reuse, and it does not prove that
Google's API behaves the way the fake does.
"""

from __future__ import annotations

import pytest

from dispatch.storage import GcsStore, LocalStore, StorageError, open_store


@pytest.fixture
def store(tmp_path):
    return open_store(tmp_path / "bronze")


def rows(n, tag="x"):
    return [{"i": i, "tag": tag} for i in range(n)]


# ------------------------------------------------------------------ routing

def test_a_gs_uri_gives_a_bucket_and_anything_else_a_directory(tmp_path):
    """The URI decides the backend, so a deployment cannot be configured with a
    bucket and a local-mode switch that disagree."""
    assert isinstance(open_store("gs://my-bucket/bronze"), GcsStore)
    assert isinstance(open_store(tmp_path / "bronze"), LocalStore)
    assert isinstance(open_store("data/bronze"), LocalStore)


def test_a_gs_uri_parses_into_bucket_and_prefix():
    s = open_store("gs://lake/dispatch/bronze")
    assert s.bucket_name == "lake"
    assert s.prefix == "dispatch/bronze"
    assert s.uri == "gs://lake/dispatch/bronze"


def test_a_bucketless_gs_uri_is_rejected():
    with pytest.raises(StorageError, match="no bucket"):
        open_store("gs://")


# ------------------------------------------------- no append, ever

def test_each_batch_becomes_its_own_object(store):
    """Object storage has no append, so the local backend does not append
    either. If it did, the easy backend would be the one the tests exercised and
    the first real bucket run would be the first check of the hard one."""
    store.write_batch("2026-09-01", rows(3), "batch-a")
    store.write_batch("2026-09-01", rows(2), "batch-b")

    assert store.object_count("2026-09-01") == 2
    assert len(store.read_partition("2026-09-01")) == 5


def test_rewriting_the_same_batch_id_does_not_duplicate(store):
    """A retried batch writes the same key, so it overwrites rather than adding.
    That is what keeps an Airflow retry from doubling a partition."""
    store.write_batch("2026-09-01", rows(3), "batch-a")
    store.write_batch("2026-09-01", rows(3), "batch-a")

    assert store.object_count("2026-09-01") == 1
    assert len(store.read_partition("2026-09-01")) == 3


def test_a_partition_reads_back_what_was_written(store):
    store.write_batch("2026-09-01", [{"trip_id": "T-1", "n": 1}], "b1")
    assert store.read_partition("2026-09-01") == [{"trip_id": "T-1", "n": 1}]


def test_reading_a_partition_that_was_never_written_is_empty_not_an_error(store):
    """An unwritten partition is a normal state -- a quiet day, a backfill that
    has not reached it yet -- and not something to crash a run over."""
    assert store.read_partition("2026-01-01") == []
    assert store.object_count("2026-01-01") == 0


# ------------------------------------------------------- replace semantics

def test_replace_leaves_exactly_what_it_was_given(store):
    """Silver is derived: recomputing a partition must produce that partition,
    not add to it."""
    store.write_batch("2026-09-01", rows(5), "b1")
    store.write_batch("2026-09-01", rows(5), "b2")
    store.replace_partition("2026-09-01", rows(2, "new"))

    out = store.read_partition("2026-09-01")
    assert len(out) == 2
    assert {r["tag"] for r in out} == {"new"}


def test_replace_removes_objects_a_larger_previous_run_left(store):
    """The case a naive overwrite misses. A partition rebuilt from fewer batches
    must not keep the extra objects the previous run wrote -- they would be read
    back as data nothing produced."""
    store.write_batch("2026-09-01", rows(1), "b1")
    store.write_batch("2026-09-01", rows(1), "b2")
    store.write_batch("2026-09-01", rows(1), "b3")
    assert store.object_count("2026-09-01") == 3

    store.replace_partition("2026-09-01", rows(1, "only"))
    assert store.object_count("2026-09-01") == 1


def test_replace_is_idempotent(store):
    store.replace_partition("2026-09-01", rows(4))
    first = store.read_partition("2026-09-01")
    store.replace_partition("2026-09-01", rows(4))
    assert store.read_partition("2026-09-01") == first


# --------------------------------------------------------- partition listing

def test_partitions_are_discovered_and_sorted(store):
    for day in ("2026-09-03", "2026-09-01", "2026-09-02"):
        store.write_batch(day, rows(1), "b1")
    assert store.partitions() == ["2026-09-01", "2026-09-02", "2026-09-03"]


def test_an_empty_lake_lists_no_partitions(tmp_path):
    assert open_store(tmp_path / "never-written").partitions() == []


def test_the_unknown_partition_is_a_real_partition(store):
    """Events whose timestamp could not be read land in dt=unknown. It has to be
    listable, or those rows are written and never looked at again."""
    store.write_batch("unknown", rows(2), "b1")
    assert "unknown" in store.partitions()


def test_a_stray_directory_is_not_mistaken_for_a_partition(store):
    (store.root / "_checkpoints").mkdir(parents=True)
    (store.root / "notes.txt").write_text("x", encoding="utf-8")
    store.write_batch("2026-09-01", rows(1), "b1")
    assert store.partitions() == ["2026-09-01"]


# ----------------------------------------------------------- GCS, against a fake

class FakeBlob:
    def __init__(self, bucket, name):
        self.bucket, self.name = bucket, name

    def upload_from_string(self, data, content_type=None):
        self.bucket.objects[self.name] = data

    def download_as_bytes(self):
        return self.bucket.objects[self.name]

    def delete(self):
        self.bucket.objects.pop(self.name, None)


class FakeBucket:
    def __init__(self):
        self.objects: dict[str, bytes] = {}

    def blob(self, name):
        return FakeBlob(self, name)


class FakeClient:
    def __init__(self):
        self.bucket_obj = FakeBucket()
        self.clients_made = 1

    def bucket(self, name):
        return self.bucket_obj

    def list_blobs(self, bucket_name, prefix=""):
        return [FakeBlob(self.bucket_obj, n)
                for n in sorted(self.bucket_obj.objects) if n.startswith(prefix)]


@pytest.fixture
def gcs():
    store = open_store("gs://lake/dispatch/bronze")
    store._client = FakeClient()
    return store


def test_gcs_keys_carry_the_prefix_and_the_partition(gcs):
    key = gcs.write_batch("2026-09-01", rows(1), "b1")
    assert key == "dispatch/bronze/dt=2026-09-01/batch-b1.jsonl"


def test_gcs_round_trips_a_partition(gcs):
    gcs.write_batch("2026-09-01", [{"trip_id": "T-1"}], "b1")
    gcs.write_batch("2026-09-01", [{"trip_id": "T-2"}], "b2")
    assert [r["trip_id"] for r in gcs.read_partition("2026-09-01")] == ["T-1", "T-2"]


def test_gcs_replace_deletes_before_it_writes(gcs):
    """Ordering matters: writing first would put the new object inside the set
    about to be deleted, and the partition would come back empty."""
    gcs.write_batch("2026-09-01", rows(1), "b1")
    gcs.write_batch("2026-09-01", rows(1), "b2")
    gcs.replace_partition("2026-09-01", rows(3, "new"))

    keys = [k for k in gcs._client.bucket_obj.objects
            if "dt=2026-09-01/" in k]
    assert keys == ["dispatch/bronze/dt=2026-09-01/part-0000.jsonl"]
    assert len(gcs.read_partition("2026-09-01")) == 3


def test_gcs_lists_partitions_from_keys_not_directories(gcs):
    """There are no directories in a bucket. Partitions are a prefix convention,
    and listing one is a query over keys."""
    for day in ("2026-09-02", "2026-09-01"):
        gcs.write_batch(day, rows(1), "b1")
    assert gcs.partitions() == ["2026-09-01", "2026-09-02"]


def test_gcs_reuses_one_client(gcs):
    """A client per call re-reads credentials and re-opens a connection pool,
    turning one network setup into one per partition."""
    before = gcs._client
    gcs.write_batch("2026-09-01", rows(1), "b1")
    gcs.read_partition("2026-09-01")
    gcs.partitions()
    assert gcs._client is before


def test_both_backends_agree_on_what_a_partition_contains(tmp_path, gcs):
    """The claim the whole layer rests on: nothing above it should be able to
    tell which backend it got."""
    local = open_store(tmp_path / "bronze")
    batches = [("2026-09-01", rows(3), "b1"), ("2026-09-01", rows(2), "b2"),
               ("2026-09-02", rows(1), "b1")]
    for day, data, batch in batches:
        local.write_batch(day, data, batch)
        gcs.write_batch(day, data, batch)

    assert local.partitions() == gcs.partitions()
    for day in local.partitions():
        assert local.read_partition(day) == gcs.read_partition(day)
        assert local.object_count(day) == gcs.object_count(day)
