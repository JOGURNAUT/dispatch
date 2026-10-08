"""Where the lake lives: a local directory, or a bucket.

One abstraction over both, chosen by URI. `data/bronze` is a path;
`gs://my-bucket/bronze` is Google Cloud Storage. Nothing above this layer knows
which it got.

THE REASON THIS IS NOT A CONFIG CHANGE

Object storage is not a filesystem, and the difference is not cosmetic:

  NO APPEND.    A GCS object is written whole or replaced whole. The bronze
                stage used to open a day's file in "a" mode and add to it, which
                has no equivalent here at all.
  NO RENAME.    What looks like a rename is a copy followed by a delete. There
                is no atomic "move this finished file into place", so the usual
                write-to-temp-then-rename trick does not give atomicity.
  LISTING IS A QUERY.   Objects live in a flat namespace. "Directories" are a
                prefix convention, and listing one is a network call that costs
                and that can page.

So appending became *writing a new object per batch*:

    data/bronze/dt=2026-09-01/batch-20260901T0300-a1b2.jsonl
    data/bronze/dt=2026-09-01/batch-20260902T0300-c3d4.jsonl

Both backends do this, including the local one. That is deliberate. If local
appended to a single file and GCS wrote many objects, the two would have
different semantics, the tests would exercise the easy one, and the first real
GCS run would be the first time anything checked the hard one.

It also turns out to be the better shape on a filesystem too: a batch either
produced its object or it did not, so a crash mid-write leaves a partial file
that is identifiable by name rather than a half-line glued onto good data.

SILVER REPLACES, BRONZE ACCUMULATES

Silver is derived and idempotent -- recomputing a partition must produce the
same answer -- so it is written as one object per partition and replaced each
time. Bronze is evidence and keeps every batch.
"""

from __future__ import annotations

import json
import pathlib
import re
from collections.abc import Iterable
from dataclasses import dataclass

GCS_SCHEME = "gs://"
PARTITION_RE = re.compile(r"dt=(\d{4}-\d{2}-\d{2}|unknown)")


class StorageError(RuntimeError):
    pass


def _encode(records: Iterable[dict]) -> bytes:
    return ("\n".join(json.dumps(r, default=str) for r in records) + "\n").encode("utf-8")


def _decode(blob: bytes) -> list[dict]:
    return [json.loads(line) for line in blob.decode("utf-8").splitlines() if line.strip()]


@dataclass
class LocalStore:
    """A directory tree. Same object-per-batch layout as the bucket."""

    root: pathlib.Path

    @property
    def uri(self) -> str:
        return str(self.root)

    def _dir(self, partition: str) -> pathlib.Path:
        return self.root / f"dt={partition}"

    def write_batch(self, partition: str, records: Iterable[dict], batch_id: str) -> str:
        d = self._dir(partition)
        d.mkdir(parents=True, exist_ok=True)
        name = f"batch-{batch_id}.jsonl"
        (d / name).write_bytes(_encode(records))
        return f"dt={partition}/{name}"

    def replace_partition(self, partition: str, records: Iterable[dict]) -> str:
        d = self._dir(partition)
        d.mkdir(parents=True, exist_ok=True)
        # Clear first: a partition rebuilt from fewer batches must not keep the
        # objects the previous, larger run left behind.
        for old in d.glob("*.jsonl"):
            old.unlink()
        (d / "part-0000.jsonl").write_bytes(_encode(records))
        return f"dt={partition}/part-0000.jsonl"

    def read_partition(self, partition: str) -> list[dict]:
        d = self._dir(partition)
        if not d.is_dir():
            return []
        out: list[dict] = []
        for path in sorted(d.glob("*.jsonl")):
            out.extend(_decode(path.read_bytes()))
        return out

    def partitions(self) -> list[str]:
        if not self.root.is_dir():
            return []
        found = {m.group(1) for p in self.root.iterdir()
                 if (m := PARTITION_RE.fullmatch(p.name))}
        return sorted(found)

    def object_count(self, partition: str) -> int:
        d = self._dir(partition)
        return len(list(d.glob("*.jsonl"))) if d.is_dir() else 0


@dataclass
class GcsStore:
    """A bucket and a prefix.

    The client is created once and reused: a new one per call re-reads
    credentials and re-opens a connection pool, which on a partitioned write
    turns one network setup into one per partition.
    """

    bucket_name: str
    prefix: str
    _client: object | None = None

    @property
    def uri(self) -> str:
        return f"{GCS_SCHEME}{self.bucket_name}/{self.prefix}".rstrip("/")

    @property
    def bucket(self):
        if self._client is None:
            try:
                from google.cloud import storage
            except ImportError as exc:
                raise StorageError(
                    "google-cloud-storage is not installed. `pip install "
                    "google-cloud-storage`, or point the lake at a local path."
                ) from exc
            self._client = storage.Client()
        return self._client.bucket(self.bucket_name)

    def _key(self, partition: str, name: str) -> str:
        return f"{self.prefix.rstrip('/')}/dt={partition}/{name}".lstrip("/")

    def write_batch(self, partition: str, records: Iterable[dict], batch_id: str) -> str:
        key = self._key(partition, f"batch-{batch_id}.jsonl")
        self.bucket.blob(key).upload_from_string(
            _encode(records), content_type="application/x-ndjson")
        return key

    def replace_partition(self, partition: str, records: Iterable[dict]) -> str:
        prefix = self._key(partition, "")
        # Delete then write, in that order. The reverse would briefly leave the
        # new object inside the set being deleted.
        for blob in list(self._client.list_blobs(self.bucket_name, prefix=prefix)):
            blob.delete()
        key = self._key(partition, "part-0000.jsonl")
        self.bucket.blob(key).upload_from_string(
            _encode(records), content_type="application/x-ndjson")
        return key

    def read_partition(self, partition: str) -> list[dict]:
        _ = self.bucket  # force the client
        prefix = self._key(partition, "")
        out: list[dict] = []
        for blob in sorted(self._client.list_blobs(self.bucket_name, prefix=prefix),
                           key=lambda b: b.name):
            out.extend(_decode(blob.download_as_bytes()))
        return out

    def partitions(self) -> list[str]:
        _ = self.bucket
        root = f"{self.prefix.rstrip('/')}/".lstrip("/")
        found = set()
        for blob in self._client.list_blobs(self.bucket_name, prefix=root):
            rest = blob.name[len(root):]
            m = PARTITION_RE.match(rest)
            if m:
                found.add(m.group(1))
        return sorted(found)

    def object_count(self, partition: str) -> int:
        _ = self.bucket
        prefix = self._key(partition, "")
        return sum(1 for _ in self._client.list_blobs(self.bucket_name, prefix=prefix))


Store = LocalStore | GcsStore


def open_store(uri: str | pathlib.Path) -> Store:
    """A store from a URI. `gs://bucket/prefix` is a bucket; anything else is a path.

    Scheme-based rather than a flag, so the same argument that names the
    location also decides the backend, and a deployment cannot be configured
    with a bucket and a local-mode switch that disagree.
    """
    text = str(uri)
    if text.startswith(GCS_SCHEME):
        rest = text[len(GCS_SCHEME):].strip("/")
        if not rest:
            raise StorageError(f"{text!r} names no bucket")
        bucket, _, prefix = rest.partition("/")
        return GcsStore(bucket_name=bucket, prefix=prefix)
    return LocalStore(root=pathlib.Path(text))
