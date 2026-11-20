"""Where lake objects live: a local folder, or an S3 bucket (AWS, MinIO, any S3 API).

One URI picks the backend: a plain path is a local folder, ``s3://bucket/prefix``
is S3. The S3 client is plain ``boto3``, so it is configured the standard way:
credentials from the usual chain, and a non-AWS server (MinIO) through
``AWS_ENDPOINT_URL_S3`` or ``AWS_ENDPOINT_URL``.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Any, Protocol

import boto3


class Storage(Protocol):
    """Keys are ``/``-separated paths relative to the storage root."""

    uri: str

    def put(self, key: str, data: bytes) -> None: ...

    def get(self, key: str) -> bytes: ...

    def list(self, prefix: str = "") -> list[str]:
        """Every key under ``prefix``, sorted."""
        ...


class LocalStorage:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.uri = str(root)

    def put(self, key: str, data: bytes) -> None:
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        # Write a temp file unique to this call, then rename it over the key: a reader
        # sees the old object or the new one, never half of one, even with two
        # writers. shortcut: on Windows, a second writer or an open reader of the
        # same key makes os.replace fail (it raises; nothing half-written lands);
        # bronze keys carry a timestamp, so this needs two runs in the same second.
        tmp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
        try:
            tmp.write_bytes(data)
            os.replace(tmp, path)
        finally:
            tmp.unlink(missing_ok=True)  # only still there if the write or rename failed

    def get(self, key: str) -> bytes:
        return (self.root / key).read_bytes()

    def list(self, prefix: str = "") -> list[str]:
        if not self.root.is_dir():
            return []
        keys = (
            p.relative_to(self.root).as_posix()
            for p in self.root.rglob("*")
            if p.is_file() and not p.name.startswith(".")
        )
        return sorted(k for k in keys if k.startswith(prefix))


class S3Storage:
    def __init__(self, bucket: str, prefix: str = "", client: Any = None) -> None:
        self.client = client if client is not None else boto3.client("s3")
        self.bucket = bucket
        self.prefix = prefix
        self.uri = f"s3://{bucket}/{prefix}"

    def put(self, key: str, data: bytes) -> None:
        self.client.put_object(
            Bucket=self.bucket, Key=self.prefix + key, Body=data, ContentType="application/json"
        )

    def get(self, key: str) -> bytes:
        body: bytes = self.client.get_object(Bucket=self.bucket, Key=self.prefix + key)[
            "Body"
        ].read()
        return body

    def list(self, prefix: str = "") -> list[str]:
        pages = self.client.get_paginator("list_objects_v2").paginate(
            Bucket=self.bucket, Prefix=self.prefix + prefix
        )
        cut = len(self.prefix)
        return sorted(obj["Key"][cut:] for page in pages for obj in page.get("Contents", []))


def open_storage(uri: str) -> Storage:
    """``s3://bucket[/prefix]`` or a local folder path."""
    if not uri.strip():
        # Path("") is the current directory: an unset variable must not write there.
        raise ValueError("empty storage URI: give a folder or s3://bucket/prefix")
    if uri.startswith("s3://"):
        bucket, _, prefix = uri.removeprefix("s3://").partition("/")
        if not bucket:
            raise ValueError(f"no bucket in {uri!r}: expected s3://bucket[/prefix]")
        prefix = prefix.strip("/")
        return S3Storage(bucket, prefix + "/" if prefix else "")
    if "://" in uri:
        raise ValueError(f"unsupported storage URI {uri!r}: use a local path or s3://bucket/prefix")
    return LocalStorage(Path(uri))
