from __future__ import annotations

import os
import uuid

import boto3
import pytest
from moto import mock_aws

from quake_lake.storage import LocalStorage, S3Storage, open_storage


def test_local_put_get_list(tmp_path):
    s = LocalStorage(tmp_path / "bronze")
    s.put("source=usgs/ingest_date=2026-11-20/a.json", b'{"a": 1}')
    s.put("source=bmkg/ingest_date=2026-11-20/b.json", b"[]")
    assert s.get("source=usgs/ingest_date=2026-11-20/a.json") == b'{"a": 1}'
    assert s.list() == [
        "source=bmkg/ingest_date=2026-11-20/b.json",
        "source=usgs/ingest_date=2026-11-20/a.json",
    ]
    assert s.list("source=usgs/") == ["source=usgs/ingest_date=2026-11-20/a.json"]


def test_local_overwrite_leaves_no_temp_file(tmp_path):
    s = LocalStorage(tmp_path)
    s.put("k/x.json", b"old")
    s.put("k/x.json", b"new")
    assert s.get("k/x.json") == b"new"
    assert sorted(p.name for p in (tmp_path / "k").iterdir()) == ["x.json"]


def test_local_list_skips_leftover_temp_files_and_missing_root(tmp_path):
    assert LocalStorage(tmp_path / "nothing-yet").list() == []
    (tmp_path / "k").mkdir()
    (tmp_path / "k" / ".x.json.123.tmp").write_bytes(b"half")  # a crash mid-write
    assert LocalStorage(tmp_path).list() == []


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="lake")
        yield client


def test_s3_put_get_list_under_prefix(s3):
    s = S3Storage("lake", "bronze/", s3)
    s.put("source=usgs/ingest_date=2026-11-20/a.json", b'{"a": 1}')
    s.put("source=bmkg/ingest_date=2026-11-20/b.json", b"[]")
    s3.put_object(Bucket="lake", Key="silver/other.parquet", Body=b"x")  # outside the prefix

    assert s.get("source=usgs/ingest_date=2026-11-20/a.json") == b'{"a": 1}'
    assert s.list() == [
        "source=bmkg/ingest_date=2026-11-20/b.json",
        "source=usgs/ingest_date=2026-11-20/a.json",
    ]
    assert s.list("source=bmkg/") == ["source=bmkg/ingest_date=2026-11-20/b.json"]
    head = s3.head_object(Bucket="lake", Key="bronze/source=usgs/ingest_date=2026-11-20/a.json")
    assert head["ContentType"] == "application/json"
    assert s.uri == "s3://lake/bronze/"


def test_s3_at_bucket_root(s3):
    s = S3Storage("lake", "", s3)
    s.put("a.json", b"1")
    assert s.list() == ["a.json"]
    assert s3.get_object(Bucket="lake", Key="a.json")["Body"].read() == b"1"


@pytest.mark.parametrize(
    ("uri", "bucket", "prefix"),
    [
        ("s3://lake", "lake", ""),
        ("s3://lake/", "lake", ""),
        ("s3://lake/bronze", "lake", "bronze/"),
        ("s3://lake/a/b/", "lake", "a/b/"),
    ],
)
def test_open_storage_s3(uri, bucket, prefix):
    s = open_storage(uri)
    assert isinstance(s, S3Storage)
    assert (s.bucket, s.prefix) == (bucket, prefix)


def test_open_storage_local(tmp_path):
    s = open_storage(str(tmp_path / "lake" / "bronze"))
    assert isinstance(s, LocalStorage)
    assert s.root == tmp_path / "lake" / "bronze"


@pytest.mark.parametrize(
    ("uri", "message"),
    [("s3://", "no bucket"), ("s3:///x", "no bucket"), ("gs://b/x", "unsupported")],
)
def test_open_storage_rejects(uri, message):
    with pytest.raises(ValueError, match=message):
        open_storage(uri)


@pytest.mark.minio
@pytest.mark.skipif(
    not os.environ.get("QUAKE_LAKE_MINIO"), reason="set QUAKE_LAKE_MINIO=1 with MinIO up"
)
def test_minio_round_trip():
    """Against a real MinIO (``make minio``), configured by the standard AWS_* variables."""
    s = open_storage(f"s3://lake/test-{uuid.uuid4().hex}")
    s.put("source=x/ingest_date=2026-11-20/a.json", b'{"ok": true}')
    assert s.get("source=x/ingest_date=2026-11-20/a.json") == b'{"ok": true}'
    assert s.list() == ["source=x/ingest_date=2026-11-20/a.json"]
