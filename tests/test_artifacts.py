"""Content-addressed artifact store tests."""

import io

import pytest

from nirs4all_cluster.server.artifacts import ArtifactStore, ArtifactTooLarge


def test_put_bytes_is_content_addressed(tmp_path):
    store = ArtifactStore(tmp_path)
    sha1, path1, size1 = store.put_bytes(b"hello world")
    sha2, path2, size2 = store.put_bytes(b"hello world")
    assert sha1 == sha2
    assert path1 == path2  # deduplicated: same bytes -> same path
    assert size1 == size2 == 11
    assert store.exists(sha1)


def test_different_bytes_distinct(tmp_path):
    store = ArtifactStore(tmp_path)
    sha_a, _, _ = store.put_bytes(b"aaa")
    sha_b, _, _ = store.put_bytes(b"bbb")
    assert sha_a != sha_b


def test_put_stream_roundtrip(tmp_path):
    store = ArtifactStore(tmp_path)
    payload = b"x" * (3 * 1024 * 1024 + 7)  # spans multiple chunks
    sha, _, size = store.put_stream(io.BytesIO(payload))
    assert size == len(payload)
    with store.open(sha) as fh:
        assert fh.read() == payload


def test_put_stream_size_limit_no_leak(tmp_path):
    store = ArtifactStore(tmp_path)
    payload = b"x" * (2 * 1024 * 1024)
    with pytest.raises(ArtifactTooLarge):
        store.put_stream(io.BytesIO(payload), max_bytes=1024)
    # No blob and no leftover temp file should remain on disk.
    leftover = [p for p in tmp_path.rglob("*") if p.is_file()]
    assert leftover == []
