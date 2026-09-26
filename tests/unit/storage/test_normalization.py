"""L0 normalization storage-I/O vs data-corruption classification."""

from __future__ import annotations


def _write_l0_partition(part, *, raw="a"):
    import json

    import zstandard as zstd

    part.mkdir(parents=True, exist_ok=True)
    rec = {
        "raw": raw, "recv_mono_ns": 1, "recv_wall_ns": 100, "conn_id": "c1",
        "conn_seq": 1, "vendor": "kis", "tr_id": "H0STCNT0",
    }
    (part / "09.jsonl.zst").write_bytes(zstd.ZstdCompressor(level=3).compress((json.dumps(rec) + "\n").encode("utf-8")))
    return part


def test_normalize_l0_partition_disk_full_is_storage_io_not_corruption(tmp_path, monkeypatch) -> None:
    import errno

    import polars as pl
    import pytest

    from src.storage.normalization import L1StorageIOError, normalize_l0_partition

    part = _write_l0_partition(tmp_path / "l0" / "kis" / "H0STCNT0" / "dt=2026-09-01")
    out_path = tmp_path / "l1" / "kis" / "H0STCNT0" / "dt=2026-09-01.parquet"

    def _no_space(self, *args, **kwargs):
        raise OSError(errno.ENOSPC, "No space left on device")

    monkeypatch.setattr(pl.DataFrame, "write_parquet", _no_space)

    with pytest.raises(L1StorageIOError):
        normalize_l0_partition(part, out_path)
    assert not out_path.exists()


def test_normalize_l0_partition_corrupt_zstd_stays_data_fault(tmp_path) -> None:
    import json

    import pytest
    import zstandard as zstd

    from src.storage.normalization import L1NormalizationError, normalize_l0_partition

    part = tmp_path / "l0" / "kis" / "H0STCNT0" / "dt=2026-09-01"
    part.mkdir(parents=True, exist_ok=True)
    rec = {
        "raw": "a", "recv_mono_ns": 1, "recv_wall_ns": 100, "conn_id": "c1",
        "conn_seq": 1, "vendor": "kis", "tr_id": "H0STCNT0",
    }
    valid = zstd.ZstdCompressor(level=3).compress((json.dumps(rec) + "\n").encode("utf-8"))
    (part / "09.jsonl.zst").write_bytes(valid[: len(valid) // 2])
    out_path = tmp_path / "l1" / "kis" / "H0STCNT0" / "dt=2026-09-01.parquet"

    with pytest.raises(L1NormalizationError):
        normalize_l0_partition(part, out_path)
