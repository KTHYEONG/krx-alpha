"""Sealed-hour L0 recompression invariants."""

from __future__ import annotations

import datetime as dt
import io
import logging
import os
from zoneinfo import ZoneInfo

import pytest
import zstandard as zstd

from src.storage import l0_recompress
from src.storage.journal import L0JournalWriter
from src.storage.l0_recompress import recompress_sealed_day

_KST = ZoneInfo("Asia/Seoul")
_DAY = dt.date(2026, 9, 1)
_GRACE_S = 600.0
_LEVEL = 9
_NS = 1_000_000_000


def _wall_ns(day: dt.date, hour: int, minute: int = 0) -> int:
    return int(dt.datetime(day.year, day.month, day.day, hour, minute, tzinfo=_KST).timestamp()) * _NS


def _hour_end_ns(day: dt.date, hour: int) -> int:
    return _wall_ns(day, hour) + 3600 * _NS


def _write_journal(root, day: dt.date, *, flushes: int = 50, hour: int = 9) -> L0JournalWriter:
    writer = L0JournalWriter(root=root, vendor="ls", stream="H0STCNT0")
    base = _wall_ns(day, hour)
    for i in range(flushes):
        writer.append(
            raw=f"0|H0STCNT0|001|row-{day.isoformat()}-{i}^" + "x" * 200,
            recv_mono_ns=i,
            recv_wall_ns=base + i * 1_000_000,
            conn_id="c1",
            conn_seq=i,
            symbol="005930",
        )
        writer.flush()
    return writer


def _hour_file(writer: L0JournalWriter, day: dt.date, hour: int = 9):
    return writer.partition_path(_wall_ns(day, hour))


def _decoded(data: bytes) -> bytes:
    dobj = zstd.ZstdDecompressor().decompressobj()
    out = dobj.decompress(data)
    assert dobj.eof
    assert dobj.unused_data == b""
    return out


def _sealed_now(day: dt.date, hour: int = 9) -> int:
    return _hour_end_ns(day, hour) + 3600 * _NS


def test_multi_frame_file_becomes_single_frame_identical_bytes(tmp_path) -> None:
    writer = _write_journal(tmp_path, _DAY)
    path = _hour_file(writer, _DAY)
    original = path.read_bytes()
    before = zstd.ZstdDecompressor().stream_reader(io.BytesIO(original), read_across_frames=True).read()
    dobj = zstd.ZstdDecompressor().decompressobj()
    dobj.decompress(original)
    assert dobj.unused_data != b""

    report = recompress_sealed_day(tmp_path, day=_DAY, now_ns=_sealed_now(_DAY), level=_LEVEL, seal_grace_s=_GRACE_S)

    assert report.recompressed == 1
    rewritten = path.read_bytes()
    assert len(rewritten) < len(original)
    assert _decoded(rewritten) == before
    assert len(_decoded(rewritten).splitlines()) == 50


def test_mtime_preserved_exactly(tmp_path) -> None:
    writer = _write_journal(tmp_path, _DAY)
    path = _hour_file(writer, _DAY)
    mtime_before = path.stat().st_mtime_ns

    recompress_sealed_day(tmp_path, day=_DAY, now_ns=_sealed_now(_DAY), level=_LEVEL, seal_grace_s=_GRACE_S)

    assert path.stat().st_mtime_ns == mtime_before


def test_unsealed_hour_skipped(tmp_path) -> None:
    writer = _write_journal(tmp_path, _DAY)
    path = _hour_file(writer, _DAY)
    original = path.read_bytes()
    hour_not_ended = _hour_end_ns(_DAY, 9) + 100 * _NS

    report = recompress_sealed_day(
        tmp_path, day=_DAY, now_ns=hour_not_ended, level=_LEVEL, seal_grace_s=_GRACE_S
    )

    assert report.skipped_unsealed == 1
    assert report.recompressed == 0
    assert path.read_bytes() == original


def test_recent_mtime_skipped(tmp_path) -> None:
    writer = _write_journal(tmp_path, _DAY)
    path = _hour_file(writer, _DAY)
    original = path.read_bytes()
    now_ns = _hour_end_ns(_DAY, 9) + 86400 * _NS
    recent = now_ns - _NS
    os.utime(path, ns=(recent, recent))

    report = recompress_sealed_day(tmp_path, day=_DAY, now_ns=now_ns, level=_LEVEL, seal_grace_s=_GRACE_S)

    assert report.skipped_unsealed == 1
    assert path.read_bytes() == original


class _SubstitutingWriter:
    """Writes a different payload of the same length through a real frame writer."""

    def __init__(self, inner) -> None:
        self._inner = inner

    def __enter__(self):
        self._inner.__enter__()
        return self

    def __exit__(self, *exc):
        return self._inner.__exit__(*exc)

    def write(self, data):
        return self._inner.write(b"x" * len(data))


class _JunkWriter:
    """Emits bytes that are not a zstd frame at all."""

    def __init__(self, fout) -> None:
        self._fout = fout

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def write(self, data):
        return self._fout.write(b"junk")


@pytest.mark.parametrize("mode", ["substituted_payload", "not_a_zstd_frame"])
def test_verify_mismatch_keeps_original(tmp_path, monkeypatch, caplog, mode) -> None:
    writer = _write_journal(tmp_path, _DAY)
    path = _hour_file(writer, _DAY)
    original = path.read_bytes()
    real_compressor = zstd.ZstdCompressor

    class _CorruptingCompressor:
        def __init__(self, level, write_checksum):
            self._real = real_compressor(level=level, write_checksum=write_checksum)

        def stream_writer(self, fout, closefd=False):
            if mode == "not_a_zstd_frame":
                return _JunkWriter(fout)
            return _SubstitutingWriter(self._real.stream_writer(fout, closefd=closefd))

    monkeypatch.setattr(l0_recompress.zstd, "ZstdCompressor", _CorruptingCompressor)

    with caplog.at_level(logging.CRITICAL):
        report = recompress_sealed_day(
            tmp_path, day=_DAY, now_ns=_sealed_now(_DAY), level=_LEVEL, seal_grace_s=_GRACE_S
        )

    assert report.failed == 1
    assert report.recompressed == 0
    assert path.read_bytes() == original
    assert not list(path.parent.glob("*.recompress.tmp"))
    assert "stage=l0_recompress status=VERIFY_FAIL" in caplog.text


def test_second_run_idempotent(tmp_path) -> None:
    writer = _write_journal(tmp_path, _DAY)
    path = _hour_file(writer, _DAY)
    recompress_sealed_day(tmp_path, day=_DAY, now_ns=_sealed_now(_DAY), level=_LEVEL, seal_grace_s=_GRACE_S)
    after_first = path.read_bytes()

    report = recompress_sealed_day(tmp_path, day=_DAY, now_ns=_sealed_now(_DAY), level=_LEVEL, seal_grace_s=_GRACE_S)

    assert report.skipped_not_smaller == report.files == 1
    assert report.recompressed == 0
    assert path.read_bytes() == after_first


def test_readers_unchanged(tmp_path) -> None:
    import polars as pl

    from src.storage.normalization import normalize_l0_partition

    writer = _write_journal(tmp_path, _DAY)
    part = _hour_file(writer, _DAY).parent
    before_out = tmp_path / "before.parquet"
    after_out = tmp_path / "after.parquet"
    normalize_l0_partition(part, before_out, work_root=tmp_path / "work_before")

    recompress_sealed_day(tmp_path, day=_DAY, now_ns=_sealed_now(_DAY), level=_LEVEL, seal_grace_s=_GRACE_S)
    normalize_l0_partition(part, after_out, work_root=tmp_path / "work_after")

    assert pl.read_parquet(before_out).equals(pl.read_parquet(after_out))


def test_other_days_untouched(tmp_path) -> None:
    other = _DAY - dt.timedelta(days=1)
    _write_journal(tmp_path, other, flushes=50)
    writer = _write_journal(tmp_path, _DAY)
    other_path = _hour_file(writer, other)
    other_bytes = other_path.read_bytes()
    other_mtime = other_path.stat().st_mtime_ns

    recompress_sealed_day(tmp_path, day=_DAY, now_ns=_sealed_now(_DAY), level=_LEVEL, seal_grace_s=_GRACE_S)

    assert other_path.read_bytes() == other_bytes
    assert other_path.stat().st_mtime_ns == other_mtime


def test_single_file_failure_never_raises(tmp_path, monkeypatch, caplog) -> None:
    writer = _write_journal(tmp_path, _DAY)
    path = _hour_file(writer, _DAY)
    sealed_mtime = path.stat().st_mtime_ns
    path.write_bytes(b"not a zstd stream at all" * 10)
    os.utime(path, ns=(sealed_mtime, sealed_mtime))

    with caplog.at_level(logging.WARNING):
        report = recompress_sealed_day(
            tmp_path, day=_DAY, now_ns=_sealed_now(_DAY), level=_LEVEL, seal_grace_s=_GRACE_S
        )

    assert report.failed == 1
    assert path.read_bytes() == b"not a zstd stream at all" * 10
    assert "stage=l0_recompress status=FAIL" in caplog.text


def test_file_without_hour_prefix_is_skipped_unsealed(tmp_path) -> None:
    writer = _write_journal(tmp_path, _DAY)
    stray = _hour_file(writer, _DAY).parent / "stray.jsonl.zst"
    stray.write_bytes(zstd.ZstdCompressor(level=3).compress(b"{}\n"))
    stray_bytes = stray.read_bytes()

    report = recompress_sealed_day(tmp_path, day=_DAY, now_ns=_sealed_now(_DAY), level=_LEVEL, seal_grace_s=_GRACE_S)

    assert report.skipped_unsealed == 1
    assert stray.read_bytes() == stray_bytes


def test_append_during_recompress_keeps_original(tmp_path, monkeypatch) -> None:
    # Given: a writer appends to the hour file after it was read but before the replace
    writer = _write_journal(tmp_path, _DAY)
    path = _hour_file(writer, _DAY)
    real_decoded = l0_recompress._decoded_sha256
    late_frame = zstd.ZstdCompressor().compress(b'{"late": true}\n')

    def append_then_hash(tmp):
        with open(path, "ab") as fh:
            fh.write(late_frame)
        return real_decoded(tmp)

    monkeypatch.setattr(l0_recompress, "_decoded_sha256", append_then_hash)
    snapshot_before_append = path.read_bytes()

    # When
    report = recompress_sealed_day(tmp_path, day=_DAY, now_ns=_sealed_now(_DAY), level=_LEVEL, seal_grace_s=_GRACE_S)

    # Then: the original with the late frame is intact and no tmp remains
    assert report.failed == 1
    assert report.recompressed == 0
    assert path.read_bytes() == snapshot_before_append + late_frame
    assert not list(path.parent.glob("*.recompress.tmp"))
