"""Sealed-hour L0 journal recompression: multi-frame append files become one zstd frame."""

from __future__ import annotations

import datetime as dt
import hashlib
import logging
import os
import pathlib
import re
from dataclasses import dataclass
from typing import Final
from zoneinfo import ZoneInfo

import zstandard as zstd

from src.storage.layout import L0_JOURNAL_GLOB

logger = logging.getLogger(__name__)

_KST = ZoneInfo("Asia/Seoul")
_HOUR_RE: Final[re.Pattern[str]] = re.compile(r"^(\d{2})[.]")
_CHUNK_BYTES: Final[int] = 1 << 20
_TMP_SUFFIX: Final[str] = ".recompress.tmp"
# A rewrite must save at least this share of the original size; below it the file is left as-is.
_MAX_RATIO_FOR_REWRITE: Final[float] = 0.98
_NS_PER_S: Final[int] = 1_000_000_000


@dataclass(frozen=True)
class L0RecompressReport:
    files: int
    recompressed: int
    skipped_not_smaller: int
    skipped_unsealed: int
    failed: int
    bytes_before: int
    bytes_after: int


class _VerifyMismatchError(Exception):
    pass


class _ChangedDuringRecompressError(Exception):
    pass


def recompress_sealed_day(
    journal_root: pathlib.Path,
    *,
    day: dt.date,
    now_ns: int,
    level: int,
    seal_grace_s: float,
) -> L0RecompressReport:
    """Rewrite every sealed hour file of `day` as a single zstd frame with identical decompressed bytes.

    Never raises for a single-file failure; the original file is kept on any failure.
    """
    grace_ns = int(seal_grace_s * _NS_PER_S)
    files = recompressed = skipped_not_smaller = skipped_unsealed = failed = 0
    bytes_before = bytes_after = 0
    for path in sorted(journal_root.glob(f"*/**/dt={day.isoformat()}/{L0_JOURNAL_GLOB}")):
        files += 1
        if not _is_sealed(path, day=day, now_ns=now_ns, grace_ns=grace_ns):
            skipped_unsealed += 1
            continue
        original_size = path.stat().st_size
        try:
            outcome = _recompress_file(path, level=level)
        except _VerifyMismatchError:
            failed += 1
            logger.critical("[DATA] stage=l0_recompress status=VERIFY_FAIL path=%s", path)
            continue
        except Exception as exc:  # noqa: BLE001 - one file must never abort the day
            failed += 1
            logger.warning("[DATA] stage=l0_recompress status=FAIL path=%s error=%s", path, exc)
            continue
        if outcome is None:
            skipped_not_smaller += 1
            bytes_before += original_size
            bytes_after += original_size
            continue
        recompressed += 1
        bytes_before += original_size
        bytes_after += outcome
    report = L0RecompressReport(
        files=files,
        recompressed=recompressed,
        skipped_not_smaller=skipped_not_smaller,
        skipped_unsealed=skipped_unsealed,
        failed=failed,
        bytes_before=bytes_before,
        bytes_after=bytes_after,
    )
    logger.info(
        "[DATA] stage=l0_recompress day=%s files=%d recompressed=%d bytes_before=%d bytes_after=%d failed=%d",
        day.isoformat(),
        report.files,
        report.recompressed,
        report.bytes_before,
        report.bytes_after,
        report.failed,
    )
    return report


def _is_sealed(path: pathlib.Path, *, day: dt.date, now_ns: int, grace_ns: int) -> bool:
    match = _HOUR_RE.match(path.name)
    if match is None:
        return False
    hour = int(match.group(1))
    hour_end = dt.datetime.combine(day, dt.time(hour), tzinfo=_KST) + dt.timedelta(hours=1)
    hour_end_ns = int(hour_end.timestamp()) * _NS_PER_S
    if now_ns - hour_end_ns < grace_ns:
        return False
    return now_ns - path.stat().st_mtime_ns >= grace_ns


def _recompress_file(path: pathlib.Path, *, level: int) -> int | None:
    """Return the new size, or None when the rewrite is not smaller (original kept)."""
    st = path.stat()
    tmp = path.with_name(path.name + _TMP_SUFFIX)
    try:
        source_digest = _write_single_frame(path, tmp, level=level)
        tmp_size = tmp.stat().st_size
        if tmp_size >= st.st_size * _MAX_RATIO_FOR_REWRITE:
            tmp.unlink()
            return None
        if _decoded_sha256(tmp) != source_digest:
            tmp.unlink()
            raise _VerifyMismatchError(str(path))
        current = path.stat()
        if (current.st_size, current.st_mtime_ns) != (st.st_size, st.st_mtime_ns):
            # An append after the read would be lost by the replace; keep the original.
            raise _ChangedDuringRecompressError(str(path))
        os.utime(tmp, ns=(st.st_atime_ns, st.st_mtime_ns))
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    _fsync_dir(path.parent)
    return tmp_size


def _write_single_frame(src: pathlib.Path, dst: pathlib.Path, *, level: int) -> str:
    """Stream `src` decompressed into `dst` as one checksummed frame; return sha256 of the decompressed bytes."""
    digest = hashlib.sha256()
    dctx = zstd.ZstdDecompressor()
    cctx = zstd.ZstdCompressor(level=level, write_checksum=True)
    with open(src, "rb") as fin, dctx.stream_reader(fin, read_across_frames=True) as reader, open(dst, "wb") as fout:
        with cctx.stream_writer(fout, closefd=False) as writer:
            while chunk := reader.read(_CHUNK_BYTES):
                digest.update(chunk)
                writer.write(chunk)
        fout.flush()
        os.fsync(fout.fileno())
    return digest.hexdigest()


def _decoded_sha256(path: pathlib.Path) -> str | None:
    digest = hashlib.sha256()
    dctx = zstd.ZstdDecompressor()
    try:
        with open(path, "rb") as fh, dctx.stream_reader(fh, read_across_frames=True) as reader:
            while chunk := reader.read(_CHUNK_BYTES):
                digest.update(chunk)
    except zstd.ZstdError:
        return None
    return digest.hexdigest()


def _fsync_dir(directory: pathlib.Path) -> None:
    fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
