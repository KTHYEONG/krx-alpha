"""수집기 디스크 워터마크 및 저널 보존 가드."""

from __future__ import annotations

import datetime as dt
import logging
import pathlib
import re
import shutil
from collections.abc import Callable
from collections.abc import Set as AbstractSet
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq

from src.core.errors import KrxAlphaError
from src.storage.layout import l1_relpath_for_l0_partition, l1_repo_path
from src.storage.normalization import (
    _BATCH_BYTES,
    _GATHER_ROWS,
    L1NormalizationError,
    L1StorageIOError,
    _dedup_sort_order,
    _iter_line_batches,
    normalize_l0_partition,
)

logger = logging.getLogger(__name__)

_KST = ZoneInfo("Asia/Seoul")
_DT_RE = re.compile(r"dt=(\d{4})-(\d{2})-(\d{2})")

__all__ = [
    "_BATCH_BYTES",
    "_GATHER_ROWS",
    "L1NormalizationError",
    "L1StorageIOError",
    "_dedup_sort_order",
    "_iter_line_batches",
    "normalize_l0_partition",
]


class StorageExhaustedError(KrxAlphaError):
    """여유 디스크가 워터마크 미만일 때 발생하는 Fail-Closed 신호."""


class L1WorkerCrashError(KrxAlphaError):
    """정규화 워커 프로세스 비정상 종료(OOM SIGKILL 등) 신호 — 데이터 결함 아님, 격리 금지."""


def check_disk_watermark(path: pathlib.Path, *, min_free_gb: float = 3.0) -> bool:
    usage = shutil.disk_usage(path)
    return usage.free >= min_free_gb * (1024**3)


class PruneStats(int):
    """L0 prune 결과: int 값은 deleted 파일 수와 동일하게 비교된다."""

    _deleted: int
    _normalized: int
    _failed: int
    _reused: int
    _invalidated_remote_l1: frozenset[str]

    def __new__(
        cls, deleted: int, normalized: int = 0, failed: int = 0, reused: int = 0,
        *, invalidated_remote_l1: AbstractSet[str] = frozenset(),
    ) -> PruneStats:
        obj = int.__new__(cls, deleted)
        obj._deleted = int(deleted)
        obj._normalized = int(normalized)
        obj._failed = int(failed)
        obj._reused = int(reused)
        obj._invalidated_remote_l1 = frozenset(invalidated_remote_l1)
        return obj

    @property
    def deleted(self) -> int:
        return self._deleted

    @property
    def normalized(self) -> int:
        return self._normalized

    @property
    def failed(self) -> int:
        return self._failed

    @property
    def reused(self) -> int:
        return self._reused

    @property
    def invalidated_remote_l1(self) -> frozenset[str]:
        """L1 paths whose previous verification cannot justify remote L0 deletion."""
        return self._invalidated_remote_l1


def _reusable_l1_rows(part: pathlib.Path, out_path: pathlib.Path) -> int | None:
    """Row count of ``out_path`` when it is a complete L1 for ``part``, else None.

    Complete means the file exists, its parquet footer is readable with at least one row,
    and its mtime is not older than the newest file under ``part``. Any I/O or parquet
    error yields None so the caller falls back to normalization.
    """
    try:
        if not out_path.is_file():
            return None
        newest = max((f.stat().st_mtime_ns for f in part.rglob("*") if f.is_file()), default=None)
        if newest is not None and out_path.stat().st_mtime_ns < newest:
            return None
        num_rows = pq.read_metadata(out_path).num_rows
    except (OSError, pa.ArrowException):
        return None
    if num_rows <= 0:
        return None
    return int(num_rows)


def prune_old_journals(
    journal_root: pathlib.Path,
    *,
    archive_root: pathlib.Path | None = None,
    retain_days: int = 3,
    reference_date: dt.date | None = None,
    quarantine_root: pathlib.Path | None = None,
    normalizer: Callable[[pathlib.Path, pathlib.Path], int] | None = None,
    verified_remote_l1: AbstractSet[str] | None = None,
    progress: Callable[[], None] | None = None,
    normalize: bool = True,
    reuse_fresh_l1: bool = False,
) -> PruneStats:
    """Normalize eligible L0 partitions and delete only remotely verified inputs.

    Args:
        journal_root: Local L0 root.
        archive_root: Local L1 root.
        retain_days: Minimum age before processing.
        reference_date: KST retention reference date.
        quarantine_root: Optional non-destructive corruption destination.
        normalizer: Optional normalization boundary for isolated workers.
        verified_remote_l1: Paths confirmed present with matching remote bytes.
        progress: Optional per-partition-attempt callback, called whatever the outcome.
        normalize: When False (low-disk recovery), never run the normalizer and
            only delete partitions whose L1 already exists locally and is in
            ``verified_remote_l1``; normalizing needs spill space the disk lacks.
        reuse_fresh_l1: When True, a partition whose L1 parquet is complete and
            not older than every file of its L0 partition is counted as normalized
            without invoking the normalizer, and the L1 file is left byte-for-byte
            untouched. A missing, unreadable, empty, or stale L1 is normalized as
            usual, so a failed earlier attempt is retried. Ignored when
            ``normalize`` is False. Rebuilt L1 files must be remotely verified
            again before their L0 inputs can be deleted.

    Returns:
        Prune counts and L1 paths whose prior remote verification was invalidated.
    """
    if archive_root is None or journal_root is None:
        return PruneStats(0, 0)
    ref = reference_date or dt.datetime.now(_KST).date()
    cutoff = ref - dt.timedelta(days=retain_days)
    deleted = 0
    normalized = 0
    failed = 0
    reused = 0
    invalidated_remote_l1: set[str] = set()
    archive_base = pathlib.Path(str(archive_root)) if not isinstance(archive_root, pathlib.Path) else archive_root
    for part in [p for p in pathlib.Path(str(journal_root)).rglob("dt=*") if p.is_dir()]:
        m = _DT_RE.fullmatch(part.name)
        part_date = dt.date.fromisoformat(m.group(0)[3:]) if m else None
        if part_date is None or part_date >= cutoff:
            continue
        try:
            out_path = archive_base / l1_relpath_for_l0_partition(
                part.relative_to(pathlib.Path(str(journal_root)))
            )
            if not normalize:
                rel = l1_repo_path(out_path.relative_to(archive_base))
                if out_path.exists() and verified_remote_l1 is not None and rel in verified_remote_l1:
                    deleted += sum(1 for f in part.rglob("*") if f.is_file())
                    shutil.rmtree(part)
                continue
            if reuse_fresh_l1:
                fresh_rows = _reusable_l1_rows(part, out_path)
                if fresh_rows is not None:
                    logger.info(
                        "[DATA] stage=prune status=REUSED part=%s rows=%d", str(part), fresh_rows
                    )
                    normalized += 1
                    reused += 1
                    rel = l1_repo_path(out_path.relative_to(archive_base))
                    if verified_remote_l1 is None or rel not in verified_remote_l1:
                        continue
                    deleted += sum(1 for f in part.rglob("*") if f.is_file())
                    shutil.rmtree(part)
                    continue
                invalidated_remote_l1.add(l1_repo_path(out_path.relative_to(archive_base)))
            try:
                normalize_fn = normalizer if normalizer is not None else normalize_l0_partition
                rows = normalize_fn(part, out_path)
            except L1StorageIOError as exc:
                logger.critical("[DATA] stage=prune status=FAIL reason=storage_io part=%s error=%s", str(part), str(exc))
                failed += 1
                continue
            except L1WorkerCrashError as exc:
                logger.critical("[DATA] stage=prune status=FAIL reason=worker_crash part=%s error=%s", str(part), str(exc))
                failed += 1
                continue
            except L1NormalizationError as exc:
                logger.critical("[DATA] stage=prune status=FAIL reason=%s part=%s", str(exc), str(part))
                failed += 1
                if quarantine_root is not None:
                    rel_part = part.relative_to(pathlib.Path(str(journal_root)))
                    dest = pathlib.Path(quarantine_root) / rel_part
                    if dest.exists():
                        logger.critical(
                            "[DATA] stage=quarantine part=%s status=FAIL reason=quarantine_exists", str(part)
                        )
                        continue
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(part), str(dest))
                    logger.critical(
                        "[DATA] stage=quarantine part=%s dest=%s status=MOVED", str(part), str(dest)
                    )
                continue
            if rows <= 0 or not out_path.exists():
                logger.critical("[DATA] stage=prune status=FAIL reason=unverified part=%s", str(part))
                continue
            normalized += 1
            if reuse_fresh_l1:
                # Remote verification predates this rewrite and cannot justify deletion.
                continue
            rel = l1_repo_path(out_path.relative_to(archive_base))
            if verified_remote_l1 is None or rel not in verified_remote_l1:
                continue
            deleted += sum(1 for f in part.rglob("*") if f.is_file())
            shutil.rmtree(part)
        finally:
            if progress is not None:
                progress()
    return PruneStats(
        deleted, normalized, failed, reused, invalidated_remote_l1=invalidated_remote_l1,
    )


def prune_local_l1(
    archive_root: pathlib.Path,
    *,
    retain_days: int = 30,
    reference_date: dt.date | None = None,
    confirmed_remote: set[str] | None = None,
) -> int:
    if confirmed_remote is None:
        return 0
    ref = reference_date or dt.datetime.now(_KST).date()
    cutoff = ref - dt.timedelta(days=retain_days)
    root = pathlib.Path(archive_root)
    purged = 0
    for pq_file in sorted(root.rglob("*.parquet")):
        m = _DT_RE.search(pq_file.name)
        part_date = dt.date.fromisoformat(m.group(0)[3:]) if m else None
        if part_date is None or part_date >= cutoff:
            continue
        rel = l1_repo_path(pq_file.relative_to(root))
        if rel not in confirmed_remote:
            continue
        pq_file.unlink()
        purged += 1
    return purged
