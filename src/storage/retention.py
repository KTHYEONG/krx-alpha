"""수집기 디스크 워터마크 및 저널 보존 가드."""

from __future__ import annotations

import datetime as dt
import logging
import pathlib
import re
import shutil
from collections.abc import Callable, Iterator
from collections.abc import Set as AbstractSet
from dataclasses import dataclass
from typing import Final
from zoneinfo import ZoneInfo

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from src.core.config import SecondaryPatchSettings
from src.core.errors import KrxAlphaError
from src.core.paths import DataPaths
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
from src.storage.secondary_patch import SecondaryInputs, publish_secondary_l1, secondary_inputs

logger = logging.getLogger(__name__)

_KST = ZoneInfo("Asia/Seoul")
_DT_RE = re.compile(r"dt=(\d{4})-(\d{2})-(\d{2})")
HISTORY_MINUTE_RETAIN_DAYS: Final[int] = 400

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
    _held: frozenset[str]

    def __new__(
        cls,
        deleted: int,
        normalized: int = 0,
        failed: int = 0,
        reused: int = 0,
        *,
        invalidated_remote_l1: AbstractSet[str] = frozenset(),
        held: AbstractSet[str] = frozenset(),
    ) -> PruneStats:
        obj = int.__new__(cls, deleted)
        obj._deleted = int(deleted)
        obj._normalized = int(normalized)
        obj._failed = int(failed)
        obj._reused = int(reused)
        obj._invalidated_remote_l1 = frozenset(invalidated_remote_l1)
        obj._held = frozenset(held)
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

    @property
    def held(self) -> frozenset[str]:
        """L1 repo paths whose deletion was held by the repair gate."""
        return self._held


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


@dataclass(frozen=True, slots=True)
class _SecondaryContext:
    """Patch-backup plan of one secondary L0 partition; the patch object is the deletion gate."""

    archive_root: pathlib.Path
    vendor: str
    venue: str
    session: str
    stream: str
    day: dt.date
    inputs: SecondaryInputs
    patch: pathlib.Path
    local: pathlib.Path
    audit: pathlib.Path


def _secondary_context(
    part: pathlib.Path,
    journal_base: pathlib.Path,
    out_rel: pathlib.PurePosixPath,
    part_date: dt.date,
    paths: DataPaths | None,
    settings: SecondaryPatchSettings | None,
) -> _SecondaryContext | None:
    if paths is None or settings is None:
        return None
    rel = part.relative_to(journal_base).parts
    if len(rel) != 5:
        return None
    vendor, venue, session, stream, _ = rel
    inputs = secondary_inputs(paths, part_date, vendor, venue, session, settings)
    if inputs is None:
        return None
    return _SecondaryContext(
        archive_root=paths.archive_root,
        vendor=vendor,
        venue=venue,
        session=session,
        stream=stream,
        day=part_date,
        inputs=inputs,
        patch=paths.secondary_patch_l1(vendor, venue, session, stream, part_date),
        local=paths.l1_local_root / out_rel,
        audit=paths.session_audit_path(part_date, "ls"),
    )


def _reusable_secondary_rows(part: pathlib.Path, ctx: _SecondaryContext) -> int | None:
    """Local full-L1 row count when the patch object and local L1 are both complete and not older than inputs."""
    rows = _reusable_l1_rows(part, ctx.local)
    if rows is None:
        return None
    try:
        patch_mtime = ctx.patch.stat().st_mtime_ns
        if ctx.audit.stat().st_mtime_ns > patch_mtime:
            return None
        pq.read_metadata(ctx.patch)
    except (OSError, pa.ArrowException):
        return None
    return rows


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
    hold: Callable[[pathlib.Path], str | None] | None = None,
    data_paths: DataPaths | None = None,
    secondary: SecondaryPatchSettings | None = None,
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
        hold: Optional repair-gate callback returning a hold reason for a partition.
        data_paths: Enables secondary-feed patch backup when given together with ``secondary``.
        secondary: Secondary-feed patch settings; None keeps every partition on the full-L1 rule.

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
    held: set[str] = set()
    journal_base = pathlib.Path(str(journal_root))
    archive_base = pathlib.Path(str(archive_root)) if not isinstance(archive_root, pathlib.Path) else archive_root
    for part in [p for p in journal_base.rglob("dt=*") if p.is_dir()]:
        m = _DT_RE.fullmatch(part.name)
        part_date = dt.date.fromisoformat(m.group(0)[3:]) if m else None
        if part_date is None or part_date >= cutoff:
            continue
        try:
            out_rel = l1_relpath_for_l0_partition(part.relative_to(journal_base))
            out_path = archive_base / out_rel
            ctx = _secondary_context(part, journal_base, out_rel, part_date, data_paths, secondary)
            if ctx is not None:
                gate_path = ctx.patch
                gate_rel = l1_repo_path(ctx.patch.relative_to(ctx.archive_root))
            else:
                gate_path = out_path
                gate_rel = l1_repo_path(out_path.relative_to(archive_base))
            if not normalize:
                if gate_path.exists() and verified_remote_l1 is not None and gate_rel in verified_remote_l1:
                    if hold is not None:
                        reason = hold(part.relative_to(journal_base))
                        if reason is not None:
                            logger.critical(
                                "[DATA] stage=repair_hold status=OVERRIDDEN reason=low_disk part=%s",
                                str(part.relative_to(journal_base).as_posix()),
                            )
                    deleted += sum(1 for f in part.rglob("*") if f.is_file())
                    shutil.rmtree(part)
                continue
            if ctx is not None or reuse_fresh_l1:
                fresh_rows = (
                    _reusable_secondary_rows(part, ctx) if ctx is not None else _reusable_l1_rows(part, out_path)
                )
                if fresh_rows is not None:
                    logger.info("[DATA] stage=prune status=REUSED part=%s rows=%d", str(part), fresh_rows)
                    normalized += 1
                    reused += 1
                    if verified_remote_l1 is None or gate_rel not in verified_remote_l1:
                        continue
                    if hold is not None:
                        reason = hold(part.relative_to(journal_base))
                        if reason is not None:
                            held.add(gate_rel)
                            continue
                    deleted += sum(1 for f in part.rglob("*") if f.is_file())
                    shutil.rmtree(part)
                    continue
                invalidated_remote_l1.add(gate_rel)
            try:
                normalize_fn = normalizer if normalizer is not None else normalize_l0_partition
                rows = normalize_fn(part, out_path)
            except L1StorageIOError as exc:
                logger.critical(
                    "[DATA] stage=prune status=FAIL reason=storage_io part=%s error=%s", str(part), str(exc)
                )
                failed += 1
                continue
            except L1WorkerCrashError as exc:
                logger.critical(
                    "[DATA] stage=prune status=FAIL reason=worker_crash part=%s error=%s", str(part), str(exc)
                )
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
                    logger.critical("[DATA] stage=quarantine part=%s dest=%s status=MOVED", str(part), str(dest))
                continue
            if rows <= 0 or not out_path.exists():
                logger.critical("[DATA] stage=prune status=FAIL reason=unverified part=%s", str(part))
                continue
            if ctx is not None:
                assert data_paths is not None
                try:
                    publish_secondary_l1(
                        data_paths,
                        ctx.inputs,
                        day=ctx.day,
                        vendor=ctx.vendor,
                        venue=ctx.venue,
                        session=ctx.session,
                        stream=ctx.stream,
                        full_l1=out_path,
                        settings=secondary,  # type: ignore[arg-type]
                    )
                except (OSError, ValueError, pa.ArrowException, pl.exceptions.PolarsError) as exc:
                    logger.critical(
                        "[DATA] stage=prune status=FAIL reason=secondary_patch part=%s error=%s", str(part), str(exc)
                    )
                    failed += 1
                    continue
            normalized += 1
            if reuse_fresh_l1:
                # Remote verification predates this rewrite and cannot justify deletion.
                continue
            if verified_remote_l1 is None or gate_rel not in verified_remote_l1:
                continue
            if hold is not None:
                reason = hold(part.relative_to(journal_base))
                if reason is not None:
                    held.add(gate_rel)
                    continue
            deleted += sum(1 for f in part.rglob("*") if f.is_file())
            shutil.rmtree(part)
        finally:
            if progress is not None:
                progress()
    return PruneStats(
        deleted,
        normalized,
        failed,
        reused,
        invalidated_remote_l1=invalidated_remote_l1,
        held=held,
    )


def _expired_l1_files(
    root: pathlib.Path, *, retain_days: int, ref: dt.date
) -> Iterator[tuple[pathlib.Path, dt.date]]:
    cutoff = ref - dt.timedelta(days=retain_days)
    minute_cutoff = ref - dt.timedelta(days=max(retain_days, HISTORY_MINUTE_RETAIN_DAYS))
    minute_root = root / "history" / "minute"
    for pq_file in sorted(root.rglob("*.parquet")):
        m = _DT_RE.search(pq_file.name)
        if m is None and pq_file.is_relative_to(root / "history" / "tick_tape" / "kiwoom"):
            m = _DT_RE.fullmatch(pq_file.parent.name)
        is_minute = pq_file.is_relative_to(minute_root)
        if m is None and is_minute:
            m = _DT_RE.fullmatch(pq_file.parent.name)
        part_date = dt.date.fromisoformat(m.group(0)[3:]) if m else None
        if part_date is None or part_date >= (minute_cutoff if is_minute else cutoff):
            continue
        yield pq_file, part_date


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
    root = pathlib.Path(archive_root)
    purged = 0
    for pq_file, _ in _expired_l1_files(root, retain_days=retain_days, ref=ref):
        rel = l1_repo_path(pq_file.relative_to(root))
        if rel not in confirmed_remote:
            continue
        pq_file.unlink()
        purged += 1
    return purged


def prune_local_secondary(paths: DataPaths, today: dt.date | None, settings: SecondaryPatchSettings) -> int:
    """Delete local-only full secondary L1 partitions older than `settings.local_retain_days`; the only deletion without a remote copy."""
    ref = today or dt.datetime.now(_KST).date()
    purged = 0
    freed = 0
    for pq_file, _ in _expired_l1_files(paths.l1_local_root, retain_days=settings.local_retain_days, ref=ref):
        freed += pq_file.stat().st_size
        pq_file.unlink()
        purged += 1
    logger.info(
        "[DATA] stage=secondary_local_prune status=OK partitions=%d bytes=%d retain_days=%d",
        purged,
        freed,
        settings.local_retain_days,
    )
    return purged


def unconfirmed_expired_l1(
    archive_root: pathlib.Path,
    *,
    retain_days: int,
    reference_date: dt.date | None,
    confirmed_remote: AbstractSet[str],
) -> list[dt.date]:
    """Partition dates of local L1 files past retention that offsite confirmation has not yet released."""
    ref = reference_date or dt.datetime.now(_KST).date()
    root = pathlib.Path(archive_root)
    return [
        part_date
        for pq_file, part_date in _expired_l1_files(root, retain_days=retain_days, ref=ref)
        if l1_repo_path(pq_file.relative_to(root)) not in confirmed_remote
    ]
