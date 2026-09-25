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

from src.core.errors import KrxAlphaError
from src.storage.normalization import (
    _BATCH_BYTES,
    _GATHER_ROWS,
    L1NormalizationError,
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

    def __new__(cls, deleted: int, normalized: int = 0) -> PruneStats:
        obj = int.__new__(cls, deleted)
        obj._deleted = int(deleted)
        obj._normalized = int(normalized)
        return obj

    @property
    def deleted(self) -> int:
        return self._deleted

    @property
    def normalized(self) -> int:
        return self._normalized


def prune_old_journals(
    journal_root: pathlib.Path,
    *,
    archive_root: pathlib.Path | None = None,
    retain_days: int = 3,
    reference_date: dt.date | None = None,
    quarantine_root: pathlib.Path | None = None,
    normalizer: Callable[[pathlib.Path, pathlib.Path], int] | None = None,
    verified_remote_l1: AbstractSet[str] | None = None,
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

    Returns:
        Existing deleted and normalized counts.
    """
    if archive_root is None or journal_root is None:
        return PruneStats(0, 0)
    ref = reference_date or dt.datetime.now(_KST).date()
    cutoff = ref - dt.timedelta(days=retain_days)
    deleted = 0
    normalized = 0
    archive_base = pathlib.Path(str(archive_root)) if not isinstance(archive_root, pathlib.Path) else archive_root
    for part in [p for p in pathlib.Path(str(journal_root)).rglob("dt=*") if p.is_dir()]:
        m = _DT_RE.fullmatch(part.name)
        part_date = dt.date.fromisoformat(m.group(0)[3:]) if m else None
        if part_date is None or part_date >= cutoff:
            continue
        rel_parent = part.relative_to(pathlib.Path(str(journal_root))).parent
        out_path = archive_base / rel_parent / f"{part.name}.parquet"
        try:
            normalize = normalizer if normalizer is not None else normalize_l0_partition
            rows = normalize(part, out_path)
        except L1WorkerCrashError as exc:
            logger.critical("[DATA] stage=prune status=FAIL reason=worker_crash part=%s error=%s", str(part), str(exc))
            continue
        except L1NormalizationError as exc:
            logger.critical("[DATA] stage=prune status=FAIL reason=%s part=%s", str(exc), str(part))
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
        rel = "l1/" + out_path.relative_to(archive_base).as_posix()
        if verified_remote_l1 is None or rel not in verified_remote_l1:
            continue
        deleted += sum(1 for f in part.rglob("*") if f.is_file())
        shutil.rmtree(part)
    return PruneStats(deleted, normalized)


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
        rel = "l1/" + pq_file.relative_to(root).as_posix()
        if rel not in confirmed_remote:
            continue
        pq_file.unlink()
        purged += 1
    return purged
