"""EOD 유지보수/오프로드 (daemon 에서 추출한 타입드 유스케이스)."""

from __future__ import annotations

import datetime as dt
import logging
import pathlib
from typing import Any

import polars as pl

from src.storage.remote import RcloneArchiver
from src.storage.retention import prune_local_l1, prune_old_journals

logger = logging.getLogger(__name__)


def run_eod_maintenance(
    journal_root: pathlib.Path,
    *,
    retain_days: int = 3,
    today: dt.date | None = None,
    archive_root: pathlib.Path | None = None,
    quarantine_root: pathlib.Path | None = None,
) -> int:
    return prune_old_journals(journal_root, retain_days=retain_days, reference_date=today, archive_root=archive_root, quarantine_root=quarantine_root)


def run_eod_offload(
    archive_root: pathlib.Path,
    *,
    archiver: Any = None,
    reference_date: dt.date | None = None,
    retain_days: int = 30,
) -> dict[str, int]:
    arc = archiver if archiver is not None else RcloneArchiver.try_from_env()
    if arc is None:
        logger.critical("[DAEMON] stage=eod_offload status=FAIL reason=rclone_settings_missing")
        return {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0}
    stats = arc.sync_l1_tree(archive_root)
    confirmed = arc.remote_files("l1/")
    stats["purged"] = prune_local_l1(
        archive_root, retain_days=retain_days, reference_date=reference_date, confirmed_remote=confirmed
    )
    return stats


def check_session_reconciliation(
    *, bars_store: pathlib.Path, manifest_path: pathlib.Path, date: dt.date
) -> bool:
    store = pathlib.Path(bars_store)
    manifest = pathlib.Path(manifest_path)
    if not store.exists():
        return True
    rows = pl.scan_parquet(store).filter(pl.col("date") == date).collect().height
    if rows == 0:
        return True
    return manifest.exists()
