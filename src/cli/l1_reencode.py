"""l1-reencode CLI subcommand: weekend-lane L1 backlog re-encode."""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import time
from zoneinfo import ZoneInfo

from src.core.config import CollectorSettings
from src.marketdata.toss_calendar import load_trading_day_cache, trading_day_from_cache
from src.storage.l1_reencode import run_backlog
from src.storage.remote import GDriveArchiver

logger = logging.getLogger(__name__)

_KST = ZoneInfo("Asia/Seoul")


def add_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """'l1-reencode' 서브커맨드를 등록한다."""
    parser = subparsers.add_parser("l1-reencode")
    parser.add_argument("--budget-minutes", type=int, required=True)
    parser.set_defaults(handler=run)


def run(args: argparse.Namespace) -> int:
    """Re-encode the L1 backlog on non-business days only; returns 0 on skip, 1 when the archiver is unavailable."""
    paths = CollectorSettings().paths
    today = dt.datetime.now(_KST).date()
    cached = load_trading_day_cache(paths.calendar_cache)
    day = trading_day_from_cache(cached, today) if cached is not None else None
    if day is None:
        logger.info("[DATA] stage=l1_reencode status=SKIP reason=calendar_unknown")
        return 0
    if day.is_business_day:
        logger.info("[DATA] stage=l1_reencode status=SKIP reason=trading_day")
        return 0
    archiver = GDriveArchiver.try_from_env()
    if archiver is None:
        logger.error("[DATA] stage=l1_reencode status=FAIL reason=rclone_unavailable")
        return 1
    deadline = time.monotonic() + int(args.budget_minutes) * 60
    results = run_backlog(
        archiver,
        l1_root=paths.archive_root,
        work_dir=paths.root / "l1_reencode_tmp",
        deadline_monotonic=deadline,
        ledger_path=paths.root / "work" / "l1_reencode_done.jsonl",
    )
    counts: dict[str, int] = {}
    for result in results:
        counts[result.status] = counts.get(result.status, 0) + 1
    logger.info("[DATA] stage=l1_reencode status=DONE files=%d counts=%s", len(results), sorted(counts.items()))
    return 0
