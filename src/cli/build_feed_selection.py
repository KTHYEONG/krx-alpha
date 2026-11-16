"""build-feed-selection CLI (nightly one-shot child; derived selection and quality only, never touches vendor partitions)."""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import pathlib
from zoneinfo import ZoneInfo

from src.core.config import CollectorSettings, FeedControlSettings
from src.core.paths import DataPaths
from src.storage.feed_quality import build_feed_quality, page_quality_floor, write_feed_quality
from src.storage.feed_selection import SelectionSpec, build_feed_selection

logger = logging.getLogger(__name__)

_KST = ZoneInfo("Asia/Seoul")


def add_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register `build-feed-selection` (one-shot nightly child after repair-ticks)."""
    parser = subparsers.add_parser("build-feed-selection")
    parser.add_argument("--deadline", default=None, help="ISO KST deadline; accepted for the nightly contract")
    parser.add_argument("--day", default=None, type=dt.date.fromisoformat, help="trading day (default: previous KST date)")
    parser.add_argument("--data-root", type=pathlib.Path, default=None)
    parser.set_defaults(handler=run)


def run(args: argparse.Namespace) -> int:
    """Build the selection and quality report; 0 when written or NO_SECONDARY, 2 when watch_groups is empty, 4 on failure."""
    try:
        return _run(args)
    except Exception as exc:  # noqa: BLE001 - setup and I/O failures map to a non-zero exit
        logger.error("[DATA] stage=feed_selection status=FAIL reason=setup error=%s", type(exc).__name__, exc_info=True)
        return 4


def _run(args: argparse.Namespace) -> int:
    settings = FeedControlSettings()
    if not settings.watch_groups:
        logger.info("[DATA] stage=feed_selection status=NOOP reason=no_watch_groups")
        return 2
    root = args.data_root or CollectorSettings().paths.root
    paths = DataPaths(root)
    day = args.day or (dt.datetime.now(_KST).date() - dt.timedelta(days=1))
    summary = build_feed_selection(day, paths=paths, spec=SelectionSpec())
    report = build_feed_quality(day, paths=paths)
    write_feed_quality(report, paths=paths)
    page_quality_floor(report, paths=paths, settings=settings)
    logger.info("[DATA] stage=feed_selection status=%s date=%s rows=%d", summary.status, day.isoformat(), summary.rows)
    return 0
