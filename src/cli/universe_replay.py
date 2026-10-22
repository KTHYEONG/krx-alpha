"""universe-replay CLI 서브커맨드."""

from __future__ import annotations

import argparse
import datetime as dt
import logging

from src.core.config import CollectorSettings, UniverseTierSettings
from src.universe.replay import replay_universe_history

logger = logging.getLogger(__name__)


def add_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("universe-replay")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--overwrite", action="store_true", default=False)
    parser.set_defaults(handler=run)


def run(args: argparse.Namespace) -> int:
    try:
        settings = CollectorSettings()
        paths = settings.paths
        start = dt.date.fromisoformat(str(args.start))
        end = dt.date.fromisoformat(str(args.end))
        summary = replay_universe_history(
            bars_root=paths.bars_daily_dir,
            out_dir=paths.universe_replay_dir,
            start=start,
            end=end,
            lookback_calendar_days=settings.selection_lookback_calendar_days,
            slot_budget=settings.universe_slot_budget,
            tiers=UniverseTierSettings(),
            overwrite=bool(getattr(args, "overwrite", False)),
        )
    except Exception as exc:
        logger.error("[DATA] stage=universe_replay status=FAIL reason=%s", str(exc))
        return 4
    logger.info(
        "[DATA] stage=universe_replay status=OK written=%d skipped=%d",
        summary.written,
        summary.skipped,
    )
    return 0
