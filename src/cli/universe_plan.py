"""Universe-plan CLI 서브커맨드."""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import pathlib

from src.core.config import CollectorSettings
from src.universe.service import UniversePlanResult as UniversePlanResult
from src.universe.service import plan_universe

logger = logging.getLogger(__name__)


def add_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register 'universe-plan'. --slot-budget defaults to None so registration reads no env."""
    parser = subparsers.add_parser("universe-plan")
    parser.add_argument("--bars-path", required=True, help="bars month-partition root directory")
    parser.add_argument("--decision-date", required=True)
    parser.add_argument("--out-path", required=True)
    parser.add_argument("--slot-budget", type=int, default=None)
    parser.add_argument("--candidates-path", default=None)
    parser.set_defaults(handler=run)


def run(args: argparse.Namespace) -> int:
    """Plan the collection universe and write output manifest, returning exit code."""
    settings = CollectorSettings()
    raw_slot_budget = getattr(args, "slot_budget", None)
    slot_budget = int(raw_slot_budget) if raw_slot_budget is not None else settings.universe_slot_budget
    decision = args.decision_date
    if isinstance(decision, str):
        decision = dt.date.fromisoformat(decision)
    result = plan_universe(
        bars_root=pathlib.Path(str(args.bars_path)),
        decision_date=decision,
        lookback_calendar_days=settings.selection_lookback_calendar_days,
        out_path=pathlib.Path(str(args.out_path)),
        slot_budget=slot_budget,
        candidates_path=(
            pathlib.Path(str(args.candidates_path)) if getattr(args, "candidates_path", None) else None
        ),
    )
    logger.info(
        "[DATA] stage=universe_plan decision=%s selected=%d dropped=%d status=OK",
        result.decision_date.isoformat(),
        result.selected,
        result.dropped,
    )
    return 0
