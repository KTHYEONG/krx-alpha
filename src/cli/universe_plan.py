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

_DEFAULTS = CollectorSettings()


def add_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """'universe-plan' 서브커맨드를 등록한다."""
    parser = subparsers.add_parser("universe-plan")
    parser.add_argument("--bars-path", required=True)
    parser.add_argument("--decision-date", required=True)
    parser.add_argument("--out-path", required=True)
    parser.add_argument("--slot-budget", type=int, default=_DEFAULTS.universe_slot_budget)
    parser.add_argument("--candidates-path", default=None)
    parser.set_defaults(handler=run)


def run(args: argparse.Namespace) -> int:
    """인자 파싱 + service 위임 + 종료코드 매핑만 수행한다."""
    decision = args.decision_date
    if isinstance(decision, str):
        decision = dt.date.fromisoformat(decision)
    result = plan_universe(
        bars_path=pathlib.Path(str(args.bars_path)),
        decision_date=decision,
        out_path=pathlib.Path(str(args.out_path)),
        slot_budget=int(args.slot_budget),
        candidates_path=(
            pathlib.Path(str(args.candidates_path)) if getattr(args, "candidates_path", None) else None
        ),
    )
    logger.info(
        "[DATA] stage=universe_plan decision=%s selected=%d status=OK",
        result.decision_date.isoformat(),
        result.selected,
    )
    return 0
