"""Universe-plan CLI 서브커맨드."""

from __future__ import annotations

import argparse
import datetime as dt
import logging
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)


def add_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """'universe-plan' 서브커맨드를 등록한다."""
    from src.universe.policy import DEEP_SLOT_BUDGET

    parser = subparsers.add_parser("universe-plan")
    parser.add_argument("--bars-path", required=True)
    parser.add_argument("--decision-date", required=True)
    parser.add_argument("--out-path", required=True)
    parser.add_argument("--slot-budget", type=int, default=DEEP_SLOT_BUDGET)
    parser.add_argument("--candidates-path", default=None)
    parser.set_defaults(handler=run)


def run(args: argparse.Namespace) -> int:
    """선정 결과를 parquet 으로 기록한다."""
    from src.universe.policy import compute_selection_features, select_universe

    decision = args.decision_date
    if isinstance(decision, str):
        decision = dt.date.fromisoformat(decision)
    bars = pl.read_parquet(Path(str(args.bars_path)))
    bars = bars.filter(pl.col("date") <= decision)
    featured = compute_selection_features(bars)
    selected = select_universe(featured, decision, slot_budget=int(args.slot_budget))
    logger.info("[DATA] stage=universe_plan decision=%s shape=%s status=OK", decision.isoformat(), str(selected.shape))
    out_path = Path(str(args.out_path))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    selected.write_parquet(out_path, compression="zstd")
    if getattr(args, "candidates_path", None):
        from src.collector.scan_bridge import emit_candidates

        emit_candidates(Path(str(args.candidates_path)), selected.to_dicts(), rev=int(decision.strftime("%Y%m%d")))
    return 0
