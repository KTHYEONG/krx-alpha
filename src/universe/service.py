"""universe-plan 유스케이스 서비스 (선정 parquet 기록 + candidates 발행)."""

from __future__ import annotations

import datetime as dt
import logging
import pathlib
from dataclasses import dataclass

import polars as pl

from src.universe.ipc import emit_candidates
from src.universe.policy import compute_selection_features, select_universe

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class UniversePlanResult:
    decision_date: dt.date
    selected: int
    out_path: pathlib.Path
    candidates_emitted: int


def plan_universe(
    *,
    bars_path: pathlib.Path,
    decision_date: dt.date,
    out_path: pathlib.Path,
    slot_budget: int,
    candidates_path: pathlib.Path | None = None,
) -> UniversePlanResult:
    """판정일 유니버스를 선정해 parquet 으로 기록하고 candidates IPC 를 발행한다."""
    bars = pl.read_parquet(pathlib.Path(bars_path))
    bars = bars.filter(pl.col("date") <= decision_date)
    featured = compute_selection_features(bars)
    selected = select_universe(featured, decision_date, slot_budget=slot_budget)
    out = pathlib.Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    selected.write_parquet(out, compression="zstd")
    logger.info(
        "[DATA] stage=universe_plan decision=%s shape=%s status=OK", decision_date.isoformat(), str(selected.shape)
    )
    emitted = 0
    if candidates_path is not None:
        emitted = emit_candidates(
            pathlib.Path(candidates_path), selected.to_dicts(), rev=int(decision_date.strftime("%Y%m%d"))
        )
    return UniversePlanResult(
        decision_date=decision_date, selected=selected.height, out_path=out, candidates_emitted=emitted
    )
