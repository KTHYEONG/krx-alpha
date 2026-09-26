"""universe-plan 유스케이스 서비스 (선정 parquet 기록 + candidates 발행)."""

from __future__ import annotations

import datetime as dt
import logging
import pathlib
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

import polars as pl

from src.execution.contracts import KisApiError
from src.marketdata.partitioned_store import scan_month_partitions
from src.marketdata.snapshot_contracts import SnapshotDataset
from src.storage.snapshot_store import SnapshotStore, SnapshotStoreError
from src.universe.ipc import emit_candidates
from src.universe.policy import compute_selection_features, select_universe

logger = logging.getLogger(__name__)


class SecurityStatusSource(Protocol):
    """Point-in-time exchange designation lookup for a single stock."""

    def get_security_status(self, symbol: str) -> dict[str, object]: ...


@dataclass(frozen=True)
class UniversePlanResult:
    decision_date: dt.date
    selected: int
    out_path: pathlib.Path
    candidates_emitted: int
    status_excluded: int = 0
    status_unknown: int = 0


def plan_universe(
    *,
    bars_root: pathlib.Path,
    decision_date: dt.date,
    lookback_calendar_days: int,
    out_path: pathlib.Path,
    slot_budget: int,
    candidates_path: pathlib.Path | None = None,
    session_date: dt.date | None = None,
    status_source: SecurityStatusSource | None = None,
    snapshot_store: SnapshotStore | None = None,
    wall_ns: Callable[[], int] = time.time_ns,
) -> UniversePlanResult:
    """Select the session universe, drop designated managed stocks, and publish candidates.

    Reads only the month partitions overlapping the lookback window ending at
    ``decision_date``. Rolling windows are at most 60 rows and corporate-action
    normalization cancels outside the window, so the decision-date output
    equals the output computed from full history.

    Managed-stock designation for KOSPI is only observable through the broker
    status flag, so it is checked after bar-based selection on the selected
    symbols. Status lookups that fail keep the symbol: over-collection can be
    filtered offline, a missed session cannot be recovered.

    Raises:
        ValueError: If ``status_source`` is given without ``session_date``.
    """
    if status_source is not None and session_date is None:
        raise ValueError("status_source requires session_date")
    bars = scan_month_partitions(
        pathlib.Path(bars_root),
        min_date=decision_date - dt.timedelta(days=lookback_calendar_days),
        max_date=decision_date,
    ).collect()
    bars = bars.filter(pl.col("date") <= decision_date)
    featured = compute_selection_features(bars)
    selected = select_universe(featured, decision_date, slot_budget=slot_budget)
    status_excluded = 0
    status_unknown = 0
    if status_source is not None:
        assert session_date is not None
        selected, status_excluded, status_unknown = _drop_managed_status(
            selected,
            status_source=status_source,
            session_date=session_date,
            snapshot_store=snapshot_store,
            wall_ns=wall_ns,
        )
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
        decision_date=decision_date,
        selected=selected.height,
        out_path=out,
        candidates_emitted=emitted,
        status_excluded=status_excluded,
        status_unknown=status_unknown,
    )


def _drop_managed_status(
    selected: pl.DataFrame,
    *,
    status_source: SecurityStatusSource,
    session_date: dt.date,
    snapshot_store: SnapshotStore | None,
    wall_ns: Callable[[], int],
) -> tuple[pl.DataFrame, int, int]:
    excluded_symbols: list[str] = []
    observations: list[dict[str, object]] = []
    unknown = 0
    symbols = sorted(selected["symbol"].to_list())
    for symbol in symbols:
        try:
            status = status_source.get_security_status(symbol)
        except KisApiError:
            unknown += 1
            continue
        observed_at_ns = wall_ns()
        if status.get("managed") is True:
            excluded_symbols.append(symbol)
        observations.append({**status, "session_date": session_date, "observed_at_ns": observed_at_ns})
    if snapshot_store is not None and observations:
        try:
            snapshot_store.append(SnapshotDataset.SECURITY_STATUS, observations)
        except SnapshotStoreError as exc:
            logger.error("[DATA] stage=universe_status status=FAIL reason=%s", str(exc))
    checked = len(symbols)
    dropped = selected.filter(~pl.col("symbol").is_in(excluded_symbols)) if excluded_symbols else selected
    if checked > 0 and unknown == checked:
        logger.warning(
            "[DATA] stage=universe_status checked=%d excluded=%d unknown=%d",
            checked,
            len(excluded_symbols),
            unknown,
        )
    else:
        logger.info(
            "[DATA] stage=universe_status checked=%d excluded=%d unknown=%d",
            checked,
            len(excluded_symbols),
            unknown,
        )
    return dropped, len(excluded_symbols), unknown
