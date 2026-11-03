"""universe-plan 유스케이스 서비스 (선정 parquet 기록 + candidates 발행)."""

from __future__ import annotations

import datetime as dt
import logging
import os
import pathlib
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Protocol

import polars as pl

from src.core.config import UniverseTierSettings
from src.core.paths import DataPaths
from src.execution.contracts import KisApiError
from src.marketdata.partitioned_store import scan_month_partitions
from src.marketdata.snapshot_contracts import SnapshotDataset
from src.storage.snapshot_store import SnapshotStore, SnapshotStoreError
from src.universe.ipc import emit_candidates
from src.universe.policy import (
    compute_selection_features,
    policy_version_for,
    select_universe_detailed,
    select_universe_tiered,
)
from src.universe.status_policy import EXCLUSION_FLAGS, StatusVerdict, WarningLevel, classify_security_status

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
    dropped: int = 0
    dropped_symbols: tuple[str, ...] = ()
    excluded_by_flag: Mapping[str, int] = field(default_factory=dict)  # priority order, policy truncation only


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
    tiers: UniverseTierSettings | None = None,
    paths: DataPaths | None = None,
) -> UniversePlanResult:
    """Select the session universe, tag or exclude by broker status, and publish candidates.

    Reads only the month partitions overlapping the lookback window ending at
    ``decision_date``. Rolling windows are at most 60 rows and corporate-action
    normalization cancels outside the window, so the decision-date output
    equals the output computed from full history.

    Broker status is checked after bar-based selection on the selected symbols.
    Status lookups that fail keep the symbol tagged ``warning_level="unknown"``:
    over-collection can be filtered offline, a missed session cannot be recovered.

    Raises:
        ValueError: If ``status_source`` is given without ``session_date``.

    Policy truncation happens on bar data only, before the status check; slots freed by status exclusion are not back-filled (keeps selection point-in-time and independent of broker status availability). ``dropped`` counts policy truncation only, never status exclusions.
    When ``paths`` is given, policy-dropped rows are written to ``paths.universe_dropped_out(decision_date)`` without status columns.
    """
    if status_source is not None and session_date is None:
        raise ValueError("status_source requires session_date")
    downside = tiers is not None and tiers.downside_enabled
    bars = scan_month_partitions(
        pathlib.Path(bars_root),
        min_date=decision_date - dt.timedelta(days=lookback_calendar_days),
        max_date=decision_date,
    ).collect()
    bars = bars.filter(pl.col("date") <= decision_date)
    featured = compute_selection_features(bars)
    if tiers is None or not tiers.core_enabled:
        selection = select_universe_detailed(featured, decision_date, slot_budget=slot_budget, downside=downside)
    else:
        selection = select_universe_tiered(featured, decision_date, slot_budget=slot_budget, tiers=tiers)
    selected = selection.selected
    status_excluded = 0
    status_unknown = 0
    excluded_by_flag: dict[str, int] = {}
    if status_source is not None:
        assert session_date is not None
        selected, status_excluded, status_unknown, excluded_by_flag = _apply_status_policy(
            selected,
            status_source=status_source,
            session_date=session_date,
            snapshot_store=snapshot_store,
            wall_ns=wall_ns,
        )
    else:
        selected = selected.with_columns([
            pl.lit(WarningLevel.UNKNOWN.value).alias("warning_level"),
            pl.lit([], dtype=pl.List(pl.String)).alias("status_flags"),
        ])
    policy_version = policy_version_for(downside_enabled=downside)
    selected = selected.with_columns(pl.lit(policy_version).alias("policy_version"))
    out = pathlib.Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    selected.write_parquet(out, compression="zstd")
    if paths is not None:
        _write_dropped_parquet(paths.universe_dropped_out(decision_date), selection.dropped, policy_version)
    logger.info(
        "[DATA] stage=universe_plan decision=%s shape=%s status=OK", decision_date.isoformat(), str(selected.shape)
    )
    emitted = 0
    if candidates_path is not None:
        emitted = emit_candidates(
            pathlib.Path(candidates_path),
            selected.to_dicts(),
            rev=int(decision_date.strftime("%Y%m%d")),
            dropped_symbols=tuple(selection.dropped["symbol"]),
        )
    return UniversePlanResult(
        decision_date=decision_date,
        selected=selected.height,
        out_path=out,
        candidates_emitted=emitted,
        status_excluded=status_excluded,
        status_unknown=status_unknown,
        dropped=selection.dropped.height,
        dropped_symbols=tuple(selection.dropped["symbol"]),
        excluded_by_flag=excluded_by_flag,
    )


def _write_dropped_parquet(target: pathlib.Path, dropped: pl.DataFrame, policy_version: str) -> None:
    """Atomically write policy-dropped rows tagged with ``policy_version``; empty input writes an empty schema-only file."""
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(f".{target.name}.tmp")
    dropped.with_columns(pl.lit(policy_version).alias("policy_version")).write_parquet(tmp, compression="zstd")
    os.replace(tmp, target)


def _apply_status_policy(
    selected: pl.DataFrame,
    *,
    status_source: SecurityStatusSource,
    session_date: dt.date,
    snapshot_store: SnapshotStore | None,
    wall_ns: Callable[[], int],
) -> tuple[pl.DataFrame, int, int, dict[str, int]]:
    verdicts: dict[str, StatusVerdict] = {}
    observations: list[dict[str, object]] = []
    lookup_failed = 0
    symbols = sorted(selected["symbol"].to_list())
    for symbol in symbols:
        try:
            status = status_source.get_security_status(symbol)
        except KisApiError:
            lookup_failed += 1
            verdicts[symbol] = StatusVerdict(exclude=False, warning_level=WarningLevel.UNKNOWN, flags=())
            continue
        verdicts[symbol] = classify_security_status(status)
        observations.append({**status, "session_date": session_date, "observed_at_ns": wall_ns()})
    if snapshot_store is not None and observations:
        try:
            snapshot_store.append(SnapshotDataset.SECURITY_STATUS, observations)
        except SnapshotStoreError as exc:
            logger.error("[DATA] stage=universe_status status=FAIL reason=%s", str(exc))
    excluded_symbols = [s for s, v in verdicts.items() if v.exclude]
    excluded_by_flag = {flag: sum(1 for s in excluded_symbols if flag in verdicts[s].flags) for flag in EXCLUSION_FLAGS}
    unknown = lookup_failed
    tags = pl.DataFrame(
        {
            "symbol": list(verdicts),
            "warning_level": [v.warning_level.value for v in verdicts.values()],
            "status_flags": [list(v.flags) for v in verdicts.values()],
        },
        schema={"symbol": selected.schema["symbol"], "warning_level": pl.String, "status_flags": pl.List(pl.String)},
    )
    kept = selected.join(tags, on="symbol", how="left").filter(~pl.col("symbol").is_in(excluded_symbols))
    checked = len(symbols)
    flag_summary = ",".join(f"{flag}:{excluded_by_flag[flag]}" for flag in EXCLUSION_FLAGS)
    if checked > 0 and unknown == checked:
        logger.warning(
            "[DATA] stage=universe_status checked=%d excluded=%d unknown=%d excluded_flags=%s",
            checked,
            len(excluded_symbols),
            unknown,
            flag_summary,
        )
    else:
        logger.info(
            "[DATA] stage=universe_status checked=%d excluded=%d unknown=%d excluded_flags=%s",
            checked,
            len(excluded_symbols),
            unknown,
            flag_summary,
        )
    return kept, len(excluded_symbols), unknown, excluded_by_flag
