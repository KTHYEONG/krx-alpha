"""Point-in-time tiered universe replay from daily bars."""

from __future__ import annotations

import datetime as dt
import logging
import os
import pathlib
from dataclasses import dataclass

import polars as pl

from src.core.config import UniverseTierSettings
from src.core.paths import DataPaths
from src.marketdata.partitioned_store import scan_month_partitions
from src.universe.policy import compute_selection_features, select_universe_tiered

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ReplaySummary:
    start: dt.date
    end: dt.date
    dates: tuple[dt.date, ...]
    written: int
    skipped: int
    replay_run_id: str
    core_enabled: bool
    short_warmup_dates: tuple[dt.date, ...] = ()


def replay_universe_history(
    *,
    bars_root: pathlib.Path,
    out_dir: pathlib.Path,
    start: dt.date,
    end: dt.date,
    lookback_calendar_days: int,
    slot_budget: int,
    tiers: UniverseTierSettings,
    overwrite: bool = False,
) -> ReplaySummary:
    """Write the tiered universe each trading date in [start, end] would have produced, using only bars dated ≤ that date."""
    if start > end:
        raise ValueError("start must be <= end")
    if lookback_calendar_days < 1:
        raise ValueError("lookback_calendar_days must be >= 1")
    if slot_budget < tiers.core_exit_rank:
        raise ValueError("slot_budget must be >= core_exit_rank")
    root = pathlib.Path(bars_root).resolve()
    out = pathlib.Path(out_dir).resolve()
    live = DataPaths(root.parent.parent).universe_dir.resolve()
    if out in (live, root) or live in out.parents or root in out.parents:
        raise ValueError("replay output must be separate from live universe and bars")
    run_id = f"replay_{start.isoformat()}_{end.isoformat()}"
    forced = tiers.model_copy(update={"core_enabled": True})
    decision_dates = sorted({
        d
        for d in scan_month_partitions(root, min_date=start, max_date=end)
        .select(pl.col("date").unique())
        .collect()
        .get_column("date")
        .to_list()
        if isinstance(d, dt.date) and start <= d <= end
    })
    total = len(decision_dates)
    written = 0
    skipped = 0
    short_warmup: list[dt.date] = []
    for idx, day in enumerate(decision_dates, start=1):
        if idx % 20 == 0:
            logger.info(
                "[DATA] stage=universe_replay progress=%d/%d date=%s",
                idx, total, day.isoformat(),
            )
        target = out / f"{day.isoformat()}.parquet"
        if target.exists() and not overwrite:
            skipped += 1
            continue
        bars = (
            scan_month_partitions(
                root,
                min_date=day - dt.timedelta(days=lookback_calendar_days),
                max_date=day,
            )
            .collect()
            .filter(pl.col("date") <= day)
        )
        featured = compute_selection_features(bars)
        ready_days = featured.filter(pl.col("tv_median_20").is_not_null())["date"].n_unique()
        is_short = ready_days < forced.core_warmup_sessions
        selection = select_universe_tiered(featured, day, slot_budget=slot_budget, tiers=forced)
        frame = selection.selected.with_columns([
            pl.lit("unavailable").cast(pl.String).alias("managed_filter"),
            pl.lit(is_short).cast(pl.Boolean).alias("short_warmup"),
            pl.lit(run_id).cast(pl.String).alias("replay_run_id"),
        ])
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_name(f".{target.name}.tmp")
        frame.write_parquet(tmp, compression="zstd")
        os.replace(tmp, target)
        written += 1
        if is_short:
            short_warmup.append(day)
    return ReplaySummary(
        start=start,
        end=end,
        dates=tuple(decision_dates),
        written=written,
        skipped=skipped,
        replay_run_id=run_id,
        core_enabled=True,
        short_warmup_dates=tuple(short_warmup),
    )
