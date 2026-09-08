"""수집 유니버스 선정 정책."""

from __future__ import annotations

import datetime as dt
import logging

import polars as pl

logger = logging.getLogger(__name__)

REQUIRED_BAR_COLUMNS: tuple[str, ...] = ("date", "symbol", "close", "volume", "trade_value_100m", "daily_change_pct")
FEATURE_COLUMNS: tuple[str, ...] = ("tv_median_20", "close_max_60", "tv_ratio")
SELECTION_REASONS: tuple[str, ...] = ("limit_up", "surge10", "volsurge", "newhigh60")
PRICE_LIMIT_GUARD_PCT: float = 31.0
LIMIT_UP_PCT: float = 29.0
SURGE_PCT: float = 10.0
VOLSURGE_RATIO: float = 5.0
VOLSURGE_MIN_CHANGE_PCT: float = 5.0
NEWHIGH_LOOKBACK: int = 60
NEWHIGH_MIN_CHANGE_PCT: float = 5.0
TV_MEDIAN_WINDOW: int = 20
LIQUIDITY_FLOOR_100M: float = 50.0
DEEP_SLOT_BUDGET: int = 40


class SlotBudgetExceededError(RuntimeError):
    """선정 종목 수가 슬롯 예산을 초과했다."""


def compute_selection_features(bars: pl.DataFrame) -> pl.DataFrame:
    """일봉에 선정용 롤링 피처를 부여한다."""
    missing = [c for c in REQUIRED_BAR_COLUMNS if c not in bars.columns]
    if missing:
        raise ValueError(f"missing required bar columns: {missing}")
    clean = bars.drop_nulls(subset=list(REQUIRED_BAR_COLUMNS))
    clean = clean.filter(
        (pl.col("volume") > 0)
        & (pl.col("close") > 0)
        & (pl.col("daily_change_pct").abs() <= PRICE_LIMIT_GUARD_PCT)
    )
    clean = clean.sort(["symbol", "date"])
    clean = clean.with_columns([
        pl.col("close").cast(pl.Float64),
        pl.col("trade_value_100m").cast(pl.Float64),
        pl.col("volume").cast(pl.Int64),
        pl.col("daily_change_pct").cast(pl.Float64),
    ])
    clean = clean.with_columns([
        pl.col("trade_value_100m").rolling_median(window_size=TV_MEDIAN_WINDOW).over("symbol").alias("tv_median_20"),
        pl.col("close").rolling_max(window_size=NEWHIGH_LOOKBACK).over("symbol").alias("close_max_60"),
    ])
    clean = clean.with_columns([
        pl.when(pl.col("tv_median_20") > 0)
        .then(pl.col("trade_value_100m") / pl.col("tv_median_20"))
        .otherwise(None)
        .alias("tv_ratio"),
    ])
    logger.info("[DATA] stage=features shape=%s status=OK", str(clean.shape))
    return clean


def select_universe(featured: pl.DataFrame, decision_date: dt.date, *, slot_budget: int = DEEP_SLOT_BUDGET) -> pl.DataFrame:
    """판정일 후보에 조건 태그를 부여하고 슬롯 예산을 강제한다."""
    missing = [c for c in list(REQUIRED_BAR_COLUMNS) + list(FEATURE_COLUMNS) if c not in featured.columns]
    if missing:
        raise ValueError(f"missing required columns: {missing}")
    if featured.height > 0:
        max_date = featured["date"].max()
        if isinstance(max_date, dt.date) and max_date > decision_date:
            raise ValueError(f"lookahead: max date {max_date!r} exceeds decision_date {decision_date!r}")
    cand = featured.filter(pl.col("date") == decision_date)
    cond_limit_up = (pl.col("daily_change_pct") >= LIMIT_UP_PCT).fill_null(False)
    cond_surge10 = (pl.col("daily_change_pct") >= SURGE_PCT).fill_null(False)
    cond_volsurge = ((pl.col("tv_ratio") >= VOLSURGE_RATIO) & (pl.col("daily_change_pct") >= VOLSURGE_MIN_CHANGE_PCT)).fill_null(
        False
    )
    cond_newhigh60 = ((pl.col("close") >= pl.col("close_max_60")) & (pl.col("daily_change_pct") >= NEWHIGH_MIN_CHANGE_PCT)).fill_null(
        False
    )
    liquid = pl.col("trade_value_100m") >= LIQUIDITY_FLOOR_100M
    cand = cand.with_columns(
        pl.concat_list([
            pl.when(cond_limit_up).then(pl.lit("limit_up")).otherwise(None),
            pl.when(cond_surge10).then(pl.lit("surge10")).otherwise(None),
            pl.when(cond_volsurge).then(pl.lit("volsurge")).otherwise(None),
            pl.when(cond_newhigh60).then(pl.lit("newhigh60")).otherwise(None),
        ])
        .list.drop_nulls()
        .alias("selection_reasons")
    )
    selected = cand.filter((cond_limit_up | cond_surge10 | cond_volsurge | cond_newhigh60) & liquid)
    if selected.height > slot_budget:
        raise SlotBudgetExceededError(f"selected {selected.height} exceeds slot_budget {slot_budget}")
    out = selected.with_columns(pl.lit(decision_date).alias("decision_date")).select([
        "decision_date",
        "symbol",
        "selection_reasons",
        "daily_change_pct",
        "trade_value_100m",
        "tv_ratio",
    ]).sort("symbol")
    logger.info("[ALGO] decision=%s selected=%d budget=%d", decision_date.isoformat(), out.height, slot_budget)
    return out
