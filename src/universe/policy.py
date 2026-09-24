"""수집 유니버스 선정 정책."""

from __future__ import annotations

import datetime as dt
import logging

import polars as pl

from src.core.errors import SlotBudgetExceededError
from src.marketdata.schema import REQUIRED_BAR_COLUMNS

logger = logging.getLogger(__name__)

FEATURE_COLUMNS: tuple[str, ...] = ("tv_median_20", "close_max_60", "tv_ratio")
SELECTION_REASONS: tuple[str, ...] = ("limit_up", "surge10", "volsurge", "newhigh60")
PRICE_LIMIT_GUARD_PCT: float = 31.0
LIMIT_UP_PCT: float = 29.0
SURGE_PCT: float = 10.0
VOLSURGE_RATIO: float = 5.0
VOLSURGE_MIN_CHANGE_PCT: float = 5.0
NEWHIGH_LOOKBACK: int = 60
NEWHIGH_MIN_CHANGE_PCT: float = 5.0
# 소수 둘째 자리 daily_change_pct 반올림 오차는 상대오차 1e-4 이하(실측 최대 7.05e-5)이며,
# 실제 분할·병합은 기준가가 2배 이상 움직이므로 0.5%는 양쪽과 두 자릿수 격차로 구분된다.
IMPLIED_BASE_EVENT_TOLERANCE: float = 0.005
TV_MEDIAN_WINDOW: int = 20
LIQUIDITY_FLOOR_100M: float = 50.0
DEEP_SLOT_BUDGET: int = 90
ELIGIBLE_STOCK_CERT_KIND: str = "보통주"
EXCLUDED_SECTIONS: frozenset[str] = frozenset({"관리종목(소속부없음)", "SPAC(소속부없음)", "투자주의환기종목(소속부없음)"})
SECURITY_CLASS_COLUMNS: tuple[str, ...] = ("section", "stock_cert_kind")


def compute_selection_features(bars: pl.DataFrame) -> pl.DataFrame:
    """Attach rolling selection features; close_max_60 is corporate-action adjusted to each row's own price basis using only rows at or before it."""
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
    for column in SECURITY_CLASS_COLUMNS:
        if column in clean.columns:
            clean = clean.with_columns(pl.col(column).forward_fill().over("symbol"))
    clean = clean.with_columns([
        pl.col("trade_value_100m").rolling_median(window_size=TV_MEDIAN_WINDOW).over("symbol").alias("tv_median_20"),
    ])
    clean = _with_close_max_60(clean)
    clean = clean.with_columns([
        pl.when(pl.col("tv_median_20") > 0)
        .then(pl.col("trade_value_100m") / pl.col("tv_median_20"))
        .otherwise(None)
        .alias("tv_ratio"),
    ])
    logger.info("[DATA] stage=features shape=%s status=OK", str(clean.shape))
    return clean


def _with_close_max_60(clean: pl.DataFrame) -> pl.DataFrame:
    prev_close = pl.col("close").shift(1).over("symbol")
    implied_expr = pl.col("close") / (1.0 + pl.col("daily_change_pct") / 100.0)
    implied_valid = (
        implied_expr.is_not_null() & implied_expr.is_finite() & (implied_expr > 0)
    ).fill_null(False)
    clean = clean.with_columns([
        implied_expr.alias("_implied_base"),
        implied_valid.alias("_implied_valid"),
        prev_close.alias("_prev_close"),
    ])
    if "base_price" in clean.columns:
        base_valid = (
            pl.col("base_price").is_not_null() & (pl.col("base_price") > 0)
        ).fill_null(False)
        clean = clean.with_columns(base_valid.alias("_base_valid"))
        clean = clean.with_columns([
            pl.when(pl.col("_base_valid"))
            .then(pl.col("base_price").cast(pl.Float64))
            .when(pl.col("_implied_valid"))
            .then(pl.col("_implied_base"))
            .otherwise(None)
            .alias("_event_base"),
            (pl.col("_base_valid").not_() & pl.col("_implied_valid")).alias("_is_implied"),
        ])
    else:
        clean = clean.with_columns([
            pl.when(pl.col("_implied_valid"))
            .then(pl.col("_implied_base"))
            .otherwise(None)
            .alias("_event_base"),
            pl.col("_implied_valid").alias("_is_implied"),
        ])
    clean = clean.with_columns([
        pl.when(
            pl.col("_event_base").is_not_null()
            & pl.col("_prev_close").is_not_null()
            & (pl.col("_prev_close") > 0)
        )
        .then(pl.col("_event_base") / pl.col("_prev_close"))
        .otherwise(1.0)
        .alias("_event_ratio_raw"),
    ])
    clean = clean.with_columns([
        pl.when(pl.col("_is_implied") & ((pl.col("_event_ratio_raw") - 1.0).abs() <= IMPLIED_BASE_EVENT_TOLERANCE))
        .then(1.0)
        .otherwise(pl.col("_event_ratio_raw"))
        .alias("_event_ratio"),
    ])
    clean = clean.with_columns(pl.col("_event_ratio").cum_prod().over("symbol").alias("_cum_event"))
    clean = clean.with_columns((pl.col("close") / pl.col("_cum_event")).alias("_norm_close"))
    clean = clean.with_columns(
        pl.col("_norm_close").rolling_max(window_size=NEWHIGH_LOOKBACK - 1).over("symbol").alias("_past_norm_max")
    )
    clean = clean.with_columns(pl.col("_past_norm_max").shift(1).over("symbol").alias("_past_norm_max"))
    clean = clean.with_columns(
        pl.when(pl.col("_past_norm_max").is_not_null())
        .then(pl.max_horizontal([pl.col("_cum_event") * pl.col("_past_norm_max"), pl.col("close")]))
        .otherwise(None)
        .alias("close_max_60"),
    )
    drop_cols = [c for c in ["_implied_base", "_implied_valid", "_prev_close", "_base_valid", "_event_base", "_is_implied", "_event_ratio_raw", "_event_ratio", "_cum_event", "_norm_close", "_past_norm_max"] if c in clean.columns]
    return clean.drop(drop_cols)


def ineligible_security_symbols(bars: pl.DataFrame) -> frozenset[str]:
    """Symbols whose latest known classification excludes them from collection universes.

    Applies the same security-class rule as ``select_universe``: a symbol is
    ineligible when its most recent non-null ``stock_cert_kind`` differs from
    ``ELIGIBLE_STOCK_CERT_KIND`` or its most recent non-null ``section`` is in
    ``EXCLUDED_SECTIONS``. Unknown classification (no non-null value, e.g. a
    listing newer than the bar store) stays eligible, matching the regular
    universe. Callers must pass bars observable at decision time only.

    Args:
        bars: Daily bars with ``date`` and ``symbol``; ``stock_cert_kind`` and
            ``section`` are optional.

    Returns:
        Frozen set of ineligible symbols; empty when neither classification
        column exists.
    """
    out: set[str] = set()
    if "stock_cert_kind" in bars.columns:
        latest = bars.drop_nulls("stock_cert_kind").sort("date").group_by("symbol").last()
        out.update(latest.filter(pl.col("stock_cert_kind") != ELIGIBLE_STOCK_CERT_KIND)["symbol"].to_list())
    if "section" in bars.columns:
        latest = bars.drop_nulls("section").sort("date").group_by("symbol").last()
        out.update(latest.filter(pl.col("section").is_in(EXCLUDED_SECTIONS))["symbol"].to_list())
    return frozenset(out)


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
    eligible = cand.height
    if "stock_cert_kind" in cand.columns:
        cand = cand.filter(
            pl.col("stock_cert_kind").is_null() | (pl.col("stock_cert_kind") == ELIGIBLE_STOCK_CERT_KIND)
        )
    if "section" in cand.columns:
        cand = cand.filter(pl.col("section").is_null() | (~pl.col("section").is_in(EXCLUDED_SECTIONS)))
    excluded = eligible - cand.height
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
    logger.info(
        "[ALGO] decision=%s selected=%d excluded=%d budget=%d",
        decision_date.isoformat(),
        out.height,
        excluded,
        slot_budget,
    )
    return out
