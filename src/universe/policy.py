"""수집 유니버스 선정 정책."""

from __future__ import annotations

import datetime as dt
import logging
import math
from dataclasses import dataclass
from typing import cast

import polars as pl

from src.core.config import UniverseTierSettings
from src.core.errors import SlotBudgetExceededError
from src.marketdata.schema import REQUIRED_BAR_COLUMNS

logger = logging.getLogger(__name__)

FEATURE_COLUMNS: tuple[str, ...] = ("tv_median_20", "close_max_60", "tv_ratio")
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
DROPPED_LOG_SAMPLE: int = 10
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


def _event_qualifying(cand: pl.DataFrame) -> pl.DataFrame:
    filtered = cand
    if "stock_cert_kind" in filtered.columns:
        filtered = filtered.filter(
            pl.col("stock_cert_kind").is_null() | (pl.col("stock_cert_kind") == ELIGIBLE_STOCK_CERT_KIND)
        )
    if "section" in filtered.columns:
        filtered = filtered.filter(pl.col("section").is_null() | (~pl.col("section").is_in(EXCLUDED_SECTIONS)))
    cond_limit_up = (pl.col("daily_change_pct") >= LIMIT_UP_PCT).fill_null(False)
    cond_surge10 = (pl.col("daily_change_pct") >= SURGE_PCT).fill_null(False)
    cond_volsurge = ((pl.col("tv_ratio") >= VOLSURGE_RATIO) & (pl.col("daily_change_pct") >= VOLSURGE_MIN_CHANGE_PCT)).fill_null(
        False
    )
    cond_newhigh60 = ((pl.col("close") >= pl.col("close_max_60")) & (pl.col("daily_change_pct") >= NEWHIGH_MIN_CHANGE_PCT)).fill_null(
        False
    )
    liquid = pl.col("trade_value_100m") >= LIQUIDITY_FLOOR_100M
    filtered = filtered.with_columns(
        pl.concat_list([
            pl.when(cond_limit_up).then(pl.lit("limit_up")).otherwise(None),
            pl.when(cond_surge10).then(pl.lit("surge10")).otherwise(None),
            pl.when(cond_volsurge).then(pl.lit("volsurge")).otherwise(None),
            pl.when(cond_newhigh60).then(pl.lit("newhigh60")).otherwise(None),
        ])
        .list.drop_nulls()
        .alias("selection_reasons")
    )
    return filtered.filter((cond_limit_up | cond_surge10 | cond_volsurge | cond_newhigh60) & liquid)


@dataclass(frozen=True)
class UniverseSelection:
    selected: pl.DataFrame  # output schema, sorted by symbol
    dropped: pl.DataFrame  # same schema, ordered by priority (trade_value_100m desc, symbol asc)


def select_universe_detailed(featured: pl.DataFrame, decision_date: dt.date, *, slot_budget: int) -> UniverseSelection:
    """Select the decision-date universe and enforce the slot budget by truncation.

    Qualifying symbols above ``slot_budget`` are ranked by ``trade_value_100m`` descending, ties by ``symbol`` ascending
    (a total order, so the result is independent of input row order); the top ``slot_budget`` are kept. Truncation
    instead of failure is deliberate: selection count is data-dependent, and aborting the session degrades collection to
    stale candidates, a far larger loss than dropping the lowest-liquidity tail. Exclusion rules and the liquidity floor
    apply before ranking, so excluded symbols never consume budget.

    Returns: ``selected`` (sorted by symbol) and ``dropped`` (priority order). ``selected + dropped`` equals the qualifying set.
    Raises: ValueError if ``slot_budget < 1``, required columns are missing, or a row is dated after ``decision_date``.
    Side effects: one ``[ALGO]`` WARNING when ``dropped`` is non-empty.
    """
    if slot_budget < 1:
        raise ValueError(f"slot_budget must be >= 1, got {slot_budget}")
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
    qualifying = _event_qualifying(featured.filter(pl.col("date") == decision_date))
    output_columns = [
        "decision_date",
        "symbol",
        "selection_reasons",
        "daily_change_pct",
        "trade_value_100m",
        "tv_ratio",
    ]
    base = qualifying.with_columns(pl.lit(decision_date).alias("decision_date")).select(output_columns)
    if base.height <= slot_budget:
        selected = base.sort("symbol")
        dropped = base.clear()
        logger.info(
            "[ALGO] decision=%s selected=%d excluded=%d budget=%d",
            decision_date.isoformat(),
            selected.height,
            excluded,
            slot_budget,
        )
        return UniverseSelection(selected=selected, dropped=dropped)
    ranked = base.with_columns(
        pl.when(pl.col("trade_value_100m").is_not_null() & pl.col("trade_value_100m").is_finite())
        .then(pl.col("trade_value_100m"))
        .otherwise(float("-inf"))
        .alias("_rank_tv")
    ).sort(["_rank_tv", "symbol"], descending=[True, False]).drop("_rank_tv")
    kept_ranked = ranked.head(slot_budget)
    dropped_ranked = ranked.slice(slot_budget)
    selected = kept_ranked.sort("symbol")
    tv_values = ranked["trade_value_100m"].to_list()
    total_tv = sum(v for v in tv_values if isinstance(v, (int, float)) and math.isfinite(v))
    dropped_values = dropped_ranked["trade_value_100m"].to_list()
    dropped_tv = sum(v for v in dropped_values if isinstance(v, (int, float)) and math.isfinite(v))
    share = (dropped_tv / total_tv) if total_tv > 0 else 0.0
    head = dropped_ranked["symbol"].to_list()[:DROPPED_LOG_SAMPLE]
    logger.info(
        "[ALGO] decision=%s selected=%d excluded=%d budget=%d",
        decision_date.isoformat(),
        selected.height,
        excluded,
        slot_budget,
    )
    logger.warning(
        "[ALGO] decision=%s status=TRUNCATED kept=%d dropped=%d budget=%d dropped_tv_share=%.6f dropped_head=%s",
        decision_date.isoformat(),
        kept_ranked.height,
        dropped_ranked.height,
        slot_budget,
        share,
        ",".join(str(s) for s in head),
    )
    return UniverseSelection(selected=selected, dropped=dropped_ranked)


def select_universe(featured: pl.DataFrame, decision_date: dt.date, *, slot_budget: int) -> pl.DataFrame:
    """Return only ``UniverseSelection.selected``; use ``select_universe_detailed`` when dropped symbols matter. Emits no extra log."""
    return select_universe_detailed(featured, decision_date, slot_budget=slot_budget).selected


def _check_lookahead(featured: pl.DataFrame, decision_date: dt.date) -> None:
    if featured.height > 0:
        max_date = featured["date"].max()
        if isinstance(max_date, dt.date) and max_date > decision_date:
            raise ValueError(f"lookahead: max date {max_date!r} exceeds decision_date {decision_date!r}")


def _eligible_core_rows(day: pl.DataFrame) -> pl.DataFrame:
    out = day
    if "stock_cert_kind" in out.columns:
        out = out.filter(
            pl.col("stock_cert_kind").is_null() | (pl.col("stock_cert_kind") == ELIGIBLE_STOCK_CERT_KIND)
        )
    if "section" in out.columns:
        out = out.filter(pl.col("section").is_null() | (~pl.col("section").is_in(EXCLUDED_SECTIONS)))
    return out.filter(pl.col("tv_median_20").is_not_null())


def select_core_panel(
    featured: pl.DataFrame, decision_date: dt.date, *, enter_rank: int, exit_rank: int, warmup_sessions: int
) -> pl.DataFrame:
    """Return the hysteresis core panel on decision_date with columns symbol, core_rank."""
    if enter_rank < 1 or exit_rank < 1:
        raise ValueError(f"core ranks must be >= 1, got enter={enter_rank} exit={exit_rank}")
    if exit_rank < enter_rank:
        raise ValueError(f"core_exit_rank {exit_rank} must be >= core_enter_rank {enter_rank}")
    if warmup_sessions < 1:
        raise ValueError(f"warmup_sessions must be >= 1, got {warmup_sessions}")
    if "tv_median_20" not in featured.columns or "symbol" not in featured.columns or "date" not in featured.columns:
        raise ValueError("missing required columns: tv_median_20, symbol, date")
    _check_lookahead(featured, decision_date)
    dates = sorted({d for d in featured["date"].to_list() if isinstance(d, dt.date) and d <= decision_date})
    if len(dates) < warmup_sessions:
        logger.warning("[ALGO] stage=core_panel status=SHORT_WARMUP dates=%d", len(dates))
    window = dates[-warmup_sessions:] if warmup_sessions <= len(dates) else dates
    empty = pl.DataFrame(
        {"symbol": pl.Series([], dtype=pl.String), "core_rank": pl.Series([], dtype=pl.Int32)}
    )
    if not window:
        return empty
    members: set[str] = set()
    rank_on_decision: dict[str, int] = {}
    for idx, day in enumerate(window):
        day_rows = _eligible_core_rows(featured.filter(pl.col("date") == day))
        if day_rows.height == 0:
            members = set()
            if day == decision_date:
                rank_on_decision = {}
            continue
        ordered = day_rows.sort(["tv_median_20", "symbol"], descending=[True, False])
        ranks = {s: i + 1 for i, s in enumerate(ordered["symbol"].to_list())}
        if idx == 0:
            members = {s for s, r in ranks.items() if r <= enter_rank}
        else:
            entering = {s for s, r in ranks.items() if r <= enter_rank}
            staying = {s for s in members if s in ranks and ranks[s] <= exit_rank}
            members = entering | staying
        if day == decision_date:
            rank_on_decision = ranks
    rows = [(s, rank_on_decision[s]) for s in members if s in rank_on_decision]
    rows.sort(key=lambda t: t[1])
    if not rows:
        return empty
    return pl.DataFrame(
        {"symbol": [s for s, _ in rows], "core_rank": pl.Series([r for _, r in rows], dtype=pl.Int32)}
    )


def select_follow_up(featured: pl.DataFrame, decision_date: dt.date, *, follow_sessions: int) -> pl.DataFrame:
    """Return symbols that qualified for the event layer on any of the previous follow_sessions dates and still trade on decision_date; columns symbol, follow_age."""
    if follow_sessions < 0:
        raise ValueError(f"follow_sessions must be >= 0, got {follow_sessions}")
    empty = pl.DataFrame(
        {"symbol": pl.Series([], dtype=pl.String), "follow_age": pl.Series([], dtype=pl.Int32)}
    )
    if follow_sessions == 0:
        return empty
    _check_lookahead(featured, decision_date)
    prior = sorted({d for d in featured["date"].to_list() if isinstance(d, dt.date) and d < decision_date})
    window = prior[-follow_sessions:] if follow_sessions <= len(prior) else prior
    if not window:
        return empty
    age_by_symbol: dict[str, int] = {}
    for idx, day in enumerate(window):
        order = len(window) - idx
        qualifying = _event_qualifying(featured.filter(pl.col("date") == day))
        for symbol in qualifying["symbol"].to_list():
            if symbol not in age_by_symbol or order < age_by_symbol[symbol]:
                age_by_symbol[symbol] = order
    if not age_by_symbol:
        return empty
    today = featured.filter(pl.col("date") == decision_date)
    if "stock_cert_kind" in today.columns:
        today = today.filter(
            pl.col("stock_cert_kind").is_null() | (pl.col("stock_cert_kind") == ELIGIBLE_STOCK_CERT_KIND)
        )
    if "section" in today.columns:
        today = today.filter(pl.col("section").is_null() | (~pl.col("section").is_in(EXCLUDED_SECTIONS)))
    today = today.filter(pl.col("trade_value_100m") >= LIQUIDITY_FLOOR_100M)
    tradable = set(today["symbol"].to_list())
    rows = [(s, a) for s, a in age_by_symbol.items() if s in tradable]
    rows.sort(key=lambda t: (t[1], t[0]))
    if not rows:
        return empty
    return pl.DataFrame(
        {"symbol": [s for s, _ in rows], "follow_age": pl.Series([a for _, a in rows], dtype=pl.Int32)}
    )


def _rank_key_value(value: object) -> float:
    if isinstance(value, bool):
        return float("-inf")
    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        return float(value)
    return float("-inf")


def select_universe_tiered(
    featured: pl.DataFrame, decision_date: dt.date, *, slot_budget: int, tiers: UniverseTierSettings
) -> UniverseSelection:
    """Merge core, event and follow-up layers into one priority-ordered selection within slot_budget."""
    core_enabled = bool(tiers.core_enabled)
    if not core_enabled:
        return select_universe_detailed(featured, decision_date, slot_budget=slot_budget)
    if slot_budget < 1:
        raise ValueError(f"slot_budget must be >= 1, got {slot_budget}")
    missing = [c for c in list(REQUIRED_BAR_COLUMNS) + list(FEATURE_COLUMNS) if c not in featured.columns]
    if missing:
        raise ValueError(f"missing required columns: {missing}")
    _check_lookahead(featured, decision_date)
    enter_rank = int(tiers.core_enter_rank)
    exit_rank = int(tiers.core_exit_rank)
    warmup_sessions = int(tiers.core_warmup_sessions)
    follow_sessions = int(tiers.follow_sessions)
    if exit_rank < enter_rank:
        raise ValueError(f"core_exit_rank {exit_rank} must be >= core_enter_rank {enter_rank}")
    if exit_rank > slot_budget:
        raise SlotBudgetExceededError(
            f"core_exit_rank {exit_rank} exceeds universe_slot_budget {slot_budget}"
        )
    core = select_core_panel(
        featured,
        decision_date,
        enter_rank=enter_rank,
        exit_rank=exit_rank,
        warmup_sessions=warmup_sessions,
    )
    follow = select_follow_up(featured, decision_date, follow_sessions=follow_sessions)
    cand_today = featured.filter(pl.col("date") == decision_date)
    eligible = cand_today.height
    if "stock_cert_kind" in cand_today.columns:
        cand_today_counted = cand_today.filter(
            pl.col("stock_cert_kind").is_null() | (pl.col("stock_cert_kind") == ELIGIBLE_STOCK_CERT_KIND)
        )
    else:
        cand_today_counted = cand_today
    if "section" in cand_today_counted.columns:
        cand_today_counted = cand_today_counted.filter(
            pl.col("section").is_null() | (~pl.col("section").is_in(EXCLUDED_SECTIONS))
        )
    excluded = eligible - cand_today_counted.height
    event_q = _event_qualifying(featured.filter(pl.col("date") == decision_date))
    event_by_symbol: dict[str, dict[str, object]] = {row["symbol"]: row for row in event_q.to_dicts()}
    dec_rows: dict[str, dict[str, object]] = {}
    for row in featured.filter(pl.col("date") == decision_date).to_dicts():
        dec_rows[str(row["symbol"])] = row
    core_by_symbol: dict[str, int] = dict(zip(core["symbol"].to_list(), core["core_rank"].to_list(), strict=True)) if core.height else {}
    follow_by_symbol: dict[str, int] = dict(zip(follow["symbol"].to_list(), follow["follow_age"].to_list(), strict=True)) if follow.height else {}
    output_columns = [
        "decision_date",
        "symbol",
        "selection_reasons",
        "daily_change_pct",
        "trade_value_100m",
        "tv_ratio",
        "tier",
        "core_rank",
    ]
    symbols = set(core_by_symbol) | set(event_by_symbol) | set(follow_by_symbol)
    core_rows: list[dict[str, object]] = []
    event_rows: list[dict[str, object]] = []
    follow_rows: list[dict[str, object]] = []

    def _event_reasons(symbol: str) -> list[object]:
        return list(cast("list[object]", event_by_symbol[symbol]["selection_reasons"]))

    for symbol in symbols:
        in_core = symbol in core_by_symbol
        in_event = symbol in event_by_symbol
        in_follow = symbol in follow_by_symbol
        if in_core:
            tier = "core"
            if in_event:
                reasons: list[object] = [*_event_reasons(symbol), "core"]
                if in_follow:
                    reasons.append(f"follow:{follow_by_symbol[symbol]}")
            else:
                reasons = ["core"]
        elif in_event:
            tier = "event"
            if in_follow:
                reasons = [*_event_reasons(symbol), f"follow:{follow_by_symbol[symbol]}"]
            else:
                reasons = _event_reasons(symbol)
        else:
            tier = "follow"
            reasons = [f"follow:{follow_by_symbol[symbol]}"]
        if in_event:
            src = event_by_symbol[symbol]
            daily = src.get("daily_change_pct")
            tv = src.get("trade_value_100m")
            ratio = src.get("tv_ratio")
        else:
            src = dec_rows.get(symbol, {})
            daily = src.get("daily_change_pct")
            tv = src.get("trade_value_100m")
            ratio = src.get("tv_ratio")
        record: dict[str, object] = {
            "decision_date": decision_date,
            "symbol": symbol,
            "selection_reasons": reasons,
            "daily_change_pct": daily,
            "trade_value_100m": tv,
            "tv_ratio": ratio,
            "tier": tier,
            "core_rank": core_by_symbol.get(symbol),
        }
        if tier == "core":
            core_rows.append(record)
        elif tier == "event":
            event_rows.append(record)
        else:
            follow_rows.append(record)
    core_rows.sort(key=lambda r: core_by_symbol[str(r["symbol"])])
    event_rows = sorted(event_rows, key=lambda r: (-_rank_key_value(r["trade_value_100m"]), str(r["symbol"])))
    follow_rows = sorted(
        follow_rows,
        key=lambda r: (
            follow_by_symbol[str(r["symbol"])],
            -_rank_key_value(r["trade_value_100m"]),
            str(r["symbol"]),
        ),
    )
    ordered = core_rows + event_rows + follow_rows
    schema: dict[str, pl.DataType] = {
        "decision_date": pl.Date(),
        "symbol": featured.schema["symbol"],
        "selection_reasons": pl.List(pl.String),
        "daily_change_pct": featured.schema["daily_change_pct"],
        "trade_value_100m": featured.schema["trade_value_100m"],
        "tv_ratio": featured.schema["tv_ratio"],
        "tier": pl.String(),
        "core_rank": pl.Int32(),
    }
    if not ordered:
        empty = pl.DataFrame(schema=schema)
        logger.info(
            "[ALGO] decision=%s selected=%d excluded=%d budget=%d",
            decision_date.isoformat(),
            0,
            excluded,
            slot_budget,
        )
        return UniverseSelection(selected=empty, dropped=empty.clear())
    frame = pl.DataFrame(ordered, schema=schema).select(output_columns)
    if frame.height <= slot_budget:
        logger.info(
            "[ALGO] decision=%s selected=%d excluded=%d budget=%d",
            decision_date.isoformat(),
            frame.height,
            excluded,
            slot_budget,
        )
        return UniverseSelection(selected=frame, dropped=frame.clear())
    kept = frame.head(slot_budget)
    dropped = frame.slice(slot_budget)
    tv_values = frame["trade_value_100m"].to_list()
    total_tv = sum(v for v in tv_values if isinstance(v, (int, float)) and math.isfinite(v))
    dropped_values = dropped["trade_value_100m"].to_list()
    dropped_tv = sum(v for v in dropped_values if isinstance(v, (int, float)) and math.isfinite(v))
    share = (dropped_tv / total_tv) if total_tv > 0 else 0.0
    head = dropped["symbol"].to_list()[:DROPPED_LOG_SAMPLE]
    tier_list = dropped["tier"].to_list()
    n_event_drop = sum(1 for t in tier_list if t == "event")
    n_follow_drop = sum(1 for t in tier_list if t == "follow")
    logger.info(
        "[ALGO] decision=%s selected=%d excluded=%d budget=%d",
        decision_date.isoformat(),
        kept.height,
        excluded,
        slot_budget,
    )
    logger.warning(
        "[ALGO] decision=%s status=TRUNCATED kept=%d dropped=%d budget=%d dropped_tv_share=%.6f dropped_head=%s tier_dropped=event:%d,follow:%d",
        decision_date.isoformat(),
        kept.height,
        dropped.height,
        slot_budget,
        share,
        ",".join(str(s) for s in head),
        n_event_drop,
        n_follow_drop,
    )
    return UniverseSelection(selected=kept, dropped=dropped)
