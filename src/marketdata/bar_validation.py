"""Structural validation for KRX daily bars (whole-day fail-closed gate)."""

from __future__ import annotations

import polars as pl

from src.core.errors import KrxAlphaError


class KrxBarsError(KrxAlphaError):
    """KRX 요청 실패/빈 응답(비영업일)/거래일 미발견 fail-closed 신호."""


class ImplausibleBarValuesError(KrxBarsError):
    """Daily bars violate a structural price/volume identity (fail-closed, whole day rejected)."""


MAX_CHANGE_PCT_DISAGREEMENT_PP: float = 0.1
# A historical KRX file with more broken rows than this is treated as a corrupt response, not isolated vendor errata.
MAX_HISTORY_QUARANTINE_ROWS_PER_DAY: int = 5


def _row_rules(columns: list[str]) -> list[tuple[str, pl.Expr]]:
    rules: list[tuple[str, pl.Expr]] = []
    if "close" in columns:
        rules.append(("close_positive", pl.col("close").is_not_null() & (pl.col("close") <= 0)))
    if "volume" in columns:
        rules.append(("volume_non_negative", pl.col("volume").is_not_null() & (pl.col("volume") < 0)))
    if "trade_value_100m" in columns:
        rules.append(
            ("trade_value_non_negative", pl.col("trade_value_100m").is_not_null() & (pl.col("trade_value_100m") < 0))
        )
    if "volume" in columns and "trade_value_100m" in columns:
        rules.append(
            (
                "volume_trade_value_consistency",
                pl.col("volume").is_not_null()
                & pl.col("trade_value_100m").is_not_null()
                & ((pl.col("volume") == 0) != (pl.col("trade_value_100m") == 0)),
            )
        )
    price_rules: tuple[tuple[str, pl.Expr], ...] = ()
    if all(c in columns for c in ("open", "high", "low", "close", "volume")):
        price_rules = (
            ("low_positive", pl.col("low").is_not_null() & (pl.col("low") <= 0)),
            (
                "low_high_order",
                pl.col("low").is_not_null() & pl.col("high").is_not_null() & (pl.col("low") > pl.col("high")),
            ),
            (
                "open_in_range",
                pl.col("low").is_not_null()
                & pl.col("open").is_not_null()
                & pl.col("high").is_not_null()
                & ((pl.col("open") < pl.col("low")) | (pl.col("open") > pl.col("high"))),
            ),
            (
                "close_in_range",
                pl.col("low").is_not_null()
                & pl.col("close").is_not_null()
                & pl.col("high").is_not_null()
                & ((pl.col("close") < pl.col("low")) | (pl.col("close") > pl.col("high"))),
            ),
        )
    for rule, condition in price_rules:
        rules.append((rule, pl.col("volume").is_not_null() & (pl.col("volume") > 0) & condition))
    if all(c in columns for c in ("close", "base_price", "daily_change_pct")):
        rules.append(
            (
                "change_pct_agreement",
                pl.col("base_price").is_not_null()
                & (pl.col("base_price") > 0)
                & pl.col("close").is_not_null()
                & pl.col("daily_change_pct").is_not_null()
                & (
                    ((pl.col("close") / pl.col("base_price") - 1) * 100 - pl.col("daily_change_pct")).abs()
                    > MAX_CHANGE_PCT_DISAGREEMENT_PP
                ),
            )
        )
    return rules


def validate_daily_bars(bars: pl.DataFrame) -> None:
    """Reject a complete KRX day when structural price/volume identities fail.

    Args:
        bars: Daily-bar frame conforming to the stored bar columns.

    Raises:
        ImplausibleBarValuesError: Any row violates a structural identity.
    """
    violations: list[str] = []

    def _report(rule: str, frame: pl.DataFrame) -> None:
        if frame.height > 0:
            samples = [f"{row[0]}:{row[1]}" for row in frame.select(["date", "symbol"]).head(5).iter_rows()]
            violations.append(f"{rule}={frame.height} rows {','.join(samples)}")

    _report("duplicate_key", bars.filter(pl.struct(["date", "symbol"]).is_duplicated()))
    for rule, condition in _row_rules(bars.columns):
        _report(rule, bars.filter(condition))
    if violations:
        raise ImplausibleBarValuesError(f"implausible daily bars: {'; '.join(violations)}")


def split_invalid_rows(bars: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Return (valid, invalid) by the row-level identities of validate_daily_bars; invalid gains a `violations` list column."""
    rules = _row_rules(bars.columns)
    if not rules:
        return bars, bars.head(0).with_columns(pl.lit([], dtype=pl.List(pl.String)).alias("violations"))
    flagged = bars.with_columns(
        pl.concat_list([pl.when(cond).then(pl.lit(name)) for name, cond in rules]).list.drop_nulls().alias("violations")
    )
    bad = pl.col("violations").list.len() > 0
    return flagged.filter(~bad).drop("violations"), flagged.filter(bad)
