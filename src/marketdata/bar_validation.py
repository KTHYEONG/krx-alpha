"""Structural validation for KRX daily bars (whole-day fail-closed gate)."""

from __future__ import annotations

import polars as pl

from src.core.errors import KrxAlphaError


class KrxBarsError(KrxAlphaError):
    """KRX 요청 실패/빈 응답(비영업일)/거래일 미발견 fail-closed 신호."""


class ImplausibleBarValuesError(KrxBarsError):
    """Daily bars violate a structural price/volume identity (fail-closed, whole day rejected)."""


MAX_CHANGE_PCT_DISAGREEMENT_PP: float = 0.1


def validate_daily_bars(bars: pl.DataFrame) -> None:
    """Reject a complete KRX day when structural price/volume identities fail.

    Args:
        bars: Daily-bar frame conforming to the stored bar columns.

    Raises:
        ImplausibleBarValuesError: Any row violates a structural identity.
    """
    violations: list[str] = []

    def _samples(frame: pl.DataFrame) -> list[str]:
        return [f"{row[0]}:{row[1]}" for row in frame.select(["date", "symbol"]).head(5).iter_rows()]

    def _report(rule: str, frame: pl.DataFrame) -> None:
        if frame.height > 0:
            violations.append(f"{rule}={frame.height} rows {','.join(_samples(frame))}")

    _report("duplicate_key", bars.filter(pl.struct(["date", "symbol"]).is_duplicated()))
    if "close" in bars.columns:
        _report("close_positive", bars.filter(pl.col("close").is_not_null() & (pl.col("close") <= 0)))
    if "volume" in bars.columns:
        _report("volume_non_negative", bars.filter(pl.col("volume").is_not_null() & (pl.col("volume") < 0)))
    if "trade_value_100m" in bars.columns:
        _report(
            "trade_value_non_negative",
            bars.filter(pl.col("trade_value_100m").is_not_null() & (pl.col("trade_value_100m") < 0)),
        )
    if "volume" in bars.columns and "trade_value_100m" in bars.columns:
        _report(
            "volume_trade_value_consistency",
            bars.filter(
                pl.col("volume").is_not_null()
                & pl.col("trade_value_100m").is_not_null()
                & ((pl.col("volume") == 0) != (pl.col("trade_value_100m") == 0))
            ),
        )
    for rule, condition in (
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
    ):
        _report(rule, bars.filter(pl.col("volume").is_not_null() & (pl.col("volume") > 0) & condition))
    if all(c in bars.columns for c in ("close", "base_price", "daily_change_pct")):
        _report(
            "change_pct_agreement",
            bars.filter(
                pl.col("base_price").is_not_null()
                & (pl.col("base_price") > 0)
                & pl.col("close").is_not_null()
                & pl.col("daily_change_pct").is_not_null()
                & (
                    ((pl.col("close") / pl.col("base_price") - 1) * 100 - pl.col("daily_change_pct")).abs()
                    > MAX_CHANGE_PCT_DISAGREEMENT_PP
                )
            ),
        )
    if violations:
        raise ImplausibleBarValuesError(f"implausible daily bars: {'; '.join(violations)}")
