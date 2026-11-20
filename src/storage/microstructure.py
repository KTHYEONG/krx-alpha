"""Microstructure fields derived from raw L0 payloads at read time."""

from __future__ import annotations

from typing import Final, TypeVar

import polars as pl
from polars.expr.whenthen import ChainedThen, Then

FrameT = TypeVar("FrameT", pl.DataFrame, pl.LazyFrame)

KIS_TRADE_TOKENS: Final[int] = 47
KIS_QUOTE_TOKENS: Final[int] = 63

# Field indices match the 2026-11-18 KIS L0 measurement, not the official spec, which lists fewer fields.
_KIS_TRADE_CCLD_DVSN: Final[int] = 21
_KIS_TRADE_NEW_MKOP_CLS_CODE: Final[int] = 34
_KIS_TRADE_TRHT_YN: Final[int] = 35
_KIS_TRADE_VI_STND_PRC: Final[int] = 45
_KIS_QUOTE_HOUR_CLS_CODE: Final[int] = 2
_KIS_QUOTE_ANTC_CNPR: Final[int] = 47
_KIS_QUOTE_ANTC_CNQN: Final[int] = 48

_KIS_BUY_CCLD: Final[str] = "1"
_KIS_SELL_CCLD: Final[str] = "5"
_LS_BUY_CGUBUN: Final[str] = "+"
_LS_SELL_CGUBUN: Final[str] = "-"
_LS_SINGLE_PRICE_DONSIGUBUN: Final[str] = "3"
_KIS_VI_HOUR_CLS: Final[str] = "C"
_KIS_SINGLE_PRICE_HOUR_CLS: Final[tuple[str, ...]] = ("A", "B", "C", "D")
_KIS_HALTED_TRHT_YN: Final[str] = "Y"

MICROSTRUCTURE_COLUMNS: tuple[str, ...] = (
    "initiator",
    "trading_halted",
    "mkop_code",
    "vi_base_price",
    "hour_cls_code",
    "single_price",
    "vi_active",
    "expected_price",
    "expected_qty",
)

__all__ = ["MICROSTRUCTURE_COLUMNS", "annotate_microstructure"]


def _nonempty(value: pl.Expr) -> pl.Expr:
    return pl.when(value != "").then(value)


def _positive_int(value: pl.Expr) -> pl.Expr:
    number = value.cast(pl.Int64, strict=False)
    return pl.when(number > 0).then(number)


def _kis_token(parts: pl.Expr, index: int) -> pl.Expr:
    return parts.list.get(index, null_on_oob=True)


def _ls_body(field: str) -> pl.Expr:
    return pl.col("raw").str.json_path_match(f"$.body.{field}")


def _typed(branch: Then | ChainedThen, dtype: pl.DataType) -> pl.Expr:
    return branch.otherwise(pl.lit(None, dtype=dtype)).cast(dtype)


def annotate_microstructure(frame: FrameT) -> FrameT:
    """Append ``MICROSTRUCTURE_COLUMNS`` derived from ``raw``, preserving every row and column.

    Args:
        frame: ``pl.DataFrame`` or ``pl.LazyFrame`` with ``vendor``, ``stream`` and ``raw``.

    Returns:
        Same frame type with the nine microstructure columns appended. Values are null for
        unsupported vendor/stream pairs and for KIS rows whose token count is not the measured one.

    Raises:
        ValueError: A required input column is missing, or an output column already exists.
    """
    names = frame.collect_schema().names()
    missing = [name for name in ("vendor", "stream", "raw") if name not in names]
    if missing:
        raise ValueError(f"frame is missing required column(s): {', '.join(missing)}")
    clashing = [name for name in MICROSTRUCTURE_COLUMNS if name in names]
    if clashing:
        raise ValueError(f"frame already has microstructure column(s): {', '.join(clashing)}")

    parts = pl.col("raw").str.split("^")
    vendor = pl.col("vendor")
    stream = pl.col("stream")
    kis_trade = (vendor == "kis") & (stream == "H0STCNT0") & (parts.list.len() == KIS_TRADE_TOKENS)
    kis_quote = (vendor == "kis") & (stream == "H0STASP0") & (parts.list.len() == KIS_QUOTE_TOKENS)
    ls_trade = (vendor == "ls") & (stream == "H0STCNT0")
    ls_quote = (vendor == "ls") & (stream == "H0STASP0")

    ccld = _kis_token(parts, _KIS_TRADE_CCLD_DVSN)
    hour_cls = _kis_token(parts, _KIS_QUOTE_HOUR_CLS_CODE)
    cgubun = _ls_body("cgubun")
    donsigubun = _ls_body("donsigubun")

    initiator = (
        pl.when(kis_trade)
        .then(
            pl.when(ccld == _KIS_BUY_CCLD)
            .then(pl.lit("buy"))
            .when(ccld == _KIS_SELL_CCLD)
            .then(pl.lit("sell"))
        )
        .when(ls_trade)
        .then(
            pl.when(cgubun == _LS_BUY_CGUBUN)
            .then(pl.lit("buy"))
            .when(cgubun == _LS_SELL_CGUBUN)
            .then(pl.lit("sell"))
        )
    )
    single_price = (
        pl.when(kis_quote)
        .then(hour_cls.is_in(_KIS_SINGLE_PRICE_HOUR_CLS))
        .when(ls_quote)
        .then(donsigubun == _LS_SINGLE_PRICE_DONSIGUBUN)
    )

    columns: list[tuple[str, Then | ChainedThen, pl.DataType]] = [
        ("initiator", initiator, pl.String()),
        ("trading_halted", pl.when(kis_trade).then(_kis_token(parts, _KIS_TRADE_TRHT_YN) == _KIS_HALTED_TRHT_YN), pl.Boolean()),
        ("mkop_code", pl.when(kis_trade).then(_nonempty(_kis_token(parts, _KIS_TRADE_NEW_MKOP_CLS_CODE))), pl.String()),
        ("vi_base_price", pl.when(kis_trade).then(_positive_int(_kis_token(parts, _KIS_TRADE_VI_STND_PRC))), pl.Int64()),
        ("hour_cls_code", pl.when(kis_quote).then(_nonempty(hour_cls)), pl.String()),
        ("single_price", single_price, pl.Boolean()),
        ("vi_active", pl.when(kis_quote).then(hour_cls == _KIS_VI_HOUR_CLS), pl.Boolean()),
        ("expected_price", pl.when(kis_quote).then(_positive_int(_kis_token(parts, _KIS_QUOTE_ANTC_CNPR))), pl.Int64()),
        ("expected_qty", pl.when(kis_quote).then(_positive_int(_kis_token(parts, _KIS_QUOTE_ANTC_CNQN))), pl.Int64()),
    ]
    return frame.with_columns(_typed(branch, dtype).alias(name) for name, branch, dtype in columns)
