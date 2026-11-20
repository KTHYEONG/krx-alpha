"""Research loader visibility: selection knowability, data origin, and cross-venue ordering flags."""

from __future__ import annotations

import pathlib
from collections.abc import Mapping, Sequence
from enum import StrEnum
from typing import Final, TypeVar

import polars as pl

from src.core.errors import KrxAlphaError
from src.universe.membership import MembershipRow

__all__ = [
    "VISIBLE_ALL_SESSION",
    "DataOrigin",
    "flag_cross_venue_same_second",
    "mask_before_visibility",
    "origin_for_l1_relpath",
    "repaired_origin_expr",
    "research_visibility",
]

FrameT = TypeVar("FrameT", pl.DataFrame, pl.LazyFrame)


class DataOrigin(StrEnum):
    WS_REALTIME = "ws_realtime"
    REST_BACKFILL = "rest_backfill"
    REST_SNAPSHOT = "rest_snapshot"
    REST_MINUTE = "rest_minute"


VISIBLE_ALL_SESSION: Final[int] = 0

_CROSS_VENUE_COLUMN: Final[str] = "cross_venue_order_unknown"
_VISIBILITY_SYMBOL: Final[str] = "__visibility_symbol"
_VISIBILITY_NS: Final[str] = "__visibility_ns"
_ROW_ORDER: Final[str] = "__row_order"

_L1_VENDORS: Final[frozenset[str]] = frozenset({"kis", "ls", "db", "toss", "kiwoom"})
_L1_REPAIRED_PREFIX: Final[tuple[str, ...]] = ("derived", "ticks_repaired")
_L1_SNAPSHOT_PREFIX: Final[str] = "snapshot"
_L1_TICK_TAPE_PREFIX: Final[tuple[str, ...]] = ("history", "tick_tape")
_L1_MINUTE_PREFIX: Final[tuple[str, ...]] = ("history", "minute")
_L1_MIN_VENDOR_DEPTH: Final[int] = 5
_L1_MIN_SNAPSHOT_DEPTH: Final[int] = 2
_L1_MIN_HISTORY_DEPTH: Final[int] = 3

_REPAIRED_LIVE_SOURCE: Final[str] = "live"


class _SourcePolicy(StrEnum):
    ALL_SESSION = "all_session"
    AT_DECISION = "at_decision"
    NONE = "none"


_SOURCE_POLICY: Final[Mapping[str, _SourcePolicy]] = {
    "static": _SourcePolicy.ALL_SESSION,
    "dynamic": _SourcePolicy.AT_DECISION,
    "intraday_capacity": _SourcePolicy.AT_DECISION,
    "ack": _SourcePolicy.NONE,
    "post_close_screen": _SourcePolicy.NONE,
}


def research_visibility(rows: Sequence[MembershipRow]) -> Mapping[str, int | None]:
    """Map each symbol to the earliest epoch-ns its selection was knowable; None = never visible."""
    visibility: dict[str, int | None] = {}
    for row in rows:
        policy = _SOURCE_POLICY.get(row.source)
        if policy is None:
            raise KrxAlphaError(f"membership source without visibility policy: {row.source!r}")
        if policy is _SourcePolicy.NONE:
            visibility.setdefault(row.symbol, None)
            continue
        at = VISIBLE_ALL_SESSION if policy is _SourcePolicy.ALL_SESSION else row.decided_at_ns
        previous = visibility.get(row.symbol)
        visibility[row.symbol] = at if previous is None else min(previous, at)
    return visibility


def mask_before_visibility(
    frame: FrameT,
    visibility: Mapping[str, int | None],
    *,
    symbol_col: str,
    time_ns_col: str,
) -> FrameT:
    """Keep rows of visible symbols at or after their visibility instant, preserving row order."""
    lazy = frame.lazy()
    names = lazy.collect_schema().names()
    if symbol_col not in names or time_ns_col not in names:
        raise ValueError(f"frame lacks required columns: {symbol_col!r}, {time_ns_col!r}")

    visible = [(symbol, at) for symbol, at in visibility.items() if at is not None]
    visibility_frame = pl.DataFrame(
        {
            _VISIBILITY_SYMBOL: [symbol for symbol, _ in visible],
            _VISIBILITY_NS: [at for _, at in visible],
        },
        schema={_VISIBILITY_SYMBOL: pl.String, _VISIBILITY_NS: pl.Int64},
    ).lazy()

    keep = (
        pl.when(pl.col(time_ns_col).is_null())
        .then(pl.col(_VISIBILITY_NS) == VISIBLE_ALL_SESSION)
        .otherwise(pl.col(time_ns_col) >= pl.col(_VISIBILITY_NS))
    )
    masked = (
        lazy.with_row_index(_ROW_ORDER)
        .join(visibility_frame, left_on=symbol_col, right_on=_VISIBILITY_SYMBOL, how="inner")
        .filter(keep)
        .sort(_ROW_ORDER)
        .drop(_ROW_ORDER, _VISIBILITY_NS)
    )
    if isinstance(frame, pl.DataFrame):
        return masked.collect()
    return masked


def origin_for_l1_relpath(relpath: pathlib.PurePosixPath) -> DataOrigin:
    """Classify an L1 path relative to the `l1/` root; repaired ticks must use `repaired_origin_expr`."""
    parts = relpath.parts
    if parts[: len(_L1_REPAIRED_PREFIX)] == _L1_REPAIRED_PREFIX:
        raise ValueError(f"mixed-origin path requires per-row origin: {relpath}")
    if parts[:1] == (_L1_SNAPSHOT_PREFIX,) and len(parts) >= _L1_MIN_SNAPSHOT_DEPTH:
        return DataOrigin.REST_SNAPSHOT
    if parts[: len(_L1_TICK_TAPE_PREFIX)] == _L1_TICK_TAPE_PREFIX and len(parts) >= _L1_MIN_HISTORY_DEPTH:
        return DataOrigin.REST_BACKFILL
    if parts[: len(_L1_MINUTE_PREFIX)] == _L1_MINUTE_PREFIX and len(parts) >= _L1_MIN_HISTORY_DEPTH:
        return DataOrigin.REST_MINUTE
    if parts[:1] and parts[0] in _L1_VENDORS and len(parts) >= _L1_MIN_VENDOR_DEPTH:
        return DataOrigin.WS_REALTIME
    raise ValueError(f"unclassified L1 path: {relpath}")


def repaired_origin_expr(source_col: str = "source") -> pl.Expr:
    """Per-row origin for repaired ticks: live rows are realtime, every other source is backfill."""
    return (
        pl.when(pl.col(source_col).is_null())
        .then(pl.lit(None, dtype=pl.String))
        .when(pl.col(source_col) == _REPAIRED_LIVE_SOURCE)
        .then(pl.lit(DataOrigin.WS_REALTIME.value))
        .otherwise(pl.lit(DataOrigin.REST_BACKFILL.value))
    )


def flag_cross_venue_same_second(
    frame: FrameT,
    *,
    symbol_col: str,
    venue_col: str,
    second_col: str,
) -> FrameT:
    """Append `cross_venue_order_unknown`: true when one symbol-second spans 2+ venues; null second gives null."""
    if _CROSS_VENUE_COLUMN in frame.lazy().collect_schema().names():
        raise ValueError(f"column already present: {_CROSS_VENUE_COLUMN!r}")
    multi_venue = pl.col(venue_col).n_unique().over([symbol_col, second_col]) >= 2
    flag = (
        pl.when(pl.col(second_col).is_null())
        .then(pl.lit(None, dtype=pl.Boolean))
        .otherwise(multi_venue)
    )
    return frame.with_columns(flag.alias(_CROSS_VENUE_COLUMN))
