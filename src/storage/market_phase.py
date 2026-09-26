"""Exchange-time market phase derived during L1 normalization."""

from __future__ import annotations

import datetime as dt
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import TypeVar

import polars as pl

from src.core.session_anchors import SessionAnchors
from src.realtime.contracts import MarketVenue

FrameT = TypeVar("FrameT", pl.DataFrame, pl.LazyFrame)

MARKET_PHASE_METADATA_KEY: str = "krx_alpha.market_phase"


class MarketPhase(StrEnum):
    """Exchange trading phase of one market-data event.

    Membership is decided by the exchange event time (HHMMSS, KST), never by
    the stream, connection or partition the frame arrived on, because one
    connection spans several phases.
    """

    PRE_MARKET_CLOSING_PRICE = "pre_closing_price"
    OPENING_AUCTION = "opening_auction"
    REGULAR = "regular"
    CLOSING_AUCTION = "closing_auction"
    POST_MARKET_CLOSING_PRICE = "post_closing_price"
    AFTERMARKET = "aftermarket"
    UNCLASSIFIED = "unclassified"


class EventKind(StrEnum):
    TRADE = "trade"
    QUOTE = "quote"


@dataclass(frozen=True)
class PhaseWindow:
    venue: MarketVenue
    kind: EventKind | None
    start: dt.time
    end: dt.time
    phase: MarketPhase


STREAM_EVENT_KIND: Mapping[str, EventKind] = {
    "H0STCNT0": EventKind.TRADE,
    "H0NXCNT0": EventKind.TRADE,
    "H0STASP0": EventKind.QUOTE,
    "H0NXASP0": EventKind.QUOTE,
}

MARKET_PHASE_WINDOWS: tuple[PhaseWindow, ...] = (
    PhaseWindow(MarketVenue.KRX, EventKind.TRADE, dt.time(8, 30), dt.time(8, 40), MarketPhase.PRE_MARKET_CLOSING_PRICE),
    PhaseWindow(MarketVenue.KRX, None, dt.time(8, 30), dt.time(9, 0), MarketPhase.OPENING_AUCTION),
    PhaseWindow(MarketVenue.KRX, None, dt.time(9, 0), dt.time(15, 20), MarketPhase.REGULAR),
    PhaseWindow(MarketVenue.KRX, None, dt.time(15, 20), dt.time(15, 40), MarketPhase.CLOSING_AUCTION),
    PhaseWindow(MarketVenue.KRX, None, dt.time(15, 40), dt.time(16, 0), MarketPhase.POST_MARKET_CLOSING_PRICE),
    PhaseWindow(MarketVenue.KRX, None, dt.time(16, 0), dt.time(20, 0), MarketPhase.AFTERMARKET),
    PhaseWindow(MarketVenue.NXT, None, dt.time(15, 40), dt.time(20, 0), MarketPhase.AFTERMARKET),
)

_TRADE_STREAMS: tuple[str, ...] = tuple(s for s, k in STREAM_EVENT_KIND.items() if k is EventKind.TRADE)
_QUOTE_STREAMS: tuple[str, ...] = tuple(s for s, k in STREAM_EVENT_KIND.items() if k is EventKind.QUOTE)
_CHETIME_RE: str = r'"chetime"\s*:\s*"([^"]*)"'
_HOTIME_RE: str = r'"hotime"\s*:\s*"([^"]*)"'
_VALID_HHMMSS_RE: str = r"^(?:[01][0-9]|2[0-3])[0-5][0-9][0-5][0-9]$"


def _parse_event_time(event_time: str) -> dt.time | None:
    if len(event_time) != 6 or not event_time.isascii() or not event_time.isdigit():
        return None
    hour, minute, second = int(event_time[0:2]), int(event_time[2:4]), int(event_time[4:6])
    if hour > 23 or minute > 59 or second > 59:
        return None
    return dt.time(hour, minute, second)


def phase_windows_for(anchors: SessionAnchors) -> tuple[PhaseWindow, ...]:
    """Shift the contracted phase table by session anchors.

    Boundary mapping (each boundary of ``MARKET_PHASE_WINDOWS`` classified once):
    08:30 and 08:40 (before 09:00, pre-open) use ``shift_pre_open``;
    09:00 maps to ``regular_open``; 15:20 maps to ``closing_auction_start``;
    15:40 and 16:00 (at or after 15:30 and before 20:00, post-close) use
    ``shift_post_close``; 20:00 maps to ``after_market_end``.
    """
    return (
        PhaseWindow(MarketVenue.KRX, EventKind.TRADE, anchors.shift_pre_open(dt.time(8, 30)), anchors.shift_pre_open(dt.time(8, 40)), MarketPhase.PRE_MARKET_CLOSING_PRICE),
        PhaseWindow(MarketVenue.KRX, None, anchors.shift_pre_open(dt.time(8, 30)), anchors.regular_open, MarketPhase.OPENING_AUCTION),
        PhaseWindow(MarketVenue.KRX, None, anchors.regular_open, anchors.closing_auction_start, MarketPhase.REGULAR),
        PhaseWindow(MarketVenue.KRX, None, anchors.closing_auction_start, anchors.shift_post_close(dt.time(15, 40)), MarketPhase.CLOSING_AUCTION),
        PhaseWindow(MarketVenue.KRX, None, anchors.shift_post_close(dt.time(15, 40)), anchors.shift_post_close(dt.time(16, 0)), MarketPhase.POST_MARKET_CLOSING_PRICE),
        PhaseWindow(MarketVenue.KRX, None, anchors.shift_post_close(dt.time(16, 0)), anchors.after_market_end, MarketPhase.AFTERMARKET),
        PhaseWindow(MarketVenue.NXT, None, anchors.shift_post_close(dt.time(15, 40)), anchors.after_market_end, MarketPhase.AFTERMARKET),
    )


def classify_market_phase(venue: MarketVenue, event_time: str, kind: EventKind, *, windows: tuple[PhaseWindow, ...] = MARKET_PHASE_WINDOWS) -> MarketPhase:
    """Classify one event by exchange time using ``MARKET_PHASE_WINDOWS``.

    Args:
        venue: Exchange that printed the event.
        event_time: Exchange event time as 6-digit ``HHMMSS`` (KST).
        kind: Trade or quote event.

    Returns:
        The first matching phase, or ``MarketPhase.UNCLASSIFIED`` when the time
        is malformed or outside every contracted window.
    """
    moment = _parse_event_time(event_time)
    if moment is None:
        return MarketPhase.UNCLASSIFIED
    for window in windows:
        if window.venue == venue and (window.kind is None or window.kind == kind) and window.start <= moment < window.end:
            return window.phase
    return MarketPhase.UNCLASSIFIED


def _event_time_value() -> pl.Expr:
    raw_time = (
        pl.when(pl.col("stream").is_in(_TRADE_STREAMS))
        .then(pl.col("raw").str.extract(_CHETIME_RE, 1))
        .when(pl.col("stream").is_in(_QUOTE_STREAMS))
        .then(pl.col("raw").str.extract(_HOTIME_RE, 1))
        .otherwise(pl.lit(""))
    )
    return (
        pl.when(pl.col("exchange_event_time").fill_null("") != "")
        .then(pl.col("exchange_event_time"))
        .otherwise(raw_time)
        .fill_null("")
    )


def event_time_expr() -> pl.Expr:
    """Exchange event time (``HHMMSS``) per row, or ``""`` when unavailable.

    Prefers a well-formed ``exchange_event_time`` column value (KIS adapters
    stamp it at ingest). Otherwise reads the LS JSON body field from ``raw``:
    ``chetime`` for trade streams and ``hotime`` for quote streams, chosen by
    ``stream``.
    """
    return _event_time_value().alias("exchange_event_time")


def market_phase_expr(*, windows: tuple[PhaseWindow, ...] = MARKET_PHASE_WINDOWS) -> pl.Expr:
    """Vectorized ``classify_market_phase`` over ``venue``, ``stream`` and ``event_time_expr()``.

    Produces exactly the value the scalar classifier returns for every row, so
    batch L1 files and ad-hoc readers never disagree.
    """
    event = _event_time_value().fill_null("")
    valid = (
        event.str.contains(_VALID_HHMMSS_RE)
        & pl.col("stream").is_in(list(STREAM_EVENT_KIND))
        & pl.col("venue").is_in(sorted({window.venue.value for window in windows}))
    )
    result: pl.Expr = pl.lit(MarketPhase.UNCLASSIFIED.value)
    for window in reversed(windows):
        cond = (
            (pl.col("venue") == window.venue.value)
            & (event >= window.start.strftime("%H%M%S"))
            & (event < window.end.strftime("%H%M%S"))
        )
        if window.kind is not None:
            cond = cond & pl.col("stream").is_in([s for s, k in STREAM_EVENT_KIND.items() if k == window.kind])
        result = pl.when(valid & cond).then(pl.lit(window.phase.value)).otherwise(result)
    return result.alias("market_phase")


def annotate_market_phase(frame: FrameT, *, windows: tuple[PhaseWindow, ...] = MARKET_PHASE_WINDOWS) -> FrameT:
    """Fill ``exchange_event_time`` and add a ``market_phase`` column.

    Accepts current L1 frames and legacy L1 frames written before venue
    routing, which lack ``venue``, ``stream`` and ``exchange_event_time``.
    Those are treated as ``venue="krx"``, ``stream=tr_id`` and an empty event
    time, because every legacy partition came from the LS KRX connection.

    Args:
        frame: ``pl.DataFrame`` or ``pl.LazyFrame`` with at least ``raw`` and
            (``stream`` or ``tr_id``).

    Returns:
        Same frame type with every original row and column preserved (row order
        unchanged), ``exchange_event_time`` filled where it was empty and the
        time is derivable, and a ``market_phase`` string column (values from
        ``MarketPhase``) appended.
    """
    names = set(frame.collect_schema().names())
    out = frame
    if "venue" not in names:
        out = out.with_columns(pl.lit(MarketVenue.KRX.value).alias("venue"))
    if "stream" not in names:
        if "tr_id" not in names:
            raise ValueError("frame has neither stream nor tr_id")
        out = out.with_columns(pl.col("tr_id").alias("stream"))
    if "exchange_event_time" not in names:
        out = out.with_columns(pl.lit("").alias("exchange_event_time"))
    return out.with_columns(event_time_expr(), market_phase_expr(windows=windows))
