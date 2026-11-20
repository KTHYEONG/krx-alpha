from __future__ import annotations

import pathlib
from typing import Literal

import polars as pl
import pytest

from src.core.errors import KrxAlphaError
from src.universe.membership import MembershipRow
from src.universe.research_visibility import (
    VISIBLE_ALL_SESSION,
    DataOrigin,
    flag_cross_venue_same_second,
    mask_before_visibility,
    origin_for_l1_relpath,
    repaired_origin_expr,
    research_visibility,
)

T = 1_000_000_000_000
SEC = 1_000_000_000


def _row(
    symbol: str,
    *,
    source: str,
    decided_at_ns: int = 0,
    tier: Literal["A", "B", "C"] = "C",
) -> MembershipRow:
    return MembershipRow(
        symbol=symbol,
        tier=tier,
        source=source,
        decided_at_ns=decided_at_ns,
        direction=None,
        warning_level=None,
        ls_shard=None,
        kis_slot=None,
        trade_value_krw=None,
        reason=None,
    )


def test_static_visible_whole_session() -> None:
    # Given: a static tier C row
    rows = [_row("A", source="static", decided_at_ns=T, tier="C")]

    # When / Then: visible for the whole session
    assert research_visibility(rows) == {"A": VISIBLE_ALL_SESSION}


def test_dynamic_visible_from_decision_ack_ignored() -> None:
    # Given: a dynamic admit at T and its subscription ack at T+5s
    rows = [
        _row("A", source="dynamic", decided_at_ns=T, tier="B"),
        _row("A", source="ack", decided_at_ns=T + 5 * SEC, tier="B"),
    ]

    # When / Then: visibility is the decision instant, not the ack
    assert research_visibility(rows) == {"A": T}


def test_static_dominates_later_dynamic() -> None:
    # Given: a static row and a later dynamic row for one symbol
    rows = [
        _row("A", source="static", decided_at_ns=T - SEC, tier="B"),
        _row("A", source="dynamic", decided_at_ns=T, tier="B"),
    ]

    # When / Then: earliest knowable instant wins
    assert research_visibility(rows) == {"A": VISIBLE_ALL_SESSION}


def test_capacity_from_decision() -> None:
    # Given: an intraday capacity decision at T
    rows = [_row("A", source="intraday_capacity", decided_at_ns=T)]

    # When / Then
    assert research_visibility(rows) == {"A": T}


def test_post_close_never_visible() -> None:
    # Given: only a post-close screen row and an ack
    rows = [
        _row("A", source="post_close_screen", decided_at_ns=T),
        _row("A", source="ack", decided_at_ns=T + SEC, tier="B"),
    ]

    # When / Then: never visible
    assert research_visibility(rows) == {"A": None}


def test_unknown_source_fails_closed() -> None:
    # Given: a row whose source has no visibility policy
    rows = [_row("A", source="foo", decided_at_ns=T)]

    # When / Then
    with pytest.raises(KrxAlphaError, match="foo"):
        research_visibility(rows)


def test_mask_drops_pre_decision_rows() -> None:
    # Given: rows at T-1, T, T+1 for a symbol visible at T
    frame = pl.DataFrame({"symbol": ["A", "A", "A"], "time_ns": [T - 1, T, T + 1], "seq": [0, 1, 2]})

    # When
    out = mask_before_visibility(frame, {"A": T}, symbol_col="symbol", time_ns_col="time_ns")

    # Then: T and T+1 kept, order preserved
    assert out["seq"].to_list() == [1, 2]


def test_mask_drops_invisible_and_unknown_symbols() -> None:
    # Given: rows for a None symbol and an unlisted symbol, plus a visible one
    frame = pl.DataFrame({"symbol": ["N", "U", "V"], "time_ns": [T, T, T]})

    # When
    out = mask_before_visibility(frame, {"N": None, "V": 0}, symbol_col="symbol", time_ns_col="time_ns")

    # Then
    assert out["symbol"].to_list() == ["V"]


def test_mask_null_time() -> None:
    # Given: null-time rows for a visibility-0 symbol and a visibility-T symbol
    frame = pl.DataFrame(
        {"symbol": ["Z", "T1"], "time_ns": [None, None]},
        schema={"symbol": pl.String, "time_ns": pl.Int64},
    )

    # When
    out = mask_before_visibility(frame, {"Z": 0, "T1": T}, symbol_col="symbol", time_ns_col="time_ns")

    # Then: kept for 0, dropped for T
    assert out["symbol"].to_list() == ["Z"]


def test_mask_future_perturbation_invariant() -> None:
    # Given: two frames differing only in rows after T
    base = {"symbol": ["A", "A", "A"], "time_ns": [T - 1, T, T + 1]}
    perturbed = {"symbol": ["A", "A", "A"], "time_ns": [T - 1, T, T + 999 * SEC]}
    vis = {"A": T}

    # When
    a = mask_before_visibility(pl.DataFrame(base), vis, symbol_col="symbol", time_ns_col="time_ns")
    b = mask_before_visibility(pl.DataFrame(perturbed), vis, symbol_col="symbol", time_ns_col="time_ns")

    # Then: restricted to time <= T, outputs are equal
    assert a.filter(pl.col("time_ns") <= T).equals(b.filter(pl.col("time_ns") <= T))


def test_origin_paths() -> None:
    # Given / When / Then
    assert origin_for_l1_relpath(pathlib.PurePosixPath("snapshot/ohlcv/dt=2026-11-20")) is DataOrigin.REST_SNAPSHOT
    assert origin_for_l1_relpath(pathlib.PurePosixPath("history/tick_tape/dt=2026-11-20")) is DataOrigin.REST_BACKFILL
    assert origin_for_l1_relpath(pathlib.PurePosixPath("history/minute/dt=2026-11-20")) is DataOrigin.REST_MINUTE
    assert (
        origin_for_l1_relpath(pathlib.PurePosixPath("ls/krx/regular/H0STCNT0/dt=2026-11-20"))
        is DataOrigin.WS_REALTIME
    )


def test_origin_repaired_and_unknown_raise() -> None:
    # Given / When / Then
    with pytest.raises(ValueError, match="ticks_repaired"):
        origin_for_l1_relpath(pathlib.PurePosixPath("derived/ticks_repaired/dt=2026-11-20"))
    with pytest.raises(ValueError, match="unclassified"):
        origin_for_l1_relpath(pathlib.PurePosixPath("foo/bar"))


def test_repaired_expr_maps_live() -> None:
    # Given: sources live, gapfill, and null
    frame = pl.DataFrame({"source": ["live", "gapfill", None]}, schema={"source": pl.String})

    # When
    out = frame.with_columns(repaired_origin_expr("source").alias("origin"))

    # Then
    assert out["origin"].to_list() == [DataOrigin.WS_REALTIME.value, DataOrigin.REST_BACKFILL.value, None]


def test_cross_venue_same_second() -> None:
    # Given: KRX and NXT rows for one symbol in second 090001, and a KRX-only row in 090002
    frame = pl.DataFrame(
        {
            "symbol": ["A", "A", "A"],
            "venue": ["KRX", "NXT", "KRX"],
            "second": [90001, 90001, 90002],
        }
    )

    # When
    out = flag_cross_venue_same_second(frame, symbol_col="symbol", venue_col="venue", second_col="second")

    # Then: flagged, flagged, not flagged; order and rows unchanged
    assert out["cross_venue_order_unknown"].to_list() == [True, True, False]
    assert out.height == 3


def test_cross_venue_other_symbol_isolated() -> None:
    # Given: a KRX row for symbol A and an NXT row for symbol B in the same second
    frame = pl.DataFrame({"symbol": ["A", "B"], "venue": ["KRX", "NXT"], "second": [90001, 90001]})

    # When
    out = flag_cross_venue_same_second(frame, symbol_col="symbol", venue_col="venue", second_col="second")

    # Then
    assert out["cross_venue_order_unknown"].to_list() == [False, False]


def test_mask_missing_columns_raise() -> None:
    # Given: a frame lacking the time column
    frame = pl.DataFrame({"symbol": ["A"]})

    # When / Then
    with pytest.raises(ValueError, match="time_ns"):
        mask_before_visibility(frame, {"A": 0}, symbol_col="symbol", time_ns_col="time_ns")


def test_mask_lazy_frame_stays_lazy() -> None:
    # Given: a lazy frame with one visible row
    frame = pl.LazyFrame({"symbol": ["A", "A"], "time_ns": [T - 1, T]})

    # When
    out = mask_before_visibility(frame, {"A": T}, symbol_col="symbol", time_ns_col="time_ns")

    # Then: still lazy, and collects to the visible row
    assert isinstance(out, pl.LazyFrame)
    assert out.collect()["time_ns"].to_list() == [T]


def test_cross_venue_existing_column_raises() -> None:
    # Given: a frame that already carries the output column
    frame = pl.DataFrame({"symbol": ["A"], "venue": ["KRX"], "second": [90001], "cross_venue_order_unknown": [False]})

    # When / Then
    with pytest.raises(ValueError, match="cross_venue_order_unknown"):
        flag_cross_venue_same_second(frame, symbol_col="symbol", venue_col="venue", second_col="second")
