import datetime as dt
import json

import polars as pl

from src.realtime.contracts import MarketVenue
from src.storage.market_phase import (
    EventKind,
    MarketPhase,
    annotate_market_phase,
    classify_market_phase,
    event_time_expr,
    market_phase_expr,
)


def _body(stream: str, event_time: str) -> str:
    field = "chetime" if stream in ("H0STCNT0", "H0NXCNT0") else "hotime"
    return json.dumps({"header": {"tr_cd": "x", "tr_key": "005930"}, "body": {"shcode": "005930", field: event_time}})


def _frame(rows: list[dict[str, object]]) -> pl.DataFrame:
    return pl.DataFrame(rows)


def test_classify_krx_trade_in_pre_market_closing_price() -> None:
    assert classify_market_phase(MarketVenue.KRX, "083500", EventKind.TRADE) is MarketPhase.PRE_MARKET_CLOSING_PRICE


def test_classify_krx_quote_in_pre_market_window_is_opening_auction() -> None:
    assert classify_market_phase(MarketVenue.KRX, "083500", EventKind.QUOTE) is MarketPhase.OPENING_AUCTION


def test_classify_regular_session_boundaries_half_open() -> None:
    assert classify_market_phase(MarketVenue.KRX, "090000", EventKind.TRADE) is MarketPhase.REGULAR
    assert classify_market_phase(MarketVenue.KRX, "151959", EventKind.QUOTE) is MarketPhase.REGULAR
    assert classify_market_phase(MarketVenue.KRX, "152000", EventKind.TRADE) is MarketPhase.CLOSING_AUCTION


def test_classify_closing_cross_stays_in_auction() -> None:
    assert classify_market_phase(MarketVenue.KRX, "153000", EventKind.TRADE) is MarketPhase.CLOSING_AUCTION


def test_classify_krx_post_market_closing_price() -> None:
    assert classify_market_phase(MarketVenue.KRX, "154500", EventKind.TRADE) is MarketPhase.POST_MARKET_CLOSING_PRICE


def test_classify_krx_aftermarket_window() -> None:
    assert classify_market_phase(MarketVenue.KRX, "160000", EventKind.TRADE) is MarketPhase.AFTERMARKET
    assert classify_market_phase(MarketVenue.KRX, "195959", EventKind.QUOTE) is MarketPhase.AFTERMARKET
    assert classify_market_phase(MarketVenue.KRX, "200000", EventKind.TRADE) is MarketPhase.UNCLASSIFIED


def test_classify_nxt_aftermarket_starts_1540() -> None:
    assert classify_market_phase(MarketVenue.NXT, "154000", EventKind.TRADE) is MarketPhase.AFTERMARKET
    assert classify_market_phase(MarketVenue.NXT, "153959", EventKind.TRADE) is MarketPhase.UNCLASSIFIED
    assert classify_market_phase(MarketVenue.NXT, "100000", EventKind.QUOTE) is MarketPhase.UNCLASSIFIED


def test_classify_malformed_event_time_unclassified() -> None:
    for event_time in ("", "9999", "246000", "12a000"):
        assert classify_market_phase(MarketVenue.KRX, event_time, EventKind.TRADE) is MarketPhase.UNCLASSIFIED
    assert classify_market_phase(MarketVenue.UNKNOWN, "100000", EventKind.TRADE) is MarketPhase.UNCLASSIFIED


def test_market_phase_expr_matches_scalar_on_every_boundary() -> None:
    boundaries = ["083000", "084000", "090000", "152000", "154000", "160000", "200000"]
    offsets = [dt.timedelta(seconds=d) for d in (-1, 0, 1)]

    def _shift(event_time: str, delta: dt.timedelta) -> str:
        moment = dt.datetime.combine(dt.date(2026, 9, 23), dt.time(int(event_time[:2]), int(event_time[2:4]), int(event_time[4:])))
        return (moment + delta).strftime("%H%M%S")

    rows: list[dict[str, object]] = []
    for venue in (MarketVenue.KRX, MarketVenue.NXT):
        for kind in (EventKind.TRADE, EventKind.QUOTE):
            stream = {"krx": {"trade": "H0STCNT0", "quote": "H0STASP0"}, "nxt": {"trade": "H0NXCNT0", "quote": "H0NXASP0"}}[venue.value][kind.value]
            for boundary in boundaries:
                for delta in offsets:
                    event_time = _shift(boundary, delta)
                    rows.append({
                        "raw": _body(stream, event_time),
                        "venue": venue.value,
                        "stream": stream,
                        "exchange_event_time": event_time,
                    })
    frame = _frame(rows).with_columns(market_phase_expr())

    for row in frame.iter_rows(named=True):
        assert row["market_phase"] == classify_market_phase(
            MarketVenue(row["venue"]), str(row["exchange_event_time"]),
            EventKind.TRADE if str(row["stream"]).endswith("CNT0") else EventKind.QUOTE,
        ).value


def test_event_time_expr_prefers_stamped_column_over_raw() -> None:
    frame = _frame([{
        "raw": _body("H0STCNT0", "090000"),
        "venue": "krx",
        "stream": "H0STCNT0",
        "exchange_event_time": "161000",
    }]).with_columns(event_time_expr(), market_phase_expr())

    assert frame["exchange_event_time"].to_list() == ["161000"]
    assert frame["market_phase"].to_list() == ["aftermarket"]


def test_event_time_expr_falls_back_to_ls_body_by_stream_kind() -> None:
    frame = _frame([
        {"raw": _body("H0STCNT0", "154500"), "venue": "krx", "stream": "H0STCNT0", "exchange_event_time": ""},
        {"raw": _body("H0STASP0", "083500"), "venue": "krx", "stream": "H0STASP0", "exchange_event_time": ""},
        {"raw": "005930^154500^caret", "venue": "krx", "stream": "UNKNOWN", "exchange_event_time": ""},
        {"raw": _body("H0STCNT0", "100000"), "venue": "krx", "stream": "H0STCNT0", "exchange_event_time": None},
    ]).with_columns(event_time_expr(), market_phase_expr())

    assert frame["exchange_event_time"].to_list() == ["154500", "083500", "", "100000"]
    assert frame["market_phase"].to_list() == [
        MarketPhase.POST_MARKET_CLOSING_PRICE.value,
        MarketPhase.OPENING_AUCTION.value,
        MarketPhase.UNCLASSIFIED.value,
        MarketPhase.REGULAR.value,
    ]


def test_annotate_legacy_l1_without_routing_columns() -> None:
    raw_trade = _body("H0STCNT0", "163000")
    frame = _frame([{
        "raw": raw_trade,
        "tr_id": "H0STCNT0",
        "recv_wall_ns": 7,
        "conn_id": "c",
        "conn_seq": 1,
        "vendor": "ls",
    }])

    out = annotate_market_phase(frame)

    assert isinstance(out, pl.DataFrame)
    assert out["venue"].to_list() == ["krx"]
    assert out["stream"].to_list() == ["H0STCNT0"]
    assert out["exchange_event_time"].to_list() == ["163000"]
    assert out["market_phase"].to_list() == ["aftermarket"]
    for column in ("raw", "tr_id", "recv_wall_ns", "conn_id", "conn_seq", "vendor"):
        assert out[column].to_list() == frame[column].to_list()


def test_annotate_lazy_frame_matches_dataframe() -> None:
    rows = [
        {"raw": _body("H0STCNT0", "083000"), "venue": "krx", "stream": "H0STCNT0", "exchange_event_time": ""},
        {"raw": _body("H0STCNT0", "100000"), "venue": "krx", "stream": "H0STCNT0", "exchange_event_time": "100000"},
    ]
    assert annotate_market_phase(_frame(rows).lazy()).collect().equals(annotate_market_phase(_frame(rows)))


def test_annotate_preserves_rows_and_raw_bytes() -> None:
    rows = [
        {"raw": _body("H0STCNT0", "154500"), "venue": "krx", "stream": "H0STCNT0", "exchange_event_time": ""},
        {"raw": _body("H0NXASP0", "170000"), "venue": "nxt", "stream": "H0NXASP0", "exchange_event_time": ""},
        {"raw": "not-json", "venue": "krx", "stream": "H0STCNT0", "exchange_event_time": ""},
    ]
    frame = _frame(rows)

    out = annotate_market_phase(frame)

    assert out.height == frame.height
    assert out["raw"].to_list() == frame["raw"].to_list()
    assert [c for c in out.columns if c in frame.columns] == frame.columns


def test_annotate_rejects_frame_without_stream_or_tr_id() -> None:
    import pytest

    with pytest.raises(ValueError, match="neither stream nor tr_id"):
        annotate_market_phase(_frame([{"raw": "{}"}]))
