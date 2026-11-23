"""LS aftermarket adapter: one connection for KRX (S3_/K3_, H1_/HA_) and NXT (NS3, NH1) aftermarket frames."""

from __future__ import annotations

import json

from src.realtime.adapters.ls import LsAftermarketRealtimeAdapter, LsNxtRealtimeAdapter
from src.realtime.contracts import MarketSession, MarketVenue

_MARKET_OF = {"005930": "KOSPI", "086520": "KOSDAQ"}


def _adapter() -> LsAftermarketRealtimeAdapter:
    return LsAftermarketRealtimeAdapter(
        app_key="k",
        app_secret="s",
        http=object(),  # type: ignore[arg-type]
        market_of=_MARKET_OF,
        token_store=object(),  # type: ignore[arg-type]
    )


def _frame_json(tr_cd: str, tr_key: str) -> tuple[dict, str]:
    o = {"header": {"tr_cd": tr_cd, "tr_key": tr_key}, "body": {}}
    return o, json.dumps(o)


def test_subscribe_bodies_per_family() -> None:
    adapter = _adapter()
    assert adapter._subscribe_body("005930", "H0STCNT0") == {"tr_cd": "S3_", "tr_key": "005930"}
    assert adapter._subscribe_body("086520", "H0STCNT0") == {"tr_cd": "K3_", "tr_key": "086520"}
    assert adapter._subscribe_body("005930", "H0NXASP0") == {"tr_cd": "NH1", "tr_key": "N005930   "}
    assert adapter._subscribe_body("005930", "H0NXCNT0") == {"tr_cd": "NS3", "tr_key": "N005930   "}


def test_frames_stamped_with_aftermarket_session() -> None:
    adapter = _adapter()
    o, raw = _frame_json("S3_", "005930")
    krx = adapter._data_frame(o, o["header"], raw)
    assert krx is not None
    assert (krx.vendor, krx.stream, krx.symbol) == ("ls", "H0STCNT0", "005930")
    assert (krx.venue, krx.session) == (MarketVenue.KRX, MarketSession.KRX_AFTER)

    o, raw = _frame_json("NS3", "N005930   ")
    nxt = adapter._data_frame(o, o["header"], raw)
    assert nxt is not None
    assert (nxt.stream, nxt.symbol) == ("H0NXCNT0", "005930")
    assert (nxt.venue, nxt.session) == (MarketVenue.NXT, MarketSession.NXT_AFTER)


def test_market_tr_is_not_a_data_frame() -> None:
    adapter = _adapter()
    for tr_cd in ("JIF", "VI_"):
        o, raw = _frame_json(tr_cd, "005930")
        assert adapter._data_frame(o, o["header"], raw) is None


def test_invalid_keys_rejected_locally() -> None:
    adapter = _adapter()
    assert adapter._invalid_key("12345", "H0STCNT0") == "invalid_tr_key"
    assert adapter._invalid_key("N005930", "H0NXCNT0") == "invalid_tr_key"
    assert adapter._invalid_key("005930", "LS_VI") == "unknown_stream"
    assert adapter._invalid_key("000660", "H0STCNT0") == "unknown_market"
    assert adapter._invalid_key("005930", "H0STCNT0") is None
    assert adapter._invalid_key("0088M0", "H0NXASP0") is None


def test_ls_nxt_adapter_keeps_regular_session_frames() -> None:
    o, raw = _frame_json("NS3", "N005930   ")
    nxt_only = LsNxtRealtimeAdapter(
        app_key="k", app_secret="s", http=object(), token_store=object()  # type: ignore[arg-type]
    )
    assert nxt_only._invalid_key("005930", "H0STCNT0") == "unknown_stream"
    frame = nxt_only._data_frame(o, o["header"], raw)
    assert frame is not None
    assert (frame.venue, frame.session) == (MarketVenue.NXT, MarketSession.REGULAR)
