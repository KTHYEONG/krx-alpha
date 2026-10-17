"""JSON-WS OAuth 어댑터 공통 기반 불변식 (LS/DB 훅 분기, 카운터, 로그 리터럴)."""

from __future__ import annotations

import asyncio
import json
import logging

import aiohttp
import pytest

from src.marketdata.toss_token_store import TossTokenStore, db_token_path, ls_token_path
from src.realtime.adapters.db import DbRealtimeAdapter
from src.realtime.adapters.ls import LsRealtimeAdapter
from src.realtime.contracts import VendorDisconnected

_LS_LITERAL = "[DATA] stage=ls_frame status=UNPARSEABLE conn_id=%s frame_len=%d"
_DB_LITERAL = "[DATA] stage=db_frame status=UNPARSEABLE conn_id=%s frame_len=%d"


class _Msg:
    def __init__(self, t, d=None):
        self.type = t
        self.data = d


class _AckWS:
    def __init__(self, replies):
        self._q = list(replies)
        self.sent: list[str] = []

    async def send_str(self, s):
        self.sent.append(s)

    async def receive_str(self):
        return self._q.pop(0)


class _RecvWS:
    def __init__(self, msgs, pong_exc=None):
        self._q = list(msgs)
        self.sent: list[str] = []
        self.ponged = 0
        self._pong_exc = pong_exc

    async def receive(self):
        return self._q.pop(0)

    async def send_str(self, s):
        self.sent.append(s)

    async def pong(self):
        self.ponged += 1
        if self._pong_exc is not None:
            raise self._pong_exc


def _ls(tmp_path, ws=None, **kw):
    adapter = LsRealtimeAdapter(
        app_key="k",
        app_secret="s",
        http=object(),
        market_of={"005930": "KOSPI", "000660": "KOSPI", "035420": "KOSPI"},
        token_store=TossTokenStore(ls_token_path(tmp_path, "k")),
        **kw,
    )
    if ws is not None:
        adapter._ws = ws
        adapter._token = "TOK"
        adapter._conn_id = "ls-test"
    return adapter


def _db(tmp_path, ws=None, **kw):
    adapter = DbRealtimeAdapter(
        app_key="k",
        app_secret="s",
        http=object(),
        token_store=TossTokenStore(db_token_path(tmp_path, "k")),
        **kw,
    )
    if ws is not None:
        adapter._ws = ws
        adapter._token = "TOK"
        adapter._conn_id = "db-test"
    return adapter


def _ls_data(symbol="005930"):
    return json.dumps({"header": {"tr_cd": "S3_", "tr_key": symbol}, "body": {"price": "70000"}})


def _db_data(symbol="005930"):
    return json.dumps({"header": {"tr_cd": "S00", "tr_key": f"J {symbol}"}, "body": {"ShrnIscd": symbol}})


def _ack(tr_cd="S00", code="00000"):
    return json.dumps({"header": {"tr_cd": tr_cd, "rsp_cd": code, "rsp_msg": "m"}, "body": None})


def test_ls_no_spacing_sleep(tmp_path, monkeypatch) -> None:
    sleeps: list[float] = []

    async def _sleep(s):
        sleeps.append(s)

    monkeypatch.setattr(asyncio, "sleep", _sleep)
    ws = _AckWS([_ack("S3_") for _ in range(3)])
    adapter = _ls(tmp_path, ws)

    asyncio.run(adapter.subscribe([("005930", "H0STCNT0"), ("000660", "H0STCNT0"), ("035420", "H0STCNT0")]))

    assert sleeps == []


def test_db_spacing_between_sends_only(tmp_path, monkeypatch) -> None:
    sleeps: list[float] = []

    async def _sleep(s):
        sleeps.append(s)

    monkeypatch.setattr(asyncio, "sleep", _sleep)
    ws = _AckWS([_ack("S00") for _ in range(3)])
    adapter = _db(tmp_path, ws, subscribe_spacing_s=0.12)

    asyncio.run(adapter.subscribe([("005930", "H0STCNT0"), ("000660", "H0STCNT0"), ("035420", "H0STCNT0")]))

    assert sleeps == [0.12, 0.12]


def test_malformed_ack_policy_per_vendor(tmp_path) -> None:
    ls = _ls(tmp_path, _AckWS(["not-json{{"]))
    with pytest.raises(VendorDisconnected, match="malformed_ack:H0STCNT0"):
        asyncio.run(ls.subscribe([("005930", "H0STCNT0")]))

    db = _db(tmp_path, _AckWS(["not-json{{", _ack("S00")]))
    acks = asyncio.run(db.subscribe([("005930", "H0STCNT0")]))
    assert db.unparsable_frames == 1
    assert acks[0].accepted is True


def test_pingpong_echo_ls_only(tmp_path) -> None:
    ping = json.dumps({"header": {"tr_cd": "PINGPONG"}})

    ls = _ls(tmp_path, _RecvWS([_Msg(aiohttp.WSMsgType.TEXT, ping), _Msg(aiohttp.WSMsgType.TEXT, _ls_data())]))
    frame = asyncio.run(ls.recv())
    assert frame.symbol == "005930"
    assert ls._ws.sent == [ping]

    db = _db(tmp_path, _RecvWS([_Msg(aiohttp.WSMsgType.TEXT, ping), _Msg(aiohttp.WSMsgType.TEXT, _db_data())]))
    frame = asyncio.run(db.recv())
    assert frame.symbol == "005930"
    assert db._ws.sent == []
    assert db.unparsable_frames == 0


class _Resp:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def json(self):
        return {"access_token": "TOK", "expires_in": 86400}


class _WSCtx:
    def __init__(self, ws):
        self._ws = ws

    async def __aenter__(self):
        return self._ws

    async def __aexit__(self, *a):
        return False


class _Http:
    def __init__(self, ws):
        self._ws = ws

    def post(self, url, **kw):
        return _Resp()

    def ws_connect(self, url, **kw):
        return _WSCtx(self._ws)


def test_conn_id_prefix_is_vendor_and_counters_reset(tmp_path) -> None:
    ls = LsRealtimeAdapter(
        app_key="k",
        app_secret="s",
        http=_Http(object()),
        market_of={"005930": "KOSPI"},
        token_store=TossTokenStore(ls_token_path(tmp_path, "k")),
    )
    ls.unparsable_frames = 3
    ls._unparsable_warned = True
    asyncio.run(ls.connect())
    assert ls._conn_id.startswith("ls-")
    assert ls.unparsable_frames == 0
    assert ls._unparsable_warned is False

    db = DbRealtimeAdapter(
        app_key="k",
        app_secret="s",
        http=_Http(object()),
        token_store=TossTokenStore(db_token_path(tmp_path, "k")),
    )
    db.unparsable_frames = 3
    db._unparsable_warned = True
    asyncio.run(db.connect())
    assert db._conn_id.startswith("db-")
    assert db.unparsable_frames == 0
    assert db._unparsable_warned is False


def test_unparsable_log_literal_preserved(tmp_path, caplog) -> None:
    ls = _ls(tmp_path, _RecvWS([_Msg(aiohttp.WSMsgType.TEXT, "<html>"), _Msg(aiohttp.WSMsgType.TEXT, "[1]"), _Msg(aiohttp.WSMsgType.TEXT, _ls_data())]))
    with caplog.at_level(logging.WARNING, logger="src.realtime.adapters.ls"):
        asyncio.run(ls.recv())
    records = [r for r in caplog.records if r.msg == _LS_LITERAL]
    assert len(records) == 1
    assert records[0].name == "src.realtime.adapters.ls"
    assert ls.unparsable_frames == 2

    caplog.clear()
    db = _db(tmp_path, _RecvWS([_Msg(aiohttp.WSMsgType.TEXT, "<html>"), _Msg(aiohttp.WSMsgType.TEXT, "[1]"), _Msg(aiohttp.WSMsgType.TEXT, _db_data())]))
    with caplog.at_level(logging.WARNING, logger="src.realtime.adapters.db"):
        asyncio.run(db.recv())
    records = [r for r in caplog.records if r.msg == _DB_LITERAL]
    assert len(records) == 1
    assert records[0].name == "src.realtime.adapters.db"
    assert db.unparsable_frames == 2


def test_seq_increments_only_on_frames(tmp_path) -> None:
    late_ack = json.dumps({"header": {"tr_cd": "S3_", "rsp_cd": "00000"}})
    ws = _RecvWS(
        [
            _Msg(aiohttp.WSMsgType.TEXT, late_ack),
            _Msg(aiohttp.WSMsgType.PING),
            _Msg(aiohttp.WSMsgType.TEXT, "garbage"),
            _Msg(aiohttp.WSMsgType.TEXT, _ls_data()),
            _Msg(aiohttp.WSMsgType.TEXT, _ls_data("000660")),
        ]
    )
    adapter = _ls(tmp_path, ws)

    first = asyncio.run(adapter.recv())
    second = asyncio.run(adapter.recv())

    assert (first.conn_seq, second.conn_seq) == (1, 2)


def test_contracts_planner_removed() -> None:
    import src.realtime.contracts as contracts

    assert not hasattr(contracts, "SubscriptionPlanner")
    assert not hasattr(contracts, "VendorCapacity")
    assert contracts.VendorAdapter._is_protocol
    for method in ("connect", "subscribe", "recv", "aclose"):
        assert hasattr(contracts.VendorAdapter, method)


def test_db_ws_auth_rejection_preserves_error_without_rotating_token(tmp_path) -> None:
    from unittest.mock import AsyncMock

    from src.realtime.contracts import VendorAuthRejected

    rejected = VendorAuthRejected("auth_rejected:401:")

    class _RejectedContext:
        async def __aenter__(self):
            raise rejected

    class _RejectedHttp:
        def ws_connect(self, url, **kwargs):
            return _RejectedContext()

    adapter = _db(tmp_path)
    adapter._http = _RejectedHttp()
    adapter._token_store.aget_or_issue = AsyncMock(return_value="cached")
    replacement = AsyncMock(side_effect=AssertionError("DB must not rotate on WS auth rejection"))
    adapter._token_store.areplace_rejected = replacement

    with pytest.raises(VendorAuthRejected) as caught:
        asyncio.run(adapter.connect())

    assert caught.value is rejected
    assert adapter._token == "cached"
    replacement.assert_not_awaited()
