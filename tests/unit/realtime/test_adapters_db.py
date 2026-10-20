import asyncio
import json

import aiohttp
import pytest

from src.marketdata.toss_token_store import TossTokenStore, db_token_path
from src.realtime.adapters.db import DbRealtimeAdapter
from src.realtime.contracts import MarketSession, MarketVenue, VendorAuthRejected, VendorDisconnected


def _store(tmp_path):
    return TossTokenStore(db_token_path(tmp_path, "k"))


def _adapter(tmp_path, ws=None, **kw):
    adapter = DbRealtimeAdapter(app_key="k", app_secret="s", http=object(), token_store=_store(tmp_path), **kw)
    if ws is not None:
        adapter._ws = ws
        adapter._token = "TOK"
        adapter._conn_id = "db-test"
    return adapter


def _ack(code, tr_cd="S00"):
    return json.dumps({"header": {"tr_cd": tr_cd, "rsp_cd": code, "rsp_msg": "m"}, "body": None})


class _AckWS:
    def __init__(self, payloads):
        self._q = list(payloads)
        self.sent: list[str] = []

    async def send_str(self, s):
        self.sent.append(s)

    async def receive_str(self):
        return self._q.pop(0)


def test_db_subscribe_sends_spaced_krx_keys_and_matches_acks_by_order(tmp_path) -> None:
    ws = _AckWS([_ack("00000"), _ack("10017")])
    adapter = _adapter(tmp_path, ws)
    out = asyncio.run(adapter.subscribe([("005930", "H0STCNT0"), ("000660", "H0STASP0")]))
    bodies = [json.loads(s)["body"] for s in ws.sent]
    assert bodies[0] == {"tr_cd": "S00", "tr_key": "J 005930"}
    assert bodies[1] == {"tr_cd": "S01", "tr_key": "J 000660"}
    assert [a.accepted for a in out] == [True, False]
    assert [a.code for a in out] == ["00000", "10017"]


def test_db_subscribe_stashes_data_frames_received_before_ack(tmp_path) -> None:
    data = json.dumps({"header": {"tr_cd": "S00", "tr_key": "J 005930"}, "body": {"ShrnIscd": "005930"}})
    ws = _AckWS([data, _ack("00000")])
    adapter = _adapter(tmp_path, ws)
    acks = asyncio.run(adapter.subscribe([("005930", "H0STCNT0")]))
    assert acks[0].accepted is True
    frame = asyncio.run(adapter.recv())
    assert frame.symbol == "005930"
    assert frame.stream == "H0STCNT0"


def test_db_subscribe_ack_timeout_disconnects(tmp_path) -> None:
    class _Silent:
        async def send_str(self, s):
            return None

        async def receive_str(self):
            await asyncio.sleep(5)
            return "{}"

    adapter = _adapter(tmp_path, _Silent(), ack_timeout_s=0.05)
    with pytest.raises(VendorDisconnected, match="ack_timeout:005930:H0STCNT0"):
        asyncio.run(adapter.subscribe([("005930", "H0STCNT0")]))


def test_db_subscribe_rejects_over_capacity_before_send(tmp_path) -> None:
    class _Counting:
        def __init__(self):
            self.sends = 0

        async def send_str(self, s):
            self.sends += 1

        async def receive_str(self):
            return _ack("00000")

    ws = _Counting()
    adapter = _adapter(tmp_path, ws, capacity_pairs=50)
    pairs = [(f"{i:06d}", "H0STCNT0") for i in range(51)]
    with pytest.raises(VendorDisconnected, match="capacity_exceeded:51>50"):
        asyncio.run(adapter.subscribe(pairs))
    assert ws.sends == 0


def test_db_all_acks_auth_rejected_rotates_token(tmp_path) -> None:
    from unittest.mock import AsyncMock

    adapter = _adapter(tmp_path, _AckWS([_ack("10009")]))
    adapter._token_store.areplace_rejected = AsyncMock(return_value="NEW")  # type: ignore[attr-defined]
    with pytest.raises(VendorAuthRejected, match="auth_rejected:all_acks_rejected:10009"):
        asyncio.run(adapter.subscribe([("005930", "H0STCNT0")]))
    adapter._token_store.areplace_rejected.assert_awaited_once()  # type: ignore[attr-defined]


def test_db_all_acks_unknown_symbol_does_not_rotate(tmp_path) -> None:
    from unittest.mock import AsyncMock

    adapter = _adapter(tmp_path, _AckWS([_ack("10017"), _ack("10017")]))
    adapter._token_store.areplace_rejected = AsyncMock(side_effect=AssertionError("must not rotate"))  # type: ignore[attr-defined]
    out = asyncio.run(adapter.subscribe([("005930", "H0STCNT0"), ("000660", "H0STCNT0")]))
    assert all(a.code == "10017" and not a.accepted for a in out)


def test_db_session_limit_ack_disconnects(tmp_path) -> None:
    adapter = _adapter(tmp_path, _AckWS([_ack("10011")]))
    with pytest.raises(VendorDisconnected, match="session_limit"):
        asyncio.run(adapter.subscribe([("005930", "H0STCNT0")]))


def test_db_token_throttle_is_disconnect_not_auth(tmp_path) -> None:
    from src.realtime.contracts import VendorAuthRejected

    class _Resp:
        status = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def json(self):
            return {"error_code": "IGW00201"}

    class _Http:
        def post(self, url, **kw):
            return _Resp()

        def ws_connect(self, url, **kw):
            raise AssertionError("ws must not open")

    adapter = DbRealtimeAdapter(app_key="k", app_secret="s", http=_Http(), token_store=_store(tmp_path))
    with pytest.raises(VendorDisconnected, match="token_throttled"):
        asyncio.run(adapter.connect())
    try:
        asyncio.run(adapter.connect())
    except VendorAuthRejected:
        pytest.fail("throttle must not raise VendorAuthRejected")
    except VendorDisconnected:
        pass


def test_db_recv_builds_krx_regular_frame_with_verbatim_raw(tmp_path) -> None:
    raw = json.dumps({"header": {"tr_cd": "S00", "tr_key": "J 005930"}, "body": {"ShrnIscd": "005930", "x": 1}})

    class _Msg:
        type = aiohttp.WSMsgType.TEXT
        data = raw

    class _WS:
        async def receive(self):
            return _Msg()

    adapter = _adapter(tmp_path, _WS())
    frame = asyncio.run(adapter.recv())
    assert frame.vendor == "db"
    assert frame.stream == "H0STCNT0"
    assert frame.symbol == "005930"
    assert frame.raw == raw
    assert frame.venue is MarketVenue.KRX
    assert frame.session is MarketSession.REGULAR


@pytest.mark.parametrize(
    ("tr_cd", "field", "value", "expected"),
    [
        ("S00", "StckCntghour", "090742", "090742"),
        ("S01", "BsopHour", "153000", "153000"),
        ("S00", "StckCntghour", "99", ""),
        ("S00", "StckCntghour", "256100", ""),
        ("S00", "BsopHour", "090742", ""),
    ],
)
def test_db_recv_stamps_valid_exchange_event_time(tmp_path, tr_cd, field, value, expected) -> None:
    raw = json.dumps({"header": {"tr_cd": tr_cd, "tr_key": None}, "body": {"ShrnIscd": "005930", field: value}})

    class _Msg:
        type = aiohttp.WSMsgType.TEXT
        data = raw

    class _WS:
        async def receive(self):
            return _Msg()

    frame = asyncio.run(_adapter(tmp_path, _WS()).recv())
    assert frame.exchange_event_time == expected


def test_db_recv_falls_back_to_header_tr_key_symbol(tmp_path) -> None:
    raw = json.dumps({"header": {"tr_cd": "S01", "tr_key": "J 000660"}, "body": {}})

    class _Msg:
        type = aiohttp.WSMsgType.TEXT
        data = raw

    class _WS:
        async def receive(self):
            return _Msg()

    adapter = _adapter(tmp_path, _WS())
    frame = asyncio.run(adapter.recv())
    assert frame.symbol == "000660"
    assert frame.stream == "H0STASP0"


def test_db_recv_counts_unparsable_without_raising(tmp_path) -> None:
    raw = json.dumps({"header": {"tr_cd": "S00", "tr_key": "J 005930"}, "body": {"ShrnIscd": "005930"}})

    class _Msg:
        def __init__(self, t, d):
            self.type = t
            self.data = d

    class _WS:
        def __init__(self):
            self._q = [
                _Msg(aiohttp.WSMsgType.TEXT, "not-json{{{"),
                _Msg(aiohttp.WSMsgType.TEXT, raw),
            ]

        async def receive(self):
            return self._q.pop(0)

    adapter = _adapter(tmp_path, _WS())
    frame = asyncio.run(adapter.recv())
    assert adapter.unparsable_frames == 1
    assert frame.symbol == "005930"


def test_db_recv_answers_ping(tmp_path) -> None:
    raw = json.dumps({"header": {"tr_cd": "S00", "tr_key": "J 005930"}, "body": {"ShrnIscd": "005930"}})

    class _Msg:
        def __init__(self, t, d=None):
            self.type = t
            self.data = d

    class _WS:
        def __init__(self):
            self._q = [_Msg(aiohttp.WSMsgType.PING), _Msg(aiohttp.WSMsgType.TEXT, raw)]
            self.ponged = 0

        async def receive(self):
            return self._q.pop(0)

        async def pong(self):
            self.ponged += 1

    ws = _WS()
    adapter = _adapter(tmp_path, ws)
    frame = asyncio.run(adapter.recv())
    assert ws.ponged == 1
    assert frame.symbol == "005930"


def test_db_streams_validation_rejects_unknown(tmp_path) -> None:
    with pytest.raises(ValueError, match="unsupported streams"):
        DbRealtimeAdapter(
            app_key="k", app_secret="s", http=object(), token_store=_store(tmp_path), streams=("H0STCNT0", "BOGUS")
        )


def test_db_session_limit_stops_further_sends(tmp_path) -> None:
    ws = _AckWS([_ack("10011"), _ack("00000")])
    adapter = _adapter(tmp_path, ws)
    with pytest.raises(VendorDisconnected, match="session_limit"):
        asyncio.run(adapter.subscribe([("005930", "H0STCNT0"), ("000660", "H0STCNT0")]))
    assert len(ws.sent) == 1


def test_db_subscribe_sleeps_spacing_between_sends_only(tmp_path, monkeypatch) -> None:
    sleeps: list[float] = []

    async def _sleep(s):
        sleeps.append(s)

    monkeypatch.setattr(asyncio, "sleep", _sleep)
    adapter = _adapter(tmp_path, _AckWS([_ack("00000")] * 3), subscribe_spacing_s=0.15)
    asyncio.run(adapter.subscribe([("005930", "H0STCNT0"), ("000660", "H0STCNT0"), ("035420", "H0STCNT0")]))
    assert sleeps == [0.15, 0.15]


def test_db_subscribe_counts_unparsable_while_awaiting_ack(tmp_path) -> None:
    adapter = _adapter(tmp_path, _AckWS(["not-json{{", "[1]", _ack("00000")]))
    acks = asyncio.run(adapter.subscribe([("005930", "H0STCNT0")]))
    assert acks[0].accepted is True
    assert adapter.unparsable_frames == 2


class _TokenResp:
    def __init__(self, payload, status=200):
        self._payload = payload
        self.status = status

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class _WsCtx:
    def __init__(self, ws):
        self._ws = ws

    async def __aenter__(self):
        if isinstance(self._ws, Exception):
            raise self._ws
        return self._ws


class _TokenHttp:
    def __init__(self, payload, status=200, ws=None):
        self._payload = payload
        self._status = status
        self._ws = ws
        self.posts: list[dict] = []
        self.ws_kw: dict = {}

    def post(self, url, **kw):
        self.posts.append(kw)
        return _TokenResp(self._payload, self._status)

    def ws_connect(self, url, **kw):
        self.ws_kw = kw
        return _WsCtx(self._ws)


def test_db_connect_issues_form_token_and_resets_connection_state(tmp_path) -> None:
    ws = object()
    http = _TokenHttp({"access_token": "TOK", "expires_in": 86400}, ws=ws)
    adapter = DbRealtimeAdapter(app_key="k", app_secret="s", http=http, token_store=_store(tmp_path), heartbeat_s=7.0)
    adapter.unparsable_frames = 3
    asyncio.run(adapter.connect())
    assert http.posts[0]["data"] == {
        "grant_type": "client_credentials",
        "appkey": "k",
        "appsecretkey": "s",
        "scope": "oob",
    }
    assert http.ws_kw == {"heartbeat": 7.0}
    assert adapter._ws is ws
    assert adapter._token == "TOK"
    assert adapter._conn_id.startswith("db-")
    assert adapter.unparsable_frames == 0


@pytest.mark.parametrize(
    ("payload", "status", "exc", "match"),
    [
        ({"error_code": "IGW00121"}, 200, VendorAuthRejected, "auth_rejected:200:IGW00121"),
        ({}, 401, VendorAuthRejected, "auth_rejected:401:"),
        ({}, 500, VendorDisconnected, "connect_failed:token_rejected:500:"),
        ([1], 200, VendorDisconnected, "connect_failed:token_response_not_object"),
        (aiohttp.ClientError("x"), 200, VendorDisconnected, "connect_failed:ClientError"),
    ],
)
def test_db_token_failures_classify_auth_vs_transport(tmp_path, payload, status, exc, match) -> None:
    http = _TokenHttp(payload, status, ws=AssertionError("ws must not open"))
    adapter = DbRealtimeAdapter(app_key="k", app_secret="s", http=http, token_store=_store(tmp_path))
    with pytest.raises(exc, match=match):
        asyncio.run(adapter.connect())


def test_db_connect_ws_failure_is_disconnect(tmp_path) -> None:
    http = _TokenHttp({"access_token": "TOK"}, ws=aiohttp.ClientError("refused"))
    adapter = DbRealtimeAdapter(app_key="k", app_secret="s", http=http, token_store=_store(tmp_path))
    with pytest.raises(VendorDisconnected, match="connect_failed:ClientError"):
        asyncio.run(adapter.connect())


def test_db_token_store_failures_are_disconnects(tmp_path) -> None:
    from unittest.mock import AsyncMock

    from src.marketdata.toss_token_store import TokenStoreLockTimeout

    adapter = _adapter(tmp_path, _AckWS([_ack("10009")]))
    adapter._token_store.aget_or_issue = AsyncMock(side_effect=TokenStoreLockTimeout("t"))  # type: ignore[attr-defined]
    adapter._token_store.areplace_rejected = AsyncMock(side_effect=OSError("disk"))  # type: ignore[attr-defined]
    with pytest.raises(VendorDisconnected, match="token_store_failed:TokenStoreLockTimeout"):
        asyncio.run(adapter.connect())
    with pytest.raises(VendorDisconnected, match="token_store_failed:OSError"):
        asyncio.run(adapter.subscribe([("005930", "H0STCNT0")]))


def test_db_subscribe_transport_failures_are_disconnects(tmp_path) -> None:
    class _SendFails(_AckWS):
        async def send_str(self, s):
            raise aiohttp.ClientError("gone")

    class _NonText(_AckWS):
        async def receive_str(self):
            raise TypeError("binary")

    with pytest.raises(VendorDisconnected, match="subscribe_failed:ClientError"):
        asyncio.run(_adapter(tmp_path, _SendFails([])).subscribe([("005930", "H0STCNT0")]))
    with pytest.raises(VendorDisconnected, match="subscribe_non_text"):
        asyncio.run(_adapter(tmp_path, _NonText([])).subscribe([("005930", "H0STCNT0")]))


def test_db_subscribe_ignores_headerless_objects_while_awaiting_ack(tmp_path) -> None:
    adapter = _adapter(tmp_path, _AckWS([json.dumps({"body": {}}), _ack("00000")]))
    acks = asyncio.run(adapter.subscribe([("005930", "H0STCNT0")]))
    assert acks[0].accepted is True
    assert adapter.unparsable_frames == 0
    assert adapter._pending == []


class _QueueWS:
    def __init__(self, msgs, pong_exc=None, close_exc=None):
        self._q = list(msgs)
        self._pong_exc = pong_exc
        self._close_exc = close_exc
        self.closed = 0

    async def receive(self):
        return self._q.pop(0)

    async def pong(self):
        if self._pong_exc is not None:
            raise self._pong_exc

    async def close(self):
        self.closed += 1
        if self._close_exc is not None:
            raise self._close_exc


class _M:
    def __init__(self, t, d=None):
        self.type = t
        self.data = d


def test_db_recv_skips_acks_and_counts_symbolless_data_frames(tmp_path) -> None:
    good = json.dumps({"header": {"tr_cd": "S00"}, "body": {"ShrnIscd": "005930"}})
    msgs = [
        _M(aiohttp.WSMsgType.TEXT, "[1]"),
        _M(aiohttp.WSMsgType.TEXT, json.dumps({"body": {}})),
        _M(aiohttp.WSMsgType.TEXT, _ack("00000")),
        _M(aiohttp.WSMsgType.TEXT, json.dumps({"header": {"tr_cd": "S00", "tr_key": "J ?"}, "body": {}})),
        _M(aiohttp.WSMsgType.TEXT, json.dumps({"header": {"tr_cd": "XX"}, "body": {}})),
        _M(aiohttp.WSMsgType.TEXT, good),
        _M(aiohttp.WSMsgType.TEXT, good),
    ]
    adapter = _adapter(tmp_path, _QueueWS(msgs))
    first = asyncio.run(adapter.recv())
    second = asyncio.run(adapter.recv())
    assert adapter.unparsable_frames == 2
    assert (first.conn_seq, second.conn_seq) == (1, 2)
    assert first.raw == good


@pytest.mark.parametrize("mtype", [aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR])
def test_db_recv_close_types_disconnect(tmp_path, mtype) -> None:
    adapter = _adapter(tmp_path, _QueueWS([_M(mtype)]))
    with pytest.raises(VendorDisconnected, match=str(mtype)):
        asyncio.run(adapter.recv())


def test_db_recv_close_frame_reports_code_and_reason(tmp_path) -> None:
    closing = _M(aiohttp.WSMsgType.CLOSE)
    closing.data = 1008
    closing.extra = "policy violation"
    adapter = _adapter(tmp_path, _QueueWS([closing]))
    with pytest.raises(VendorDisconnected, match=r"code=1008:reason=policy violation"):
        asyncio.run(adapter.recv())


def test_db_recv_pong_failure_disconnects(tmp_path) -> None:
    adapter = _adapter(tmp_path, _QueueWS([_M(aiohttp.WSMsgType.PING)], pong_exc=OSError("x")))
    with pytest.raises(VendorDisconnected, match="pingpong_echo_failed:OSError"):
        asyncio.run(adapter.recv())


def test_db_aclose_is_idempotent_and_suppresses_errors(tmp_path) -> None:
    ws = _QueueWS([], close_exc=aiohttp.ClientError("x"))
    adapter = _adapter(tmp_path, ws)
    asyncio.run(adapter.aclose())
    asyncio.run(adapter.aclose())
    assert ws.closed == 1
