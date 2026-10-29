"""Kiwoom REST WebSocket adapter invariant tests."""

from __future__ import annotations

import asyncio
import json
import logging

import aiohttp
import pytest

from src.realtime.adapters.kiwoom import KIWOOM_STREAM_TYPE, KIWOOM_WS_URL, KiwoomRealtimeAdapter
from src.realtime.contracts import MarketSession, MarketVenue, VendorAuthRejected, VendorDisconnected

_REAL = {"trnm": "REAL", "data": []}


def _entry(item="005930", type_code="0B", values=None):
    return {
        "type": type_code,
        "name": "삼성전자",
        "item": item,
        "values": values
        if values is not None
        else {"20": "112114", "10": "-916000", "15": "-1", "13": "74090", "290": "2"},
    }


class _FakeWs:
    def __init__(self, received=None):
        self.received = list(received or [])
        self.sent: list[str] = []
        self.closed = False
        self.connect_kwargs: dict = {}

    async def send_str(self, raw):
        self.sent.append(raw)

    async def receive_str(self):
        if not self.received:
            raise aiohttp.ClientConnectionError("empty")
        item = self.received.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    async def close(self):
        self.closed = True


class _FakeHttp:
    def __init__(self, ws):
        self._ws = ws
        self.calls: list[dict] = []

    async def ws_connect(self, url, **kwargs):
        self.calls.append({"url": url, **kwargs})
        self._ws.connect_kwargs = {"url": url, **kwargs}
        return self._ws


def _adapter(ws, **overrides):
    async def _token():
        return "tok"

    kwargs = {
        "app_key": "app",
        "secret_key": "sec",
        "http": _FakeHttp(ws),
        "streams": ("H0STCNT0", "H0STASP0"),
        "capacity_pairs": 200,
        "user_agent": "krx-alpha",
        "ack_timeout_s": 5.0,
        "issue_token": _token,
    }
    kwargs.update(overrides)
    return KiwoomRealtimeAdapter(**kwargs)


async def _connect(ws, **overrides):
    adapter = _adapter(ws, **overrides)
    await adapter.connect()
    return adapter


def test_handshake_sends_explicit_user_agent() -> None:
    ws = _FakeWs(['{"trnm":"LOGIN","return_code":0}'])
    http = _FakeHttp(ws)
    adapter = _adapter(ws, http=http, user_agent="krx-alpha")
    asyncio.run(adapter.connect())
    assert http.calls[0]["url"] == KIWOOM_WS_URL
    assert http.calls[0]["headers"] == {"User-Agent": "krx-alpha"}
    assert http.calls[0]["compress"] == 0
    assert http.calls[0]["heartbeat"] is None
    assert adapter.name == "kiwoom"
    assert adapter.capacity_pairs == 200
    assert adapter.pingpong_count == 0
    assert adapter.unparsable_frames == 0


def test_login_rejection_is_auth_failure() -> None:
    ws = _FakeWs(['{"trnm":"LOGIN","return_code":8005}'])
    with pytest.raises(VendorAuthRejected, match="kiwoom_login:8005"):
        asyncio.run(_connect(ws))


def test_login_malformed_code_fails_closed() -> None:
    ws = _FakeWs(['{"trnm":"LOGIN"}'])
    with pytest.raises(VendorDisconnected, match="login_malformed"):
        asyncio.run(_connect(ws))


def test_login_timeout_fails_closed() -> None:
    class _HangingWs(_FakeWs):
        async def receive_str(self):
            await asyncio.sleep(10)

    ws = _HangingWs(['{"trnm":"LOGIN","return_code":0}'])
    with pytest.raises(VendorDisconnected, match="ack_timeout:LOGIN"):
        asyncio.run(_connect(ws, ack_timeout_s=0.05))


def test_batched_reg_acks_all_pairs() -> None:
    ws = _FakeWs(['{"trnm":"LOGIN","return_code":0}', '{"trnm":"REG","return_code":0}'])
    adapter = asyncio.run(_connect(ws))
    pairs = [(f"{i:06d}", "H0STCNT0") for i in range(1, 101)]
    acks = asyncio.run(adapter.subscribe(pairs))
    assert len(acks) == 100
    assert all(a.accepted and a.code == "0" for a in acks)
    regs = [json.loads(raw) for raw in ws.sent if json.loads(raw).get("trnm") == "REG"]
    assert len(regs) == 1
    assert regs[0]["refresh"] == "1"
    assert len(regs[0]["data"][0]["item"]) == 100
    assert regs[0]["data"][0]["type"] == [KIWOOM_STREAM_TYPE["H0STCNT0"]]


def test_reg_failure_rejects_batch() -> None:
    ws = _FakeWs(['{"trnm":"LOGIN","return_code":0}', '{"trnm":"REG","return_code":1702}'])
    adapter = asyncio.run(_connect(ws))
    acks = asyncio.run(adapter.subscribe([("005930", "H0STCNT0"), ("000660", "H0STCNT0")]))
    assert [a.accepted for a in acks] == [False, False]
    assert {a.code for a in acks} == {"1702"}


def test_capacity_overflow_rejected_without_send() -> None:
    ws = _FakeWs(['{"trnm":"LOGIN","return_code":0}', '{"trnm":"REG","return_code":0}'])
    adapter = asyncio.run(_connect(ws, capacity_pairs=2, streams=("H0STCNT0",)))
    pairs = [("005930", "H0STCNT0"), ("000660", "H0STCNT0"), ("005380", "H0STCNT0")]
    acks = asyncio.run(adapter.subscribe(pairs))
    assert acks[2].accepted is False
    assert acks[2].code == "capacity"
    assert acks[0].accepted
    assert acks[1].accepted
    regs = [json.loads(raw) for raw in ws.sent if json.loads(raw).get("trnm") == "REG"]
    assert len(regs) == 1
    assert "005380" not in regs[0]["data"][0]["item"]


def test_per_type_limit_overflow_rejected_without_send() -> None:
    ws = _FakeWs(
        [
            '{"trnm":"LOGIN","return_code":0}',
            '{"trnm":"REG","return_code":0}',
            '{"trnm":"REG","return_code":0}',
        ]
    )
    adapter = asyncio.run(_connect(ws, capacity_pairs=4))
    pairs = [("005930", "H0STCNT0"), ("000660", "H0STCNT0"), ("005380", "H0STCNT0"), ("005930", "H0STASP0")]
    acks = asyncio.run(adapter.subscribe(pairs))
    overflow = [a for a in acks if a.code == "capacity"]
    assert len(overflow) == 1
    assert overflow[0].symbol == "005380"


def test_unknown_stream_rejected() -> None:
    ws = _FakeWs(['{"trnm":"LOGIN","return_code":0}'])
    adapter = asyncio.run(_connect(ws))
    with pytest.raises(ValueError, match="unsupported stream"):
        asyncio.run(adapter.subscribe([("005930", "H0STCNT0X")]))
    with pytest.raises(ValueError, match="unsupported streams"):
        _adapter(ws, streams=("H0STCNT0X",))
    with pytest.raises(ValueError, match="ack_timeout"):
        _adapter(ws, ack_timeout_s=0)
    with pytest.raises(ValueError, match="capacity_pairs"):
        _adapter(ws, capacity_pairs=0)


def test_suffix_subscription_rejected_before_send() -> None:
    ws = _FakeWs(['{"trnm":"LOGIN","return_code":0}'])
    adapter = asyncio.run(_connect(ws))
    with pytest.raises(ValueError, match="bare KRX"):
        asyncio.run(adapter.subscribe([("005930_NX", "H0STCNT0")]))
    assert len(ws.sent) == 1


def test_zero_per_stream_capacity_sends_no_registration() -> None:
    ws = _FakeWs(['{"trnm":"LOGIN","return_code":0}'])
    adapter = asyncio.run(_connect(ws, capacity_pairs=1))
    acks = asyncio.run(adapter.subscribe([("005930", "H0STCNT0")]))
    assert acks[0].code == "capacity"
    assert len(ws.sent) == 1


def test_ping_echo_obeys_login_deadline() -> None:
    class SlowEcho(_FakeWs):
        async def send_str(self, raw):
            if json.loads(raw).get("trnm") == "PING":
                await asyncio.sleep(1)
            await super().send_str(raw)

    ws = SlowEcho(['{"trnm":"PING"}'])
    with pytest.raises(VendorDisconnected, match="ack_timeout:LOGIN"):
        asyncio.run(_connect(ws, ack_timeout_s=0.01))


def test_ping_echoed_verbatim_and_hidden() -> None:
    ping = '{"trnm":"PING"}'
    real = json.dumps({**_REAL, "data": [_entry()]})
    ws = _FakeWs(['{"trnm":"LOGIN","return_code":0}', ping, real])
    adapter = asyncio.run(_connect(ws))
    frame = asyncio.run(adapter.recv())
    assert frame.symbol == "005930"
    assert adapter.pingpong_count == 1
    assert adapter.last_pingpong_wall_ns is not None
    assert ping in ws.sent


def test_ping_during_login_is_echoed() -> None:
    ping = '{"trnm":"PING"}'
    ws = _FakeWs([ping, '{"trnm":"LOGIN","return_code":0}'])
    adapter = asyncio.run(_connect(ws))
    assert adapter.pingpong_count == 1
    assert ping in ws.sent


def test_real_during_subscribe_is_buffered() -> None:
    entry = _entry()
    real = json.dumps({**_REAL, "data": [entry]})
    ws = _FakeWs(['{"trnm":"LOGIN","return_code":0}', real, '{"trnm":"REG","return_code":0}'])
    adapter = asyncio.run(_connect(ws))
    acks = asyncio.run(adapter.subscribe([("005930", "H0STCNT0")]))
    assert acks[0].accepted
    frame = asyncio.run(adapter.recv())
    assert json.loads(frame.raw) == entry


def test_stray_frames_during_login_are_skipped() -> None:
    ws = _FakeWs(
        [
            json.dumps([1, 2]),
            json.dumps({"trnm": "BOGUS"}),
            '{"trnm":"LOGIN","return_code":0}',
        ]
    )
    adapter = asyncio.run(_connect(ws))
    assert adapter.unparsable_frames == 2
    assert adapter.pingpong_count == 0


def test_await_reply_transport_errors_fail_closed() -> None:
    ws = _FakeWs(['{"trnm":"LOGIN","return_code":0}'])
    adapter = asyncio.run(_connect(ws))
    ws.received.append(TypeError("binary frame"))
    with pytest.raises(VendorDisconnected, match="subscribe_non_text"):
        asyncio.run(adapter.subscribe([("005930", "H0STCNT0")]))
    ws2 = _FakeWs(['{"trnm":"LOGIN","return_code":0}', aiohttp.ClientConnectionError("down")])
    adapter2 = asyncio.run(_connect(ws2))
    with pytest.raises(VendorDisconnected, match="recv_failed"):
        asyncio.run(adapter2.subscribe([("005930", "H0STCNT0")]))


def test_recv_non_dict_frame_counted() -> None:
    ws = _FakeWs(['{"trnm":"LOGIN","return_code":0}', "[1,2]", json.dumps({**_REAL, "data": [_entry()]})])
    adapter = asyncio.run(_connect(ws))
    frame = asyncio.run(adapter.recv())
    assert frame.symbol == "005930"
    assert adapter.unparsable_frames == 1


def test_batch_split_preserves_order_and_receive_time() -> None:
    entries = [
        _entry(values={"20": "112114", "10": "91600", "15": "5", "13": "100", "290": "2"}),
        _entry(values={"20": "112115", "10": "91610", "15": "-3", "13": "103", "290": "2"}),
        _entry(item="000660", values={"20": "112116", "10": "70000", "15": "2", "13": "50", "290": "2"}),
    ]
    ws = _FakeWs(['{"trnm":"LOGIN","return_code":0}', json.dumps({**_REAL, "data": entries})])
    adapter = asyncio.run(_connect(ws))
    frames = [asyncio.run(adapter.recv()) for _ in range(3)]
    assert [f.symbol for f in frames] == ["005930", "005930", "000660"]
    assert frames[0].recv_wall_ns == frames[1].recv_wall_ns == frames[2].recv_wall_ns
    assert frames[0].recv_mono_ns == frames[1].recv_mono_ns
    seqs = [f.conn_seq for f in frames]
    assert seqs == sorted(seqs)
    assert len(set(seqs)) == 3
    assert frames[0].conn_id == frames[2].conn_id
    assert json.loads(frames[0].raw) == entries[0]
    assert frames[0].exchange_event_time == "112114"
    assert frames[0].venue is MarketVenue.KRX
    assert frames[0].session is MarketSession.REGULAR
    assert frames[0].vendor == "kiwoom"
    assert frames[0].stream == "H0STCNT0"


def test_stream_mapping_book() -> None:
    ws = _FakeWs(['{"trnm":"LOGIN","return_code":0}', json.dumps({**_REAL, "data": [_entry(type_code="0D")]})])
    adapter = asyncio.run(_connect(ws))
    frame = asyncio.run(adapter.recv())
    assert frame.stream == "H0STASP0"


def test_suffix_item_journaled_with_unknown_venue() -> None:
    ws = _FakeWs(['{"trnm":"LOGIN","return_code":0}', json.dumps({**_REAL, "data": [_entry(item="005930_NX")]})])
    adapter = asyncio.run(_connect(ws))
    frame = asyncio.run(adapter.recv())
    assert frame.symbol == "005930_NX"
    assert frame.venue is MarketVenue.UNKNOWN
    assert frame.session is MarketSession.REGULAR


def test_unparsable_frame_counted_and_logged(caplog) -> None:
    ws = _FakeWs(
        [
            '{"trnm":"LOGIN","return_code":0}',
            "not json",
            json.dumps({"trnm": "BOGUS"}),
            json.dumps({**_REAL, "data": [_entry()]}),
        ]
    )
    adapter = asyncio.run(_connect(ws))
    with caplog.at_level(logging.WARNING):
        frame = asyncio.run(adapter.recv())
    assert frame.symbol == "005930"
    assert adapter.unparsable_frames == 2
    assert "stage=kiwoom_frame status=UNPARSEABLE" in caplog.text


def test_malformed_entry_skipped() -> None:
    ws = _FakeWs(
        [
            '{"trnm":"LOGIN","return_code":0}',
            json.dumps({**_REAL, "data": [{"type": "0B"}, _entry()]}),
        ]
    )
    adapter = asyncio.run(_connect(ws))
    frame = asyncio.run(adapter.recv())
    assert frame.symbol == "005930"
    assert adapter.unparsable_frames == 1


def test_empty_and_fully_invalid_real_batches_skipped() -> None:
    entry = _entry()
    ws = _FakeWs(
        [
            '{"trnm":"LOGIN","return_code":0}',
            json.dumps({"trnm": "REAL", "data": []}),
            json.dumps({"trnm": "REAL", "data": "oops"}),
            json.dumps({**_REAL, "data": [{"type": "ZZ", "item": "005930", "values": {}}, 42]}),
            json.dumps({**_REAL, "data": [entry]}),
        ]
    )
    adapter = asyncio.run(_connect(ws))
    frame = asyncio.run(adapter.recv())
    assert json.loads(frame.raw) == entry
    assert adapter.unparsable_frames == 4


def test_recv_socket_error_fails_closed() -> None:
    ws = _FakeWs(['{"trnm":"LOGIN","return_code":0}'])
    adapter = asyncio.run(_connect(ws))
    with pytest.raises(VendorDisconnected, match=r"recv_failed|ws_non_text"):
        asyncio.run(adapter.recv())


def test_recv_non_text_frame_fails_closed() -> None:
    ws = _FakeWs(['{"trnm":"LOGIN","return_code":0}'])
    ws.received.append(aiohttp.WSMessageTypeError("binary"))
    adapter = asyncio.run(_connect(ws))
    with pytest.raises(VendorDisconnected, match="ws_non_text_frame"):
        asyncio.run(adapter.recv())


def test_subscribe_send_failure_fails_closed() -> None:
    ws = _FakeWs(['{"trnm":"LOGIN","return_code":0}'])
    adapter = asyncio.run(_connect(ws))

    async def _boom(raw):
        raise aiohttp.ClientConnectionError("down")

    ws.send_str = _boom
    with pytest.raises(VendorDisconnected, match="send_failed"):
        asyncio.run(adapter.subscribe([("005930", "H0STCNT0")]))


def test_ping_echo_failure_fails_closed() -> None:
    ws = _FakeWs(['{"trnm":"LOGIN","return_code":0}', '{"trnm":"PING"}'])
    adapter = asyncio.run(_connect(ws))

    async def _boom(raw):
        if json.loads(raw).get("trnm") == "PING":
            raise aiohttp.ClientConnectionError("down")

    ws.send_str = _boom
    with pytest.raises(VendorDisconnected, match="pingpong_echo_failed"):
        asyncio.run(adapter.recv())


def test_connect_token_and_socket_failures() -> None:
    async def _auth():
        raise VendorAuthRejected("nope")

    with pytest.raises(VendorAuthRejected):
        asyncio.run(_connect(_FakeWs([]), issue_token=_auth))

    async def _boom():
        raise RuntimeError("down")

    with pytest.raises(VendorDisconnected, match="connect_failed"):
        asyncio.run(_connect(_FakeWs([]), issue_token=_boom))

    class _BadHttp:
        async def ws_connect(self, *a, **k):
            raise aiohttp.ClientConnectionError("down")

    with pytest.raises(VendorDisconnected, match="connect_failed"):
        asyncio.run(_connect(_FakeWs([]), http=_BadHttp()))


def test_secrets_never_logged(caplog) -> None:
    ws = _FakeWs(['{"trnm":"LOGIN","return_code":0}', "junk", '{"trnm":"REG","return_code":0}'])

    async def _token():
        return "super-secret-token"

    adapter = _adapter(ws, app_key="appKEY123", secret_key="secKEY456", issue_token=_token)
    with caplog.at_level(logging.DEBUG):
        asyncio.run(adapter.connect())
        asyncio.run(adapter.subscribe([("005930", "H0STCNT0")]))
    assert "super-secret-token" not in caplog.text
    assert "appKEY123" not in caplog.text
    assert "secKEY456" not in caplog.text


def test_aclose_idempotent() -> None:
    ws = _FakeWs(['{"trnm":"LOGIN","return_code":0}'])
    adapter = asyncio.run(_connect(ws))
    asyncio.run(adapter.aclose())
    asyncio.run(adapter.aclose())
    assert ws.closed


def test_aclose_suppresses_transport_error(caplog) -> None:
    ws = _FakeWs(['{"trnm":"LOGIN","return_code":0}'])
    adapter = asyncio.run(_connect(ws))

    async def _boom():
        raise aiohttp.ClientConnectionError("down")

    ws.close = _boom
    with caplog.at_level(logging.DEBUG):
        asyncio.run(adapter.aclose())
    assert "stage=kiwoom_close" in caplog.text


def test_registry_build_wraps_sync_token_via_thread(monkeypatch) -> None:
    from src.core.config import CollectorSettings
    from src.realtime.adapters import registry
    from src.realtime.adapters.registry import AppCredentials, RegularAdapterContext

    monkeypatch.setenv("KIWOOM_APP_KEY", "k")
    monkeypatch.setenv("KIWOOM_SECRET_KEY", "s")
    entry = registry.regular_vendor("kiwoom")
    assert entry.requires_market_map is False
    creds = entry.load_credentials()
    assert creds == AppCredentials("k", "s")

    seen: dict = {}

    def _fake_issue(*, app_key, secret_key, session, timeout_s):
        seen.update(app_key=app_key, secret_key=secret_key, timeout_s=timeout_s)
        return "live-token"

    monkeypatch.setattr("src.marketdata.kiwoom_ticks.issue_kiwoom_token", _fake_issue)
    settings = CollectorSettings()
    ctx = RegularAdapterContext(object(), settings, None, None)  # type: ignore[arg-type]
    adapter = entry.build(creds, ctx)
    assert adapter.capacity_pairs == settings.kiwoom_capacity_pairs
    token = asyncio.run(adapter._issue_token())
    assert token == "live-token"
    assert seen["app_key"] == "k"
