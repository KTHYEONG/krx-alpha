"""Toss realtime adapter invariants (fakes only; no network, no token issuance)."""

from __future__ import annotations

import asyncio
import itertools
import json
import logging
from types import SimpleNamespace
from typing import Any

import aiohttp
import pytest

from src.marketdata.toss_token_store import TossTokenStore, toss_token_path
from src.realtime.adapters.toss import TOSS_TOPIC_TYPE, TossRealtimeAdapter
from src.realtime.contracts import (
    HeartbeatProbe,
    LiveSubscribable,
    MarketSession,
    MarketVenue,
    VendorAuthRejected,
    VendorDisconnected,
)

TOKEN = "TOKEN-SECRET-VALUE"
TRADE = "H0STCNT0"
BOOK = "H0STASP0"


def _text(data: str) -> Any:
    return SimpleNamespace(type=aiohttp.WSMsgType.TEXT, data=data)


def _ack(subscribed: list[str], rejected: list[Any] | None = None) -> str:
    return json.dumps({"type": "subscriptions", "subscribed": subscribed, "rejected": rejected or []})


def _trade(symbol: str = "000660", ts: str = "2026-11-17T09:45:24.821+09:00") -> str:
    return (
        f'{{"type":"message","topic":"trade:kr:{symbol}","data":'
        f'{{"price":"1108000","volume":"1","timestamp":"{ts}","currency":"KRW"}}}}'
    )


class FakeWS:
    def __init__(self) -> None:
        self.inbox: asyncio.Queue[Any] = asyncio.Queue()
        self.sent: list[str] = []
        self.closed = False
        self.auto_ack = True

    async def send_str(self, text: str) -> None:
        self.sent.append(text)
        if self.auto_ack and text.startswith("["):
            body = json.loads(text)
            topics = [f"{item['type']}:{code}" for item in body for code in item["codes"]]
            self.inbox.put_nowait(_text(_ack(topics)))

    async def receive(self, timeout: float | None = None) -> Any:  # noqa: ASYNC109 - mirrors aiohttp's receive signature
        return await asyncio.wait_for(self.inbox.get(), timeout)

    async def close(self) -> None:
        self.closed = True

    def push(self, data: str) -> None:
        self.inbox.put_nowait(_text(data))

    def declarations(self) -> list[list[dict[str, Any]]]:
        return [json.loads(s) for s in self.sent if s.startswith("[")]


class FakeStore:
    def __init__(self, token: str = TOKEN) -> None:
        self.token = token
        self.calls: list[str] = []

    async def aget_or_issue(self, issue: Any) -> str:
        self.calls.append("aget_or_issue")
        return self.token

    async def areplace_rejected(self, rejected: str, issue: Any) -> str:
        self.calls.append("areplace_rejected")
        return self.token


class FakeHTTP:
    def __init__(self, ws: FakeWS | None = None, error: Exception | None = None) -> None:
        self.ws = ws or FakeWS()
        self.error = error
        self.headers: dict[str, str] = {}

    async def ws_connect(self, url: str, **kwargs: Any) -> FakeWS:
        if self.error is not None:
            raise self.error
        self.headers = kwargs["headers"]
        return self.ws


def _adapter(http: FakeHTTP | None = None, store: Any = None, **kw: Any) -> tuple[TossRealtimeAdapter, FakeHTTP, Any]:
    http = http or FakeHTTP()
    store = store or FakeStore()
    kw.setdefault("declare_spacing_s", 0.001)
    return TossRealtimeAdapter(http=http, token_store=store, **kw), http, store


async def _connected(**kw: Any) -> tuple[TossRealtimeAdapter, FakeWS, Any]:
    adapter, http, store = _adapter(**kw)
    await adapter.connect()
    return adapter, http.ws, store


def test_protocols_and_topic_whitelist() -> None:
    adapter, _, _ = _adapter()
    assert isinstance(adapter, LiveSubscribable)
    assert isinstance(adapter, HeartbeatProbe)
    assert dict(TOSS_TOPIC_TYPE) == {TRADE: "trade:kr", BOOK: "orderbook:kr"}


def test_topic_mapping_and_invalid_stream_rejected() -> None:
    async def run() -> None:
        adapter, ws, _ = await _connected()
        acks = await adapter.subscribe([("005930", TRADE), ("005930", BOOK)])
        assert [a.accepted for a in acks] == [True, True]
        assert ws.declarations() == [[{"type": "trade:kr", "codes": ["005930"]}, {"type": "orderbook:kr", "codes": ["005930"]}]]
        for bad in ("personal:order", "H0NXCNT0", "trade:kr"):
            with pytest.raises(ValueError, match="unsupported stream"):
                await adapter.subscribe([("005930", bad)])
        with pytest.raises(ValueError, match="invalid symbol"):
            await adapter.subscribe([("0059", TRADE)])
        assert len(ws.declarations()) == 1

    asyncio.run(run())


def test_live_declaration_is_full_state_and_rate_limited() -> None:
    async def run() -> None:
        clock = [0.0]
        slept: list[float] = []

        async def sleep(seconds: float) -> None:
            slept.append(seconds)
            clock[0] += seconds

        adapter, ws, _ = await _connected(clock=lambda: clock[0], sleep=sleep, declare_spacing_s=0.25, heartbeat_s=1e9)
        stamps: list[float] = []
        original = ws.send_str

        async def stamped(text: str) -> None:
            if text.startswith("["):
                stamps.append(clock[0])
            await original(text)

        ws.send_str = stamped  # type: ignore[method-assign]
        receiver = asyncio.create_task(_drain(adapter))
        for symbol in ("000660", "005930", "009150"):
            acks = await adapter.subscribe_live([(symbol, TRADE)])
            assert all(a.accepted for a in acks)
        receiver.cancel()
        sizes = [sum(len(item["codes"]) for item in body) for body in ws.declarations()]
        assert sizes == [1, 2, 3]
        assert [item["codes"] for item in ws.declarations()[-1]] == [["000660", "005930", "009150"]]
        assert all(b - a >= 0.25 - 1e-9 for a, b in itertools.pairwise(stamps))
        assert adapter.subscribed_pairs() == (("000660", TRADE), ("005930", TRADE), ("009150", TRADE))

    asyncio.run(run())


async def _drain(adapter: TossRealtimeAdapter) -> None:
    while True:
        await adapter.recv()


def test_capacity_guard_raises_before_any_send() -> None:
    async def run() -> None:
        adapter, ws, _ = await _connected(capacity_pairs=100)
        pairs = [(f"{i:06d}", TRADE) for i in range(101)]
        with pytest.raises(VendorDisconnected, match=r"capacity_exceeded:101>100"):
            await adapter.subscribe(pairs)
        with pytest.raises(VendorDisconnected, match="capacity_exceeded"):
            await adapter.subscribe_live(pairs)
        assert ws.sent == []
        acks = await adapter.subscribe(pairs[:100])
        assert all(a.accepted for a in acks)
        with pytest.raises(VendorDisconnected, match=r"capacity_exceeded:101>100"):
            await adapter.subscribe_live([("999999", TRADE)])

    asyncio.run(run())


def test_acks_map_accepted_rejected_and_missing() -> None:
    async def run() -> None:
        adapter, ws, _ = await _connected()
        ws.auto_ack = False
        ws.push(_ack(["trade:kr:000660"], [{"topic": "trade:kr:005930", "reason": "unknown"}]))
        acks = await adapter.subscribe([("000660", TRADE), ("005930", TRADE), ("009150", TRADE)])
        assert [(a.accepted, a.code) for a in acks] == [(True, "subscribed"), (False, "rejected"), (False, "missing_ack")]
        assert adapter.subscribed_pairs() == (("000660", TRADE),)
        ws.auto_ack = True
        again = await adapter.subscribe([("009150", TRADE)])
        assert again[0].accepted
        assert [item["codes"] for item in ws.declarations()[-1]] == [["000660", "009150"]]

    asyncio.run(run())


def test_ack_timeout_reports_timeout_and_keeps_earlier_state() -> None:
    async def run() -> None:
        adapter, ws, _ = await _connected(ack_timeout_s=0.02)
        await adapter.subscribe([("000660", TRADE)])
        ws.auto_ack = False
        acks = await adapter.subscribe([("005930", TRADE)])
        assert [(a.accepted, a.code) for a in acks] == [(False, "timeout")]
        assert adapter.subscribed_pairs() == (("000660", TRADE),)

    asyncio.run(run())


def test_data_frames_met_during_subscribe_are_buffered_for_recv() -> None:
    async def run() -> None:
        adapter, ws, _ = await _connected()
        ws.push(_trade())
        await adapter.subscribe([("000660", TRADE)])
        frame = await adapter.recv()
        assert frame.symbol == "000660"

    asyncio.run(run())


def test_ping_is_sent_on_interval_and_pong_resets() -> None:
    async def run() -> None:
        adapter, ws, _ = await _connected(heartbeat_s=0.03)
        task = asyncio.create_task(adapter.recv())
        await asyncio.sleep(0.04)
        assert ws.sent.count("PING") == 1
        ws.push("PONG")
        await asyncio.sleep(0.04)
        assert ws.sent.count("PING") >= 2
        assert adapter.pingpong_count == 1
        assert adapter.last_pingpong_wall_ns is not None
        assert not task.done()
        task.cancel()

    asyncio.run(run())


def test_two_missed_pong_intervals_raise() -> None:
    async def run() -> None:
        adapter, ws, _ = await _connected(heartbeat_s=0.02)
        with pytest.raises(VendorDisconnected, match="pong_timeout"):
            await asyncio.wait_for(adapter.recv(), 1.0)
        assert ws.sent.count("PING") >= 1

    asyncio.run(run())


def test_frames_are_verbatim_logical_unified_and_timed(caplog: pytest.LogCaptureFixture) -> None:
    async def run() -> None:
        adapter, ws, _ = await _connected()
        await adapter.subscribe([("000660", TRADE)])
        raw = _trade(ts="2026-11-17T09:45:24.821+09:00")
        ws.push("not-json")
        ws.push('{"type":"message","topic":"personal:order","data":{"x":1}}')
        ws.push(raw)
        frame = await adapter.recv()
        assert frame.raw == raw
        assert (frame.vendor, frame.stream, frame.symbol) == ("toss", TRADE, "000660")
        assert frame.venue is MarketVenue.UNIFIED
        assert frame.session is MarketSession.REGULAR
        assert frame.exchange_event_time == "094524"
        assert frame.conn_seq == 1
        assert adapter.unparsable_frames == 2

    with caplog.at_level(logging.DEBUG):
        asyncio.run(run())
    assert TOKEN not in caplog.text


def test_book_frame_stream_is_logical_book_id() -> None:
    async def run() -> None:
        adapter, ws, _ = await _connected()
        ws.push('{"type":"message","topic":"orderbook:kr:005930","data":{"timestamp":"2026-11-17T15:19:59.999+09:00","asks":[],"bids":[]}}')
        frame = await adapter.recv()
        assert frame.stream == BOOK
        assert frame.exchange_event_time == "151959"

    asyncio.run(run())


def test_connect_uses_store_token_only_and_never_logs_it(caplog: pytest.LogCaptureFixture) -> None:
    async def run() -> FakeStore:
        adapter, http, store = _adapter()
        await adapter.connect()
        assert http.headers == {"Authorization": f"Bearer {TOKEN}"}
        return store

    with caplog.at_level(logging.DEBUG):
        store = asyncio.run(run())
    assert store.calls == ["aget_or_issue"]
    assert TOKEN not in caplog.text


def test_handshake_401_replaces_token_once_then_auth_rejected() -> None:
    async def run() -> None:
        error = aiohttp.WSServerHandshakeError(None, (), status=401, message="unauthorized")  # type: ignore[arg-type]
        adapter, _, store = _adapter(http=FakeHTTP(error=error))
        with pytest.raises(VendorAuthRejected, match="auth_rejected:401"):
            await adapter.connect()
        assert store.calls == ["aget_or_issue", "areplace_rejected"]

    asyncio.run(run())


def test_invalid_token_envelope_replaces_token_once_then_auth_rejected() -> None:
    async def run() -> None:
        adapter, ws, store = await _connected()
        ws.push('{"type":"error","error":{"code":"invalid-token"}}')
        with pytest.raises(VendorAuthRejected):
            await adapter.recv()
        assert store.calls == ["aget_or_issue", "areplace_rejected"]

    asyncio.run(run())


def test_server_close_maps_to_vendor_disconnected_with_code() -> None:
    async def run() -> None:
        adapter, ws, _ = await _connected()
        ws.inbox.put_nowait(SimpleNamespace(type=aiohttp.WSMsgType.CLOSE, data=1008))
        with pytest.raises(VendorDisconnected, match="close:1008"):
            await adapter.recv()
        await adapter.aclose()
        await adapter.aclose()
        assert ws.closed

    asyncio.run(run())


def test_real_store_without_shared_token_never_issues(tmp_path) -> None:
    store = TossTokenStore(toss_token_path(tmp_path, "app-key"))
    adapter = TossRealtimeAdapter(http=FakeHTTP(), token_store=store)
    with pytest.raises(VendorDisconnected, match="token_unavailable"):
        asyncio.run(adapter.connect())
    assert store.read() is None


def test_real_store_with_shared_token_is_consumed(tmp_path) -> None:
    from src.marketdata.toss_token_store import IssuedToken

    store = TossTokenStore(toss_token_path(tmp_path, "app-key"))

    async def run() -> FakeHTTP:
        async def issue() -> IssuedToken:
            return IssuedToken(TOKEN, 3600.0)

        await store.aget_or_issue(issue)  # stands in for kca, the sole issuer
        http = FakeHTTP()
        await TossRealtimeAdapter(http=http, token_store=store).connect()
        return http

    assert asyncio.run(run()).headers["Authorization"] == f"Bearer {TOKEN}"


def test_real_sample_frames_parse() -> None:
    """Shapes captured live 2026-11-17 (market data only)."""

    async def run() -> None:
        adapter, ws, _ = await _connected()
        ws.auto_ack = False
        ws.push('{"type":"subscriptions","subscribed":["trade:kr:000660","trade:kr:005930"],"rejected":[]}')
        acks = await adapter.subscribe([("000660", TRADE), ("005930", TRADE)])
        assert all(a.accepted for a in acks)
        ws.push('{"type":"message","topic":"trade:kr:000660","data":{"price":"1108000","volume":"1","timestamp":"2026-11-17T09:45:24.821+09:00","currency":"KRW"}}')
        assert (await adapter.recv()).exchange_event_time == "094524"

    asyncio.run(run())
