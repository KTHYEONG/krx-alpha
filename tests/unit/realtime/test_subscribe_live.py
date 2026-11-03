from __future__ import annotations

import asyncio
import json
import logging

import aiohttp
import pytest

from src.marketdata.toss_token_store import TossTokenStore, ls_token_path
from src.realtime.adapters.kis import KisRealtimeAdapter
from src.realtime.adapters.ls import LsRealtimeAdapter
from src.realtime.connection_handover import Handover, SwapRequest
from src.realtime.contracts import MarketSession, MarketVenue, VendorAck
from src.realtime.session import StreamRoute

_TR_CNT = "H0STCNT0"
_TR_ASP = "H0STASP0"
_MARKET = {"005930": "KOSPI", "000660": "KOSPI", "035420": "KOSPI", "051910": "KOSPI"}


class _Msg:
    def __init__(self, type_, data=None) -> None:
        self.type = type_
        self.data = data


class _QueueWS:
    """Socket fake whose inbound frames are queued by the test; recv and subscribe share one reader."""

    def __init__(self) -> None:
        self.inbox: asyncio.Queue[_Msg] = asyncio.Queue()
        self.sent: list[str] = []

    async def send_str(self, text: str) -> None:
        self.sent.append(text)

    async def receive(self) -> _Msg:
        return await self.inbox.get()

    async def receive_str(self) -> str:
        return (await self.inbox.get()).data

    async def pong(self) -> None:
        return None

    def push_text(self, text: str) -> None:
        self.inbox.put_nowait(_Msg(aiohttp.WSMsgType.TEXT, text))


def _ls_data(symbol: str, tr_cd: str = "S3_") -> str:
    return json.dumps({"header": {"tr_cd": tr_cd, "tr_key": symbol}, "body": {"price": "70000"}})


def _ls_ack(code: str = "00000") -> str:
    return json.dumps({"header": {"tr_cd": "S3_", "rsp_cd": code, "rsp_msg": "m"}, "body": None})


def _ls(tmp_path, ws: _QueueWS, **kwargs) -> LsRealtimeAdapter:
    adapter = LsRealtimeAdapter(
        app_key="k",
        app_secret="s",
        http=object(),
        market_of=_MARKET,
        token_store=TossTokenStore(ls_token_path(tmp_path, "k")),
        **kwargs,
    )
    adapter._ws = ws
    adapter._token = "TOK"
    adapter._conn_id = "ls-live"
    return adapter


def _kis(ws: _QueueWS, *, capacity: int = 4, ack_timeout_s: float = 2.0) -> KisRealtimeAdapter:
    adapter = KisRealtimeAdapter(
        app_key="k",
        app_secret="s",
        http=object(),
        route=StreamRoute(MarketVenue.KRX, MarketSession.REGULAR),
        allowed_streams=(_TR_CNT, _TR_ASP),
        capacity_pairs=capacity,
        ack_timeout_s=ack_timeout_s,
    )
    adapter._ws = ws
    adapter._approval_key = "APPROVAL"
    adapter._conn_id = "kis-live"
    return adapter


def _kis_ack(symbol: str, stream: str, code: str = "0") -> str:
    return json.dumps(
        {"header": {"tr_id": stream, "tr_key": symbol}, "body": {"rt_cd": code, "msg_cd": "OPSP0000"}}
    )


def test_data_keeps_flowing_during_live_subscribe(tmp_path) -> None:
    async def scenario() -> tuple[list[str], list[VendorAck]]:
        ws = _QueueWS()
        adapter = _ls(tmp_path, ws, capacity_pairs=10, ack_timeout_s=2.0)
        ws.push_text(_ls_data("000660"))
        ws.push_text(_ls_data("035420"))
        ws.push_text(_ls_ack("00000"))
        ws.push_text(_ls_data("000660"))
        live = asyncio.ensure_future(adapter.subscribe_live([("051910", _TR_CNT)]))
        await asyncio.sleep(0)
        frames = [(await adapter.recv()).symbol for _ in range(3)]
        acks = await live
        assert adapter.subscribed_pairs() == (("051910", _TR_CNT),)
        return frames, acks

    frames, acks = asyncio.run(scenario())

    assert frames == ["000660", "035420", "000660"]
    assert acks == [VendorAck("051910", _TR_CNT, True, "00000")]


def test_live_timeout_is_non_fatal_and_late_ack_is_ignored(tmp_path, caplog) -> None:
    caplog.set_level(logging.INFO)

    async def scenario() -> tuple[list[VendorAck], str]:
        ws = _QueueWS()
        adapter = _ls(tmp_path, ws, capacity_pairs=10, ack_timeout_s=0.05)
        acks = await adapter.subscribe_live([("005930", _TR_CNT)])
        ws.push_text(_ls_ack("00000"))
        ws.push_text(_ls_data("005930"))
        frame = await adapter.recv()
        return acks, frame.symbol

    acks, symbol = asyncio.run(scenario())

    assert acks == [VendorAck("005930", _TR_CNT, False, "timeout")]
    assert symbol == "005930"
    assert "LATE_ACK_IGNORED" in caplog.text


def test_cumulative_capacity_rejects_before_sending(tmp_path) -> None:
    async def scenario() -> _QueueWS:
        ws = _QueueWS()
        adapter = _ls(tmp_path, ws, capacity_pairs=82, ack_timeout_s=0.05)
        adapter._subscribed = [("000001", _TR_CNT)] * 80
        with pytest.raises(ValueError, match="exceeds capacity"):
            await adapter.subscribe_live([("005930", _TR_CNT), ("005930", _TR_ASP), ("000660", _TR_CNT), ("000660", _TR_ASP)])
        return ws

    ws = asyncio.run(scenario())

    assert ws.sent == []


def test_kis_live_subscribe_routes_ack_through_recv(tmp_path) -> None:
    async def scenario() -> tuple[list[VendorAck], list[str]]:
        ws = _QueueWS()
        adapter = _kis(ws, capacity=4)
        ws.push_text("0|H0STCNT0|1|005930^093000^1^2^3")
        ws.push_text(_kis_ack("005930", _TR_CNT))
        ws.push_text("0|H0STCNT0|1|005930^093001^1^2^3")
        live = asyncio.ensure_future(adapter.subscribe_live([("005930", _TR_CNT)]))
        await asyncio.sleep(0)
        frames = [(await adapter.recv()).exchange_event_time for _ in range(2)]
        acks = await live
        assert adapter.subscribed_pairs() == (("005930", _TR_CNT),)
        return acks, frames

    acks, frames = asyncio.run(scenario())

    assert acks == [VendorAck("005930", _TR_CNT, True, "0")]
    assert frames == ["093000", "093001"]


def test_standby_subscribes_current_pairs_including_dynamic(tmp_path) -> None:
    dynamic: list[tuple[str, str]] = []
    static = [("005930", _TR_CNT), ("005930", _TR_ASP)]
    subscribed: list[list[tuple[str, str]]] = []

    class _Standby:
        name = "ls"
        capacity_pairs = 200

        async def connect(self) -> None:
            return None

        async def subscribe(self, pairs):
            subscribed.append(list(pairs))
            return [VendorAck(s, t, True, "00000") for s, t in pairs]

        async def recv(self):
            raise AssertionError("standby is never received from in this test")

        async def aclose(self) -> None:
            return None

    handover = Handover(
        adapter_factory=lambda: _Standby(),
        pairs=lambda: [*static, *dynamic],
        request=SwapRequest(),
        max_s=1.0,
        cooldown_s=0.0,
    )
    dynamic.extend([("000660", _TR_CNT), ("000660", _TR_ASP), ("035420", _TR_CNT)])

    asyncio.run(handover._open())

    assert subscribed == [[*static, *dynamic]]


def test_live_spacing_is_applied_between_pairs(tmp_path) -> None:
    async def scenario() -> list[VendorAck]:
        ws = _QueueWS()
        adapter = _ls(tmp_path, ws, capacity_pairs=10, ack_timeout_s=0.05)
        adapter._subscribe_spacing_s = 0.0
        return await adapter.subscribe_live([("005930", _TR_CNT), ("000660", _TR_CNT)])

    acks = asyncio.run(scenario())

    assert [a.code for a in acks] == ["timeout", "timeout"]


def test_live_send_failure_is_disconnect(tmp_path) -> None:
    from src.realtime.contracts import VendorDisconnected

    class _BrokenWS(_QueueWS):
        async def send_str(self, text: str) -> None:
            raise OSError("broken pipe")

    async def scenario() -> None:
        adapter = _ls(tmp_path, _BrokenWS(), capacity_pairs=10, ack_timeout_s=0.05)

        async def _no_replace(token: str) -> None:
            return None

        adapter._replace_rejected_token = _no_replace  # type: ignore[method-assign]
        await adapter.subscribe_live([("005930", _TR_CNT)])

    with pytest.raises(VendorDisconnected):
        asyncio.run(scenario())


def test_live_all_rejected_answers_raise_auth_rejected(tmp_path) -> None:
    from src.realtime.contracts import VendorAuthRejected

    async def scenario() -> None:
        ws = _QueueWS()
        adapter = _ls(tmp_path, ws, capacity_pairs=10, ack_timeout_s=2.0)

        async def _no_replace(token: str) -> None:
            return None

        adapter._replace_rejected_token = _no_replace  # type: ignore[method-assign]
        ws.push_text(_ls_ack("10011"))
        live = asyncio.ensure_future(adapter.subscribe_live([("005930", _TR_CNT)]))
        await asyncio.sleep(0)
        ws.push_text(_ls_data("005930"))
        await adapter.recv()
        await live

    with pytest.raises(VendorAuthRejected, match="all_acks_rejected"):
        asyncio.run(scenario())


def test_kis_live_capacity_route_and_send_failures(tmp_path) -> None:
    from src.realtime.contracts import VendorDisconnected

    async def capacity() -> None:
        adapter = _kis(_QueueWS(), capacity=2)
        adapter._subscribed = [("000001", _TR_CNT)] * 2
        await adapter.subscribe_live([("005930", _TR_CNT)])

    async def route() -> None:
        adapter = _kis(_QueueWS(), capacity=4)
        await adapter.subscribe_live([("005930", "H0NXCNT0")])

    class _BrokenWS(_QueueWS):
        async def send_str(self, text: str) -> None:
            raise OSError("reset")

    async def send_failure() -> None:
        adapter = _kis(_BrokenWS(), capacity=4)
        await adapter.subscribe_live([("005930", _TR_CNT)])

    with pytest.raises(ValueError, match="exceeds capacity"):
        asyncio.run(capacity())
    with pytest.raises(VendorDisconnected, match="route_mismatch"):
        asyncio.run(route())
    with pytest.raises(VendorDisconnected, match="subscribe_failed"):
        asyncio.run(send_failure())


def test_kis_live_timeout_and_late_ack_are_non_fatal(tmp_path, caplog) -> None:
    caplog.set_level(logging.INFO)

    async def scenario() -> tuple[list[VendorAck], str]:
        ws = _QueueWS()
        adapter = _kis(ws, capacity=4, ack_timeout_s=0.05)
        acks = await adapter.subscribe_live([("005930", _TR_CNT)])
        ws.push_text(_kis_ack("005930", _TR_CNT))
        ws.push_text("0|H0STCNT0|1|005930^093005^1^2^3")
        frame = await adapter.recv()
        return acks, frame.exchange_event_time

    acks, event_time = asyncio.run(scenario())

    assert acks == [VendorAck("005930", _TR_CNT, False, "timeout")]
    assert event_time == "093005"
    assert "status=TIMEOUT" in caplog.text
    assert "LATE_ACK_IGNORED" in caplog.text


def test_kis_stale_subscribe_response_is_consumed_while_waiting(tmp_path, caplog) -> None:
    caplog.set_level(logging.DEBUG)

    async def scenario() -> list[VendorAck]:
        ws = _QueueWS()
        adapter = _kis(ws, capacity=4, ack_timeout_s=2.0)
        ws.push_text(_kis_ack("000660", _TR_CNT))
        ws.push_text(_kis_ack("005930", _TR_CNT))
        live = asyncio.ensure_future(adapter.subscribe_live([("005930", _TR_CNT)]))
        await asyncio.sleep(0)
        ws.push_text("0|H0STCNT0|1|005930^093010^1^2^3")
        await adapter.recv()
        return await live

    acks = asyncio.run(scenario())

    assert acks == [VendorAck("005930", _TR_CNT, True, "0")]
    assert "MISMATCH_IGNORED" in caplog.text


def test_lease_probe_reports_held_lock(tmp_path) -> None:
    import fcntl

    from src.realtime.kis_lease import KisWebSocketLease

    lease = KisWebSocketLease(root=tmp_path, credential_key_id="probe")
    assert lease.is_free() is True
    with open(tmp_path / "probe.lock", "a+b") as holder:
        fcntl.flock(holder.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert lease.is_free() is False
