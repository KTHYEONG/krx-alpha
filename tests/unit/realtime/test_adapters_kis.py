

def test_kis_adapter_emits_one_frame_per_caret_row():
    import asyncio
    from src.realtime.adapters.kis import KisRealtimeAdapter
    from src.realtime.contracts import MarketSession, MarketVenue
    from src.realtime.session import StreamRoute
    class Ws:
        async def receive_str(self): return '0|H0NXCNT0|002|005930^154001^a^b^005930^154002^c^d'
        async def close(self): return None
    adapter = KisRealtimeAdapter(app_key='k', app_secret='s', http=object(), route=StreamRoute(MarketVenue.NXT, MarketSession.NXT_AFTER), allowed_streams=('H0NXCNT0', 'H0NXASP0'), capacity_pairs=4)
    adapter._ws = Ws()
    first = asyncio.run(adapter.recv())
    second = asyncio.run(adapter.recv())
    assert (first.venue, first.session, first.raw, first.exchange_event_time, first.conn_seq) == (MarketVenue.NXT, MarketSession.NXT_AFTER, '005930^154001^a^b', '154001', 1)
    assert (second.raw, second.exchange_event_time, second.conn_seq) == ('005930^154002^c^d', '154002', 2)


def test_kis_adapter_rejects_route_mismatched_tr_id():
    import pytest
    from src.realtime.adapters.kis import KisRealtimeAdapter
    from src.realtime.contracts import MarketSession, MarketVenue, VendorDisconnected
    from src.realtime.session import StreamRoute
    with pytest.raises(VendorDisconnected, match='H0STCNT0'):
        KisRealtimeAdapter(app_key='k', app_secret='s', http=object(), route=StreamRoute(MarketVenue.NXT, MarketSession.NXT_AFTER), allowed_streams=('H0STCNT0', 'H0STASP0'), capacity_pairs=4)


def test_kis_adapter_connects_without_context_manager_wrapper():
    import asyncio

    from src.realtime.adapters.kis import KisRealtimeAdapter
    from src.realtime.contracts import MarketSession, MarketVenue
    from src.realtime.session import StreamRoute

    class Response:
        status = 200

        async def json(self): return {"approval_key": "approved"}

        async def __aenter__(self): return self

        async def __aexit__(self, *args): return None

    class Http:
        def __init__(self): self.ws = object()

        def post(self, *args, **kwargs): return Response()

        async def ws_connect(self, *args): return self.ws

    http = Http()
    adapter = KisRealtimeAdapter(app_key='k', app_secret='s', http=http, route=StreamRoute(MarketVenue.NXT, MarketSession.NXT_AFTER), allowed_streams=('H0NXCNT0', 'H0NXASP0'), capacity_pairs=4)
    asyncio.run(adapter.connect())
    assert adapter._ws is http.ws


def test_kis_adapter_rejects_zero_count_envelope():
    import pytest

    from src.realtime.adapters.kis import KisRealtimeAdapter
    from src.realtime.contracts import MarketSession, MarketVenue, VendorDisconnected
    from src.realtime.session import StreamRoute

    adapter = KisRealtimeAdapter(app_key='k', app_secret='s', http=object(), route=StreamRoute(MarketVenue.NXT, MarketSession.NXT_AFTER), allowed_streams=('H0NXCNT0', 'H0NXASP0'), capacity_pairs=4)
    with pytest.raises(VendorDisconnected, match='malformed_count'):
        adapter._parse_envelope('0|H0NXCNT0|000|005930^154001')


def test_kis_adapter_parses_vendor_ack_body():
    from src.realtime.adapters.kis import KisRealtimeAdapter
    from src.realtime.contracts import MarketSession, MarketVenue
    from src.realtime.session import StreamRoute

    adapter = KisRealtimeAdapter(app_key='k', app_secret='s', http=object(), route=StreamRoute(MarketVenue.NXT, MarketSession.NXT_AFTER), allowed_streams=('H0NXCNT0', 'H0NXASP0'), capacity_pairs=4)
    ack = adapter._parse_ack('{"header":{"tr_id":"H0NXCNT0"},"body":{"rt_cd":"0","msg_cd":"OPSP0000"}}', '005930', 'H0NXCNT0')
    assert (ack.symbol, ack.stream, ack.accepted, ack.code) == ('005930', 'H0NXCNT0', True, '0')


def test_kis_adapter_rejects_non_integer_and_misaligned_envelopes():
    import pytest

    from src.realtime.adapters.kis import KisRealtimeAdapter
    from src.realtime.contracts import MarketSession, MarketVenue, VendorDisconnected
    from src.realtime.session import StreamRoute

    adapter = KisRealtimeAdapter(app_key='k', app_secret='s', http=object(), route=StreamRoute(MarketVenue.NXT, MarketSession.NXT_AFTER), allowed_streams=('H0NXCNT0', 'H0NXASP0'), capacity_pairs=4)
    with pytest.raises(VendorDisconnected, match='malformed_count'):
        adapter._parse_envelope('0|H0NXCNT0|bad|005930^154001')
    with pytest.raises(VendorDisconnected, match='malformed_rows'):
        adapter._parse_envelope('0|H0NXCNT0|002|005930^154001^x')
    with pytest.raises(VendorDisconnected, match='malformed_rows'):
        adapter._parse_envelope('0|H0NXCNT0|001|005930')


def test_kis_adapter_subscribe_skips_interleaved_ticks_and_pingpong():
    import asyncio
    import json

    from src.realtime.adapters.kis import KisRealtimeAdapter
    from src.realtime.contracts import MarketSession, MarketVenue
    from src.realtime.session import StreamRoute

    sent = []

    class Ws:
        def __init__(self):
            self._replies = iter([
                '0|H0NXCNT0|001|005930^154001^a^b',
                json.dumps({"header": {"tr_id": "PINGPONG"}}),
                '{"header":{"tr_id":"H0NXCNT0"},"body":{"rt_cd":"0","msg_cd":"OPSP0000"}}',
            ])

        async def send_str(self, payload):
            sent.append(payload)

        async def receive_str(self):
            return next(self._replies)

    adapter = KisRealtimeAdapter(app_key='k', app_secret='s', http=object(), route=StreamRoute(MarketVenue.NXT, MarketSession.NXT_AFTER), allowed_streams=('H0NXCNT0', 'H0NXASP0'), capacity_pairs=4)
    adapter._ws = Ws()
    acks = asyncio.run(adapter.subscribe([('005930', 'H0NXCNT0')]))

    assert (acks[0].symbol, acks[0].stream, acks[0].accepted) == ('005930', 'H0NXCNT0', True)
    assert len(adapter._pending) == 1
    assert adapter._pending[0].raw == '005930^154001^a^b'
    # PINGPONG 프레임은 그대로 에코 응답되어야 한다(마지막 send가 pong).
    assert json.loads(sent[-1])['header']['tr_id'] == 'PINGPONG'


def test_kis_websocket_lease_exclusive_and_adapter_lifecycle(tmp_path) -> None:
    import asyncio
    import pytest
    from src.realtime.contracts import VendorAuthRejected
    from src.realtime.kis_lease import KisWebSocketLease
    async def run() -> None:
        one = KisWebSocketLease(root=tmp_path, credential_key_id='id')
        two = KisWebSocketLease(root=tmp_path, credential_key_id='id')
        await one.acquire()
        with pytest.raises(VendorAuthRejected, match='key_lease_busy'):
            await two.acquire()
        await one.release()
        await two.acquire()
        await two.release()
    asyncio.run(run())


def test_kis_adapter_acquires_lease_before_approval_and_releases_on_close(tmp_path) -> None:
    import asyncio

    import pytest
    from src.realtime.adapters.kis import KisRealtimeAdapter
    from src.realtime.contracts import MarketSession, MarketVenue, VendorAuthRejected
    from src.realtime.kis_lease import KisWebSocketLease
    from src.realtime.session import StreamRoute

    posted = []

    class Response:
        status = 200

        async def json(self):
            return {"approval_key": "approved"}

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

    class Ws:
        async def close(self):
            return None

    class Http:
        def __init__(self):
            self.ws = Ws()

        def post(self, *args, **kwargs):
            posted.append(kwargs)
            return Response()

        async def ws_connect(self, *args):
            return self.ws

        async def close(self):
            return None

    async def run() -> None:
        route = StreamRoute(MarketVenue.NXT, MarketSession.NXT_AFTER)
        held = KisWebSocketLease(root=tmp_path, credential_key_id="busy")
        await held.acquire()
        blocked = KisRealtimeAdapter(app_key="k", app_secret="s", http=Http(), route=route, allowed_streams=("H0NXCNT0", "H0NXASP0"), capacity_pairs=4, lease=KisWebSocketLease(root=tmp_path, credential_key_id="busy"))
        with pytest.raises(VendorAuthRejected, match="key_lease_busy"):
            await blocked.connect()
        assert posted == []
        await held.release()
        http = Http()
        adapter = KisRealtimeAdapter(app_key="k", app_secret="s", http=http, route=route, allowed_streams=("H0NXCNT0", "H0NXASP0"), capacity_pairs=4, lease=KisWebSocketLease(root=tmp_path, credential_key_id="busy"))
        await adapter.connect()
        assert len(posted) == 1
        await adapter.aclose()
        follower = KisWebSocketLease(root=tmp_path, credential_key_id="busy")
        await follower.acquire()
        await follower.release()

    asyncio.run(run())


def _nxt_adapter(ws) -> object:
    from src.realtime.adapters.kis import KisRealtimeAdapter
    from src.realtime.contracts import MarketSession, MarketVenue
    from src.realtime.session import StreamRoute

    adapter = KisRealtimeAdapter(app_key='k', app_secret='s', http=object(), route=StreamRoute(MarketVenue.NXT, MarketSession.NXT_AFTER), allowed_streams=('H0NXCNT0', 'H0NXASP0'), capacity_pairs=4)
    adapter._ws = ws
    return adapter


def test_recv_echoes_pingpong_then_returns_data() -> None:
    import asyncio
    import json

    pingpong_raw = json.dumps({"header": {"tr_id": "PINGPONG"}})

    class Ws:
        def __init__(self):
            self.sent: list[str] = []
            self._replies = iter([pingpong_raw, '0|H0NXCNT0|001|005930^154001^a^b'])

        async def send_str(self, payload):
            self.sent.append(payload)

        async def receive_str(self):
            return next(self._replies)

    ws = Ws()
    adapter = _nxt_adapter(ws)
    frame = asyncio.run(adapter.recv())

    assert frame.conn_seq == 1
    assert ws.sent == [pingpong_raw]
    assert adapter._pending == []


def test_recv_consumes_consecutive_pingpongs() -> None:
    import asyncio
    import json

    pingpong_raw = json.dumps({"header": {"tr_id": "PINGPONG"}})

    class Ws:
        def __init__(self):
            self.sent: list[str] = []
            self._replies = iter([pingpong_raw, pingpong_raw, pingpong_raw, '0|H0NXCNT0|001|005930^154001^a^b'])

        async def send_str(self, payload):
            self.sent.append(payload)

        async def receive_str(self):
            return next(self._replies)

    ws = Ws()
    adapter = _nxt_adapter(ws)
    frame = asyncio.run(adapter.recv())

    assert ws.sent == [pingpong_raw] * 3
    assert frame.conn_seq == 1


def test_recv_still_fails_closed_on_unknown_json() -> None:
    import asyncio

    import pytest

    from src.realtime.contracts import VendorDisconnected

    class Ws:
        async def receive_str(self):
            return '{"header":{"tr_id":"H0NXCNT0"},"body":{"rt_cd":"0"}}'

        async def send_str(self, payload):
            raise AssertionError("nothing sent")

    adapter = _nxt_adapter(Ws())
    with pytest.raises(VendorDisconnected, match="unknown_frame"):
        asyncio.run(adapter.recv())


def test_recv_maps_server_close_to_vendor_disconnected() -> None:
    import asyncio

    import aiohttp
    import pytest

    from src.realtime.contracts import VendorDisconnected

    class Ws:
        async def receive_str(self):
            raise aiohttp.WSMessageTypeError("received message of type WSMsgType.CLOSE")

    adapter = _nxt_adapter(Ws())
    with pytest.raises(VendorDisconnected, match=r"ws_non_text_frame:received message of type WSMsgType\.CLOSE"):
        asyncio.run(adapter.recv())


def test_recv_does_not_mask_unrelated_type_errors() -> None:
    import asyncio

    import pytest

    class Ws:
        async def receive_str(self):
            raise TypeError("programming error")

    adapter = _nxt_adapter(Ws())
    with pytest.raises(TypeError, match="programming error"):
        asyncio.run(adapter.recv())


def test_recv_fails_closed_on_non_json_and_non_object() -> None:
    import asyncio

    import pytest

    from src.realtime.adapters.kis import _is_pingpong
    from src.realtime.contracts import VendorDisconnected

    assert _is_pingpong("hello") is False
    assert _is_pingpong("[1]") is False
    assert _is_pingpong('{"header":"x"}') is False

    for raw in ("hello", "[1]"):
        class Ws:
            def __init__(self, payload):
                self._payload = payload

            async def receive_str(self):
                return self._payload

        adapter = _nxt_adapter(Ws(raw))
        with pytest.raises(VendorDisconnected, match="unknown_frame"):
            asyncio.run(adapter.recv())


def test_echo_failure_becomes_disconnect() -> None:
    import asyncio
    import json

    import pytest

    from src.realtime.contracts import VendorDisconnected

    pingpong_raw = json.dumps({"header": {"tr_id": "PINGPONG"}})

    class Ws:
        async def receive_str(self):
            return pingpong_raw

        async def send_str(self, payload):
            raise ConnectionResetError("reset")

    adapter = _nxt_adapter(Ws())
    with pytest.raises(VendorDisconnected, match="pingpong_echo_failed:ConnectionResetError") as excinfo:
        asyncio.run(adapter.recv())
    assert isinstance(excinfo.value.__cause__, ConnectionResetError)


def test_pingpong_telemetry_counts_receipts_before_echo(caplog) -> None:
    # 하트비트 수신 사실은 echo 성공 여부와 무관하게 관측 가능해야 한다.
    import asyncio
    import json
    import logging
    import time

    from src.realtime.contracts import HeartbeatProbe

    pingpong_raw = json.dumps({"header": {"tr_id": "PINGPONG"}})

    class Ws:
        def __init__(self):
            self._replies = iter([pingpong_raw, pingpong_raw, '0|H0NXCNT0|001|005930^154001^a^b'])

        async def send_str(self, payload):
            return None

        async def receive_str(self):
            return next(self._replies)

    with caplog.at_level(logging.DEBUG):
        before_ns = time.time_ns()
        adapter = _nxt_adapter(Ws())
        assert isinstance(adapter, HeartbeatProbe)
        frame = asyncio.run(adapter.recv())
        after_ns = time.time_ns()

    assert adapter.pingpong_count == 2
    assert before_ns <= adapter.last_pingpong_wall_ns <= after_ns
    assert frame.conn_seq == 1

    heartbeats = [r for r in caplog.records if "stage=kis_pingpong" in r.getMessage()]
    assert len(heartbeats) == 2
    assert {r.levelno for r in heartbeats} == {logging.DEBUG}
    assert {r.getMessage().split("count=")[-1] for r in heartbeats} == {"1", "2"}
    assert all(pingpong_raw not in r.getMessage() for r in caplog.records)


def test_pingpong_telemetry_counts_failed_echo() -> None:
    import asyncio
    import json

    import pytest

    from src.realtime.contracts import VendorDisconnected

    pingpong_raw = json.dumps({"header": {"tr_id": "PINGPONG"}})

    class Ws:
        async def receive_str(self):
            return pingpong_raw

        async def send_str(self, payload):
            raise OSError("reset")

    adapter = _nxt_adapter(Ws())
    with pytest.raises(VendorDisconnected, match="pingpong_echo_failed"):
        asyncio.run(adapter.recv())
    assert adapter.pingpong_count == 1


def test_kis_adapter_uses_nxt_route_for_probe_assertion() -> None:
    from src.realtime.adapters.kis import KisRealtimeAdapter
    from src.realtime.contracts import HeartbeatProbe, MarketSession, MarketVenue
    from src.realtime.session import StreamRoute

    adapter = KisRealtimeAdapter(
        app_key="k", app_secret="s", http=object(),
        route=StreamRoute(MarketVenue.NXT, MarketSession.NXT_AFTER),
        allowed_streams=("H0NXCNT0", "H0NXASP0"), capacity_pairs=4,
    )

    assert isinstance(adapter, HeartbeatProbe)
    assert (adapter.pingpong_count, adapter.last_pingpong_wall_ns) == (0, None)


def _kis_connect_adapter(http) -> object:
    from src.realtime.adapters.kis import KisRealtimeAdapter
    from src.realtime.contracts import MarketSession, MarketVenue
    from src.realtime.session import StreamRoute

    return KisRealtimeAdapter(app_key="k", app_secret="s", http=http, route=StreamRoute(MarketVenue.NXT, MarketSession.NXT_AFTER), allowed_streams=("H0NXCNT0", "H0NXASP0"), capacity_pairs=4)


def test_kis_connect_approval_post_network_error_disconnects() -> None:
    import asyncio

    import aiohttp
    import pytest

    from src.realtime.contracts import VendorDisconnected

    class Http:
        def post(self, *a, **k):
            raise aiohttp.ClientConnectorError(None, OSError("dns"))

        async def ws_connect(self, *a):
            raise AssertionError("ws must not open")

    with pytest.raises(VendorDisconnected, match="connect_failed:ClientConnectorError"):
        asyncio.run(_kis_connect_adapter(Http()).connect())


def test_kis_connect_ws_failure_disconnects_and_aclose_safe(tmp_path) -> None:
    import asyncio

    import aiohttp
    import pytest

    from src.realtime.contracts import VendorDisconnected
    from src.realtime.kis_lease import KisWebSocketLease

    class Response:
        status = 200

        async def json(self):
            return {"approval_key": "approved"}

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

    class Http:
        def post(self, *a, **k):
            return Response()

        async def ws_connect(self, *a):
            raise aiohttp.WSServerHandshakeError(None, (), status=503)

    adapter = _kis_connect_adapter(Http())
    adapter._lease = KisWebSocketLease(root=tmp_path, credential_key_id="failed-connect")
    with pytest.raises(VendorDisconnected, match="connect_failed:WSServerHandshakeError"):
        asyncio.run(adapter.connect())
    asyncio.run(adapter.aclose())

    async def acquire_follower() -> None:
        follower = KisWebSocketLease(root=tmp_path, credential_key_id="failed-connect")
        await follower.acquire()
        await follower.release()

    asyncio.run(acquire_follower())


def test_kis_connect_approval_json_decode_error_disconnects() -> None:
    import asyncio

    import pytest

    from src.realtime.contracts import VendorDisconnected

    class Response:
        status = 200

        async def json(self):
            raise ValueError("Expecting value")

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

    class Http:
        def post(self, *a, **k):
            return Response()

        async def ws_connect(self, *a):
            raise AssertionError("ws must not open")

    with pytest.raises(VendorDisconnected, match="connect_failed:ValueError"):
        asyncio.run(_kis_connect_adapter(Http()).connect())


def test_kis_connect_auth_rejection_unchanged() -> None:
    import asyncio

    import pytest

    from src.realtime.contracts import VendorAuthRejected

    class Response:
        def __init__(self, status, payload):
            self.status = status
            self._payload = payload

        async def json(self):
            return self._payload

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

    class Http:
        def __init__(self, resp):
            self._resp = resp

        def post(self, *a, **k):
            return self._resp

        async def ws_connect(self, *a):
            raise AssertionError("ws must not open after auth rejection")

    with pytest.raises(VendorAuthRejected, match="auth_rejected:401"):
        asyncio.run(_kis_connect_adapter(Http(Response(401, {}))).connect())
    with pytest.raises(VendorAuthRejected, match="no_approval_key"):
        asyncio.run(_kis_connect_adapter(Http(Response(200, {}))).connect())


def test_kis_close_failure_releases_lease_and_allows_reconnect(tmp_path) -> None:
    import asyncio
    from unittest.mock import AsyncMock

    from src.realtime.kis_lease import KisWebSocketLease

    async def run():
        lease = KisWebSocketLease(root=tmp_path, credential_key_id="close-failure")
        adapter = _kis_connect_adapter(object())
        adapter._lease = lease
        await lease.acquire()
        ws = type("Ws", (), {"close": AsyncMock(side_effect=OSError("reset"))})()
        adapter._ws = ws
        await adapter.aclose()
        await adapter.aclose()
        ws.close.assert_awaited_once()
        assert adapter._ws is None
        await lease.acquire()
        await adapter.aclose()

    asyncio.run(run())


def test_kis_close_cancellation_still_releases_lease(tmp_path) -> None:
    import asyncio
    from unittest.mock import AsyncMock

    import pytest

    from src.realtime.kis_lease import KisWebSocketLease

    async def run():
        adapter = _kis_connect_adapter(object())
        lease = KisWebSocketLease(root=tmp_path, credential_key_id="cancel-close")
        adapter._lease = lease
        await lease.acquire()
        adapter._ws = type("Ws", (), {"close": AsyncMock(side_effect=asyncio.CancelledError)})()
        with pytest.raises(asyncio.CancelledError):
            await adapter.aclose()
        follower = KisWebSocketLease(root=tmp_path, credential_key_id="cancel-close")
        await follower.acquire()
        await follower.release()

    asyncio.run(run())


def _ack_raw(code="0", msg="OPSP0000"):
    import json
    return json.dumps({"header": {"tr_id": "H0NXCNT0"}, "body": {"rt_cd": code, "msg_cd": msg}})


def _timeout_adapter(ws, **kwargs):
    from src.realtime.adapters.kis import KisRealtimeAdapter
    from src.realtime.contracts import MarketSession, MarketVenue
    from src.realtime.session import StreamRoute

    kwargs.setdefault("ack_timeout_s", 10.0)
    adapter = KisRealtimeAdapter(
        app_key="k", app_secret="s", http=object(),
        route=StreamRoute(MarketVenue.NXT, MarketSession.NXT_AFTER),
        allowed_streams=("H0NXCNT0", "H0NXASP0"), capacity_pairs=4, **kwargs,
    )
    adapter._ws = ws
    return adapter


def test_kis_subscribe_returns_acks_in_send_order():
    import asyncio
    import json

    sent = []

    class Ws:
        def __init__(self):
            self._replies = iter([_ack_raw(), _ack_raw()])

        async def send_str(self, payload):
            sent.append(payload)

        async def receive_str(self):
            return next(self._replies)

    acks = asyncio.run(_timeout_adapter(Ws()).subscribe([("005930", "H0NXCNT0"), ("000660", "H0NXCNT0")]))
    assert [(a.symbol, a.stream, a.accepted) for a in acks] == [
        ("005930", "H0NXCNT0", True), ("000660", "H0NXCNT0", True)]
    bodies = [json.loads(p) for p in sent]
    assert [b["header"]["tr_type"] for b in bodies] == ["1", "1"]
    assert [b["body"]["input"]["tr_key"] for b in bodies] == ["005930", "000660"]


def test_kis_hung_ack_becomes_plain_disconnect():
    import asyncio
    import time

    import pytest

    from src.realtime.contracts import VendorAuthRejected, VendorDisconnected

    class Ws:
        async def send_str(self, payload):
            return None

        async def receive_str(self):
            await asyncio.sleep(10)

    start = time.monotonic()
    with pytest.raises(VendorDisconnected, match=r"ack_timeout:005930:H0NXCNT0") as excinfo:
        asyncio.run(_timeout_adapter(Ws(), ack_timeout_s=0.05).subscribe([("005930", "H0NXCNT0")]))
    elapsed = time.monotonic() - start
    assert type(excinfo.value) is VendorDisconnected
    assert not isinstance(excinfo.value, VendorAuthRejected)
    assert 0.04 <= elapsed < 2.0
    assert isinstance(excinfo.value.__cause__, TimeoutError)


def test_kis_ack_deadline_bounds_blocked_pingpong_echo():
    import asyncio
    import json
    from unittest.mock import AsyncMock

    import pytest

    from src.realtime.contracts import VendorDisconnected

    ping = json.dumps({"header": {"tr_id": "PINGPONG"}})

    class Ws:
        async def send_str(self, raw):
            if raw == ping:
                await asyncio.Event().wait()

        receive_str = AsyncMock(return_value=ping)

    async def run():
        adapter = _timeout_adapter(Ws(), ack_timeout_s=0.02)
        with pytest.raises(VendorDisconnected, match="ack_timeout:005930:H0NXCNT0") as exc:
            await asyncio.wait_for(adapter.subscribe([("005930", "H0NXCNT0")]), timeout=1.0)
        assert isinstance(exc.value.__cause__, TimeoutError)

    asyncio.run(run())


def test_kis_deadline_expired_before_receive_is_chained(monkeypatch):
    import asyncio
    from unittest.mock import AsyncMock

    import pytest

    import src.realtime.adapters.kis as kis
    from src.realtime.contracts import VendorDisconnected

    adapter = _timeout_adapter(type("Ws", (), {"receive_str": AsyncMock()})())
    clock = iter([0.0, 11.0])
    monkeypatch.setattr(kis, "time", type("Clock", (), {"monotonic": lambda: next(clock)}))
    with pytest.raises(VendorDisconnected, match="ack_timeout") as exc:
        asyncio.run(adapter._await_subscribe_ack("005930", "H0NXCNT0"))
    assert isinstance(exc.value.__cause__, TimeoutError)
    adapter._ws.receive_str.assert_not_called()


def test_kis_live_ticks_cannot_starve_deadline():
    import asyncio
    import json
    import time

    import pytest

    from src.realtime.contracts import VendorDisconnected

    pingpong = json.dumps({"header": {"tr_id": "PINGPONG"}})

    class Ws:
        def __init__(self):
            self.sent = []
            self.n = 0

        async def send_str(self, payload):
            self.sent.append(payload)

        async def receive_str(self):
            await asyncio.sleep(0.01)
            self.n += 1
            if self.n % 3 == 0:
                return pingpong
            return "0|H0NXCNT0|001|005930^154001^a^b"

    ws = Ws()
    adapter = _timeout_adapter(ws, ack_timeout_s=0.1)
    start = time.monotonic()
    with pytest.raises(VendorDisconnected, match="ack_timeout"):
        asyncio.run(adapter.subscribe([("005930", "H0NXCNT0")]))
    assert time.monotonic() - start < 1.0
    assert len(adapter._pending) > 0
    assert all(f.raw == "005930^154001^a^b" for f in adapter._pending)
    assert pingpong in ws.sent


def test_kis_second_pair_hang_names_second_pair():
    import asyncio

    import pytest

    from src.realtime.contracts import VendorDisconnected

    class Ws:
        def __init__(self):
            self.calls = 0

        async def send_str(self, payload):
            return None

        async def receive_str(self):
            self.calls += 1
            if self.calls == 1:
                return _ack_raw()
            await asyncio.sleep(10)

    with pytest.raises(VendorDisconnected, match=r"ack_timeout:000660:H0NXCNT0"):
        asyncio.run(_timeout_adapter(Ws(), ack_timeout_s=0.05).subscribe(
            [("005930", "H0NXCNT0"), ("000660", "H0NXCNT0")]))


def test_kis_rejected_ack_is_not_timeout():
    import asyncio
    import json

    import pytest

    from src.realtime.contracts import VendorAuthRejected

    class Ws:
        def __init__(self, payload):
            self._payload = payload

        async def send_str(self, payload):
            return None

        async def receive_str(self):
            return self._payload

    acks = asyncio.run(_timeout_adapter(Ws(_ack_raw(code="1", msg="OPSP0011"))).subscribe([("005930", "H0NXCNT0")]))
    assert acks[0].accepted is False
    assert acks[0].code == "1"
    with pytest.raises(VendorAuthRejected):
        asyncio.run(_timeout_adapter(
            Ws(json.dumps({"header": {}, "body": {"rt_cd": "1", "msg_cd": "IGW00121"}}))
        ).subscribe([("005930", "H0NXCNT0")]))


def test_kis_closed_socket_during_wait():
    import asyncio

    import pytest

    from src.realtime.contracts import VendorDisconnected

    class Ws:
        async def send_str(self, payload):
            return None

        async def receive_str(self):
            raise TypeError("cannot read")

    with pytest.raises(VendorDisconnected, match="subscribe_non_text"):
        asyncio.run(_timeout_adapter(Ws()).subscribe([("005930", "H0NXCNT0")]))


def test_kis_send_failure_disconnects():
    import asyncio

    import pytest

    from src.realtime.contracts import VendorDisconnected

    class Ws:
        async def send_str(self, payload):
            raise ConnectionResetError("reset")

        async def receive_str(self):
            raise AssertionError("no wait after send failure")

    with pytest.raises(VendorDisconnected, match="subscribe_failed:ConnectionResetError"):
        asyncio.run(_timeout_adapter(Ws()).subscribe([("005930", "H0NXCNT0")]))


def test_kis_pingpong_echo_failure_mid_wait():
    import asyncio
    import json

    import pytest

    from src.realtime.contracts import VendorDisconnected

    pingpong = json.dumps({"header": {"tr_id": "PINGPONG"}})

    class Ws:
        async def send_str(self, payload):
            if payload == pingpong:
                raise ConnectionResetError("reset")

        async def receive_str(self):
            return pingpong

    with pytest.raises(VendorDisconnected, match="pingpong_echo_failed:ConnectionResetError"):
        asyncio.run(_timeout_adapter(Ws()).subscribe([("005930", "H0NXCNT0")]))


def test_kis_subscribe_cancellation_propagates():
    import asyncio

    import pytest

    class Ws:
        async def send_str(self, payload):
            return None

        async def receive_str(self):
            await asyncio.sleep(10)

    async def run():
        adapter = _timeout_adapter(Ws(), ack_timeout_s=10.0)
        task = asyncio.ensure_future(adapter.subscribe([("005930", "H0NXCNT0")]))
        await asyncio.sleep(0.02)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(run())


def test_kis_ack_timeout_constructor_validation():
    import pytest

    from src.realtime.adapters.kis import KIS_ACK_TIMEOUT_S
    from src.core.config import CollectorSettings

    with pytest.raises(ValueError, match="ack_timeout_s"):
        _timeout_adapter(object(), ack_timeout_s=0)
    with pytest.raises(ValueError, match="ack_timeout_s"):
        _timeout_adapter(object(), ack_timeout_s=-1.0)
    assert CollectorSettings().kis_ack_timeout_s == KIS_ACK_TIMEOUT_S


def test_kis_ack_timeout_hides_secrets(caplog):
    import asyncio
    import logging

    import pytest

    from src.realtime.adapters.kis import KisRealtimeAdapter
    from src.realtime.contracts import MarketSession, MarketVenue, VendorDisconnected
    from src.realtime.session import StreamRoute

    class Ws:
        async def send_str(self, payload):
            return None

        async def receive_str(self):
            await asyncio.sleep(10)

    adapter = KisRealtimeAdapter(
        app_key="LIVEKEY-9f8e7d", app_secret="LIVESECRET-1234", http=object(),
        route=StreamRoute(MarketVenue.NXT, MarketSession.NXT_AFTER),
        allowed_streams=("H0NXCNT0", "H0NXASP0"), capacity_pairs=4, ack_timeout_s=0.02,
    )
    adapter._ws = Ws()
    adapter._approval_key = "APPROVAL-abcdef"
    with caplog.at_level(logging.DEBUG), pytest.raises(VendorDisconnected) as excinfo:
        asyncio.run(adapter.subscribe([("005930", "H0NXCNT0")]))
    assert "LIVEKEY-9f8e7d" not in str(excinfo.value)
    assert "LIVESECRET-1234" not in str(excinfo.value)
    assert "APPROVAL-abcdef" not in str(excinfo.value)
    assert "LIVEKEY-9f8e7d" not in caplog.text


def _hanging_kis_http(*, ack_timeout_s=0.05, scripts=None, closed=None, lease=None):
    import asyncio
    class _Response:
        status = 200

        async def json(self):
            return {"approval_key": "approved"}

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

    state = {"conns": 0}

    class _Ws:
        def __init__(self, script):
            self._script = list(script)
            self.close_calls = 0

        async def send_str(self, payload):
            return None

        async def receive_str(self):
            if not self._script:
                await asyncio.sleep(10)
            item = self._script.pop(0)
            if isinstance(item, BaseException):
                raise item
            return item

        async def close(self):
            self.close_calls += 1
            if closed is not None:
                closed.append(1)

    class _Http:
        def post(self, *a, **k):
            return _Response()

        async def ws_connect(self, *a, **k):
            state["conns"] += 1
            script = scripts[state["conns"] - 1] if scripts and state["conns"] - 1 < len(scripts) else []
            ws = _Ws(script)
            if closed is not None:
                orig = ws.close

                async def _close():
                    await orig()

                ws.close = _close
            return ws

    return _Http(), state


def _kis_streamer(http, *, ack_timeout_s=0.05, **streamer_kwargs):
    from src.realtime.adapters.kis import KisRealtimeAdapter
    from src.realtime.contracts import MarketSession, MarketVenue
    from src.realtime.session import StreamRoute
    from src.realtime.streamer import RealtimeStreamer

    adapter = KisRealtimeAdapter(
        app_key="k", app_secret="s", http=http,
        route=StreamRoute(MarketVenue.NXT, MarketSession.NXT_AFTER),
        allowed_streams=("H0NXCNT0", "H0NXASP0"), capacity_pairs=4,
        ack_timeout_s=ack_timeout_s,
    )
    frames: list = []
    gaps: list = []

    class _Sink:
        def record(self, frame):
            frames.append(frame)

        def note_ack(self, vendor, ack):
            return None

        def note_gap(self, symbol, start_ns, end_ns, reason):
            gaps.append((symbol, start_ns, end_ns, reason))

        def flush(self):
            return len(frames)

    sink = _Sink()
    streamer = RealtimeStreamer(
        adapter=adapter, sink=sink, replay_pairs=[("005930", "H0NXCNT0")], **streamer_kwargs
    )
    return streamer, adapter, sink, frames, gaps


def test_kis_pump_hung_ack_uses_reconnect_path():
    import asyncio

    closed: list = []
    http, state = _hanging_kis_http(closed=closed)
    streamer, adapter, sink, frames, gaps = _kis_streamer(http)
    stop = asyncio.Event()

    async def _sleep(delay):
        return None

    asyncio.run(streamer.run_forever(stop, max_cycles=3, sleep=_sleep))
    assert state["conns"] == 3
    assert streamer._last_disconnect_detail.startswith("ack_timeout:")
    assert frames == []
    assert len(closed) == 3


def test_kis_ack_timeout_backoff_bounds_reconnects():
    import asyncio

    http, state = _hanging_kis_http()
    delays: list = []
    wall = {"ns": 1_000_000_000_000_000_000}

    async def _sleep(delay):
        delays.append(delay)

    streamer, *_ = _kis_streamer(
        http, rng=lambda: 1.0, wall_ns=lambda: wall["ns"], monotonic=lambda: wall["ns"] / 1e9,
    )
    stop = asyncio.Event()
    asyncio.run(streamer.run_forever(stop, max_cycles=5, sleep=_sleep))
    assert state["conns"] == 5
    assert len(delays) == 4
    assert delays == sorted(delays)
    assert delays[0] >= 0.5
    for delay, n in zip(delays, range(1, 5), strict=True):
        assert abs(delay - min(60.0, 1.0 * 2 ** (n - 1))) < 1e-9


def test_kis_ack_timeout_outage_escalation_carries_detail(caplog):
    import asyncio
    import logging

    http, _ = _hanging_kis_http()
    wall = {"ns": 1_000_000_000_000_000_000}

    streamer, *_ = _kis_streamer(
        http, wall_ns=lambda: wall["ns"], silence_limit=lambda: 30.0,
    )
    stop = asyncio.Event()

    async def _sleep(delay):
        wall["ns"] += 200_000_000_000

    with caplog.at_level(logging.CRITICAL):
        asyncio.run(streamer.run_forever(stop, max_cycles=3, sleep=_sleep))
    outages = [r for r in caplog.records if "stage=stream_outage" in r.getMessage()]
    assert len(outages) == 1
    assert "detail=ack_timeout:" in outages[0].getMessage()


def test_kis_gap_recorded_on_recovery_after_timeouts():
    import asyncio

    frame_ns = 2_000_000_000_000_000_000
    ack = _ack_raw()
    http, _ = _hanging_kis_http(scripts=[[], [], [ack]])
    streamer, adapter, sink, frames, gaps = _kis_streamer(http)

    seen = {"n": 0}

    async def _recv():
        seen["n"] += 1
        if seen["n"] == 1:
            from src.realtime.contracts import L0Frame, MarketSession, MarketVenue

            stop.set()
            return L0Frame(
                vendor="kis", stream="H0NXCNT0", symbol="005930", raw="005930^154001",
                recv_mono_ns=1, recv_wall_ns=frame_ns, conn_id="c", conn_seq=1,
                venue=MarketVenue.NXT, session=MarketSession.NXT_AFTER, exchange_event_time="154001",
            )
        await asyncio.sleep(10)

    adapter.recv = _recv  # type: ignore[method-assign]
    stop = asyncio.Event()

    async def _sleep(delay):
        return None

    asyncio.run(streamer.run_forever(stop, max_cycles=5, sleep=_sleep))
    assert gaps
    assert all(g[3] == "disconnect" for g in gaps)
    assert {g[0] for g in gaps} == {"005930"}
