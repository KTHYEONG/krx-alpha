def test_ls_adapter_connect_issues_token_and_opens_ws(tmp_path) -> None:
    from src.marketdata.toss_token_store import TossTokenStore, ls_token_path
    import asyncio
    from src.realtime.adapters.ls import LsRealtimeAdapter

    class _Resp:
        async def __aenter__(self):
            self.entered = True
            return self
        async def __aexit__(self, *a):
            self.exited = True
            return False
        async def json(self):
            return {'access_token': 'TOK', 'expires_in': 86400}

    class _WS:
        def __init__(self):
            self.sent: list[str] = []
        async def send_str(self, s):
            self.sent.append(s)
        async def close(self):
            self.sent.append('__closed__')

    class _WSCtx:
        def __init__(self, ws):
            self._ws = ws
        async def __aenter__(self):
            return self._ws
        async def __aexit__(self, *a):
            self._done = True
            return False

    class _Http:
        def __init__(self):
            self.ws = _WS()
            self.posts: list[str] = []
        def post(self, url, **kw):
            self.posts.append(url)
            return _Resp()
        def ws_connect(self, url, **kw):
            return _WSCtx(self.ws)

    http = _Http()
    adapter = LsRealtimeAdapter(app_key='k', app_secret='s', http=http, market_of={'005930': 'KOSPI'}, token_store=TossTokenStore(ls_token_path(tmp_path, "k")))

    asyncio.run(adapter.connect())

    assert http.posts and 'oauth2/token' in http.posts[0]  # noqa: PT018 - verbatim contract skeleton
    assert adapter.name == 'ls'
def test_ls_adapter_subscribe_normalizes_ack_by_rsp_cd(tmp_path) -> None:
    from src.marketdata.toss_token_store import TossTokenStore, ls_token_path
    import asyncio
    import json
    from src.realtime.adapters.ls import LsRealtimeAdapter
    from src.realtime.contracts import VendorAck

    acks_raw = [
        json.dumps({'header': {'tr_cd': 'S3_', 'rsp_cd': '00000', 'rsp_msg': 'ok'}, 'body': {}}),
        json.dumps({'header': {'tr_cd': 'H1_', 'rsp_cd': 'IGW00001', 'rsp_msg': 'bad'}, 'body': {}}),
    ]

    class _WS:
        def __init__(self):
            self._q = list(acks_raw)
            self.sent: list[str] = []
        async def send_str(self, s):
            self.sent.append(s)
        async def receive_str(self):
            return self._q.pop(0)
        async def close(self):
            self.sent.append('__closed__')

    adapter = LsRealtimeAdapter(app_key='k', app_secret='s', http=object(), market_of={'005930': 'KOSPI'}, token_store=TossTokenStore(ls_token_path(tmp_path, "k")))
    adapter._ws = _WS()  # type: ignore[attr-defined]
    adapter._token = 'TOK'  # type: ignore[attr-defined]

    out = asyncio.run(adapter.subscribe([('005930', 'H0STCNT0'), ('005930', 'H0STASP0')]))

    assert out[0] == VendorAck(symbol='005930', stream='H0STCNT0', accepted=True, code='00000')
    assert out[1].accepted is False
def test_ls_adapter_recv_echoes_pingpong_and_returns_data_frame(tmp_path) -> None:
    from src.marketdata.toss_token_store import TossTokenStore, ls_token_path
    import asyncio
    import json
    import aiohttp
    from src.realtime.adapters.ls import LsRealtimeAdapter

    ping = json.dumps({'header': {'tr_cd': 'PINGPONG'}})
    data = json.dumps({'header': {'tr_cd': 'S3_', 'tr_key': '005930'}, 'body': {'price': '70000'}})

    class _Msg:
        def __init__(self, t, d):
            self.type = t
            self.data = d

    class _WS:
        def __init__(self):
            self._q = [_Msg(aiohttp.WSMsgType.TEXT, ping), _Msg(aiohttp.WSMsgType.TEXT, data)]
            self.sent: list[str] = []
        async def receive(self):
            return self._q.pop(0)
        async def send_str(self, s):
            self.sent.append(s)
        async def pong(self):
            self.sent.append('__pong__')
        async def close(self):
            self.sent.append('__closed__')

    ws = _WS()
    adapter = LsRealtimeAdapter(app_key='k', app_secret='s', http=object(), market_of={'005930': 'KOSPI'}, token_store=TossTokenStore(ls_token_path(tmp_path, "k")))
    adapter._ws = ws  # type: ignore[attr-defined]

    frame = asyncio.run(adapter.recv())

    assert ws.sent == [ping]
    assert frame.vendor == 'ls' and frame.symbol == '005930' and frame.stream == 'H0STCNT0'  # noqa: PT018 - verbatim contract skeleton
    assert frame.raw == data and frame.conn_seq == 1 and frame.recv_wall_ns > 0  # noqa: PT018 - verbatim contract skeleton
def test_ls_adapter_recv_raises_vendor_disconnected_on_close(tmp_path) -> None:
    from src.marketdata.toss_token_store import TossTokenStore, ls_token_path
    import asyncio
    import aiohttp
    import pytest
    from src.realtime.adapters.ls import LsRealtimeAdapter
    from src.realtime.contracts import VendorDisconnected

    class _Msg:
        def __init__(self, t):
            self.type = t
            self.data = None

    class _WS:
        async def receive(self):
            return _Msg(aiohttp.WSMsgType.CLOSED)
        async def send_str(self, s):
            self.last = s
        async def close(self):
            self.closed = True

    adapter = LsRealtimeAdapter(app_key='k', app_secret='s', http=object(), market_of={'005930': 'KOSPI'}, token_store=TossTokenStore(ls_token_path(tmp_path, "k")))
    adapter._ws = _WS()  # type: ignore[attr-defined]

    with pytest.raises(VendorDisconnected):
        asyncio.run(adapter.recv())
def test_ls_adapter_subscribe_unknown_market_raises_keyerror(tmp_path) -> None:
    from src.marketdata.toss_token_store import TossTokenStore, ls_token_path
    import asyncio
    import pytest
    from src.realtime.adapters.ls import LsRealtimeAdapter

    class _WS:
        async def send_str(self, s):
            self.last = s
        async def receive_str(self):
            return '{}'
        async def close(self):
            self.closed = True

    adapter = LsRealtimeAdapter(app_key='k', app_secret='s', http=object(), market_of={}, token_store=TossTokenStore(ls_token_path(tmp_path, "k")))
    adapter._ws = _WS()  # type: ignore[attr-defined]
    adapter._token = 'TOK'  # type: ignore[attr-defined]

    with pytest.raises(KeyError):
        asyncio.run(adapter.subscribe([('999999', 'H0STCNT0')]))


def test_ls_adapter_recv_echoes_ws_ping(tmp_path) -> None:
    from src.marketdata.toss_token_store import TossTokenStore, ls_token_path
    import asyncio
    import json
    import aiohttp
    from src.realtime.adapters.ls import LsRealtimeAdapter

    data = json.dumps({'header': {'tr_cd': 'S3_', 'tr_key': '005930'}, 'body': {}})

    class _Msg:
        def __init__(self, t, d=None):
            self.type = t
            self.data = d

    class _WS:
        def __init__(self):
            self._q = [_Msg(aiohttp.WSMsgType.PING), _Msg(aiohttp.WSMsgType.TEXT, data)]
            self.ponged = 0
        async def receive(self):
            return self._q.pop(0)
        async def pong(self):
            self.ponged += 1
        async def close(self):
            self.closed = True

    adapter = LsRealtimeAdapter(app_key='k', app_secret='s', http=object(), market_of={'005930': 'KOSPI'}, token_store=TossTokenStore(ls_token_path(tmp_path, "k")))
    adapter._ws = _WS()  # type: ignore[attr-defined]

    frame = asyncio.run(adapter.recv())

    assert adapter._ws.ponged == 1  # type: ignore[attr-defined]
    assert frame.symbol == '005930'
    assert frame.stream == 'H0STCNT0'


def test_ls_adapter_aclose_closes_ws(tmp_path) -> None:
    from src.marketdata.toss_token_store import TossTokenStore, ls_token_path
    import asyncio
    from src.realtime.adapters.ls import LsRealtimeAdapter

    class _WS:
        def __init__(self):
            self.closed = False
        async def close(self):
            self.closed = True

    adapter = LsRealtimeAdapter(app_key='k', app_secret='s', http=object(), market_of={'005930': 'KOSPI'}, token_store=TossTokenStore(ls_token_path(tmp_path, "k")))
    adapter._ws = _WS()  # type: ignore[attr-defined]

    asyncio.run(adapter.aclose())

    assert adapter._ws.closed is True  # type: ignore[attr-defined]


def test_ls_adapter_subscribe_stashes_interleaved_data_frame_instead_of_dropping(tmp_path) -> None:
    from src.marketdata.toss_token_store import TossTokenStore, ls_token_path
    import asyncio
    import json
    from src.realtime.adapters.ls import LsRealtimeAdapter

    stray_data = json.dumps({'header': {'tr_cd': 'S3_', 'tr_key': '000660'}, 'body': {'price': '1000'}})
    real_ack = json.dumps({'header': {'tr_cd': 'S3_', 'rsp_cd': '00000', 'rsp_msg': 'ok'}, 'body': {}})

    class _WS:
        def __init__(self):
            self._q = [stray_data, real_ack]
            self.sent: list[str] = []
        async def send_str(self, s):
            self.sent.append(s)
        async def receive_str(self):
            return self._q.pop(0)
        async def close(self):
            self.sent.append('__closed__')

    adapter = LsRealtimeAdapter(app_key='k', app_secret='s', http=object(), market_of={'005930': 'KOSPI'}, token_store=TossTokenStore(ls_token_path(tmp_path, "k")))
    adapter._ws = _WS()  # type: ignore[attr-defined]
    adapter._token = 'TOK'  # type: ignore[attr-defined]

    acks = asyncio.run(adapter.subscribe([('005930', 'H0STCNT0')]))

    assert acks[0].accepted is True
    assert len(adapter._pending) == 1  # type: ignore[attr-defined]
    assert adapter._pending[0].symbol == '000660'  # type: ignore[attr-defined]


def test_ls_adapter_recv_drains_pending_before_reading_socket(tmp_path) -> None:
    from src.marketdata.toss_token_store import TossTokenStore, ls_token_path
    import asyncio
    from src.realtime.adapters.ls import LsRealtimeAdapter
    from src.realtime.contracts import L0Frame

    class _WS:
        async def receive(self):
            raise AssertionError('socket must not be read while pending queue is non-empty')

    adapter = LsRealtimeAdapter(app_key='k', app_secret='s', http=object(), market_of={'005930': 'KOSPI'}, token_store=TossTokenStore(ls_token_path(tmp_path, "k")))
    adapter._ws = _WS()  # type: ignore[attr-defined]
    stashed = L0Frame('ls', 'H0STCNT0', '000660', '{}', 1, 2, 1, 'ls-1')
    adapter._pending.append(stashed)  # type: ignore[attr-defined]

    frame = asyncio.run(adapter.recv())

    assert frame is stashed


def test_ls_adapter_recv_skips_late_ack_frame_without_crashing(tmp_path) -> None:
    from src.marketdata.toss_token_store import TossTokenStore, ls_token_path
    import asyncio
    import json
    import aiohttp
    from src.realtime.adapters.ls import LsRealtimeAdapter

    late_ack = json.dumps({'header': {'tr_cd': 'S3_', 'rsp_cd': '00000', 'rsp_msg': 'ok'}, 'body': {}})
    data = json.dumps({'header': {'tr_cd': 'S3_', 'tr_key': '005930'}, 'body': {'price': '70000'}})

    class _Msg:
        def __init__(self, t, d):
            self.type = t
            self.data = d

    class _WS:
        def __init__(self):
            self._q = [_Msg(aiohttp.WSMsgType.TEXT, late_ack), _Msg(aiohttp.WSMsgType.TEXT, data)]
        async def receive(self):
            return self._q.pop(0)

    adapter = LsRealtimeAdapter(app_key='k', app_secret='s', http=object(), market_of={'005930': 'KOSPI'}, token_store=TossTokenStore(ls_token_path(tmp_path, "k")))
    adapter._ws = _WS()  # type: ignore[attr-defined]

    frame = asyncio.run(adapter.recv())

    assert frame.symbol == '005930'


def test_ls_adapter_subscribe_skips_pingpong_before_ack(tmp_path) -> None:
    from src.marketdata.toss_token_store import TossTokenStore, ls_token_path
    import asyncio
    import json
    from src.realtime.adapters.ls import LsRealtimeAdapter

    ping = json.dumps({'header': {'tr_cd': 'PINGPONG'}})
    ack = json.dumps({'header': {'tr_cd': 'S3_', 'rsp_cd': '00000', 'rsp_msg': 'ok'}, 'body': {}})

    class _WS:
        def __init__(self):
            self._q = [ping, ack]
            self.sent: list[str] = []
        async def send_str(self, s):
            self.sent.append(s)
        async def receive_str(self):
            return self._q.pop(0)
        async def close(self):
            self.sent.append('__closed__')

    ws = _WS()
    adapter = LsRealtimeAdapter(app_key='k', app_secret='s', http=object(), market_of={'005930': 'KOSPI'}, token_store=TossTokenStore(ls_token_path(tmp_path, "k")))
    adapter._ws = ws  # type: ignore[attr-defined]
    adapter._token = 'TOK'  # type: ignore[attr-defined]

    out = asyncio.run(adapter.subscribe([('005930', 'H0STCNT0')]))

    assert out[0].accepted is True
    assert ws.sent[1] == ping


def test_ls_adapter_assigns_unique_conn_id_per_connect(tmp_path) -> None:
    from src.marketdata.toss_token_store import TossTokenStore, ls_token_path
    # Given: 동일 어댑터가 두 번 접속(재접속)하며 conn_seq 를 0 으로 리셋한다
    import asyncio
    import json

    from src.realtime.adapters.ls import LsRealtimeAdapter

    class _Resp:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def json(self):
            return {'access_token': 'TOK', 'expires_in': 86400}

    class _WS:
        async def send_str(self, s):
            self.last = s

        async def close(self):
            self.closed = True

    class _WSCtx:
        def __init__(self, ws):
            self._ws = ws

        async def __aenter__(self):
            return self._ws

        async def __aexit__(self, *a):
            return False

    class _Http:
        def __init__(self):
            self.ws = _WS()

        def post(self, url, **kw):
            return _Resp()

        def ws_connect(self, url, **kw):
            return _WSCtx(self.ws)

    adapter = LsRealtimeAdapter(app_key='k', app_secret='s', http=_Http(), market_of={'005930': 'KOSPI'}, token_store=TossTokenStore(ls_token_path(tmp_path, "k")))
    payload = {'header': {'tr_cd': 'S3_', 'tr_key': '005930'}}

    # When: 두 접속에서 각각 첫 프레임을 만든다
    asyncio.run(adapter.connect())
    frame1 = adapter._build_frame(payload, json.dumps(payload))
    asyncio.run(adapter.connect())
    frame2 = adapter._build_frame(payload, json.dumps(payload))

    # Then: conn_seq 는 리셋되지만 conn_id 는 접속마다 달라 (conn_id, conn_seq) 가 충돌하지 않는다
    assert frame1.conn_seq == frame2.conn_seq == 1
    assert frame1.conn_id != frame2.conn_id
    assert frame1.conn_id.startswith('ls-')
    assert frame2.conn_id.startswith('ls-')


def test_ls_adapter_connect_enables_ws_heartbeat(tmp_path) -> None:
    from src.marketdata.toss_token_store import TossTokenStore, ls_token_path
    import asyncio

    from src.realtime.adapters.ls import LsRealtimeAdapter

    class _Resp:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def json(self):
            return {"access_token": "TOK"}

    class _WSCtx:
        async def __aenter__(self):
            return object()

        async def __aexit__(self, *a):
            return False

    class _Http:
        def __init__(self):
            self.ws_kwargs = []

        def post(self, url, **kw):
            return _Resp()

        def ws_connect(self, url, **kw):
            self.ws_kwargs.append(kw)
            return _WSCtx()

    default_http, custom_http = _Http(), _Http()
    asyncio.run(LsRealtimeAdapter(app_key="k", app_secret="s", http=default_http, market_of={}, token_store=TossTokenStore(ls_token_path(tmp_path, "k"))).connect())
    asyncio.run(LsRealtimeAdapter(app_key="k", app_secret="s", http=custom_http, market_of={}, heartbeat_s=3.0, token_store=TossTokenStore(ls_token_path(tmp_path, "k"))).connect())

    assert default_http.ws_kwargs == [{"heartbeat": 10.0}]
    assert custom_http.ws_kwargs == [{"heartbeat": 3.0}]


def test_ls_adapter_connect_maps_vendor_failures_to_vendor_disconnected(tmp_path) -> None:
    from src.marketdata.toss_token_store import TossTokenStore, ls_token_path
    import asyncio

    import aiohttp
    import pytest

    from src.realtime.adapters.ls import LsRealtimeAdapter
    from src.realtime.contracts import VendorDisconnected

    class _Resp:
        def __init__(self, payload):
            self.payload = payload

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def json(self):
            if isinstance(self.payload, BaseException):
                raise self.payload
            return self.payload

    class _Http:
        def __init__(self, outcome):
            self.outcome = outcome

        def post(self, url, **kw):
            if isinstance(self.outcome, aiohttp.ClientError):
                raise self.outcome
            return _Resp(self.outcome)

        def ws_connect(self, url, **kw):
            raise AssertionError("ws must not open when the token step failed")

    cases = [
        (aiohttp.ClientConnectionError("dns"), "connect_failed:ClientConnectionError"),
        (ValueError("Expecting value"), "connect_failed:ValueError"),
        ({"error": "invalid_client"}, "connect_failed:KeyError"),
    ]
    for outcome, expected in cases:
        adapter = LsRealtimeAdapter(app_key="k", app_secret="s", http=_Http(outcome), market_of={}, token_store=TossTokenStore(ls_token_path(tmp_path, "k")))
        with pytest.raises(VendorDisconnected, match=expected):
            asyncio.run(adapter.connect())


def test_ls_adapter_subscribe_raises_vendor_disconnected_on_ack_timeout(tmp_path) -> None:
    from src.marketdata.toss_token_store import TossTokenStore, ls_token_path
    import asyncio

    import pytest

    from src.realtime.adapters.ls import LsRealtimeAdapter
    from src.realtime.contracts import VendorDisconnected

    class _WS:
        async def send_str(self, s):
            self.last = s

        async def receive_str(self):
            await asyncio.sleep(5)
            return "{}"

    adapter = LsRealtimeAdapter(app_key="k", app_secret="s", http=object(), market_of={"005930": "KOSPI"}, ack_timeout_s=0.05, token_store=TossTokenStore(ls_token_path(tmp_path, "k")))
    adapter._ws = _WS()  # type: ignore[attr-defined]
    adapter._token = "TOK"  # type: ignore[attr-defined]

    with pytest.raises(VendorDisconnected, match="ack_timeout:005930:H0STCNT0"):
        asyncio.run(adapter.subscribe([("005930", "H0STCNT0")]))


def test_ls_adapter_subscribe_raises_vendor_disconnected_on_non_text_frame(tmp_path) -> None:
    from src.marketdata.toss_token_store import TossTokenStore, ls_token_path
    import asyncio

    import pytest
    from aiohttp import WSMessageTypeError

    from src.realtime.adapters.ls import LsRealtimeAdapter
    from src.realtime.contracts import VendorDisconnected

    class _WS:
        async def send_str(self, s):
            self.last = s

        async def receive_str(self):
            raise WSMessageTypeError("Received message 257:None is not WSMsgType.TEXT")

    adapter = LsRealtimeAdapter(app_key="k", app_secret="s", http=object(), market_of={"005930": "KOSPI"}, token_store=TossTokenStore(ls_token_path(tmp_path, "k")))
    adapter._ws = _WS()  # type: ignore[attr-defined]
    adapter._token = "TOK"  # type: ignore[attr-defined]

    with pytest.raises(VendorDisconnected, match="subscribe_non_text"):
        asyncio.run(adapter.subscribe([("005930", "H0STCNT0")]))


def test_ls_adapter_connect_raises_auth_rejected_on_invalid_app_key(tmp_path) -> None:
    from src.marketdata.toss_token_store import TossTokenStore, ls_token_path
    import asyncio

    import pytest

    from src.realtime.adapters.ls import LsRealtimeAdapter
    from src.realtime.contracts import VendorAuthRejected, VendorDisconnected

    class _Resp:
        def __init__(self, status, payload):
            self.status = status
            self.payload = payload

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def json(self):
            return self.payload

    class _Http:
        def __init__(self, resp):
            self.resp = resp

        def post(self, url, **kw):
            return self.resp

        def ws_connect(self, url, **kw):
            raise AssertionError("ws must not open after an auth rejection")

    body = {"error_code": "IGW00103", "error_description": "유효하지 않은 AppKey입니다."}
    adapter = LsRealtimeAdapter(app_key="k", app_secret="s", http=_Http(_Resp(403, body)), market_of={}, token_store=TossTokenStore(ls_token_path(tmp_path, "k")))

    with pytest.raises(VendorAuthRejected, match="auth_rejected:403:IGW00103") as excinfo:
        asyncio.run(adapter.connect())
    assert isinstance(excinfo.value, VendorDisconnected)

def test_ls_adapter_connect_raises_auth_rejected_on_401_without_error_code(tmp_path) -> None:
    from src.marketdata.toss_token_store import TossTokenStore, ls_token_path
    import asyncio

    import pytest

    from src.realtime.adapters.ls import LsRealtimeAdapter
    from src.realtime.contracts import VendorAuthRejected

    class _Resp:
        def __init__(self, status, payload):
            self.status = status
            self.payload = payload

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def json(self):
            return self.payload

    class _Http:
        def __init__(self, resp):
            self.resp = resp

        def post(self, url, **kw):
            return self.resp

        def ws_connect(self, url, **kw):
            raise AssertionError("ws must not open after an auth rejection")

    adapter = LsRealtimeAdapter(app_key="k", app_secret="s", http=_Http(_Resp(401, {"error": "unauthorized"})), market_of={}, token_store=TossTokenStore(ls_token_path(tmp_path, "k")))

    with pytest.raises(VendorAuthRejected, match=r"auth_rejected:401:$"):
        asyncio.run(adapter.connect())



def test_ls_adapter_reconnect_reuses_stored_token(tmp_path) -> None:
    # Given: 유효한 저장 토큰을 쥔 어댑터
    import asyncio

    from src.marketdata.toss_token_store import TossTokenStore, ls_token_path
    from src.realtime.adapters.ls import LsRealtimeAdapter

    store = TossTokenStore(ls_token_path(tmp_path, "k"))
    posts: list[str] = []

    class _Resp:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def json(self):
            posts.append("token-post")
            return {"access_token": "STORED"}

    class _WSCtx:
        async def __aenter__(self):
            return object()

        async def __aexit__(self, *a):
            return False

    class _Http:
        def post(self, url, **kw):
            return _Resp()

        def ws_connect(self, url, **kw):
            return _WSCtx()

    adapter = LsRealtimeAdapter(app_key="k", app_secret="s", http=_Http(), market_of={}, token_store=store)

    # When: connect()를 두 번 수행한다
    asyncio.run(adapter.connect())
    asyncio.run(adapter.connect())

    # Then: 토큰 POST는 0회다
    assert posts == ["token-post"]


def test_ls_adapter_rejected_token_rotates_once(tmp_path) -> None:
    # Given: 저장 토큰 A에서 WS 인증 거부가 나는 어댑터
    import asyncio

    import pytest

    from src.marketdata.toss_token_store import TossTokenStore, ls_token_path
    from src.realtime.adapters.ls import LsRealtimeAdapter
    from src.realtime.contracts import VendorAuthRejected

    store = TossTokenStore(ls_token_path(tmp_path, "k"))
    issued: list[str] = []

    class _Resp:
        def __init__(self, token):
            self._token = token

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def json(self):
            issued.append(self._token)
            return {"access_token": self._token}

    class _WSCtx:
        def __init__(self, fail):
            self._fail = fail

        async def __aenter__(self):
            if self._fail:
                raise VendorAuthRejected("auth_rejected:401:")
            return object()

        async def __aexit__(self, *a):
            return False

    class _Http:
        def __init__(self):
            self.reject_ws = False

        def post(self, url, **kw):
            return _Resp("B" if len(issued) else "A")

        def ws_connect(self, url, **kw):
            return _WSCtx(self.reject_ws)

    http = _Http()
    adapter = LsRealtimeAdapter(app_key="k", app_secret="s", http=http, market_of={}, token_store=store)

    # When: 첫 접속은 저장 토큰 A를 쓰고 WS 거부가 난다
    asyncio.run(adapter.connect())
    http.reject_ws = True
    with pytest.raises(VendorAuthRejected):
        asyncio.run(adapter.connect())

    # Then: 1회 발급으로 세대가 전진하고 다음 접속은 POST 없이 B를 쓴다
    assert issued == ["A", "B"]
    assert store.read().generation == 2
    http.reject_ws = False
    asyncio.run(adapter.connect())
    assert issued == ["A", "B"]


def test_ls_adapter_ws_transport_failure_maps_to_disconnected(tmp_path) -> None:
    # Given: WS 접속이 전송 실패하는 어댑터
    import asyncio

    import aiohttp
    import pytest

    from src.marketdata.toss_token_store import TossTokenStore, ls_token_path
    from src.realtime.adapters.ls import LsRealtimeAdapter
    from src.realtime.contracts import VendorDisconnected

    store = TossTokenStore(ls_token_path(tmp_path, "k"))

    class _Resp:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def json(self):
            return {"access_token": "TOK"}

    class _Http:
        def post(self, url, **kw):
            return _Resp()

        def ws_connect(self, url, **kw):
            raise aiohttp.ClientConnectionError("reset")

    adapter = LsRealtimeAdapter(app_key="k", app_secret="s", http=_Http(), market_of={}, token_store=store)

    # When / Then
    with pytest.raises(VendorDisconnected, match="connect_failed"):
        asyncio.run(adapter.connect())

def _reset_adapter(tmp_path, *, fail_on_send: int):
    import asyncio
    import aiohttp
    from src.marketdata.toss_token_store import TossTokenStore, ls_token_path
    from src.realtime.adapters.ls import LsRealtimeAdapter

    class _Resp:
        async def __aenter__(self):
            return self
        async def __aexit__(self, *a):
            return False
        async def json(self):
            return {'access_token': 'NEWTOK', 'expires_in': 86400}

    class _WS:
        def __init__(self):
            self.sends = 0
        async def send_str(self, s):
            self.sends += 1
            if self.sends >= fail_on_send:
                raise aiohttp.ClientConnectionResetError('Cannot write to closing transport')
        async def receive_str(self):
            import json
            return json.dumps({'header': {'tr_cd': 'S3_', 'rsp_cd': '00000', 'rsp_msg': 'ok'}, 'body': {}})
        async def close(self):
            return None

    class _WSCtx:
        def __init__(self, ws):
            self._ws = ws
        async def __aenter__(self):
            return self._ws
        async def __aexit__(self, *a):
            return False

    class _Http:
        def __init__(self):
            self.ws = _WS()
            self.posts = 0
        def post(self, url, **kw):
            self.posts += 1
            return _Resp()
        def ws_connect(self, url, **kw):
            return _WSCtx(self.ws)

    store = TossTokenStore(ls_token_path(tmp_path, 'k'))
    http = _Http()
    adapter = LsRealtimeAdapter(app_key='k', app_secret='s', http=http, market_of={'005930': 'KOSPI', '000660': 'KOSPI'}, token_store=store)
    asyncio.run(adapter.connect())
    return adapter, http, store


def test_ls_adapter_subscribe_reset_before_first_ack_rotates_token_and_reports_auth_rejected(tmp_path) -> None:
    import asyncio
    import json
    import pytest
    from src.realtime.contracts import VendorAuthRejected

    # Given: 연결 직후 서버가 소켓을 닫는 만료 토큰 상황(첫 구독 전송에서 reset)
    adapter, http, store = _reset_adapter(tmp_path, fail_on_send=1)
    assert http.posts == 1

    # When: 구독한다
    with pytest.raises(VendorAuthRejected):
        asyncio.run(adapter.subscribe([('005930', 'H0STCNT0')]))

    # Then: 크래시 대신 토큰이 교체(재발급)되어 다음 연결이 새 토큰을 쓴다
    assert http.posts == 2
    assert json.loads(store._path.read_text(encoding='utf-8'))['generation'] == 2


def test_ls_adapter_subscribe_reset_after_acks_is_plain_disconnect(tmp_path) -> None:
    import asyncio
    import pytest
    from src.realtime.contracts import VendorDisconnected

    # Given: 첫 구독은 성공하고 두 번째 전송에서 reset
    adapter, http, _ = _reset_adapter(tmp_path, fail_on_send=2)

    # When / Then: 토큰은 교체하지 않고 VendorDisconnected 로 변환된다
    with pytest.raises(VendorDisconnected):
        asyncio.run(adapter.subscribe([('005930', 'H0STCNT0'), ('000660', 'H0STCNT0')]))
    assert http.posts == 1


def test_ls_adapter_issue_token_records_vendor_expiry(tmp_path) -> None:
    import json

    # Given: expires_in 를 반환하는 LS 응답
    adapter, _, store = _reset_adapter(tmp_path, fail_on_send=99)

    # Then: 저장소에는 만료 시각이 기록된다(영구 유효로 취급하지 않는다)
    assert json.loads(store._path.read_text(encoding='utf-8'))['expires_at'] is not None
