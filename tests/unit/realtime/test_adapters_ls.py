def test_ls_adapter_connect_issues_token_and_opens_ws() -> None:
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
    adapter = LsRealtimeAdapter(app_key='k', app_secret='s', http=http, market_of={'005930': 'KOSPI'})

    asyncio.run(adapter.connect())

    assert http.posts and 'oauth2/token' in http.posts[0]  # noqa: PT018 - verbatim contract skeleton
    assert adapter.name == 'ls'
def test_ls_adapter_subscribe_normalizes_ack_by_rsp_cd() -> None:
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

    adapter = LsRealtimeAdapter(app_key='k', app_secret='s', http=object(), market_of={'005930': 'KOSPI'})
    adapter._ws = _WS()  # type: ignore[attr-defined]
    adapter._token = 'TOK'  # type: ignore[attr-defined]

    out = asyncio.run(adapter.subscribe([('005930', 'H0STCNT0'), ('005930', 'H0STASP0')]))

    assert out[0] == VendorAck(symbol='005930', stream='H0STCNT0', accepted=True, code='00000')
    assert out[1].accepted is False
def test_ls_adapter_recv_echoes_pingpong_and_returns_data_frame() -> None:
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
    adapter = LsRealtimeAdapter(app_key='k', app_secret='s', http=object(), market_of={'005930': 'KOSPI'})
    adapter._ws = ws  # type: ignore[attr-defined]

    frame = asyncio.run(adapter.recv())

    assert ws.sent == [ping]
    assert frame.vendor == 'ls' and frame.symbol == '005930' and frame.stream == 'H0STCNT0'  # noqa: PT018 - verbatim contract skeleton
    assert frame.raw == data and frame.conn_seq == 1 and frame.recv_wall_ns > 0  # noqa: PT018 - verbatim contract skeleton
def test_ls_adapter_recv_raises_vendor_disconnected_on_close() -> None:
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

    adapter = LsRealtimeAdapter(app_key='k', app_secret='s', http=object(), market_of={'005930': 'KOSPI'})
    adapter._ws = _WS()  # type: ignore[attr-defined]

    with pytest.raises(VendorDisconnected):
        asyncio.run(adapter.recv())
def test_ls_adapter_subscribe_unknown_market_raises_keyerror() -> None:
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

    adapter = LsRealtimeAdapter(app_key='k', app_secret='s', http=object(), market_of={})
    adapter._ws = _WS()  # type: ignore[attr-defined]
    adapter._token = 'TOK'  # type: ignore[attr-defined]

    with pytest.raises(KeyError):
        asyncio.run(adapter.subscribe([('999999', 'H0STCNT0')]))


def test_ls_adapter_recv_echoes_ws_ping() -> None:
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

    adapter = LsRealtimeAdapter(app_key='k', app_secret='s', http=object(), market_of={'005930': 'KOSPI'})
    adapter._ws = _WS()  # type: ignore[attr-defined]

    frame = asyncio.run(adapter.recv())

    assert adapter._ws.ponged == 1  # type: ignore[attr-defined]
    assert frame.symbol == '005930'
    assert frame.stream == 'H0STCNT0'


def test_ls_adapter_aclose_closes_ws() -> None:
    import asyncio
    from src.realtime.adapters.ls import LsRealtimeAdapter

    class _WS:
        def __init__(self):
            self.closed = False
        async def close(self):
            self.closed = True

    adapter = LsRealtimeAdapter(app_key='k', app_secret='s', http=object(), market_of={'005930': 'KOSPI'})
    adapter._ws = _WS()  # type: ignore[attr-defined]

    asyncio.run(adapter.aclose())

    assert adapter._ws.closed is True  # type: ignore[attr-defined]


def test_ls_adapter_subscribe_stashes_interleaved_data_frame_instead_of_dropping() -> None:
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

    adapter = LsRealtimeAdapter(app_key='k', app_secret='s', http=object(), market_of={'005930': 'KOSPI'})
    adapter._ws = _WS()  # type: ignore[attr-defined]
    adapter._token = 'TOK'  # type: ignore[attr-defined]

    acks = asyncio.run(adapter.subscribe([('005930', 'H0STCNT0')]))

    assert acks[0].accepted is True
    assert len(adapter._pending) == 1  # type: ignore[attr-defined]
    assert adapter._pending[0].symbol == '000660'  # type: ignore[attr-defined]


def test_ls_adapter_recv_drains_pending_before_reading_socket() -> None:
    import asyncio
    from src.realtime.adapters.ls import LsRealtimeAdapter
    from src.realtime.contracts import L0Frame

    class _WS:
        async def receive(self):
            raise AssertionError('socket must not be read while pending queue is non-empty')

    adapter = LsRealtimeAdapter(app_key='k', app_secret='s', http=object(), market_of={'005930': 'KOSPI'})
    adapter._ws = _WS()  # type: ignore[attr-defined]
    stashed = L0Frame('ls', 'H0STCNT0', '000660', '{}', 1, 2, 1)
    adapter._pending.append(stashed)  # type: ignore[attr-defined]

    frame = asyncio.run(adapter.recv())

    assert frame is stashed


def test_ls_adapter_recv_skips_late_ack_frame_without_crashing() -> None:
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

    adapter = LsRealtimeAdapter(app_key='k', app_secret='s', http=object(), market_of={'005930': 'KOSPI'})
    adapter._ws = _WS()  # type: ignore[attr-defined]

    frame = asyncio.run(adapter.recv())

    assert frame.symbol == '005930'


def test_ls_adapter_subscribe_skips_pingpong_before_ack() -> None:
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
    adapter = LsRealtimeAdapter(app_key='k', app_secret='s', http=object(), market_of={'005930': 'KOSPI'})
    adapter._ws = ws  # type: ignore[attr-defined]
    adapter._token = 'TOK'  # type: ignore[attr-defined]

    out = asyncio.run(adapter.subscribe([('005930', 'H0STCNT0')]))

    assert out[0].accepted is True
    assert ws.sent[1] == ping
