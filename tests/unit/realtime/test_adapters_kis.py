

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
