def test_session_frame_sink_routes_frame_to_session_journal(tmp_path) -> None:
    import datetime as dt
    from src.collector.ipc import write_candidates
    from src.collector.session import SessionConfig, bootstrap_session
    from src.collector.streamer import SessionFrameSink
    from src.collector.vendor import L0Frame

    class _FC:
        def request(self, host, version=3, timeout=5):
            class _S:
                offset = 0.0
            return _S()

    cp = tmp_path / 'c.json'
    write_candidates(cp, [{'symbol': '005930', 'selection_reasons': ['limit_up']}], rev=1)
    cfg = SessionConfig(session_date=dt.date(2026, 9, 8), journal_root=tmp_path / 'l0',
                        manifest_path=tmp_path / 's.json', candidates_path=cp, ntp_host='h',
                        slot_budget=200, max_clock_offset_ns=2_000_000_000,
                        desired_streams=('H0STCNT0',), vendor='ls')
    session = bootstrap_session(cfg, ntp_client=_FC(), now_ns=1)
    sink = SessionFrameSink(session=session)

    sink.record(L0Frame(vendor='ls', stream='H0STCNT0', symbol='005930', raw='{"x":1}',
                        recv_mono_ns=10, recv_wall_ns=1_735_954_200_000_000_000, conn_seq=1))
    written = sink.flush()

    assert written == 1
    assert any((tmp_path / 'l0').rglob('*.jsonl.zst'))
def test_streamer_pump_subscribes_records_frames_and_returns_on_disconnect() -> None:
    import asyncio
    from src.collector.streamer import RealtimeStreamer
    from src.collector.vendor import L0Frame, VendorAck, VendorDisconnected

    frames = [L0Frame('ls', 'H0STCNT0', '005930', 'a', 1, 2, 1),
              L0Frame('ls', 'H0STCNT0', '005930', 'b', 3, 4, 2)]

    class _Adapter:
        name = 'ls'
        capacity_pairs = 200
        def __init__(self):
            self.log: list[str] = []
            self.subscribed = None
            self._q = list(frames)
        async def connect(self):
            self.log.append('connect')
        async def subscribe(self, pairs):
            self.subscribed = pairs
            return [VendorAck(s, t, True, '00000') for s, t in pairs]
        async def recv(self):
            if self._q:
                return self._q.pop(0)
            raise VendorDisconnected('closed')
        async def aclose(self):
            self.log.append('aclose')

    class _Sink:
        def __init__(self):
            self.records: list = []
            self.acks: list = []
            self.gaps: list = []
            self.flushed = 0
        def record(self, f):
            self.records.append(f)
        def note_ack(self, v, a):
            self.acks.append((v, a))
        def note_gap(self, *a):
            self.gaps.append(a)
        def flush(self):
            self.flushed += 1
            return len(self.records)

    adapter = _Adapter()
    sink = _Sink()
    streamer = RealtimeStreamer(adapter=adapter, sink=sink, replay_pairs=[('005930', 'H0STCNT0')], flush_every=1)

    reason = asyncio.run(streamer.pump(asyncio.Event()))

    assert reason == 'disconnect'
    assert adapter.subscribed == [('005930', 'H0STCNT0')]
    assert [f.raw for f in sink.records] == ['a', 'b']
    # flush_every=1 -> 프레임당 1회 + 절단 시 1회
    assert len(sink.acks) == 1 and sink.flushed >= 3 and 'aclose' in adapter.log  # noqa: PT018 - verbatim contract skeleton
def test_streamer_run_forever_records_gap_between_reconnect_cycles() -> None:
    import asyncio
    from src.collector.streamer import RealtimeStreamer
    from src.collector.vendor import VendorAck, VendorDisconnected

    class _Adapter:
        name = 'ls'
        capacity_pairs = 200
        async def connect(self):
            self.c = True
        async def subscribe(self, pairs):
            return [VendorAck(s, t, True, '00000') for s, t in pairs]
        async def recv(self):
            raise VendorDisconnected('closed')
        async def aclose(self):
            self.a = True

    class _Sink:
        def __init__(self):
            self.gaps: list = []
        def record(self, f):
            self.gaps.append(('rec', f))
        def note_ack(self, v, a):
            self.gaps.append(('ack', v))
        def note_gap(self, symbol, s, e, reason):
            self.gaps.append((symbol, reason))
        def flush(self):
            return 0

    sink = _Sink()
    streamer = RealtimeStreamer(adapter=_Adapter(), sink=sink,
                               replay_pairs=[('005930', 'H0STCNT0'), ('005930', 'H0STASP0')])

    async def _nosleep(_):
        return await asyncio.sleep(0)

    asyncio.run(streamer.run_forever(asyncio.Event(), max_cycles=2, sleep=_nosleep))

    gap_events = [g for g in sink.gaps if g[0] == '005930']
    assert gap_events == [('005930', 'disconnect'), ('005930', 'disconnect')]
def test_streamer_pump_stops_on_event_and_flushes() -> None:
    import asyncio
    from src.collector.streamer import RealtimeStreamer
    from src.collector.vendor import L0Frame, VendorAck

    stop = asyncio.Event()

    class _Adapter:
        name = 'ls'
        capacity_pairs = 200
        async def connect(self):
            self.c = True
        async def subscribe(self, pairs):
            return [VendorAck(s, t, True, '00000') for s, t in pairs]
        async def recv(self):
            stop.set()
            return L0Frame('ls', 'H0STCNT0', '005930', 'a', 1, 2, 1)
        async def aclose(self):
            self.a = True

    class _Sink:
        def __init__(self):
            self.flushed = 0
        def record(self, f):
            self.last = f
        def note_ack(self, v, a):
            self.ack = (v, a)
        def note_gap(self, *a):
            self.gap = a
        def flush(self):
            self.flushed += 1
            return 0

    sink = _Sink()
    streamer = RealtimeStreamer(adapter=_Adapter(), sink=sink, replay_pairs=[('005930', 'H0STCNT0')])

    reason = asyncio.run(streamer.pump(stop))

    assert reason == 'stopped' and sink.flushed >= 1  # noqa: PT018 - verbatim contract skeleton
