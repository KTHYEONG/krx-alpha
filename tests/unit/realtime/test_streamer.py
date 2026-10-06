def test_session_frame_sink_routes_frame_to_session_journal(tmp_path) -> None:
    import datetime as dt
    from src.universe.ipc import write_candidates
    from src.realtime.session import SessionConfig, bootstrap_session
    from src.realtime.streamer import SessionFrameSink
    from src.realtime.contracts import L0Frame

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
                        recv_mono_ns=10, recv_wall_ns=1_735_954_200_000_000_000, conn_seq=1, conn_id='ls-1'))
    written = sink.flush()

    assert written == 1
    assert any((tmp_path / 'l0').rglob('*.jsonl.zst'))
def test_streamer_pump_subscribes_records_frames_and_returns_on_disconnect() -> None:
    import asyncio
    from src.realtime.streamer import RealtimeStreamer
    from src.realtime.contracts import L0Frame, VendorAck, VendorDisconnected

    frames = [L0Frame('ls', 'H0STCNT0', '005930', 'a', 1, 2, 1, 'ls-1'),
              L0Frame('ls', 'H0STCNT0', '005930', 'b', 3, 4, 2, 'ls-1')]

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
    from src.realtime.streamer import RealtimeStreamer
    from src.realtime.contracts import VendorAck, VendorDisconnected

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
    assert gap_events == [('005930', 'disconnect')]

def test_streamer_pump_stops_on_event_and_flushes() -> None:
    import asyncio
    from src.realtime.streamer import RealtimeStreamer
    from src.realtime.contracts import L0Frame, VendorAck

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
            return L0Frame('ls', 'H0STCNT0', '005930', 'a', 1, 2, 1, 'ls-1')
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

def test_streamer_pump_interrupts_blocked_recv_on_stop() -> None:
    import asyncio
    import time
    from src.realtime.streamer import RealtimeStreamer
    from src.realtime.contracts import VendorAck

    class _Adapter:
        name = 'ls'
        capacity_pairs = 200
        async def connect(self):
            self.c = True
        async def subscribe(self, pairs):
            return [VendorAck(s, t, True, '00000') for s, t in pairs]
        async def recv(self):
            await asyncio.sleep(999)
            raise AssertionError('unreachable')
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

    async def _run() -> tuple[str, float, int]:
        stop = asyncio.Event()
        sink = _Sink()
        streamer = RealtimeStreamer(adapter=_Adapter(), sink=sink, replay_pairs=[('005930', 'H0STCNT0')])
        task = asyncio.ensure_future(streamer.pump(stop))
        await asyncio.sleep(0.05)
        t0 = time.monotonic()
        stop.set()
        reason = await asyncio.wait_for(task, timeout=1.0)
        return reason, time.monotonic() - t0, sink.flushed

    reason, elapsed, flushed = asyncio.run(_run())

    assert reason == 'stopped'
    assert elapsed < 0.5
    assert flushed >= 1


def test_session_frame_sink_persists_frame_conn_id(tmp_path) -> None:
    # Given: 서로 다른 접속에서 온 동일 conn_seq 프레임 2건
    import datetime as dt
    import json

    import zstandard as zstd

    from src.realtime.contracts import L0Frame
    from src.realtime.session import SessionConfig, bootstrap_session
    from src.realtime.streamer import SessionFrameSink
    from src.universe.ipc import write_candidates

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

    # When: vendor 는 같지만 conn_id 가 다른 두 프레임을 기록한다
    sink.record(L0Frame(vendor='ls', stream='H0STCNT0', symbol='005930', raw='{"x":1}',
                        recv_mono_ns=10, recv_wall_ns=1_735_954_200_000_000_000, conn_seq=1, conn_id='ls-111'))
    sink.record(L0Frame(vendor='ls', stream='H0STCNT0', symbol='005930', raw='{"x":2}',
                        recv_mono_ns=20, recv_wall_ns=1_735_954_200_000_000_001, conn_seq=1, conn_id='ls-222'))
    written = sink.flush()

    # Then: 저널에 vendor 가 아닌 접속 고유 conn_id 가 기록된다
    assert written == 2
    zst = next((tmp_path / 'l0').rglob('*.jsonl.zst'))
    dctx = zstd.ZstdDecompressor()
    with open(zst, 'rb') as fh, dctx.stream_reader(fh, read_across_frames=True) as reader:
        lines = reader.read().decode('utf-8').strip().splitlines()
    conn_ids = [json.loads(x)['conn_id'] for x in lines]
    assert conn_ids == ['ls-111', 'ls-222']


def test_regular_session_silence_limit_only_during_regular_session() -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo

    from src.core.session_anchors import standard_session_anchors
    from src.realtime.streamer import SILENCE_LIMIT_S, regular_session_silence_limit_s

    kst = ZoneInfo("Asia/Seoul")
    anchors = standard_session_anchors(dt.date(2026, 9, 14))

    assert SILENCE_LIMIT_S == 30.0
    assert regular_session_silence_limit_s(dt.datetime(2026, 9, 14, 8, 59, 59, tzinfo=kst), anchors=anchors) is None
    assert regular_session_silence_limit_s(dt.datetime(2026, 9, 14, 9, 0, 0, tzinfo=kst), anchors=anchors) == 30.0
    assert regular_session_silence_limit_s(dt.datetime(2026, 9, 14, 15, 29, 59, tzinfo=kst), anchors=anchors) == 30.0
    assert regular_session_silence_limit_s(dt.datetime(2026, 9, 14, 15, 30, 0, tzinfo=kst), anchors=anchors) is None
    assert regular_session_silence_limit_s(dt.datetime(2026, 9, 14, 1, 0, 0, tzinfo=dt.UTC), anchors=anchors) == 30.0
    assert regular_session_silence_limit_s(dt.datetime(2026, 9, 14, 0, 0, 0, tzinfo=dt.UTC), anchors=anchors, limit_s=5.0) == 5.0


def test_streamer_pump_returns_watchdog_when_vendor_goes_silent() -> None:
    import asyncio
    import time

    from src.realtime.contracts import VendorAck
    from src.realtime.streamer import RealtimeStreamer

    class _Adapter:
        name = "ls"
        capacity_pairs = 200

        def __init__(self):
            self.closed = False

        async def connect(self):
            return None

        async def subscribe(self, pairs):
            return [VendorAck(s, t, True, "00000") for s, t in pairs]

        async def recv(self):
            await asyncio.sleep(999)

        async def aclose(self):
            self.closed = True

    class _Sink:
        def __init__(self):
            self.flushed = 0

        def record(self, f):
            raise AssertionError("no frames expected")

        def note_ack(self, v, a):
            return None

        def note_gap(self, *a):
            return None

        def flush(self):
            self.flushed += 1
            return 0

    adapter, sink = _Adapter(), _Sink()
    streamer = RealtimeStreamer(adapter=adapter, sink=sink, replay_pairs=[("005930", "H0STCNT0")], silence_limit=lambda: 0.05)

    t0 = time.monotonic()
    reason = asyncio.run(streamer.pump(asyncio.Event()))

    assert reason == "watchdog"
    assert time.monotonic() - t0 < 1.0
    assert adapter.closed is True
    assert sink.flushed >= 1


def test_streamer_pump_disables_watchdog_when_limit_is_none() -> None:
    import asyncio

    from src.realtime.contracts import VendorAck
    from src.realtime.streamer import RealtimeStreamer

    class _Adapter:
        name = "ls"
        capacity_pairs = 200

        async def connect(self):
            return None

        async def subscribe(self, pairs):
            return [VendorAck(s, t, True, "00000") for s, t in pairs]

        async def recv(self):
            await asyncio.sleep(999)

        async def aclose(self):
            return None

    class _Sink:
        def record(self, f):
            return None

        def note_ack(self, v, a):
            return None

        def note_gap(self, *a):
            return None

        def flush(self):
            return 0

    async def _run() -> str:
        stop = asyncio.Event()
        streamer = RealtimeStreamer(adapter=_Adapter(), sink=_Sink(), replay_pairs=[("005930", "H0STCNT0")], silence_limit=lambda: None)
        task = asyncio.ensure_future(streamer.pump(stop))
        await asyncio.sleep(0.2)
        assert not task.done()
        stop.set()
        return await asyncio.wait_for(task, timeout=1.0)

    assert asyncio.run(_run()) == "stopped"


def test_streamer_records_gap_from_disconnect_until_first_frame_after_reconnect(caplog) -> None:
    import asyncio
    import logging

    from src.realtime.contracts import L0Frame, VendorAck, VendorDisconnected
    from src.realtime.streamer import RealtimeStreamer

    stop = asyncio.Event()

    class _Adapter:
        name = "ls"
        capacity_pairs = 200

        def __init__(self):
            self.cycle = 0
            self.sent = 0

        async def connect(self):
            self.cycle += 1
            self.sent = 0

        async def subscribe(self, pairs):
            return [VendorAck(s, t, True, "00000") for s, t in pairs]

        async def recv(self):
            self.sent += 1
            if self.cycle == 1:
                if self.sent == 1:
                    return L0Frame("ls", "H0STCNT0", "005930", "a", 1, 100, 1, "ls-1")
                raise VendorDisconnected("closed")
            stop.set()
            return L0Frame("ls", "H0STCNT0", "005930", "b", 2, 5_000, 1, "ls-2")

        async def aclose(self):
            return None

    class _Sink:
        def __init__(self):
            self.gaps = []

        def record(self, f):
            return None

        def note_ack(self, v, a):
            return None

        def note_gap(self, symbol, start, end, reason):
            self.gaps.append((symbol, start, end, reason))

        def flush(self):
            return 0

    async def _nosleep(_):
        return None

    sink = _Sink()
    streamer = RealtimeStreamer(
        adapter=_Adapter(), sink=sink, replay_pairs=[("005930", "H0STCNT0"), ("000660", "H0STCNT0")],
        wall_ns=lambda: 1_000, rng=lambda: 1.0,
    )

    with caplog.at_level(logging.WARNING):
        asyncio.run(streamer.run_forever(stop, sleep=_nosleep))

    assert sink.gaps == [("000660", 100, 5_000, "disconnect"), ("005930", 100, 5_000, "disconnect")]
    assert "stage=stream_gap reason=disconnect gap_ms=0 symbols=2" in caplog.text
    assert "stage=stream_disconnect reason=disconnect frames=1 consecutive_failures=1 backoff_s=1.00" in caplog.text


def test_streamer_backoff_is_capped_exponential_and_resets_after_healthy_connection() -> None:
    import asyncio

    from src.realtime.contracts import L0Frame, VendorDisconnected
    from src.realtime.streamer import RealtimeStreamer

    class _Adapter:
        name = "ls"
        capacity_pairs = 200

        def __init__(self, healthy_cycles):
            self.cycle = 0
            self.sent = 0
            self.healthy_cycles = healthy_cycles

        async def connect(self):
            self.cycle += 1
            self.sent = 0

        async def subscribe(self, pairs):
            return []

        async def recv(self):
            self.sent += 1
            if self.cycle in self.healthy_cycles and self.sent == 1:
                return L0Frame("ls", "H0STCNT0", "005930", "x", 1, 10, 1, "c")
            raise VendorDisconnected("closed")

        async def aclose(self):
            return None

    class _Sink:
        def record(self, f):
            return None

        def note_ack(self, v, a):
            return None

        def note_gap(self, *a):
            return None

        def flush(self):
            return 0

    def run(healthy, cycles, rng):
        slept: list[float] = []

        async def _sleep(s):
            slept.append(round(s, 3))

        streamer = RealtimeStreamer(adapter=_Adapter(healthy), sink=_Sink(), replay_pairs=[("005930", "H0STCNT0")],
                                    backoff_max_s=4.0, rng=rng, wall_ns=lambda: 1)
        asyncio.run(streamer.run_forever(asyncio.Event(), max_cycles=cycles, backoff_s=1.0, sleep=_sleep))
        return slept

    assert run(set(), 5, lambda: 1.0) == [1.0, 2.0, 4.0, 4.0]
    assert run({2}, 4, lambda: 1.0) == [1.0, 1.0, 2.0]
    assert run(set(), 2, lambda: 0.0) == [0.5]


def test_streamer_pump_flushes_buffer_when_unexpected_exception_escapes(caplog) -> None:
    import asyncio
    import logging

    from src.realtime.contracts import L0Frame
    from src.realtime.streamer import RealtimeStreamer

    class _Adapter:
        name = "ls"
        capacity_pairs = 200

        def __init__(self):
            self.n = 0
            self.closed = 0

        async def connect(self):
            return None

        async def subscribe(self, pairs):
            return []

        async def recv(self):
            self.n += 1
            if self.n > 3:
                raise RuntimeError("parser bug")
            return L0Frame("ls", "H0STCNT0", "005930", "x", 1, 10 + self.n, self.n, "c")

        async def aclose(self):
            self.closed += 1

    class _Sink:
        def __init__(self):
            self.buffer = 0
            self.persisted = 0

        def record(self, f):
            self.buffer += 1

        def note_ack(self, v, a):
            return None

        def note_gap(self, *a):
            return None

        def flush(self):
            n, self.buffer = self.buffer, 0
            self.persisted += n
            return n

    adapter, sink = _Adapter(), _Sink()
    streamer = RealtimeStreamer(adapter=adapter, sink=sink, replay_pairs=[("005930", "H0STCNT0")], flush_every=200)

    with caplog.at_level(logging.ERROR, logger="src.realtime.streamer"):
        reason = asyncio.run(streamer.pump(asyncio.Event()))

    assert reason == "disconnect"
    assert streamer._last_disconnect_detail == "unexpected:RuntimeError"
    assert any(r.levelno == logging.ERROR and r.exc_info for r in caplog.records)
    assert sink.persisted == 3
    assert sink.buffer == 0
    assert adapter.closed == 1


def test_streamer_pump_flushes_on_time_interval_between_count_flushes() -> None:
    import asyncio

    from src.realtime.contracts import L0Frame
    from src.realtime.streamer import RealtimeStreamer

    clock = {"t": 0.0}
    stop = asyncio.Event()
    times = [0.1, 0.2, 0.7]

    class _Adapter:
        name = "ls"
        capacity_pairs = 200

        def __init__(self):
            self.n = 0

        async def connect(self):
            return None

        async def subscribe(self, pairs):
            return []

        async def recv(self):
            clock["t"] = times[self.n]
            self.n += 1
            if self.n == len(times):
                stop.set()
            return L0Frame("ls", "H0STCNT0", "005930", "x", 1, self.n, self.n, "c")

        async def aclose(self):
            return None

    class _Sink:
        def __init__(self):
            self.flush_at: list[float] = []

        def record(self, f):
            return None

        def note_ack(self, v, a):
            return None

        def note_gap(self, *a):
            return None

        def flush(self):
            self.flush_at.append(clock["t"])
            return 0

    sink = _Sink()
    streamer = RealtimeStreamer(adapter=_Adapter(), sink=sink, replay_pairs=[], flush_every=1000,
                                flush_interval_s=0.5, monotonic=lambda: clock["t"])

    assert asyncio.run(streamer.pump(stop)) == "stopped"
    assert sink.flush_at == [0.7, 0.7]


def test_streamer_pump_logs_connect_summary_and_rejected_subscriptions(caplog) -> None:
    import asyncio
    import logging

    from src.realtime.contracts import VendorAck, VendorDisconnected
    from src.realtime.streamer import RealtimeStreamer

    class _Adapter:
        name = "ls"
        capacity_pairs = 200

        async def connect(self):
            return None

        async def subscribe(self, pairs):
            return [VendorAck("005930", "H0STCNT0", True, "00000"), VendorAck("000660", "H0STCNT0", False, "IGW00001")]

        async def recv(self):
            raise VendorDisconnected("closed")

        async def aclose(self):
            return None

    class _Sink:
        def record(self, f):
            return None

        def note_ack(self, v, a):
            return None

        def note_gap(self, *a):
            return None

        def flush(self):
            return 0

    streamer = RealtimeStreamer(adapter=_Adapter(), sink=_Sink(), replay_pairs=[("005930", "H0STCNT0"), ("000660", "H0STCNT0")])

    with caplog.at_level(logging.INFO):
        assert asyncio.run(streamer.pump(asyncio.Event())) == "disconnect"

    assert "[DATA] stage=stream_connect pairs=2 accepted=1 rejected=1" in caplog.text
    assert "[DATA] stage=stream_subscribe status=PARTIAL rejected=1 codes=IGW00001" in caplog.text


def test_session_frame_sink_persists_manifest_on_gap_and_throttles_flush_checkpoints(tmp_path) -> None:
    import datetime as dt
    import json

    from src.realtime.session import SessionConfig, bootstrap_session
    from src.realtime.streamer import SessionFrameSink
    from src.universe.ipc import write_candidates

    class _FC:
        def request(self, host, version=3, timeout=5):
            return type("S", (), {"offset": 0.0})()

    cp = tmp_path / "c.json"
    write_candidates(cp, [{"symbol": "005930", "selection_reasons": ["limit_up"]}], rev=1)
    cfg = SessionConfig(session_date=dt.date(2026, 9, 14), journal_root=tmp_path / "l0", manifest_path=tmp_path / "s.json",
                        candidates_path=cp, ntp_host="h", slot_budget=200, max_clock_offset_ns=2_000_000_000,
                        desired_streams=("H0STCNT0",), vendor="ls")
    session = bootstrap_session(cfg, ntp_client=_FC(), now_ns=1)
    persists: list[int] = []
    real_persist = session.persist

    def _counting_persist() -> None:
        persists.append(1)
        real_persist()

    session.persist = _counting_persist  # type: ignore[method-assign]
    clock = {"t": 0.0}
    sink = SessionFrameSink(session=session, checkpoint_interval_s=60.0, monotonic=lambda: clock["t"])

    sink.note_gap("005930", 10, 20, "disconnect")
    assert len(persists) == 1
    assert json.loads(cfg.manifest_path.read_text(encoding="utf-8"))["gaps"][0]["reason"] == "disconnect"
    clock["t"] = 10.0
    sink.flush()
    assert len(persists) == 1
    clock["t"] = 70.5
    sink.flush()
    assert len(persists) == 2


def test_streamer_pump_logs_critical_when_final_flush_fails(caplog) -> None:
    import asyncio
    import logging

    import pytest

    from src.realtime.contracts import VendorAck, VendorDisconnected
    from src.realtime.streamer import RealtimeStreamer
    from src.storage.journal import JournalWriteError

    class _Adapter:
        name = "ls"
        capacity_pairs = 200

        def __init__(self):
            self.closed = False

        async def connect(self):
            return None

        async def subscribe(self, pairs):
            return [VendorAck(s, t, True, "00000") for s, t in pairs]

        async def recv(self):
            raise VendorDisconnected("closed")

        async def aclose(self):
            self.closed = True

    class _Sink:
        def record(self, f):
            return None

        def note_ack(self, v, a):
            return None

        def note_gap(self, *a):
            return None

        def flush(self):
            raise JournalWriteError("disk full")

    adapter, sink = _Adapter(), _Sink()
    streamer = RealtimeStreamer(adapter=adapter, sink=sink, replay_pairs=[("005930", "H0STCNT0")])

    with caplog.at_level(logging.CRITICAL), pytest.raises(JournalWriteError, match="disk full"):
        asyncio.run(streamer.pump(asyncio.Event()))

    assert "[DATA] stage=stream_flush status=FAIL error=disk full" in caplog.text
    assert adapter.closed is True


def test_streamer_escalates_auth_rejection_once_and_caps_backoff_at_auth_limit(caplog) -> None:
    import asyncio
    import logging

    from src.realtime.contracts import VendorAuthRejected
    from src.realtime.streamer import AUTH_BACKOFF_MAX_S, RealtimeStreamer

    class _Adapter:
        name = "ls"
        capacity_pairs = 200

        async def connect(self):
            raise VendorAuthRejected("auth_rejected:403:IGW00103")

        async def subscribe(self, pairs):
            return []

        async def recv(self):
            raise AssertionError("unreachable")

        async def aclose(self):
            return None

    class _Sink:
        def __init__(self):
            self.gaps = []

        def record(self, f):
            return None

        def note_ack(self, v, a):
            return None

        def note_gap(self, symbol, start, end, reason):
            self.gaps.append((symbol, start, end, reason))

        def flush(self):
            return 0

    clock = {"ns": 0}
    slept: list[float] = []

    async def _sleep(s):
        slept.append(s)
        clock["ns"] += int(s * 1e9)

    sink = _Sink()
    streamer = RealtimeStreamer(adapter=_Adapter(), sink=sink, replay_pairs=[("005930", "H0STCNT0")],
                                backoff_max_s=60.0, rng=lambda: 1.0, wall_ns=lambda: clock["ns"])

    with caplog.at_level(logging.WARNING):
        asyncio.run(streamer.run_forever(asyncio.Event(), max_cycles=4, backoff_s=100.0, sleep=_sleep))

    criticals = [r.getMessage() for r in caplog.records if r.levelno == logging.CRITICAL]
    assert AUTH_BACKOFF_MAX_S == 300.0
    assert slept == [100.0, 200.0, 300.0]
    assert len(criticals) == 1
    assert criticals[0].startswith("[DATA] stage=stream_outage status=CRITICAL reason=auth_rejected detail=auth_rejected:403:IGW00103")
    assert sink.gaps == [("005930", 0, 600_000_000_000, "auth_rejected")]
    assert "stage=stream_outage status=UNRESOLVED reason=auth_rejected outage_s=600" in caplog.text

def test_streamer_escalates_regular_session_outage_after_threshold_and_logs_recovery(caplog) -> None:
    import asyncio
    import logging

    from src.realtime.contracts import L0Frame, VendorDisconnected
    from src.realtime.streamer import OUTAGE_CRITICAL_S, RealtimeStreamer

    stop = asyncio.Event()
    clock = {"ns": 0}

    class _Adapter:
        name = "ls"
        capacity_pairs = 200

        def __init__(self):
            self.cycle = 0

        async def connect(self):
            self.cycle += 1

        async def subscribe(self, pairs):
            return []

        async def recv(self):
            if self.cycle < 5:
                raise VendorDisconnected("closed")
            stop.set()
            return L0Frame("ls", "H0STCNT0", "005930", "x", 1, 470_000_000_000, 1, "ls-5")

        async def aclose(self):
            return None

    class _Sink:
        def __init__(self):
            self.gaps = []

        def record(self, f):
            return None

        def note_ack(self, v, a):
            return None

        def note_gap(self, symbol, start, end, reason):
            self.gaps.append((symbol, start, end, reason))

        def flush(self):
            return 0

    async def _sleep(s):
        clock["ns"] += int(s * 1e9)

    sink = _Sink()
    streamer = RealtimeStreamer(adapter=_Adapter(), sink=sink, replay_pairs=[("005930", "H0STCNT0")],
                                silence_limit=lambda: 30.0, backoff_max_s=120.0, rng=lambda: 1.0, wall_ns=lambda: clock["ns"])

    with caplog.at_level(logging.WARNING):
        asyncio.run(streamer.run_forever(stop, backoff_s=100.0, sleep=_sleep))

    criticals = [r.getMessage() for r in caplog.records if r.levelno == logging.CRITICAL]
    assert OUTAGE_CRITICAL_S == 300.0
    assert len(criticals) == 1
    assert criticals[0].startswith("[DATA] stage=stream_outage status=CRITICAL reason=disconnect detail=closed outage_s=340")
    assert sink.gaps == [("005930", 0, 470_000_000_000, "disconnect")]
    assert "stage=stream_outage status=RECOVERED reason=disconnect outage_s=470" in caplog.text

def test_streamer_does_not_escalate_transient_outage_outside_regular_session(caplog) -> None:
    import asyncio
    import logging

    from src.realtime.contracts import L0Frame, VendorDisconnected
    from src.realtime.streamer import RealtimeStreamer

    stop = asyncio.Event()
    clock = {"ns": 0}

    class _Adapter:
        name = "ls"
        capacity_pairs = 200

        def __init__(self):
            self.cycle = 0

        async def connect(self):
            self.cycle += 1

        async def subscribe(self, pairs):
            return []

        async def recv(self):
            if self.cycle < 5:
                raise VendorDisconnected("closed")
            stop.set()
            return L0Frame("ls", "H0STCNT0", "005930", "x", 1, 470_000_000_000, 1, "ls-5")

        async def aclose(self):
            return None

    class _Sink:
        def __init__(self):
            self.gaps = []

        def record(self, f):
            return None

        def note_ack(self, v, a):
            return None

        def note_gap(self, symbol, start, end, reason):
            self.gaps.append((symbol, start, end, reason))

        def flush(self):
            return 0

    async def _sleep(s):
        clock["ns"] += int(s * 1e9)

    sink = _Sink()
    streamer = RealtimeStreamer(adapter=_Adapter(), sink=sink, replay_pairs=[("005930", "H0STCNT0")],
                                silence_limit=lambda: None, backoff_max_s=120.0, rng=lambda: 1.0, wall_ns=lambda: clock["ns"])

    with caplog.at_level(logging.WARNING):
        asyncio.run(streamer.run_forever(stop, backoff_s=100.0, sleep=_sleep))

    assert [r for r in caplog.records if r.levelno == logging.CRITICAL] == []
    assert "stage=stream_outage" not in caplog.text
    assert sink.gaps == [("005930", 0, 470_000_000_000, "disconnect")]



def test_aftermarket_silence_limit_honors_distinct_starts():
    import datetime as dt
    from zoneinfo import ZoneInfo
    from src.realtime.contracts import MarketSession, MarketVenue
    from src.realtime.session import StreamRoute
    from src.core.session_anchors import standard_session_anchors
    from src.realtime.streamer import aftermarket_silence_limit_s
    kst = ZoneInfo('Asia/Seoul')
    anchors = standard_session_anchors(dt.date(2026, 9, 15))
    nxt = StreamRoute(MarketVenue.NXT, MarketSession.NXT_AFTER)
    krx = StreamRoute(MarketVenue.KRX, MarketSession.KRX_AFTER)
    now = dt.datetime(2026, 9, 15, 15, 45, tzinfo=kst)
    assert aftermarket_silence_limit_s(now, route=nxt, anchors=anchors) == 30.0
    assert aftermarket_silence_limit_s(now, route=krx, anchors=anchors) is None


def test_regular_session_silence_limit_follows_anchors() -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo

    from src.core.session_anchors import AnchorSource, SessionAnchors
    from src.realtime.streamer import regular_session_silence_limit_s

    kst = ZoneInfo("Asia/Seoul")
    anchors = SessionAnchors(
        date=dt.date(2026, 11, 19),
        regular_open=dt.time(10, 0),
        closing_auction_start=dt.time(16, 20),
        regular_close=dt.time(16, 30),
        after_market_end=dt.time(20, 0),
        source=AnchorSource.VENDOR,
    )

    assert regular_session_silence_limit_s(dt.datetime(2026, 11, 19, 9, 30, tzinfo=kst), anchors=anchors) is None
    assert regular_session_silence_limit_s(dt.datetime(2026, 11, 19, 10, 5, tzinfo=kst), anchors=anchors) == 30.0
    assert regular_session_silence_limit_s(dt.datetime(2026, 11, 19, 16, 25, tzinfo=kst), anchors=anchors) == 30.0


def test_aftermarket_silence_limit_follows_shifted_close() -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo

    from src.core.session_anchors import AnchorSource, SessionAnchors
    from src.realtime.contracts import MarketSession, MarketVenue
    from src.realtime.session import StreamRoute
    from src.realtime.streamer import aftermarket_silence_limit_s

    kst = ZoneInfo("Asia/Seoul")
    anchors = SessionAnchors(
        date=dt.date(2026, 11, 19),
        regular_open=dt.time(10, 0),
        closing_auction_start=dt.time(16, 20),
        regular_close=dt.time(16, 30),
        after_market_end=dt.time(20, 0),
        source=AnchorSource.VENDOR,
    )
    nxt = StreamRoute(MarketVenue.NXT, MarketSession.NXT_AFTER)

    assert aftermarket_silence_limit_s(dt.datetime(2026, 11, 19, 16, 35, tzinfo=kst), route=nxt, anchors=anchors) is None
    assert aftermarket_silence_limit_s(dt.datetime(2026, 11, 19, 16, 45, tzinfo=kst), route=nxt, anchors=anchors) == 30.0


class _GapSink:
    def __init__(self) -> None:
        self.gaps: list = []
        self.records: list = []

    def record(self, frame):
        self.records.append(frame.raw)

    def note_ack(self, vendor, ack):
        return None

    def note_gap(self, symbol, start_ns, end_ns, reason):
        self.gaps.append((symbol, start_ns, end_ns, reason))

    def flush(self):
        return 0


class _NotifySink(_GapSink):
    """Sink that trips ``event`` once the Nth frame has been recorded."""

    def __init__(self, event, *, nth: int = 3) -> None:
        super().__init__()
        self._event = event
        self._nth = nth

    def record(self, frame):
        super().record(frame)
        if len(self.records) == self._nth:
            self._event.set()


def _gap_sink() -> _GapSink:
    return _GapSink()


def _silent_after_one_frame(recv_wall_ns: int, stop) -> "object":
    import asyncio

    from src.realtime.contracts import L0Frame, VendorAck

    class _Adapter:
        name = "ls"
        capacity_pairs = 200

        def __init__(self) -> None:
            self.cycle = 0
            self.sent = 0

        async def connect(self):
            self.cycle += 1
            self.sent = 0

        async def subscribe(self, pairs):
            return [VendorAck(s, t, True, "00000") for s, t in pairs]

        async def recv(self):
            self.sent += 1
            if self.sent == 1:
                if self.cycle == 2:
                    stop.set()
                return L0Frame(
                    "ls", "H0STCNT0", "005930",
                    "a" if self.cycle == 1 else "b",
                    1, recv_wall_ns if self.cycle == 1 else 50_000,
                    self.sent, f"ls-{self.cycle}",
                )
            await asyncio.sleep(999)

        async def aclose(self):
            return None

    return _Adapter()


def test_watchdog_gap_starts_at_last_received_frame_not_trigger_time() -> None:
    # 침묵 감시 발동까지의 지연만큼 공백이 과소 계상되지 않아야 한다.
    import asyncio

    from src.realtime.streamer import RealtimeStreamer

    stop = asyncio.Event()
    sink = _gap_sink()
    streamer = RealtimeStreamer(
        adapter=_silent_after_one_frame(1_000, stop), sink=sink, replay_pairs=[("005930", "H0STCNT0")],
        silence_limit=lambda: 0.05, wall_ns=lambda: 40_000, rng=lambda: 1.0,
    )

    async def _nosleep(_):
        return None

    asyncio.run(streamer.run_forever(stop, max_cycles=2, sleep=_nosleep))

    assert sink.records == ["a", "b"]
    assert sink.gaps == [("005930", 1_000, 50_000, "watchdog")]


def test_gap_never_inverts_when_clock_steps_backwards() -> None:
    # NTP 보정으로 벽시계가 뒤로 물러나도 start <= end 를 보장한다.
    import asyncio

    from src.realtime.streamer import RealtimeStreamer

    stop = asyncio.Event()
    sink = _gap_sink()
    streamer = RealtimeStreamer(
        adapter=_silent_after_one_frame(9_000, stop), sink=sink, replay_pairs=[("005930", "H0STCNT0")],
        silence_limit=lambda: 0.05, wall_ns=lambda: 40_000, rng=lambda: 1.0,
    )

    async def _nosleep(_):
        return None

    asyncio.run(streamer.run_forever(stop, max_cycles=2, sleep=_nosleep))

    assert all(start_ns <= end_ns for _, start_ns, end_ns, _ in sink.gaps)


def test_outage_without_any_frame_starts_at_run_start_wall_time() -> None:
    # 프레임을 한 번도 못 받았으면 run_forever 시작 시각이 유일한 공백 시작점이다.
    import asyncio

    from src.realtime.contracts import L0Frame, VendorAck, VendorDisconnected
    from src.realtime.streamer import RealtimeStreamer

    stop = asyncio.Event()

    class _Adapter:
        name = "ls"
        capacity_pairs = 200

        def __init__(self) -> None:
            self.cycle = 0

        async def connect(self):
            self.cycle += 1

        async def subscribe(self, pairs):
            return [VendorAck(s, t, True, "00000") for s, t in pairs]

        async def recv(self):
            if self.cycle < 4:
                raise VendorDisconnected("closed")
            stop.set()
            return L0Frame("ls", "H0STCNT0", "005930", "x", 1, 470_000_000_000, 1, "ls-4")

        async def aclose(self):
            return None

    clock = {"ns": 0}
    sink = _gap_sink()
    streamer = RealtimeStreamer(
        adapter=_Adapter(), sink=sink, replay_pairs=[("005930", "H0STCNT0")],
        wall_ns=lambda: clock["ns"], rng=lambda: 1.0,
    )

    async def _sleep(s):
        clock["ns"] += int(s * 1e9)

    asyncio.run(streamer.run_forever(stop, max_cycles=4, backoff_s=100.0, sleep=_sleep))

    assert sink.gaps == [("005930", 0, 470_000_000_000, "disconnect")]


def test_unresolved_outage_starts_at_last_frame_and_closes_at_final_wall_time() -> None:
    # max_cycles 로 끝나 미해결 상태로 닫히는 공백도 마지막 프레임에서 시작한다.
    import asyncio

    from src.realtime.contracts import L0Frame, VendorAck, VendorDisconnected
    from src.realtime.streamer import RealtimeStreamer

    class _Adapter:
        name = "ls"
        capacity_pairs = 200

        def __init__(self) -> None:
            self.sent = 0

        async def connect(self):
            return None

        async def subscribe(self, pairs):
            return [VendorAck(s, t, True, "00000") for s, t in pairs]

        async def recv(self):
            self.sent += 1
            if self.sent == 1:
                return L0Frame("ls", "H0STCNT0", "005930", "a", 1, 1_000, 1, "ls-1")
            raise VendorDisconnected("closed")

        async def aclose(self):
            return None

    sink = _gap_sink()
    streamer = RealtimeStreamer(
        adapter=_Adapter(), sink=sink, replay_pairs=[("005930", "H0STCNT0")],
        wall_ns=lambda: 9_000, rng=lambda: 1.0,
    )

    asyncio.run(streamer.run_forever(asyncio.Event(), max_cycles=1))

    assert sink.gaps == [("005930", 1_000, 9_000, "disconnect")]


def test_watchdog_arms_only_after_window_opens_and_fires_limit_after_arming() -> None:
    # 감시 창이 열린 뒤 limit 시간이 지나야 발동해야 한다(창 열림 즉시 발동 금지).
    import asyncio
    import time

    from src.realtime.contracts import VendorAck
    from src.realtime.streamer import RealtimeStreamer

    class _Adapter:
        name = "ls"
        capacity_pairs = 200

        async def connect(self):
            return None

        async def subscribe(self, pairs):
            return [VendorAck(s, t, True, "00000") for s, t in pairs]

        async def recv(self):
            await asyncio.sleep(999)

        async def aclose(self):
            return None

    opened_at = {"t": None}

    def _limit() -> float | None:
        if opened_at["t"] is None:
            opened_at["t"] = time.monotonic()
        return 0.05 if time.monotonic() - opened_at["t"] >= 0.15 else None

    streamer = RealtimeStreamer(
        adapter=_Adapter(), sink=_gap_sink(), replay_pairs=[("005930", "H0STCNT0")],
        silence_limit=_limit, silence_poll_interval_s=0.02,
    )

    t0 = time.monotonic()
    reason = asyncio.run(streamer.pump(asyncio.Event()))
    elapsed = time.monotonic() - t0

    assert reason == "watchdog"
    assert elapsed < 1.0
    assert elapsed >= 0.20


def test_in_flight_recv_survives_poll_wakeups() -> None:
    # 폴링 웨이크업마다 recv 를 취소/재생성하면 이미 읽은 프레임을 버린다.
    import asyncio

    from src.realtime.contracts import L0Frame, VendorAck
    from src.realtime.streamer import RealtimeStreamer

    class _Adapter:
        name = "ls"
        capacity_pairs = 200

        def __init__(self) -> None:
            self.calls = 0

        async def connect(self):
            return None

        async def subscribe(self, pairs):
            return [VendorAck(s, t, True, "00000") for s, t in pairs]

        async def recv(self):
            self.calls += 1
            if self.calls > 3:
                await asyncio.sleep(999)
            await asyncio.sleep(0.1)
            return L0Frame("ls", "H0STCNT0", "005930", str(self.calls), 1, self.calls, self.calls, "ls-1")

        async def aclose(self):
            return None

    async def _run() -> str:
        async def _stop_once_third_frame_recorded() -> None:
            while len(sink.records) < 3:
                await third_recorded.wait()
            await asyncio.sleep(0)
            stop.set()

        stopper = asyncio.ensure_future(_stop_once_third_frame_recorded())
        try:
            return await asyncio.wait_for(streamer.pump(stop), timeout=3.0)
        finally:
            stopper.cancel()

    third_recorded = asyncio.Event()
    stop = asyncio.Event()
    adapter = _Adapter()
    sink = _NotifySink(third_recorded)
    streamer = RealtimeStreamer(
        adapter=adapter, sink=sink, replay_pairs=[("005930", "H0STCNT0")],
        silence_limit=lambda: 1.0, silence_poll_interval_s=0.01,
    )

    reason = asyncio.run(_run())

    assert reason == "stopped"
    assert sink.records == ["1", "2", "3"]
    # 폴링은 in-flight recv 를 건드리지 않아야 하므로 stop 시점의 1건만 취소된다.
    assert adapter.calls == 4


def test_poll_interval_must_be_positive() -> None:
    import pytest

    from src.realtime.streamer import RealtimeStreamer

    class _Adapter:
        name = "ls"
        capacity_pairs = 200

    with pytest.raises(ValueError, match="silence_poll_interval_s"):
        RealtimeStreamer(
            adapter=_Adapter(), sink=_gap_sink(), replay_pairs=[],
            silence_poll_interval_s=0,
        )


def test_disconnect_log_carries_frame_and_heartbeat_ages(caplog) -> None:
    # "하트비트는 살아있지만 데이터는 정체" 를 하트비트만으론 판별할 수 없으므로 나이를 함께 남긴다.
    import asyncio
    import logging

    from src.realtime.contracts import L0Frame, VendorAck
    from src.realtime.streamer import RealtimeStreamer

    pingpong_at = 1_700_000_000_000_000_000
    stop = asyncio.Event()

    class _Adapter:
        name = "ls"
        capacity_pairs = 200
        pingpong_count = 3
        last_pingpong_wall_ns = pingpong_at

        def __init__(self) -> None:
            self.sent = 0

        async def connect(self):
            return None

        async def subscribe(self, pairs):
            return [VendorAck(s, t, True, "00000") for s, t in pairs]

        async def recv(self):
            self.sent += 1
            if self.sent == 1:
                return L0Frame("ls", "H0STCNT0", "005930", "a", 1, pingpong_at - 1_000_000_000, 1, "ls-1")
            await asyncio.sleep(999)

        async def aclose(self):
            return None

    streamer = RealtimeStreamer(
        adapter=_Adapter(), sink=_gap_sink(), replay_pairs=[("005930", "H0STCNT0")],
        silence_limit=lambda: 0.05,
        wall_ns=lambda: pingpong_at + 2_500_000_000, rng=lambda: 1.0,
    )

    with caplog.at_level(logging.WARNING):
        asyncio.run(streamer.run_forever(stop, max_cycles=1))

    assert (
        "last_frame_age_s=3.50 last_pingpong_age_s=2.50 pingpongs=3"
        in caplog.text
    )


def test_disconnect_log_reports_na_without_heartbeat_probe(caplog) -> None:
    import asyncio
    import logging

    from src.realtime.contracts import VendorAck, VendorDisconnected
    from src.realtime.streamer import RealtimeStreamer

    class _Adapter:
        name = "ls"
        capacity_pairs = 200

        async def connect(self):
            return None

        async def subscribe(self, pairs):
            return [VendorAck(s, t, True, "00000") for s, t in pairs]

        async def recv(self):
            raise VendorDisconnected("closed")

        async def aclose(self):
            return None

    streamer = RealtimeStreamer(
        adapter=_Adapter(), sink=_gap_sink(), replay_pairs=[("005930", "H0STCNT0")],
        wall_ns=lambda: 5_000, rng=lambda: 1.0,
    )

    with caplog.at_level(logging.WARNING):
        asyncio.run(streamer.run_forever(asyncio.Event(), max_cycles=1))

    assert "last_frame_age_s=na last_pingpong_age_s=na pingpongs=na" in caplog.text


def test_aftermarket_silence_limit_honors_configured_limit_s() -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo

    from src.core.session_anchors import standard_session_anchors
    from src.realtime.contracts import MarketSession, MarketVenue
    from src.realtime.session import StreamRoute
    from src.realtime.streamer import aftermarket_silence_limit_s

    kst = ZoneInfo("Asia/Seoul")
    anchors = standard_session_anchors(dt.date(2026, 9, 15))
    nxt = StreamRoute(MarketVenue.NXT, MarketSession.NXT_AFTER)

    assert aftermarket_silence_limit_s(
        dt.datetime(2026, 9, 15, 15, 45, tzinfo=kst), route=nxt, anchors=anchors, limit_s=10.0
    ) == 10.0
    assert aftermarket_silence_limit_s(
        dt.datetime(2026, 9, 15, 15, 0, tzinfo=kst), route=nxt, anchors=anchors, limit_s=10.0
    ) is None


def test_watchdog_rearms_after_window_closes_and_reopens() -> None:
    # 창이 닫혔다 다시 열리면 침묵 기준선도 다시 잡혀야 한다(오래된 기준선으로 즉시 발동 금지).
    import asyncio
    import time

    from src.realtime.contracts import VendorAck
    from src.realtime.streamer import RealtimeStreamer

    class _Adapter:
        name = "ls"
        capacity_pairs = 200

        async def connect(self):
            return None

        async def subscribe(self, pairs):
            return [VendorAck(s, t, True, "00000") for s, t in pairs]

        async def recv(self):
            await asyncio.sleep(999)

        async def aclose(self):
            return None

    started = time.monotonic()

    def _limit() -> float | None:
        elapsed = time.monotonic() - started
        return None if 0.10 <= elapsed < 0.40 else 0.30

    streamer = RealtimeStreamer(
        adapter=_Adapter(), sink=_gap_sink(), replay_pairs=[("005930", "H0STCNT0")],
        silence_limit=_limit, silence_poll_interval_s=0.02,
    )

    reason = asyncio.run(streamer.pump(asyncio.Event()))
    elapsed = time.monotonic() - started

    assert reason == "watchdog"
    assert elapsed >= 0.65


def test_premarket_silence_watchdog_arms_only_in_premarket_window() -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo

    from src.core.session_anchors import standard_session_anchors
    from src.realtime.streamer import premarket_silence_limit_s

    kst = ZoneInfo("Asia/Seoul")
    anchors = standard_session_anchors(dt.date(2026, 10, 2))

    assert premarket_silence_limit_s(dt.datetime(2026, 10, 2, 7, 59, 59, tzinfo=kst), anchors=anchors) is None
    assert premarket_silence_limit_s(dt.datetime(2026, 10, 2, 8, 0, 0, tzinfo=kst), anchors=anchors) == 30.0
    assert premarket_silence_limit_s(dt.datetime(2026, 10, 2, 8, 49, 59, tzinfo=kst), anchors=anchors) == 30.0
    assert premarket_silence_limit_s(dt.datetime(2026, 10, 2, 8, 50, 0, tzinfo=kst), anchors=anchors) is None


def test_premarket_window_follows_open_shift() -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo

    from src.core.session_anchors import AnchorSource, SessionAnchors
    from src.realtime.streamer import premarket_silence_limit_s

    kst = ZoneInfo("Asia/Seoul")
    anchors = SessionAnchors(
        date=dt.date(2025, 11, 13),
        regular_open=dt.time(10, 0),
        closing_auction_start=dt.time(16, 20),
        regular_close=dt.time(16, 30),
        after_market_end=dt.time(20, 0),
        source=AnchorSource.VENDOR,
    )

    assert premarket_silence_limit_s(dt.datetime(2025, 11, 13, 8, 30, tzinfo=kst), anchors=anchors) is None
    assert premarket_silence_limit_s(dt.datetime(2025, 11, 13, 9, 30, tzinfo=kst), anchors=anchors) == 30.0


def test_streamer_pump_propagates_fail_closed_domain_errors() -> None:
    import asyncio

    import pytest

    from src.core.errors import SlotBudgetExceededError
    from src.realtime.streamer import RealtimeStreamer
    from src.storage.journal import JournalWriteError

    class _Adapter:
        name = "ls"
        capacity_pairs = 200

        def __init__(self):
            self.flushed = 0
            self.closed = 0

        async def connect(self):
            return None

        async def subscribe(self, pairs):
            return []

        async def recv(self):
            from src.realtime.contracts import L0Frame

            return L0Frame("ls", "H0STCNT0", "005930", "x", 1, 2, 1, "ls-1")

        async def aclose(self):
            self.closed += 1

    class _FailSink:
        def __init__(self, exc):
            self._exc = exc
            self.flushed = 0

        def record(self, f):
            raise self._exc

        def note_ack(self, v, a):
            return None

        def note_gap(self, *a):
            return None

        def flush(self):
            self.flushed += 1
            return 0

    for exc in (JournalWriteError("disk full"), SlotBudgetExceededError("budget")):
        adapter = _Adapter()
        sink = _FailSink(exc)
        streamer = RealtimeStreamer(adapter=adapter, sink=sink, replay_pairs=[("005930", "H0STCNT0")])
        with pytest.raises(type(exc)):
            asyncio.run(streamer.pump(asyncio.Event()))
        assert sink.flushed >= 1
        assert adapter.closed == 1


def test_streamer_pump_does_not_swallow_cancellation() -> None:
    import asyncio

    import pytest

    from src.realtime.streamer import RealtimeStreamer

    class _Adapter:
        name = "ls"
        capacity_pairs = 200

        def __init__(self):
            self.closed = 0

        async def connect(self):
            return None

        async def subscribe(self, pairs):
            return []

        async def recv(self):
            await asyncio.sleep(3600)

        async def aclose(self):
            self.closed += 1

    class _Sink:
        def record(self, f):
            return None

        def note_ack(self, v, a):
            return None

        def note_gap(self, *a):
            return None

        def flush(self):
            return 0

    async def _run() -> None:
        streamer = RealtimeStreamer(adapter=_Adapter(), sink=_Sink(), replay_pairs=[("005930", "H0STCNT0")])
        task = asyncio.ensure_future(streamer.pump(asyncio.Event()))
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(_run())


def test_streamer_pump_aclose_failure_does_not_mask_result() -> None:
    import asyncio

    from src.realtime.contracts import VendorDisconnected
    from src.realtime.streamer import RealtimeStreamer

    class _Adapter:
        name = "ls"
        capacity_pairs = 200

        async def connect(self):
            return None

        async def subscribe(self, pairs):
            return []

        async def recv(self):
            raise VendorDisconnected("closed")

        async def aclose(self):
            raise OSError("close reset")

    class _Sink:
        def record(self, f):
            return None

        def note_ack(self, v, a):
            return None

        def note_gap(self, *a):
            return None

        def flush(self):
            return 0

    streamer = RealtimeStreamer(adapter=_Adapter(), sink=_Sink(), replay_pairs=[("005930", "H0STCNT0")])
    assert asyncio.run(streamer.pump(asyncio.Event())) == "disconnect"
    assert streamer._last_disconnect_detail == "closed"


def test_streamer_run_forever_survives_unexpected_errors() -> None:
    import asyncio

    from src.realtime.streamer import RealtimeStreamer

    stop = asyncio.Event()

    class _Adapter:
        name = "ls"
        capacity_pairs = 200

        def __init__(self):
            self.calls = 0
            self.closes = 0

        async def connect(self):
            return None

        async def subscribe(self, pairs):
            return []

        async def recv(self):
            self.calls += 1
            if self.calls <= 3:
                raise ValueError(f"bad frame {self.calls}")
            stop.set()
            from src.realtime.contracts import L0Frame

            return L0Frame("ls", "H0STCNT0", "005930", "ok", 1, 99, 1, "ls-9")

        async def aclose(self):
            self.closes += 1

    class _Sink:
        def record(self, f):
            return None

        def note_ack(self, v, a):
            return None

        def note_gap(self, *a):
            return None

        def flush(self):
            return 0

    slept: list[float] = []

    async def _sleep(s):
        slept.append(s)

    adapter = _Adapter()
    streamer = RealtimeStreamer(adapter=adapter, sink=_Sink(), replay_pairs=[("005930", "H0STCNT0")], rng=lambda: 1.0)
    asyncio.run(streamer.run_forever(stop, sleep=_sleep))

    assert len(slept) == 3
    assert streamer._failures == 3
    assert adapter.closes == 4


def _cleanup_streamer(*, close_error=None, flush_error=None):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, Mock

    from src.realtime.contracts import VendorDisconnected
    from src.realtime.streamer import RealtimeStreamer

    adapter = SimpleNamespace(
        name="ls", capacity_pairs=200,
        connect=AsyncMock(), subscribe=AsyncMock(return_value=[]),
        recv=AsyncMock(side_effect=VendorDisconnected("closed")),
        aclose=AsyncMock(side_effect=close_error),
    )
    sink = Mock()
    sink.flush = Mock(return_value=0, side_effect=flush_error)
    return RealtimeStreamer(adapter=adapter, sink=sink, replay_pairs=[]), adapter, sink


def test_repeated_disconnects_leave_no_pending_tasks() -> None:
    import asyncio

    async def run():
        baseline = asyncio.all_tasks()
        streamer, adapter, _ = _cleanup_streamer()
        stop = asyncio.Event()
        for _ in range(5):
            assert await streamer.pump(stop) == "disconnect"
            assert asyncio.all_tasks() == baseline
        assert adapter.aclose.await_count == 5

    asyncio.run(run())


def test_cleanup_propagates_domain_errors_and_preserves_primary() -> None:
    import asyncio
    from unittest.mock import AsyncMock

    import pytest

    from src.core.errors import KrxAlphaError
    from src.storage.journal import JournalWriteError

    for close_error, flush_error in (
        (KrxAlphaError("domain close"), None),
        (None, OSError("disk full")),
        (KrxAlphaError("domain close"), JournalWriteError("journal failure")),
    ):
        streamer, adapter, _ = _cleanup_streamer(close_error=close_error, flush_error=flush_error)
        expected = JournalWriteError if flush_error else KrxAlphaError
        with pytest.raises(expected):
            asyncio.run(streamer.pump(asyncio.Event()))
        adapter.aclose.assert_awaited_once()

    primary = JournalWriteError("primary")
    streamer, adapter, sink = _cleanup_streamer(
        close_error=KrxAlphaError("secondary close"), flush_error=JournalWriteError("secondary flush"),
    )
    adapter.connect = AsyncMock(side_effect=primary)
    with pytest.raises(JournalWriteError) as caught:
        asyncio.run(streamer.pump(asyncio.Event()))
    assert caught.value is primary
    sink.flush.assert_called_once()
    adapter.aclose.assert_awaited_once()


def test_close_vendor_disconnect_keeps_primary_reason() -> None:
    import asyncio

    from src.realtime.contracts import VendorDisconnected

    streamer, _, _ = _cleanup_streamer(close_error=VendorDisconnected("secondary"))
    assert asyncio.run(streamer.pump(asyncio.Event())) == "disconnect"
    assert streamer._last_disconnect_detail == "closed"


def test_cancelled_recv_domain_error_propagates_after_all_tasks_drained() -> None:
    import asyncio

    import pytest

    from src.storage.journal import JournalWriteError

    async def run():
        baseline = asyncio.all_tasks()
        stop = asyncio.Event()
        entered = asyncio.Event()
        domain_error = JournalWriteError("recv cleanup")

        async def recv():
            entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                raise domain_error from None

        streamer, adapter, _ = _cleanup_streamer()
        adapter.recv = recv
        task = asyncio.create_task(streamer.pump(stop))
        await entered.wait()
        stop.set()
        with pytest.raises(JournalWriteError) as caught:
            await task
        assert caught.value is domain_error
        assert asyncio.all_tasks() == baseline

    asyncio.run(run())


def test_pump_cancellation_survives_cleanup_domain_errors() -> None:
    import asyncio

    import pytest

    from src.core.errors import KrxAlphaError
    from src.storage.journal import JournalWriteError

    async def run():
        baseline = asyncio.all_tasks()
        entered = asyncio.Event()

        async def recv():
            entered.set()
            await asyncio.Event().wait()

        streamer, adapter, _ = _cleanup_streamer(
            close_error=KrxAlphaError("close"), flush_error=JournalWriteError("flush"),
        )
        adapter.recv = recv
        task = asyncio.create_task(streamer.pump(asyncio.Event()))
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        adapter.aclose.assert_awaited_once()
        assert asyncio.all_tasks() == baseline

    asyncio.run(run())


def test_streamer_all_rejected_is_not_a_silent_run(caplog) -> None:
    import asyncio
    import logging

    from src.realtime.contracts import VendorAuthRejected
    from src.realtime.streamer import RealtimeStreamer

    class _Adapter:
        name = "ls"
        capacity_pairs = 200

        async def connect(self):
            return None

        async def subscribe(self, pairs):
            raise VendorAuthRejected("auth_rejected:all_acks_rejected:01234")

        async def recv(self):
            raise AssertionError("unreachable")

        async def aclose(self):
            return None

    class _Sink:
        def __init__(self):
            self.gaps = []

        def record(self, f):
            raise AssertionError("no frames on auth rejection")

        def note_ack(self, v, a):
            raise AssertionError("rejected acks are not recorded")

        def note_gap(self, symbol, start, end, reason):
            self.gaps.append((symbol, start, end, reason))

        def flush(self):
            return 0

    sink = _Sink()
    streamer = RealtimeStreamer(adapter=_Adapter(), sink=sink, replay_pairs=[("005930", "H0STCNT0")])
    with caplog.at_level(logging.WARNING):
        reason = asyncio.run(streamer.pump(asyncio.Event()))
    assert reason == "auth_rejected"

    async def _run() -> None:
        await streamer.run_forever(asyncio.Event(), max_cycles=1, backoff_s=0.01, sleep=lambda s: asyncio.sleep(0))

    with caplog.at_level(logging.WARNING):
        asyncio.run(_run())
    criticals = [r.getMessage() for r in caplog.records if r.levelno == logging.CRITICAL]
    assert any("reason=auth_rejected" in m and "auth_rejected:all_acks_rejected:01234" in m for m in criticals)
    assert sink.gaps
    assert sink.gaps[0][3] == "auth_rejected"
