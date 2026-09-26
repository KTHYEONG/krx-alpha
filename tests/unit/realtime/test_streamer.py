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

    assert sink.gaps == [("000660", 1_000, 5_000, "disconnect"), ("005930", 1_000, 5_000, "disconnect")]
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


def test_streamer_pump_flushes_buffer_when_unexpected_exception_escapes() -> None:
    import asyncio

    import pytest

    from src.realtime.contracts import L0Frame
    from src.realtime.streamer import RealtimeStreamer

    class _Adapter:
        name = "ls"
        capacity_pairs = 200

        def __init__(self):
            self.n = 0
            self.closed = False

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
            self.closed = True

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

    with pytest.raises(RuntimeError, match="parser bug"):
        asyncio.run(streamer.pump(asyncio.Event()))

    assert sink.persisted == 3
    assert sink.buffer == 0
    assert adapter.closed is True


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

    with caplog.at_level(logging.CRITICAL):
        assert asyncio.run(streamer.pump(asyncio.Event())) == "disconnect"

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
