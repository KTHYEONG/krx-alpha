"""After-close silence limit: the streamer reconnects on silence and pages only through the regular gate."""

from __future__ import annotations

import asyncio
import logging

import pytest

from src.core.alerts import PageKind, page_kind
from src.realtime.contracts import L0Frame, VendorAck, VendorDisconnected
from src.realtime.streamer import OUTAGE_CRITICAL_S, RealtimeStreamer

_NS = 1_000_000_000
_AFTER_CLOSE_LIMIT_S = 900.0


def _frame(at_s: float, seq: int) -> L0Frame:
    return L0Frame("ls", "H0STCNT0", "005930", "x", seq, int(at_s * _NS), 1, f"ls-{seq}")


class _Sink:
    def __init__(self) -> None:
        self.gaps: list[tuple[str, int, int, str]] = []
        self.records: list[L0Frame] = []

    def record(self, frame: L0Frame) -> None:
        self.records.append(frame)

    def note_ack(self, vendor, ack) -> None:
        return None

    def note_gap(self, symbol: str, start: int, end: int, reason: str) -> None:
        self.gaps.append((symbol, start, end, reason))

    def flush(self) -> int:
        return 0


def test_silent_zombie_reconnects_after_close_limit() -> None:
    stop = asyncio.Event()
    mono = {"s": 0.0}
    wall = {"ns": 0}

    def _monotonic() -> float:
        mono["s"] += 100.0
        return mono["s"]

    class _Adapter:
        name = "ls"
        capacity_pairs = 200

        def __init__(self) -> None:
            self.connects = 0
            self.delivered_first = False

        async def connect(self) -> None:
            self.connects += 1

        async def subscribe(self, pairs):
            return [VendorAck(s, t, True, "00000") for s, t in pairs]

        async def recv(self):
            if self.connects == 1:
                if not self.delivered_first:
                    self.delivered_first = True
                    wall["ns"] = 500 * _NS
                    return _frame(500, 1)
                await asyncio.sleep(999)
            wall["ns"] = 1_300 * _NS
            stop.set()
            return _frame(1_300, 2)

        async def aclose(self) -> None:
            return None

    async def _sleep(_s: float) -> None:
        return None

    adapter, sink = _Adapter(), _Sink()
    streamer = RealtimeStreamer(
        adapter=adapter,
        sink=sink,
        replay_pairs=[("005930", "H0STCNT0")],
        silence_limit=lambda: _AFTER_CLOSE_LIMIT_S,
        silence_poll_interval_s=0.01,
        rng=lambda: 1.0,
        wall_ns=lambda: wall["ns"],
        monotonic=_monotonic,
    )

    asyncio.run(streamer.run_forever(stop, sleep=_sleep))

    assert adapter.connects == 2
    assert [f.conn_id for f in sink.records] == ["ls-1", "ls-2"]
    assert sink.gaps == [("005930", 500 * _NS, 1_300 * _NS, "watchdog")]


def test_quiet_frames_within_after_close_limit_never_reconnect() -> None:
    stop = asyncio.Event()
    mono = {"s": 0.0}
    wall = {"ns": 0}

    class _Adapter:
        name = "ls"
        capacity_pairs = 200

        def __init__(self) -> None:
            self.connects = 0
            self.frames = 0

        async def connect(self) -> None:
            self.connects += 1

        async def subscribe(self, pairs):
            return [VendorAck(s, t, True, "00000") for s, t in pairs]

        async def recv(self):
            await asyncio.sleep(0)
            self.frames += 1
            mono["s"] += 450.0
            wall["ns"] = int(mono["s"] * _NS)
            if self.frames == 3:
                stop.set()
            return _frame(mono["s"], self.frames)

        async def aclose(self) -> None:
            return None

    adapter, sink = _Adapter(), _Sink()
    streamer = RealtimeStreamer(
        adapter=adapter,
        sink=sink,
        replay_pairs=[("005930", "H0STCNT0")],
        silence_limit=lambda: _AFTER_CLOSE_LIMIT_S,
        silence_poll_interval_s=0.01,
        wall_ns=lambda: wall["ns"],
        monotonic=lambda: mono["s"],
    )

    reason = asyncio.run(streamer.pump(stop))

    assert reason == "stopped"
    assert adapter.connects == 1
    assert sink.gaps == []


@pytest.mark.parametrize(
    ("limit", "expected_pages"),
    [(_AFTER_CLOSE_LIMIT_S, 1), (None, 0)],
)
def test_reconnect_failures_page_only_while_a_silence_limit_is_armed(caplog, limit, expected_pages) -> None:
    stop = asyncio.Event()
    clock = {"ns": 0}

    class _Adapter:
        name = "ls"
        capacity_pairs = 200

        def __init__(self) -> None:
            self.cycle = 0

        async def connect(self) -> None:
            self.cycle += 1

        async def subscribe(self, pairs):
            return []

        async def recv(self):
            if self.cycle < 5:
                raise VendorDisconnected("closed")
            stop.set()
            return _frame(470, 1)

        async def aclose(self) -> None:
            return None

    async def _sleep(s: float) -> None:
        clock["ns"] += int(s * _NS)

    streamer = RealtimeStreamer(
        adapter=_Adapter(),
        sink=_Sink(),
        replay_pairs=[("005930", "H0STCNT0")],
        silence_limit=lambda: limit,
        backoff_max_s=120.0,
        rng=lambda: 1.0,
        wall_ns=lambda: clock["ns"],
    )

    with caplog.at_level(logging.WARNING):
        asyncio.run(streamer.run_forever(stop, backoff_s=100.0, sleep=_sleep))

    assert OUTAGE_CRITICAL_S == 300.0
    criticals = [r for r in caplog.records if r.levelno == logging.CRITICAL and "stage=stream_outage" in r.getMessage()]
    assert len(criticals) == expected_pages
    if expected_pages:
        assert page_kind(criticals[0]) is PageKind.STREAM_OUTAGE
