from __future__ import annotations

import asyncio

from src.realtime.contracts import L0Frame, VendorAck, VendorDisconnected
from src.realtime.streamer import RealtimeStreamer


class _Adapter:
    name = "ls"
    capacity_pairs = 200

    def __init__(self, frames: list[L0Frame]) -> None:
        self.subscribed: list[tuple[str, str]] | None = None
        self._frames = list(frames)

    async def connect(self) -> None:
        return None

    async def subscribe(self, pairs):
        self.subscribed = list(pairs)
        return [VendorAck(s, t, True, "00000") for s, t in pairs]

    async def recv(self) -> L0Frame:
        if self._frames:
            return self._frames.pop(0)
        await asyncio.sleep(0)
        raise VendorDisconnected("closed")

    async def aclose(self) -> None:
        return None


class _Sink:
    def __init__(self) -> None:
        self.records: list[L0Frame] = []

    def record(self, frame: L0Frame) -> None:
        self.records.append(frame)

    def note_ack(self, vendor, ack) -> None:
        return None

    def note_gap(self, *args) -> None:
        return None

    def flush(self) -> int:
        return len(self.records)


class _Watch:
    def __init__(self, pairs: list[tuple[str, str]]) -> None:
        self._pairs = pairs
        self.started_with: list[object] = []
        self.cancelled = False

    def pairs(self) -> list[tuple[str, str]]:
        return list(self._pairs)

    async def run(self, adapter_of, stop: asyncio.Event) -> None:
        self.started_with.append(adapter_of())
        try:
            await stop.wait()
        except asyncio.CancelledError:
            self.cancelled = True
            raise


def _frame(raw: str) -> L0Frame:
    return L0Frame("ls", "H0STCNT0", "005930", raw, 1, 2, 1, "ls-1")


def test_connect_uses_provider_pairs_and_watch_runs_then_is_cancelled() -> None:
    adapter = _Adapter([_frame("a")])
    sink = _Sink()
    watch = _Watch([("005930", "H0STCNT0"), ("000660", "H0STCNT0")])
    streamer = RealtimeStreamer(
        adapter=adapter,  # type: ignore[arg-type]
        sink=sink,  # type: ignore[arg-type]
        replay_pairs=[("005930", "H0STCNT0")],
        flush_every=1,
        pairs_provider=watch.pairs,
        live_watch=watch,  # type: ignore[arg-type]
    )

    reason = asyncio.run(streamer.pump(asyncio.Event()))

    assert reason == "disconnect"
    assert adapter.subscribed == [("005930", "H0STCNT0"), ("000660", "H0STCNT0")]
    assert watch.started_with == [adapter]
    assert watch.cancelled is True
    assert [f.raw for f in sink.records] == ["a"]


def test_without_provider_connect_uses_static_replay_pairs() -> None:
    adapter = _Adapter([])
    streamer = RealtimeStreamer(
        adapter=adapter,  # type: ignore[arg-type]
        sink=_Sink(),  # type: ignore[arg-type]
        replay_pairs=[("005930", "H0STCNT0")],
    )

    asyncio.run(streamer.pump(asyncio.Event()))

    assert adapter.subscribed == [("005930", "H0STCNT0")]
