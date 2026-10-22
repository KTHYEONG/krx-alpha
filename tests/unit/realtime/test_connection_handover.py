from __future__ import annotations

import asyncio
import functools
from collections.abc import Awaitable, Callable
from typing import Any

import pytest

from src.realtime.connection_handover import Handover, SwapRequest
from src.realtime.contracts import L0Frame, VendorAck, VendorDisconnected
from src.realtime.lag_monitor import event_second_of
from src.realtime.streamer import RealtimeStreamer

_PAIRS = [("005930", "H0STCNT0")]


def _sync(test: Callable[..., Awaitable[None]]) -> Callable[..., None]:
    @functools.wraps(test)
    def runner(*args: Any, **kwargs: Any) -> None:
        asyncio.run(test(*args, **kwargs))

    return runner


class _Adapter:
    name = "ls"
    capacity_pairs = 200

    def __init__(self, *, accept: bool = True, connect_error: Exception | None = None, hang_connect: bool = False) -> None:
        self.q: asyncio.Queue[object] = asyncio.Queue()
        self.accept = accept
        self.connect_error = connect_error
        self.hang_connect = hang_connect
        self.closed = False
        self.subscribed = False

    async def connect(self) -> None:
        if self.hang_connect:
            await asyncio.Event().wait()
        if self.connect_error is not None:
            raise self.connect_error

    async def subscribe(self, pairs: list[tuple[str, str]]) -> list[VendorAck]:
        self.subscribed = True
        return [VendorAck(sym, stream, self.accept, "0") for sym, stream in pairs]

    async def recv(self) -> L0Frame:
        item = await self.q.get()
        if isinstance(item, Exception):
            raise item
        assert isinstance(item, L0Frame)
        return item

    async def aclose(self) -> None:
        self.closed = True


class _Sink:
    def __init__(self) -> None:
        self.raws: list[str] = []

    def record(self, frame: L0Frame) -> None:
        self.raws.append(frame.raw)

    def note_ack(self, vendor: str, ack: VendorAck) -> None:
        return None

    def note_gap(self, symbol: str, start_ns: int, end_ns: int, reason: str) -> None:
        return None

    def flush(self) -> int:
        return 0


def _frame(raw: str, event: str, conn: str) -> L0Frame:
    return L0Frame("ls", "H0STCNT0", "005930", raw, 1, 2, 1, conn, exchange_event_time=event)


async def _until(cond: Callable[[], bool]) -> None:
    async with asyncio.timeout(2.0):
        while not cond():  # noqa: ASYNC110 - polling a plain predicate in a test helper
            await asyncio.sleep(0.005)


class _Rig:
    def __init__(
        self, *, new: _Adapter, max_s: float = 60.0, cooldown_s: float = 0.0, window: Callable[[], bool] | None = None
    ) -> None:
        self.old = _Adapter()
        self.new = new
        self.calls = 0
        self.request = SwapRequest()

        def _factory() -> _Adapter:
            self.calls += 1
            return self.new

        self.handover = Handover(
            adapter_factory=_factory, pairs=_PAIRS, request=self.request, max_s=max_s, cooldown_s=cooldown_s,
            window=window,
        )
        self.sink = _Sink()
        self.stop = asyncio.Event()
        self.streamer = RealtimeStreamer(
            adapter=self.old,
            sink=self.sink,
            replay_pairs=_PAIRS,
            handover=self.handover,
            silence_limit=lambda: 30.0,
            silence_poll_interval_s=0.01,
            flush_interval_s=0.01,
        )
        self.task: asyncio.Task[str] | None = None

    def start(self) -> None:
        self.task = asyncio.ensure_future(self.streamer.pump(self.stop))

    async def first_frame(self) -> None:
        self.old.q.put_nowait(_frame("o0", "090000", "old"))
        await _until(lambda: self.sink.raws == ["o0"])

    async def finish(self) -> str:
        self.stop.set()
        assert self.task is not None
        return await asyncio.wait_for(self.task, 2.0)


@_sync
async def test_cut_over_after_old_connection_catches_up_without_duplicates() -> None:
    rig = _Rig(new=_Adapter())
    rig.start()
    await rig.first_frame()
    rig.request.request()
    await _until(lambda: rig.new.subscribed)
    rig.new.q.put_nowait(_frame("x1", "090020", "new"))
    await _until(lambda: "x1" in rig.sink.raws)
    rig.old.q.put_nowait(_frame("o1", "090010", "old"))
    await _until(lambda: "o1" in rig.sink.raws)
    assert not rig.old.closed
    rig.old.q.put_nowait(_frame("x1", "090020", "old"))
    await _until(lambda: rig.old.closed)
    rig.new.q.put_nowait(_frame("n2", "090021", "new"))
    await _until(lambda: "n2" in rig.sink.raws)
    assert await rig.finish() == "stopped"
    assert sorted(rig.sink.raws) == ["n2", "o0", "o1", "x1"]
    assert rig.new.closed


@_sync
async def test_old_connection_failure_promotes_live_standby() -> None:
    rig = _Rig(new=_Adapter())
    rig.start()
    await rig.first_frame()
    rig.request.request()
    await _until(lambda: rig.new.subscribed)
    await asyncio.sleep(0.05)
    rig.old.q.put_nowait(VendorDisconnected("closed"))
    await _until(lambda: rig.old.closed)
    rig.new.q.put_nowait(_frame("n1", "090030", "new"))
    await _until(lambda: "n1" in rig.sink.raws)
    assert not rig.task.done()  # type: ignore[union-attr]
    assert await rig.finish() == "stopped"
    assert rig.new.closed


@_sync
async def test_primary_failure_without_standby_still_disconnects() -> None:
    rig = _Rig(new=_Adapter())
    rig.start()
    await rig.first_frame()
    rig.old.q.put_nowait(VendorDisconnected("closed"))
    assert await asyncio.wait_for(rig.task, 2.0) == "disconnect"  # type: ignore[arg-type]
    assert rig.calls == 0


@_sync
@pytest.mark.parametrize(
    "new",
    [
        _Adapter(connect_error=VendorDisconnected("refused")),
        _Adapter(accept=False),
    ],
)
async def test_standby_open_failure_leaves_live_connection_untouched(new: _Adapter) -> None:
    rig = _Rig(new=new)
    rig.start()
    await rig.first_frame()
    rig.request.request()
    await _until(lambda: rig.calls == 1 and not rig.handover.active)
    rig.old.q.put_nowait(_frame("o1", "090001", "old"))
    await _until(lambda: "o1" in rig.sink.raws)
    assert await rig.finish() == "stopped"
    assert new.closed


@_sync
async def test_forced_cut_over_when_old_never_catches_up() -> None:
    rig = _Rig(new=_Adapter(), max_s=0.05)
    rig.start()
    await rig.first_frame()
    rig.request.request()
    await _until(lambda: rig.old.closed)
    rig.new.q.put_nowait(_frame("n1", "090040", "new"))
    await _until(lambda: "n1" in rig.sink.raws)
    assert await rig.finish() == "stopped"


@_sync
async def test_requests_before_first_frame_or_within_cooldown_are_ignored() -> None:
    rig = _Rig(new=_Adapter(), cooldown_s=3600.0)
    rig.start()
    rig.request.request()
    await asyncio.sleep(0.05)
    assert rig.calls == 0
    await rig.first_frame()
    rig.request.request()
    await _until(lambda: rig.new.subscribed)
    rig.new.q.put_nowait(_frame("x1", "090005", "new"))
    rig.old.q.put_nowait(_frame("x1", "090005", "old"))
    await _until(lambda: rig.old.closed)
    rig.request.request()
    await asyncio.sleep(0.05)
    assert rig.calls == 1
    assert await rig.finish() == "stopped"


@_sync
async def test_standby_receive_failure_keeps_old_connection() -> None:
    new = _Adapter()
    rig = _Rig(new=new)
    rig.start()
    await rig.first_frame()
    rig.request.request()
    await _until(lambda: new.subscribed)
    new.q.put_nowait(VendorDisconnected("lost"))
    await _until(lambda: new.closed)
    rig.old.q.put_nowait(_frame("o1", "090001", "old"))
    await _until(lambda: "o1" in rig.sink.raws)
    assert not rig.old.closed
    assert await rig.finish() == "stopped"


@_sync
async def test_stop_while_standby_is_connecting_cancels_and_closes_it() -> None:
    new = _Adapter(hang_connect=True)
    rig = _Rig(new=new)
    rig.start()
    await rig.first_frame()
    rig.request.request()
    await _until(lambda: rig.handover.active)
    assert await rig.finish() == "stopped"
    assert new.closed
    assert not rig.handover.active


@_sync
async def test_aclose_closes_a_bring_up_that_already_finished() -> None:
    new = _Adapter()
    request = SwapRequest()
    handover = Handover(adapter_factory=lambda: new, pairs=_PAIRS, request=request, max_s=10, cooldown_s=0)
    request.request()
    handover.maybe_start(eligible=True)
    await _until(lambda: new.subscribed)
    await asyncio.sleep(0.01)
    await handover.aclose()
    assert new.closed
    await handover.aclose()


def test_swap_request_is_one_shot_and_arguments_are_validated() -> None:
    request = SwapRequest()
    assert not request.take()
    request.request()
    assert request.take()
    assert not request.take()
    with pytest.raises(ValueError, match="max_s"):
        Handover(adapter_factory=_Adapter, pairs=_PAIRS, request=request, max_s=0, cooldown_s=0)


def test_event_second_of_reads_field_or_payload_and_rejects_garbage() -> None:
    assert event_second_of(_frame("{}", "090001", "c")) == 9 * 3600 + 1
    assert event_second_of(_frame('{"body":{"hotime":"153000"}}', "", "c")) == 15 * 3600 + 30 * 60
    assert event_second_of(_frame('{"body":{"chetime":"256100"}}', "", "c")) is None
    assert event_second_of(_frame("{}", "", "c")) is None


@_sync
async def test_retiring_the_old_adapter_tolerates_a_failing_close() -> None:
    class _BrokenClose(_Adapter):
        async def aclose(self) -> None:
            raise OSError("socket already gone")

    rig = _Rig(new=_Adapter(), max_s=0.05)
    rig.old = _BrokenClose()
    rig.streamer._adapter = rig.old  # noqa: SLF001 - swap in the failing adapter before the pump starts
    rig.start()
    await rig.first_frame()
    rig.request.request()
    await _until(lambda: rig.streamer._adapter is rig.new)  # noqa: SLF001
    rig.new.q.put_nowait(_frame("n1", "090040", "new"))
    await _until(lambda: "n1" in rig.sink.raws)
    assert await rig.finish() == "stopped"


@_sync
async def test_window_starts_a_standby_ahead_of_time_and_holds_both_connections() -> None:
    window = {"open": True}
    rig = _Rig(new=_Adapter(), max_s=0.05, window=lambda: window["open"])
    rig.start()
    await rig.first_frame()
    await _until(lambda: rig.new.subscribed)
    rig.new.q.put_nowait(_frame("x1", "090005", "new"))
    rig.old.q.put_nowait(_frame("x1", "090005", "old"))
    await _until(lambda: "x1" in rig.sink.raws)
    await asyncio.sleep(0.15)
    assert not rig.old.closed
    assert rig.streamer._adapter is rig.old  # noqa: SLF001
    window["open"] = False
    await _until(lambda: rig.old.closed)
    assert await rig.finish() == "stopped"
    assert rig.calls == 1


@_sync
async def test_primary_failure_inside_the_window_promotes_the_ready_standby() -> None:
    rig = _Rig(new=_Adapter(), window=lambda: True)
    rig.start()
    await rig.first_frame()
    await _until(lambda: rig.new.subscribed)
    await asyncio.sleep(0.05)
    rig.old.q.put_nowait(VendorDisconnected("closed"))
    await _until(lambda: rig.old.closed)
    rig.new.q.put_nowait(_frame("n1", "090030", "new"))
    await _until(lambda: "n1" in rig.sink.raws)
    assert await rig.finish() == "stopped"


@_sync
async def test_duplicate_memory_is_bounded() -> None:
    from src.realtime import connection_handover

    rig = _Rig(new=_Adapter(), window=lambda: True)
    rig.start()
    await rig.first_frame()
    await _until(lambda: rig.new.subscribed)
    await asyncio.sleep(0.05)
    original = connection_handover._SEEN_CAP  # noqa: SLF001
    connection_handover._SEEN_CAP = 8  # noqa: SLF001
    try:
        for index in range(40):
            rig.old.q.put_nowait(_frame(f"o{index}", "090001", "old"))
        await _until(lambda: len(rig.sink.raws) >= 41)
        assert len(rig.handover._seen) <= 9  # noqa: SLF001
    finally:
        connection_handover._SEEN_CAP = original  # noqa: SLF001
    assert await rig.finish() == "stopped"
