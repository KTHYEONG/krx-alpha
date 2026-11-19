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


def _kst_at(hms: str) -> Any:
    import datetime as dt
    from zoneinfo import ZoneInfo

    return dt.datetime.strptime(hms, "%H%M%S").replace(tzinfo=ZoneInfo("Asia/Seoul"))  # noqa: DTZ007


def _refresh_handover(**kwargs: Any) -> tuple[Handover, SwapRequest, list[_Adapter]]:
    request = SwapRequest()
    made: list[_Adapter] = []
    now = {"t": 1000.0}

    def _factory() -> _Adapter:
        adapter = _Adapter()
        made.append(adapter)
        return adapter

    wall = kwargs.pop("wall_clock", lambda: _kst_at("090100"))
    handover = Handover(
        adapter_factory=_factory,
        pairs=_PAIRS,
        request=request,
        max_s=120.0,
        cooldown_s=300.0,
        monotonic=lambda: now["t"],
        stall_lag_s=10.0,
        refresh_cooldown_s=30.0,
        wall_clock=wall,
        **kwargs,
    )
    handover._monotonic_advance = now  # type: ignore[attr-defined]
    return handover, request, made


async def _adopt_standby(handover: Handover, made: list[_Adapter], primary: asyncio.Task[Any]) -> _Adapter:
    handover._request.request()  # noqa: SLF001
    handover.maybe_start(eligible=True)
    assert handover._opening is not None  # noqa: SLF001
    done = {handover._opening}  # noqa: SLF001
    await asyncio.wait(done)
    await handover.step(done, primary)
    assert handover._standby is not None  # noqa: SLF001
    return handover._standby.adapter  # noqa: SLF001


@_sync
async def test_stalled_standby_is_refreshed_on_lag_request() -> None:
    handover, request, made = _refresh_handover()
    primary: asyncio.Task[Any] = asyncio.ensure_future(asyncio.Event().wait())
    try:
        old = await _adopt_standby(handover, made, primary)
        handover.admit(_frame("s1", "090000", "new"), from_standby=True)
        assert handover.standby_lag_s() == 60.0
        request.request()
        handover.maybe_start(eligible=True)
        assert handover._refresh_pending  # noqa: SLF001
        await handover.step(set(), primary)
        assert old.closed
        assert handover._opening is not None  # noqa: SLF001
        assert handover._standby is None  # noqa: SLF001
        assert not primary.done()
    finally:
        primary.cancel()
        await asyncio.gather(primary, return_exceptions=True)
        await handover.aclose()


@_sync
async def test_healthy_standby_is_kept() -> None:
    handover, request, made = _refresh_handover()
    primary: asyncio.Task[Any] = asyncio.ensure_future(asyncio.Event().wait())
    try:
        await _adopt_standby(handover, made, primary)
        handover.admit(_frame("s1", "090059", "new"), from_standby=True)
        assert handover.standby_lag_s() == 1.0
        handover.admit(_frame("s0", "090058", "new"), from_standby=True)
        assert handover.standby_lag_s() == 1.0
        calls_before = len(made)
        request.request()
        handover.maybe_start(eligible=True)
        assert not handover._refresh_pending  # noqa: SLF001
        assert len(made) == calls_before
        assert handover._standby is not None  # noqa: SLF001
    finally:
        primary.cancel()
        await asyncio.gather(primary, return_exceptions=True)
        await handover.aclose()


@_sync
async def test_silent_standby_counts_as_stalled() -> None:
    handover, request, made = _refresh_handover()
    primary: asyncio.Task[Any] = asyncio.ensure_future(asyncio.Event().wait())
    try:
        old = await _adopt_standby(handover, made, primary)
        assert handover.standby_lag_s() == 0.0
        handover._monotonic_advance["t"] += 15.0  # type: ignore[attr-defined]
        assert handover.standby_lag_s() == 15.0
        request.request()
        handover.maybe_start(eligible=True)
        assert handover._refresh_pending  # noqa: SLF001
        await handover.step(set(), primary)
        assert old.closed
    finally:
        primary.cancel()
        await asyncio.gather(primary, return_exceptions=True)
        await handover.aclose()


@_sync
async def test_refresh_cooldown_blocks_second_refresh() -> None:
    handover, request, made = _refresh_handover()
    primary: asyncio.Task[Any] = asyncio.ensure_future(asyncio.Event().wait())
    try:
        await _adopt_standby(handover, made, primary)
        handover.admit(_frame("s1", "090000", "new"), from_standby=True)
        request.request()
        handover.maybe_start(eligible=True)
        await handover.step(set(), primary)
        done_opening = handover._opening  # noqa: SLF001
        assert done_opening is not None
        await asyncio.wait({done_opening})
        await handover.step({done_opening}, primary)
        handover._monotonic_advance["t"] += 10.0  # type: ignore[attr-defined]
        request.request()
        handover.maybe_start(eligible=True)
        assert not handover._refresh_pending  # noqa: SLF001
    finally:
        primary.cancel()
        await asyncio.gather(primary, return_exceptions=True)
        await handover.aclose()


def test_refresh_validation_and_disabled_behaviour() -> None:
    import datetime as dt

    request = SwapRequest()
    with pytest.raises(ValueError, match="stall_lag_s"):
        Handover(adapter_factory=_Adapter, pairs=_PAIRS, request=request, max_s=10, cooldown_s=0, stall_lag_s=0)
    with pytest.raises(ValueError, match="refresh_cooldown_s"):
        Handover(adapter_factory=_Adapter, pairs=_PAIRS, request=request, max_s=10, cooldown_s=0, refresh_cooldown_s=-1)
    assert SwapRequest().peek() is False
    peekable = SwapRequest()
    peekable.request()
    assert peekable.peek() is True
    assert peekable.take() is True
    plain = Handover(adapter_factory=_Adapter, pairs=_PAIRS, request=SwapRequest(), max_s=10, cooldown_s=0)
    assert plain.standby_lag_s() is None
    from src.realtime.connection_handover import _now_kst

    assert isinstance(_now_kst(), dt.datetime)


@_sync
async def test_request_during_opening_is_retained() -> None:
    handover, request, made = _refresh_handover(wall_clock=lambda: _kst_at("090100"))
    primary: asyncio.Task[Any] = asyncio.ensure_future(asyncio.Event().wait())
    try:
        request.request()
        handover.maybe_start(eligible=True)
        assert handover._opening is not None  # noqa: SLF001
        request.request()
        handover.maybe_start(eligible=True)
        assert request.peek() is True
        opening = handover._opening  # noqa: SLF001
        assert opening is not None
        await asyncio.wait({opening})
        await handover.step({opening}, primary)
        assert request.peek() is True
        request.request()
        handover.maybe_start(eligible=True)
        assert not handover._refresh_pending  # noqa: SLF001
    finally:
        primary.cancel()
        await asyncio.gather(primary, return_exceptions=True)
        await handover.aclose()


@_sync
async def test_primary_failure_promotes_fresh_standby_after_refresh() -> None:
    handover, request, made = _refresh_handover()
    primary: asyncio.Task[Any] = asyncio.ensure_future(asyncio.Event().wait())
    try:
        old_standby = await _adopt_standby(handover, made, primary)
        handover.admit(_frame("s1", "090000", "new"), from_standby=True)
        request.request()
        handover.maybe_start(eligible=True)
        await handover.step(set(), primary)
        assert old_standby.closed
        opening = handover._opening  # noqa: SLF001
        assert opening is not None
        await asyncio.wait({opening})
        await handover.step({opening}, primary)
        fresh = handover._standby.adapter  # noqa: SLF001
        assert fresh is not old_standby
        cut = await handover.on_primary_failure()
        assert cut is not None
        assert cut.adapter is fresh
    finally:
        primary.cancel()
        await asyncio.gather(primary, return_exceptions=True)
        await handover.aclose()


@_sync
async def test_refresh_preserves_deduplication_through_opening() -> None:
    handover, request, made = _refresh_handover(window=lambda: True)
    primary: asyncio.Task[Any] = asyncio.ensure_future(asyncio.Event().wait())
    try:
        await _adopt_standby(handover, made, primary)
        assert handover.admit(_frame("before", "090000", "old"), from_standby=False)
        assert handover.admit(_frame("standby", "090000", "standby"), from_standby=True)
        request.request()
        handover.maybe_start(eligible=True, live_connections=2)
        await handover.step(set(), primary)
        assert handover.admit(_frame("during", "090001", "old"), from_standby=False)
        assert not handover.admit(_frame("standby", "090000", "old"), from_standby=False)
        opening = handover._opening  # noqa: SLF001
        assert opening is not None
        await asyncio.wait({opening})
        await handover.step({opening}, primary)
        for raw in ("before", "during", "standby"):
            assert not handover.admit(_frame(raw, "090001", "fresh"), from_standby=True)
    finally:
        primary.cancel()
        await asyncio.gather(primary, return_exceptions=True)
        await handover.aclose()


@_sync
async def test_request_below_stall_threshold_is_retained_until_standby_stalls() -> None:
    wall = {"now": _kst_at("090005")}
    handover, request, made = _refresh_handover(wall_clock=lambda: wall["now"])
    primary: asyncio.Task[Any] = asyncio.ensure_future(asyncio.Event().wait())
    try:
        await _adopt_standby(handover, made, primary)
        handover.admit(_frame("s1", "090000", "new"), from_standby=True)
        assert handover.standby_lag_s() == 5.0
        request.request()
        handover.maybe_start(eligible=True)
        assert not handover._refresh_pending  # noqa: SLF001
        assert request.peek() is True
        wall["now"] = _kst_at("090012")
        handover.maybe_start(eligible=True)
        assert handover._refresh_pending  # noqa: SLF001
        assert request.peek() is False
    finally:
        primary.cancel()
        await asyncio.gather(primary, return_exceptions=True)
        await handover.aclose()


@_sync
async def test_request_during_refresh_cooldown_fires_after_cooldown() -> None:
    handover, request, made = _refresh_handover()
    primary: asyncio.Task[Any] = asyncio.ensure_future(asyncio.Event().wait())
    try:
        await _adopt_standby(handover, made, primary)
        handover.admit(_frame("s1", "090000", "new"), from_standby=True)
        request.request()
        handover.maybe_start(eligible=True)
        await handover.step(set(), primary)
        opening = handover._opening  # noqa: SLF001
        assert opening is not None
        await asyncio.wait({opening})
        await handover.step({opening}, primary)
        handover.admit(_frame("f1", "090000", "fresh"), from_standby=True)
        request.request()
        handover.maybe_start(eligible=True)
        assert not handover._refresh_pending  # noqa: SLF001
        assert request.peek() is True
        handover._monotonic_advance["t"] += 30.0  # type: ignore[attr-defined]
        handover.maybe_start(eligible=True)
        assert handover._refresh_pending  # noqa: SLF001
    finally:
        primary.cancel()
        await asyncio.gather(primary, return_exceptions=True)
        await handover.aclose()


@_sync
async def test_cut_over_discards_pending_request() -> None:
    handover, request, made = _refresh_handover()
    primary: asyncio.Task[Any] = asyncio.ensure_future(asyncio.Event().wait())
    try:
        await _adopt_standby(handover, made, primary)
        handover.admit(_frame("s1", "090059", "new"), from_standby=True)
        request.request()
        handover.maybe_start(eligible=True)
        assert request.peek() is True
        assert await handover.on_primary_failure() is not None
        assert request.peek() is False
        handover._monotonic_advance["t"] += 600.0  # type: ignore[attr-defined]
        handover.maybe_start(eligible=True)
        assert handover._opening is None  # noqa: SLF001
    finally:
        primary.cancel()
        await asyncio.gather(primary, return_exceptions=True)
        await handover.aclose()


@_sync
async def test_primary_does_not_rerecord_dropped_standby_payloads_after_refresh() -> None:
    handover, request, made = _refresh_handover(window=lambda: True)
    primary: asyncio.Task[Any] = asyncio.ensure_future(asyncio.Event().wait())
    try:
        await _adopt_standby(handover, made, primary)
        assert handover.admit(_frame("standby", "090000", "standby"), from_standby=True)
        request.request()
        handover.maybe_start(eligible=True, live_connections=2)
        await handover.step(set(), primary)
        opening = handover._opening  # noqa: SLF001
        assert opening is not None
        await asyncio.wait({opening})
        await handover.step({opening}, primary)
        assert not handover.admit(_frame("standby", "090000", "old"), from_standby=False)
        assert not handover.admit(_frame("standby", "090000", "fresh"), from_standby=True)
        assert handover.admit(_frame("fresh-only", "090001", "fresh"), from_standby=True)
        assert not handover.admit(_frame("fresh-only", "090001", "old"), from_standby=False)
    finally:
        primary.cancel()
        await asyncio.gather(primary, return_exceptions=True)
        await handover.aclose()


def _records(caplog: pytest.LogCaptureFixture, needle: str) -> list[str]:
    return [r.getMessage() for r in caplog.records if needle in r.getMessage()]


@_sync
async def test_cut_over_logs_cause_and_silence(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level("DEBUG")
    rig = _Rig(new=_Adapter())
    rig.start()
    await rig.first_frame()
    rig.request.request()
    await _until(lambda: rig.new.subscribed)
    await asyncio.sleep(0.05)
    rig.old.q.put_nowait(VendorDisconnected("8:code=1006:reason=abnormal closure"))
    await _until(lambda: rig.old.closed)
    rig.new.q.put_nowait(_frame("n1", "090030", "new"))
    await _until(lambda: "n1" in rig.sink.raws)
    assert await rig.finish() == "stopped"
    (line,) = _records(caplog, "status=CUT_OVER")
    assert "reason=primary_failed" in line
    assert "error_type=VendorDisconnected close_code=1006" in line
    assert "primary_silence_s=" in line
    assert "primary_silence_s=na" not in line
    assert "close_reason=abnormal closure" in line
    assert "dropped_duplicates=0" in line


@_sync
async def test_standby_lost_logs_cause(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level("DEBUG")
    rig = _Rig(new=_Adapter())
    rig.start()
    await rig.first_frame()
    rig.request.request()
    await _until(lambda: rig.new.subscribed)
    rig.new.q.put_nowait(VendorDisconnected("8:code=1008:reason=policy"))
    await _until(lambda: bool(_records(caplog, "status=STANDBY_LOST")))
    assert await rig.finish() == "stopped"
    (line,) = _records(caplog, "status=STANDBY_LOST")
    assert "reason=unknown" in line
    assert "error_type=VendorDisconnected close_code=1008" in line
    assert "standby_age_s=" in line
    assert "standby_lag_s=" in line


@_sync
async def test_open_failed_logs_error_type_and_code(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level("DEBUG")
    rig = _Rig(new=_Adapter(connect_error=VendorDisconnected("8:code=1011:reason=x")))
    rig.start()
    await rig.first_frame()
    rig.request.request()
    await _until(lambda: rig.calls == 1 and not rig.handover.active)
    assert await rig.finish() == "stopped"
    (line,) = _records(caplog, "status=OPEN_FAILED")
    assert "error_type=VendorDisconnected" in line
    assert "close_code=1011" in line


@_sync
async def test_state_line_present_absent_opening() -> None:
    handover, _request, made = _refresh_handover()
    assert "standby=absent" in handover.state_line()
    assert "standby_lag_s=na" in handover.state_line()
    primary: asyncio.Task[Any] = asyncio.ensure_future(asyncio.Event().wait())
    handover._request.request()  # noqa: SLF001
    handover.maybe_start(eligible=True)
    assert "standby=opening" in handover.state_line()
    await asyncio.wait({handover._opening})  # type: ignore[arg-type]  # noqa: SLF001
    await handover.step({handover._opening}, primary)  # type: ignore[arg-type]  # noqa: SLF001
    line = handover.state_line()
    assert "standby=present" in line
    assert "stage=handover_state" in line
    assert "dropped_duplicates=0" in line
    assert len(made) == 1
    primary.cancel()
    await handover.aclose()


@_sync
async def test_state_line_failure_never_raises(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level("DEBUG")

    def _broken() -> Any:
        raise RuntimeError("clock")

    handover, _request, made = _refresh_handover(wall_clock=_broken)
    primary: asyncio.Task[Any] = asyncio.ensure_future(asyncio.Event().wait())
    await _adopt_standby(handover, made, primary)
    handover.admit(_frame("s1", "090000", "new"), from_standby=True)
    assert handover.state_line() == ""
    assert len(_records(caplog, "FORMAT_FAILED")) == 1
    primary.cancel()
    await handover.aclose()


@_sync
async def test_state_line_rate_limited(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level("INFO")
    rig = _Rig(new=_Adapter())
    rig.start()
    await rig.first_frame()
    await asyncio.sleep(0.15)
    assert await rig.finish() == "stopped"
    assert len(_records(caplog, "stage=handover_state")) == 1


@_sync
async def test_primary_failure_passes_exception_to_handover() -> None:
    rig = _Rig(new=_Adapter())
    seen: list[BaseException | None] = []
    original = rig.handover.on_primary_failure

    async def _spy(cause: BaseException | None = None) -> Any:
        seen.append(cause)
        return await original(cause)

    rig.handover.on_primary_failure = _spy  # type: ignore[method-assign]
    rig.start()
    await rig.first_frame()
    rig.request.request()
    await _until(lambda: rig.new.subscribed)
    await asyncio.sleep(0.05)
    boom = VendorDisconnected("closed")
    rig.old.q.put_nowait(boom)
    await _until(lambda: rig.old.closed)
    assert seen == [boom]
    assert await rig.finish() == "stopped"


@_sync
async def test_standby_lost_survives_broken_lag_source(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level("DEBUG")

    def _broken() -> Any:
        raise RuntimeError("clock")

    handover, _request, made = _refresh_handover(wall_clock=_broken)
    primary: asyncio.Task[Any] = asyncio.ensure_future(asyncio.Event().wait())
    await _adopt_standby(handover, made, primary)
    handover.admit(_frame("s1", "090000", "new"), from_standby=True)
    await handover._lose_standby("unknown", VendorDisconnected("8:code=1006:reason=x"))  # noqa: SLF001
    (line,) = _records(caplog, "status=STANDBY_LOST")
    assert "standby_lag_s=na" in line
    assert not handover.active
    primary.cancel()


def test_state_interval_must_be_positive() -> None:
    with pytest.raises(ValueError, match="handover_state_interval_s"):
        RealtimeStreamer(adapter=_Adapter(), sink=_Sink(), replay_pairs=_PAIRS, handover_state_interval_s=0.0)
