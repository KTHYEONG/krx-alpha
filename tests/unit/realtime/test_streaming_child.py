"""Invariant guard tests for the shared streaming child runner."""

from __future__ import annotations

import asyncio
import signal
import time

import pytest

from src.core.config import CollectorSettings
from src.core.errors import SlotBudgetExceededError
from src.realtime import streaming_child
from src.realtime.contracts import VendorDisconnected
from src.realtime.streaming_child import resolve_clock_overrides, run_streaming_child


def test_clock_overrides_explicit_wins() -> None:
    settings = CollectorSettings(ntp_host="a", max_clock_offset_ns=1)
    assert resolve_clock_overrides("b", 2, settings) == streaming_child.ClockOverrides("b", 2)
    assert resolve_clock_overrides(None, None, settings) == streaming_child.ClockOverrides("a", 1)


def _make_http(monkeypatch: pytest.MonkeyPatch) -> type:
    closed: list[bool] = []

    class _FakeHttp:
        async def close(self) -> None:
            closed.append(True)

    _FakeHttp.closed = closed  # type: ignore[attr-defined]
    monkeypatch.setattr("aiohttp.ClientSession", lambda *a, **k: _FakeHttp())
    return _FakeHttp


def test_runner_registers_sigterm_once(tmp_path, monkeypatch) -> None:
    import contextlib as _ctxlib

    registered: list[int] = []
    real_add = asyncio.unix_events._UnixSelectorEventLoop.add_signal_handler

    def _spy(self, sig, callback, *cb_args):  # type: ignore[no-untyped-def]
        registered.append(sig)
        return real_add(self, sig, callback, *cb_args)

    monkeypatch.setattr(asyncio.AbstractEventLoop, "add_signal_handler", _spy)
    with _ctxlib.suppress(AttributeError):
        monkeypatch.setattr(asyncio.BaseEventLoop, "add_signal_handler", _spy)
    with _ctxlib.suppress(AttributeError):
        monkeypatch.setattr(asyncio.unix_events._UnixSelectorEventLoop, "add_signal_handler", _spy)
    _make_http(monkeypatch)

    class _FakeStreamer:
        async def run_forever(self, stop, *, max_cycles=None) -> None:
            return None

    session = object()
    asyncio.run(
        run_streaming_child(
            session=session,
            adapter_factory=lambda http: object(),  # type: ignore[return-value]
            replay_pairs=[],
            silence_limit=lambda: None,
            max_cycles=1,
            streamer_factory=lambda **kw: _FakeStreamer(),
        )
    )
    assert registered == [signal.SIGTERM]


def test_runner_closes_http_on_streamer_error(monkeypatch) -> None:
    closed: list[bool] = []

    class _FakeHttp:
        async def close(self) -> None:
            closed.append(True)

    monkeypatch.setattr("aiohttp.ClientSession", lambda *a, **k: _FakeHttp())

    class _Boom:
        async def run_forever(self, stop, *, max_cycles=None) -> None:
            raise VendorDisconnected("closed")

    persisted: list[bool] = []

    class _Session:
        def persist(self) -> None:
            persisted.append(True)

    with pytest.raises(VendorDisconnected):
        asyncio.run(
            run_streaming_child(
                session=_Session(),
                adapter_factory=lambda http: object(),  # type: ignore[return-value]
                replay_pairs=[],
                silence_limit=lambda: None,
                max_cycles=None,
                streamer_factory=lambda **kw: _Boom(),
            )
        )
    assert closed == [True]
    assert persisted == []


def test_runner_rejects_over_capacity_before_streamer_connects(monkeypatch) -> None:
    from types import SimpleNamespace

    http = _make_http(monkeypatch)
    persisted = []

    def _streamer(**kwargs):
        pytest.fail("over-capacity plans must fail before opening a vendor connection")

    with pytest.raises(SlotBudgetExceededError, match="planned pairs 3 exceed capacity 2"):
        asyncio.run(run_streaming_child(
            session=SimpleNamespace(persist=lambda: persisted.append(True)),
            adapter_factory=lambda http: SimpleNamespace(capacity_pairs=2),
            replay_pairs=[("005930", "trade"), ("005930", "quote"), ("000660", "trade")],
            silence_limit=lambda: None,
            max_cycles=1,
            streamer_factory=_streamer,
        ))
    assert http.closed == [True]
    assert persisted == []


def test_runner_close_writer_stamps_and_persists_twice(monkeypatch) -> None:
    _make_http(monkeypatch)

    class _FakeStreamer:
        async def run_forever(self, stop, *, max_cycles=None) -> None:
            return None

    events: list[str] = []

    class _Manifest:
        writer_closed_at_ns: int = 0

        def __setattr__(self, name, value) -> None:  # type: ignore[no-untyped-def]
            if name == "writer_closed_at_ns":
                events.append("stamp")
            super().__setattr__(name, value)

    class _Session:
        manifest = _Manifest()

        def persist(self) -> None:
            events.append("persist")

    asyncio.run(
        run_streaming_child(
            session=_Session(),
            adapter_factory=lambda http: object(),  # type: ignore[return-value]
            replay_pairs=[],
            silence_limit=lambda: None,
            max_cycles=None,
            streamer_factory=lambda **kw: _FakeStreamer(),
            close_writer=True,
        )
    )
    assert events == ["persist", "stamp", "persist"]


def test_runner_tolerates_partial_session(monkeypatch) -> None:
    _make_http(monkeypatch)

    class _FakeStreamer:
        async def run_forever(self, stop, *, max_cycles=None) -> None:
            return None

    asyncio.run(
        run_streaming_child(
            session=object(),
            adapter_factory=lambda http: object(),  # type: ignore[return-value]
            replay_pairs=[],
            silence_limit=lambda: None,
            max_cycles=None,
            streamer_factory=lambda **kw: _FakeStreamer(),
        )
    )


def test_deadline_stops_without_sigterm(monkeypatch) -> None:
    _make_http(monkeypatch)
    seen_stop: list[bool] = []
    created: list[asyncio.Task[None]] = []
    real_ensure = asyncio.ensure_future

    def _spy_ensure(coro, *a, **k):  # type: ignore[no-untyped-def]
        task = real_ensure(coro, *a, **k)
        created.append(task)
        return task

    monkeypatch.setattr(asyncio, "ensure_future", _spy_ensure)

    class _Waiting:
        async def run_forever(self, stop, *, max_cycles=None) -> None:
            await stop.wait()
            seen_stop.append(stop.is_set())

    deadline_ns = time.time_ns() + 50_000_000
    started = time.monotonic()
    async def _run() -> None:
        await asyncio.wait_for(run_streaming_child(
            session=object(),
            adapter_factory=lambda http: object(),  # type: ignore[return-value]
            replay_pairs=[],
            silence_limit=lambda: None,
            max_cycles=None,
            streamer_factory=lambda **kw: _Waiting(),
            stop_deadline_ns=deadline_ns,
        ), timeout=2.0)
        assert time.time_ns() >= deadline_ns
        assert created
        assert all(task.done() for task in created)

    asyncio.run(_run())
    elapsed = time.monotonic() - started
    assert seen_stop == [True]
    assert elapsed < 5.0
    assert created
    assert all(task.done() for task in created)


@pytest.mark.parametrize("failure_stage", ["http", "adapter", "streamer", "run", "persist", None])
def test_runner_cleans_deadline_before_returning(monkeypatch, failure_stage) -> None:
    from types import SimpleNamespace

    tasks = []
    closed = []
    persisted = []
    real_ensure = asyncio.ensure_future

    def _ensure(coro):
        task = real_ensure(coro)
        tasks.append(task)
        return task

    class _Http:
        def __init__(self):
            if failure_stage == "http":
                raise RuntimeError("http")

        async def close(self):
            closed.append(True)

    def _adapter(http):
        if failure_stage == "adapter":
            raise RuntimeError("adapter")
        return object()

    class _Streamer:
        def __init__(self, **kwargs):
            if failure_stage == "streamer":
                raise RuntimeError("streamer")

        async def run_forever(self, stop, *, max_cycles):
            await asyncio.sleep(0)
            if failure_stage == "run":
                raise RuntimeError("run")

    def _persist():
        persisted.append(True)
        if failure_stage == "persist":
            raise RuntimeError("persist")

    monkeypatch.setattr(asyncio, "ensure_future", _ensure)
    monkeypatch.setattr(streaming_child.aiohttp, "ClientSession", _Http)

    async def _run():
        child = run_streaming_child(
            session=SimpleNamespace(persist=_persist),
            adapter_factory=_adapter,
            replay_pairs=[],
            silence_limit=lambda: None,
            max_cycles=1,
            streamer_factory=_Streamer,
            stop_deadline_ns=time.time_ns() + 60_000_000_000,
        )
        if failure_stage is None:
            await child
        else:
            with pytest.raises(RuntimeError, match=failure_stage):
                await child
        assert len(tasks) == 1
        assert tasks[0].cancelled()

    asyncio.run(_run())
    assert closed == ([] if failure_stage == "http" else [True])
    assert persisted == ([True] if failure_stage in (None, "persist") else [])


def test_runner_builds_a_handover_whose_standby_comes_from_the_adapter_factory(monkeypatch) -> None:
    from types import SimpleNamespace

    from src.realtime.connection_handover import Handover, SwapRequest
    from src.realtime.streaming_child import HandoverSettings

    _make_http(monkeypatch)
    built: list[object] = []
    captured: dict[str, object] = {}

    def _factory(http):
        adapter = SimpleNamespace(capacity_pairs=10)
        built.append(adapter)
        return adapter

    class _FakeStreamer:
        def __init__(self, **kwargs) -> None:
            captured.update(kwargs)

        async def run_forever(self, stop, *, max_cycles=None) -> None:
            return None

    asyncio.run(run_streaming_child(
        session=SimpleNamespace(),
        adapter_factory=_factory,
        replay_pairs=[("005930", "trade")],
        silence_limit=lambda: None,
        max_cycles=1,
        streamer_factory=_FakeStreamer,
        handover=HandoverSettings(SwapRequest(), 120.0, 300.0),
    ))

    handover = captured["handover"]
    assert isinstance(handover, Handover)
    assert len(built) == 1
    assert handover._factory() is built[1]  # noqa: SLF001 - the standby adapter must come from the same factory


def test_runner_feeds_recorded_frames_to_the_lag_monitor(monkeypatch) -> None:
    from types import SimpleNamespace

    from src.realtime.lag_monitor import LagMonitor
    from src.realtime.streaming_child import LagMonitoringSink

    _make_http(monkeypatch)
    captured: dict[str, object] = {}

    class _FakeStreamer:
        def __init__(self, **kwargs) -> None:
            captured.update(kwargs)

        async def run_forever(self, stop, *, max_cycles=None) -> None:
            return None

    monitor = LagMonitor(threshold_s=10.0)
    asyncio.run(run_streaming_child(
        session=SimpleNamespace(),
        adapter_factory=lambda http: SimpleNamespace(capacity_pairs=10),
        replay_pairs=[("005930", "trade")],
        silence_limit=lambda: None,
        max_cycles=1,
        streamer_factory=_FakeStreamer,
        lag_monitor=monitor,
    ))

    sink = captured["sink"]
    assert isinstance(sink, LagMonitoringSink)
    assert sink._monitor is monitor  # noqa: SLF001 - wiring guard: every recorded frame reaches the monitor
