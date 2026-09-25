"""Daemon shutdown cases split from test_daemon.py (shutdown handling, signal handlers, and daemon entrypoint)."""

from __future__ import annotations

import logging

import pytest


@pytest.fixture(autouse=True)
def _verified_aftermarket_capacity(monkeypatch) -> None:
    """Daemon tests simulate a deployed host with verified aftermarket capacity."""
    monkeypatch.setenv("KRX_ALPHA_AFTERMARKET_PAIR_CAPACITY_PER_CONNECTION", "4")


@pytest.fixture(autouse=True)


def _verified_aftermarket_capacity(monkeypatch) -> None:
    """Daemon tests simulate a deployed host with verified aftermarket capacity."""
    monkeypatch.setenv("KRX_ALPHA_AFTERMARKET_PAIR_CAPACITY_PER_CONNECTION", "4")


def test_run_collector_daemon_main_invokes_runner(monkeypatch) -> None:
    import src.orchestration.daemon as daemon_mod

    calls: list[str] = []
    monkeypatch.setattr(daemon_mod, "run_collector_daemon", lambda **kw: calls.append("ran"))

    daemon_mod.main()

    assert calls == ["ran"]


def test_shutdown_before_first_cycle_starts_nothing(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import threading
    from zoneinfo import ZoneInfo

    import src.orchestration.daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(daemon_mod, "resolve_trading_day", lambda ref_date: (_ for _ in ()).throw(AssertionError("must not resolve")))
    ensured: list[str] = []

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            raise AssertionError("must not construct supervisor")

        def ensure_running(self):
            ensured.append("ensure")
            return "started"

    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)
    monkeypatch.setattr(daemon_mod, "run_session_orchestration", lambda **kw: (_ for _ in ()).throw(AssertionError("must not orchestrate")))
    shutdown = threading.Event()
    shutdown.set()
    active = dt.datetime(2026, 9, 8, 9, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(sleep_fn=lambda s: None, max_cycles=5, now_fn=lambda: active, shutdown=shutdown)

    assert ensured == []
    assert "stage=shutdown status=STOPPED" in caplog.text


def test_shutdown_during_session_stops_all_children_together(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import threading
    from types import SimpleNamespace
    from zoneinfo import ZoneInfo

    import src.orchestration.daemon as daemon_mod
    from src.core.config import CollectorSettings
    from src.realtime.contracts import MarketVenue
    from src.realtime.kis_sharding import AftermarketShard

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("KRX_ALPHA_SNAPSHOT_ENABLED", "true")
    settings = CollectorSettings(data_root=tmp_path / "data", after_market_enabled=True)
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda *a: None)
    monkeypatch.setattr(daemon_mod, "run_session_orchestration", lambda **kw: True)
    monkeypatch.setattr(daemon_mod, "refresh_aftermarket_candidates", lambda **kw: None)
    monkeypatch.setattr(daemon_mod, "_build_kis_client", lambda paths: object())
    monkeypatch.setattr(
        daemon_mod,
        "read_candidate_snapshot",
        lambda *a, **kw: SimpleNamespace(candidates=({"symbol": "005930"}, {"symbol": "000001"})),
    )
    monkeypatch.setattr(daemon_mod, "load_kis_data_credentials", lambda: ())
    shards = (
        AftermarketShard(MarketVenue.NXT, 0, ("005930",), ("H0NXCNT0", "H0NXASP0"), "0", "id0"),
        AftermarketShard(MarketVenue.KRX, 0, ("000001",), ("H0STCNT0", "H0STASP0"), "1", "id1"),
    )
    monkeypatch.setattr(daemon_mod, "plan_aftermarket_shards", lambda **kw: shards)

    ensure_calls: list[str] = []

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            self.cmd = list(cmd)
            self._running = True

        def ensure_running(self):
            ensure_calls.append("ensure")
            return "started"

        def request_stop(self):
            return True

        def wait_stopped(self, timeout_s: float = 0.0):
            return "graceful"

    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)
    seen: dict[str, object] = {}
    real_stop = daemon_mod.stop_supervisors

    def _capture(supervisors, *, deadline_s: float):
        seen["n"] = len(list(supervisors))
        seen["deadline"] = deadline_s
        return real_stop(supervisors, deadline_s=deadline_s)

    monkeypatch.setattr(daemon_mod, "stop_supervisors", _capture)

    shutdown = threading.Event()
    calls = {"n": 0}

    def _sleep(sec: float) -> None:
        calls["n"] += 1
        if calls["n"] >= 2:
            shutdown.set()

    kst = ZoneInfo("Asia/Seoul")
    times = iter([
        dt.datetime(2026, 9, 8, 9, 0, 0, tzinfo=kst),
        dt.datetime(2026, 9, 8, 18, 0, 0, tzinfo=kst),
        dt.datetime(2026, 9, 8, 18, 1, 0, tzinfo=kst),
    ])

    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=_sleep, max_cycles=10, now_fn=lambda: next(times), shutdown=shutdown)

    assert seen["n"] == 4
    assert seen["deadline"] == daemon_mod.SHUTDOWN_CHILD_DEADLINE_S
    n_after_shutdown = len(ensure_calls)
    assert n_after_shutdown >= 3
    assert "stage=shutdown status=STOPPED" in caplog.text


def test_shutdown_wakes_default_sleep(tmp_path, monkeypatch) -> None:
    import datetime as dt
    import threading
    import time
    from zoneinfo import ZoneInfo

    import src.orchestration.daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    shutdown = threading.Event()
    night = dt.datetime(2026, 9, 8, 20, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    def _set_later():
        time.sleep(0.05)
        shutdown.set()

    t = threading.Thread(target=_set_later)
    t.start()
    start = time.monotonic()
    daemon_mod.run_collector_daemon(sleep_fn=None, max_cycles=10, now_fn=lambda: night, shutdown=shutdown)
    elapsed = time.monotonic() - start
    t.join()
    assert elapsed < 1.0


def test_shutdown_logs_single_stop_event(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import threading
    from zoneinfo import ZoneInfo

    import src.orchestration.daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    shutdown = threading.Event()
    shutdown.set()
    night = dt.datetime(2026, 9, 8, 20, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(sleep_fn=lambda s: None, max_cycles=5, now_fn=lambda: night, shutdown=shutdown)

    records = [r for r in caplog.records if "stage=shutdown status=STOPPED" in r.getMessage()]
    assert len(records) == 1
    assert "graceful=" in records[0].getMessage()


def test_main_installs_sigterm_and_sigint_handlers(monkeypatch) -> None:
    import signal
    import threading

    import src.orchestration.daemon as daemon_mod

    registered: dict[int, object] = {}
    monkeypatch.setattr(signal, "signal", lambda sig, handler: registered.__setitem__(sig, handler) or handler)
    seen: dict[str, object] = {}

    def _fake_run(*, shutdown=None, **kw):
        seen["shutdown"] = shutdown
        assert isinstance(shutdown, threading.Event)
        registered[signal.SIGTERM](signal.SIGTERM, None)

    monkeypatch.setattr(daemon_mod, "run_collector_daemon", _fake_run)

    daemon_mod.main()

    assert signal.SIGTERM in registered
    assert signal.SIGINT in registered
    assert isinstance(seen["shutdown"], threading.Event)
    assert seen["shutdown"].is_set() is True


def test_shutdown_deadline_fits_compose_grace() -> None:
    import pathlib

    import yaml

    from src.orchestration.daemon import SHUTDOWN_CHILD_DEADLINE_S

    raw = yaml.safe_load(pathlib.Path("docker-compose.yml").read_text(encoding="utf-8"))
    grace_raw = raw["services"]["krx-collector"]["stop_grace_period"]
    grace_s = float(str(grace_raw).rstrip("s"))
    assert grace_s >= SHUTDOWN_CHILD_DEADLINE_S + 5


def test_main_handler_falls_back_to_numeric_signal_name(monkeypatch) -> None:
    import signal

    import src.orchestration.daemon as daemon_mod

    registered: dict[int, object] = {}
    monkeypatch.setattr(signal, "signal", lambda sig, handler: registered.__setitem__(sig, handler) or handler)
    seen: dict[str, object] = {}

    def _fake_run(*, shutdown=None, **kw):
        seen["shutdown"] = shutdown
        registered[signal.SIGTERM](999999, None)

    monkeypatch.setattr(daemon_mod, "run_collector_daemon", _fake_run)

    daemon_mod.main()

    assert seen["shutdown"].signal_name == "999999"  # type: ignore[attr-defined]


