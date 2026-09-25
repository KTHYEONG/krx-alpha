"""Daemon supervision cases split from test_daemon.py (child supervision, schedule sleeps, snapshot children, ingest watchdog, and active-phase holiday handling)."""

from __future__ import annotations

import logging

import pytest
from tests.unit.orchestration.daemon_fixtures import (
    _business_trading_day,
    _holiday_trading_day,
    _snapshot_fake_supervisor,
    _snapshot_ready_daemon,
)


@pytest.fixture(autouse=True)
def _verified_aftermarket_capacity(monkeypatch) -> None:
    """Daemon tests simulate a deployed host with verified aftermarket capacity."""
    monkeypatch.setenv("KRX_ALPHA_AFTERMARKET_PAIR_CAPACITY_PER_CONNECTION", "4")




def test_run_collector_daemon_single_cycle() -> None:
    import datetime as dt
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo
    from src.orchestration.daemon import run_collector_daemon

    mock_sleep = MagicMock()
    night = dt.datetime(2026, 9, 8, 20, 0, 0, tzinfo=ZoneInfo('Asia/Seoul'))

    run_collector_daemon(sleep_fn=mock_sleep, max_cycles=1, now_fn=lambda: night)

    mock_sleep.assert_called_once()


def test_run_collector_daemon_streamer_active_spawns_supervised_process(tmp_path, monkeypatch) -> None:
    import datetime as dt
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo
    import src.orchestration.daemon as daemon_mod
    from src.orchestration.daemon import run_collector_daemon

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(daemon_mod, 'resolve_trading_day', lambda ref_date: None)
    monkeypatch.setattr(daemon_mod, 'run_session_orchestration', lambda **kw: True)

    calls: list[str] = []

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            calls.append('constructed')
        def ensure_running(self):
            calls.append('ensure_running')
            return 'started'

    monkeypatch.setattr(daemon_mod, 'ProcessSupervisor', _FakeSupervisor)

    active = dt.datetime(2026, 9, 8, 9, 0, 0, tzinfo=ZoneInfo('Asia/Seoul'))

    run_collector_daemon(sleep_fn=MagicMock(), max_cycles=1, now_fn=lambda: active)

    assert calls == ['constructed', 'ensure_running']


def test_run_collector_daemon_streamer_active_skips_spawn_when_not_ready(tmp_path, monkeypatch) -> None:
    import datetime as dt
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo
    import src.orchestration.daemon as daemon_mod
    from src.orchestration.daemon import run_collector_daemon

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(daemon_mod, 'resolve_trading_day', lambda ref_date: None)
    monkeypatch.setattr(daemon_mod, 'run_session_orchestration', lambda **kw: False)

    constructed: list[str] = []

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            constructed.append('constructed')
        def ensure_running(self):
            constructed.append('ensure_running')
            return 'started'

    monkeypatch.setattr(daemon_mod, 'ProcessSupervisor', _FakeSupervisor)

    active = dt.datetime(2026, 9, 8, 9, 0, 0, tzinfo=ZoneInfo('Asia/Seoul'))

    run_collector_daemon(sleep_fn=MagicMock(), max_cycles=1, now_fn=lambda: active)

    assert constructed == []


def test_run_collector_daemon_streamer_active_logs_circuit_open(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo
    import src.orchestration.daemon as daemon_mod
    from src.orchestration.daemon import run_collector_daemon

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(daemon_mod, 'resolve_trading_day', lambda ref_date: None)
    monkeypatch.setattr(daemon_mod, 'run_session_orchestration', lambda **kw: True)

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            self.cmd = cmd
        def ensure_running(self):
            return 'circuit_open'

    monkeypatch.setattr(daemon_mod, 'ProcessSupervisor', _FakeSupervisor)

    active = dt.datetime(2026, 9, 8, 9, 0, 0, tzinfo=ZoneInfo('Asia/Seoul'))

    with caplog.at_level(logging.CRITICAL):
        run_collector_daemon(sleep_fn=MagicMock(), max_cycles=1, now_fn=lambda: active)

    assert any('circuit_open' in r.message for r in caplog.records)


def test_run_collector_daemon_weekend_sleeps_hourly() -> None:
    import datetime as dt
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo
    from src.orchestration.daemon import run_collector_daemon

    mock_sleep = MagicMock()
    saturday = dt.datetime(2026, 9, 12, 12, 0, 0, tzinfo=ZoneInfo('Asia/Seoul'))

    run_collector_daemon(sleep_fn=mock_sleep, max_cycles=1, now_fn=lambda: saturday)

    mock_sleep.assert_called_once_with(3600.0)


def test_run_collector_daemon_pre_market_sleeps_until_streamer_start() -> None:
    import datetime as dt
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo
    from src.orchestration.daemon import run_collector_daemon

    mock_sleep = MagicMock()
    pre_market = dt.datetime(2026, 9, 10, 8, 0, 0, tzinfo=ZoneInfo('Asia/Seoul'))

    run_collector_daemon(sleep_fn=mock_sleep, max_cycles=1, now_fn=lambda: pre_market)

    mock_sleep.assert_called_once_with(300.0)


def test_run_collector_daemon_configures_logging_with_persistent_dir_flag(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    calls: list[tuple[str, object]] = []
    monkeypatch.setattr(daemon_mod, "configure_logging", lambda component, *, log_dir=None: calls.append((component, log_dir)) or "daemon-77")
    night = dt.datetime(2026, 9, 8, 20, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    monkeypatch.delenv("KRX_ALPHA_PERSISTENT_LOGS", raising=False)
    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: night)
    monkeypatch.setenv("KRX_ALPHA_PERSISTENT_LOGS", "true")
    daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: night)

    assert calls == [("daemon", None), ("daemon", settings.paths.logs_dir)]
    assert "stage=start status=ONLINE timezone=Asia/Seoul run_id=daemon-77" in caplog.text


def test_run_collector_daemon_logs_state_changes_and_ten_minute_heartbeat_only(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.setattr(daemon_mod, "configure_logging", lambda component, *, log_dir=None: "r")
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    kst = ZoneInfo("Asia/Seoul")
    times = iter([
        dt.datetime(2026, 9, 8, 20, 0, 0, tzinfo=kst),
        dt.datetime(2026, 9, 8, 20, 0, 10, tzinfo=kst),
        dt.datetime(2026, 9, 8, 20, 10, 30, tzinfo=kst),
    ])

    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=3, now_fn=lambda: next(times))

    info = [r.getMessage() for r in caplog.records if r.levelno == logging.INFO and r.name == daemon_mod.logger.name]
    assert sum("stage=state_change" in m for m in info) == 1
    assert sum("stage=heartbeat" in m for m in info) == 2
    assert not any("sleeping" in m for m in info)
    assert not any(" cycle=" in m and "stage=" not in m for m in info)


def test_run_collector_daemon_logs_streamer_restart_and_circuit_transition_once(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo

    import src.orchestration.daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(daemon_mod, "configure_logging", lambda component, *, log_dir=None: "r")
    monkeypatch.setattr(daemon_mod, "resolve_trading_day", lambda ref_date: None)
    monkeypatch.setattr(daemon_mod, "run_session_orchestration", lambda **kw: True)
    results = iter(["started", "restarted", "circuit_open", "circuit_open"])

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            self.cmd = cmd
            self.last_exit_code = -9

        def ensure_running(self):
            return next(results)

    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)
    active = dt.datetime(2026, 9, 8, 9, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(sleep_fn=lambda s: None, max_cycles=4, now_fn=lambda: active)

    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    criticals = [r.getMessage() for r in caplog.records if r.levelno == logging.CRITICAL]
    assert warnings.count("[DAEMON] stage=streamer status=RESTARTED exit_code=-9 restarts=1") == 1
    assert sum("reason=circuit_open" in m for m in criticals) == 1
    assert any("stage=streamer status=STARTED" in r.getMessage() for r in caplog.records if r.levelno == logging.INFO)


def test_run_collector_daemon_ingest_watchdog_alerts_stale_journal_once_and_recovers(tmp_path, monkeypatch, caplog) -> None:

    import datetime as dt
    import os
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    kst = ZoneInfo("Asia/Seoul")

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            self.cmd = cmd
            self.last_exit_code = None

        def ensure_running(self):
            return "started"

        def stop(self, *, timeout_s=15.0):
            return "graceful"

    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)
    from src.marketdata.toss_calendar import TradingDay as _TradingDay

    _business = _TradingDay(
        date=dt.date(2026, 9, 14),
        is_business_day=True,
        previous_business_day=dt.date(2026, 9, 11),
        next_business_day=dt.date(2026, 9, 15),
    )
    monkeypatch.setattr(daemon_mod, "resolve_trading_day", lambda ref_date: _business)
    monkeypatch.setattr(daemon_mod, "run_session_orchestration", lambda **kw: True)
    times = [dt.datetime(2026, 9, 14, 10, m, tzinfo=kst) for m in (0, 1, 2, 3)]
    calls = {"n": 0}

    def _now():
        t = times[calls["n"]]
        calls["n"] += 1
        if calls["n"] == 3:
            part = settings.paths.journal_root / "ls" / "H0STCNT0" / "dt=2026-09-14"
            part.mkdir(parents=True, exist_ok=True)
            f = part / "10.jsonl.zst"
            f.write_bytes(b"x")
            os.utime(f, (t.timestamp() - 10, t.timestamp() - 10))
        return t

    with caplog.at_level(logging.WARNING):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=4, now_fn=_now)

    stale = [r.getMessage() for r in caplog.records if "stage=ingest_watchdog status=STALE" in r.getMessage()]
    recovered = [r for r in caplog.records if "stage=ingest_watchdog status=RECOVERED" in r.getMessage()]
    assert stale == ["[DAEMON] stage=ingest_watchdog status=STALE date=2026-09-14 age_s=none"]
    assert [r.levelno for r in caplog.records if "status=STALE" in r.getMessage()] == [logging.CRITICAL]
    assert len(recovered) == 1
    assert recovered[0].levelno == logging.WARNING


def test_run_collector_daemon_ingest_watchdog_skips_open_and_close_auction_windows(tmp_path, monkeypatch, caplog) -> None:

    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    kst = ZoneInfo("Asia/Seoul")

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            self.cmd = cmd
            self.last_exit_code = None

        def ensure_running(self):
            return "started"

        def stop(self, *, timeout_s=15.0):
            return "graceful"

    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)
    monkeypatch.setattr(daemon_mod, "resolve_trading_day", lambda ref_date: None)
    monkeypatch.setattr(daemon_mod, "run_session_orchestration", lambda **kw: True)

    times = iter([dt.datetime(2026, 9, 14, 9, 0, tzinfo=kst), dt.datetime(2026, 9, 14, 9, 4, tzinfo=kst), dt.datetime(2026, 9, 14, 15, 26, tzinfo=kst)])

    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=3, now_fn=lambda: next(times))

    assert "stage=ingest_watchdog" not in caplog.text


def test_daemon_spawns_snapshot_supervisor_when_enabled(tmp_path, monkeypatch) -> None:
    import datetime as dt
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo
    import src.orchestration.daemon as daemon_mod
    from src.orchestration.daemon import run_collector_daemon

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv('KRX_ALPHA_SNAPSHOT_ENABLED', 'true')
    _snapshot_ready_daemon(monkeypatch, daemon_mod)
    created = _snapshot_fake_supervisor(monkeypatch, daemon_mod)

    run_collector_daemon(
        sleep_fn=MagicMock(), max_cycles=1,
        now_fn=lambda: dt.datetime(2026, 9, 8, 9, 0, 0, tzinfo=ZoneInfo('Asia/Seoul')),
    )

    kinds = ['snapshots' if 'collect-snapshots' in sup.cmd else 'streamer' for sup in created]
    assert sorted(kinds) == ['snapshots', 'streamer']
    assert all(sup.ensure_calls == 1 for sup in created)


def test_daemon_does_not_restart_snapshots_after_run_end(tmp_path, monkeypatch) -> None:
    import datetime as dt
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo
    import src.orchestration.daemon as daemon_mod
    from src.orchestration.daemon import run_collector_daemon

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv('KRX_ALPHA_SNAPSHOT_ENABLED', 'true')
    _snapshot_ready_daemon(monkeypatch, daemon_mod)
    created = _snapshot_fake_supervisor(monkeypatch, daemon_mod)

    run_collector_daemon(
        sleep_fn=MagicMock(), max_cycles=1,
        now_fn=lambda: dt.datetime(2026, 9, 8, 15, 39, 30, tzinfo=ZoneInfo('Asia/Seoul')),
    )

    by_kind = {'snapshots' if 'collect-snapshots' in sup.cmd else 'streamer': sup for sup in created}
    assert set(by_kind) == {'snapshots', 'streamer'}
    assert by_kind['snapshots'].ensure_calls == 0
    assert by_kind['streamer'].ensure_calls == 1


def test_daemon_without_snapshot_flag_keeps_existing_spawn_set(tmp_path, monkeypatch) -> None:
    import datetime as dt
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo
    import src.orchestration.daemon as daemon_mod
    from src.orchestration.daemon import run_collector_daemon

    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv('KRX_ALPHA_SNAPSHOT_ENABLED', raising=False)
    _snapshot_ready_daemon(monkeypatch, daemon_mod)
    created = _snapshot_fake_supervisor(monkeypatch, daemon_mod)

    run_collector_daemon(
        sleep_fn=MagicMock(), max_cycles=1,
        now_fn=lambda: dt.datetime(2026, 9, 8, 9, 0, 0, tzinfo=ZoneInfo('Asia/Seoul')),
    )

    assert len(created) == 1
    assert 'collect-snapshots' not in created[0].cmd


def test_daemon_recreates_snapshot_supervisor_after_degraded_retry(tmp_path, monkeypatch) -> None:
    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo
    import src.orchestration.daemon as daemon_mod
    from src.orchestration.daemon import run_collector_daemon
    from src.core.config import CollectorSettings
    from src.universe.ipc import write_candidates

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv('KRX_ALPHA_SNAPSHOT_ENABLED', 'true')
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    settings.paths.candidates.parent.mkdir(parents=True, exist_ok=True)
    write_candidates(settings.paths.candidates, [{"symbol": "005930", "selection_reasons": ["limit_up"]}], rev=20260911)
    created: list = []

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            self.cmd = list(cmd)
            self.stop_calls: list = []
            created.append(self)

        def ensure_running(self):
            return 'started'

        def stop(self, *, timeout_s=15.0):
            self.stop_calls.append(timeout_s)
            return 'graceful'

    monkeypatch.setattr(daemon_mod, 'ProcessSupervisor', _FakeSupervisor)
    monkeypatch.setattr(daemon_mod, 'resolve_trading_day', lambda ref_date: None)
    outcomes = iter([False, True])
    monkeypatch.setattr(daemon_mod, 'run_session_orchestration', lambda **kw: next(outcomes))
    kst = ZoneInfo('Asia/Seoul')
    times = iter([
        dt.datetime(2026, 9, 14, 8, 25, tzinfo=kst),
        dt.datetime(2026, 9, 14, 8, 31, tzinfo=kst),
    ])

    run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=2, now_fn=lambda: next(times))

    snapshots = [sup for sup in created if 'collect-snapshots' in sup.cmd]
    assert len(snapshots) == 2
    assert snapshots[0].stop_calls == [15.0]


def test_daemon_stops_stale_snapshot_supervisor_on_day_change(tmp_path, monkeypatch) -> None:
    import datetime as dt
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo
    import src.orchestration.daemon as daemon_mod
    from src.orchestration.daemon import run_collector_daemon

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv('KRX_ALPHA_SNAPSHOT_ENABLED', 'true')
    _snapshot_ready_daemon(monkeypatch, daemon_mod)
    created = _snapshot_fake_supervisor(monkeypatch, daemon_mod)
    kst = ZoneInfo('Asia/Seoul')
    times = iter([
        dt.datetime(2026, 9, 8, 9, 0, 0, tzinfo=kst),
        dt.datetime(2026, 9, 9, 9, 0, 0, tzinfo=kst),
    ])

    run_collector_daemon(sleep_fn=MagicMock(), max_cycles=2, now_fn=lambda: next(times))

    snapshots = [sup for sup in created if 'collect-snapshots' in sup.cmd]
    assert len(snapshots) == 2
    assert snapshots[0].stop_calls == [15.0]


def test_daemon_logs_snapshot_restart_and_circuit_open(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import logging
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo
    import src.orchestration.daemon as daemon_mod
    from src.orchestration.daemon import run_collector_daemon

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv('KRX_ALPHA_SNAPSHOT_ENABLED', 'true')
    _snapshot_ready_daemon(monkeypatch, daemon_mod)

    class _FlappingSupervisor:
        def __init__(self, *, cmd, breaker=None):
            self.cmd = list(cmd)
            self.last_exit_code = 3

        def ensure_running(self):
            return 'restarted' if 'collect-snapshots' in self.cmd else 'started'

        def stop(self, *, timeout_s=15.0):
            return 'graceful'

    monkeypatch.setattr(daemon_mod, 'ProcessSupervisor', _FlappingSupervisor)

    with caplog.at_level(logging.WARNING):
        run_collector_daemon(
            sleep_fn=MagicMock(), max_cycles=1,
            now_fn=lambda: dt.datetime(2026, 9, 8, 9, 0, 0, tzinfo=ZoneInfo('Asia/Seoul')),
        )

    assert 'stage=snapshots status=RESTARTED' in caplog.text

    class _TrippingSupervisor(_FlappingSupervisor):
        def ensure_running(self):
            return 'circuit_open' if 'collect-snapshots' in self.cmd else 'started'

    monkeypatch.setattr(daemon_mod, 'ProcessSupervisor', _TrippingSupervisor)

    with caplog.at_level(logging.CRITICAL):
        run_collector_daemon(
            sleep_fn=MagicMock(), max_cycles=1,
            now_fn=lambda: dt.datetime(2026, 9, 8, 9, 5, 0, tzinfo=ZoneInfo('Asia/Seoul')),
        )

    assert 'stage=snapshots status=FAIL reason=circuit_open' in caplog.text


def test_daemon_holiday_sleep_aligns_to_state_boundary(tmp_path, monkeypatch) -> None:
    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    holiday = _holiday_trading_day(dt.date(2026, 9, 14))
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c: holiday)
    monkeypatch.setattr(daemon_mod, "run_session_orchestration", lambda **kw: (_ for _ in ()).throw(AssertionError("no orchestration")))
    sleeps: list[float] = []
    now = dt.datetime(2026, 9, 14, 15, 10, tzinfo=ZoneInfo("Asia/Seoul"))
    daemon_mod.run_collector_daemon(settings=settings, sleep_fn=sleeps.append, max_cycles=1, now_fn=lambda: now)
    assert sleeps == [1800.0]


def test_daemon_holiday_resolved_once_across_states(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import logging
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data", after_market_enabled=True)
    kst = ZoneInfo("Asia/Seoul")
    holiday = _holiday_trading_day(dt.date(2026, 9, 24))
    resolver_calls: list[object] = []

    def _counting(day, cache):
        resolver_calls.append(day)
        return holiday

    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", _counting)
    monkeypatch.setattr(daemon_mod, "run_session_orchestration", lambda **kw: (_ for _ in ()).throw(AssertionError("no orchestration")))
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: 0)
    monkeypatch.setattr(daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0})
    monkeypatch.setattr(daemon_mod, "run_eod_remote_l0_purge", lambda *a, **kw: 0)
    monkeypatch.setattr(daemon_mod, "check_backup_freshness", lambda **kw: [])

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            pass

        def ensure_running(self):
            return "started"

        def stop(self, *, timeout_s=15.0):
            return "graceful"

    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)
    times = iter([
        dt.datetime(2026, 9, 24, 8, 25, tzinfo=kst),
        dt.datetime(2026, 9, 24, 12, 0, tzinfo=kst),
        dt.datetime(2026, 9, 24, 16, 30, tzinfo=kst),
        dt.datetime(2026, 9, 24, 20, 5, tzinfo=kst),
    ])
    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=4, now_fn=lambda: next(times))
    assert resolver_calls == [dt.date(2026, 9, 24)]
    assert sum("reason=market_holiday" in r.getMessage() for r in caplog.records) == 1


def test_daemon_unknown_calendar_without_journals_warns_once(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import logging
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    kst = ZoneInfo("Asia/Seoul")
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c: None)
    monkeypatch.setattr(daemon_mod, "run_session_orchestration", lambda **kw: True)

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            pass

        def ensure_running(self):
            return "started"

    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)
    times = iter([dt.datetime(2026, 9, 14, 9, 30, tzinfo=kst), dt.datetime(2026, 9, 14, 9, 40, tzinfo=kst)])
    with caplog.at_level(logging.WARNING):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=2, now_fn=lambda: next(times))
    warns = [r for r in caplog.records if "status=POSSIBLE_HOLIDAY" in r.getMessage()]
    assert len(warns) == 1
    assert warns[0].levelno == logging.WARNING
    assert not any("status=STALE" in r.getMessage() for r in caplog.records)


def test_daemon_unknown_calendar_with_stale_journal_still_critical(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import logging
    import os
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    kst = ZoneInfo("Asia/Seoul")
    now = dt.datetime(2026, 9, 14, 9, 30, tzinfo=kst)
    part = settings.paths.journal_root / "ls" / "H0STCNT0" / "dt=2026-09-14"
    part.mkdir(parents=True, exist_ok=True)
    f = part / "09.jsonl.zst"
    f.write_bytes(b"x")
    os.utime(f, (now.timestamp() - 600, now.timestamp() - 600))
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c: None)
    monkeypatch.setattr(daemon_mod, "run_session_orchestration", lambda **kw: True)

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            pass

        def ensure_running(self):
            return "started"

    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)
    with caplog.at_level(logging.WARNING):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: now)
    assert any("status=STALE" in r.getMessage() and r.levelno == logging.CRITICAL for r in caplog.records)


def test_daemon_stops_stale_supervisors_on_holiday(tmp_path, monkeypatch) -> None:
    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    kst = ZoneInfo("Asia/Seoul")
    business = _business_trading_day(dt.date(2026, 9, 14))
    holiday = _holiday_trading_day(dt.date(2026, 9, 15))

    def _resolve(day, cache):
        return business if day == dt.date(2026, 9, 14) else holiday

    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", _resolve)
    monkeypatch.setattr(daemon_mod, "run_session_orchestration", lambda **kw: True)
    stops: list[str] = []
    ensures: list[str] = []

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            self.cmd = cmd

        def ensure_running(self):
            ensures.append(self.cmd[self.cmd.index("--session-date") + 1])
            return "started"

        def stop(self, *, timeout_s=15.0):
            stops.append(self.cmd[self.cmd.index("--session-date") + 1])
            return "graceful"

    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)
    times = iter([dt.datetime(2026, 9, 14, 9, 0, tzinfo=kst), dt.datetime(2026, 9, 15, 9, 0, tzinfo=kst)])
    daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=2, now_fn=lambda: next(times))
    assert stops == ["2026-09-14"]
    assert ensures == ["2026-09-14"]


def test_holiday_stops_snapshot_child_and_shutdown_collects_regular_child(tmp_path, monkeypatch) -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings, resolve_collector_runtime
    from src.orchestration import daemon as daemon_mod

    now = dt.datetime(2026, 9, 14, 9, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    runtime = resolve_collector_runtime(collector=CollectorSettings(data_root=tmp_path))
    runner = daemon_mod.DaemonRunner(runtime=runtime, shutdown=None, now=lambda: now, sleep=lambda _: None)
    monkeypatch.setattr(
        daemon_mod, "_resolve_trading_day_with_cache", lambda day, cache: _holiday_trading_day(day)
    )
    stops: list[float] = []

    class _Child:
        def stop(self, *, timeout_s: float = 15.0) -> str:
            stops.append(timeout_s)
            return "graceful"

    snapshot = _Child()
    runner.children.snapshot = snapshot
    runner.state.last_snapshot_result = "running"

    runner.step(now)

    assert stops == [15.0]
    assert runner.children.snapshot is None
    assert runner.state.last_snapshot_result is None

    regular = _Child()
    runner.children.regular = regular
    selected: list[object] = []

    def _capture(children, *, deadline_s):
        selected.extend(children)
        assert deadline_s == daemon_mod.SHUTDOWN_CHILD_DEADLINE_S
        return {"graceful": 1, "killed": 0, "not_running": 0}

    monkeypatch.setattr(daemon_mod, "stop_supervisors", _capture)
    runner.stop_children()
    assert selected == [regular]

