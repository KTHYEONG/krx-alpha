"""Daemon liveness: crash boundary, dead-man's-switch pings, lifecycle records."""

from __future__ import annotations

import pytest


def _enable_liveness(monkeypatch) -> None:
    monkeypatch.setenv("KRX_ALPHA_LIVENESS_HEALTHCHECK_URL", "https://hc.example.com/ping/token")


def _install_fake_pinger(monkeypatch, daemon_mod):
    calls: list[tuple] = []

    class _FakePinger:
        def __init__(self, url, *, timeout_s, run_id):
            calls.append(("init", url, timeout_s, run_id))

        def start(self):
            calls.append(("start",))
            return True

        def success(self):
            calls.append(("success",))
            return True

        def fail(self, reason):
            calls.append(("fail", reason))
            return True

    monkeypatch.setattr(daemon_mod, "HealthcheckPinger", _FakePinger)
    return calls


def _install_fake_sender(monkeypatch, daemon_mod):
    from src.core.config import AlertSettings

    sent: list[tuple[str, str]] = []
    real_configure = daemon_mod.configure_logging

    def _configure(component, **kw):
        kw["alert_settings"] = AlertSettings(
            alert_gmail_user="u", alert_gmail_app_password="p", alert_gmail_to="t"
        )
        kw["alert_sender"] = lambda subject, body, **_: sent.append((subject, body))
        return real_configure(component, **kw)

    monkeypatch.setattr(daemon_mod, "configure_logging", _configure)
    return sent


def _cycle_step(delay):
    """Fake step that advances the cycle counter like the real one."""

    def _step(self, now):
        self.state.cycle += 1
        return delay

    return _step


def _boom(self, now):
    raise OSError(28, "No space left on device")


def test_crash_emits_critical_alert_and_healthcheck_fail(tmp_path, monkeypatch) -> None:
    import src.orchestration.daemon as daemon_mod
    from src.core.lifecycle import read_lifecycle

    monkeypatch.chdir(tmp_path)
    _enable_liveness(monkeypatch)
    pings = _install_fake_pinger(monkeypatch, daemon_mod)
    sent = _install_fake_sender(monkeypatch, daemon_mod)
    monkeypatch.setattr(daemon_mod.signal, "signal", lambda *a, **k: None)
    monkeypatch.setattr(daemon_mod.DaemonRunner, "step", _boom)

    with pytest.raises(OSError, match="No space left on device"):
        daemon_mod.main()

    assert len(sent) == 1
    assert "daemon_crash" in sent[0][1]
    assert ("fail", "daemon_crash:OSError") in pings
    record = read_lifecycle(tmp_path / "data" / "work" / "daemon_lifecycle.json")
    assert record is not None
    assert record.crash_error == "OSError"
    assert record.clean_exit is False


def test_crash_loop_bounded_by_daily_budget(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import logging
    from zoneinfo import ZoneInfo

    import src.orchestration.daemon as daemon_mod
    from src.core.config import CollectorSettings

    settings = CollectorSettings(data_root=tmp_path / "data")
    _enable_liveness(monkeypatch)
    pings = _install_fake_pinger(monkeypatch, daemon_mod)
    sent = _install_fake_sender(monkeypatch, daemon_mod)
    monkeypatch.setattr(daemon_mod.DaemonRunner, "step", _boom)
    now = dt.datetime(2026, 9, 25, 12, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    with caplog.at_level(logging.DEBUG):
        for _ in range(5):
            with pytest.raises(OSError, match="No space left on device"):
                daemon_mod.run_collector_daemon(
                    settings=settings, sleep_fn=lambda s: None, now_fn=lambda: now
                )

    assert len(sent) == 3
    crit = [
        r for r in caplog.records
        if r.levelno == logging.CRITICAL and "stage=daemon_crash" in r.getMessage()
    ]
    errs = [
        r for r in caplog.records
        if r.levelno == logging.ERROR and "stage=daemon_crash" in r.getMessage()
    ]
    assert len(crit) == 3
    assert len(errs) == 2
    assert sum(1 for c in pings if c[0] == "fail") == 5


def _write_previous(tmp_path, *, clean_exit, crash_error):
    import datetime as dt

    from src.core.lifecycle import DaemonLifecycleRecord, write_lifecycle

    write_lifecycle(
        tmp_path / "data" / "work" / "daemon_lifecycle.json",
        DaemonLifecycleRecord(
            run_id="prev-1",
            started_at=dt.datetime(2026, 9, 25, 8, 0, tzinfo=dt.UTC),
            clean_exit=clean_exit,
            crash_error=crash_error,
            crash_alert_day=None,
            crash_alerts_sent=0,
        ),
    )


def test_killed_previous_run_reported_at_start(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import logging
    from zoneinfo import ZoneInfo

    import src.orchestration.daemon as daemon_mod
    from src.core.config import CollectorSettings

    settings = CollectorSettings(data_root=tmp_path / "data")
    _write_previous(tmp_path, clean_exit=False, crash_error=None)
    _enable_liveness(monkeypatch)
    pings = _install_fake_pinger(monkeypatch, daemon_mod)
    monkeypatch.setattr(daemon_mod.DaemonRunner, "step", _cycle_step(5.0))
    now = dt.datetime(2026, 9, 25, 12, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    with caplog.at_level(logging.DEBUG):
        daemon_mod.run_collector_daemon(
            settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: now
        )

    assert any(
        r.levelno == logging.CRITICAL
        and "stage=daemon_restart status=UNCLEAN reason=killed" in r.getMessage()
        for r in caplog.records
    )
    assert ("fail", "unclean_restart:killed") in pings


def test_crashed_previous_run_not_double_alerted(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import logging
    from zoneinfo import ZoneInfo

    import src.orchestration.daemon as daemon_mod
    from src.core.config import CollectorSettings

    settings = CollectorSettings(data_root=tmp_path / "data")
    _write_previous(tmp_path, clean_exit=False, crash_error="OSError")
    _enable_liveness(monkeypatch)
    _install_fake_pinger(monkeypatch, daemon_mod)
    monkeypatch.setattr(daemon_mod.DaemonRunner, "step", _cycle_step(5.0))
    now = dt.datetime(2026, 9, 25, 12, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    with caplog.at_level(logging.DEBUG):
        daemon_mod.run_collector_daemon(
            settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: now
        )

    assert any(
        r.levelno == logging.WARNING and "RESTARTED_AFTER_CRASH" in r.getMessage()
        for r in caplog.records
    )
    assert not [r for r in caplog.records if r.levelno == logging.CRITICAL]


def test_graceful_shutdown_marks_clean_exit(tmp_path, monkeypatch, caplog) -> None:
    import logging
    import threading

    import src.orchestration.daemon as daemon_mod
    from src.core.config import CollectorSettings
    from src.core.lifecycle import read_lifecycle

    settings = CollectorSettings(data_root=tmp_path / "data")
    _enable_liveness(monkeypatch)
    pings = _install_fake_pinger(monkeypatch, daemon_mod)

    def _must_not_step(self, now):
        raise AssertionError("step must not run after shutdown")

    monkeypatch.setattr(daemon_mod.DaemonRunner, "step", _must_not_step)
    shutdown = threading.Event()
    shutdown.set()

    with caplog.at_level(logging.DEBUG):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, shutdown=shutdown)

    record = read_lifecycle(tmp_path / "data" / "work" / "daemon_lifecycle.json")
    assert record is not None
    assert record.clean_exit is True
    assert ("success",) in pings

    caplog.clear()
    shutdown2 = threading.Event()
    shutdown2.set()
    with caplog.at_level(logging.DEBUG):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, shutdown=shutdown2)

    assert "UNCLEAN" not in caplog.text


def test_pings_sent_at_interval_and_sleep_capped(tmp_path, monkeypatch) -> None:
    import datetime as dt
    import time as time_mod
    from zoneinfo import ZoneInfo

    import src.orchestration.daemon as daemon_mod
    from src.core.config import CollectorSettings

    settings = CollectorSettings(data_root=tmp_path / "data")
    _enable_liveness(monkeypatch)
    mono = {"t": 0.0}
    monkeypatch.setattr(time_mod, "monotonic", lambda: mono["t"])
    success_at: list[float] = []

    class _TimedPinger:
        def __init__(self, url, *, timeout_s, run_id):
            pass

        def start(self):
            return True

        def success(self):
            success_at.append(time_mod.monotonic())
            return True

        def fail(self, reason):
            return True

    monkeypatch.setattr(daemon_mod, "HealthcheckPinger", _TimedPinger)
    monkeypatch.setattr(daemon_mod.DaemonRunner, "step", _cycle_step(3600.0))
    sleeps: list[float] = []

    def _sleep(s):
        sleeps.append(s)
        mono["t"] += s

    now = dt.datetime(2026, 9, 25, 12, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    daemon_mod.run_collector_daemon(
        settings=settings, sleep_fn=_sleep, max_cycles=3, now_fn=lambda: now
    )

    assert sleeps == [60.0, 60.0, 60.0]
    assert all(s <= 60.0 for s in sleeps)
    assert success_at == [60.0, 120.0, 180.0]


def test_disabled_liveness_warns_once_and_never_touches_network(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import logging
    from zoneinfo import ZoneInfo

    import src.orchestration.daemon as daemon_mod
    from src.core.config import CollectorSettings

    settings = CollectorSettings(data_root=tmp_path / "data")
    monkeypatch.delenv("KRX_ALPHA_LIVENESS_HEALTHCHECK_URL", raising=False)

    def _no_pinger(*a, **k):
        raise AssertionError("pinger must not be constructed when disabled")

    monkeypatch.setattr(daemon_mod, "HealthcheckPinger", _no_pinger)
    monkeypatch.setattr(daemon_mod.DaemonRunner, "step", _cycle_step(5.0))
    now = dt.datetime(2026, 9, 25, 12, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    with caplog.at_level(logging.DEBUG):
        daemon_mod.run_collector_daemon(
            settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: now
        )

    disabled = [
        r for r in caplog.records
        if r.levelno == logging.WARNING and "stage=healthcheck status=DISABLED" in r.getMessage()
    ]
    assert len(disabled) == 1


def test_lifecycle_write_failure_warns_without_blocking_startup(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import logging
    from zoneinfo import ZoneInfo

    import src.orchestration.daemon as daemon_mod
    from src.core.config import CollectorSettings

    settings = CollectorSettings(data_root=tmp_path / "data")
    monkeypatch.delenv("KRX_ALPHA_LIVENESS_HEALTHCHECK_URL", raising=False)

    def _boom(path, record):
        raise OSError("read-only filesystem")

    monkeypatch.setattr(daemon_mod, "write_lifecycle", _boom)
    monkeypatch.setattr(daemon_mod.DaemonRunner, "step", _cycle_step(5.0))
    now = dt.datetime(2026, 9, 25, 12, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    with caplog.at_level(logging.DEBUG):
        daemon_mod.run_collector_daemon(
            settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: now
        )

    assert any(
        r.levelno == logging.WARNING and "stage=daemon_lifecycle status=WRITE_FAIL" in r.getMessage()
        for r in caplog.records
    )


def test_unclean_restart_beyond_budget_logs_error(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import logging
    from zoneinfo import ZoneInfo

    import src.orchestration.daemon as daemon_mod
    from src.core.config import CollectorSettings
    from src.core.lifecycle import DaemonLifecycleRecord, write_lifecycle

    settings = CollectorSettings(data_root=tmp_path / "data")
    day = dt.date(2026, 9, 25)
    write_lifecycle(
        tmp_path / "data" / "work" / "daemon_lifecycle.json",
        DaemonLifecycleRecord(
            run_id="prev-9",
            started_at=dt.datetime(2026, 9, 25, 8, 0, tzinfo=dt.UTC),
            clean_exit=False,
            crash_error=None,
            crash_alert_day=day,
            crash_alerts_sent=3,
        ),
    )
    monkeypatch.delenv("KRX_ALPHA_LIVENESS_HEALTHCHECK_URL", raising=False)
    monkeypatch.setattr(daemon_mod.DaemonRunner, "step", _cycle_step(5.0))
    now = dt.datetime(2026, 9, 25, 12, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    with caplog.at_level(logging.DEBUG):
        daemon_mod.run_collector_daemon(
            settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: now
        )

    assert any(
        r.levelno == logging.ERROR
        and "stage=daemon_restart status=UNCLEAN reason=killed" in r.getMessage()
        for r in caplog.records
    )
    assert not [
        r for r in caplog.records
        if r.levelno == logging.CRITICAL and "stage=daemon_restart" in r.getMessage()
    ]


def test_kill_loop_bounded_by_daily_budget(tmp_path, monkeypatch, caplog) -> None:
    import dataclasses
    import datetime as dt
    import logging
    from zoneinfo import ZoneInfo

    import src.orchestration.daemon as daemon_mod
    from src.core.config import CollectorSettings
    from src.core.lifecycle import read_lifecycle, write_lifecycle

    settings = CollectorSettings(data_root=tmp_path / "data")
    lifecycle = tmp_path / "data" / "work" / "daemon_lifecycle.json"
    _write_previous(tmp_path, clean_exit=False, crash_error=None)
    _enable_liveness(monkeypatch)
    _install_fake_pinger(monkeypatch, daemon_mod)
    monkeypatch.setattr(daemon_mod.DaemonRunner, "step", _cycle_step(5.0))
    now = dt.datetime(2026, 9, 25, 12, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    with caplog.at_level(logging.DEBUG):
        for _ in range(6):
            daemon_mod.run_collector_daemon(
                settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: now
            )
            # SIGKILL 을 흉내: 정상 종료 기록을 지우고 예산 카운트는 그대로 둔다.
            record = read_lifecycle(lifecycle)
            assert record is not None
            write_lifecycle(lifecycle, dataclasses.replace(record, clean_exit=False, crash_error=None))

    killed = [r for r in caplog.records if "stage=daemon_restart status=UNCLEAN reason=killed" in r.getMessage()]
    assert [r.levelno for r in killed] == [logging.CRITICAL] * 3 + [logging.ERROR] * 3


def test_startup_failure_reported_as_crash(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import logging
    from zoneinfo import ZoneInfo

    import src.orchestration.daemon as daemon_mod
    from src.core.config import CollectorSettings
    from src.core.lifecycle import read_lifecycle

    settings = CollectorSettings(data_root=tmp_path / "data")
    _enable_liveness(monkeypatch)
    pings = _install_fake_pinger(monkeypatch, daemon_mod)

    def _fail_migration(paths):
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(daemon_mod, "_migrate_market_stores", _fail_migration)
    now = dt.datetime(2026, 9, 25, 12, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    with caplog.at_level(logging.DEBUG), pytest.raises(OSError, match="No space left on device"):
        daemon_mod.run_collector_daemon(
            settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: now
        )

    assert any(
        r.levelno == logging.CRITICAL and "stage=daemon_crash error=OSError" in r.getMessage() for r in caplog.records
    )
    assert ("fail", "daemon_crash:OSError") in pings
    record = read_lifecycle(tmp_path / "data" / "work" / "daemon_lifecycle.json")
    assert record is not None
    assert record.crash_error == "OSError"


def test_intentional_stop_recorded_as_clean_exit(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import logging
    from zoneinfo import ZoneInfo

    import src.orchestration.daemon as daemon_mod
    from src.core.config import CollectorSettings
    from src.core.lifecycle import read_lifecycle

    settings = CollectorSettings(data_root=tmp_path / "data")
    _enable_liveness(monkeypatch)
    _install_fake_pinger(monkeypatch, daemon_mod)

    def _interrupt(self, now):
        raise KeyboardInterrupt

    monkeypatch.setattr(daemon_mod.DaemonRunner, "step", _interrupt)
    now = dt.datetime(2026, 9, 25, 12, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    with caplog.at_level(logging.DEBUG), pytest.raises(KeyboardInterrupt):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: now)

    # 의도된 중지는 크래시·강제 종료로 보고하지 않는다
    record = read_lifecycle(tmp_path / "data" / "work" / "daemon_lifecycle.json")
    assert record is not None
    assert record.clean_exit is True
    assert not any("stage=daemon_crash" in r.getMessage() for r in caplog.records)
