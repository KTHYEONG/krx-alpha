"""Daemon eod cases split from test_daemon.py (EOD housekeeping, session reconciliation, backup freshness, and daily digest)."""

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


@pytest.fixture(autouse=True)
def _verified_aftermarket_capacity(monkeypatch) -> None:
    """Daemon tests simulate a deployed host with verified aftermarket capacity."""
    monkeypatch.setenv("KRX_ALPHA_AFTERMARKET_PAIR_CAPACITY_PER_CONNECTION", "4")


def test_run_collector_daemon_eod_stops_supervised_process(tmp_path, monkeypatch) -> None:
    import datetime as dt
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo
    import src.orchestration.daemon as daemon_mod
    from src.orchestration.daemon import run_collector_daemon

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(daemon_mod, "resolve_trading_day", lambda ref_date: None)
    monkeypatch.setattr(daemon_mod, "run_session_orchestration", lambda **kw: True)

    stop_calls: list[float] = []

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            self.cmd = cmd

        def ensure_running(self):
            self.checked = True
            return "started"

        def stop(self, *, timeout_s=15.0):
            stop_calls.append(timeout_s)
            return "graceful"

    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)

    kst = ZoneInfo("Asia/Seoul")
    times = iter(
        [
            dt.datetime(2026, 9, 8, 9, 0, 0, tzinfo=kst),
            dt.datetime(2026, 9, 8, 15, 45, 0, tzinfo=kst),
        ]
    )

    run_collector_daemon(sleep_fn=MagicMock(), max_cycles=2, now_fn=lambda: next(times))

    assert stop_calls == [15.0]


def test_run_collector_daemon_eod_passes_quarantine_root(tmp_path, monkeypatch) -> None:
    # Given: EOD 시각에 진입한 데몬
    import datetime as dt
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo

    import src.orchestration.daemon as daemon_mod
    from src.core.config import CollectorSettings
    from src.orchestration.daemon import run_collector_daemon

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(daemon_mod, "resolve_trading_day", lambda ref_date: None)

    seen: dict[str, object] = {}

    def _fake_maintenance(
        journal_root,
        *,
        retain_days=3,
        today=None,
        archive_root=None,
        quarantine_root=None,
        work_root=None,
        verified_remote_l1=None,
        progress=None,
        normalize=True,
        reuse_fresh_l1=False,
    ):
        seen.update({"quarantine_root": quarantine_root, "archive_root": archive_root, "work_root": work_root})
        return 0

    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", _fake_maintenance)
    monkeypatch.setattr(
        daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0}
    )

    settings = CollectorSettings()
    mock_sleep = MagicMock()
    eod_time = dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    # When
    run_collector_daemon(settings=settings, sleep_fn=mock_sleep, max_cycles=1, now_fn=lambda: eod_time)

    # Then: 설정에서 파생된 격리/작업 경로가 EOD 유지보수로 전달된다
    assert seen["quarantine_root"] == settings.paths.quarantine_root
    assert seen["archive_root"] == settings.paths.archive_root
    assert seen["work_root"] == settings.paths.work_root


def test_run_collector_daemon_eod_logs_session_data_gap(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")

    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: 0)
    monkeypatch.setattr(
        daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0}
    )
    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", lambda **kw: False)
    monkeypatch.setattr(
        daemon_mod, "_resolve_trading_day_with_cache", lambda d, c, _a: _business_trading_day(dt.date(2026, 9, 10))
    )

    eod_time = dt.datetime(2026, 9, 10, 15, 45, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    with caplog.at_level(logging.CRITICAL):
        daemon_mod.run_collector_daemon(
            settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time
        )

    assert "session_data_gap" in caplog.text


def test_run_collector_daemon_eod_attempts_maintenance_once_per_date(tmp_path, monkeypatch) -> None:
    # Given: 같은 날 EOD 윈도우에서 3사이클 반복
    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    counts = {"maintenance": 0, "offload": 0, "reconcile": 0}

    def _maintenance(*a, **kw):
        counts["maintenance"] += 1
        return 0

    def _offload(*a, **kw):
        counts["offload"] += 1
        return {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0}

    def _reconcile(**kw):
        counts["reconcile"] += 1
        return True

    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", _maintenance)
    monkeypatch.setattr(daemon_mod, "run_eod_offload", _offload)
    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", _reconcile)
    eod_time = dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    monkeypatch.setattr(
        daemon_mod, "_resolve_trading_day_with_cache", lambda d, c, _a: _business_trading_day(dt.date(2026, 9, 14))
    )

    # When
    daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=3, now_fn=lambda: eod_time)

    # Then: 거래일당 1회만 시도 (정규화 2회: offload 전 None + offload 후 verified)
    assert counts == {"maintenance": 2, "offload": 1, "reconcile": 1}


def test_run_collector_daemon_eod_attempts_again_on_next_date(tmp_path, monkeypatch) -> None:
    # Given: 이틀 연속 EOD 사이클
    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    days = []

    def _maintenance(*a, **kw):
        days.append(kw["today"])
        return 0

    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", _maintenance)
    monkeypatch.setattr(
        daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0}
    )
    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", lambda **kw: True)
    kst = ZoneInfo("Asia/Seoul")
    times = iter(
        [
            dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=kst),
            dt.datetime(2026, 9, 14, 15, 46, 0, tzinfo=kst),
            dt.datetime(2026, 9, 15, 15, 45, 0, tzinfo=kst),
        ]
    )

    # When
    daemon_mod.run_collector_daemon(
        settings=settings, sleep_fn=lambda s: None, max_cycles=3, now_fn=lambda: next(times)
    )

    # Then
    assert days == [dt.date(2026, 9, 14), dt.date(2026, 9, 14), dt.date(2026, 9, 15), dt.date(2026, 9, 15)]


def test_run_collector_daemon_eod_runs_offload_and_reconciliation_when_maintenance_fails(
    tmp_path, monkeypatch, caplog
) -> None:
    # Given: 정규화 유지보수가 워커 크래시로 실패
    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod
    from src.storage.retention import L1WorkerCrashError

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    counts = {"offload": 0, "reconcile": 0}

    def _maintenance(*a, **kw):
        raise L1WorkerCrashError("normalize worker crashed: returncode=-9")

    def _offload(*a, **kw):
        counts["offload"] += 1
        return {"uploaded": 2, "skipped": 0, "failed": 0, "purged": 1}

    def _reconcile(**kw):
        counts["reconcile"] += 1
        return True

    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", _maintenance)
    monkeypatch.setattr(daemon_mod, "run_eod_offload", _offload)
    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", _reconcile)
    eod_time = dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    # When
    monkeypatch.setattr(
        daemon_mod, "_resolve_trading_day_with_cache", lambda d, c, _a: _business_trading_day(dt.date(2026, 9, 14))
    )
    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(
            settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time
        )

    # Then: 오프로드/정합성 검사는 계속되고 요약은 DEGRADED
    assert counts == {"offload": 1, "reconcile": 1}
    assert "stage=eod_maintenance status=FAIL reason=maintenance_error" in caplog.text
    assert "deleted_partitions=0 uploaded=2 purged=1 status=DEGRADED" in caplog.text


def test_run_collector_daemon_eod_logs_error_when_offload_raises_and_does_not_retry_same_date(
    tmp_path, monkeypatch, caplog
) -> None:
    # Given: 오프로드 단계에서 예기치 못한 예외
    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    counts = {"maintenance": 0, "offload": 0, "reconcile": 0}

    def _maintenance(*a, **kw):
        counts["maintenance"] += 1
        return 0

    def _offload(*a, **kw):
        counts["offload"] += 1
        raise RuntimeError("rclone lsjson failed")

    def _reconcile(**kw):
        counts["reconcile"] += 1
        return True

    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", _maintenance)
    monkeypatch.setattr(daemon_mod, "run_eod_offload", _offload)
    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", _reconcile)
    eod_time = dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    # When
    monkeypatch.setattr(
        daemon_mod, "_resolve_trading_day_with_cache", lambda d, c, _a: _business_trading_day(dt.date(2026, 9, 14))
    )
    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(
            settings=settings, sleep_fn=lambda s: None, max_cycles=2, now_fn=lambda: eod_time
        )

    # Then: 오프로드 실패와 무관하게 정합성 검사는 수행되고 같은 날 재시도하지 않는다
    assert counts == {"maintenance": 1, "offload": 1, "reconcile": 1}
    assert "stage=eod_maintenance error=rclone lsjson failed" in caplog.text
    assert "deleted_partitions=0 uploaded=0 purged=0 status=DEGRADED" in caplog.text


def test_run_collector_daemon_eod_unexpected_error_logs_traceback(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(daemon_mod, "configure_logging", lambda component, *, log_dir=None, alert_ledger_path=None: "r")
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: 0)

    def _offload(*a, **kw):
        raise RuntimeError("rclone lsjson failed")

    monkeypatch.setattr(daemon_mod, "run_eod_offload", _offload)
    eod_time = dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    with caplog.at_level(logging.ERROR):
        daemon_mod.run_collector_daemon(
            settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time
        )

    records = [r for r in caplog.records if "stage=eod_maintenance error=" in r.getMessage()]
    assert len(records) == 1
    assert records[0].exc_info is not None
    assert records[0].exc_info[0] is RuntimeError


def test_run_collector_daemon_eod_reconciliation_failure_is_critical_and_degraded(
    tmp_path, monkeypatch, caplog
) -> None:
    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: 0)
    monkeypatch.setattr(
        daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 1, "skipped": 0, "failed": 0, "purged": 0}
    )

    def _broken(**kw):
        raise OSError("bars parquet unreadable")

    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", _broken)
    monkeypatch.setattr(
        daemon_mod, "_resolve_trading_day_with_cache", lambda d, c, _a: _business_trading_day(dt.date(2026, 9, 14))
    )
    eod_time = dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(
            settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time
        )

    records = [r for r in caplog.records if "stage=eod_reconciliation status=FAIL" in r.getMessage()]
    assert len(records) == 1
    assert records[0].levelno == logging.CRITICAL
    assert records[0].exc_info is not None
    assert "deleted_partitions=0 uploaded=1 purged=0 status=DEGRADED" in caplog.text


def test_run_collector_daemon_clears_supervisor_after_eod_so_next_day_never_restarts_stale_session(
    tmp_path, monkeypatch
) -> None:
    # Given: 전일 스트리머가 EOD 에서 정지된 뒤 다음 날 오케스트레이션이 실패
    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    kst = ZoneInfo("Asia/Seoul")
    ensured_dates: list[str] = []

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            self.cmd = cmd
            self.last_exit_code = 0

        def ensure_running(self):
            ensured_dates.append(self.cmd[self.cmd.index("--session-date") + 1])
            return "restarted"

        def stop(self, *, timeout_s=15.0):
            return "graceful"

    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)
    monkeypatch.setattr(daemon_mod, "resolve_trading_day", lambda ref_date: None)
    outcomes = iter([True, False])
    monkeypatch.setattr(daemon_mod, "run_session_orchestration", lambda **kw: next(outcomes))
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: 0)
    monkeypatch.setattr(
        daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0}
    )
    times = iter(
        [
            dt.datetime(2026, 9, 14, 9, 0, tzinfo=kst),
            dt.datetime(2026, 9, 14, 15, 45, tzinfo=kst),
            dt.datetime(2026, 9, 15, 9, 0, tzinfo=kst),
        ]
    )

    # When
    daemon_mod.run_collector_daemon(
        settings=settings, sleep_fn=lambda s: None, max_cycles=3, now_fn=lambda: next(times)
    )

    # Then: 다음 날에는 전일 session-date 스트리머를 재기동하지 않는다
    assert ensured_dates == ["2026-09-14"]


def test_run_collector_daemon_stops_stale_day_streamer_when_eod_was_missed(tmp_path, monkeypatch, caplog) -> None:
    # Given: EOD 윈도우를 거치지 못한 채 날짜가 바뀐 경우 (데몬이 15:40-16:00 사이 중단 등)
    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    kst = ZoneInfo("Asia/Seoul")
    stops: list[str] = []
    ensured_dates: list[str] = []

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            self.cmd = cmd
            self.last_exit_code = None

        def ensure_running(self):
            ensured_dates.append(self.cmd[self.cmd.index("--session-date") + 1])
            return "running"

        def stop(self, *, timeout_s=15.0):
            stops.append(self.cmd[self.cmd.index("--session-date") + 1])
            return "graceful"

    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)
    monkeypatch.setattr(daemon_mod, "resolve_trading_day", lambda ref_date: None)
    outcomes = iter([True, False])
    monkeypatch.setattr(daemon_mod, "run_session_orchestration", lambda **kw: next(outcomes))
    times = iter([dt.datetime(2026, 9, 14, 9, 0, tzinfo=kst), dt.datetime(2026, 9, 15, 9, 0, tzinfo=kst)])

    # When
    with caplog.at_level(logging.WARNING):
        daemon_mod.run_collector_daemon(
            settings=settings, sleep_fn=lambda s: None, max_cycles=2, now_fn=lambda: next(times)
        )

    # Then: 전일 스트리머를 정지하고 재기동하지 않는다
    assert stops == ["2026-09-14"]
    assert ensured_dates == ["2026-09-14"]
    assert "stage=streamer status=STOP_STALE_DAY stop_result=graceful" in caplog.text


def test_run_collector_daemon_eod_offload_remote_auth_failure_is_critical(tmp_path, monkeypatch, caplog) -> None:

    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod
    from src.storage.remote import RemoteArchiveError

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: 0)
    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", lambda **kw: True)
    digests: list[tuple[str, str]] = []
    monkeypatch.setattr(daemon_mod, "send_digest", lambda subject, body: digests.append((subject, body)) or True)
    eod_time = dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    def _offload(*a, **kw):
        raise RemoteArchiveError("lsjson failed: l1/ couldn't fetch token: invalid_grant: maybe token expired?")

    monkeypatch.setattr(daemon_mod, "run_eod_offload", _offload)
    monkeypatch.setattr(daemon_mod, "check_backup_freshness", lambda **kw: [])

    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(
            settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time
        )

    criticals = [r.getMessage() for r in caplog.records if r.levelno == logging.CRITICAL]
    assert any(
        m.startswith("[DAEMON] stage=eod_offload status=FAIL reason=auth_expired hint=rclone_config_reconnect_gdrive")
        for m in criticals
    )
    assert "deleted_partitions=0 uploaded=0 purged=0 status=DEGRADED" in caplog.text


def test_run_collector_daemon_eod_backup_freshness_stale_is_critical(tmp_path, monkeypatch, caplog) -> None:

    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: 0)
    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", lambda **kw: True)
    digests: list[tuple[str, str]] = []
    monkeypatch.setattr(daemon_mod, "send_digest", lambda subject, body: digests.append((subject, body)) or True)
    eod_time = dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    seen: dict[str, object] = {}
    monkeypatch.setattr(
        daemon_mod, "_resolve_trading_day_with_cache", lambda d, c, _a: _business_trading_day(dt.date(2026, 9, 14))
    )
    monkeypatch.setattr(
        daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0}
    )

    def _freshness(**kw):
        seen.update(kw)
        return ["2026-09-11.json", "2026-09-12.json"]

    monkeypatch.setattr(daemon_mod, "check_backup_freshness", _freshness)

    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(
            settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time
        )

    assert seen == {"manifest_dir": settings.paths.manifest_dir, "today": dt.date(2026, 9, 14)}
    assert "[DAEMON] stage=backup_freshness status=STALE missing=2 oldest=2026-09-11.json" in caplog.text
    assert [r.levelno for r in caplog.records if "stage=backup_freshness" in r.getMessage()] == [logging.CRITICAL]
    assert "status=DEGRADED" in caplog.text


def test_run_collector_daemon_eod_backup_freshness_remote_failure_is_critical(tmp_path, monkeypatch, caplog) -> None:

    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod
    from src.storage.remote import RemoteArchiveError

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: 0)
    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", lambda **kw: True)
    digests: list[tuple[str, str]] = []
    monkeypatch.setattr(daemon_mod, "send_digest", lambda subject, body: digests.append((subject, body)) or True)
    eod_time = dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    monkeypatch.setattr(
        daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0}
    )

    def _freshness(**kw):
        raise RemoteArchiveError("lsjson failed: manifest/ dial tcp: i/o timeout")

    monkeypatch.setattr(daemon_mod, "check_backup_freshness", _freshness)

    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(
            settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time
        )

    assert "[DAEMON] stage=backup_freshness status=FAIL reason=remote_error" in caplog.text
    assert "status=DEGRADED" in caplog.text


def test_run_collector_daemon_eod_passes_substantive_reconciliation_inputs(tmp_path, monkeypatch) -> None:

    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: 0)
    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", lambda **kw: True)
    digests: list[tuple[str, str]] = []
    monkeypatch.setattr(daemon_mod, "send_digest", lambda subject, body: digests.append((subject, body)) or True)
    eod_time = dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    seen: dict[str, object] = {}
    monkeypatch.setattr(
        daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0}
    )
    monkeypatch.setattr(daemon_mod, "check_backup_freshness", lambda **kw: [])

    def _reconcile(**kw):
        seen.update(kw)
        return True

    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", _reconcile)
    monkeypatch.setattr(
        daemon_mod, "_resolve_trading_day_with_cache", lambda d, c, _a: _business_trading_day(dt.date(2026, 9, 14))
    )

    daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time)

    assert seen["journal_root"] == settings.paths.journal_root
    assert seen["streams"] == settings.streams
    assert seen["vendor"] == settings.vendor
    assert seen["venue"] == "krx"
    assert seen["session"] == "regular"
    assert "bars_store" not in seen
    assert seen["manifest_path"] == settings.paths.manifest_path(dt.date(2026, 9, 14))


def test_run_collector_daemon_eod_sends_daily_digest_once_per_date(tmp_path, monkeypatch) -> None:

    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(
        data_root=pathlib.Path(tmp_path) / "data",
        host_backup_status_path=_host_status_path(tmp_path),
    )
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: 0)
    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", lambda **kw: True)
    digests: list[tuple[str, str]] = []
    monkeypatch.setattr(daemon_mod, "send_digest", lambda subject, body: digests.append((subject, body)) or True)
    eod_time = dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    monkeypatch.setattr(
        daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 2, "skipped": 0, "failed": 0, "purged": 1}
    )
    monkeypatch.setattr(daemon_mod, "check_backup_freshness", lambda **kw: [])
    eod_time = dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    _write_eod_host_status(settings, eod_time, 20)
    monkeypatch.setattr(
        daemon_mod, "_resolve_trading_day_with_cache", lambda d, c, _a: _business_trading_day(dt.date(2026, 9, 14))
    )
    daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=2, now_fn=lambda: eod_time)

    assert len(digests) == 1
    subject, body = digests[0]
    assert subject == "[krx-alpha] EOD 2026-09-14 OK"
    lines = body.splitlines()
    assert "status=OK" in lines
    assert "uploaded=2" in lines
    assert "purged=1" in lines
    assert "reconciled=True" in lines
    assert "aftermarket_ready=True" in lines
    assert "backup_missing=0" in lines


def test_eod_runs_housekeeping_when_aftermarket_not_ready(monkeypatch, tmp_path, caplog):
    import datetime as dt
    import logging
    from zoneinfo import ZoneInfo
    import src.orchestration.daemon as daemon
    from src.core.config import CollectorSettings

    calls = []
    monkeypatch.setattr(daemon, "aftermarket_eod_ready", lambda **_: False)
    monkeypatch.setattr(daemon, "run_eod_maintenance", lambda *a, **k: calls.append("maintenance") or 0)
    monkeypatch.setattr(
        daemon,
        "run_eod_offload",
        lambda *a, **k: calls.append("offload") or {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0},
    )
    monkeypatch.setattr(daemon, "run_eod_remote_l0_purge", lambda *a, **k: calls.append("purge") or 0)
    monkeypatch.setattr(daemon, "check_session_reconciliation", lambda **kw: True)
    monkeypatch.setattr(daemon, "check_backup_freshness", lambda **kw: [])
    monkeypatch.setattr(daemon, "_resolve_trading_day_with_cache", lambda d, c, _a: _business_trading_day(d))
    now = dt.datetime(2026, 9, 15, 20, 1, tzinfo=ZoneInfo("Asia/Seoul"))
    cfg = CollectorSettings(data_root=tmp_path, after_market_enabled=True, universe_slot_budget=1, ls_capacity_pairs=2)
    with caplog.at_level(logging.INFO):
        daemon.run_collector_daemon(settings=cfg, now_fn=lambda: now, sleep_fn=lambda _: None, max_cycles=1)
    assert "maintenance" in calls
    assert "offload" in calls
    assert "purge" in calls
    assert "reason=aftermarket_not_ready" in caplog.text
    assert "status=DEGRADED" in caplog.text


def test_daemon_stops_snapshot_supervisor_at_eod(tmp_path, monkeypatch) -> None:
    import datetime as dt
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo
    import src.orchestration.daemon as daemon_mod
    from src.orchestration.daemon import run_collector_daemon

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("KRX_ALPHA_SNAPSHOT_ENABLED", "true")
    _snapshot_ready_daemon(monkeypatch, daemon_mod)
    created = _snapshot_fake_supervisor(monkeypatch, daemon_mod)
    kst = ZoneInfo("Asia/Seoul")
    times = iter(
        [
            dt.datetime(2026, 9, 8, 9, 0, 0, tzinfo=kst),
            dt.datetime(2026, 9, 8, 15, 45, 0, tzinfo=kst),
        ]
    )

    run_collector_daemon(sleep_fn=MagicMock(), max_cycles=2, now_fn=lambda: next(times))

    snapshots = [sup for sup in created if "collect-snapshots" in sup.cmd]
    assert len(snapshots) == 1
    assert snapshots[0].stop_calls == [15.0]


def test_eod_remote_l0_purge_runs_after_pruning_and_cannot_fail_eod(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import logging
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(
        data_root=pathlib.Path(tmp_path) / "data",
        host_backup_status_path=_host_status_path(tmp_path),
    )
    order: list[str] = []

    def _maintenance(journal_root, **kw):
        order.append("maintenance")
        return 0

    class _Offload:
        verified_remote_l1 = frozenset({"l1/ls/H0STASP0/dt=2026-09-11.parquet"})
        l1 = type("S", (), {"uploaded": 1})()
        purged = 0

    def _offload(*a, **kw):
        return _Offload()

    def _purge(journal_root, verified, *, progress=None, quarantine_root=None):
        order.append("purge")
        assert verified == _Offload.verified_remote_l1
        raise RuntimeError("purge boom")

    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", _maintenance)
    monkeypatch.setattr(daemon_mod, "run_eod_offload", _offload)
    monkeypatch.setattr(daemon_mod, "run_eod_remote_l0_purge", _purge)
    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", lambda **kw: True)
    monkeypatch.setattr(daemon_mod, "check_backup_freshness", lambda **kw: [])
    eod_time = dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    _write_eod_host_status(settings, eod_time, 20)
    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(
            settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time
        )

    assert order.count("maintenance") == 2
    assert order[-1] == "purge"
    assert "stage=eod_l0_remote_purge status=FAIL" in caplog.text
    assert "deleted_partitions=0 uploaded=1 purged=0 status=OK" in caplog.text


def test_daemon_holiday_eod_runs_housekeeping_without_alerts(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import logging
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(
        data_root=pathlib.Path(tmp_path) / "data",
        after_market_enabled=True,
        host_backup_status_path=_host_status_path(tmp_path),
    )
    kst = ZoneInfo("Asia/Seoul")
    holiday = _holiday_trading_day(dt.date(2026, 9, 24))
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c, _a: holiday)
    calls: dict[str, int] = {"maintenance": 0, "offload": 0, "purge": 0, "freshness": 0}

    def _maintenance(*a, **kw):
        calls["maintenance"] += 1
        return 3

    def _offload(*a, **kw):
        calls["offload"] += 1
        return {"uploaded": 2, "skipped": 0, "failed": 0, "purged": 1}

    def _purge(*a, **kw):
        calls["purge"] += 1
        return 0

    def _freshness(**kw):
        calls["freshness"] += 1
        return []

    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", _maintenance)
    monkeypatch.setattr(daemon_mod, "run_eod_offload", _offload)
    monkeypatch.setattr(daemon_mod, "run_eod_remote_l0_purge", _purge)
    monkeypatch.setattr(daemon_mod, "check_backup_freshness", _freshness)

    def _must_not_call(**kw):
        raise AssertionError("holiday EOD must skip session checks")

    monkeypatch.setattr(daemon_mod, "aftermarket_eod_ready", _must_not_call)
    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", _must_not_call)
    monkeypatch.setattr(daemon_mod, "send_digest", _must_not_call)
    now = dt.datetime(2026, 9, 24, 20, 5, tzinfo=kst)
    _write_eod_host_status(settings, now, 20)
    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: now)
    assert calls == {"maintenance": 2, "offload": 1, "purge": 1, "freshness": 1}
    assert not any(r.levelno == logging.CRITICAL for r in caplog.records)
    assert (
        "stage=eod_maintenance status=HOLIDAY deleted_partitions=6 uploaded=2 purged=1 date=2026-09-24" in caplog.text
    )


def test_daemon_holiday_eod_offload_remote_failure_keeps_holiday_summary(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import logging
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod
    from src.storage.remote import RemoteArchiveError

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data", after_market_enabled=True)
    holiday = _holiday_trading_day(dt.date(2026, 9, 24))
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c, _a: holiday)
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: 1)
    monkeypatch.setattr(daemon_mod, "run_eod_remote_l0_purge", lambda *a, **kw: 0)
    monkeypatch.setattr(daemon_mod, "check_backup_freshness", lambda **kw: [])

    def _offload(*a, **kw):
        raise RemoteArchiveError("lsjson failed: token expired invalid_grant")

    monkeypatch.setattr(daemon_mod, "run_eod_offload", _offload)
    now = dt.datetime(2026, 9, 24, 20, 5, tzinfo=ZoneInfo("Asia/Seoul"))
    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: now)
    assert "stage=eod_offload status=FAIL reason=auth_expired" in caplog.text
    assert "status=HOLIDAY" in caplog.text


def test_daemon_holiday_eod_offload_unexpected_error_logged(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import logging
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data", after_market_enabled=True)
    holiday = _holiday_trading_day(dt.date(2026, 9, 24))
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c, _a: holiday)
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: 1)
    monkeypatch.setattr(daemon_mod, "run_eod_remote_l0_purge", lambda *a, **kw: 0)
    monkeypatch.setattr(daemon_mod, "check_backup_freshness", lambda **kw: [])

    def _offload(*a, **kw):
        raise RuntimeError("rclone lsjson failed")

    monkeypatch.setattr(daemon_mod, "run_eod_offload", _offload)
    now = dt.datetime(2026, 9, 24, 20, 5, tzinfo=ZoneInfo("Asia/Seoul"))
    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: now)
    assert "stage=eod_maintenance error=rclone lsjson failed" in caplog.text
    assert "status=HOLIDAY" in caplog.text


def test_daemon_aftermarket_failure_still_runs_housekeeping(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import logging
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data", after_market_enabled=True)
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c, _a: _business_trading_day(d))
    monkeypatch.setattr(daemon_mod, "aftermarket_eod_ready", lambda **kw: False)
    calls: dict[str, int] = {"maintenance": 0, "offload": 0, "purge": 0}
    monkeypatch.setattr(
        daemon_mod,
        "run_eod_maintenance",
        lambda *a, **kw: calls.__setitem__("maintenance", calls["maintenance"] + 1) or 0,
    )
    monkeypatch.setattr(
        daemon_mod,
        "run_eod_offload",
        lambda *a, **kw: (
            calls.__setitem__("offload", calls["offload"] + 1)
            or {"uploaded": 1, "skipped": 0, "failed": 0, "purged": 0}
        ),
    )
    monkeypatch.setattr(
        daemon_mod, "run_eod_remote_l0_purge", lambda *a, **kw: calls.__setitem__("purge", calls["purge"] + 1) or 0
    )
    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", lambda **kw: True)
    monkeypatch.setattr(daemon_mod, "check_backup_freshness", lambda **kw: [])
    digests: list[tuple[str, str]] = []
    monkeypatch.setattr(daemon_mod, "send_digest", lambda subject, body: digests.append((subject, body)) or True)
    now = dt.datetime(2026, 9, 15, 20, 5, tzinfo=ZoneInfo("Asia/Seoul"))

    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: now)

    assert calls["maintenance"] == 2
    assert calls["offload"] == 1
    assert calls["purge"] == 1
    assert "reason=aftermarket_not_ready" in caplog.text
    assert "status=DEGRADED" in caplog.text
    assert len(digests) == 1
    assert "aftermarket_ready=False" in digests[0][1]


def test_daemon_ready_aftermarket_and_clean_checks_report_ok(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import logging
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(
        data_root=pathlib.Path(tmp_path) / "data",
        after_market_enabled=True,
        host_backup_status_path=_host_status_path(tmp_path),
    )
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c, _a: _business_trading_day(d))
    monkeypatch.setattr(daemon_mod, "aftermarket_eod_ready", lambda **kw: True)
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: 0)
    monkeypatch.setattr(
        daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 1, "skipped": 0, "failed": 0, "purged": 0}
    )
    monkeypatch.setattr(daemon_mod, "run_eod_remote_l0_purge", lambda *a, **kw: 0)
    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", lambda **kw: True)
    monkeypatch.setattr(daemon_mod, "check_backup_freshness", lambda **kw: [])
    digests: list[tuple[str, str]] = []
    monkeypatch.setattr(daemon_mod, "send_digest", lambda subject, body: digests.append((subject, body)) or True)
    now = dt.datetime(2026, 9, 15, 20, 5, tzinfo=ZoneInfo("Asia/Seoul"))
    _write_eod_host_status(settings, now, 20)
    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: now)

    assert "status=OK" in caplog.text
    assert len(digests) == 1
    assert "aftermarket_ready=True" in digests[0][1]


def test_daemon_reconciliation_receives_routed_layout(tmp_path, monkeypatch) -> None:
    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c, _a: _business_trading_day(d))
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: 0)
    monkeypatch.setattr(
        daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0}
    )
    monkeypatch.setattr(daemon_mod, "run_eod_remote_l0_purge", lambda *a, **kw: 0)
    monkeypatch.setattr(daemon_mod, "check_backup_freshness", lambda **kw: [])
    seen: dict[str, object] = {}

    def _record(**kw):
        seen.update(kw)
        return True

    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", _record)
    now = dt.datetime(2026, 9, 14, 15, 45, tzinfo=ZoneInfo("Asia/Seoul"))
    daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: now)

    assert seen["venue"] == "krx"
    assert seen["session"] == "regular"
    assert "bars_store" not in seen


def test_daemon_unknown_calendar_skips_reconciliation_with_warning(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import logging
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c, _a: None)
    calls: dict[str, int] = {"maintenance": 0, "offload": 0}

    def _maintenance(*a, **kw):
        calls["maintenance"] += 1
        return 0

    def _offload(*a, **kw):
        calls["offload"] += 1
        return {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0}

    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", _maintenance)
    monkeypatch.setattr(daemon_mod, "run_eod_offload", _offload)
    monkeypatch.setattr(daemon_mod, "run_eod_remote_l0_purge", lambda *a, **kw: 0)
    monkeypatch.setattr(daemon_mod, "check_backup_freshness", lambda **kw: [])

    def _must_not_call(**kw):
        raise AssertionError("reconciliation must be skipped when the calendar is unknown")

    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", _must_not_call)
    now = dt.datetime(2026, 9, 14, 15, 45, tzinfo=ZoneInfo("Asia/Seoul"))

    with caplog.at_level(logging.WARNING):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: now)

    assert calls == {"maintenance": 2, "offload": 1}
    assert sum("reason=calendar_unknown" in r.getMessage() for r in caplog.records) == 1


def test_daemon_holiday_eod_maintenance_failure_still_summarizes(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import logging
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod
    from src.storage.retention import L1WorkerCrashError

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data", after_market_enabled=True)
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c, _a: _holiday_trading_day(d))
    monkeypatch.setattr(
        daemon_mod, "run_eod_maintenance", lambda *a, **kw: (_ for _ in ()).throw(L1WorkerCrashError("crashed"))
    )
    monkeypatch.setattr(
        daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0}
    )
    monkeypatch.setattr(daemon_mod, "run_eod_remote_l0_purge", lambda *a, **kw: 0)
    monkeypatch.setattr(daemon_mod, "check_backup_freshness", lambda **kw: [])
    now = dt.datetime(2026, 9, 24, 20, 5, tzinfo=ZoneInfo("Asia/Seoul"))

    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: now)

    assert caplog.text.count("reason=maintenance_error") == 2
    assert "status=HOLIDAY" in caplog.text


def test_daemon_holiday_eod_purge_failure_never_fails_holiday(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import logging
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data", after_market_enabled=True)
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c, _a: _holiday_trading_day(d))
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: 0)
    monkeypatch.setattr(
        daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0}
    )

    def _purge(*a, **kw):
        raise RuntimeError("purge boom")

    monkeypatch.setattr(daemon_mod, "run_eod_remote_l0_purge", _purge)
    monkeypatch.setattr(daemon_mod, "check_backup_freshness", lambda **kw: [])
    now = dt.datetime(2026, 9, 24, 20, 5, tzinfo=ZoneInfo("Asia/Seoul"))

    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: now)

    assert "stage=eod_l0_remote_purge status=FAIL" in caplog.text
    assert "status=HOLIDAY" in caplog.text


def test_daemon_holiday_eod_backup_stale_is_critical(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import logging
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data", after_market_enabled=True)
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c, _a: _holiday_trading_day(d))
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: 0)
    monkeypatch.setattr(
        daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0}
    )
    monkeypatch.setattr(daemon_mod, "run_eod_remote_l0_purge", lambda *a, **kw: 0)
    monkeypatch.setattr(daemon_mod, "check_backup_freshness", lambda **kw: ["2026-09-23.json"])
    now = dt.datetime(2026, 9, 24, 20, 5, tzinfo=ZoneInfo("Asia/Seoul"))

    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: now)

    assert "stage=backup_freshness status=STALE" in caplog.text
    assert "status=HOLIDAY" in caplog.text


def test_daemon_holiday_eod_backup_remote_failure_is_critical(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import logging
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod
    from src.storage.remote import RemoteArchiveError

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data", after_market_enabled=True)
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c, _a: _holiday_trading_day(d))
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: 0)
    monkeypatch.setattr(
        daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0}
    )
    monkeypatch.setattr(daemon_mod, "run_eod_remote_l0_purge", lambda *a, **kw: 0)

    def _freshness(**kw):
        raise RemoteArchiveError("lsjson failed: dial tcp: i/o timeout")

    monkeypatch.setattr(daemon_mod, "check_backup_freshness", _freshness)
    now = dt.datetime(2026, 9, 24, 20, 5, tzinfo=ZoneInfo("Asia/Seoul"))

    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: now)

    assert "stage=backup_freshness status=FAIL" in caplog.text
    assert "status=HOLIDAY" in caplog.text


def _host_status_path(tmp_path):
    return tmp_path / "host-state" / "host_backup_status.json"


def _write_eod_host_status(settings, now, age_h) -> None:
    import datetime as dt
    import json

    status_path = settings.host_backup_status_path
    status_path.parent.mkdir(parents=True, exist_ok=True)
    last_ok = now - dt.timedelta(hours=age_h)
    status_path.write_text(json.dumps({"last_ok_at": last_ok.isoformat()}), encoding="utf-8")


def test_stale_host_backup_degrades_business_day_eod(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import logging
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(
        data_root=pathlib.Path(tmp_path) / "data",
        host_backup_status_path=_host_status_path(tmp_path),
    )
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: 0)
    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", lambda **kw: True)
    digests: list[tuple[str, str]] = []
    monkeypatch.setattr(daemon_mod, "send_digest", lambda subject, body: digests.append((subject, body)) or True)
    monkeypatch.setattr(
        daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 2, "skipped": 0, "failed": 0, "purged": 1}
    )
    monkeypatch.setattr(daemon_mod, "check_backup_freshness", lambda **kw: [])
    eod_time = dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    _write_eod_host_status(settings, eod_time, 40)
    monkeypatch.setattr(
        daemon_mod, "_resolve_trading_day_with_cache", lambda d, c, _a: _business_trading_day(dt.date(2026, 9, 14))
    )

    with caplog.at_level(logging.CRITICAL):
        daemon_mod.run_collector_daemon(
            settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time
        )

    stale = [r for r in caplog.records if "stage=host_backup_freshness" in r.getMessage()]
    assert len(stale) == 1
    assert "reason=stale:40.0h" in stale[0].getMessage()
    assert len(digests) == 1
    assert digests[0][0] == "[krx-alpha] EOD 2026-09-14 DEGRADED"
    assert "host_backup=stale:40.0h" in digests[0][1].splitlines()


def test_business_eod_ignores_data_root_work_status_and_reports_missing(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import json
    import logging
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(
        data_root=pathlib.Path(tmp_path) / "data",
        host_backup_status_path=_host_status_path(tmp_path),
    )
    eod_time = dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    old_status_path = settings.paths.work_root / "host_backup_status.json"
    old_status_path.parent.mkdir(parents=True, exist_ok=True)
    old_status_path.write_text(json.dumps({"last_ok_at": eod_time.isoformat()}), encoding="utf-8")
    assert not settings.host_backup_status_path.exists()

    digests: list[tuple[str, str]] = []
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: 0)
    monkeypatch.setattr(
        daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0}
    )
    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", lambda **kw: True)
    monkeypatch.setattr(daemon_mod, "check_backup_freshness", lambda **kw: [])
    monkeypatch.setattr(daemon_mod, "send_digest", lambda subject, body: digests.append((subject, body)) or True)
    monkeypatch.setattr(
        daemon_mod, "_resolve_trading_day_with_cache", lambda d, c, _a: _business_trading_day(dt.date(2026, 9, 14))
    )

    with caplog.at_level(logging.CRITICAL):
        daemon_mod.run_collector_daemon(
            settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time
        )

    missing = [record for record in caplog.records if "stage=host_backup_freshness" in record.getMessage()]
    assert len(missing) == 1
    assert missing[0].levelno == logging.CRITICAL
    assert "reason=missing" in missing[0].getMessage()
    assert digests[0][0] == "[krx-alpha] EOD 2026-09-14 DEGRADED"
    assert "host_backup=missing" in digests[0][1].splitlines()


def test_fresh_host_backup_keeps_business_day_eod_ok(tmp_path, monkeypatch) -> None:
    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(
        data_root=pathlib.Path(tmp_path) / "data",
        host_backup_status_path=_host_status_path(tmp_path),
    )
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: 0)
    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", lambda **kw: True)
    digests: list[tuple[str, str]] = []
    monkeypatch.setattr(daemon_mod, "send_digest", lambda subject, body: digests.append((subject, body)) or True)
    monkeypatch.setattr(
        daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 2, "skipped": 0, "failed": 0, "purged": 1}
    )
    monkeypatch.setattr(daemon_mod, "check_backup_freshness", lambda **kw: [])
    eod_time = dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    _write_eod_host_status(settings, eod_time, 20)
    monkeypatch.setattr(
        daemon_mod, "_resolve_trading_day_with_cache", lambda d, c, _a: _business_trading_day(dt.date(2026, 9, 14))
    )

    daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time)

    assert len(digests) == 1
    assert digests[0][0] == "[krx-alpha] EOD 2026-09-14 OK"
    assert "host_backup=ok" in digests[0][1].splitlines()


def test_holiday_eod_checks_host_backup_freshness_without_digest(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import logging
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(
        data_root=pathlib.Path(tmp_path) / "data",
        after_market_enabled=True,
        host_backup_status_path=_host_status_path(tmp_path),
    )
    holiday = _holiday_trading_day(dt.date(2026, 9, 24))
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c, _a: holiday)
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: 0)
    monkeypatch.setattr(
        daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0}
    )
    monkeypatch.setattr(daemon_mod, "check_backup_freshness", lambda **kw: [])

    def _must_not_send(**kw):
        raise AssertionError("holiday EOD must not send a digest")

    monkeypatch.setattr(daemon_mod, "send_digest", _must_not_send)
    eod_time = dt.datetime(2026, 9, 24, 20, 5, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    with caplog.at_level(logging.CRITICAL):
        daemon_mod.run_collector_daemon(
            settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time
        )

    missing = [record for record in caplog.records if "stage=host_backup_freshness" in record.getMessage()]
    assert len(missing) == 1
    assert missing[0].levelno == logging.CRITICAL
    assert "reason=missing" in missing[0].getMessage()


def _housekeeping_offload_stub(*, uploaded=1):
    from types import SimpleNamespace

    return SimpleNamespace(l1=SimpleNamespace(uploaded=uploaded), purged=0)


def test_eod_housekeeping_low_disk_skips_maintenance_but_offloads(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import logging
    from unittest.mock import MagicMock

    import src.orchestration.daemon as daemon_mod
    from src.core.config import CollectorSettings

    from src.storage.retention import PruneStats

    monkeypatch.setattr(daemon_mod, "check_disk_watermark", lambda path, *, min_free_gb: False)
    maintenance = MagicMock(return_value=PruneStats(0, 0))
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", maintenance)
    stub = _housekeeping_offload_stub()
    stub.verified_remote_l1 = frozenset({"l1/ls/H0STCNT0/dt=2026-09-01.parquet"})
    offload = MagicMock(return_value=stub)
    monkeypatch.setattr(daemon_mod, "run_eod_offload", offload)
    monkeypatch.setattr(daemon_mod, "run_eod_remote_l0_purge", MagicMock())

    settings = CollectorSettings(data_root=tmp_path)

    with caplog.at_level(logging.CRITICAL):
        result = daemon_mod._run_eod_housekeeping(settings, settings.paths, dt.date(2026, 9, 14), progress=lambda: None)

    # 정규화 패스는 건너뛰고, 원격 검증 집합으로 정규화 없는 삭제 패스만 1회 실행한다.
    maintenance.assert_called_once()
    assert maintenance.call_args.kwargs["normalize"] is False
    assert maintenance.call_args.kwargs["verified_remote_l1"] == stub.verified_remote_l1
    offload.assert_called_once()
    assert result.maintenance_ok is False
    criticals = [r.getMessage() for r in caplog.records if r.levelno == logging.CRITICAL]
    assert any("stage=eod_disk_guard" in message for message in criticals)


def test_eod_housekeeping_failed_partitions_degrade_maintenance(tmp_path, monkeypatch) -> None:
    import datetime as dt

    import src.orchestration.daemon as daemon_mod
    from src.core.config import CollectorSettings
    from src.storage.retention import PruneStats

    monkeypatch.setattr(daemon_mod, "check_disk_watermark", lambda path, *, min_free_gb: True)
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *args, **kwargs: PruneStats(0, 0, failed=1))
    monkeypatch.setattr(daemon_mod, "run_eod_offload", lambda *args, **kwargs: _housekeeping_offload_stub())
    monkeypatch.setattr(daemon_mod, "run_eod_remote_l0_purge", lambda *args, **kwargs: None)

    settings = CollectorSettings(data_root=tmp_path)
    result = daemon_mod._run_eod_housekeeping(settings, settings.paths, dt.date(2026, 9, 14), progress=lambda: None)

    assert result.maintenance_ok is False


def test_housekeeping_second_pass_reuses_l1_first_does_not(tmp_path, monkeypatch) -> None:
    import datetime as dt

    import src.orchestration.daemon as daemon_mod
    from src.core.config import CollectorSettings
    from src.storage.retention import PruneStats

    monkeypatch.setattr(daemon_mod, "check_disk_watermark", lambda path, *, min_free_gb: True)
    calls: list[dict] = []

    def _spy(journal_root, **kw):
        calls.append(dict(kw))
        return PruneStats(0, 0)

    verified = frozenset({"l1/kis/H0STCNT0/dt=2026-09-01.parquet"})
    stub = _housekeeping_offload_stub()
    stub.verified_remote_l1 = verified
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", _spy)
    monkeypatch.setattr(daemon_mod, "run_eod_offload", lambda *a, **k: stub)
    monkeypatch.setattr(daemon_mod, "run_eod_remote_l0_purge", lambda *a, **k: None)

    settings = CollectorSettings(data_root=tmp_path)
    daemon_mod._run_eod_housekeeping(settings, settings.paths, dt.date(2026, 9, 14), progress=lambda: None)

    assert len(calls) == 2
    assert calls[0]["verified_remote_l1"] is None
    assert "reuse_fresh_l1" not in calls[0]
    assert calls[1]["verified_remote_l1"] == verified
    assert calls[1]["reuse_fresh_l1"] is True


def test_housekeeping_failed_offload_skips_second_pass(tmp_path, monkeypatch) -> None:
    import datetime as dt

    import src.orchestration.daemon as daemon_mod
    from src.core.config import CollectorSettings
    from src.storage.remote import RemoteArchiveError
    from src.storage.retention import PruneStats

    monkeypatch.setattr(daemon_mod, "check_disk_watermark", lambda path, *, min_free_gb: True)
    calls: list[dict] = []

    def _spy(journal_root, **kw):
        calls.append(dict(kw))
        return PruneStats(0, 0)

    def _fail(*a, **k):
        raise RemoteArchiveError("lsjson failed")

    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", _spy)
    monkeypatch.setattr(daemon_mod, "run_eod_offload", _fail)
    monkeypatch.setattr(daemon_mod, "run_eod_remote_l0_purge", lambda *a, **k: None)

    settings = CollectorSettings(data_root=tmp_path)
    daemon_mod._run_eod_housekeeping(settings, settings.paths, dt.date(2026, 9, 14), progress=lambda: None)

    assert len(calls) == 1
    assert not any(c.get("reuse_fresh_l1") is True for c in calls)


def test_housekeeping_low_disk_second_pass_keeps_contract(tmp_path, monkeypatch) -> None:
    import datetime as dt

    import src.orchestration.daemon as daemon_mod
    from src.core.config import CollectorSettings
    from src.storage.retention import PruneStats

    monkeypatch.setattr(daemon_mod, "check_disk_watermark", lambda path, *, min_free_gb: False)
    calls: list[dict] = []

    def _spy(journal_root, **kw):
        calls.append(dict(kw))
        return PruneStats(0, 0)

    stub = _housekeeping_offload_stub()
    stub.verified_remote_l1 = frozenset({"l1/kis/H0STCNT0/dt=2026-09-01.parquet"})
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", _spy)
    monkeypatch.setattr(daemon_mod, "run_eod_offload", lambda *a, **k: stub)
    monkeypatch.setattr(daemon_mod, "run_eod_remote_l0_purge", lambda *a, **k: None)

    settings = CollectorSettings(data_root=tmp_path)
    daemon_mod._run_eod_housekeeping(settings, settings.paths, dt.date(2026, 9, 14), progress=lambda: None)

    assert len(calls) == 1
    assert calls[0]["normalize"] is False
    assert calls[0]["reuse_fresh_l1"] is True


@pytest.mark.parametrize("change", ["unchanged", "appended", "missing_l1", "corrupt_l0"])
def test_housekeeping_preserves_invalidated_inputs_until_reverified(tmp_path, monkeypatch, change) -> None:
    import datetime as dt
    import json
    import os
    import subprocess
    from unittest.mock import Mock

    import pyarrow.parquet as pq
    import zstandard as zstd

    from src.core.config import CollectorSettings
    from src.orchestration import daemon, eod
    from src.storage.normalization import normalize_l0_partition
    from src.storage.remote import GDriveArchiver, SyncStats
    from tests.unit.orchestration.test_eod import _write_due_journal

    cfg = CollectorSettings(data_root=tmp_path)
    paths = cfg.paths
    journal = _write_due_journal(tmp_path)
    l1 = paths.archive_root / "kis/H0STCNT0/dt=2026-09-01.parquet"
    repo_path = "l1/kis/H0STCNT0/dt=2026-09-01.parquet"

    class MemoryArchive(GDriveArchiver):
        def __init__(self):
            super().__init__(remote_name="fake", remote_path="archive", runner=self.run)
            self.l1_bytes = {}
            self.remote_l0_exists = True

        def run(self, cmd, **kwargs):
            if cmd[1] == "purge":
                self.remote_l0_exists = False
            return subprocess.CompletedProcess(cmd, 0, stdout="[]", stderr="")

        def sync_l1_tree(self, root, *, progress=None):
            self.l1_bytes = {
                "l1/" + path.relative_to(root).as_posix(): path.read_bytes() for path in root.rglob("*.parquet")
            }
            return SyncStats(uploaded=len(self.l1_bytes))

        def sync_manifest_tree(self, root):
            return SyncStats()

        def remote_file_sizes(self, prefix):
            return {path: len(content) for path, content in self.l1_bytes.items()}

        def remote_files(self, prefix):
            return set(self.l1_bytes)

    arc = MemoryArchive()
    monkeypatch.setattr(GDriveArchiver, "try_from_env", classmethod(lambda cls: arc))
    normalizer = Mock(wraps=normalize_l0_partition)
    monkeypatch.setattr(eod, "run_isolated_normalize", normalizer)
    monkeypatch.setattr(daemon, "check_disk_watermark", lambda *args, **kwargs: True)
    changed = False

    def offload(*args, **kwargs):
        nonlocal changed
        assert kwargs["journal_root"] == paths.journal_root
        result = eod.run_eod_offload(*args, **kwargs)
        assert l1.exists()
        assert result.purged == 0
        if not changed:
            changed = True
            if change == "appended":
                record = {
                    "raw": "new",
                    "recv_mono_ns": 3,
                    "recv_wall_ns": 4,
                    "conn_id": "c1",
                    "conn_seq": 2,
                    "vendor": "kis",
                    "tr_id": "H0STCNT0",
                }
                with journal.open("ab") as stream:
                    stream.write(zstd.ZstdCompressor().compress((json.dumps(record) + "\n").encode()))
            elif change == "missing_l1":
                l1.unlink()
            elif change == "corrupt_l0":
                journal.write_bytes(b"broken frame")
            if change in {"appended", "corrupt_l0"}:
                newer = l1.stat().st_mtime_ns + 1
                os.utime(journal, ns=(newer, newer))
        return result

    monkeypatch.setattr(daemon, "run_eod_offload", offload)
    day = dt.date(2026, 10, 15)
    result = daemon._run_eod_housekeeping(cfg, paths, day, progress=lambda: None)

    if change == "unchanged":
        assert normalizer.call_count == 1
        assert result.deleted == 1
        assert not journal.exists()
        assert not arc.remote_l0_exists
        assert l1.read_bytes() == arc.l1_bytes[repo_path]
    elif change == "corrupt_l0":
        assert result.deleted == 0
        assert not result.maintenance_ok
        assert not journal.exists()
        assert (paths.quarantine_root / journal.relative_to(paths.journal_root)).read_bytes() == b"broken frame"
        assert arc.remote_l0_exists
        monkeypatch.setattr(daemon, "run_eod_offload", eod.run_eod_offload)
        daemon._run_eod_housekeeping(cfg, paths, day + dt.timedelta(days=1), progress=lambda: None)
        assert arc.remote_l0_exists
    else:
        assert result.deleted == 0
        assert journal.exists()
        assert arc.remote_l0_exists
        assert pq.read_metadata(l1).num_rows == (2 if change == "appended" else 1)

        monkeypatch.setattr(daemon, "run_eod_offload", eod.run_eod_offload)
        result = daemon._run_eod_housekeeping(cfg, paths, day + dt.timedelta(days=1), progress=lambda: None)
        assert result.deleted == 1
        assert not journal.exists()
        assert not arc.remote_l0_exists
        assert l1.read_bytes() == arc.l1_bytes[repo_path]

    if change != "corrupt_l0":
        cleanup = eod.run_eod_offload(
            paths.archive_root,
            paths.manifest_dir,
            journal_root=paths.journal_root,
            reference_date=day + dt.timedelta(days=2),
        )
        assert cleanup.purged == 1
        assert not l1.exists()


def _prearm_daemon_runner(tmp_path, plan, *, enabled=True):
    from types import SimpleNamespace
    import src.orchestration.daemon as daemon_mod
    from src.core.config import AuditMode, AuditSettings
    from src.core.paths import DataPaths

    runner = object.__new__(daemon_mod.DaemonRunner)
    runner._runtime = SimpleNamespace(audit=AuditSettings(mode=AuditMode.OFF))
    runner._cfg = SimpleNamespace(
        after_market_enabled=enabled, host_backup_status_path=tmp_path / "host.json", host_backup_max_age_h=36
    )
    runner._paths = DataPaths(tmp_path / "data")
    runner._premarket_cfg = SimpleNamespace(enabled=False)
    st = daemon_mod.DaemonState()
    st.aftermarket_plan = tuple(plan)
    runner._state = st
    return runner


def _ns(day, h, m, s=0):
    import datetime as dt
    from zoneinfo import ZoneInfo

    return int(
        dt.datetime(day.year, day.month, day.day, h, m, s, tzinfo=ZoneInfo("Asia/Seoul")).timestamp() * 1_000_000_000
    )


def _write_daemon_manifest(paths, day, shard, *, started_ns, ack_ns):
    from src.realtime.manifest import SessionManifest

    manifest = SessionManifest(
        session_date=day,
        clock_offset_ns=0,
        started_at_ns=started_ns,
        venue=shard.venue.value,
        session="nxt_after" if shard.venue.value == "nxt" else "krx_after",
        expected_close_ns=1,
        writer_closed_at_ns=started_ns + 1,
        planned_pairs=[{"symbol": s, "tr_id": t} for s in shard.symbols for t in shard.streams],
        shard_index=shard.shard_index,
        credential_key_id=shard.credential_key_id,
    )
    for s in shard.symbols:
        for t in shard.streams:
            manifest.record_ack(vendor="kis", tr_id=t, symbol=s, rt_cd="0", accepted=True, acked_at_ns=ack_ns)
    path = paths.aftermarket_manifest_path(day, shard.venue, shard.shard_index)
    path.parent.mkdir(parents=True, exist_ok=True)
    manifest.save(path)
    return path


def test_eod_logs_one_error_per_late_shard(tmp_path, caplog) -> None:
    import datetime as dt
    import logging
    from src.core.session_anchors import standard_session_anchors
    from src.realtime.contracts import MarketVenue
    from src.realtime.kis_sharding import AftermarketShard

    day = dt.date(2026, 10, 18)
    anchors = standard_session_anchors(day)
    nxt = AftermarketShard(MarketVenue.NXT, 0, ("005930",), ("H0NXCNT0", "H0NXASP0"), "0", "id0")
    krx = AftermarketShard(MarketVenue.KRX, 0, ("000660",), ("H0STCNT0", "H0STASP0"), "1", "id1")
    runner = _prearm_daemon_runner(tmp_path, (nxt, krx))
    _write_daemon_manifest(runner._paths, day, nxt, started_ns=_ns(day, 15, 39, 2), ack_ns=_ns(day, 15, 39, 6))
    _write_daemon_manifest(runner._paths, day, krx, started_ns=_ns(day, 16, 0, 11), ack_ns=_ns(day, 16, 0, 12))
    with caplog.at_level(logging.INFO):
        late = runner._log_aftermarket_prearm(day, anchors)
    assert late == 1
    errors = [r for r in caplog.records if r.levelno == logging.ERROR and "stage=aftermarket_prearm" in r.getMessage()]
    assert len(errors) == 1
    assert "status=LATE_START venue=krx shard=0" in errors[0].getMessage()
    assert "stage=aftermarket_prearm status=DEGRADED armed=1 shards=2" in caplog.text


def test_digest_carries_prearm_late_count(tmp_path, monkeypatch) -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo
    import src.orchestration.daemon as daemon_mod
    from src.core.session_anchors import standard_session_anchors
    from src.realtime.contracts import MarketVenue
    from src.realtime.kis_sharding import AftermarketShard

    day = dt.date(2026, 10, 18)
    anchors = standard_session_anchors(day)
    kst = ZoneInfo("Asia/Seoul")
    now = dt.datetime(2026, 10, 18, 20, 5, tzinfo=kst)
    nxt = AftermarketShard(MarketVenue.NXT, 0, ("005930",), ("H0NXCNT0", "H0NXASP0"), "0", "id0")
    krx = AftermarketShard(MarketVenue.KRX, 0, ("000660",), ("H0STCNT0", "H0STASP0"), "1", "id1")
    runner = _prearm_daemon_runner(tmp_path, (nxt, krx))
    runner._run_id = "test"
    runner._eod_progress = lambda: None
    _write_daemon_manifest(runner._paths, day, nxt, started_ns=_ns(day, 15, 39, 2), ack_ns=_ns(day, 15, 39, 6))
    _write_daemon_manifest(runner._paths, day, krx, started_ns=_ns(day, 16, 0, 11), ack_ns=_ns(day, 16, 0, 12))
    monkeypatch.setattr(
        daemon_mod,
        "_run_eod_housekeeping",
        lambda cfg, paths, ref_day, progress=None: type(
            "H", (), {"deleted": 0, "uploaded": 0, "purged": 0, "maintenance_ok": True, "offload_ok": True}
        )(),
    )
    monkeypatch.setattr(
        daemon_mod.DaemonRunner, "_reconcile_regular_session", lambda self, ref_day, day_v, anchors_v, audit=None: (True, True)
    )
    monkeypatch.setattr(
        daemon_mod.DaemonRunner, "_reconcile_crosscheck_sessions", lambda self, ref_day, day_v, anchors_v: {}
    )
    monkeypatch.setattr(daemon_mod, "_check_backup", lambda *a, **k: ([], True, "fresh"))
    digests = []
    monkeypatch.setattr(daemon_mod, "send_digest", lambda subject, body: digests.append((subject, body)))
    from tests.unit.orchestration.daemon_fixtures import _business_trading_day

    runner._run_business_eod(now, day, _business_trading_day(day), anchors)
    assert len(digests) == 1
    assert "aftermarket_prearm_late=1" in digests[0][1].splitlines()


def test_prearm_verdicts_never_change_eod_status(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import logging
    from zoneinfo import ZoneInfo
    import src.orchestration.daemon as daemon_mod
    from src.core.session_anchors import standard_session_anchors
    from src.realtime.contracts import MarketVenue
    from src.realtime.kis_sharding import AftermarketShard

    day = dt.date(2026, 10, 18)
    anchors = standard_session_anchors(day)
    now = dt.datetime(2026, 10, 18, 20, 5, tzinfo=ZoneInfo("Asia/Seoul"))
    krx = AftermarketShard(MarketVenue.KRX, 0, ("000660",), ("H0STCNT0", "H0STASP0"), "1", "id1")
    runner = _prearm_daemon_runner(tmp_path, (krx,))
    runner._run_id = "test"
    runner._eod_progress = lambda: None
    _write_daemon_manifest(runner._paths, day, krx, started_ns=_ns(day, 16, 0, 11), ack_ns=_ns(day, 16, 0, 12))
    monkeypatch.setattr(
        daemon_mod,
        "_run_eod_housekeeping",
        lambda cfg, paths, ref_day, progress=None: type(
            "H", (), {"deleted": 0, "uploaded": 0, "purged": 0, "maintenance_ok": True, "offload_ok": True}
        )(),
    )
    monkeypatch.setattr(
        daemon_mod.DaemonRunner, "_reconcile_regular_session", lambda self, ref_day, day_v, anchors_v, audit=None: (True, True)
    )
    monkeypatch.setattr(
        daemon_mod.DaemonRunner, "_reconcile_crosscheck_sessions", lambda self, ref_day, day_v, anchors_v: {}
    )
    monkeypatch.setattr(daemon_mod, "_check_backup", lambda *a, **k: ([], True, "fresh"))
    monkeypatch.setattr(daemon_mod, "send_digest", lambda subject, body: True)
    monkeypatch.setattr(
        daemon_mod.DaemonRunner, "_aftermarket_eod_blocked", lambda self, now_v, ref_day, anchors_v: False
    )
    with caplog.at_level(logging.INFO):
        runner._run_business_eod(now, day, object(), anchors)
    summaries = [r.getMessage() for r in caplog.records if "stage=eod_maintenance deleted_partitions=" in r.getMessage()]
    assert len(summaries) == 1
    assert summaries[0].endswith("status=OK")
    assert "stage=aftermarket_prearm status=DEGRADED" in caplog.text


def test_no_prearm_logs_without_plan(tmp_path, caplog) -> None:
    import datetime as dt
    import logging
    from src.core.session_anchors import standard_session_anchors

    day = dt.date(2026, 10, 18)
    anchors = standard_session_anchors(day)
    runner = _prearm_daemon_runner(tmp_path, (), enabled=False)
    with caplog.at_level(logging.INFO):
        assert runner._log_aftermarket_prearm(day, anchors) == 0
    assert "stage=aftermarket_prearm" not in caplog.text
    runner2 = _prearm_daemon_runner(tmp_path, (), enabled=True)
    with caplog.at_level(logging.INFO):
        assert runner2._log_aftermarket_prearm(day, anchors) == 0
    assert "stage=aftermarket_prearm" not in caplog.text


def _write_coverage_daemon_manifest(paths, day, shard, *, counts):
    from src.realtime.manifest import SessionManifest

    manifest = SessionManifest(
        session_date=day,
        clock_offset_ns=0,
        started_at_ns=1,
        venue=shard.venue.value,
        session="nxt_after" if shard.venue.value == "nxt" else "krx_after",
        expected_close_ns=1,
        writer_closed_at_ns=2,
        planned_pairs=[{"symbol": s, "tr_id": t} for s in shard.symbols for t in shard.streams],
        shard_index=shard.shard_index,
        credential_key_id=shard.credential_key_id,
    )
    for s in shard.symbols:
        for t in shard.streams:
            manifest.record_ack(vendor="kis", tr_id=t, symbol=s, rt_cd="0", accepted=True, acked_at_ns=1)
    for symbol, per_stream in counts.items():
        for stream, n in per_stream.items():
            for _ in range(n):
                manifest.record_frame_count(symbol, stream)
    path = paths.aftermarket_manifest_path(day, shard.venue, shard.shard_index)
    path.parent.mkdir(parents=True, exist_ok=True)
    manifest.save(path)
    return path


def test_eod_warns_with_zero_frame_symbol_list(tmp_path, caplog) -> None:
    import datetime as dt
    import logging
    from src.realtime.contracts import MarketVenue
    from src.realtime.kis_sharding import AftermarketShard

    day = dt.date(2026, 10, 18)
    shard = AftermarketShard(MarketVenue.NXT, 0, ("A", "B", "C", "D"), ("H0NXCNT0", "H0NXASP0"), "0", "id0")
    runner = _prearm_daemon_runner(tmp_path, (shard,))
    _write_coverage_daemon_manifest(
        runner._paths, day, shard, counts={"A": {"H0NXCNT0": 5}, "C": {"H0NXASP0": 2}}
    )
    with caplog.at_level(logging.INFO):
        total = runner._log_aftermarket_coverage(day)
    assert total == 2
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING and "stage=aftermarket_coverage" in r.getMessage()]
    assert len(warnings) == 1
    assert "status=ZERO_FRAMES venue=nxt shard=0" in warnings[0].getMessage()
    assert "symbols=B,D" in warnings[0].getMessage()


def test_eod_digest_carries_zero_frame_count_and_keeps_ok(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import logging
    from zoneinfo import ZoneInfo
    import src.orchestration.daemon as daemon_mod
    from src.core.session_anchors import standard_session_anchors
    from src.realtime.contracts import MarketVenue
    from src.realtime.kis_sharding import AftermarketShard

    day = dt.date(2026, 10, 18)
    anchors = standard_session_anchors(day)
    now = dt.datetime(2026, 10, 18, 20, 5, tzinfo=ZoneInfo("Asia/Seoul"))
    shard = AftermarketShard(MarketVenue.NXT, 0, ("A", "B", "C", "D"), ("H0NXCNT0", "H0NXASP0"), "0", "id0")
    runner = _prearm_daemon_runner(tmp_path, (shard,))
    runner._run_id = "test"
    runner._eod_progress = lambda: None
    _write_coverage_daemon_manifest(
        runner._paths, day, shard, counts={"A": {"H0NXCNT0": 5}, "C": {"H0NXASP0": 2}}
    )
    monkeypatch.setattr(
        daemon_mod,
        "_run_eod_housekeeping",
        lambda cfg, paths, ref_day, progress=None: type(
            "H", (), {"deleted": 0, "uploaded": 0, "purged": 0, "maintenance_ok": True, "offload_ok": True}
        )(),
    )
    monkeypatch.setattr(
        daemon_mod.DaemonRunner, "_reconcile_regular_session", lambda self, ref_day, day_v, anchors_v, audit=None: (True, True)
    )
    monkeypatch.setattr(
        daemon_mod.DaemonRunner, "_reconcile_crosscheck_sessions", lambda self, ref_day, day_v, anchors_v: {}
    )
    monkeypatch.setattr(daemon_mod, "_check_backup", lambda *a, **k: ([], True, "fresh"))
    digests = []
    monkeypatch.setattr(daemon_mod, "send_digest", lambda subject, body: digests.append((subject, body)))
    monkeypatch.setattr(
        daemon_mod.DaemonRunner, "_aftermarket_eod_blocked", lambda self, now_v, ref_day, anchors_v: False
    )
    from tests.unit.orchestration.daemon_fixtures import _business_trading_day

    with caplog.at_level(logging.INFO):
        runner._run_business_eod(now, day, _business_trading_day(day), anchors)
    assert len(digests) == 1
    assert "aftermarket_zero_frame_symbols=2" in digests[0][1].splitlines()
    assert digests[0][0].endswith("OK")


def test_eod_clean_coverage_logs_ok_summary_only(tmp_path, caplog) -> None:
    import datetime as dt
    import logging
    from src.realtime.contracts import MarketVenue
    from src.realtime.kis_sharding import AftermarketShard

    day = dt.date(2026, 10, 18)
    shard = AftermarketShard(MarketVenue.NXT, 0, ("A", "B"), ("H0NXCNT0", "H0NXASP0"), "0", "id0")
    runner = _prearm_daemon_runner(tmp_path, (shard,))
    _write_coverage_daemon_manifest(
        runner._paths, day, shard, counts={"A": {"H0NXCNT0": 1}, "B": {"H0NXASP0": 1}}
    )
    with caplog.at_level(logging.INFO):
        assert runner._log_aftermarket_coverage(day) == 0
    assert not [r for r in caplog.records if r.levelno == logging.WARNING and "stage=aftermarket_coverage" in r.getMessage()]
    assert "stage=aftermarket_coverage status=OK zero=0" in caplog.text


def test_eod_no_coverage_logs_without_plan(tmp_path, caplog) -> None:
    import datetime as dt
    import logging

    day = dt.date(2026, 10, 18)
    from src.realtime.contracts import MarketVenue
    from src.realtime.kis_sharding import AftermarketShard

    shard = AftermarketShard(MarketVenue.NXT, 0, ("A",), ("H0NXCNT0", "H0NXASP0"), "0", "id0")
    for enabled, plan in ((False, (shard,)), (True, ())):
        runner = _prearm_daemon_runner(tmp_path, plan, enabled=enabled)
        with caplog.at_level(logging.INFO):
            assert runner._log_aftermarket_coverage(day) == 0
    assert "stage=aftermarket_coverage" not in caplog.text


def _stub_session_audit(day, *, status="OK", reason="ok", confirmed=7140, trades=1_000_000, tail=5.0):
    from src.storage.l0_audit import (
        AuditStatus,
        LedgerTotals,
        LatencyMinute,
        SessionAudit,
        StreamLatency,
        SymbolLedger,
    )

    latency = (
        StreamLatency(
            stream="H0STCNT0",
            status=AuditStatus.OK,
            reason="ok",
            rows=60,
            event_time_missing=0,
            minutes=(LatencyMinute(minute_kst="09:01", count=60, p50_s=12.0, max_s=29.0, min_s=1.0),),
        ),
        StreamLatency(
            stream="H0STASP0",
            status=AuditStatus.OK,
            reason="ok",
            rows=60,
            event_time_missing=0,
            minutes=(LatencyMinute(minute_kst="09:02", count=60, p50_s=3.0, max_s=6.0, min_s=0.5),),
        ),
    )
    symbols = (
        SymbolLedger(
            symbol="005930",
            status="OK",
            frames=trades + 1,
            trades_observed=trades,
            confirmed_missing=confirmed,
            count_only_missing=0,
            net_missing=0,
            volume_deficit=0,
            duplicates=0,
            regressions=0,
            resets=0,
            decode_fail=0,
            intervals=(),
            intervals_truncated=0,
            manifest_frames=None,
        ),
    )
    totals = LedgerTotals(
        symbols=1,
        symbols_with_loss=1,
        symbols_without_counter=0,
        trades_observed=trades,
        confirmed_missing=confirmed,
        count_only_missing=0,
        net_missing_positive=0,
        volume_deficit=0,
        loss_ratio=(confirmed / trades) if trades else 0.0,
    )
    return SessionAudit(
        schema_version=1,
        session_date=day,
        vendor="ls",
        venue="krx",
        session="regular",
        status=AuditStatus(status),
        reason=reason,
        generated_at_ns=1,
        sources=(),
        latency=latency,
        symbols=symbols,
        totals=totals,
        tail_silence_s=tail,
        last_regular_tick_recv_ns=1,
    )


def _stub_full_eod(monkeypatch, tmp_path):
    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    import src.orchestration.daemon as daemon_mod
    from src.core.config import CollectorSettings

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(
        data_root=pathlib.Path(tmp_path) / "data",
        host_backup_status_path=_host_status_path(tmp_path),
    )
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: 0)
    monkeypatch.setattr(
        daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0}
    )
    monkeypatch.setattr(daemon_mod, "check_backup_freshness", lambda **kw: [])
    digests: list[tuple[str, str]] = []
    monkeypatch.setattr(daemon_mod, "send_digest", lambda subject, body: digests.append((subject, body)) or True)
    eod_time = dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    _write_eod_host_status(settings, eod_time, 20)
    monkeypatch.setattr(
        daemon_mod, "_resolve_trading_day_with_cache", lambda d, c, _a: _business_trading_day(dt.date(2026, 9, 14))
    )
    return settings, digests, eod_time


def test_audit_derivation_precedes_housekeeping(tmp_path, monkeypatch) -> None:
    # Given: 파생·유지보수 호출을 기록하는 EOD 데몬
    import src.orchestration.daemon as daemon_mod

    settings, digests, eod_time = _stub_full_eod(monkeypatch, tmp_path)
    order: list[str] = []
    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", lambda **kw: True)
    monkeypatch.setattr(
        daemon_mod.DaemonRunner,
        "_derive_session_audit",
        lambda self, ref_day, day, anchors: order.append("derive") or None,
    )
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: order.append("maintenance") or 0)

    # When
    daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time)

    # Then: 파생이 정확히 한 번, 유지보수보다 먼저 실행된다
    assert order.count("derive") == 1
    assert order.index("derive") < order.index("maintenance")
    assert len(digests) == 1


def test_audit_derivation_runs_once_per_date(tmp_path, monkeypatch) -> None:
    # Given: 같은 날짜에 두 번 도는 EOD
    import src.orchestration.daemon as daemon_mod

    settings, digests, eod_time = _stub_full_eod(monkeypatch, tmp_path)
    calls: list = []
    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", lambda **kw: True)
    monkeypatch.setattr(
        daemon_mod.DaemonRunner,
        "_derive_session_audit",
        lambda self, ref_day, day, anchors: calls.append(ref_day) or None,
    )

    # When
    daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=2, now_fn=lambda: eod_time)

    # Then: 두 번째 스텝은 eod_attempted_for 게이트로 파생을 건너뛴다
    assert len(calls) == 1
    assert len(digests) == 1


def test_reconcile_receives_derived_audit(tmp_path, monkeypatch) -> None:
    # Given: 알려진 감사를 반환하는 파생 스텁
    import datetime as dt

    import src.orchestration.daemon as daemon_mod
    from src.core.config import AuditSettings

    settings, digests, eod_time = _stub_full_eod(monkeypatch, tmp_path)
    stub = _stub_session_audit(dt.date(2026, 9, 14))
    monkeypatch.setattr(daemon_mod.DaemonRunner, "_derive_session_audit", lambda self, ref_day, day, anchors: stub)
    seen: dict = {}

    def _reconcile(**kw):
        seen.update(kw)
        return True

    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", _reconcile)

    # When
    daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time)

    # Then: 기록된 호출에 감사 객체와 런타임 설정이 그대로 전달된다
    assert seen["audit"] is stub
    assert isinstance(seen["audit_settings"], AuditSettings)


def test_derivation_failure_never_aborts_eod(tmp_path, monkeypatch, caplog) -> None:
    # Given: 파생이 OSError로 실패하는 EOD
    import logging

    import src.orchestration.daemon as daemon_mod

    settings, digests, eod_time = _stub_full_eod(monkeypatch, tmp_path)

    def _boom(*a, **kw):
        raise OSError("journal vanished")

    monkeypatch.setattr(daemon_mod, "derive_session_audit", _boom)
    seen: dict = {}
    calls: list[str] = []

    def _reconcile(**kw):
        seen.update(kw)
        return True

    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", _reconcile)
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: calls.append("maintenance") or 0)

    # When
    with caplog.at_level(logging.ERROR):
        daemon_mod.run_collector_daemon(
            settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time
        )

    # Then: ERROR+traceback 한 줄, 나머지는 전부 실행, 상태는 OK
    errors = [r for r in caplog.records if "stage=session_audit status=ERROR" in r.getMessage()]
    assert len(errors) == 1
    assert errors[0].levelno == logging.ERROR
    assert errors[0].exc_info is not None
    assert calls == ["maintenance", "maintenance"]
    assert seen["audit"] is None
    assert len(digests) == 1
    assert digests[0][0].endswith("OK")


def test_write_failure_keeps_verdict_for_reconciliation(tmp_path, monkeypatch, caplog) -> None:
    # Given: 파생은 성공하고 저장이 실패하는 EOD
    import datetime as dt
    import logging

    import src.orchestration.daemon as daemon_mod

    settings, digests, eod_time = _stub_full_eod(monkeypatch, tmp_path)
    stub = _stub_session_audit(dt.date(2026, 9, 14))
    monkeypatch.setattr(daemon_mod, "derive_session_audit", lambda *a, **kw: stub)

    def _boom(audit, path):
        raise OSError("disk full")

    monkeypatch.setattr(daemon_mod, "write_session_audit", _boom)
    seen: dict = {}

    def _reconcile(**kw):
        seen.update(kw)
        return True

    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", _reconcile)

    # When
    with caplog.at_level(logging.ERROR):
        daemon_mod.run_collector_daemon(
            settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time
        )

    # Then: WRITE_FAIL 기록 + 파생 감사가 그대로 정합성 검사로 전달
    assert [r for r in caplog.records if "stage=session_audit status=WRITE_FAIL" in r.getMessage()]
    assert seen["audit"] is stub
    assert len(digests) == 1


def test_mode_off_skips_audit_pipeline(tmp_path, monkeypatch) -> None:
    # Given: 감사가 꺼진 런타임으로 직접 호출하는 EOD
    import dataclasses
    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    import src.orchestration.daemon as daemon_mod
    from src.core.config import AuditMode, AuditSettings, CollectorSettings, resolve_collector_runtime
    from src.core.session_anchors import standard_session_anchors
    from src.orchestration.trading_day_gate import TradingDayStatus, TradingDayView

    from tests.unit.orchestration.daemon_fixtures import _business_trading_day as _biz

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    runtime = dataclasses.replace(
        resolve_collector_runtime(collector=settings), audit=AuditSettings(mode=AuditMode.OFF)
    )
    runner = daemon_mod.DaemonRunner(runtime=runtime, shutdown=None, now=lambda: None, sleep=lambda s: None)
    day = dt.date(2026, 9, 14)
    calls: dict[str, int] = {"derive": 0, "write": 0, "log": 0}
    monkeypatch.setattr(daemon_mod, "derive_session_audit", lambda *a, **kw: calls.__setitem__("derive", 1) or None)
    monkeypatch.setattr(daemon_mod, "write_session_audit", lambda *a, **kw: calls.__setitem__("write", 1) or None)
    monkeypatch.setattr(daemon_mod, "log_session_audit", lambda *a, **kw: calls.__setitem__("log", 1) or None)
    seen: dict = {}

    def _reconcile(**kw):
        seen.update(kw)
        return True

    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", _reconcile)
    monkeypatch.setattr(
        daemon_mod,
        "_run_eod_housekeeping",
        lambda *a, **k: daemon_mod._EodHousekeeping(
            deleted=0, uploaded=0, purged=0, maintenance_ok=True, offload_ok=True
        ),
    )
    monkeypatch.setattr(daemon_mod, "_check_backup", lambda *a, **k: ([], True, "ok"))
    digests: list = []
    monkeypatch.setattr(daemon_mod, "send_digest", lambda subject, body: digests.append((subject, body)) or True)
    view = TradingDayView(date=day, status=TradingDayStatus.BUSINESS, trading_day=_biz(day))
    now = dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    # When
    runner._run_business_eod(now, day, view, standard_session_anchors(day))

    # Then: 파생·저장·로그 미호출, 정합성 검사는 audit=None
    assert calls == {"derive": 0, "write": 0, "log": 0}
    assert seen["audit"] is None
    assert "audit_status=OFF" in digests[0][1].splitlines()
    assert "regular_trade_loss=-" in digests[0][1].splitlines()
    assert "open_max_latency_s=-" in digests[0][1].splitlines()


def test_holiday_and_unknown_days_skip_derivation(tmp_path, monkeypatch) -> None:
    # Given: HOLIDAY/UNKNOWN 날짜의 EOD
    import datetime as dt

    import src.orchestration.daemon as daemon_mod

    from tests.unit.orchestration.daemon_fixtures import _holiday_trading_day

    day = dt.date(2026, 9, 14)

    # When: HOLIDAY
    settings, digests, eod_time = _stub_full_eod(monkeypatch, tmp_path)
    calls: list = []
    monkeypatch.setattr(
        daemon_mod.DaemonRunner,
        "_derive_session_audit",
        lambda self, ref_day, day_v, anchors: calls.append(ref_day) or None,
    )
    monkeypatch.setattr(
        daemon_mod,
        "_resolve_trading_day_with_cache",
        lambda d, c, _a: _holiday_trading_day(d),
    )
    daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time)

    # Then: 파생 미호출 (휴일 EOD는 다이제스트를 보내지 않는다)
    assert calls == []
    assert digests == []

    # When: UNKNOWN (실제 파생 경로, 빈 저널)
    settings2, digests2, eod_time2 = _stub_full_eod(monkeypatch, tmp_path)
    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", lambda **kw: True)
    monkeypatch.setattr(
        daemon_mod,
        "_resolve_trading_day_with_cache",
        lambda d, c, _a: None,
    )
    daemon_mod.run_collector_daemon(
        settings=settings2, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time2
    )

    # Then: 비즈니스일이 아니라 파생이 조기 종료되어 아티팩트 없음, EOD는 정상 완료
    assert not list(settings2.paths.manifest_dir.rglob("*.audit.json"))
    assert len(digests2) == 1


def test_digest_carries_audit_lines(tmp_path, monkeypatch) -> None:
    # Given: 확정 결손 감사를 반환하는 파생 스텁
    import datetime as dt

    import src.orchestration.daemon as daemon_mod

    settings, digests, eod_time = _stub_full_eod(monkeypatch, tmp_path)
    day = dt.date(2026, 9, 14)
    stub = _stub_session_audit(day)
    monkeypatch.setattr(daemon_mod, "derive_session_audit", lambda *a, **kw: stub)
    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", lambda **kw: True)

    # When
    daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time)

    # Then: reconciled 다음 줄에 감사 3종 세트, 기존 키 순서 유지
    lines = digests[0][1].splitlines()
    assert "audit_status=OK" in lines
    assert "regular_trade_loss=7140/1000000" in lines
    assert "open_max_latency_s=29.0" in lines
    assert lines.index("audit_status=OK") == lines.index("reconciled=True") + 1
    assert "aftermarket_ready=True" in lines


def test_digest_without_audit_reports_dashes(tmp_path, monkeypatch) -> None:
    # Given: 감사가 없는 EOD
    import src.orchestration.daemon as daemon_mod

    settings, digests, eod_time = _stub_full_eod(monkeypatch, tmp_path)
    monkeypatch.setattr(daemon_mod.DaemonRunner, "_derive_session_audit", lambda self, ref_day, day_v, anchors: None)
    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", lambda **kw: True)

    # When
    daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time)

    # Then
    lines = digests[0][1].splitlines()
    assert "audit_status=UNAVAILABLE" in lines
    assert "regular_trade_loss=-" in lines
    assert "open_max_latency_s=-" in lines


def test_artifact_lands_in_offloaded_tree(tmp_path, monkeypatch) -> None:
    # Given: 스텁 감사를 쓰는 실제 저장 경로
    import datetime as dt
    import json
    import pathlib

    import src.orchestration.daemon as daemon_mod

    settings, digests, eod_time = _stub_full_eod(monkeypatch, tmp_path)
    day = dt.date(2026, 9, 14)
    stub = _stub_session_audit(day)
    monkeypatch.setattr(daemon_mod, "derive_session_audit", lambda *a, **kw: stub)
    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", lambda **kw: True)
    real_write = daemon_mod.write_session_audit
    order: list[str] = []

    def _write(audit, path):
        order.append("write")
        return real_write(audit, path)

    def _offload(*a, **kw):
        order.append("offload")
        return {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0}

    monkeypatch.setattr(daemon_mod, "write_session_audit", _write)
    monkeypatch.setattr(daemon_mod, "run_eod_offload", _offload)

    # When
    daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time)

    # Then: 오프로드 전에 아티팩트가 날짜 우선 파일명으로 존재
    artifact = settings.paths.session_audit_path(day, settings.vendor)
    assert artifact.exists()
    assert order.index("write") < order.index("offload")
    assert artifact.name.split(".")[0] == day.isoformat()
    assert json.loads(pathlib.Path(artifact).read_text(encoding="utf-8"))["status"] == "OK"


def test_empty_journals_end_to_end_without_exception(tmp_path, monkeypatch, caplog) -> None:
    # Given: 빈 저널의 실제 파생 + 실제 정합성 검사
    import datetime as dt
    import logging

    import src.orchestration.daemon as daemon_mod

    settings, digests, eod_time = _stub_full_eod(monkeypatch, tmp_path)
    day = dt.date(2026, 9, 14)

    # When
    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(
            settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time
        )

    # Then: UNAVAILABLE 감사 + legacy 판정, 예외 없음
    assert len(digests) == 1
    lines = digests[0][1].splitlines()
    assert "audit_status=UNAVAILABLE" in lines
    assert "reconciled=False" in lines
    assert "regular_trade_loss=-" in lines
    assert "open_max_latency_s=-" in lines
    assert "status=UNAVAILABLE" in caplog.text
    assert "basis=legacy" in caplog.text


@pytest.mark.parametrize("payload", ["{}", "[]", '{"session_date": null}', "{broken"])
def test_unreadable_audit_manifest_does_not_abort_eod(tmp_path, monkeypatch, caplog, payload) -> None:
    import datetime as dt

    import src.orchestration.daemon as daemon_mod

    settings, digests, eod_time = _stub_full_eod(monkeypatch, tmp_path)
    day = dt.date(2026, 9, 14)
    manifest = settings.paths.manifest_path(day)
    manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest.write_text(payload, encoding="utf-8")
    audit = _stub_session_audit(day)
    derived: dict = {}

    def _derive(*a, **kw):
        derived.update(kw)
        return audit

    monkeypatch.setattr(daemon_mod, "derive_session_audit", _derive)
    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", lambda **kw: True)
    with caplog.at_level(logging.ERROR):
        daemon_mod.run_collector_daemon(
            settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time
        )
    assert derived["manifest_frame_counts"] is None
    assert settings.paths.session_audit_path(day, settings.vendor).exists()
    assert len(digests) == 1
    errors = [r for r in caplog.records if "stage=session_audit status=ERROR" in r.getMessage()]
    assert len(errors) == 1
    assert errors[0].exc_info is not None


@pytest.mark.parametrize("status", ["HOLIDAY", "UNKNOWN"])
def test_nonbusiness_audit_derivation_skips_all_io(tmp_path, monkeypatch, status) -> None:
    import datetime as dt

    import src.orchestration.daemon as daemon_mod
    from src.core.config import CollectorSettings, resolve_collector_runtime
    from src.core.session_anchors import standard_session_anchors
    from src.orchestration.trading_day_gate import TradingDayStatus, TradingDayView

    runtime = resolve_collector_runtime(collector=CollectorSettings(data_root=tmp_path))
    runner = daemon_mod.DaemonRunner(runtime=runtime, shutdown=None, now=lambda: None, sleep=lambda s: None)

    def _unexpected(*a, **kw):
        pytest.fail("nonbusiness audit must not perform I/O")

    monkeypatch.setattr(daemon_mod.SessionManifest, "load", _unexpected)
    monkeypatch.setattr(daemon_mod, "derive_session_audit", _unexpected)
    monkeypatch.setattr(daemon_mod, "write_session_audit", _unexpected)
    monkeypatch.setattr(daemon_mod, "log_session_audit", _unexpected)
    day = dt.date(2026, 9, 14)
    view = TradingDayView(date=day, status=TradingDayStatus[status], trading_day=None)
    assert runner._derive_session_audit(day, view, standard_session_anchors(day)) is None
