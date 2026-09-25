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
    monkeypatch.setattr(daemon_mod, 'resolve_trading_day', lambda ref_date: None)
    monkeypatch.setattr(daemon_mod, 'run_session_orchestration', lambda **kw: True)

    stop_calls: list[float] = []

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            self.cmd = cmd
        def ensure_running(self):
            self.checked = True
            return 'started'
        def stop(self, *, timeout_s=15.0):
            stop_calls.append(timeout_s)
            return 'graceful'

    monkeypatch.setattr(daemon_mod, 'ProcessSupervisor', _FakeSupervisor)

    kst = ZoneInfo('Asia/Seoul')
    times = iter([
        dt.datetime(2026, 9, 8, 9, 0, 0, tzinfo=kst),
        dt.datetime(2026, 9, 8, 15, 45, 0, tzinfo=kst),
    ])

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
    monkeypatch.setattr(daemon_mod, 'resolve_trading_day', lambda ref_date: None)

    seen: dict[str, object] = {}

    def _fake_maintenance(journal_root, *, retain_days=3, today=None, archive_root=None, quarantine_root=None, work_root=None, verified_remote_l1=None):
        seen.update({'quarantine_root': quarantine_root, 'archive_root': archive_root, 'work_root': work_root})
        return 0

    monkeypatch.setattr(daemon_mod, 'run_eod_maintenance', _fake_maintenance)
    monkeypatch.setattr(daemon_mod, 'run_eod_offload', lambda *a, **kw: {'uploaded': 0, 'skipped': 0, 'failed': 0, 'purged': 0})

    settings = CollectorSettings()
    mock_sleep = MagicMock()
    eod_time = dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=ZoneInfo('Asia/Seoul'))

    # When
    run_collector_daemon(settings=settings, sleep_fn=mock_sleep, max_cycles=1, now_fn=lambda: eod_time)

    # Then: 설정에서 파생된 격리/작업 경로가 EOD 유지보수로 전달된다
    assert seen['quarantine_root'] == settings.paths.quarantine_root
    assert seen['archive_root'] == settings.paths.archive_root
    assert seen['work_root'] == settings.paths.work_root


def test_run_collector_daemon_eod_logs_session_data_gap(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")

    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: 0)
    monkeypatch.setattr(daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0})
    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", lambda **kw: False)
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c: _business_trading_day(dt.date(2026, 9, 10)))

    eod_time = dt.datetime(2026, 9, 10, 15, 45, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    with caplog.at_level(logging.CRITICAL):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time)

    assert "session_data_gap" in caplog.text


def test_run_collector_daemon_eod_attempts_maintenance_once_per_date(tmp_path, monkeypatch) -> None:
    # Given: 같은 날 EOD 윈도우에서 3사이클 반복
    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / 'data')
    counts = {'maintenance': 0, 'offload': 0, 'reconcile': 0}

    def _maintenance(*a, **kw):
        counts['maintenance'] += 1
        return 0

    def _offload(*a, **kw):
        counts['offload'] += 1
        return {'uploaded': 0, 'skipped': 0, 'failed': 0, 'purged': 0}

    def _reconcile(**kw):
        counts['reconcile'] += 1
        return True

    monkeypatch.setattr(daemon_mod, 'run_eod_maintenance', _maintenance)
    monkeypatch.setattr(daemon_mod, 'run_eod_offload', _offload)
    monkeypatch.setattr(daemon_mod, 'check_session_reconciliation', _reconcile)
    eod_time = dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=ZoneInfo('Asia/Seoul'))

    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c: _business_trading_day(dt.date(2026, 9, 14)))

    # When
    daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=3, now_fn=lambda: eod_time)

    # Then: 거래일당 1회만 시도 (정규화 2회: offload 전 None + offload 후 verified)
    assert counts == {'maintenance': 2, 'offload': 1, 'reconcile': 1}


def test_run_collector_daemon_eod_attempts_again_on_next_date(tmp_path, monkeypatch) -> None:
    # Given: 이틀 연속 EOD 사이클
    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / 'data')
    days = []

    def _maintenance(*a, **kw):
        days.append(kw['today'])
        return 0

    monkeypatch.setattr(daemon_mod, 'run_eod_maintenance', _maintenance)
    monkeypatch.setattr(daemon_mod, 'run_eod_offload', lambda *a, **kw: {'uploaded': 0, 'skipped': 0, 'failed': 0, 'purged': 0})
    monkeypatch.setattr(daemon_mod, 'check_session_reconciliation', lambda **kw: True)
    kst = ZoneInfo('Asia/Seoul')
    times = iter([
        dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=kst),
        dt.datetime(2026, 9, 14, 15, 46, 0, tzinfo=kst),
        dt.datetime(2026, 9, 15, 15, 45, 0, tzinfo=kst),
    ])

    # When
    daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=3, now_fn=lambda: next(times))

    # Then
    assert days == [dt.date(2026, 9, 14), dt.date(2026, 9, 14), dt.date(2026, 9, 15), dt.date(2026, 9, 15)]


def test_run_collector_daemon_eod_runs_offload_and_reconciliation_when_maintenance_fails(tmp_path, monkeypatch, caplog) -> None:
    # Given: 정규화 유지보수가 워커 크래시로 실패
    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod
    from src.storage.retention import L1WorkerCrashError

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / 'data')
    counts = {'offload': 0, 'reconcile': 0}

    def _maintenance(*a, **kw):
        raise L1WorkerCrashError('normalize worker crashed: returncode=-9')

    def _offload(*a, **kw):
        counts['offload'] += 1
        return {'uploaded': 2, 'skipped': 0, 'failed': 0, 'purged': 1}

    def _reconcile(**kw):
        counts['reconcile'] += 1
        return True

    monkeypatch.setattr(daemon_mod, 'run_eod_maintenance', _maintenance)
    monkeypatch.setattr(daemon_mod, 'run_eod_offload', _offload)
    monkeypatch.setattr(daemon_mod, 'check_session_reconciliation', _reconcile)
    eod_time = dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=ZoneInfo('Asia/Seoul'))

    # When
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c: _business_trading_day(dt.date(2026, 9, 14)))
    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time)

    # Then: 오프로드/정합성 검사는 계속되고 요약은 DEGRADED
    assert counts == {'offload': 1, 'reconcile': 1}
    assert 'stage=eod_maintenance status=FAIL reason=maintenance_error' in caplog.text
    assert 'deleted_partitions=0 uploaded=2 purged=1 status=DEGRADED' in caplog.text


def test_run_collector_daemon_eod_logs_error_when_offload_raises_and_does_not_retry_same_date(tmp_path, monkeypatch, caplog) -> None:
    # Given: 오프로드 단계에서 예기치 못한 예외
    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / 'data')
    counts = {'maintenance': 0, 'offload': 0, 'reconcile': 0}

    def _maintenance(*a, **kw):
        counts['maintenance'] += 1
        return 0

    def _offload(*a, **kw):
        counts['offload'] += 1
        raise RuntimeError('rclone lsjson failed')

    def _reconcile(**kw):
        counts['reconcile'] += 1
        return True

    monkeypatch.setattr(daemon_mod, 'run_eod_maintenance', _maintenance)
    monkeypatch.setattr(daemon_mod, 'run_eod_offload', _offload)
    monkeypatch.setattr(daemon_mod, 'check_session_reconciliation', _reconcile)
    eod_time = dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=ZoneInfo('Asia/Seoul'))

    # When
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c: _business_trading_day(dt.date(2026, 9, 14)))
    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=2, now_fn=lambda: eod_time)

    # Then: 오프로드 실패와 무관하게 정합성 검사는 수행되고 같은 날 재시도하지 않는다
    assert counts == {'maintenance': 1, 'offload': 1, 'reconcile': 1}
    assert 'stage=eod_maintenance error=rclone lsjson failed' in caplog.text
    assert 'deleted_partitions=0 uploaded=0 purged=0 status=DEGRADED' in caplog.text


def test_run_collector_daemon_eod_unexpected_error_logs_traceback(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(daemon_mod, "configure_logging", lambda component, *, log_dir=None: "r")
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: 0)

    def _offload(*a, **kw):
        raise RuntimeError("rclone lsjson failed")

    monkeypatch.setattr(daemon_mod, "run_eod_offload", _offload)
    eod_time = dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    with caplog.at_level(logging.ERROR):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time)

    records = [r for r in caplog.records if "stage=eod_maintenance error=" in r.getMessage()]
    assert len(records) == 1
    assert records[0].exc_info is not None
    assert records[0].exc_info[0] is RuntimeError


def test_run_collector_daemon_eod_reconciliation_failure_is_critical_and_degraded(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: 0)
    monkeypatch.setattr(daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 1, "skipped": 0, "failed": 0, "purged": 0})

    def _broken(**kw):
        raise OSError("bars parquet unreadable")

    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", _broken)
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c: _business_trading_day(dt.date(2026, 9, 14)))
    eod_time = dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time)

    records = [r for r in caplog.records if "stage=eod_reconciliation status=FAIL" in r.getMessage()]
    assert len(records) == 1
    assert records[0].levelno == logging.CRITICAL
    assert records[0].exc_info is not None
    assert "deleted_partitions=0 uploaded=1 purged=0 status=DEGRADED" in caplog.text


def test_run_collector_daemon_clears_supervisor_after_eod_so_next_day_never_restarts_stale_session(tmp_path, monkeypatch) -> None:
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
    monkeypatch.setattr(daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0})
    times = iter([
        dt.datetime(2026, 9, 14, 9, 0, tzinfo=kst),
        dt.datetime(2026, 9, 14, 15, 45, tzinfo=kst),
        dt.datetime(2026, 9, 15, 9, 0, tzinfo=kst),
    ])

    # When
    daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=3, now_fn=lambda: next(times))

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
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=2, now_fn=lambda: next(times))

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
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time)

    criticals = [r.getMessage() for r in caplog.records if r.levelno == logging.CRITICAL]
    assert any(m.startswith("[DAEMON] stage=eod_offload status=FAIL reason=auth_expired hint=rclone_config_reconnect_gdrive") for m in criticals)
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
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c: _business_trading_day(dt.date(2026, 9, 14)))
    monkeypatch.setattr(daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0})

    def _freshness(**kw):
        seen.update(kw)
        return ["2026-09-11.json", "2026-09-12.json"]

    monkeypatch.setattr(daemon_mod, "check_backup_freshness", _freshness)

    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time)

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

    monkeypatch.setattr(daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0})

    def _freshness(**kw):
        raise RemoteArchiveError("lsjson failed: manifest/ dial tcp: i/o timeout")

    monkeypatch.setattr(daemon_mod, "check_backup_freshness", _freshness)

    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time)

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
    monkeypatch.setattr(daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0})
    monkeypatch.setattr(daemon_mod, "check_backup_freshness", lambda **kw: [])

    def _reconcile(**kw):
        seen.update(kw)
        return True

    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", _reconcile)
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c: _business_trading_day(dt.date(2026, 9, 14)))

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

    monkeypatch.setattr(daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 2, "skipped": 0, "failed": 0, "purged": 1})
    monkeypatch.setattr(daemon_mod, "check_backup_freshness", lambda **kw: [])
    eod_time = dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    _write_eod_host_status(settings, eod_time, 20)
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c: _business_trading_day(dt.date(2026, 9, 14)))
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
    monkeypatch.setattr(daemon, 'aftermarket_eod_ready', lambda **_: False)
    monkeypatch.setattr(daemon, 'run_eod_maintenance', lambda *a, **k: calls.append('maintenance') or 0)
    monkeypatch.setattr(daemon, 'run_eod_offload', lambda *a, **k: calls.append('offload') or {'uploaded': 0, 'skipped': 0, 'failed': 0, 'purged': 0})
    monkeypatch.setattr(daemon, 'run_eod_remote_l0_purge', lambda *a, **k: calls.append('purge') or 0)
    monkeypatch.setattr(daemon, 'check_session_reconciliation', lambda **kw: True)
    monkeypatch.setattr(daemon, 'check_backup_freshness', lambda **kw: [])
    monkeypatch.setattr(daemon, '_resolve_trading_day_with_cache', lambda d, c: _business_trading_day(d))
    now = dt.datetime(2026,9,15,20,1,tzinfo=ZoneInfo('Asia/Seoul'))
    cfg = CollectorSettings(data_root=tmp_path, after_market_enabled=True, universe_slot_budget=1, ls_capacity_pairs=2)
    with caplog.at_level(logging.INFO):
        daemon.run_collector_daemon(settings=cfg, now_fn=lambda: now, sleep_fn=lambda _: None, max_cycles=1)
    assert 'maintenance' in calls
    assert 'offload' in calls
    assert 'purge' in calls
    assert 'reason=aftermarket_not_ready' in caplog.text
    assert 'status=DEGRADED' in caplog.text


def test_daemon_stops_snapshot_supervisor_at_eod(tmp_path, monkeypatch) -> None:
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
        dt.datetime(2026, 9, 8, 15, 45, 0, tzinfo=kst),
    ])

    run_collector_daemon(sleep_fn=MagicMock(), max_cycles=2, now_fn=lambda: next(times))

    snapshots = [sup for sup in created if 'collect-snapshots' in sup.cmd]
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

    def _purge(journal_root, verified):
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
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time)

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
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c: holiday)
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
    assert "stage=eod_maintenance status=HOLIDAY deleted_partitions=6 uploaded=2 purged=1 date=2026-09-24" in caplog.text


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
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c: holiday)
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
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c: holiday)
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
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c: _business_trading_day(d))
    monkeypatch.setattr(daemon_mod, "aftermarket_eod_ready", lambda **kw: False)
    calls: dict[str, int] = {"maintenance": 0, "offload": 0, "purge": 0}
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: calls.__setitem__("maintenance", calls["maintenance"] + 1) or 0)
    monkeypatch.setattr(daemon_mod, "run_eod_offload", lambda *a, **kw: calls.__setitem__("offload", calls["offload"] + 1) or {"uploaded": 1, "skipped": 0, "failed": 0, "purged": 0})
    monkeypatch.setattr(daemon_mod, "run_eod_remote_l0_purge", lambda *a, **kw: calls.__setitem__("purge", calls["purge"] + 1) or 0)
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
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c: _business_trading_day(d))
    monkeypatch.setattr(daemon_mod, "aftermarket_eod_ready", lambda **kw: True)
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: 0)
    monkeypatch.setattr(daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 1, "skipped": 0, "failed": 0, "purged": 0})
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
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c: _business_trading_day(d))
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: 0)
    monkeypatch.setattr(daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0})
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
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c: None)
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
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c: _holiday_trading_day(d))
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: (_ for _ in ()).throw(L1WorkerCrashError("crashed")))
    monkeypatch.setattr(daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0})
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
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c: _holiday_trading_day(d))
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: 0)
    monkeypatch.setattr(daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0})

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
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c: _holiday_trading_day(d))
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: 0)
    monkeypatch.setattr(daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0})
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
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c: _holiday_trading_day(d))
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: 0)
    monkeypatch.setattr(daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0})
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
    monkeypatch.setattr(daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 2, "skipped": 0, "failed": 0, "purged": 1})
    monkeypatch.setattr(daemon_mod, "check_backup_freshness", lambda **kw: [])
    eod_time = dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    _write_eod_host_status(settings, eod_time, 40)
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c: _business_trading_day(dt.date(2026, 9, 14)))

    with caplog.at_level(logging.CRITICAL):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time)

    stale = [r for r in caplog.records if "stage=host_backup_freshness" in r.getMessage()]
    assert len(stale) == 1
    assert "reason=stale:40.0h" in stale[0].getMessage()
    assert len(digests) == 1
    assert digests[0][0] == "[krx-alpha] EOD 2026-09-14 DEGRADED"
    assert "host_backup=stale:40.0h" in digests[0][1].splitlines()


def test_business_eod_ignores_data_root_work_status_and_reports_missing(
    tmp_path, monkeypatch, caplog
) -> None:
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
    old_status_path.write_text(
        json.dumps({"last_ok_at": eod_time.isoformat()}), encoding="utf-8"
    )
    assert not settings.host_backup_status_path.exists()

    digests: list[tuple[str, str]] = []
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: 0)
    monkeypatch.setattr(daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0})
    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", lambda **kw: True)
    monkeypatch.setattr(daemon_mod, "check_backup_freshness", lambda **kw: [])
    monkeypatch.setattr(daemon_mod, "send_digest", lambda subject, body: digests.append((subject, body)) or True)
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c: _business_trading_day(dt.date(2026, 9, 14)))

    with caplog.at_level(logging.CRITICAL):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time)

    missing = [
        record
        for record in caplog.records
        if "stage=host_backup_freshness" in record.getMessage()
    ]
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
    monkeypatch.setattr(daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 2, "skipped": 0, "failed": 0, "purged": 1})
    monkeypatch.setattr(daemon_mod, "check_backup_freshness", lambda **kw: [])
    eod_time = dt.datetime(2026, 9, 14, 15, 45, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    _write_eod_host_status(settings, eod_time, 20)
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c: _business_trading_day(dt.date(2026, 9, 14)))

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
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c: holiday)
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", lambda *a, **kw: 0)
    monkeypatch.setattr(daemon_mod, "run_eod_offload", lambda *a, **kw: {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0})
    monkeypatch.setattr(daemon_mod, "check_backup_freshness", lambda **kw: [])

    def _must_not_send(**kw):
        raise AssertionError("holiday EOD must not send a digest")

    monkeypatch.setattr(daemon_mod, "send_digest", _must_not_send)
    eod_time = dt.datetime(2026, 9, 24, 20, 5, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    with caplog.at_level(logging.CRITICAL):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: eod_time)

    missing = [
        record
        for record in caplog.records
        if "stage=host_backup_freshness" in record.getMessage()
    ]
    assert len(missing) == 1
    assert missing[0].levelno == logging.CRITICAL
    assert "reason=missing" in missing[0].getMessage()


