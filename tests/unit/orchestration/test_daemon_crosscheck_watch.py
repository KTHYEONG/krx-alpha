"""Vendor-aware crosscheck watchdogs, EOD reconciliation and digest invariant tests."""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import pathlib
from zoneinfo import ZoneInfo

import pytest

import src.orchestration.daemon as daemon_mod
from src.core.config import CollectorSettings, resolve_collector_runtime
from src.orchestration.trading_day_gate import TradingDayStatus, TradingDayView
from tests.unit.orchestration.daemon_fixtures import _business_trading_day

KST = ZoneInfo("Asia/Seoul")
DAY = dt.date(2026, 9, 14)
GOLDEN_PATH = pathlib.Path(__file__).parent / "golden" / "daemon_step_timeline.json"


@pytest.fixture(autouse=True)
def _verified_aftermarket_capacity(monkeypatch) -> None:
    monkeypatch.setenv("KRX_ALPHA_AFTERMARKET_PAIR_CAPACITY_PER_CONNECTION", "4")


def _runner(tmp_path, monkeypatch, **settings_kw):
    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data", **settings_kw)
    runtime = resolve_collector_runtime(collector=settings)
    return daemon_mod.DaemonRunner(runtime=runtime, shutdown=None, now=lambda: None, sleep=lambda s: None)


def _anchors(day):
    from src.core.session_anchors import standard_session_anchors

    return standard_session_anchors(day)


def _business_view(day: dt.date) -> TradingDayView:
    return TradingDayView(date=day, status=TradingDayStatus.BUSINESS, trading_day=_business_trading_day(day))


def _unknown_view(day: dt.date) -> TradingDayView:
    return TradingDayView(date=day, status=TradingDayStatus.UNKNOWN, trading_day=None)


def _write_journal(journal_root: pathlib.Path, vendor: str, day: dt.date, now: dt.datetime) -> pathlib.Path:
    part = journal_root / vendor / "krx" / "regular" / "H0STCNT0" / f"dt={day.isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    path = part / "09.jsonl.zst"
    path.write_bytes(b"xyz")
    ts = now.timestamp()
    os.utime(path, (ts, ts))
    return path


def _stub_eod(monkeypatch, runner) -> list[tuple[str, str]]:
    digests: list[tuple[str, str]] = []
    monkeypatch.setattr(
        daemon_mod,
        "_run_eod_housekeeping",
        lambda *a, **k: daemon_mod._EodHousekeeping(
            deleted=1, uploaded=2, purged=3, maintenance_ok=True, offload_ok=True
        ),
    )
    monkeypatch.setattr(daemon_mod, "_check_backup", lambda *a, **k: ([], True, "ok"))
    monkeypatch.setattr(daemon_mod, "send_digest", lambda subject, body: digests.append((subject, body)) or True)
    return digests


def test_crosscheck_stale_warns_never_critical(tmp_path, monkeypatch, caplog) -> None:
    runner = _runner(tmp_path, monkeypatch, db_crosscheck_enabled=True)

    class _Running:
        def ensure_running(self):
            return "running"

    runner.children.regular = _Running()
    runner.children.db_crosscheck = _Running()
    runner.state.orchestration_day = DAY
    runner.state.orchestrated_for = DAY
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda *a: _business_trading_day(DAY))
    now = dt.datetime(2026, 9, 14, 10, 0, tzinfo=KST)
    _write_journal(runner._paths.journal_root, "ls", DAY, now)
    with caplog.at_level(logging.INFO, logger=daemon_mod.logger.name):
        assert runner.step(now) == 10.0

    stale = [r for r in caplog.records if "stage=crosscheck_watchdog status=STALE" in r.getMessage()]
    assert len(stale) == 1
    assert stale[0].levelno == logging.WARNING
    assert "child=db_crosscheck vendor=db" in stale[0].getMessage()
    assert not any(r.levelno >= logging.CRITICAL for r in caplog.records)
    assert runner.state.ingest_stale is False


def test_crosscheck_stale_alert_once_per_day_then_recovers(tmp_path, monkeypatch, caplog) -> None:
    runner = _runner(tmp_path, monkeypatch, db_crosscheck_enabled=True)
    runner.children.db_crosscheck = object()
    anchors = _anchors(DAY)
    view = _business_view(DAY)
    from src.core.calendar import SessionState

    t0 = dt.datetime(2026, 9, 14, 10, 0, tzinfo=KST)
    with caplog.at_level(logging.INFO, logger=daemon_mod.logger.name):
        runner._run_crosscheck_watchdogs(t0, SessionState.FULL_ACTIVE, view, anchors)
        runner._run_crosscheck_watchdogs(t0 + dt.timedelta(seconds=60), SessionState.FULL_ACTIVE, view, anchors)
        _write_journal(runner._paths.journal_root, "db", DAY, t0 + dt.timedelta(seconds=120))
        runner._run_crosscheck_watchdogs(t0 + dt.timedelta(seconds=120), SessionState.FULL_ACTIVE, view, anchors)

    stale = [r for r in caplog.records if "status=STALE" in r.getMessage()]
    recovered = [r for r in caplog.records if "status=RECOVERED" in r.getMessage()]
    assert len(stale) == 1
    assert stale[0].levelno == logging.WARNING
    assert len(recovered) == 1
    assert recovered[0].levelno == logging.INFO


def test_crosscheck_watch_respects_cadence(tmp_path, monkeypatch) -> None:
    runner = _runner(tmp_path, monkeypatch, db_crosscheck_enabled=True)
    runner.children.db_crosscheck = object()
    anchors = _anchors(DAY)
    view = _business_view(DAY)
    from src.core.calendar import SessionState

    probes: list = []

    def _age(*args, **kwargs):
        probes.append(args)
        return None

    monkeypatch.setattr(daemon_mod, "_journal_age_s", _age)
    t0 = dt.datetime(2026, 9, 14, 10, 0, tzinfo=KST)
    runner._run_crosscheck_watchdogs(t0, SessionState.FULL_ACTIVE, view, anchors)
    runner._run_crosscheck_watchdogs(t0 + dt.timedelta(seconds=30), SessionState.FULL_ACTIVE, view, anchors)
    assert len(probes) == 1


def test_primary_stale_unaffected_by_crosscheck_journal(tmp_path, monkeypatch, caplog) -> None:
    runner = _runner(tmp_path, monkeypatch, db_crosscheck_enabled=True)
    runner.children.regular = object()
    runner.children.db_crosscheck = object()
    now = dt.datetime(2026, 9, 14, 10, 0, tzinfo=KST)
    _write_journal(runner._paths.journal_root, "db", DAY, now)
    anchors = _anchors(DAY)
    view = _business_view(DAY)
    from src.core.calendar import SessionState

    with caplog.at_level(logging.INFO, logger=daemon_mod.logger.name):
        runner._run_ingest_watchdog(now, SessionState.FULL_ACTIVE, view, anchors)
        runner._run_crosscheck_watchdogs(now, SessionState.FULL_ACTIVE, view, anchors)

    primary = [r for r in caplog.records if "stage=ingest_watchdog status=STALE" in r.getMessage()]
    assert len(primary) == 1
    assert primary[0].levelno == logging.CRITICAL
    assert not any("crosscheck_watchdog" in r.getMessage() for r in caplog.records)


def test_crosscheck_skipped_when_child_absent_or_unknown_day(tmp_path, monkeypatch) -> None:
    runner = _runner(tmp_path, monkeypatch, db_crosscheck_enabled=True)
    anchors = _anchors(DAY)
    from src.core.calendar import SessionState

    probes: list = []
    monkeypatch.setattr(daemon_mod, "_journal_age_s", lambda *a, **k: probes.append(a) or None)
    now = dt.datetime(2026, 9, 14, 10, 0, tzinfo=KST)

    runner._run_crosscheck_watchdogs(now, SessionState.FULL_ACTIVE, _business_view(DAY), anchors)
    assert probes == []

    runner.children.db_crosscheck = object()
    runner._run_crosscheck_watchdogs(now, SessionState.FULL_ACTIVE, _unknown_view(DAY), anchors)
    assert probes == []


def test_crosscheck_first_frame_warns_once(tmp_path, monkeypatch, caplog) -> None:
    runner = _runner(tmp_path, monkeypatch, db_crosscheck_enabled=True)
    runner.children.db_crosscheck = object()
    anchors = _anchors(DAY)
    view = _business_view(DAY)
    from src.core.calendar import SessionState

    t0 = dt.datetime(2026, 9, 14, 8, 36, tzinfo=KST)
    with caplog.at_level(logging.INFO, logger=daemon_mod.logger.name):
        runner._run_crosscheck_watchdogs(t0, SessionState.STREAMER_ACTIVE, view, anchors)
        runner._run_crosscheck_watchdogs(t0 + dt.timedelta(seconds=90), SessionState.STREAMER_ACTIVE, view, anchors)

    warnings = [r for r in caplog.records if "status=NO_FIRST_FRAME" in r.getMessage()]
    assert len(warnings) == 1
    assert warnings[0].levelno == logging.WARNING
    assert "child=db_crosscheck" in warnings[0].getMessage()
    assert runner._first_frame_alerted_for is None


def test_stream_spec_journal_fields_validated() -> None:
    import dataclasses

    primary = daemon_mod.DaemonRunner._stream_spec(daemon_mod.StreamChildKey.REGULAR)
    with pytest.raises(ValueError, match="must define journal_vendor and manifest_path"):
        dataclasses.replace(primary, journal_vendor=None)
    snapshot = daemon_mod.DaemonRunner._stream_spec(daemon_mod.StreamChildKey.SNAPSHOT)
    with pytest.raises(ValueError, match="must not define journal_vendor or manifest_path"):
        dataclasses.replace(snapshot, journal_vendor=lambda cfg: cfg.vendor)


def test_crosscheck_first_frame_cadence_and_recovery(tmp_path, monkeypatch, caplog) -> None:
    runner = _runner(tmp_path, monkeypatch, db_crosscheck_enabled=True)
    runner.children.db_crosscheck = object()
    anchors = _anchors(DAY)
    view = _business_view(DAY)
    from src.core.calendar import SessionState

    probes = []
    has_frames = daemon_mod._journal_has_frames

    def _probe(*args):
        probes.append(args)
        return has_frames(*args)

    monkeypatch.setattr(daemon_mod, "_journal_has_frames", _probe)
    t0 = dt.datetime(2026, 9, 14, 8, 36, tzinfo=KST)
    with caplog.at_level(logging.INFO, logger=daemon_mod.logger.name):
        runner._run_crosscheck_watchdogs(t0, SessionState.STREAMER_ACTIVE, view, anchors)
        runner._run_crosscheck_watchdogs(t0 + dt.timedelta(seconds=30), SessionState.STREAMER_ACTIVE, view, anchors)
        _write_journal(runner._paths.journal_root, "db", DAY, t0 + dt.timedelta(seconds=90))
        runner._run_crosscheck_watchdogs(t0 + dt.timedelta(seconds=90), SessionState.STREAMER_ACTIVE, view, anchors)

    assert len([r for r in caplog.records if "status=NO_FIRST_FRAME" in r.getMessage()]) == 1
    recovered = [r for r in caplog.records if "status=FIRST_FRAME_RECOVERED" in r.getMessage()]
    assert len(recovered) == 1
    assert recovered[0].levelno == logging.INFO
    assert len(probes) == 2
    assert runner._crosscheck_watch[daemon_mod.StreamChildKey.DB_CROSSCHECK].first_frame_alerted_for is None


def test_reconcile_crosscheck_sessions_oserror_logs_warning(tmp_path, monkeypatch, caplog) -> None:
    runner = _runner(tmp_path, monkeypatch, db_crosscheck_enabled=True)
    runner.state.orchestrated_for = DAY

    def _boom(**kwargs):
        raise OSError("db manifest unreadable")

    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", _boom)
    with caplog.at_level(logging.WARNING, logger=daemon_mod.logger.name):
        outcomes = runner._reconcile_crosscheck_sessions(DAY, _business_view(DAY), _anchors(DAY))

    assert outcomes == {daemon_mod.StreamChildKey.DB_CROSSCHECK: "error"}
    failure = [r for r in caplog.records if "stage=crosscheck_reconciliation status=FAIL" in r.getMessage()]
    assert len(failure) == 1
    assert failure[0].levelno == logging.WARNING
    assert failure[0].exc_info is not None


def test_eod_crosscheck_gap_keeps_status_ok(tmp_path, monkeypatch, caplog) -> None:
    runner = _runner(tmp_path, monkeypatch, db_crosscheck_enabled=True)
    digests = _stub_eod(monkeypatch, runner)
    monkeypatch.setattr(runner, "_reconcile_regular_session", lambda *a, **k: (True, True))
    runner.children.db_crosscheck = object()
    runner.state.orchestrated_for = DAY
    anchors = _anchors(DAY)
    view = _business_view(DAY)
    now = dt.datetime(2026, 9, 14, 20, 5, tzinfo=KST)

    with caplog.at_level(logging.WARNING, logger=daemon_mod.logger.name):
        runner._run_business_eod(now, DAY, view, anchors)

    subject, body = digests[0]
    assert subject == "[krx-alpha] EOD 2026-09-14 OK"
    assert "crosscheck_db_crosscheck=gap" in body.splitlines()
    failure = [r for r in caplog.records if "stage=session_reconciliation status=FAIL" in r.getMessage()]
    assert len(failure) == 1
    assert failure[0].levelno == logging.WARNING
    assert not any(r.levelno >= logging.CRITICAL for r in caplog.records)


def test_eod_crosscheck_skipped_when_not_orchestrated(tmp_path, monkeypatch) -> None:
    runner = _runner(tmp_path, monkeypatch, db_crosscheck_enabled=True)
    digests = _stub_eod(monkeypatch, runner)
    calls: list = []
    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", lambda **kw: calls.append(kw) or True)
    runner.children.db_crosscheck = object()
    runner.state.orchestrated_for = DAY - dt.timedelta(days=1)
    anchors = _anchors(DAY)
    view = _business_view(DAY)
    now = dt.datetime(2026, 9, 14, 20, 5, tzinfo=KST)

    runner._run_business_eod(now, DAY, view, anchors)

    assert len(calls) == 1
    assert "crosscheck_db_crosscheck=skipped" in digests[0][1].splitlines()


def test_digest_body_unchanged_without_crosscheck(tmp_path, monkeypatch) -> None:
    runner = _runner(tmp_path, monkeypatch)
    digests = _stub_eod(monkeypatch, runner)
    monkeypatch.setattr(runner, "_reconcile_regular_session", lambda *a, **k: (True, True))
    anchors = _anchors(DAY)
    view = _business_view(DAY)
    now = dt.datetime(2026, 9, 14, 20, 5, tzinfo=KST)

    runner._run_business_eod(now, DAY, view, anchors)

    expected = [
        "run_id=None",
        "status=OK",
        "deleted_partitions=1",
        "uploaded=2",
        "purged=3",
        "reconciled=True",
        "audit_status=UNAVAILABLE",
        "regular_trade_loss=-",
        "open_max_latency_s=-",
        "aftermarket_ready=True",
        "aftermarket_zero_frame_symbols=0",
        "aftermarket_prearm_late=0",
        "backup_missing=0",
        "host_backup=ok",
        "streamer_restarts=0",
        "aftermarket_restarts=0",
        "shard_stale_restarts=0",
        "orchestration_attempts=0",
        "degraded_reasons=none",
        f"digest_path={tmp_path / 'data' / 'logs' / 'eod_digests' / '2026-09-14.txt'}",
    ]
    assert digests[0][1].splitlines() == expected
    archived = (tmp_path / "data" / "logs" / "eod_digests" / "2026-09-14.txt").read_text(encoding="utf-8")
    assert archived == "[krx-alpha] EOD 2026-09-14 OK\n\n" + "\n".join(expected[:-1]) + "\n"


def test_reconciliation_failure_level_validated(tmp_path, monkeypatch) -> None:
    from src.orchestration.eod import check_session_reconciliation

    def _unexpected_io(*args, **kwargs):
        raise AssertionError("invalid failure level must be rejected before filesystem access")

    monkeypatch.setattr(pathlib.Path, "exists", _unexpected_io)
    monkeypatch.setattr(pathlib.Path, "glob", _unexpected_io)
    with pytest.raises(ValueError, match="failure_level must be WARNING, ERROR or CRITICAL"):
        check_session_reconciliation(
            manifest_path=tmp_path / "missing.json",
            date=DAY,
            journal_root=tmp_path / "journals",
            streams=("H0STCNT0",),
            vendor="ls",
            venue="krx",
            session="regular",
            regular_open=dt.time(9, 0),
            regular_close=dt.time(15, 30),
            failure_level=logging.INFO,
        )


@pytest.mark.parametrize("shift_hours", [0, 1])
@pytest.mark.parametrize(
    ("clock", "expected"),
    [
        (dt.time(8, 34, 59), None),
        (dt.time(8, 35), "NO_FIRST_FRAME"),
        (dt.time(9, 4, 59), "NO_FIRST_FRAME"),
        (dt.time(9, 5), "STALE"),
        (dt.time(15, 24, 59), "STALE"),
        (dt.time(15, 25), None),
    ],
)
def test_crosscheck_watch_window_boundaries(tmp_path, monkeypatch, caplog, shift_hours, clock, expected) -> None:
    from dataclasses import replace

    from src.core.calendar import SessionState

    runner = _runner(tmp_path, monkeypatch, db_crosscheck_enabled=True)
    runner.children.db_crosscheck = object()
    anchors = _anchors(DAY)
    shift = dt.timedelta(hours=shift_hours)

    def _shift(clock):
        return (dt.datetime.combine(DAY, clock) + shift).time()

    anchors = replace(
        anchors,
        regular_open=_shift(anchors.regular_open),
        closing_auction_start=_shift(anchors.closing_auction_start),
        regular_close=_shift(anchors.regular_close),
        after_market_end=_shift(anchors.after_market_end),
    )
    now = dt.datetime.combine(DAY, _shift(clock), tzinfo=KST).astimezone(dt.UTC)
    with caplog.at_level(logging.WARNING):
        runner._run_crosscheck_watchdogs(now, SessionState.FULL_ACTIVE, _business_view(DAY), anchors)
    records = [r for r in caplog.records if "stage=crosscheck_watchdog" in r.getMessage()]
    if expected is None:
        assert records == []
    else:
        assert len(records) == 1
        assert f"status={expected}" in records[0].getMessage()


def test_crosscheck_reconciliation_unexpected_error_propagates(tmp_path, monkeypatch) -> None:
    runner = _runner(tmp_path, monkeypatch, db_crosscheck_enabled=True)
    runner.state.orchestrated_for = DAY

    def _broken(**kwargs):
        raise RuntimeError("unexpected reconciliation defect")

    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", _broken)
    with pytest.raises(RuntimeError, match="unexpected reconciliation defect"):
        runner._reconcile_crosscheck_sessions(DAY, _business_view(DAY), _anchors(DAY))


def test_golden_unchanged() -> None:
    from tests.unit.orchestration.test_daemon_step_characterization import _stage_multiset

    golden = json.loads(GOLDEN_PATH.read_text(encoding="utf-8"))
    assert sorted(golden["log_stage_multiset"]) == _stage_multiset()
    assert golden["steps"]


def test_eod_digest_archive_failure_is_logged_and_skips_path(tmp_path, caplog) -> None:
    import logging

    from src.core.paths import DataPaths
    from src.orchestration import daemon as daemon_mod

    root = tmp_path / "data"
    root.mkdir()
    (root / "logs").write_text("not a dir", encoding="utf-8")
    with caplog.at_level(logging.WARNING):
        result = daemon_mod._archive_eod_digest(DataPaths(root), DAY, "s", "status=OK")

    assert result is None
    assert "stage=eod_digest_archive status=FAIL" in caplog.text
