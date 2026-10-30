from __future__ import annotations

import datetime as dt
import json
import logging

from src.core.config import CollectorSettings, RepairSettings
from src.core.paths import DataPaths
from src.orchestration import daemon as daemon_mod


def _settings(tmp_path, **overrides) -> CollectorSettings:
    return CollectorSettings(data_root=str(tmp_path), **overrides)


def _audit(paths: DataPaths, day: dt.date, vendor: str = "ls", *, missing: int) -> None:
    path = paths.session_audit_path(day, vendor)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "session_date": day.isoformat(),
                "vendor": vendor,
                "venue": "krx",
                "session": "regular",
                "status": "OK",
                "totals": {"confirmed_missing": missing},
                "symbols": [
                    {"symbol": "005930", "confirmed_missing": missing},
                ],
            }
        )
    )


def _journal(paths: DataPaths, day: dt.date, vendor: str = "ls") -> None:
    part = paths.journal_root / vendor / "krx" / "regular" / "H0STCNT0" / f"dt={day.isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    (part / "09.jsonl.zst").write_bytes(b"data")


def test_overdue_repair_pages_and_repaired_is_ignored(tmp_path, caplog) -> None:
    paths = DataPaths(tmp_path)
    cfg = _settings(tmp_path, journal_retain_days=3)
    repair = RepairSettings(overdue_days=2)
    ref_day = dt.date(2026, 10, 28)
    old = ref_day - dt.timedelta(days=2)
    _journal(paths, old, "ls")
    _audit(paths, old, "ls", missing=5)
    repaired_day = ref_day - dt.timedelta(days=3)
    _journal(paths, repaired_day, "ls")
    _audit(paths, repaired_day, "ls", missing=0)
    overdue = daemon_mod._repair_overdue(paths, ref_day, cfg, repair)
    assert overdue == [f"{old.isoformat()}:ls:tape_pending"]
    with caplog.at_level(logging.INFO):
        if overdue:
            daemon_mod.logger.critical("[DATA] stage=repair_overdue status=FAIL days=%s", ",".join(overdue))
    assert "repair_overdue" in caplog.text
    assert repaired_day.isoformat() not in ",".join(overdue)


def test_young_pending_not_overdue_and_none_returns_empty(tmp_path) -> None:
    paths = DataPaths(tmp_path)
    cfg = _settings(tmp_path, journal_retain_days=3)
    repair = RepairSettings(overdue_days=2)
    ref_day = dt.date(2026, 10, 28)
    young = ref_day - dt.timedelta(days=1)
    _journal(paths, young, "ls")
    _audit(paths, young, "ls", missing=5)
    assert daemon_mod._repair_overdue(paths, ref_day, cfg, repair) == []
    assert daemon_mod._repair_overdue(paths, ref_day, cfg, None) == []


def test_business_eod_emits_overdue_critical(tmp_path, monkeypatch, caplog) -> None:
    from types import SimpleNamespace

    from src.core.session_anchors import standard_session_anchors

    day = dt.date(2026, 10, 28)
    anchors = standard_session_anchors(day)
    paths = DataPaths(tmp_path / "data")
    old = day - dt.timedelta(days=2)
    _journal(paths, old, "ls")
    _audit(paths, old, "ls", missing=5)
    cfg = SimpleNamespace(
        journal_retain_days=3,
        host_backup_status_path=tmp_path / "host.json",
        host_backup_max_age_h=36,
    )
    repair = RepairSettings(overdue_days=2)
    runner = object.__new__(daemon_mod.DaemonRunner)
    runner._cfg = cfg
    runner._paths = paths
    runner._runtime = SimpleNamespace(repair=repair, audit=None)
    runner._state = daemon_mod.DaemonState()
    runner._run_id = "test"
    runner._eod_progress = lambda: None
    monkeypatch.setattr(daemon_mod.DaemonRunner, "_aftermarket_eod_blocked", lambda self, *a, **k: False)
    monkeypatch.setattr(daemon_mod.DaemonRunner, "_log_aftermarket_prearm", lambda self, *a, **k: 0)
    monkeypatch.setattr(daemon_mod.DaemonRunner, "_log_premarket_eod", lambda self, *a, **k: None)
    monkeypatch.setattr(daemon_mod.DaemonRunner, "_log_aftermarket_coverage", lambda self, *a, **k: "-")
    monkeypatch.setattr(daemon_mod.DaemonRunner, "_derive_session_audit", lambda self, *a, **k: None)
    monkeypatch.setattr(daemon_mod.DaemonRunner, "_derive_kis_regular_audit", lambda self, *a, **k: None)
    monkeypatch.setattr(
        daemon_mod,
        "_run_eod_housekeeping",
        lambda *a, **k: SimpleNamespace(deleted=0, uploaded=0, purged=0, maintenance_ok=True, offload_ok=True, held=0),
    )
    monkeypatch.setattr(daemon_mod.DaemonRunner, "_reconcile_regular_session", lambda self, *a, **k: (True, True))
    monkeypatch.setattr(daemon_mod.DaemonRunner, "_reconcile_crosscheck_sessions", lambda self, *a, **k: {})
    monkeypatch.setattr(daemon_mod, "_check_backup", lambda *a, **k: ([], True, "ok"))
    monkeypatch.setattr(daemon_mod, "send_digest", lambda subject, body: True)
    from tests.unit.orchestration.daemon_fixtures import _business_trading_day

    with caplog.at_level(logging.INFO):
        runner._run_business_eod(
            dt.datetime(2026, 10, 28, 20, 5, tzinfo=dt.timezone(dt.timedelta(hours=9))),
            day,
            _business_trading_day(day),
            anchors,
        )
    assert any(
        r.levelno == logging.CRITICAL
        and "stage=repair_overdue" in r.getMessage()
        and "reason=stalled" in r.getMessage()
        for r in caplog.records
    )


def test_torn_ledger_degrades_to_pending(tmp_path, monkeypatch) -> None:
    paths = DataPaths(tmp_path)
    cfg = _settings(tmp_path, journal_retain_days=3)
    repair = RepairSettings(overdue_days=2)
    ref_day = dt.date(2026, 10, 28)
    old = ref_day - dt.timedelta(days=2)
    _journal(paths, old, "ls")
    _audit(paths, old, "ls", missing=5)
    paths.tick_tape_ledger.parent.mkdir(parents=True, exist_ok=True)
    paths.tick_tape_ledger.write_bytes(b"bad\n")

    def boom(*args, **kwargs):
        raise OSError("torn")

    monkeypatch.setattr(daemon_mod, "read_ledger_done", boom)
    overdue = daemon_mod._repair_overdue(paths, ref_day, cfg, repair)
    assert overdue == [f"{old.isoformat()}:ls:tape_pending"]


def test_exhausted_none_settings_returns_empty(tmp_path) -> None:
    paths = DataPaths(tmp_path)
    cfg = _settings(tmp_path, journal_retain_days=3)
    assert daemon_mod._repair_exhausted(paths, dt.date(2026, 10, 28), cfg, None) == []


def test_judge_error_never_raises_nor_pages(tmp_path, monkeypatch, caplog) -> None:
    import datetime as _dt

    runner, _, _, _ = _judge_runner(tmp_path, repair_job=True, tape=True)
    identity = _dt.date(2026, 10, 24)

    def boom(*args, **kwargs):
        raise RuntimeError("ledger gone")

    monkeypatch.setattr(daemon_mod, "_repair_exhausted", boom)
    with caplog.at_level(logging.INFO):
        runner._judge_repair_exhausted(identity)
    assert runner._state.overdue_judged_for == identity
    assert any("reason=judge_error" in r.getMessage() for r in caplog.records)
    assert not any(
        r.levelno == logging.CRITICAL and "attempts_exhausted" in r.getMessage() for r in caplog.records
    )


def _ledger(paths: DataPaths, records: list[dict]) -> None:
    paths.tick_tape_ledger.parent.mkdir(parents=True, exist_ok=True)
    with paths.tick_tape_ledger.open("a", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record) + "\n")


def _failed(symbol: str, day: dt.date, run_id: str) -> dict:
    return {"symbol": symbol, "date": day.isoformat(), "status": "failed", "run_id": run_id}


def test_single_failed_attempt_stays_on_backstop(tmp_path) -> None:
    paths = DataPaths(tmp_path)
    cfg = _settings(tmp_path, journal_retain_days=3)
    repair = RepairSettings(overdue_days=2)
    ref_day = dt.date(2026, 10, 28)
    day = ref_day - dt.timedelta(days=2)
    _journal(paths, day, "ls")
    _audit(paths, day, "ls", missing=5)
    _ledger(paths, [_failed("005930", day, "aaa")])
    assert daemon_mod._repair_exhausted(paths, ref_day, cfg, repair) == []
    assert daemon_mod._repair_overdue(paths, ref_day, cfg, repair) == [f"{day.isoformat()}:ls:tape_pending"]


def test_two_failed_attempts_exhaust_at_night(tmp_path) -> None:
    paths = DataPaths(tmp_path)
    cfg = _settings(tmp_path, journal_retain_days=3)
    repair = RepairSettings(overdue_days=2)
    ref_day = dt.date(2026, 10, 28)
    day = ref_day - dt.timedelta(days=2)
    _journal(paths, day, "ls")
    _audit(paths, day, "ls", missing=5)
    _ledger(paths, [_failed("005930", day, "aaa"), _failed("005930", day, "bbb")])
    assert daemon_mod._repair_exhausted(paths, ref_day, cfg, repair) == [
        f"{day.isoformat()}:ls:tape_pending:attempts=2"
    ]
    assert daemon_mod._repair_overdue(paths, ref_day, cfg, repair) == []


def test_checkpoint_labels_are_single_attempt(tmp_path) -> None:
    paths = DataPaths(tmp_path)
    cfg = _settings(tmp_path, journal_retain_days=3)
    repair = RepairSettings(overdue_days=2)
    ref_day = dt.date(2026, 10, 28)
    day = ref_day - dt.timedelta(days=2)
    _journal(paths, day, "ls")
    _audit(paths, day, "ls", missing=5)
    _ledger(paths, [_failed("005930", day, "aaa"), _failed("005930", day, "aaa-1")])
    assert daemon_mod._repair_exhausted(paths, ref_day, cfg, repair) == []
    assert daemon_mod._repair_overdue(paths, ref_day, cfg, repair) == [f"{day.isoformat()}:ls:tape_pending"]


def test_repaired_day_ignored_by_both_rules(tmp_path) -> None:
    paths = DataPaths(tmp_path)
    cfg = _settings(tmp_path, journal_retain_days=3)
    repair = RepairSettings(overdue_days=2)
    ref_day = dt.date(2026, 10, 28)
    day = ref_day - dt.timedelta(days=2)
    _journal(paths, day, "ls")
    _audit(paths, day, "ls", missing=5)
    _ledger(
        paths,
        [
            {"symbol": "005930", "date": day.isoformat(), "status": "complete", "run_id": "aaa"},
            _failed("005930", day, "old-1"),
            _failed("005930", day, "old-2"),
        ],
    )
    _, report_path = paths.gapfill_paths(day, "ls")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps({"totals": {"residual": 0}, "intervals": []}))
    assert daemon_mod._repair_exhausted(paths, ref_day, cfg, repair) == []
    assert daemon_mod._repair_overdue(paths, ref_day, cfg, repair) == []


def test_gapfill_pending_stays_on_backstop(tmp_path) -> None:
    paths = DataPaths(tmp_path)
    cfg = _settings(tmp_path, journal_retain_days=3)
    repair = RepairSettings(overdue_days=2)
    ref_day = dt.date(2026, 10, 28)
    day = ref_day - dt.timedelta(days=2)
    _journal(paths, day, "ls")
    _audit(paths, day, "ls", missing=5)
    _ledger(paths, [{"symbol": "005930", "date": day.isoformat(), "status": "complete", "run_id": "aaa"}])
    assert daemon_mod._repair_exhausted(paths, ref_day, cfg, repair) == []
    assert daemon_mod._repair_overdue(paths, ref_day, cfg, repair) == [f"{day.isoformat()}:ls:gapfill_pending"]


def test_unreadable_attempt_ledger_fails_closed(tmp_path, caplog) -> None:
    paths = DataPaths(tmp_path)
    cfg = _settings(tmp_path, journal_retain_days=3)
    repair = RepairSettings(overdue_days=2)
    ref_day = dt.date(2026, 10, 28)
    day = ref_day - dt.timedelta(days=2)
    _journal(paths, day, "ls")
    _audit(paths, day, "ls", missing=5)
    paths.tick_tape_ledger.parent.mkdir(parents=True, exist_ok=True)
    paths.tick_tape_ledger.write_text('{"broken"\n' + json.dumps(_failed("005930", day, "aaa")) + "\n")
    with caplog.at_level(logging.WARNING):
        assert daemon_mod._repair_exhausted(paths, ref_day, cfg, repair) == []
    assert any("reason=attempts_unreadable" in r.getMessage() for r in caplog.records)
    assert daemon_mod._repair_overdue(paths, ref_day, cfg, repair) == [f"{day.isoformat()}:ls:tape_pending"]


def _judge_runner(tmp_path, *, repair_job: bool, tape: bool):
    from types import SimpleNamespace

    from src.core.config import TickTapeBackfillSettings

    paths = DataPaths(tmp_path)
    cfg = _settings(tmp_path, journal_retain_days=3)
    repair = RepairSettings(overdue_days=2, job_enabled=repair_job)
    runner = object.__new__(daemon_mod.DaemonRunner)
    runner._cfg = cfg
    runner._paths = paths
    runner._runtime = SimpleNamespace(repair=repair)
    runner._repair_cfg = repair
    runner._tick_tape_cfg = TickTapeBackfillSettings(enabled=tape)
    runner._minute_cfg = SimpleNamespace(enabled=False)
    runner._state = daemon_mod.DaemonState()
    runner._children = daemon_mod.DaemonChildren()
    return runner, paths, cfg, repair


class _DoneProc:
    def __init__(self, code: int) -> None:
        self._code = code

    def poll(self):
        return self._code


class _HungProc:
    def poll(self):
        return None

    def terminate(self) -> None:
        return None

    def wait(self, timeout=None):
        return 0

    def kill(self) -> None:
        return None


def _exhausted_state(paths: DataPaths, day: dt.date) -> None:
    _journal(paths, day, "ls")
    _audit(paths, day, "ls", missing=5)
    _ledger(paths, [_failed("005930", day, "aaa"), _failed("005930", day, "bbb")])


def test_deadline_kill_judges_exhaustion(tmp_path, caplog) -> None:
    from zoneinfo import ZoneInfo

    kst = ZoneInfo("Asia/Seoul")
    runner, paths, _, _ = _judge_runner(tmp_path, repair_job=True, tape=True)
    identity = dt.date(2026, 10, 24)
    _exhausted_state(paths, identity)
    runner._children.nightly = _HungProc()  # type: ignore[assignment]
    runner._children.nightly_job = daemon_mod.NightlyJob.REPAIR
    runner._children.nightly_started_at = dt.datetime(2026, 10, 24, 23, 45, tzinfo=kst)
    now = dt.datetime(2026, 10, 25, 5, 33, tzinfo=kst)
    with caplog.at_level(logging.INFO):
        runner._poll_nightly(now)
        runner._poll_nightly(now)
    assert runner._state.overdue_judged_for == identity
    assert sum(
        r.levelno == logging.CRITICAL and "reason=attempts_exhausted" in r.getMessage() for r in caplog.records
    ) == 1


def test_nightly_judgment_fires_once_per_window(tmp_path, caplog) -> None:
    import datetime as _dt

    from zoneinfo import ZoneInfo

    kst = ZoneInfo("Asia/Seoul")
    runner, paths, _, _ = _judge_runner(tmp_path, repair_job=True, tape=True)
    identity = _dt.date(2026, 10, 24)
    _exhausted_state(paths, identity)
    started = _dt.datetime(2026, 10, 24, 23, 45, tzinfo=kst)
    runner._children.nightly = _DoneProc(0)  # type: ignore[assignment]
    runner._children.nightly_job = daemon_mod.NightlyJob.REPAIR
    runner._children.nightly_started_at = started
    with caplog.at_level(logging.INFO):
        runner._poll_nightly(_dt.datetime(2026, 10, 25, 1, 0, tzinfo=kst))
    assert runner._state.overdue_judged_for == identity
    runner._children.nightly = _HungProc()  # type: ignore[assignment]
    runner._children.nightly_job = daemon_mod.NightlyJob.REPAIR
    runner._children.nightly_started_at = started
    with caplog.at_level(logging.INFO):
        runner._poll_nightly(_dt.datetime(2026, 10, 25, 5, 33, tzinfo=kst))
    criticals = [
        r
        for r in caplog.records
        if r.levelno == logging.CRITICAL and "reason=attempts_exhausted" in r.getMessage()
    ]
    assert len(criticals) == 1


def test_tape_only_judges_after_tape_job(tmp_path, caplog) -> None:
    import datetime as _dt

    from zoneinfo import ZoneInfo

    kst = ZoneInfo("Asia/Seoul")
    runner, paths, _, _ = _judge_runner(tmp_path, repair_job=False, tape=True)
    identity = _dt.date(2026, 10, 24)
    _exhausted_state(paths, identity)
    started = _dt.datetime(2026, 10, 24, 23, 45, tzinfo=kst)
    runner._children.nightly = _DoneProc(0)  # type: ignore[assignment]
    runner._children.nightly_job = daemon_mod.NightlyJob.TICK_TAPE
    runner._children.nightly_started_at = started
    with caplog.at_level(logging.INFO):
        runner._poll_nightly(_dt.datetime(2026, 10, 25, 1, 0, tzinfo=kst))
    assert runner._state.overdue_judged_for == identity
    assert any("reason=attempts_exhausted" in r.getMessage() for r in caplog.records)
    caplog.clear()
    runner2, _, _, _ = _judge_runner(tmp_path, repair_job=False, tape=True)
    runner2._children.nightly = _DoneProc(0)  # type: ignore[assignment]
    runner2._children.nightly_job = daemon_mod.NightlyJob.MINUTE_BARS
    runner2._children.nightly_started_at = started
    with caplog.at_level(logging.INFO):
        runner2._poll_nightly(_dt.datetime(2026, 10, 25, 1, 0, tzinfo=kst))
    assert runner2._state.overdue_judged_for is None
    assert not any("attempts_exhausted" in r.getMessage() for r in caplog.records)
