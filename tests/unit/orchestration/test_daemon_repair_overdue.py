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
        r.levelno == logging.CRITICAL and "stage=repair_overdue" in r.getMessage() for r in caplog.records
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
