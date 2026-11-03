"""Operator page selection at daemon sites: backup staleness, repair hold expiry, shard budget, EOD digest."""

from __future__ import annotations

import datetime as dt
import json
import logging
from zoneinfo import ZoneInfo

from src.core.alerts import PageKind, page_kind
from src.core.config import CollectorSettings, RepairSettings
from src.core.paths import DataPaths
from src.orchestration import daemon as daemon_mod
from src.orchestration.shard_watchdog import ShardDecision, ShardVerdict, ShardWatchdog

KST = ZoneInfo("Asia/Seoul")
REF_DAY = dt.date(2026, 10, 28)


def _journal(paths: DataPaths, day: dt.date, vendor: str = "ls") -> None:
    part = paths.journal_root / vendor / "krx" / "regular" / "H0STCNT0" / f"dt={day.isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    (part / "09.jsonl.zst").write_bytes(b"data")


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
                "symbols": [{"symbol": "005930", "confirmed_missing": missing}],
            }
        )
    )


def _host_status(path, *, hours_ago: float) -> None:
    last_ok = dt.datetime(2026, 10, 28, 20, 0, tzinfo=KST) - dt.timedelta(hours=hours_ago)
    path.write_text(json.dumps({"last_ok_at": last_ok.isoformat()}), encoding="utf-8")


def _backup_records(tmp_path, monkeypatch, caplog, *, hours_ago: float, missing: list[str]) -> list[logging.LogRecord]:
    status = tmp_path / "host.json"
    _host_status(status, hours_ago=hours_ago)
    monkeypatch.setattr(daemon_mod, "check_backup_freshness", lambda **kw: list(missing))
    with caplog.at_level(logging.CRITICAL, logger=daemon_mod.logger.name):
        daemon_mod._check_backup(
            status,
            DataPaths(tmp_path / "data"),
            REF_DAY,
            now=dt.datetime(2026, 10, 28, 20, 0, tzinfo=KST),
            max_age=dt.timedelta(hours=36),
            page_after_days=2,
        )
    return [r for r in caplog.records if r.levelno == logging.CRITICAL]


def test_backup_stale_one_day_logs_without_page(tmp_path, monkeypatch, caplog) -> None:
    records = _backup_records(
        tmp_path, monkeypatch, caplog, hours_ago=40, missing=["krx/2026-10-27.json"]
    )
    assert len(records) == 2
    assert all(page_kind(r) is None for r in records)


def test_backup_stale_two_days_pages(tmp_path, monkeypatch, caplog) -> None:
    records = _backup_records(
        tmp_path, monkeypatch, caplog, hours_ago=50, missing=["krx/2026-10-27.json", "nxt/2026-10-26.json"]
    )
    assert [page_kind(r) for r in records] == [PageKind.BACKUP_STALE, PageKind.BACKUP_STALE]


def test_unparsable_manifest_names_never_page(tmp_path, monkeypatch, caplog) -> None:
    records = _backup_records(tmp_path, monkeypatch, caplog, hours_ago=1, missing=["krx/latest.json"])
    assert [page_kind(r) for r in records] == [None]
    assert daemon_mod._oldest_manifest_day(["a/2026-10-20.json", "b/2026-10-01.x.json", "c/bad.json"]) == dt.date(
        2026, 10, 1
    )


def _expiry_setup(tmp_path) -> tuple[DataPaths, CollectorSettings, RepairSettings]:
    paths = DataPaths(tmp_path)
    cfg = CollectorSettings(data_root=str(tmp_path), journal_retain_days=3)
    repair = RepairSettings(hold_enabled=True, hold_max_extra_days=2, gate_vendors=("ls",), repair_vendors=("ls",))
    # horizon = 3 + 2 = 5: the EOD at age 6 deletes the held day.
    for age, missing in ((5, 4), (4, 4), (3, 4), (6, 4)):
        day = REF_DAY - dt.timedelta(days=age)
        _journal(paths, day)
        _audit(paths, day, missing=missing)
    repaired = REF_DAY - dt.timedelta(days=4)
    _journal(paths, repaired, "kis")
    return paths, cfg, repair


def test_repair_expiring_returns_only_days_within_lead(tmp_path) -> None:
    paths, cfg, repair = _expiry_setup(tmp_path)
    expiring = daemon_mod._repair_expiring(paths, REF_DAY, cfg, repair)
    assert expiring == [
        ("2026-10-23:ls:tape_pending", dt.date(2026, 10, 29)),
        ("2026-10-24:ls:tape_pending", dt.date(2026, 10, 30)),
    ]


def test_repair_expiring_skips_clean_days_and_disabled_hold(tmp_path, monkeypatch) -> None:
    paths, cfg, repair = _expiry_setup(tmp_path)
    _audit(paths, REF_DAY - dt.timedelta(days=5), missing=0)
    assert daemon_mod._repair_expiring(paths, REF_DAY, cfg, repair) == [
        ("2026-10-24:ls:tape_pending", dt.date(2026, 10, 30))
    ]
    assert daemon_mod._repair_expiring(paths, REF_DAY, cfg, None) == []
    assert daemon_mod._repair_expiring(paths, REF_DAY, cfg, repair.model_copy(update={"hold_enabled": False})) == []

    def _torn(path):
        raise ValueError("torn ledger")

    monkeypatch.setattr(daemon_mod, "read_ledger_done", _torn)
    assert len(daemon_mod._repair_expiring(paths, REF_DAY, cfg, repair)) == 1


def _expiry_runner(paths: DataPaths, cfg: CollectorSettings) -> daemon_mod.DaemonRunner:
    runner = object.__new__(daemon_mod.DaemonRunner)
    runner._paths = paths
    runner._cfg = cfg
    runner._state = daemon_mod.DaemonState()
    return runner


def test_repair_expiry_pages_once_with_earliest_deadline(tmp_path, caplog) -> None:
    paths, cfg, repair = _expiry_setup(tmp_path)
    runner = _expiry_runner(paths, cfg)
    with caplog.at_level(logging.CRITICAL, logger=daemon_mod.logger.name):
        runner._page_repair_expiry(REF_DAY, repair)
        runner._page_repair_expiry(REF_DAY, repair)
    pages = [r for r in caplog.records if "stage=repair_hold status=EXPIRING" in r.getMessage()]
    assert len(pages) == 1
    assert page_kind(pages[0]) is PageKind.REPAIR_HOLD_EXPIRING
    assert pages[0].krx_page_deadline == dt.date(2026, 10, 29)
    assert "delete_on=2026-10-29" in pages[0].getMessage()


def test_repair_expiry_check_error_is_contained(tmp_path, monkeypatch, caplog) -> None:
    paths, cfg, repair = _expiry_setup(tmp_path)
    runner = _expiry_runner(paths, cfg)

    def _boom(*a, **k):
        raise OSError("io")

    monkeypatch.setattr(daemon_mod, "_repair_expiring", _boom)
    with caplog.at_level(logging.ERROR, logger=daemon_mod.logger.name):
        runner._page_repair_expiry(REF_DAY, repair)
    assert "reason=expiry_check_error error=OSError" in caplog.text
    assert runner._state.repair_expiry_paged == set()


def test_shard_budget_pages_only_when_requested(caplog) -> None:
    runner = object.__new__(daemon_mod.DaemonRunner)
    runner._state = daemon_mod.DaemonState()
    decision = ShardDecision(
        key="krx-00", verdict=ShardVerdict.BUDGET_EXHAUSTED, silent_s=300.0, limit_s=120.0, restart=False
    )
    watchdog = ShardWatchdog(limit_s=120.0, max_restarts_per_day=3)
    with caplog.at_level(logging.CRITICAL, logger=daemon_mod.logger.name):
        runner._apply_shard_decisions(
            (decision,), watchdog=watchdog, venue="nxt", today=REF_DAY, supervisors={}, max_restarts=3
        )
        runner._apply_shard_decisions(
            (decision,),
            watchdog=watchdog,
            venue="krx",
            today=REF_DAY + dt.timedelta(days=1),
            supervisors={},
            max_restarts=3,
            budget_page=PageKind.REGULAR_SHARD_STALE,
        )
    budget = [r for r in caplog.records if "reason=stale_budget_exhausted" in r.getMessage()]
    assert [page_kind(r) for r in budget] == [None, PageKind.REGULAR_SHARD_STALE]


def test_only_primary_streamer_circuit_pages() -> None:
    pages = {spec.key: spec.logs.circuit_page for spec in daemon_mod.DaemonRunner._STREAM_SPECS if spec.logs}
    assert pages.pop(daemon_mod.StreamChildKey.REGULAR) is PageKind.REGULAR_FEED_DOWN
    assert set(pages.values()) == {None}


def test_eod_digest_is_logged_not_mailed(caplog) -> None:
    with caplog.at_level(logging.INFO, logger=daemon_mod.logger.name):
        daemon_mod.record_eod_digest(
            "[krx-alpha] EOD 2026-10-28 DEGRADED",
            "run_id=d\nstatus=DEGRADED\ndegraded_reasons=backup\ndigest_path=/x/2026-10-28.txt",
        )
    [record] = [r for r in caplog.records if "stage=eod_digest" in r.getMessage()]
    assert record.levelno == logging.INFO
    assert record.krx_event is True
    assert page_kind(record) is None
    assert "status=DEGRADED" in record.getMessage()
    assert "degraded_reasons=backup" in record.getMessage()
    assert not hasattr(daemon_mod, "send_digest")
