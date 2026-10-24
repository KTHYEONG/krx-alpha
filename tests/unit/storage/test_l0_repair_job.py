from __future__ import annotations

import datetime as dt
import fcntl
import hashlib
import json
import pathlib
import threading

import polars as pl
import pytest
import zstandard as zstd

from src.core.config import AuditSettings, RepairSettings
from src.core.paths import DataPaths
from src.marketdata.tick_tape_backfill import read_ledger_done
from src.storage import l0_repair_job as job
from src.storage.l0_repair import RepairState, derive_day_repair

DAY = dt.date(2026, 10, 23)
KST = dt.timezone(dt.timedelta(hours=9))
NOW = dt.datetime(2026, 10, 24, 0, 10, tzinfo=KST)
DEADLINE = NOW.replace(hour=5, minute=30)


def journal(paths: DataPaths, day: dt.date = DAY, vendor: str = "ls") -> pathlib.Path:
    part = paths.journal_root / vendor / "krx/regular/H0STCNT0" / f"dt={day}"
    part.mkdir(parents=True, exist_ok=True)
    records = []
    for seq, (hms, cum, volume) in enumerate((("090000", 1, 1), ("090002", 3, 3))):
        body = {"shcode": "000001", "chetime": hms, "mdchecnt": "0", "mschecnt": str(cum),
                "price": "100", "cvolume": "1", "volume": str(volume)}
        raw = json.dumps({"body": body})
        if vendor == "kis":
            from src.storage import quality_kis as q

            fields = ["0"] * q.KIS_TICK_MIN_FIELDS
            for index, key in ((q.KIS_TICK_SYMBOL, "shcode"), (q.KIS_TICK_TIME, "chetime"),
                               (q.KIS_TICK_PRICE, "price"), (q.KIS_TICK_VOLUME, "volume"),
                               (q.KIS_TICK_CVOLUME, "cvolume"), (q.KIS_TICK_MDCHECNT, "mdchecnt"),
                               (q.KIS_TICK_MSCHECNT, "mschecnt")):
                fields[index] = body[key]
            raw = "^".join(fields)
        records.append({"raw": raw, "conn_id": "c", "conn_seq": seq,
                        "recv_wall_ns": int(dt.datetime.combine(day, dt.time(9, 0, seq * 2), KST).timestamp() * 1e9)})
    out = part / "09.jsonl.zst"
    out.write_bytes(zstd.ZstdCompressor().compress("".join(json.dumps(r) + "\n" for r in records).encode()))
    return out


def audit(paths: DataPaths, day: dt.date = DAY, vendor: str = "ls", *, missing: int = 1, status: str = "OK") -> pathlib.Path:
    path = paths.session_audit_path(day, vendor)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "session_date": day.isoformat(), "vendor": vendor, "venue": "krx", "session": "regular", "status": status,
        "totals": {"confirmed_missing": missing, "trades_observed": 2, "trades_received": 2},
        "symbols": [{"symbol": "000001", "confirmed_missing": missing, "intervals": [{
            "missing_trades": missing, "chetime_prev": "090000", "chetime_next": "090002",
            "cum_prev": 1, "cum_next": missing + 2,
        }]}],
    }))
    return path


def tape(paths: DataPaths, day: dt.date = DAY, *, corrupt: bool = False) -> None:
    path = paths.tick_tape_partition(day, "test")
    path.parent.mkdir(parents=True, exist_ok=True)
    if corrupt:
        path.write_bytes(b"not parquet")
    else:
        pl.DataFrame({"symbol": ["000001"] * 3, "ts_hms": [90000, 90001, 90002],
                      "price": [100] * 3, "volume": [1] * 3}).write_parquet(path)
    paths.tick_tape_ledger.parent.mkdir(parents=True, exist_ok=True)
    with paths.tick_tape_ledger.open("a") as handle:
        handle.write(json.dumps({"symbol": "000001", "date": day.isoformat(), "status": "complete"}) + "\n")


def run(paths: DataPaths, **kwargs) -> job.RepairRunSummary:
    args = {"paths": paths, "journal_root": paths.journal_root,
            "audit_settings": AuditSettings(latency_streams=("H0STCNT0",)), "repair_settings": RepairSettings(),
            "retain_days": 3, "window_date": DAY, "deadline": DEADLINE, "now": lambda: NOW, "run_id": "test"}
    return job.run_repair(**(args | kwargs))


@pytest.mark.parametrize("has_tape", [False, True])
def test_unaudited_day_rederived_and_advanced_in_same_run(tmp_path, has_tape) -> None:
    paths = DataPaths(tmp_path)
    source = journal(paths)
    audit(paths, status="UNAVAILABLE")
    if has_tape:
        tape(paths)
    before = source.read_bytes()
    summary = run(paths)
    assert summary.status == "OK"
    assert summary.failed == 0
    assert summary.merged == int(has_tape)
    assert summary.audited == 1
    assert summary.gapfilled == int(has_tape)
    assert summary.waiting_tape == int(not has_tape)
    assert json.loads(paths.session_audit_path(DAY, "ls").read_bytes())["status"] == "OK"
    assert source.read_bytes() == before


@pytest.mark.parametrize("status", ["OK", "PARTIAL"])
def test_ok_audit_immutable_gapfill_hash_and_second_run_noop(tmp_path, status) -> None:
    paths = DataPaths(tmp_path)
    journal(paths)
    ap = audit(paths, status=status)
    tape(paths)
    before = ap.read_bytes(), ap.stat().st_mtime_ns
    assert run(paths).gapfilled == 1
    rows, report = paths.gapfill_paths(DAY, "ls")
    doc = json.loads(report.read_bytes())
    assert doc["notes"]["audit_sha256"] == hashlib.sha256(before[0]).hexdigest()
    assert doc["totals"] == {"intervals": 1, "missing_trades": 1, "recovered": 1, "residual": 0}
    assert pl.read_parquet(rows)["chetime"].to_list() == ["090001"]
    assert derive_day_repair(paths, DAY, "ls", settings=RepairSettings(),
                             tape_done=read_ledger_done(paths.tick_tape_ledger)).state is RepairState.REPAIRED
    outputs = [(p.read_bytes(), p.stat().st_mtime_ns) for p in (rows, report)]
    second = run(paths)
    assert (second.audited, second.gapfilled) == (0, 0)
    assert outputs == [(p.read_bytes(), p.stat().st_mtime_ns) for p in (rows, report)]
    assert (ap.read_bytes(), ap.stat().st_mtime_ns) == before


def test_waiting_tape_is_expected_progress(tmp_path, caplog) -> None:
    paths = DataPaths(tmp_path)
    journal(paths)
    audit(paths)
    with caplog.at_level("INFO"):
        summary = run(paths)
    assert summary.status == "OK"
    assert summary.waiting_tape == 1
    assert "symbol_days=1" in caplog.text
    assert not paths.gapfill_paths(DAY, "ls")[0].exists()


@pytest.mark.parametrize("stop_kind", ["deadline", "signal"])
def test_stops_between_oldest_days_and_reports_remaining_unaudited(tmp_path, monkeypatch, stop_kind) -> None:
    paths = DataPaths(tmp_path)
    days = [DAY - dt.timedelta(days=i) for i in (2, 1, 0)]
    for day in days:
        journal(paths, day)
    original = job.write_session_audit
    stopped = threading.Event()

    def write(audit, path):
        original(audit, path)
        stopped.set()

    monkeypatch.setattr(job, "write_session_audit", write)
    summary = run(paths, now=lambda: DEADLINE if stopped.is_set() and stop_kind == "deadline" else NOW,
                  should_stop=lambda: stopped.is_set() and stop_kind == "signal")
    assert summary.status == "DEGRADED"
    assert summary.audited == 1
    assert paths.session_audit_path(days[0], "ls").exists()
    assert all(not paths.session_audit_path(day, "ls").exists() for day in days[1:])


@pytest.mark.parametrize("unexpected", [False, True])
def test_corrupt_oldest_day_isolated(tmp_path, monkeypatch, unexpected, caplog) -> None:
    paths = DataPaths(tmp_path)
    older = DAY - dt.timedelta(days=1)
    for day in (older, DAY):
        journal(paths, day)
        audit(paths, day)
        tape(paths, day, corrupt=day == older)
    if unexpected:
        original = job.load_tape_for_symbols

        def load(paths, day, symbols):
            if day == older:
                raise RuntimeError("unexpected")
            return original(paths, day, symbols)

        monkeypatch.setattr(job, "load_tape_for_symbols", load)
    with caplog.at_level("INFO"):
        summary = run(paths)
    assert summary.status == "DEGRADED"
    assert summary.failed == 1
    assert summary.gapfilled == 1
    assert any(r.levelname == "ERROR" and f"day={older}" in r.message and "vendor=ls" in r.message for r in caplog.records)
    assert sum("state=" in r.message and "stage=repair_ticks" in r.message for r in caplog.records) == 2


def test_unavailable_audit_persisted_once_and_all_failed(tmp_path, monkeypatch) -> None:
    paths = DataPaths(tmp_path)
    source = journal(paths)
    source.write_bytes(b"corrupt zstd")
    summary = run(paths)
    assert summary.status == "FAIL"
    assert summary.failed == 1
    assert json.loads(paths.session_audit_path(DAY, "ls").read_bytes())["status"] == "UNAVAILABLE"


def test_lock_contention_and_creation_failure_fail_closed(tmp_path, monkeypatch) -> None:
    paths = DataPaths(tmp_path)
    journal(paths)
    paths.work_root.mkdir()
    with (paths.work_root / "repair.lock").open("a+b") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert run(paths).status == "SKIP"
    assert not paths.session_audit_path(DAY, "ls").exists()
    original = pathlib.Path.open

    def fail_lock(self, *args, **kwargs):
        if self.name == "repair.lock":
            raise PermissionError("lock denied")
        return original(self, *args, **kwargs)

    monkeypatch.setattr(pathlib.Path, "open", fail_lock)
    assert run(paths).status == "FAIL"
    assert not paths.session_audit_path(DAY, "ls").exists()


def test_non_contention_lock_failure_is_fail(tmp_path, monkeypatch) -> None:
    def fail(*args):
        raise OSError("flock unavailable")

    monkeypatch.setattr(job.fcntl, "flock", fail)
    assert run(DataPaths(tmp_path)).status == "FAIL"


def test_kis_audit_ensured_never_gapfilled(tmp_path, monkeypatch) -> None:
    paths = DataPaths(tmp_path)
    journal(paths, vendor="kis")
    journal(paths)
    summary = run(paths, repair_settings=RepairSettings(gate_vendors=("kis", "ls")))
    assert summary.audited == 2
    assert summary.days == 1
    assert paths.session_audit_path(DAY, "kis").exists()
    assert not paths.gapfill_paths(DAY, "kis")[0].exists()


def test_candidate_scope_and_only_day(tmp_path) -> None:
    paths = DataPaths(tmp_path)
    for offset in (-12, -11, -1, 0, 1):
        journal(paths, DAY + dt.timedelta(days=offset))
    journal(paths, vendor="db")
    base = paths.journal_root / "ls/krx/regular/H0STCNT0"
    (base / "dt=bad").mkdir()
    (base / "junk").mkdir()
    (base / "dt=2026-10-21").write_bytes(b"not a directory")
    units = job.repair_candidate_units(paths.journal_root, repair_settings=RepairSettings(), window_date=DAY, retain_days=3)
    assert units == tuple((DAY + dt.timedelta(days=i), "ls") for i in (-11, -1, 0))
    summary = run(paths, only_day=DAY)
    assert summary.days == 1
    assert summary.audited == 1
    assert not paths.session_audit_path(DAY - dt.timedelta(days=1), "ls").exists()


def test_empty_and_deadline_before_start(tmp_path) -> None:
    paths = DataPaths(tmp_path)
    assert run(paths).status == "SKIP"
    journal(paths)
    assert run(paths, now=lambda: DEADLINE).status == "DEGRADED"
    audit(paths, missing=0)
    assert run(paths, should_stop=lambda: True).status == "OK"
    assert run(paths, deadline=DEADLINE.replace(tzinfo=None)).status == "FAIL"


def test_clean_and_merge_pending_do_nothing(tmp_path) -> None:
    paths = DataPaths(tmp_path)
    journal(paths)
    ap = audit(paths, missing=0)
    before = ap.read_bytes()
    assert run(paths).status == "OK"
    assert run(paths, repair_settings=RepairSettings(merge_required=True)).gapfilled == 0
    assert ap.read_bytes() == before


def test_malformed_ok_audit_not_overwritten(tmp_path) -> None:
    paths = DataPaths(tmp_path)
    journal(paths)
    ap = audit(paths)
    doc = json.loads(ap.read_bytes())
    del doc["totals"]
    ap.write_text(json.dumps(doc))
    before = ap.read_bytes()
    assert run(paths).status == "DEGRADED"
    assert ap.read_bytes() == before


@pytest.mark.parametrize("contents", [b"not json", b"[]", b'{"status":"UNAVAILABLE"}'])
def test_bad_audit_replaced(tmp_path, contents) -> None:
    paths = DataPaths(tmp_path)
    journal(paths)
    audit(paths).write_bytes(contents)
    assert run(paths).audited == 1


def test_heartbeat_during_blocking_gapfill_and_no_thread_leak(tmp_path, monkeypatch, caplog) -> None:
    paths = DataPaths(tmp_path)
    journal(paths)
    audit(paths)
    tape(paths)
    received = threading.Event()
    original = job.load_tape_for_symbols

    class SignalHandler(__import__("logging").Handler):
        def emit(self, record):
            if "progress=heartbeat" in record.getMessage():
                received.set()

    def slow(*args):
        assert received.wait(2), "long work must emit heartbeat without returning"
        return original(*args)

    monkeypatch.setattr(job, "_HEARTBEAT_S", 0.001)
    monkeypatch.setattr(job, "load_tape_for_symbols", slow)
    handler = SignalHandler()
    job.logger.addHandler(handler)
    threads = set(threading.enumerate())
    try:
        with caplog.at_level("INFO"):
            assert run(paths).gapfilled == 1
    finally:
        job.logger.removeHandler(handler)
    assert set(threading.enumerate()) == threads


def test_keyboard_interrupt_releases_lock(tmp_path, monkeypatch) -> None:
    paths = DataPaths(tmp_path)
    journal(paths)

    def interrupt(*args, **kwargs):
        raise KeyboardInterrupt

    with monkeypatch.context() as patch:
        patch.setattr(job, "derive_session_audit", interrupt)
        with pytest.raises(KeyboardInterrupt):
            run(paths)
    assert run(paths).audited == 1


def test_ledger_failure_and_clock_failure_are_not_success(tmp_path) -> None:
    paths = DataPaths(tmp_path)
    journal(paths)
    paths.tick_tape_ledger.parent.mkdir(parents=True)
    paths.tick_tape_ledger.write_bytes(b"bad\n")
    assert run(paths).status == "FAIL"
    paths.tick_tape_ledger.write_bytes(b"")

    def broken_clock():
        raise RuntimeError("clock failed")

    assert run(paths, now=broken_clock).status == "FAIL"


def test_state_read_failure_does_not_abort_later_day(tmp_path, monkeypatch) -> None:
    paths = DataPaths(tmp_path)
    older = DAY - dt.timedelta(days=1)
    journal(paths, older)
    journal(paths)
    original = job.derive_day_repair

    def classify(paths, day, vendor, **kwargs):
        if day == older:
            raise OSError("state unreadable")
        return original(paths, day, vendor, **kwargs)

    monkeypatch.setattr(job, "derive_day_repair", classify)
    summary = run(paths)
    assert summary.status == "DEGRADED"
    assert summary.failed == 1
    assert summary.audited == 1
    assert paths.session_audit_path(DAY, "ls").exists()
