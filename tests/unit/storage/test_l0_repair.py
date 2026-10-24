"""Part 1 repair-gate guards: config, derive/hold, retention hold, EOD wiring."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import logging

import pytest


def _audit_doc(day, vendor, status="OK", confirmed=0, symbols=(), reason="ok"):
    return {
        "schema_version": 1,
        "session_date": day.isoformat(),
        "vendor": vendor,
        "venue": "krx",
        "session": "regular",
        "status": status,
        "reason": reason,
        "generated_at_ns": 1,
        "sources": [],
        "latency": [],
        "symbols": list(symbols),
        "totals": {
            "symbols": len(symbols),
            "symbols_with_loss": 0,
            "symbols_without_counter": 0,
            "trades_observed": 10,
            "confirmed_missing": confirmed,
            "count_only_missing": 0,
            "net_missing_positive": 0,
            "volume_deficit": 0,
            "loss_ratio": 0.0,
        },
        "tail_silence_s": 0.0,
        "last_regular_tick_recv_ns": 1,
    }


def _write_audit(paths, day, vendor, doc):
    target = paths.session_audit_path(day, vendor)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(doc), encoding="utf-8")
    return target


def test_repair_defaults_inert(monkeypatch) -> None:
    import os

    from src.core.config import RepairSettings

    for name in [n for n in os.environ if n.startswith("KRX_ALPHA_REPAIR_")]:
        monkeypatch.delenv(name, raising=False)
    settings = RepairSettings()
    assert settings.hold_enabled is False
    assert settings.job_enabled is False
    assert settings.merge_required is False
    assert set(settings.repair_vendors) <= set(settings.gate_vendors)


def test_repair_vendor_subset_enforced() -> None:
    import pytest

    from src.core.config import RepairSettings

    with pytest.raises(ValueError, match="subset"):
        RepairSettings(repair_vendors=("ls", "toss"), gate_vendors=("ls",))


def test_repair_expiry_bound() -> None:
    import pytest

    from pydantic import ValidationError

    from src.core.config import RepairSettings

    with pytest.raises(ValidationError):
        RepairSettings(hold_max_extra_days=11)


def test_repair_vendor_scope_rejects_empty_and_upper() -> None:
    import pytest

    from src.core.config import RepairSettings

    with pytest.raises(ValueError, match="must be non-empty"):
        RepairSettings(gate_vendors=())
    with pytest.raises(ValueError, match="lowercase"):
        RepairSettings(gate_vendors=("LS",))


def test_audit_default_enforce(monkeypatch) -> None:
    import os

    from src.core.config import AuditMode, AuditSettings

    for name in [n for n in os.environ if n.startswith("KRX_ALPHA_AUDIT_")]:
        monkeypatch.delenv(name, raising=False)
    assert AuditSettings().mode is AuditMode.ENFORCE
    monkeypatch.setenv("KRX_ALPHA_AUDIT_MODE", "shadow")
    assert AuditSettings().mode is AuditMode.SHADOW


def _paths(tmp_path):
    from src.core.paths import DataPaths

    return DataPaths(tmp_path)


def _settings(**kw):
    from src.core.config import RepairSettings

    base = {"hold_enabled": True}
    base.update(kw)
    return RepairSettings(**base)


def test_missing_audit_is_unaudited(tmp_path) -> None:
    from src.storage.l0_repair import DayRepair, RepairState, derive_day_repair

    paths = _paths(tmp_path)
    day = dt.date(2026, 9, 14)
    out: DayRepair = derive_day_repair(paths, day, "ls", settings=_settings(), tape_done=frozenset())
    assert out.state is RepairState.UNAUDITED


def test_unavailable_audit_is_unaudited(tmp_path) -> None:
    from src.storage.l0_repair import RepairState, derive_day_repair

    paths = _paths(tmp_path)
    day = dt.date(2026, 9, 14)
    _write_audit(paths, day, "ls", _audit_doc(day, "ls", status="UNAVAILABLE", reason="time_budget"))
    out = derive_day_repair(paths, day, "ls", settings=_settings(), tape_done=frozenset())
    assert out.state is RepairState.UNAUDITED


def test_zero_loss_is_clean(tmp_path) -> None:
    from src.storage.l0_repair import RepairState, derive_day_repair

    paths = _paths(tmp_path)
    day = dt.date(2026, 9, 14)
    _write_audit(paths, day, "ls", _audit_doc(day, "ls", confirmed=0))
    out = derive_day_repair(paths, day, "ls", settings=_settings(), tape_done=frozenset())
    assert out.state is RepairState.CLEAN


def test_kis_loss_is_not_repaired_but_clean(tmp_path) -> None:
    from src.storage.l0_repair import RepairState, derive_day_repair

    paths = _paths(tmp_path)
    day = dt.date(2026, 9, 14)
    _write_audit(paths, day, "kis", _audit_doc(day, "kis", confirmed=5))
    out = derive_day_repair(paths, day, "kis", settings=_settings(), tape_done=frozenset())
    assert out.state is RepairState.CLEAN


def test_gap_without_tape_is_tape_pending(tmp_path) -> None:
    from src.storage.l0_repair import RepairState, derive_day_repair

    paths = _paths(tmp_path)
    day = dt.date(2026, 9, 14)
    sym = {"symbol": "005930", "confirmed_missing": 3, "intervals_truncated": 0, "intervals": []}
    _write_audit(paths, day, "ls", _audit_doc(day, "ls", confirmed=3, symbols=[sym]))
    out = derive_day_repair(paths, day, "ls", settings=_settings(), tape_done=frozenset())
    assert out.state is RepairState.TAPE_PENDING
    assert out.gap_symbol_days_pending == 1


def test_tape_done_without_report_is_gapfill_pending(tmp_path) -> None:
    from src.storage.l0_repair import RepairState, derive_day_repair

    paths = _paths(tmp_path)
    day = dt.date(2026, 9, 14)
    sym = {"symbol": "005930", "confirmed_missing": 3, "intervals_truncated": 0, "intervals": []}
    _write_audit(paths, day, "ls", _audit_doc(day, "ls", confirmed=3, symbols=[sym]))
    out = derive_day_repair(paths, day, "ls", settings=_settings(), tape_done=frozenset({("005930", day)}))
    assert out.state is RepairState.GAPFILL_PENDING


def _write_report(paths, day, vendor, audit_sha=None, legacy=False, torn=False):
    rows_path, report_path = paths.gapfill_paths(day, vendor)
    rows_path.parent.mkdir(parents=True, exist_ok=True)
    rows_path.write_bytes(b"rows")
    if torn:
        report_path.write_text("{truncated", encoding="utf-8")
        return report_path
    notes = {} if legacy else {"audit_sha256": audit_sha}
    report_path.write_text(json.dumps({"totals": {"residual": 2}, "intervals": [], "notes": notes}), encoding="utf-8")
    return report_path


def test_valid_report_is_repaired_with_residual(tmp_path) -> None:
    from src.storage.l0_repair import RepairState, derive_day_repair

    paths = _paths(tmp_path)
    day = dt.date(2026, 9, 14)
    sym = {"symbol": "005930", "confirmed_missing": 3, "intervals_truncated": 0, "intervals": []}
    target = _write_audit(paths, day, "ls", _audit_doc(day, "ls", confirmed=3, symbols=[sym]))
    digest = hashlib.sha256(target.read_bytes()).hexdigest()
    _write_report(paths, day, "ls", audit_sha=digest)
    out = derive_day_repair(paths, day, "ls", settings=_settings(), tape_done=frozenset({("005930", day)}))
    assert out.state is RepairState.REPAIRED
    assert out.residual == 2


def test_stale_report_is_gapfill_pending(tmp_path) -> None:
    from src.storage.l0_repair import RepairState, derive_day_repair

    paths = _paths(tmp_path)
    day = dt.date(2026, 9, 14)
    sym = {"symbol": "005930", "confirmed_missing": 3, "intervals_truncated": 0, "intervals": []}
    _write_audit(paths, day, "ls", _audit_doc(day, "ls", confirmed=3, symbols=[sym]))
    _write_report(paths, day, "ls", audit_sha="0" * 64)
    out = derive_day_repair(paths, day, "ls", settings=_settings(), tape_done=frozenset({("005930", day)}))
    assert out.state is RepairState.GAPFILL_PENDING


def test_legacy_report_uses_mtime(tmp_path) -> None:
    import os
    import time

    from src.storage.l0_repair import RepairState, derive_day_repair

    paths = _paths(tmp_path)
    day = dt.date(2026, 9, 14)
    sym = {"symbol": "005930", "confirmed_missing": 3, "intervals_truncated": 0, "intervals": []}
    target = _write_audit(paths, day, "ls", _audit_doc(day, "ls", confirmed=3, symbols=[sym]))
    report = _write_report(paths, day, "ls", legacy=True)
    os.utime(report, (target.stat().st_mtime + 5, target.stat().st_mtime + 5))
    out = derive_day_repair(paths, day, "ls", settings=_settings(), tape_done=frozenset({("005930", day)}))
    assert out.state is RepairState.REPAIRED
    os.utime(report, (target.stat().st_mtime - 50, target.stat().st_mtime - 50))
    time.sleep(0.01)
    out2 = derive_day_repair(paths, day, "ls", settings=_settings(), tape_done=frozenset({("005930", day)}))
    assert out2.state is RepairState.GAPFILL_PENDING


def test_torn_report_never_repaired(tmp_path) -> None:
    from src.storage.l0_repair import RepairState, derive_day_repair

    paths = _paths(tmp_path)
    day = dt.date(2026, 9, 14)
    sym = {"symbol": "005930", "confirmed_missing": 3, "intervals_truncated": 0, "intervals": []}
    _write_audit(paths, day, "ls", _audit_doc(day, "ls", confirmed=3, symbols=[sym]))
    _write_report(paths, day, "ls", torn=True)
    out = derive_day_repair(paths, day, "ls", settings=_settings(), tape_done=frozenset({("005930", day)}))
    assert out.state is not RepairState.REPAIRED


def test_merge_required_gates_repaired_and_clean(tmp_path) -> None:
    from src.storage.l0_repair import RepairState, derive_day_repair

    paths = _paths(tmp_path)
    day = dt.date(2026, 9, 14)
    _write_audit(paths, day, "ls", _audit_doc(day, "ls", confirmed=0))
    out = derive_day_repair(paths, day, "ls", settings=_settings(merge_required=True), tape_done=frozenset())
    assert out.state is RepairState.MERGE_PENDING
    sym = {"symbol": "005930", "confirmed_missing": 1, "intervals_truncated": 0, "intervals": []}
    target = _write_audit(paths, day, "ls", _audit_doc(day, "ls", confirmed=1, symbols=[sym]))
    _write_report(paths, day, "ls", audit_sha=hashlib.sha256(target.read_bytes()).hexdigest())
    out2 = derive_day_repair(
        paths,
        day,
        "ls",
        settings=_settings(merge_required=True),
        tape_done=frozenset({("005930", day)}),
    )
    assert out2.state is RepairState.MERGE_PENDING


def test_hold_ignores_other_streams(tmp_path) -> None:
    import pathlib

    from src.storage.l0_repair import l0_hold_reason

    paths = _paths(tmp_path)
    day = dt.date(2026, 9, 1)
    rel = pathlib.PurePath("ls/krx/regular/H0STASP0/dt=2026-09-01")
    assert (
        l0_hold_reason(
            rel,
            day,
            paths=paths,
            settings=_settings(),
            tape_done=frozenset(),
            cutoff=dt.date(2026, 9, 14),
        )
        is None
    )


def test_hold_disabled_is_none(tmp_path) -> None:
    import pathlib

    from src.storage.l0_repair import l0_hold_reason

    paths = _paths(tmp_path)
    day = dt.date(2026, 9, 1)
    rel = pathlib.PurePath("ls/krx/regular/H0STCNT0/dt=2026-09-01")
    assert (
        l0_hold_reason(
            rel,
            day,
            paths=paths,
            settings=_settings(hold_enabled=False),
            tape_done=frozenset(),
            cutoff=dt.date(2026, 9, 14),
        )
        is None
    )


def test_hold_expires_loudly(tmp_path, caplog) -> None:
    import pathlib

    from src.storage.l0_repair import l0_hold_reason

    paths = _paths(tmp_path)
    day = dt.date(2026, 9, 1)
    sym = {"symbol": "005930", "confirmed_missing": 2, "intervals_truncated": 0, "intervals": []}
    _write_audit(paths, day, "ls", _audit_doc(day, "ls", confirmed=2, symbols=[sym]))
    rel = pathlib.PurePath("ls/krx/regular/H0STCNT0/dt=2026-09-01")
    with caplog.at_level(logging.CRITICAL):
        out = l0_hold_reason(
            rel,
            day,
            paths=paths,
            settings=_settings(hold_max_extra_days=0),
            tape_done=frozenset({("005930", day)}),
            cutoff=dt.date(2026, 9, 14),
        )
    assert out is None
    assert "status=EXPIRED" in caplog.text


def test_hold_active_reason(tmp_path) -> None:
    import pathlib

    from src.storage.l0_repair import l0_hold_reason

    paths = _paths(tmp_path)
    day = dt.date(2026, 9, 12)
    sym = {"symbol": "005930", "confirmed_missing": 2, "intervals_truncated": 0, "intervals": []}
    _write_audit(paths, day, "ls", _audit_doc(day, "ls", confirmed=2, symbols=[sym]))
    rel = pathlib.PurePath("ls/krx/regular/H0STCNT0/dt=2026-09-12")
    out = l0_hold_reason(
        rel,
        day,
        paths=paths,
        settings=_settings(),
        tape_done=frozenset({("005930", day)}),
        cutoff=dt.date(2026, 9, 14),
    )
    assert out == "ls:gapfill_pending"


def test_audited_tick_vendor_shapes() -> None:
    import pathlib

    from src.storage.l0_repair import audited_tick_vendor

    assert audited_tick_vendor(pathlib.PurePath("ls/krx/regular/H0STCNT0/dt=2026-09-01")) == "ls"
    assert audited_tick_vendor(pathlib.PurePath("ls/krx/regular/H0STASP0/dt=2026-09-01")) is None
    assert audited_tick_vendor(pathlib.PurePath("ls/krx/regular/H0STCNT0/dt=oops")) is None
    assert audited_tick_vendor(pathlib.PurePath("ls/krx/regular/H0STCNT0")) is None


def _write_l0_tick(tmp_path, day):
    import json

    import zstandard as zstd

    part = tmp_path / "l0" / "ls" / "krx" / "regular" / "H0STCNT0" / f"dt={day.isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    rec = {
        "raw": "a",
        "recv_mono_ns": 1,
        "recv_wall_ns": 2,
        "conn_id": "c1",
        "conn_seq": 1,
        "vendor": "ls",
        "tr_id": "H0STCNT0",
    }
    (part / "09.jsonl.zst").write_bytes(zstd.ZstdCompressor().compress((json.dumps(rec) + "\n").encode()))
    return part


def test_held_partition_survives_but_is_normalized(tmp_path) -> None:
    from src.storage.retention import prune_old_journals

    day = dt.date(2026, 9, 1)
    _write_l0_tick(tmp_path, day)
    archive_root = tmp_path / "l1"
    stats = prune_old_journals(
        tmp_path / "l0",
        archive_root=archive_root,
        retain_days=3,
        reference_date=dt.date(2026, 9, 14),
        verified_remote_l1=frozenset({"l1/ls/krx/regular/H0STCNT0/dt=2026-09-01.parquet"}),
        hold=lambda rel: "ls:gapfill_pending",
    )
    assert stats.deleted == 0
    assert "l1/ls/krx/regular/H0STCNT0/dt=2026-09-01.parquet" in stats.held
    assert (archive_root / "ls" / "krx" / "regular" / "H0STCNT0" / "dt=2026-09-01.parquet").exists()
    assert (tmp_path / "l0" / "ls" / "krx" / "regular" / "H0STCNT0" / "dt=2026-09-01").exists()


def test_low_disk_overrides_hold(tmp_path, caplog) -> None:
    from src.storage.retention import prune_old_journals

    day = dt.date(2026, 9, 1)
    part = _write_l0_tick(tmp_path, day)
    out = tmp_path / "l1" / "ls" / "krx" / "regular" / "H0STCNT0" / "dt=2026-09-01.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(b"l1")
    with caplog.at_level(logging.CRITICAL):
        stats = prune_old_journals(
            tmp_path / "l0",
            archive_root=tmp_path / "l1",
            retain_days=3,
            reference_date=dt.date(2026, 9, 14),
            verified_remote_l1=frozenset({"l1/ls/krx/regular/H0STCNT0/dt=2026-09-01.parquet"}),
            normalize=False,
            hold=lambda rel: "ls:gapfill_pending",
        )
    assert stats.deleted == 1
    assert not part.exists()
    assert "OVERRIDDEN" in caplog.text


def test_eod_maintenance_forwards_hold(tmp_path) -> None:
    from src.orchestration.eod import run_eod_maintenance

    day = dt.date(2026, 9, 1)
    _write_l0_tick(tmp_path, day)
    stats = run_eod_maintenance(
        tmp_path / "l0",
        retain_days=3,
        today=dt.date(2026, 9, 14),
        archive_root=tmp_path / "l1",
        verified_remote_l1=frozenset(),
        hold=lambda rel: None,
    )
    assert int(stats) >= 0


def test_reuse_fresh_hold_skips_deletion(tmp_path) -> None:
    import pyarrow.parquet as pq

    from src.storage.retention import _reusable_l1_rows, prune_old_journals

    day = dt.date(2026, 9, 1)
    part = _write_l0_tick(tmp_path, day)
    out = tmp_path / "l1" / "ls" / "krx" / "regular" / "H0STCNT0" / "dt=2026-09-01.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    import polars as pl

    pl.DataFrame({"a": [1]}).write_parquet(out)
    assert _reusable_l1_rows(part, out) == 1
    stats = prune_old_journals(
        tmp_path / "l0",
        archive_root=tmp_path / "l1",
        retain_days=3,
        reference_date=dt.date(2026, 9, 14),
        verified_remote_l1=frozenset({"l1/ls/krx/regular/H0STCNT0/dt=2026-09-01.parquet"}),
        reuse_fresh_l1=True,
        hold=lambda rel: "ls:gapfill_pending",
    )
    assert stats.held == frozenset({"l1/ls/krx/regular/H0STCNT0/dt=2026-09-01.parquet"})
    assert part.exists()
    assert pq.read_metadata(out).num_rows == 1


def test_merged_valid_branches(tmp_path) -> None:
    import sys
    import types

    from src.storage import l0_repair as mod

    paths = _paths(tmp_path)
    day = dt.date(2026, 9, 14)
    assert mod._merged_ticks_valid(paths, day, "ls") is False
    fake = types.ModuleType("src.storage.l0_repair_merge")
    fake.repaired_ticks_valid = lambda *a, **k: True  # type: ignore[attr-defined]
    sys.modules["src.storage.l0_repair_merge"] = fake
    try:
        assert mod._merged_ticks_valid(paths, day, "ls") is True
        fake.repaired_ticks_valid = lambda *a, **k: 1 / 0  # type: ignore[attr-defined]
        assert mod._merged_ticks_valid(paths, day, "ls") is False
    finally:
        del sys.modules["src.storage.l0_repair_merge"]


def test_report_non_dict_is_gapfill_pending(tmp_path) -> None:
    from src.storage.l0_repair import RepairState, derive_day_repair

    paths = _paths(tmp_path)
    day = dt.date(2026, 9, 14)
    sym = {"symbol": "005930", "confirmed_missing": 1, "intervals_truncated": 0, "intervals": []}
    _write_audit(paths, day, "ls", _audit_doc(day, "ls", confirmed=1, symbols=[sym]))
    rows_path, report_path = paths.gapfill_paths(day, "ls")
    rows_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("[]", encoding="utf-8")
    out = derive_day_repair(paths, day, "ls", settings=_settings(), tape_done=frozenset({("005930", day)}))
    assert out.state is RepairState.GAPFILL_PENDING


def test_repair_hold_helper_shapes(tmp_path) -> None:
    import pathlib

    import src.orchestration.daemon as daemon_mod
    from src.core.config import RepairSettings

    paths = _paths(tmp_path)
    hold = daemon_mod._repair_hold_for(paths, dt.date(2026, 9, 14), RepairSettings(hold_enabled=True), frozenset())
    assert hold(pathlib.Path("ls/dt=oops")) is None
    assert hold(pathlib.Path("odd-name")) is None


def test_daemon_hold_and_repair_days(tmp_path, monkeypatch) -> None:
    import src.orchestration.daemon as daemon_mod
    from src.core.config import CollectorSettings, RepairSettings
    from src.core.paths import DataPaths

    paths = DataPaths(tmp_path)
    day = dt.date(2026, 9, 12)
    sym = {"symbol": "005930", "confirmed_missing": 1, "intervals_truncated": 0, "intervals": []}
    target = paths.session_audit_path(day, "ls")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(_audit_doc(day, "ls", confirmed=1, symbols=[sym])), encoding="utf-8")
    cfg = CollectorSettings(data_root=tmp_path)
    repair = RepairSettings(hold_enabled=True)
    summary = daemon_mod._repair_days_summary(paths, dt.date(2026, 9, 14), cfg, repair)
    assert "ls:tape_pending" in summary or "ls:gapfill_pending" in summary or "unaudited" in summary
    summary_off = daemon_mod._repair_days_summary(paths, dt.date(2026, 9, 14), cfg, RepairSettings())
    assert summary_off == "2026-09-12:ls:tape_pending"
    assert daemon_mod._repair_days_summary(paths, dt.date(2026, 9, 14), cfg, None) == "none"
    calls: dict[str, object] = {}

    def _fake_maintenance(journal_root, **kw):
        from src.storage.retention import PruneStats

        calls.update(kw)
        return PruneStats(0, 0, held=frozenset({"l1/ls/krx/regular/H0STCNT0/dt=2026-09-12.parquet"}))

    class _Offload:
        verified_remote_l1 = frozenset({"l1/ls/krx/regular/H0STCNT0/dt=2026-09-12.parquet"})
        purged = 0

        class l1:  # noqa: N801
            uploaded = 0

    monkeypatch.setattr(daemon_mod, "check_disk_watermark", lambda path, *, min_free_gb: True)
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", _fake_maintenance)
    monkeypatch.setattr(daemon_mod, "run_eod_offload", lambda *a, **k: _Offload())
    monkeypatch.setattr(daemon_mod, "run_eod_remote_l0_purge", lambda *a, **k: None)
    house = daemon_mod._run_eod_housekeeping(
        cfg, paths, dt.date(2026, 9, 14), progress=lambda: None, repair_settings=repair
    )
    assert house.held == 1
    assert "hold" in calls
    ledger = paths.tick_tape_ledger
    ledger.parent.mkdir(parents=True, exist_ok=True)
    ledger.write_bytes(b'{"symbol": "A", "date": "bad"\n')
    summary_torn = daemon_mod._repair_days_summary(paths, dt.date(2026, 9, 14), cfg, repair)
    assert isinstance(summary_torn, str)
    house_torn = daemon_mod._run_eod_housekeeping(
        cfg, paths, dt.date(2026, 9, 14), progress=lambda: None, repair_settings=repair
    )
    assert house_torn.held >= 0


def test_derive_empty_vendor_raises(tmp_path) -> None:
    import pytest

    from src.storage.l0_repair import derive_day_repair

    with pytest.raises(ValueError, match="vendor"):
        derive_day_repair(_paths(tmp_path), dt.date(2026, 9, 14), "", settings=_settings(), tape_done=frozenset())


def test_derive_torn_and_shape_variants_are_unaudited(tmp_path) -> None:
    from src.storage.l0_repair import derive_day_repair

    paths = _paths(tmp_path)
    day = dt.date(2026, 9, 14)
    target = paths.session_audit_path(day, "ls")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("{torn", encoding="utf-8")
    assert derive_day_repair(paths, day, "ls", settings=_settings(), tape_done=frozenset()).state.value == "unaudited"
    target.write_text("[]", encoding="utf-8")
    assert derive_day_repair(paths, day, "ls", settings=_settings(), tape_done=frozenset()).state.value == "unaudited"
    target.write_text(json.dumps({"session_date": "2026-09-13", "status": "OK"}), encoding="utf-8")
    assert derive_day_repair(paths, day, "ls", settings=_settings(), tape_done=frozenset()).state.value == "unaudited"
    target.write_text(json.dumps({"session_date": day.isoformat(), "status": "OK"}), encoding="utf-8")
    assert derive_day_repair(paths, day, "ls", settings=_settings(), tape_done=frozenset()).state.value == "unaudited"
    target.write_text(
        json.dumps(
            {"session_date": day.isoformat(), "status": "OK", "totals": {"confirmed_missing": "x"}, "symbols": []}
        ),
        encoding="utf-8",
    )
    assert derive_day_repair(paths, day, "ls", settings=_settings(), tape_done=frozenset()).state.value == "unaudited"


def test_derive_report_without_totals_is_gapfill_pending(tmp_path) -> None:
    from src.storage.l0_repair import derive_day_repair

    paths = _paths(tmp_path)
    day = dt.date(2026, 9, 14)
    sym = {"symbol": "005930", "confirmed_missing": 1, "intervals_truncated": 0, "intervals": []}
    _write_audit(paths, day, "ls", _audit_doc(day, "ls", confirmed=1, symbols=[sym]))
    rows_path, report_path = paths.gapfill_paths(day, "ls")
    rows_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps({"intervals": []}), encoding="utf-8")
    assert (
        derive_day_repair(paths, day, "ls", settings=_settings(), tape_done=frozenset({("005930", day)})).state.value
        == "gapfill_pending"
    )


def test_derive_report_stat_failure_is_gapfill_pending(tmp_path, monkeypatch) -> None:
    import pathlib

    from src.storage.l0_repair import derive_day_repair

    paths = _paths(tmp_path)
    day = dt.date(2026, 9, 14)
    sym = {"symbol": "005930", "confirmed_missing": 1, "intervals_truncated": 0, "intervals": []}
    _write_audit(paths, day, "ls", _audit_doc(day, "ls", confirmed=1, symbols=[sym]))
    rows_path, report_path = paths.gapfill_paths(day, "ls")
    rows_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps({"totals": {"residual": 0}, "intervals": []}), encoding="utf-8")
    real_stat = pathlib.Path.stat

    def _boom(self, *a, **k):
        if self == report_path:
            raise OSError("stat lost")
        return real_stat(self, *a, **k)

    monkeypatch.setattr(pathlib.Path, "stat", _boom)
    assert (
        derive_day_repair(paths, day, "ls", settings=_settings(), tape_done=frozenset({("005930", day)})).state.value
        == "gapfill_pending"
    )


def test_derive_audit_stat_failure_is_unaudited(tmp_path, monkeypatch) -> None:
    import pathlib

    from src.storage.l0_repair import derive_day_repair

    paths = _paths(tmp_path)
    day = dt.date(2026, 9, 14)
    _write_audit(paths, day, "ls", _audit_doc(day, "ls", confirmed=0))
    target = paths.session_audit_path(day, "ls")
    real_stat = pathlib.Path.stat

    def _boom(self, *a, **k):
        if self == target:
            raise OSError("stat lost")
        return real_stat(self, *a, **k)

    monkeypatch.setattr(pathlib.Path, "stat", _boom)
    assert derive_day_repair(paths, day, "ls", settings=_settings(), tape_done=frozenset()).state.value == "unaudited"


def test_derive_bad_session_date_is_unaudited(tmp_path) -> None:
    from src.storage.l0_repair import derive_day_repair

    paths = _paths(tmp_path)
    day = dt.date(2026, 9, 14)
    _write_audit(paths, day, "ls", {"session_date": "not-a-date", "status": "OK"})
    assert derive_day_repair(paths, day, "ls", settings=_settings(), tape_done=frozenset()).state.value == "unaudited"


def test_derive_malformed_symbols_is_unaudited(tmp_path) -> None:
    from src.storage.l0_repair import derive_day_repair

    paths = _paths(tmp_path)
    day = dt.date(2026, 9, 14)
    doc = _audit_doc(day, "ls", confirmed=0)
    doc["symbols"] = [123, {"symbol": "X", "confirmed_missing": "bad"}, {"symbol": "", "confirmed_missing": 2}]
    _write_audit(paths, day, "ls", doc)
    assert derive_day_repair(paths, day, "ls", settings=_settings(), tape_done=frozenset()).state.value == "unaudited"


def test_derive_bad_residual_is_gapfill_pending(tmp_path) -> None:
    from src.storage.l0_repair import derive_day_repair

    paths = _paths(tmp_path)
    day = dt.date(2026, 9, 14)
    sym = {"symbol": "005930", "confirmed_missing": 1, "intervals_truncated": 0, "intervals": []}
    _write_audit(paths, day, "ls", _audit_doc(day, "ls", confirmed=1, symbols=[sym]))
    rows_path, report_path = paths.gapfill_paths(day, "ls")
    rows_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps({"totals": {"residual": None}, "intervals": [], "notes": {}}), encoding="utf-8")
    assert (
        derive_day_repair(paths, day, "ls", settings=_settings(), tape_done=frozenset({("005930", day)})).state.value
        == "gapfill_pending"
    )


def test_audited_tick_vendor_rejects_non_dt_name() -> None:
    import pathlib

    from src.storage.l0_repair import audited_tick_vendor

    assert audited_tick_vendor(pathlib.PurePath("ls/krx/regular/H0STCNT0/2026-09-01")) is None


def test_hold_rejects_non_gate_vendor(tmp_path) -> None:
    import pathlib

    from src.storage.l0_repair import l0_hold_reason
    from src.core.config import RepairSettings

    paths = _paths(tmp_path)
    day = dt.date(2026, 9, 12)
    rel = pathlib.PurePath("kis/krx/regular/H0STCNT0/dt=2026-09-12")
    settings = RepairSettings(hold_enabled=True, gate_vendors=("ls",))
    assert l0_hold_reason(rel, day, paths=paths, settings=settings, tape_done=frozenset(), cutoff=dt.date(2026, 9, 14)) is None


def test_hold_clean_day_is_none(tmp_path) -> None:
    import pathlib

    from src.storage.l0_repair import l0_hold_reason

    paths = _paths(tmp_path)
    day = dt.date(2026, 9, 12)
    _write_audit(paths, day, "ls", _audit_doc(day, "ls", confirmed=0))
    rel = pathlib.PurePath("ls/krx/regular/H0STCNT0/dt=2026-09-12")
    assert (
        l0_hold_reason(rel, day, paths=paths, settings=_settings(), tape_done=frozenset(), cutoff=dt.date(2026, 9, 14))
        is None
    )


@pytest.mark.parametrize("value", [None, True, -1, 0.5, float("inf"), "0"])
def test_invalid_audit_counts_never_release_hold(tmp_path, value) -> None:
    from src.storage.l0_repair import RepairState, derive_day_repair

    paths, day = _paths(tmp_path), dt.date(2026, 9, 12)
    doc = _audit_doc(day, "ls", confirmed=value)
    _write_audit(paths, day, "ls", doc)
    assert derive_day_repair(paths, day, "ls", settings=_settings(), tape_done=frozenset()).state is RepairState.UNAUDITED


@pytest.mark.parametrize("status", [None, "", "UNKNOWN", "unavailable"])
def test_unknown_audit_status_is_not_clean(tmp_path, status) -> None:
    from src.storage.l0_repair import RepairState, derive_day_repair

    paths, day = _paths(tmp_path), dt.date(2026, 9, 12)
    _write_audit(paths, day, "ls", _audit_doc(day, "ls", status=status))
    assert derive_day_repair(paths, day, "ls", settings=_settings(), tape_done=frozenset()).state is RepairState.UNAUDITED


def test_missing_confirmed_count_and_missing_gap_symbols_are_unaudited(tmp_path) -> None:
    from src.storage.l0_repair import RepairState, derive_day_repair

    paths, day = _paths(tmp_path), dt.date(2026, 9, 12)
    doc = _audit_doc(day, "ls")
    del doc["totals"]["confirmed_missing"]
    _write_audit(paths, day, "ls", doc)
    assert derive_day_repair(paths, day, "ls", settings=_settings(), tape_done=frozenset()).state is RepairState.UNAUDITED
    _write_audit(paths, day, "ls", _audit_doc(day, "ls", confirmed=2))
    assert derive_day_repair(paths, day, "ls", settings=_settings(), tape_done=frozenset()).state is RepairState.UNAUDITED


@pytest.mark.parametrize("totals", [{}, {"residual": -1}, {"residual": True}, {"residual": float("inf")}])
def test_incomplete_report_counts_never_repaired(tmp_path, totals) -> None:
    from src.storage.l0_repair import RepairState, derive_day_repair

    paths, day = _paths(tmp_path), dt.date(2026, 9, 12)
    _write_audit(paths, day, "ls", _audit_doc(day, "ls", confirmed=1, symbols=[{"symbol": "005930", "confirmed_missing": 1}]))
    report = _write_report(paths, day, "ls", legacy=True)
    report.write_text(json.dumps({"totals": totals, "intervals": []}))
    state = derive_day_repair(paths, day, "ls", settings=_settings(), tape_done={("005930", day)})
    assert state.state is RepairState.GAPFILL_PENDING


def test_explicit_null_hash_is_not_a_legacy_report(tmp_path) -> None:
    from src.storage.l0_repair import RepairState, derive_day_repair

    paths, day = _paths(tmp_path), dt.date(2026, 9, 12)
    _write_audit(paths, day, "ls", _audit_doc(day, "ls", confirmed=1, symbols=[{"symbol": "005930", "confirmed_missing": 1}]))
    _write_report(paths, day, "ls", audit_sha=None)
    state = derive_day_repair(paths, day, "ls", settings=_settings(), tape_done={("005930", day)})
    assert state.state is RepairState.GAPFILL_PENDING


@pytest.mark.parametrize("route", ["toss/krx/regular/H0STCNT0/dt=2026-09-12", "ls/krx/regular/H0STCNT0/dt=20260912", "ls/bad/regular/H0STCNT0/dt=2026-09-12"])
def test_audited_vendor_rejects_noncanonical_partitions(route) -> None:
    from pathlib import PurePath

    from src.storage.l0_repair import audited_tick_vendor

    assert audited_tick_vendor(PurePath(route)) is None


def test_hold_exception_never_deletes_verified_inputs(tmp_path) -> None:
    from src.storage.retention import prune_old_journals

    part = _write_l0_tick(tmp_path, dt.date(2026, 9, 1))

    def fail_hold(rel):
        raise RuntimeError("hold failed")

    with pytest.raises(RuntimeError, match="hold failed"):
        prune_old_journals(
            tmp_path / "l0", archive_root=tmp_path / "l1", reference_date=dt.date(2026, 9, 8),
            verified_remote_l1={"l1/ls/krx/regular/H0STCNT0/dt=2026-09-01.parquet"}, hold=fail_hold,
        )
    assert part.exists()


@pytest.mark.parametrize(("field", "value"), [("session_date", "2026-09-11"), ("vendor", "kis")])
def test_audit_identity_mismatch_stays_unaudited(tmp_path, field, value) -> None:
    from src.storage.l0_repair import RepairState, derive_day_repair

    paths, day = _paths(tmp_path), dt.date(2026, 9, 12)
    doc = _audit_doc(day, "ls")
    doc[field] = value
    _write_audit(paths, day, "ls", doc)
    assert derive_day_repair(paths, day, "ls", settings=_settings(), tape_done=frozenset()).state is RepairState.UNAUDITED


def test_hold_expiry_boundary_and_custom_gate_vendor(tmp_path, caplog) -> None:
    from pathlib import PurePath

    from src.core.config import RepairSettings
    from src.storage.l0_repair import audited_tick_vendor, l0_hold_reason

    paths, day = _paths(tmp_path), dt.date(2026, 9, 12)
    settings = RepairSettings(hold_enabled=True, hold_max_extra_days=2, gate_vendors=("other",), repair_vendors=("other",))
    rel = PurePath("other/krx/regular/H0STCNT0/dt=2026-09-12")
    assert audited_tick_vendor(rel) is None
    assert l0_hold_reason(rel, day, paths=paths, settings=settings, tape_done=frozenset(), cutoff=day + dt.timedelta(days=2)) == "other:unaudited"
    with caplog.at_level(logging.CRITICAL):
        assert l0_hold_reason(rel, day, paths=paths, settings=settings, tape_done=frozenset(), cutoff=day + dt.timedelta(days=3)) is None
    assert len([r for r in caplog.records if "status=EXPIRED" in r.getMessage()]) == 1
