"""Repair-hold EOD wiring guards (split suite for src/orchestration/daemon.py)."""

from __future__ import annotations

import datetime as dt
import pathlib

import pytest


def _audit_doc(day, vendor, confirmed=0, symbols=()):
    return {
        "schema_version": 1,
        "session_date": day.isoformat(),
        "vendor": vendor,
        "venue": "krx",
        "session": "regular",
        "status": "OK",
        "reason": "ok",
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


def test_repair_hold_for_shapes(tmp_path) -> None:
    import src.orchestration.daemon as daemon_mod
    from src.core.config import RepairSettings
    from src.core.paths import DataPaths

    paths = DataPaths(tmp_path)
    hold = daemon_mod._repair_hold_for(paths, dt.date(2026, 9, 14), RepairSettings(hold_enabled=True), frozenset())
    assert hold(pathlib.Path("ls/dt=oops")) is None
    assert hold(pathlib.Path("odd-name")) is None


def test_repair_hold_for_bad_date(tmp_path) -> None:
    import src.orchestration.daemon as daemon_mod
    from src.core.config import RepairSettings
    from src.core.paths import DataPaths

    paths = DataPaths(tmp_path)
    hold = daemon_mod._repair_hold_for(paths, dt.date(2026, 9, 14), RepairSettings(hold_enabled=True), frozenset())
    assert hold(pathlib.Path("ls/krx/regular/H0STCNT0/dt=2026-13-99")) is None


def test_repair_days_torn_ledger_degrades(tmp_path) -> None:
    import src.orchestration.daemon as daemon_mod
    from src.core.config import CollectorSettings, RepairSettings
    from src.core.paths import DataPaths

    paths = DataPaths(tmp_path)
    ledger = paths.tick_tape_ledger
    ledger.parent.mkdir(parents=True, exist_ok=True)
    ledger.write_bytes(b'{"symbol": "A", "date": "2026-09-1"\n{"symbol": "B"}\n')
    cfg = CollectorSettings(data_root=tmp_path)
    summary = daemon_mod._repair_days_summary(paths, dt.date(2026, 9, 14), cfg, RepairSettings(hold_enabled=True))
    assert isinstance(summary, str)


def test_housekeeping_hold_enabled_counts_held(tmp_path, monkeypatch) -> None:
    import src.orchestration.daemon as daemon_mod
    from src.core.config import CollectorSettings, RepairSettings
    from src.core.paths import DataPaths
    from src.storage.retention import PruneStats

    paths = DataPaths(tmp_path)
    cfg = CollectorSettings(data_root=tmp_path)
    repair = RepairSettings(hold_enabled=True)
    calls: dict[str, object] = {}

    def _fake_maintenance(journal_root, **kw):
        calls.update(kw)
        return PruneStats(0, 0, held=frozenset({"l1/a/dt=2026-09-01.parquet"}))

    class _Offload:
        verified_remote_l1 = frozenset()
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


def test_business_eod_degrades_on_hold(tmp_path, monkeypatch) -> None:
    import datetime as dt_mod
    from zoneinfo import ZoneInfo

    import src.orchestration.daemon as daemon_mod
    from src.core.config import CollectorSettings, resolve_collector_runtime
    from src.orchestration.trading_day_gate import TradingDayStatus, TradingDayView
    from tests.unit.orchestration.daemon_fixtures import _business_trading_day

    def _runner():
        monkeypatch.chdir(tmp_path)
        settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
        runtime = resolve_collector_runtime(collector=settings)
        return daemon_mod.DaemonRunner(runtime=runtime, shutdown=None, now=lambda: None, sleep=lambda s: None)

    def _anchors(day):
        from src.core.session_anchors import standard_session_anchors

        return standard_session_anchors(day)

    day = dt_mod.date(2026, 9, 14)
    runner = _runner()
    object.__setattr__(runner._runtime, "repair", runner._runtime.repair.model_copy(update={"hold_enabled": True}))
    digests: list[tuple[str, str]] = []
    monkeypatch.setattr(
        daemon_mod,
        "_run_eod_housekeeping",
        lambda *a, **k: daemon_mod._EodHousekeeping(
            deleted=1, uploaded=0, purged=0, maintenance_ok=True, offload_ok=True, held=1
        ),
    )
    monkeypatch.setattr(daemon_mod, "_check_backup", lambda *a, **k: ([], True, "ok"))
    monkeypatch.setattr(daemon_mod, "send_digest", lambda subject, body: digests.append((subject, body)) or True)
    monkeypatch.setattr(runner, "_reconcile_regular_session", lambda *a, **k: (True, True))
    monkeypatch.setattr(daemon_mod, "_repair_days_summary", lambda *a, **k: "2026-09-12:ls:gapfill_pending")
    anchors = _anchors(day)
    view = TradingDayView(date=day, status=TradingDayStatus.BUSINESS, trading_day=_business_trading_day(day))
    now = dt_mod.datetime(2026, 9, 14, 20, 5, tzinfo=ZoneInfo("Asia/Seoul"))
    runner._run_business_eod(now, day, view, anchors)
    body = digests[0][1]
    assert "repair_hold" in body
    assert "repair_held=1" in body
    assert "status=DEGRADED" in body


def test_business_eod_ok_when_nothing_held(tmp_path, monkeypatch) -> None:
    import datetime as dt_mod
    from zoneinfo import ZoneInfo

    import src.orchestration.daemon as daemon_mod
    from src.core.config import CollectorSettings, resolve_collector_runtime
    from src.orchestration.trading_day_gate import TradingDayStatus, TradingDayView
    from tests.unit.orchestration.daemon_fixtures import _business_trading_day

    def _runner():
        monkeypatch.chdir(tmp_path)
        settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
        runtime = resolve_collector_runtime(collector=settings)
        return daemon_mod.DaemonRunner(runtime=runtime, shutdown=None, now=lambda: None, sleep=lambda s: None)

    def _anchors(day):
        from src.core.session_anchors import standard_session_anchors

        return standard_session_anchors(day)

    day = dt_mod.date(2026, 9, 14)
    runner = _runner()
    object.__setattr__(runner._runtime, "repair", runner._runtime.repair.model_copy(update={"hold_enabled": True}))
    digests: list[tuple[str, str]] = []
    monkeypatch.setattr(
        daemon_mod,
        "_run_eod_housekeeping",
        lambda *a, **k: daemon_mod._EodHousekeeping(
            deleted=1, uploaded=0, purged=0, maintenance_ok=True, offload_ok=True, held=0
        ),
    )
    monkeypatch.setattr(daemon_mod, "_check_backup", lambda *a, **k: ([], True, "ok"))
    monkeypatch.setattr(daemon_mod, "send_digest", lambda subject, body: digests.append((subject, body)) or True)
    monkeypatch.setattr(runner, "_reconcile_regular_session", lambda *a, **k: (True, True))
    monkeypatch.setattr(daemon_mod, "_repair_days_summary", lambda *a, **k: "none")
    anchors = _anchors(day)
    view = TradingDayView(date=day, status=TradingDayStatus.BUSINESS, trading_day=_business_trading_day(day))
    now = dt_mod.datetime(2026, 9, 14, 20, 5, tzinfo=ZoneInfo("Asia/Seoul"))
    runner._run_business_eod(now, day, view, anchors)
    body = digests[0][1]
    assert "repair_hold" not in body.split("degraded_reasons=")[1].splitlines()[0]
    assert "repair_days=none" in body


def test_repair_hold_for_returns_reason(tmp_path) -> None:
    import pathlib

    import src.orchestration.daemon as daemon_mod
    from src.core.config import RepairSettings
    from src.core.paths import DataPaths

    paths = DataPaths(tmp_path)
    day = dt.date(2026, 9, 12)
    target = paths.session_audit_path(day, "ls")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        __import__("json").dumps(
            _audit_doc(
                day,
                "ls",
                confirmed=1,
                symbols=[{"symbol": "005930", "confirmed_missing": 1, "intervals_truncated": 0, "intervals": []}],
            )
        ),
        encoding="utf-8",
    )
    hold = daemon_mod._repair_hold_for(
        paths, dt.date(2026, 9, 14), RepairSettings(hold_enabled=True), frozenset({("005930", day)})
    )
    assert hold(pathlib.Path("ls/krx/regular/H0STCNT0/dt=2026-09-12")) == "ls:gapfill_pending"


def test_holiday_eod_forwards_repair_settings(tmp_path, monkeypatch) -> None:
    import datetime as dt_mod

    import src.orchestration.daemon as daemon_mod
    from src.core.config import CollectorSettings, resolve_collector_runtime

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=__import__("pathlib").Path(tmp_path) / "data")
    runtime = resolve_collector_runtime(collector=settings)
    object.__setattr__(runtime, "repair", runtime.repair.model_copy(update={"hold_enabled": True}))
    runner = daemon_mod.DaemonRunner(runtime=runtime, shutdown=None, now=lambda: None, sleep=lambda s: None)
    seen: dict[str, object] = {}

    def _fake_housekeeping(cfg, paths, ref_day, *, progress, repair_settings=None):
        seen["repair_settings"] = repair_settings
        return daemon_mod._EodHousekeeping(deleted=0, uploaded=0, purged=0, maintenance_ok=True, offload_ok=True)

    monkeypatch.setattr(daemon_mod, "_run_eod_housekeeping", _fake_housekeeping)
    monkeypatch.setattr(daemon_mod, "_check_backup", lambda *a, **k: ([], True, "ok"))
    runner._run_holiday_eod(dt_mod.datetime(2026, 9, 14, 20, 5), dt_mod.date(2026, 9, 14))
    assert seen.get("repair_settings") is not None


@pytest.mark.parametrize("hold_enabled", [False, True])
def test_repair_summary_only_includes_persisted_vendor_days(tmp_path, hold_enabled) -> None:
    import json

    from src.core.config import CollectorSettings, RepairSettings
    from src.orchestration.daemon import _repair_days_summary

    cfg = CollectorSettings(data_root=tmp_path)
    paths = cfg.paths
    ref_day, gap_day = dt.date(2026, 9, 14), dt.date(2026, 9, 10)
    settings = RepairSettings(hold_enabled=hold_enabled)
    assert _repair_days_summary(paths, ref_day, cfg, settings) == "none"
    partition = paths.journal_root / "ls/krx/regular/H0STCNT0/dt=2026-09-10"
    partition.mkdir(parents=True)
    assert _repair_days_summary(paths, ref_day, cfg, settings) == "2026-09-10:ls:unaudited"
    audit_path = paths.session_audit_path(gap_day, "ls")
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    audit_path.write_text(json.dumps(_audit_doc(gap_day, "ls")))
    assert _repair_days_summary(paths, ref_day, cfg, settings) == "none"


def test_real_housekeeping_preserves_held_l0_and_excludes_remote_purge(tmp_path, monkeypatch) -> None:
    import json

    import zstandard as zstd

    import src.orchestration.daemon as daemon_mod
    import src.orchestration.eod as eod_mod
    from src.core.config import CollectorSettings, RepairSettings
    from src.orchestration.eod import EodOffloadResult
    from src.storage.normalization import normalize_l0_partition
    from src.storage.remote import SyncStats

    cfg = CollectorSettings(data_root=tmp_path)
    paths = cfg.paths
    rel = pathlib.Path("ls/krx/regular/H0STCNT0/dt=2026-09-10")
    part = paths.journal_root / rel
    part.mkdir(parents=True)
    record = {"raw": "a", "recv_mono_ns": 1, "recv_wall_ns": 2, "conn_id": "c1", "conn_seq": 1, "vendor": "ls", "tr_id": "H0STCNT0"}
    (part / "09.jsonl.zst").write_bytes(zstd.ZstdCompressor().compress((json.dumps(record) + "\n").encode()))
    repo_path = "l1/" + rel.as_posix() + ".parquet"
    monkeypatch.setattr(eod_mod, "run_isolated_normalize", lambda p, o, **kw: normalize_l0_partition(p, o))
    monkeypatch.setattr(daemon_mod, "check_disk_watermark", lambda *a, **kw: True)

    def offload(*a, **kw):
        assert (paths.archive_root / (rel.as_posix() + ".parquet")).is_file()
        return EodOffloadResult(l1=SyncStats(), manifests=SyncStats(), verified_remote_l1=frozenset({repo_path}))

    purged_sets = []
    monkeypatch.setattr(daemon_mod, "run_eod_offload", offload)
    monkeypatch.setattr(daemon_mod, "run_eod_remote_l0_purge", lambda root, verified, **kw: purged_sets.append(verified))
    result = daemon_mod._run_eod_housekeeping(
        cfg, paths, dt.date(2026, 9, 14), progress=lambda: None, repair_settings=RepairSettings(hold_enabled=True)
    )
    assert result.held == 1
    assert result.deleted == 0
    assert (part / "09.jsonl.zst").is_file()
    assert purged_sets == [frozenset()]
