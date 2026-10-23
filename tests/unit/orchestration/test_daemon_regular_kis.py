"""Invariant guards for KIS regular-shard daemon wiring (part4)."""

from __future__ import annotations

import datetime as dt
import logging
from zoneinfo import ZoneInfo

import pytest

import src.orchestration.daemon as daemon_mod
from src.orchestration.trading_day_gate import TradingDayStatus, TradingDayView

KST = ZoneInfo("Asia/Seoul")
DAY = dt.date(2026, 9, 16)


@pytest.fixture(autouse=True)
def _kis_data_pool(monkeypatch) -> None:
    monkeypatch.setenv("KIS_DATA_SLOTS", "1,2,3")
    for slot in ("1", "2", "3"):
        monkeypatch.setenv(f"KIS_DATA_{slot}_APP_KEY", f"test-regular-app-key-{slot}")
        monkeypatch.setenv(f"KIS_DATA_{slot}_APP_SECRET", f"test-regular-app-secret-{slot}")
        monkeypatch.setenv(f"KIS_DATA_{slot}_HTS_ID", f"test-regular-hts-id-{slot}")


@pytest.fixture(autouse=True)
def _verified_regular_kis_capacity(monkeypatch) -> None:
    monkeypatch.setenv("KRX_ALPHA_REGULAR_KIS_PAIR_CAPACITY_PER_CONNECTION", "41")


class _FakeSupervisor:
    def __init__(self, *, cmd, breaker=None):
        self.cmd = list(cmd)
        self.last_exit_code = None
        self.started_at_wall = None
        self._running = False
        self.stop_calls: list = []
        self.restarts: list = []
        self._script: list = []

    def ensure_running(self):
        if self._script:
            result = self._script.pop(0)
        elif self._running:
            result = "running"
        else:
            result = "started"
        if result == "started":
            self._running = True
        elif result == "restarted":
            self.last_exit_code = 1
        return result

    def is_running(self):
        return self._running

    def stop(self, *, timeout_s=15.0):
        self.stop_calls.append(timeout_s)
        self._running = False
        return "graceful"

    def stop_for_restart(self, *, reason=None, timeout_s=15.0):
        self.restarts.append(reason)
        self._running = False
        return "graceful"


class _FakeGate:
    def __init__(self, status):
        self.status = status

    def view(self, today, now):
        return TradingDayView(date=today, status=self.status, trading_day=None)


def _at(day, hour, minute, second=0):
    return dt.datetime(day.year, day.month, day.day, hour, minute, second, tzinfo=KST)


def _anchors(day):
    from src.core.session_anchors import standard_session_anchors

    return standard_session_anchors(day)


def _runner(monkeypatch, tmp_path, *, enabled=True, gate_status=TradingDayStatus.BUSINESS, **rk_kwargs):
    from src.core.config import CollectorSettings, RegularKisSettings, resolve_collector_runtime

    kwargs = {"credential_slots": ("1", "2", "3"), "pair_capacity_per_connection": 2}
    kwargs.update(rk_kwargs)
    rk = RegularKisSettings(enabled=enabled, **kwargs)
    settings = CollectorSettings(data_root=tmp_path / "data")
    runtime = resolve_collector_runtime(collector=settings, regular_kis=rk)
    runner = daemon_mod.DaemonRunner(runtime=runtime, shutdown=None, now=lambda: None, sleep=lambda s: None)
    runner._gate = _FakeGate(gate_status)
    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)
    return runner


def _write_candidates(paths, symbols) -> None:
    from src.universe.ipc import write_candidates

    paths.candidates.parent.mkdir(parents=True, exist_ok=True)
    write_candidates(
        paths.candidates,
        [{"symbol": symbol, "selection_reasons": ["limit_up"]} for symbol in symbols],
        rev=20260915,
    )


def _ready_plan(runner, symbols=("005930", "000660", "000020")) -> None:
    _write_candidates(runner._paths, symbols)
    runner.state.orchestrated_for = DAY


def test_children_set_and_stop_reject_regular_kis() -> None:
    children = daemon_mod.DaemonChildren()
    with pytest.raises(ValueError, match="regular_kis shards live in the regular_kis mapping"):
        children.set(daemon_mod.StreamChildKey.REGULAR_KIS, object())  # type: ignore[arg-type]
    import pathlib

    from src.core.config import CollectorSettings, resolve_collector_runtime

    runtime = resolve_collector_runtime(collector=CollectorSettings(data_root=pathlib.Path("data")))
    runner = daemon_mod.DaemonRunner(runtime=runtime, shutdown=None, now=lambda: None, sleep=lambda s: None)
    with pytest.raises(ValueError, match="regular_kis shards are stopped"):
        runner._stop_stream_child(daemon_mod.StreamChildKey.REGULAR_KIS)


def test_disabled_spawns_nothing(monkeypatch, tmp_path) -> None:
    runner = _runner(monkeypatch, tmp_path, enabled=False)
    _ready_plan(runner)
    for hour, minute in ((8, 0), (10, 0), (15, 0), (16, 0)):
        runner._supervise_regular_kis(_at(DAY, hour, minute), _anchors(DAY))
    assert runner.children.regular_kis == {}
    assert runner.state.regular_kis_plan == ()


def test_daemon_assigns_ls_omissions_before_priority_overlap(monkeypatch, tmp_path, caplog) -> None:
    from src.core.config import CollectorSettings, resolve_collector_runtime
    from src.universe.ipc import write_candidates

    runner = _runner(monkeypatch, tmp_path)
    collector = CollectorSettings(data_root=tmp_path / "data", ls_max_subscriptions_per_session=4)
    runner._runtime = resolve_collector_runtime(collector=collector, regular_kis=runner._runtime.regular_kis)
    rows = [
        {"symbol": "000005", "tier": "follow"},
        {"symbol": "000004", "tier": "event"},
        {"symbol": "000003", "tier": "core"},
        {"symbol": "000002", "tier": "core"},
        {"symbol": "000001", "tier": "core"},
    ]
    runner._paths.candidates.parent.mkdir(parents=True, exist_ok=True)
    write_candidates(runner._paths.candidates, rows, rev=20260915)
    runner.state.orchestrated_for = DAY
    with caplog.at_level(logging.INFO):
        runner._ensure_regular_kis_plan(DAY)
    assert tuple(s for shard in runner.state.regular_kis_plan for s in shard.symbols) == (
        "000001", "000004", "000005",
    )
    assert "ls_covered=2 kis_planned=3 kis_exclusive=3 uncovered=0" in caplog.text


def test_window_start_and_stop(monkeypatch, tmp_path) -> None:
    runner = _runner(monkeypatch, tmp_path)
    _ready_plan(runner)
    runner._supervise_regular_kis(_at(DAY, 8, 27, 59), _anchors(DAY))
    assert runner.children.regular_kis == {}
    runner._supervise_regular_kis(_at(DAY, 8, 28, 0), _anchors(DAY))
    assert sorted(runner.children.regular_kis) == ["krx-00", "krx-01", "krx-02"]
    for sup in runner.children.regular_kis.values():
        assert sup.is_running()
        assert "collect-regular-kis" in sup.cmd
    supervisors = tuple(runner.children.regular_kis.values())
    runner._supervise_regular_kis(_at(DAY, 15, 32, 0), _anchors(DAY))
    assert runner.children.regular_kis == {}
    assert all(sup.stop_calls for sup in supervisors)
    assert not any(sup.is_running() for sup in supervisors)


def test_no_plan_from_degraded_candidates(monkeypatch, tmp_path, caplog) -> None:
    runner = _runner(monkeypatch, tmp_path)
    _ready_plan(runner)
    runner.state.degraded_active = True
    runner.state.orchestrated_for = None
    with caplog.at_level(logging.ERROR, logger=daemon_mod.logger.name):
        runner._ensure_regular_kis_plan(DAY)
    assert runner.state.regular_kis_plan == ()
    assert runner.children.regular_kis == {}
    skips = [r for r in caplog.records if "stage=regular_kis_plan" in r.getMessage()]
    assert len(skips) == 1
    assert "SKIP" in skips[0].getMessage()


def test_ready_upgrade_can_plan_after_degraded_skip(monkeypatch, tmp_path) -> None:
    runner = _runner(monkeypatch, tmp_path)
    _ready_plan(runner)
    runner._install_stream_child(daemon_mod.StreamChildKey.REGULAR, ["collect-stream"], degraded=True)
    runner.state.orchestrated_for = None
    runner._ensure_regular_kis_plan(DAY)
    assert runner.state.regular_kis_plan == ()
    runner._start_ready_children(DAY)
    runner._ensure_regular_kis_plan(DAY)
    assert len(runner.state.regular_kis_plan) == 3
    assert runner.state.degraded_active is False


def test_holiday_stops_shards(monkeypatch, tmp_path) -> None:
    runner = _runner(monkeypatch, tmp_path)
    _ready_plan(runner)
    runner._supervise_regular_kis(_at(DAY, 8, 28), _anchors(DAY))
    assert len(runner.children.regular_kis) == 3
    supervisors = tuple(runner.children.regular_kis.values())
    assert all(sup.is_running() for sup in supervisors)
    runner._gate = _FakeGate(TradingDayStatus.HOLIDAY)
    runner._supervise_regular_kis(_at(DAY, 10, 30), _anchors(DAY))
    assert runner.children.regular_kis == {}
    assert all(sup.stop_calls for sup in supervisors)


def test_aftermarket_deferral_on_busy_slot(monkeypatch, tmp_path, caplog) -> None:
    from src.core.config import AftermarketSettings, CollectorSettings, RegularKisSettings, resolve_collector_runtime
    from src.realtime.contracts import MarketVenue
    from src.realtime.kis_sharding import AftermarketShard

    rk = RegularKisSettings(enabled=True, credential_slots=("2",), pair_capacity_per_connection=41)
    settings = CollectorSettings(data_root=tmp_path / "data", after_market_enabled=True)
    after = AftermarketSettings(enabled=True, pair_capacity_per_connection=4)
    runtime = resolve_collector_runtime(collector=settings, regular_kis=rk, aftermarket=after)
    runner = daemon_mod.DaemonRunner(runtime=runtime, shutdown=None, now=lambda: None, sleep=lambda s: None)
    runner._gate = _FakeGate(TradingDayStatus.BUSINESS)
    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)
    streams = ("H0STCNT0", "H0STASP0")
    runner.state.regular_kis_plan = (AftermarketShard(MarketVenue.KRX, 0, ("005930",), streams, "2", "id2"),)
    runner.state.regular_kis_plan_day = DAY
    live = _FakeSupervisor(cmd=["collect-regular-kis"], breaker=None)
    live._running = True
    runner.children.regular_kis["krx-00"] = live
    nxt_streams = ("H0NXCNT0", "H0NXASP0")
    runner.state.aftermarket_plan = (AftermarketShard(MarketVenue.NXT, 0, ("005930",), nxt_streams, "2", "id2"),)
    runner.state.aftermarket_plan_day = DAY
    runner.state.aftermarket_refresh_day = DAY
    runner.state.aftermarket_nxt_refresh_day = DAY
    now = _at(DAY, 15, 39, 0)
    with caplog.at_level(logging.WARNING, logger=daemon_mod.logger.name):
        runner._supervise_aftermarket(now, daemon_mod.SessionState.FULL_ACTIVE, DAY, _anchors(DAY))
    assert runner.children.aftermarket == {}
    deferred = [r for r in caplog.records if "slot_busy_regular_kis" in r.getMessage()]
    assert len(deferred) == 1
    caplog.clear()
    live._running = False
    with caplog.at_level(logging.WARNING, logger=daemon_mod.logger.name):
        runner._supervise_aftermarket(now, daemon_mod.SessionState.FULL_ACTIVE, DAY, _anchors(DAY))
    assert sorted(runner.children.aftermarket) == ["nxt:0"]
    assert not [r for r in caplog.records if "slot_busy_regular_kis" in r.getMessage()]


def test_circuit_alert_once_per_shard_per_day(monkeypatch, tmp_path, caplog) -> None:
    runner = _runner(monkeypatch, tmp_path, credential_slots=("1",))
    _ready_plan(runner, symbols=("005930",))
    now = _at(DAY, 10, 0)
    runner._supervise_regular_kis(now, _anchors(DAY))
    assert sorted(runner.children.regular_kis) == ["krx-00"]
    runner.children.regular_kis["krx-00"]._script = ["circuit_open", "circuit_open"]
    with caplog.at_level(logging.CRITICAL, logger=daemon_mod.logger.name):
        runner._supervise_regular_kis(now, _anchors(DAY))
        runner._supervise_regular_kis(now, _anchors(DAY))
    alerts = [r for r in caplog.records if "circuit_open" in r.getMessage()]
    assert len(alerts) == 1
    assert "krx-00" in alerts[0].getMessage()


def _ok_audit(day):
    from src.storage.l0_audit import AuditStatus, LedgerTotals, SessionAudit

    return SessionAudit(
        schema_version=1,
        session_date=day,
        vendor="ls",
        venue="krx",
        session="regular",
        status=AuditStatus.OK,
        reason="ok",
        generated_at_ns=1,
        sources=(),
        latency=(),
        symbols=(),
        totals=LedgerTotals(
            symbols=0,
            symbols_with_loss=0,
            symbols_without_counter=0,
            trades_observed=1,
            confirmed_missing=0,
            count_only_missing=0,
            net_missing_positive=0,
            volume_deficit=0,
            loss_ratio=0.0,
        ),
        tail_silence_s=None,
        last_regular_tick_recv_ns=None,
    )


def _eod_runner(monkeypatch, tmp_path, *, ref_day, enabled, digests):
    from src.core.config import CollectorSettings, RegularKisSettings, resolve_collector_runtime

    rk = RegularKisSettings(enabled=enabled, credential_slots=("1",), pair_capacity_per_connection=41)
    settings = CollectorSettings(data_root=tmp_path / "data")
    runtime = resolve_collector_runtime(collector=settings, regular_kis=rk)
    runner = daemon_mod.DaemonRunner(
        runtime=runtime, shutdown=None, now=lambda: _at(ref_day, 20, 30), sleep=lambda s: None
    )
    runner._gate = _FakeGate(TradingDayStatus.BUSINESS)
    monkeypatch.setattr(
        daemon_mod,
        "_run_eod_housekeeping",
        lambda *a, **k: daemon_mod._EodHousekeeping(
            deleted=0, uploaded=0, purged=0, maintenance_ok=True, offload_ok=True
        ),
    )
    monkeypatch.setattr(daemon_mod, "_check_backup", lambda *a, **k: ([], True, "ok"))
    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", lambda **k: True)
    return runner


def test_eod_audit_never_alters_status(monkeypatch, tmp_path, caplog) -> None:
    ref_day = DAY
    anchors = _anchors(ref_day)
    day = TradingDayView(date=ref_day, status=TradingDayStatus.BUSINESS, trading_day=None)
    digests: list = []
    monkeypatch.setattr(daemon_mod, "send_digest", lambda subject, body: digests.append((subject, body)))
    runners = {}
    for enabled in (True, False):
        runners[enabled] = _eod_runner(monkeypatch, tmp_path, ref_day=ref_day, enabled=enabled, digests=digests)

    def _derive(journal_root, **kwargs):
        if kwargs.get("vendor") == "kis":
            raise OSError("boom")
        return _ok_audit(ref_day)

    monkeypatch.setattr(daemon_mod, "derive_session_audit", _derive)
    statuses = {}
    with caplog.at_level(logging.ERROR, logger=daemon_mod.logger.name):
        for enabled, runner in runners.items():
            before = len(digests)
            runner._run_business_eod(_at(ref_day, 20, 30), ref_day, day, anchors)
            statuses[enabled] = digests[before][1]
    assert "status=OK" in statuses[True]
    assert statuses[True].split("status=")[1].split()[0] == statuses[False].split("status=")[1].split()[0]
    errors = [r for r in caplog.records if "stage=regular_kis_audit" in r.getMessage()]
    assert len(errors) == 1


def test_generic_l0_lifecycle_covers_kis_regular(tmp_path, monkeypatch) -> None:
    import json

    import zstandard as zstd
    import src.orchestration.eod as eod_mod
    from src.core.config import CollectorSettings
    from src.storage.remote import PurgeStats, SyncStats

    old = dt.date(2026, 9, 1)
    part = tmp_path / "l0" / "kis" / "krx" / "regular" / "H0STCNT0" / f"dt={old.isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    rec = {
        "raw": "005930^090000",
        "recv_mono_ns": 1,
        "recv_wall_ns": 2,
        "conn_id": "c",
        "conn_seq": 1,
        "vendor": "kis",
        "tr_id": "H0STCNT0",
        "venue": "krx",
        "session": "regular",
        "stream": "H0STCNT0",
        "symbol": "005930",
        "exchange_event_time": "090000",
    }
    (part / "09.s0.jsonl.zst").write_bytes(
        zstd.ZstdCompressor(level=3).compress((json.dumps(rec) + "\n").encode())
    )
    archive_root = tmp_path / "l1"
    rel = f"l1/kis/krx/regular/H0STCNT0/dt={old.isoformat()}.parquet"
    produced = archive_root / "kis" / "krx" / "regular" / "H0STCNT0" / f"dt={old.isoformat()}.parquet"
    events = []
    remote_bytes = {}

    class _FakeArchiver:
        def sync_l1_tree(self, root, *, progress=None):
            assert part.is_dir()
            assert produced.is_file()
            remote_bytes[rel] = produced.read_bytes()
            events.append("offload")
            return SyncStats(uploaded=1)

        def sync_manifest_tree(self, root):
            return SyncStats()

        def remote_file_sizes(self, prefix):
            return {key: len(value) for key, value in remote_bytes.items()}

        def remote_files(self, prefix):
            return set(remote_bytes)

        def purge_superseded_l0(self, verified, journal_root, *, progress=None):
            assert rel in verified
            assert not part.exists()
            events.append("purge")
            return PurgeStats(purged=1)

    monkeypatch.setattr(eod_mod.GDriveArchiver, "try_from_env", lambda: _FakeArchiver())
    cfg = CollectorSettings(data_root=tmp_path, min_free_disk_gb=0)
    result = daemon_mod._run_eod_housekeeping(
        cfg, cfg.paths, dt.date(2026, 9, 8), progress=lambda: None,
    )
    assert result.maintenance_ok
    assert result.offload_ok
    assert result.deleted == 1
    assert result.uploaded == 1
    assert produced.read_bytes() == remote_bytes[rel]
    assert events == ["offload", "purge"]


def test_plan_skipped_when_candidates_not_ready(monkeypatch, tmp_path) -> None:
    runner = _runner(monkeypatch, tmp_path)
    runner.state.orchestrated_for = DAY
    runner._ensure_regular_kis_plan(DAY)
    assert runner.state.regular_kis_plan_day is None
    assert runner.state.regular_kis_plan == ()


@pytest.mark.parametrize("payload", [
    [],
    {"rev": 20260915, "candidates": [{"n": 1}]},
    {"rev": 20260915, "candidates": [{"symbol": 5930}]},
])
def test_plan_skipped_on_invalid_candidates(monkeypatch, tmp_path, caplog, payload) -> None:
    import json

    runner = _runner(monkeypatch, tmp_path)
    runner.state.orchestrated_for = DAY
    runner._paths.candidates.parent.mkdir(parents=True, exist_ok=True)
    runner._paths.candidates.write_text(json.dumps(payload))
    with caplog.at_level(logging.ERROR, logger=daemon_mod.logger.name):
        runner._ensure_regular_kis_plan(DAY)
    assert runner.state.regular_kis_plan == ()
    skips = [r for r in caplog.records if "stage=regular_kis_plan" in r.getMessage()]
    assert len(skips) == 1
    assert "SKIP" in skips[0].getMessage()


def test_restart_logged(monkeypatch, tmp_path, caplog) -> None:
    runner = _runner(monkeypatch, tmp_path, credential_slots=("1",))
    _ready_plan(runner, symbols=("005930",))
    now = _at(DAY, 10, 0)
    runner._supervise_regular_kis(now, _anchors(DAY))
    runner.children.regular_kis["krx-00"]._script = ["restarted"]
    with caplog.at_level(logging.WARNING, logger=daemon_mod.logger.name):
        runner._supervise_regular_kis(now, _anchors(DAY))
    assert [r for r in caplog.records if "stage=regular_kis_stream" in r.getMessage() and "RESTARTED" in r.getMessage()]


def test_supervise_failure_is_isolated(monkeypatch, tmp_path, caplog) -> None:
    from src.core.errors import KrxAlphaError

    runner = _runner(monkeypatch, tmp_path)
    _ready_plan(runner)
    monkeypatch.setattr(
        daemon_mod, "regular_kis_collection_due", lambda *a, **k: (_ for _ in ()).throw(KrxAlphaError("boom"))
    )
    with caplog.at_level(logging.WARNING, logger=daemon_mod.logger.name):
        runner._supervise_regular_kis(_at(DAY, 10, 0), _anchors(DAY))
    assert [r for r in caplog.records if "regular_kis_supervise" in r.getMessage()]
    assert runner.children.regular_kis == {}


def test_watch_guards_cover_disabled_empty_and_unplanned(monkeypatch, tmp_path, caplog) -> None:
    from src.core.config import CollectorSettings, RegularKisSettings, ShardWatchdogSettings, resolve_collector_runtime

    now = _at(DAY, 10, 0)
    anchors = _anchors(DAY)
    rk = RegularKisSettings(enabled=True, credential_slots=("1",), pair_capacity_per_connection=2)
    settings = CollectorSettings(data_root=tmp_path / "data")
    off = resolve_collector_runtime(
        collector=settings, regular_kis=rk, shard_watchdog=ShardWatchdogSettings(enabled=False)
    )
    runner = daemon_mod.DaemonRunner(runtime=off, shutdown=None, now=lambda: None, sleep=lambda s: None)
    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)
    sup = _FakeSupervisor(cmd=["collect-regular-kis"], breaker=None)
    sup._running = True
    runner.children.regular_kis["krx-00"] = sup
    runner._watch_regular_kis_shards(now, DAY, anchors)
    assert runner.children.regular_kis != {}

    runner = _runner(monkeypatch, tmp_path, credential_slots=("1",))
    _ready_plan(runner, symbols=("005930",))
    runner._watch_regular_kis_shards(now, DAY, anchors)
    assert runner.children.regular_kis == {}

    runner.state.regular_kis_plan_day = DAY
    stray = _FakeSupervisor(cmd=["collect-regular-kis"], breaker=None)
    stray._running = True
    runner.children.regular_kis["krx-99"] = stray
    runner._watch_regular_kis_shards(now, DAY, anchors)
    assert runner.children.regular_kis != {}


def test_watch_error_is_contained(monkeypatch, tmp_path, caplog) -> None:
    runner = _runner(monkeypatch, tmp_path, credential_slots=("1",))
    _ready_plan(runner, symbols=("005930",))
    runner._supervise_regular_kis(_at(DAY, 10, 0), _anchors(DAY))
    assert runner.children.regular_kis != {}
    monkeypatch.setattr(daemon_mod, "shard_window", lambda *a, **k: (_ for _ in ()).throw(OSError("boom")))
    with caplog.at_level(logging.WARNING, logger=daemon_mod.logger.name):
        runner._watch_regular_kis_shards(_at(DAY, 10, 0), DAY, _anchors(DAY))
    assert [r for r in caplog.records if "stage=shard_watchdog" in r.getMessage()]


def test_stop_regular_kis_children_clears_handles(monkeypatch, tmp_path) -> None:
    runner = _runner(monkeypatch, tmp_path, credential_slots=("1",))
    _ready_plan(runner, symbols=("005930",))
    runner._supervise_regular_kis(_at(DAY, 8, 28, 0), _anchors(DAY))
    sup = runner.children.regular_kis["krx-00"]
    assert sup.is_running()
    runner.state.regular_kis_circuit_alerted.add("krx-00")
    runner._stop_regular_kis_children(clear_circuit_alerts=False)
    assert runner.children.regular_kis == {}
    assert runner.state.regular_kis_circuit_alerted == {"krx-00"}
    runner.children.regular_kis["krx-00"] = sup
    runner._stop_regular_kis_children(clear_circuit_alerts=True)
    assert runner.children.regular_kis == {}
    assert runner.state.regular_kis_circuit_alerted == set()
    assert sup.stop_calls


def test_kis_audit_skipped_when_audit_off(monkeypatch, tmp_path) -> None:
    from src.core.config import AuditMode, AuditSettings, CollectorSettings, RegularKisSettings, resolve_collector_runtime

    rk = RegularKisSettings(enabled=True, credential_slots=("1",), pair_capacity_per_connection=2)
    settings = CollectorSettings(data_root=tmp_path / "data")
    runtime = resolve_collector_runtime(
        collector=settings, regular_kis=rk, audit=AuditSettings(mode=AuditMode.OFF)
    )
    runner = daemon_mod.DaemonRunner(runtime=runtime, shutdown=None, now=lambda: None, sleep=lambda s: None)
    day = TradingDayView(date=DAY, status=TradingDayStatus.BUSINESS, trading_day=None)
    assert runner._derive_kis_regular_audit(DAY, day, _anchors(DAY)) is None


def test_kis_audit_write_fail_still_compares(monkeypatch, tmp_path, caplog) -> None:
    runner = _runner(monkeypatch, tmp_path, credential_slots=("1",))
    day = TradingDayView(date=DAY, status=TradingDayStatus.BUSINESS, trading_day=None)
    monkeypatch.setattr(daemon_mod, "derive_session_audit", lambda *a, **k: _ok_audit(DAY))
    monkeypatch.setattr(daemon_mod, "write_session_audit", lambda *a, **k: (_ for _ in ()).throw(OSError("boom")))
    with caplog.at_level(logging.INFO, logger=daemon_mod.logger.name):
        audit = runner._derive_kis_regular_audit(DAY, day, _anchors(DAY), ls_audit=_ok_audit(DAY))
    assert audit is not None
    assert [r for r in caplog.records if "WRITE_FAIL" in r.getMessage()]
    assert [r for r in caplog.records if "stage=regular_kis_compare" in r.getMessage()]


def test_plan_waits_for_today_and_contains_corrupt_file_once(monkeypatch, tmp_path, caplog) -> None:
    runner = _runner(monkeypatch, tmp_path)
    runner._ensure_regular_kis_plan(DAY)
    assert runner.state.regular_kis_plan_day is None
    runner.state.orchestrated_for = DAY
    runner._paths.candidates.parent.mkdir(parents=True, exist_ok=True)
    runner._paths.candidates.write_text("{", encoding="utf-8")
    with caplog.at_level(logging.ERROR, logger=daemon_mod.logger.name):
        runner._ensure_regular_kis_plan(DAY)
        runner._ensure_regular_kis_plan(DAY)
    assert runner.state.regular_kis_plan == ()
    assert len([r for r in caplog.records if "regular_kis_plan" in r.message]) == 1


def test_rollover_stops_previous_day_before_replanning(monkeypatch, tmp_path) -> None:
    runner = _runner(monkeypatch, tmp_path)
    _ready_plan(runner)
    runner._supervise_regular_kis(_at(DAY, 8, 28), _anchors(DAY))
    supervisors = tuple(runner.children.regular_kis.values())
    runner.state.regular_kis_circuit_alerted.add("krx-00")
    runner._reset_for_new_session_day(DAY + dt.timedelta(days=1))
    assert not any(sup.is_running() for sup in supervisors)
    assert all(sup.stop_calls for sup in supervisors)
    assert runner.children.regular_kis == {}
    assert runner.state.regular_kis_plan == ()
    assert runner.state.regular_kis_plan_day is None
    assert not runner.state.regular_kis_circuit_alerted


def test_eod_stops_shards_even_when_window_end_cycle_was_missed(monkeypatch, tmp_path) -> None:
    runner = _runner(monkeypatch, tmp_path)
    _ready_plan(runner)
    runner._supervise_regular_kis(_at(DAY, 8, 28), _anchors(DAY))
    supervisors = tuple(runner.children.regular_kis.values())
    runner.state.eod_attempted_for = DAY
    runner._step_post_market_eod(_at(DAY, 20, 5), None, _anchors(DAY))
    assert not any(sup.is_running() for sup in supervisors)
    assert all(sup.stop_calls for sup in supervisors)
    assert runner.children.regular_kis == {}


def test_comparison_persists_kis_audit_and_limits_open_latency(monkeypatch, tmp_path, caplog) -> None:
    from dataclasses import replace

    from src.storage.l0_audit import AuditStatus, LatencyMinute, StreamLatency

    runner = _runner(monkeypatch, tmp_path)
    anchors = _anchors(DAY)
    day = TradingDayView(date=DAY, status=TradingDayStatus.BUSINESS, trading_day=None)
    latency = StreamLatency("H0STCNT0", AuditStatus.OK, "ok", 3, 3, (
        LatencyMinute("09:00", 1, 2.0, 2.0, 2.0),
        LatencyMinute("09:04", 1, 4.0, 4.0, 4.0),
        LatencyMinute("09:05", 1, 99.0, 99.0, 99.0),
    ))
    audit = replace(_ok_audit(DAY), vendor="kis", latency=(latency,))
    calls = []

    def derive(root, **kwargs):
        calls.append(kwargs)
        return audit

    monkeypatch.setattr(daemon_mod, "derive_session_audit", derive)
    with caplog.at_level(logging.INFO, logger=daemon_mod.logger.name):
        assert runner._derive_kis_regular_audit(DAY, day, anchors, ls_audit=audit) is audit
    assert runner._paths.session_audit_path(DAY, "kis").is_file()
    assert calls[0]["vendor"] == "kis"
    assert calls[0]["manifest_frame_counts"] is None
    comparison = next(r.message for r in caplog.records if "regular_kis_compare" in r.message)
    assert "kis_open_max_lag_s=4.0" in comparison
    assert "ls_open_max_lag_s=4.0" in comparison


def test_kis_audit_uses_its_own_time_budget(monkeypatch, tmp_path) -> None:
    from src.core.config import AuditSettings, CollectorSettings, RegularKisSettings, resolve_collector_runtime

    rk = RegularKisSettings(enabled=True, credential_slots=("1",), pair_capacity_per_connection=2)
    runtime = resolve_collector_runtime(
        collector=CollectorSettings(data_root=tmp_path / "data"),
        regular_kis=rk,
        audit=AuditSettings(time_budget_s=1800.0, kis_time_budget_s=123.0),
    )
    runner = daemon_mod.DaemonRunner(runtime=runtime, shutdown=None, now=lambda: None, sleep=lambda s: None)
    day = TradingDayView(date=DAY, status=TradingDayStatus.BUSINESS, trading_day=None)
    seen: list[float] = []

    def _capture(*args, **kwargs):
        seen.append(kwargs["settings"].time_budget_s)
        return _ok_audit(DAY)

    monkeypatch.setattr(daemon_mod, "derive_session_audit", _capture)
    monkeypatch.setattr(daemon_mod, "write_session_audit", lambda *a, **k: None)
    runner._derive_kis_regular_audit(DAY, day, _anchors(DAY), ls_audit=_ok_audit(DAY))
    assert seen == [123.0]
    assert runtime.audit.time_budget_s == 1800.0
