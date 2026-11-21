"""DB crosscheck daemon child invariant tests."""

from __future__ import annotations

import datetime as dt
import logging
from zoneinfo import ZoneInfo

import pytest

_KST = ZoneInfo("Asia/Seoul")


@pytest.fixture(autouse=True)
def _verified_aftermarket_capacity(monkeypatch) -> None:
    monkeypatch.setenv("KRX_ALPHA_AFTERMARKET_PAIR_CAPACITY_PER_CONNECTION", "4")


def _runner(tmp_path, monkeypatch, **settings_kw):
    import pathlib

    from src.core.config import CollectorSettings, resolve_collector_runtime
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    db_enabled = settings_kw.pop("db_feeds_enabled", False)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data", **settings_kw)
    feed_control = _db_feed_control() if db_enabled else None
    runtime = resolve_collector_runtime(collector=settings, feed_control=feed_control)
    return daemon_mod, daemon_mod.DaemonRunner(runtime=runtime, shutdown=None, now=lambda: None, sleep=lambda s: None)


def _db_feed_control():
    from src.core.config import FeedControlSettings

    return FeedControlSettings(watch_groups=("ls:0",), secondary_feeds=("db:0", "db:1"))


def _anchors(day):
    from src.core.session_anchors import standard_session_anchors

    return standard_session_anchors(day)


def test_db_crosscheck_not_installed_when_disabled(tmp_path, monkeypatch) -> None:
    daemon_mod, runner = _runner(tmp_path, monkeypatch)
    day = dt.date(2026, 9, 14)
    runner._paths.candidates.parent.mkdir(parents=True, exist_ok=True)
    from src.universe.ipc import write_candidates

    write_candidates(runner._paths.candidates, [{"symbol": "005930", "selection_reasons": ["limit_up"]}], rev=1)
    runner._start_ready_children(day)
    assert runner.children.db_crosscheck is None
    assert runner.children.regular is not None


def test_db_crosscheck_installed_with_own_manifest_cmd(tmp_path, monkeypatch) -> None:
    daemon_mod, runner = _runner(tmp_path, monkeypatch, db_feeds_enabled=True)
    day = dt.date(2026, 9, 14)
    created: list = []

    class _FakeSup:
        def __init__(self, *, cmd, breaker=None):
            self.cmd = list(cmd)
            created.append(self)

        def ensure_running(self):
            return "started"

        def stop(self, *, timeout_s=15.0):
            return "graceful"

    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSup)
    runner._paths.candidates.parent.mkdir(parents=True, exist_ok=True)
    from src.universe.ipc import write_candidates

    write_candidates(runner._paths.candidates, [{"symbol": "005930", "selection_reasons": ["limit_up"]}], rev=1)
    runner._start_ready_children(day)
    assert runner.children.db_crosscheck is not None
    cmd = runner.children.db_crosscheck.cmd
    assert "collect-stream-db" in cmd
    assert any(str(c).endswith(".db.json") for c in cmd)
    direct = daemon_mod._db_crosscheck_cmd(day, runner._paths)
    assert "collect-stream-db" in direct
    assert str(runner._paths.db_crosscheck_manifest_path(day)) in direct


def test_db_crosscheck_install_replaces_live_predecessor(tmp_path, monkeypatch, caplog) -> None:
    daemon_mod, runner = _runner(tmp_path, monkeypatch, db_feeds_enabled=True)
    stops: list[float] = []

    class _Old:
        def stop(self, *, timeout_s=15.0):
            stops.append(timeout_s)
            return "graceful"

    runner.children.db_crosscheck = _Old()  # type: ignore[assignment]
    with caplog.at_level(logging.CRITICAL):
        runner._install_stream_child(daemon_mod.StreamChildKey.DB_CROSSCHECK, ["cmd"])
    assert stops == [15.0]
    assert runner.children.db_crosscheck is not None
    assert any("REPLACE_LIVE" in r.getMessage() and "slot=db_crosscheck" in r.getMessage() for r in caplog.records)


def test_db_crosscheck_circuit_open_warns_not_critical(tmp_path, monkeypatch, caplog) -> None:
    daemon_mod, runner = _runner(tmp_path, monkeypatch, db_feeds_enabled=True)
    day = dt.date(2026, 9, 14)

    class _Tripping:
        last_exit_code = 3

        def ensure_running(self):
            return "circuit_open"

        def take_circuit_alert(self):
            return True

    runner.children.regular = object()
    runner.children.db_crosscheck = _Tripping()  # type: ignore[assignment]
    now = dt.datetime(2026, 9, 14, 9, 0, tzinfo=_KST)
    with caplog.at_level(logging.WARNING):
        runner._supervise_stream_child(daemon_mod.StreamChildKey.DB_CROSSCHECK, now, _anchors(day))
    assert any("stage=db_crosscheck" in r.getMessage() and r.levelno == logging.WARNING for r in caplog.records)
    assert not any("stage=db_crosscheck" in r.getMessage() and r.levelno == logging.CRITICAL for r in caplog.records)
    assert runner.state.last_db_crosscheck_result == "circuit_open"


def test_db_crosscheck_restart_warns_and_after_close_skips(tmp_path, monkeypatch, caplog) -> None:
    daemon_mod, runner = _runner(tmp_path, monkeypatch, db_feeds_enabled=True)
    day = dt.date(2026, 9, 14)
    calls: list[str] = []

    class _Flap:
        last_exit_code = 9

        def ensure_running(self):
            calls.append("ensure")
            return "restarted"

        def take_circuit_alert(self):
            return True

    runner.children.regular = object()
    runner.children.db_crosscheck = _Flap()  # type: ignore[assignment]
    with caplog.at_level(logging.WARNING):
        runner._supervise_stream_child(
            daemon_mod.StreamChildKey.DB_CROSSCHECK, dt.datetime(2026, 9, 14, 9, 0, tzinfo=_KST), _anchors(day)
        )
    assert calls == ["ensure"]
    assert "stage=db_crosscheck status=RESTARTED" in caplog.text
    calls.clear()
    runner._supervise_stream_child(
        daemon_mod.StreamChildKey.DB_CROSSCHECK, dt.datetime(2026, 9, 14, 16, 0, tzinfo=_KST), _anchors(day)
    )
    assert calls == []
    runner.children.regular = None
    runner._supervise_stream_child(
        daemon_mod.StreamChildKey.DB_CROSSCHECK, dt.datetime(2026, 9, 14, 9, 0, tzinfo=_KST), _anchors(day)
    )
    assert calls == []


def test_db_crosscheck_stopped_with_snapshot_child(tmp_path, monkeypatch) -> None:
    daemon_mod, runner = _runner(tmp_path, monkeypatch, db_feeds_enabled=True)
    runner._paths.candidates.parent.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(daemon_mod, "_run_eod_housekeeping", lambda *a, **k: daemon_mod._EodHousekeeping(deleted=0, uploaded=0, purged=0, maintenance_ok=True, offload_ok=True))
    monkeypatch.setattr(daemon_mod, "_check_backup", lambda *a, **k: ([], True, "ok"))
    stops: list[str] = []

    class _Child:
        def __init__(self, name):
            self._name = name

        def stop(self, *, timeout_s=15.0):
            stops.append(self._name)
            return "graceful"

    for stopper in ("_stop_holiday_children", "_reset_for_new_session_day", "_step_post_market_eod"):
        stops.clear()
        runner.children.snapshot = _Child("snapshot")  # type: ignore[assignment]
        runner.children.db_crosscheck = _Child("db")  # type: ignore[assignment]
        runner.state.last_snapshot_result = "running"
        runner.state.last_db_crosscheck_result = "running"
        if stopper == "_stop_holiday_children":
            runner._stop_holiday_children()
        elif stopper == "_reset_for_new_session_day":
            runner._reset_for_new_session_day(dt.date(2026, 9, 15))
        else:
            from tests.unit.orchestration.daemon_fixtures import _holiday_trading_day
            from src.orchestration.trading_day_gate import TradingDayStatus, TradingDayView

            day = dt.date(2026, 9, 14)
            view = TradingDayView(date=day, status=TradingDayStatus.HOLIDAY, trading_day=_holiday_trading_day(day))
            runner._step_post_market_eod(dt.datetime(2026, 9, 14, 20, 5, tzinfo=_KST), view, _anchors(day))
        assert "snapshot" in stops
        assert "db" in stops
        assert runner.children.db_crosscheck is None
        assert runner.state.last_db_crosscheck_result is None
    runner._stop_stream_child(daemon_mod.StreamChildKey.DB_CROSSCHECK)


def test_db_crosscheck_absent_from_degraded_mode(tmp_path, monkeypatch) -> None:
    import pathlib

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    settings.paths.candidates.parent.mkdir(parents=True, exist_ok=True)
    from src.universe.ipc import write_candidates

    write_candidates(settings.paths.candidates, [{"symbol": "005930", "selection_reasons": ["limit_up"]}], rev=20260911)
    created: list = []

    class _FakeSup:
        def __init__(self, *, cmd, breaker=None):
            self.cmd = list(cmd)
            created.append(self)

        def ensure_running(self):
            return "started"

        def stop(self, *, timeout_s=15.0):
            return "graceful"

    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSup)
    from src.core.config import resolve_collector_runtime

    runtime = resolve_collector_runtime(collector=settings, feed_control=_db_feed_control())
    runner = daemon_mod.DaemonRunner(runtime=runtime, shutdown=None, now=lambda: None, sleep=lambda s: None)
    runner._handle_orchestration_failure(dt.datetime(2026, 9, 14, 8, 25, tzinfo=_KST), dt.date(2026, 9, 14))
    assert runner.children.db_crosscheck is None
    assert runner.children.regular is not None


def test_ls_watchdogs_and_reconciliation_ignore_db_journals(tmp_path, monkeypatch, caplog) -> None:
    import pathlib

    from src.core.calendar import SessionState
    from src.core.config import CollectorSettings, resolve_collector_runtime
    from src.orchestration import daemon as daemon_mod
    from src.orchestration.trading_day_gate import TradingDayStatus, TradingDayView

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    runtime = resolve_collector_runtime(collector=settings, feed_control=_db_feed_control())
    runner = daemon_mod.DaemonRunner(runtime=runtime, shutdown=None, now=lambda: None, sleep=lambda s: None)
    runner.children.regular = object()
    day = dt.date(2026, 9, 14)
    part = settings.paths.journal_root / "db" / "krx" / "regular" / "H0STCNT0" / "dt=2026-09-14"
    part.mkdir(parents=True, exist_ok=True)
    (part / "09.jsonl.zst").write_bytes(b"x")
    from tests.unit.orchestration.daemon_fixtures import _business_trading_day

    view = TradingDayView(date=day, status=TradingDayStatus.BUSINESS, trading_day=_business_trading_day(day))
    anchors = _anchors(day)
    with caplog.at_level(logging.CRITICAL):
        runner._run_ingest_watchdog(dt.datetime(2026, 9, 14, 10, 0, tzinfo=_KST), SessionState.FULL_ACTIVE, view, anchors)
        runner._run_first_frame_watchdog(dt.datetime(2026, 9, 14, 8, 36, tzinfo=_KST), SessionState.STREAMER_ACTIVE, view, anchors)
    assert "status=STALE" in caplog.text
    assert "status=NO_FIRST_FRAME" in caplog.text
    assert "vendor=db" not in caplog.text
    ok, _ = runner._reconcile_regular_session(day, view, anchors)
    assert ok is False


def test_stop_children_includes_db_crosscheck(tmp_path, monkeypatch) -> None:
    daemon_mod, runner = _runner(tmp_path, monkeypatch, db_feeds_enabled=True)
    selected: list = []

    class _Sup:
        pass

    db = _Sup()
    runner.children.db_crosscheck = db  # type: ignore[assignment]
    monkeypatch.setattr(daemon_mod, "stop_supervisors", lambda children, *, deadline_s: selected.extend(children) or {"graceful": 1, "killed": 0, "not_running": 0})
    runner.stop_children()
    assert db in selected


def test_db_specs_enabled_only_by_secondary_feeds(tmp_path, monkeypatch) -> None:
    import pathlib

    from src.core.config import CollectorSettings, FeedControlSettings, resolve_collector_runtime
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    collector = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")

    def _enabled(feed_control: FeedControlSettings | None) -> dict:
        runtime = resolve_collector_runtime(collector=collector, feed_control=feed_control)
        specs = {spec.key: spec for spec in daemon_mod.DaemonRunner._STREAM_SPECS}
        return {
            key: specs[key].enabled(runtime)
            for key in (daemon_mod.StreamChildKey.DB_CROSSCHECK, daemon_mod.StreamChildKey.DB_CROSSCHECK_B)
        }

    assert _enabled(None) == {
        daemon_mod.StreamChildKey.DB_CROSSCHECK: False,
        daemon_mod.StreamChildKey.DB_CROSSCHECK_B: False,
    }
    assert _enabled(FeedControlSettings(watch_groups=("ls:0",), secondary_feeds=("db:0",))) == {
        daemon_mod.StreamChildKey.DB_CROSSCHECK: True,
        daemon_mod.StreamChildKey.DB_CROSSCHECK_B: False,
    }
    assert _enabled(FeedControlSettings(watch_groups=("ls:0",), secondary_feeds=("db:1",))) == {
        daemon_mod.StreamChildKey.DB_CROSSCHECK: False,
        daemon_mod.StreamChildKey.DB_CROSSCHECK_B: True,
    }


def _hot_runner(tmp_path, monkeypatch, *, regular_kis: bool):
    import pathlib

    from src.core.config import CollectorSettings, FeedControlSettings, RegularKisSettings, resolve_collector_runtime
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data")
    feed = FeedControlSettings(
        watch_groups=("ls:0",),
        secondary_feeds=("db:0", "db:1"),
        hot_symbols=(("db:0", 25), ("db:1", 25)),
    )
    rk = RegularKisSettings(enabled=True, credential_slots=("1",), pair_capacity_per_connection=41) if regular_kis else None
    runtime = resolve_collector_runtime(collector=settings, feed_control=feed, regular_kis=rk)
    return daemon_mod, daemon_mod.DaemonRunner(runtime=runtime, shutdown=None, now=lambda: None, sleep=lambda s: None)


def _capture_supervisors(monkeypatch, daemon_mod) -> None:
    class _FakeSup:
        def __init__(self, *, cmd, breaker=None):
            self.cmd = list(cmd)

        def ensure_running(self):
            return "started"

        def stop(self, *, timeout_s=15.0):
            return "graceful"

    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSup)


def _ranked_candidates(path, count: int) -> None:
    from src.universe.ipc import write_candidates

    path.parent.mkdir(parents=True, exist_ok=True)
    write_candidates(path, [{"symbol": f"{i:06d}", "selection_reasons": ["limit_up"]} for i in range(count)], rev=1)


def _hot_list(cmd: list[str]) -> tuple[str, ...]:
    return tuple(cmd[cmd.index("--hot-symbol-list") + 1].split(","))


def test_db_hot_lists_disjoint_and_exclude_kis_plan(tmp_path, monkeypatch) -> None:
    from src.realtime.contracts import MarketVenue
    from src.realtime.kis_sharding import AftermarketShard

    daemon_mod, runner = _hot_runner(tmp_path, monkeypatch, regular_kis=True)
    _capture_supervisors(monkeypatch, daemon_mod)
    day = dt.date(2026, 9, 14)
    _ranked_candidates(runner._paths.candidates, 60)
    kis_symbols = tuple(f"{i:06d}" for i in range(20))
    runner.state.regular_kis_plan = (AftermarketShard(MarketVenue.KRX, 0, kis_symbols, ("H0STCNT0", "H0STASP0"), "1", "id1"),)
    runner.state.regular_kis_plan_day = day

    runner._start_ready_children(day)

    db0 = runner.children.db_crosscheck.cmd
    db1 = runner.children.db_crosscheck_b.cmd
    list0, list1 = _hot_list(db0), _hot_list(db1)
    assert list0 == tuple(f"{i:06d}" for i in range(20, 45))
    assert list1 == tuple(f"{i:06d}" for i in range(45, 60))
    assert not set(list0) & set(list1)
    assert db0[db0.index("--hot-symbols") + 1] == str(len(list0))
    assert db1[db1.index("--hot-symbols") + 1] == str(len(list1))


def test_db_hot_without_regular_kis_uses_candidate_order(tmp_path, monkeypatch) -> None:
    daemon_mod, runner = _hot_runner(tmp_path, monkeypatch, regular_kis=False)
    _capture_supervisors(monkeypatch, daemon_mod)
    day = dt.date(2026, 9, 14)
    _ranked_candidates(runner._paths.candidates, 60)

    runner._start_ready_children(day)

    assert _hot_list(runner.children.db_crosscheck.cmd) == tuple(f"{i:06d}" for i in range(25))
    assert _hot_list(runner.children.db_crosscheck_b.cmd) == tuple(f"{i:06d}" for i in range(25, 50))


def test_db_hot_plan_failure_falls_back_to_warm_standby(tmp_path, monkeypatch, caplog) -> None:
    daemon_mod, runner = _hot_runner(tmp_path, monkeypatch, regular_kis=False)
    _capture_supervisors(monkeypatch, daemon_mod)
    day = dt.date(2026, 9, 14)
    runner._paths.candidates.parent.mkdir(parents=True, exist_ok=True)
    runner._paths.candidates.write_text("{not json", encoding="utf-8")

    with caplog.at_level(logging.WARNING):
        runner._start_ready_children(day)

    assert any("stage=db_hot_plan status=FAIL" in r.getMessage() for r in caplog.records)
    for cmd in (runner.children.db_crosscheck.cmd, runner.children.db_crosscheck_b.cmd):
        assert cmd[cmd.index("--hot-symbols") + 1] == "0"
        assert "--hot-symbol-list" not in cmd
