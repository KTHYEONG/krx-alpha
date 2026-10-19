"""Invariant guards for daemon-side stale shard supervision (aftermarket + premarket)."""

from __future__ import annotations

import datetime as dt
import logging
import os
from zoneinfo import ZoneInfo

import src.orchestration.daemon as daemon_mod
from src.core.calendar import SessionState
from src.orchestration.trading_day_gate import TradingDayStatus, TradingDayView

KST = ZoneInfo("Asia/Seoul")
DAY = dt.date(2026, 9, 16)


def _at(day, hour, minute, second=0):
    return dt.datetime(day.year, day.month, day.day, hour, minute, second, tzinfo=KST)


class _FakeSup:
    def __init__(self, *, running=True, started_at=None, ensure_script=()):
        self._running = running
        self.started_at_wall = started_at
        self.ensure_calls = 0
        self.ensure_script = list(ensure_script)
        self.stop_restart_calls: list = []
        self.stop_calls: list = []
        self.last_exit_code = None

    def is_running(self):
        return self._running

    def ensure_running(self):
        self.ensure_calls += 1
        if self.ensure_script:
            return self.ensure_script.pop(0)
        return "running"

    def stop_for_restart(self, *, reason, timeout_s=15.0):
        self.stop_restart_calls.append({"reason": reason, "timeout_s": timeout_s})
        self._running = False
        return "graceful"

    def stop(self, *, timeout_s=15.0):
        self.stop_calls.append(timeout_s)
        self._running = False
        return "graceful"


def _shard(venue, idx, streams):
    from src.realtime.kis_sharding import AftermarketShard

    return AftermarketShard(venue, idx, ("005930",), streams, "0", "id0")


def _am_runner(monkeypatch, tmp_path, *, stale_limit=120.0, max_restarts=3, enabled=True):
    monkeypatch.setenv("KRX_ALPHA_AFTERMARKET_PAIR_CAPACITY_PER_CONNECTION", "4")
    from src.core.config import (
        AftermarketSettings,
        CollectorSettings,
        ShardWatchdogSettings,
        resolve_collector_runtime,
    )

    collector = CollectorSettings(data_root=tmp_path / "data", after_market_enabled=True)
    after = AftermarketSettings(enabled=True, pair_capacity_per_connection=4)
    wd = ShardWatchdogSettings(
        enabled=enabled,
        aftermarket_stale_limit_s=stale_limit,
        premarket_stale_limit_s=max(stale_limit, 31.0),
        max_restarts_per_day=max_restarts,
    )
    runtime = resolve_collector_runtime(collector=collector, aftermarket=after, shard_watchdog=wd)
    return daemon_mod.DaemonRunner(runtime=runtime, shutdown=None, now=lambda: _at(DAY, 18, 0), sleep=lambda s: None)


def _anchors(day):
    from src.core.session_anchors import standard_session_anchors

    return standard_session_anchors(day)


def _install(runner, shards, fakes):
    runner._state.aftermarket_plan = tuple(shards)
    runner._state.aftermarket_plan_day = DAY
    for shard, fake in zip(shards, fakes, strict=True):
        runner._children.aftermarket[f"{shard.venue.value}:{shard.shard_index}"] = fake


def _write_shard_journal(journal_root, *, venue, session, stream, day, shard_index, hour, age_s, now):
    part = journal_root / "kis" / venue / session / stream / f"dt={day.isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    path = part / f"{hour:02d}.s{shard_index}.jsonl.zst"
    path.write_bytes(b"x")
    ts = now.timestamp() - age_s
    os.utime(path, (ts, ts))
    return path


def _nxt_shards():
    from src.realtime.contracts import MarketVenue

    return (
        _shard(MarketVenue.NXT, 0, ("H0NXCNT0", "H0NXASP0")),
        _shard(MarketVenue.NXT, 1, ("H0NXCNT0", "H0NXASP0")),
    )


def _supervise(runner, now):
    runner._supervise_aftermarket(now, SessionState.AFTER_MARKET_ACTIVE, DAY, _anchors(DAY))


def test_stale_shard_stopped_and_restarted_same_cycle(monkeypatch, tmp_path, caplog) -> None:
    runner = _am_runner(monkeypatch, tmp_path)
    now = _at(DAY, 18, 0)
    shards = _nxt_shards()[:1]
    fake = _FakeSup(running=True, started_at=now.timestamp() - 1000)
    _install(runner, shards, [fake])
    _write_shard_journal(
        runner._paths.journal_root, venue="nxt", session="nxt_after", stream="H0NXCNT0",
        day=DAY, shard_index=0, hour=16, age_s=200, now=now,
    )
    with caplog.at_level(logging.WARNING, logger=daemon_mod.logger.name):
        _supervise(runner, now)
    assert len(fake.stop_restart_calls) == 1
    assert fake.stop_restart_calls[0]["reason"] == "stale_child"
    assert fake.ensure_calls == 1
    lines = [r.getMessage() for r in caplog.records]
    assert any("stage=child_restart status=STALE reason=stale_child key=nxt:0" in m for m in lines)
    assert runner.state.shard_stale_restarts == 1
    assert not any(r.levelno >= logging.CRITICAL for r in caplog.records)


def test_fresh_shard_untouched(monkeypatch, tmp_path, caplog) -> None:
    runner = _am_runner(monkeypatch, tmp_path)
    now = _at(DAY, 18, 0)
    fake = _FakeSup(running=True, started_at=now.timestamp() - 1000)
    _install(runner, _nxt_shards()[:1], [fake])
    _write_shard_journal(
        runner._paths.journal_root, venue="nxt", session="nxt_after", stream="H0NXCNT0",
        day=DAY, shard_index=0, hour=17, age_s=5, now=now,
    )
    with caplog.at_level(logging.DEBUG, logger=daemon_mod.logger.name):
        _supervise(runner, now)
    assert fake.stop_restart_calls == []
    assert not any("child_restart" in r.getMessage() for r in caplog.records)


def test_no_kill_outside_window(monkeypatch, tmp_path) -> None:
    runner = _am_runner(monkeypatch, tmp_path)
    fake = _FakeSup(running=True, started_at=_at(DAY, 12, 0).timestamp())
    _install(runner, _nxt_shards()[:1], [fake])
    for now in (_at(DAY, 15, 30), _at(DAY, 20, 0, 0)):
        _write_shard_journal(
            runner._paths.journal_root, venue="nxt", session="nxt_after", stream="H0NXCNT0",
            day=DAY, shard_index=0, hour=15, age_s=10_000, now=now,
        )
        _supervise(runner, now)
    assert fake.stop_restart_calls == []


def test_outside_windows_do_not_read_journals(monkeypatch, tmp_path) -> None:
    from unittest.mock import Mock

    aftermarket = _am_runner(monkeypatch, tmp_path)
    _install(aftermarket, _nxt_shards()[:1], [_FakeSup()])
    premarket = _pm_runner(monkeypatch, tmp_path)
    shard = _pm_install(premarket, _FakeSup())
    read = Mock(side_effect=AssertionError("outside-window journal access"))
    monkeypatch.setattr(daemon_mod, "shard_last_journal_wall", read)
    for now in (_at(DAY, 15, 30), _at(DAY, 20, 0)):
        aftermarket._watch_aftermarket_shards(now, DAY, _anchors(DAY))
    for now in (_at(DAY, 7, 59), _at(DAY, 8, 50)):
        premarket._watch_premarket_shard(now, DAY, _anchors(DAY), shard)
    read.assert_not_called()


def test_restarted_child_not_rekilled(monkeypatch, tmp_path) -> None:
    runner = _am_runner(monkeypatch, tmp_path)
    t0 = _at(DAY, 18, 0)
    fake = _FakeSup(running=True, started_at=t0.timestamp() - 1000)
    _install(runner, _nxt_shards()[:1], [fake])
    _write_shard_journal(
        runner._paths.journal_root, venue="nxt", session="nxt_after", stream="H0NXCNT0",
        day=DAY, shard_index=0, hour=16, age_s=200, now=t0,
    )
    _supervise(runner, t0)
    assert len(fake.stop_restart_calls) == 1
    fresh = _FakeSup(running=True, started_at=t0.timestamp())
    runner._children.aftermarket["nxt:0"] = fresh
    for step in (10, 20, 60, 110):
        _supervise(runner, t0 + dt.timedelta(seconds=step))
    assert fresh.stop_restart_calls == []
    _supervise(runner, t0 + dt.timedelta(seconds=131))
    assert len(fresh.stop_restart_calls) == 1


def test_one_restart_per_cycle(monkeypatch, tmp_path) -> None:
    from src.realtime.contracts import MarketVenue

    runner = _am_runner(monkeypatch, tmp_path)
    now = _at(DAY, 18, 0)
    shards = (
        _shard(MarketVenue.NXT, 0, ("H0NXCNT0", "H0NXASP0")),
        _shard(MarketVenue.KRX, 0, ("H0STCNT0", "H0STASP0")),
    )
    fakes = [_FakeSup(running=True, started_at=now.timestamp() - 1000) for _ in shards]
    _install(runner, shards, fakes)
    for venue, session, stream in (("nxt", "nxt_after", "H0NXCNT0"), ("krx", "krx_after", "H0STCNT0")):
        _write_shard_journal(
            runner._paths.journal_root, venue=venue, session=session, stream=stream,
            day=DAY, shard_index=0, hour=16, age_s=500, now=now,
        )
    _supervise(runner, now)
    assert sum(len(f.stop_restart_calls) for f in fakes) == 1


def test_venue_wide_silence_no_kill_once(monkeypatch, tmp_path, caplog) -> None:
    runner = _am_runner(monkeypatch, tmp_path)
    now = _at(DAY, 18, 0)
    shards = _nxt_shards()
    fakes = [_FakeSup(running=True, started_at=now.timestamp() - 1000) for _ in shards]
    _install(runner, shards, fakes)
    for idx in (0, 1):
        _write_shard_journal(
            runner._paths.journal_root, venue="nxt", session="nxt_after", stream="H0NXCNT0",
            day=DAY, shard_index=idx, hour=16, age_s=500, now=now,
        )
    with caplog.at_level(logging.WARNING, logger=daemon_mod.logger.name):
        _supervise(runner, now)
        _supervise(runner, now + dt.timedelta(seconds=10))
    assert all(f.stop_restart_calls == [] for f in fakes)
    warns = [r for r in caplog.records if "reason=venue_wide_silence venue=nxt" in r.getMessage()]
    assert len(warns) == 1
    assert warns[0].levelno == logging.WARNING


def test_storm_bound_and_budget_critical_once(monkeypatch, tmp_path, caplog) -> None:
    runner = _am_runner(monkeypatch, tmp_path)
    t0 = _at(DAY, 18, 0)
    fake = _FakeSup(running=True, started_at=t0.timestamp() - 1000)
    _install(runner, _nxt_shards()[:1], [fake])
    _write_shard_journal(
        runner._paths.journal_root, venue="nxt", session="nxt_after", stream="H0NXCNT0",
        day=DAY, shard_index=0, hour=16, age_s=500, now=t0,
    )
    stops: list = []
    stop_times: list = []
    with caplog.at_level(logging.DEBUG, logger=daemon_mod.logger.name):
        for cycle in range(100):
            now = t0 + dt.timedelta(seconds=10 * cycle)
            _supervise(runner, now)
            current = runner._children.aftermarket["nxt:0"]
            if current.stop_restart_calls and (not stops or stops[-1] is not current):
                stops.append(current)
                stop_times.append(now)
                runner._children.aftermarket["nxt:0"] = _FakeSup(running=True, started_at=now.timestamp())
    assert len(stops) <= 3
    import itertools

    for first, second in itertools.pairwise(stop_times):
        assert (second - first).total_seconds() >= 120
    criticals = [r for r in caplog.records if "reason=stale_budget_exhausted" in r.getMessage()]
    assert len(criticals) == 1
    assert criticals[0].levelno == logging.CRITICAL
    assert "key=nxt:0" in criticals[0].getMessage()
    current = runner._children.aftermarket["nxt:0"]
    assert current.stop_restart_calls == []


def test_circuit_open_after_stale_kill(monkeypatch, tmp_path, caplog) -> None:
    runner = _am_runner(monkeypatch, tmp_path)
    now = _at(DAY, 18, 0)
    fake = _FakeSup(running=True, started_at=now.timestamp() - 1000, ensure_script=["circuit_open"])
    _install(runner, _nxt_shards()[:1], [fake])
    _write_shard_journal(
        runner._paths.journal_root, venue="nxt", session="nxt_after", stream="H0NXCNT0",
        day=DAY, shard_index=0, hour=16, age_s=200, now=now,
    )
    with caplog.at_level(logging.WARNING, logger=daemon_mod.logger.name):
        _supervise(runner, now)
    assert len(fake.stop_restart_calls) == 1
    assert any("reason=circuit_open" in r.getMessage() for r in caplog.records)


def test_disabled_watchdog_stats_nothing(monkeypatch, tmp_path) -> None:
    runner = _am_runner(monkeypatch, tmp_path, enabled=False)
    now = _at(DAY, 18, 0)
    fake = _FakeSup(running=True, started_at=now.timestamp() - 1000)
    _install(runner, _nxt_shards()[:1], [fake])
    called: list = []

    def _boom(*a, **k):
        called.append(1)
        raise AssertionError("must not stat")

    monkeypatch.setattr(daemon_mod, "shard_last_journal_wall", _boom)
    _supervise(runner, now)
    assert called == []
    assert fake.stop_restart_calls == []


def test_detect_only_budget_zero(monkeypatch, tmp_path, caplog) -> None:
    runner = _am_runner(monkeypatch, tmp_path, max_restarts=0)
    now = _at(DAY, 18, 0)
    fake = _FakeSup(running=True, started_at=now.timestamp() - 1000)
    _install(runner, _nxt_shards()[:1], [fake])
    _write_shard_journal(
        runner._paths.journal_root, venue="nxt", session="nxt_after", stream="H0NXCNT0",
        day=DAY, shard_index=0, hour=16, age_s=200, now=now,
    )
    with caplog.at_level(logging.WARNING, logger=daemon_mod.logger.name):
        _supervise(runner, now)
    assert fake.stop_restart_calls == []
    assert any("reason=stale_budget_exhausted" in r.getMessage() for r in caplog.records)


def test_containment_and_remaining_supervised(monkeypatch, tmp_path, caplog) -> None:
    from src.realtime.contracts import MarketVenue

    runner = _am_runner(monkeypatch, tmp_path)
    now = _at(DAY, 18, 0)
    shards = (
        _shard(MarketVenue.NXT, 0, ("H0NXCNT0", "H0NXASP0")),
        _shard(MarketVenue.KRX, 0, ("H0STCNT0", "H0STASP0")),
    )
    fakes = [_FakeSup(running=True, started_at=now.timestamp() - 1000) for _ in shards]
    _install(runner, shards, fakes)
    real = daemon_mod.shard_last_journal_wall

    def _flaky(root, **kwargs):
        if kwargs.get("venue") == "nxt":
            raise OSError("disk gone")
        return real(root, **kwargs)

    monkeypatch.setattr(daemon_mod, "shard_last_journal_wall", _flaky)
    with caplog.at_level(logging.WARNING, logger=daemon_mod.logger.name):
        _supervise(runner, now)
    assert any("stage=shard_watchdog status=ERROR" in r.getMessage() for r in caplog.records)
    assert all(f.ensure_calls == 1 for f in fakes)


def test_legacy_supervisor_without_watchdog_api_skipped(monkeypatch, tmp_path) -> None:
    runner = _am_runner(monkeypatch, tmp_path)
    now = _at(DAY, 18, 0)

    class _Legacy:
        def __init__(self):
            self.ensure_calls = 0

        def ensure_running(self):
            self.ensure_calls += 1
            return "running"

    legacy = _Legacy()
    _install(runner, _nxt_shards()[:1], [legacy])
    _write_shard_journal(
        runner._paths.journal_root, venue="nxt", session="nxt_after", stream="H0NXCNT0",
        day=DAY, shard_index=0, hour=16, age_s=500, now=now,
    )
    _supervise(runner, now)
    assert legacy.ensure_calls == 1


def _pm_runner(monkeypatch, tmp_path, *, max_restarts=3, enabled=True):
    monkeypatch.setenv("KIS_DATA_SLOTS", "5")
    monkeypatch.setenv("KIS_DATA_5_APP_KEY", "k")
    monkeypatch.setenv("KIS_DATA_5_APP_SECRET", "s")
    monkeypatch.setenv("KIS_DATA_5_HTS_ID", "h")
    from src.core.config import (
        CollectorSettings,
        PremarketSettings,
        ShardWatchdogSettings,
        resolve_collector_runtime,
    )

    collector = CollectorSettings(data_root=tmp_path / "data")
    pre = PremarketSettings(enabled=True, credential_slot="5", pair_capacity_per_connection=41)
    wd = ShardWatchdogSettings(enabled=enabled, premarket_stale_limit_s=120.0, max_restarts_per_day=max_restarts)
    runtime = resolve_collector_runtime(collector=collector, premarket=pre, shard_watchdog=wd)
    return daemon_mod.DaemonRunner(runtime=runtime, shutdown=None, now=lambda: _at(DAY, 8, 20), sleep=lambda s: None)


class _FakeGate:
    def view(self, today, now):
        return TradingDayView(date=today, status=TradingDayStatus.BUSINESS, trading_day=None)


def _pm_install(runner, fake):
    from src.realtime.contracts import MarketVenue

    shard = _shard(MarketVenue.NXT, 0, ("H0NXCNT0", "H0NXASP0"))
    runner._state.premarket_plan_day = DAY
    runner._state.premarket_shard = shard
    runner._children.premarket = fake
    runner._gate = _FakeGate()
    return shard


def test_premarket_stale_stop_and_window_end_inert(monkeypatch, tmp_path, caplog) -> None:
    runner = _pm_runner(monkeypatch, tmp_path)
    now = _at(DAY, 8, 20)
    fake = _FakeSup(running=True, started_at=now.timestamp() - 1000)
    _pm_install(runner, fake)
    _write_shard_journal(
        runner._paths.journal_root, venue="nxt", session="nxt_pre", stream="H0NXCNT0",
        day=DAY, shard_index=0, hour=8, age_s=200, now=now,
    )
    with caplog.at_level(logging.WARNING, logger=daemon_mod.logger.name):
        runner._supervise_premarket(now, _anchors(DAY))
    assert len(fake.stop_restart_calls) == 1
    assert fake.stop_restart_calls[0]["reason"] == "stale_child"
    assert any("key=premarket" in r.getMessage() and "child_restart" in r.getMessage() for r in caplog.records)
    assert runner.state.shard_stale_restarts == 1
    caplog.clear()
    late = _at(DAY, 8, 51)
    with caplog.at_level(logging.WARNING, logger=daemon_mod.logger.name):
        runner._supervise_premarket(late, _anchors(DAY))
    assert len(fake.stop_restart_calls) == 1


def test_digest_counts_stale_restarts(monkeypatch, tmp_path) -> None:
    runner = _am_runner(monkeypatch, tmp_path)
    digests: list = []
    monkeypatch.setattr(
        daemon_mod, "_run_eod_housekeeping",
        lambda *a, **k: daemon_mod._EodHousekeeping(deleted=1, uploaded=2, purged=3, maintenance_ok=True, offload_ok=True),
    )
    monkeypatch.setattr(daemon_mod, "_check_backup", lambda *a, **k: ([], True, "ok"))
    monkeypatch.setattr(daemon_mod, "send_digest", lambda subject, body: digests.append(body) or True)
    monkeypatch.setattr(runner, "_aftermarket_eod_blocked", lambda *a, **k: False)
    monkeypatch.setattr(runner, "_log_aftermarket_prearm", lambda *a, **k: 0)
    monkeypatch.setattr(runner, "_log_aftermarket_coverage", lambda *a, **k: 0)
    monkeypatch.setattr(runner, "_log_premarket_eod", lambda *a, **k: None)
    monkeypatch.setattr(runner, "_reconcile_regular_session", lambda *a, **k: (True, True))
    monkeypatch.setattr(runner, "_reconcile_crosscheck_sessions", lambda *a, **k: {})
    from tests.unit.orchestration.daemon_fixtures import _business_trading_day

    view = TradingDayView(date=DAY, status=TradingDayStatus.BUSINESS, trading_day=_business_trading_day(DAY))
    runner.state.shard_stale_restarts = 1
    runner._run_business_eod(_at(DAY, 20, 5), DAY, view, _anchors(DAY))
    assert "shard_stale_restarts=1" in digests[0].splitlines()
    assert runner.state.shard_stale_restarts == 0


def test_gap_lands_under_new_reason_end_to_end(tmp_path, monkeypatch) -> None:
    import src.realtime.session as session_mod
    from src.orchestration.supervisor import ProcessSupervisor
    from src.realtime.session import bootstrap_session
    from src.universe.ipc import write_candidates

    argv_seen: list = []

    class _Proc:
        def __init__(self):
            self._killed = False

        def poll(self):
            return 0 if self._killed else None

        def terminate(self):
            pass

        def kill(self):
            self._killed = True

        def wait(self, timeout=None):
            import subprocess

            if timeout is not None:
                raise subprocess.TimeoutExpired("cmd", timeout)
            return 0

    def _popen(cmd):
        argv_seen.append(list(cmd))
        return _Proc()

    sup = ProcessSupervisor(cmd=["collect-aftermarket", "--venue", "nxt"], popen=_popen)
    assert sup.ensure_running() == "started"
    assert sup.stop_for_restart(reason="stale_child", timeout_s=0.01) == "killed"
    assert sup.ensure_running() == "restarted"
    assert argv_seen[-1][-2:] == ["--restart-reason", "stale_child"]

    monkeypatch.setattr(session_mod, "measure_ntp_offset_ns", lambda host, *, client=None: 0)
    day = dt.date(2026, 9, 16)
    journal_root = tmp_path / "l0"
    manifest_path = tmp_path / "s.json"
    candidates_path = tmp_path / "c.json"
    write_candidates(candidates_path, [{"symbol": "005930", "selection_reasons": ["limit_up"]}], rev=20260915)

    def _cfg(**over):
        base = {
            "session_date": day, "journal_root": journal_root, "manifest_path": manifest_path,
            "candidates_path": candidates_path, "ntp_host": "ntp", "slot_budget": 2,
            "max_clock_offset_ns": 2_000_000_000, "desired_streams": ("H0STCNT0",), "vendor": "ls",
        }
        base.update(over)
        return session_mod.SessionConfig(**base)

    from zoneinfo import ZoneInfo as _ZI

    t0 = int(dt.datetime(2026, 9, 16, 15, 45, tzinfo=_ZI("Asia/Seoul")).timestamp() * 1e9)
    bootstrap_session(_cfg(), now_ns=t0)
    journal_file = journal_root / "ls" / "krx" / "regular" / "H0STCNT0" / "dt=2026-09-16" / "15.jsonl.zst"
    journal_file.parent.mkdir(parents=True, exist_ok=True)
    journal_file.write_bytes(b"x")
    last_write_ns = t0 + 3_600_000_000_000
    os.utime(journal_file, ns=(last_write_ns, last_write_ns))
    reason = argv_seen[-1][argv_seen[-1].index("--restart-reason") + 1]
    session = bootstrap_session(_cfg(restart_reason=reason), now_ns=last_write_ns + 120_000_000_000)
    assert session.manifest.gaps[-1]["reason"] == "stale_child"
    assert session.manifest.gaps[-1]["gap_start_ns"] == last_write_ns


def test_premarket_disabled_watchdog_stats_nothing(monkeypatch, tmp_path) -> None:
    runner = _pm_runner(monkeypatch, tmp_path, enabled=False)
    fake = _FakeSup(running=True, started_at=_at(DAY, 8, 0).timestamp())
    shard = _pm_install(runner, fake)
    called: list = []

    def _boom(*a, **k):
        called.append(1)
        raise AssertionError("must not stat")

    monkeypatch.setattr(daemon_mod, "shard_last_journal_wall", _boom)
    runner._watch_premarket_shard(_at(DAY, 8, 20), DAY, _anchors(DAY), shard)
    assert called == []
    assert fake.stop_restart_calls == []


def test_runner_without_watchdog_settings_still_constructs(tmp_path) -> None:
    from types import SimpleNamespace

    from src.core.config import CollectorSettings, resolve_collector_runtime

    runtime = resolve_collector_runtime(collector=CollectorSettings(data_root=tmp_path / "data"))
    bare = SimpleNamespace(
        collector=runtime.collector, aftermarket=runtime.aftermarket, snapshot=runtime.snapshot,
        premarket=runtime.premarket, paths=runtime.paths,
    )
    runner = daemon_mod.DaemonRunner(runtime=bare, shutdown=None, now=lambda: None, sleep=lambda s: None)
    assert runner._aftermarket_watchdog is not None
    assert runner._premarket_watchdog is not None


def test_premarket_containment(monkeypatch, tmp_path, caplog) -> None:
    import logging

    runner = _pm_runner(monkeypatch, tmp_path)
    fake = _FakeSup(running=True, started_at=_at(DAY, 8, 0).timestamp())
    _pm_install(runner, fake)

    def _boom(root, **kwargs):
        raise OSError("disk gone")

    monkeypatch.setattr(daemon_mod, "shard_last_journal_wall", _boom)
    with caplog.at_level(logging.WARNING, logger=daemon_mod.logger.name):
        runner._supervise_premarket(_at(DAY, 8, 20), _anchors(DAY))
    assert any("stage=shard_watchdog status=ERROR" in r.getMessage() for r in caplog.records)
    assert fake.ensure_calls == 1
    assert fake.stop_restart_calls == []


def test_premarket_legacy_supervisor_skipped(monkeypatch, tmp_path) -> None:
    runner = _pm_runner(monkeypatch, tmp_path)

    class _Legacy:
        def __init__(self):
            self.ensure_calls = 0

        def ensure_running(self):
            self.ensure_calls += 1
            return "running"

    legacy = _Legacy()
    runner._state.premarket_plan_day = DAY
    from src.realtime.contracts import MarketVenue

    runner._state.premarket_shard = _shard(MarketVenue.NXT, 0, ("H0NXCNT0", "H0NXASP0"))
    runner._children.premarket = legacy
    runner._gate = _FakeGate()
    runner._supervise_premarket(_at(DAY, 8, 20), _anchors(DAY))
    assert legacy.ensure_calls == 1


def test_apply_skips_unknown_key(monkeypatch, tmp_path) -> None:
    from src.orchestration.shard_watchdog import ShardDecision, ShardVerdict

    runner = _am_runner(monkeypatch, tmp_path)
    decision = ShardDecision(
        key="nxt:9", verdict=ShardVerdict.STALE, silent_s=500.0, limit_s=120.0, restart=True
    )
    assert runner._apply_shard_decisions(
        (decision,), watchdog=runner._aftermarket_watchdog, venue="nxt", today=DAY,
        supervisors={}, max_restarts=3,
    ) is False
    assert runner.state.shard_stale_restarts == 0
