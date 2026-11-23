"""Invariant guards for NXT regular-session shard supervision in the daemon."""

from __future__ import annotations

import dataclasses
import datetime as dt
import logging
from zoneinfo import ZoneInfo

import pytest

import src.orchestration.daemon as daemon_mod
from src.core.alerts import PAGE_ATTR
from src.core.config import (
    CollectorSettings,
    IntradayAdmissionSettings,
    RegularKisSettings,
    ShardWatchdogSettings,
    resolve_collector_runtime,
)
from src.core.errors import KrxAlphaError
from src.core.session_anchors import AnchorSource, SessionAnchors, standard_session_anchors
from src.orchestration.trading_day_gate import TradingDayStatus, TradingDayView
from src.realtime.contracts import MarketVenue
from src.universe.ipc import CandidateSnapshot, write_candidate_snapshot

KST = ZoneInfo("Asia/Seoul")
DAY = dt.date(2026, 9, 16)
NXT_SYMBOLS = tuple(f"{code:06d}" for code in range(1, 41))


@pytest.fixture(autouse=True)
def _kis_data_pool(monkeypatch) -> None:
    monkeypatch.setenv("KIS_DATA_SLOTS", "1,2,3")
    for slot in ("1", "2", "3"):
        monkeypatch.setenv(f"KIS_DATA_{slot}_APP_KEY", f"test-nxt-app-key-{slot}")
        monkeypatch.setenv(f"KIS_DATA_{slot}_APP_SECRET", f"test-nxt-app-secret-{slot}")
        monkeypatch.setenv(f"KIS_DATA_{slot}_HTS_ID", f"test-nxt-hts-id-{slot}")


class _FakeSupervisor:
    def __init__(self, *, cmd, breaker=None):
        self.cmd = list(cmd)
        self.last_exit_code = None
        self.started_at_wall = None
        self._running = False
        self.stop_calls: list = []
        self.restarts: list = []

    def ensure_running(self):
        if self._running:
            return "running"
        self._running = True
        return "started"

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


class _CircuitSupervisor(_FakeSupervisor):
    def ensure_running(self):
        return "circuit_open"


class _FakeGate:
    def __init__(self, status=TradingDayStatus.BUSINESS):
        self.status = status

    def view(self, today, now):
        return TradingDayView(date=today, status=self.status, trading_day=None)


def _at(day, hour, minute, second=0):
    return dt.datetime(day.year, day.month, day.day, hour, minute, second, tzinfo=KST)


def _runner(monkeypatch, tmp_path, *, nxt_slots=("2", "3"), credential_slots=("1",), watchdog=None):
    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)
    rk = RegularKisSettings(
        enabled=True,
        credential_slots=credential_slots,
        pair_capacity_per_connection=41,
        nxt_credential_slots=nxt_slots,
    )
    settings = CollectorSettings(data_root=tmp_path / "data")
    runtime = resolve_collector_runtime(
        collector=settings,
        regular_kis=rk,
        shard_watchdog=watchdog if watchdog is not None else ShardWatchdogSettings(),
    )
    runner = daemon_mod.DaemonRunner(runtime=runtime, shutdown=None, now=lambda: None, sleep=lambda s: None)
    runner._gate = _FakeGate()
    return runner


def _write_nxt_symbols(paths, symbols, *, day=DAY, session_date=None) -> None:
    rows = tuple({"symbol": symbol, "rank": rank} for rank, symbol in enumerate(symbols, start=1))
    stamp = dt.datetime(day.year, day.month, day.day, 8, 0, tzinfo=KST)
    write_candidate_snapshot(
        paths.regular_nxt_candidates(DAY),
        CandidateSnapshot(
            schema_version=1,
            rev=int(day.strftime("%Y%m%d")),
            session_date=session_date or day,
            session="regular_nxt",
            generated_at=stamp,
            source_asof=stamp,
            effective_from=stamp,
            policy_version="test",
            capacity=len(rows),
            eligible_count=len(rows),
            selected_count=len(rows),
            candidates=rows,
        ),
    )


def _ready(runner, symbols=NXT_SYMBOLS) -> None:
    _write_nxt_symbols(runner._paths, symbols)
    runner.state.orchestrated_for = DAY


def _shifted(day) -> SessionAnchors:
    return SessionAnchors(
        date=day,
        regular_open=dt.time(9, 5),
        closing_auction_start=dt.time(15, 25),
        regular_close=dt.time(15, 30),
        after_market_end=dt.time(18, 0),
        source=AnchorSource.DEFAULT,
    )


def test_nxt_off_without_slots(monkeypatch, tmp_path) -> None:
    runner = _runner(monkeypatch, tmp_path, nxt_slots=())
    _ready(runner)

    runner._supervise_regular_nxt(_at(DAY, 9, 0), standard_session_anchors(DAY))

    assert runner.children.regular_nxt == {}
    assert runner.state.regular_nxt_plan == ()


def test_nxt_plan_from_symbol_file(monkeypatch, tmp_path) -> None:
    runner = _runner(monkeypatch, tmp_path)
    _ready(runner)

    runner._ensure_regular_nxt_plan(DAY)

    plan = runner.state.regular_nxt_plan
    assert [shard.venue for shard in plan] == [MarketVenue.NXT, MarketVenue.NXT]
    assert [shard.symbols for shard in plan] == [NXT_SYMBOLS[0::2], NXT_SYMBOLS[1::2]]
    assert [shard.credential_slot for shard in plan] == ["2", "3"]

    runner._supervise_regular_nxt(_at(DAY, 9, 0), standard_session_anchors(DAY))

    assert sorted(runner.children.regular_nxt) == ["nxt-00", "nxt-01"]
    assert runner.children.regular_kis == {}


@pytest.mark.parametrize("variant", ["missing", "stale_session"])
def test_missing_or_stale_symbol_file_fails_closed(monkeypatch, tmp_path, caplog, variant) -> None:
    runner = _runner(monkeypatch, tmp_path)
    if variant == "stale_session":
        previous = DAY - dt.timedelta(days=1)
        _write_nxt_symbols(runner._paths, NXT_SYMBOLS, day=previous, session_date=previous)
    runner.state.orchestrated_for = DAY

    with caplog.at_level(logging.ERROR, logger=daemon_mod.logger.name):
        runner._supervise_regular_nxt(_at(DAY, 9, 0), standard_session_anchors(DAY))

    assert runner.state.regular_nxt_plan == ()
    assert runner.children.regular_nxt == {}
    skips = [r for r in caplog.records if "stage=regular_kis_plan status=SKIP reason=nxt_" in r.getMessage()]
    assert len(skips) == 1


def test_shifted_day_skips_nxt(monkeypatch, tmp_path) -> None:
    runner = _runner(monkeypatch, tmp_path)
    _ready(runner)
    shifted = _shifted(DAY)

    runner._supervise_regular_nxt(_at(DAY, 9, 4), shifted)
    runner._supervise_regular_nxt(_at(DAY, 9, 10), shifted)

    assert runner.children.regular_nxt == {}


def test_window_and_stop(monkeypatch, tmp_path) -> None:
    runner = _runner(monkeypatch, tmp_path)
    _ready(runner)
    anchors = standard_session_anchors(DAY)

    runner._supervise_regular_nxt(_at(DAY, 8, 57, 59), anchors)
    assert runner.children.regular_nxt == {}
    runner._supervise_regular_nxt(_at(DAY, 8, 58, 0), anchors)
    assert sorted(runner.children.regular_nxt) == ["nxt-00", "nxt-01"]
    runner._supervise_regular_nxt(_at(DAY, 15, 21, 59), anchors)
    assert sorted(runner.children.regular_nxt) == ["nxt-00", "nxt-01"]
    runner._supervise_regular_nxt(_at(DAY, 15, 22, 0), anchors)
    assert runner.children.regular_nxt == {}


def test_krx_children_untouched_when_nxt_leaves_window(monkeypatch, tmp_path) -> None:
    runner = _runner(monkeypatch, tmp_path)
    _ready(runner)
    krx = _FakeSupervisor(cmd=["collect-regular-kis"], breaker=None)
    krx._running = True
    runner.children.regular_kis["krx-00"] = krx
    runner._supervise_regular_nxt(_at(DAY, 8, 58, 0), standard_session_anchors(DAY))

    runner._supervise_regular_nxt(_at(DAY, 15, 30, 0), standard_session_anchors(DAY))

    assert runner.children.regular_nxt == {}
    assert runner.children.regular_kis == {"krx-00": krx}
    assert krx.stop_calls == []


def test_circuit_page_once(monkeypatch, tmp_path, caplog) -> None:
    runner = _runner(monkeypatch, tmp_path)
    _ready(runner)
    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _CircuitSupervisor)
    anchors = standard_session_anchors(DAY)

    with caplog.at_level(logging.CRITICAL, logger=daemon_mod.logger.name):
        runner._supervise_regular_nxt(_at(DAY, 9, 0), anchors)
        runner._supervise_regular_nxt(_at(DAY, 9, 0, 10), anchors)

    circuit = [r for r in caplog.records if "reason=circuit_open" in r.getMessage() and "nxt-00" in r.getMessage()]
    assert len(circuit) == 1
    assert getattr(circuit[0], PAGE_ATTR) is daemon_mod.PageKind.REGULAR_FEED_DOWN


def test_stale_nxt_shard_restarted_alone(monkeypatch, tmp_path) -> None:
    runner = _runner(monkeypatch, tmp_path)
    _ready(runner)
    anchors = standard_session_anchors(DAY)
    now = _at(DAY, 10, 0)
    runner._supervise_regular_nxt(_at(DAY, 9, 0), anchors)
    stale = runner.children.regular_nxt["nxt-00"]
    fresh = runner.children.regular_nxt["nxt-01"]
    stale.started_at_wall = now.timestamp() - 3600
    fresh.started_at_wall = now.timestamp() - 3600

    def _journal_wall(root, *, venue, shard_index, **_kwargs):
        return now.timestamp() - 3600 if shard_index == 0 else now.timestamp() - 1

    monkeypatch.setattr(daemon_mod, "shard_last_journal_wall", _journal_wall)
    runner._watch_regular_nxt_shards(now, DAY, anchors)

    assert stale.restarts == ["stale_child"]
    assert fresh.restarts == []
    assert fresh.is_running()


def test_intraday_dynamic_slot_overlapping_nxt_fails_closed(monkeypatch, tmp_path) -> None:
    runner = _runner(monkeypatch, tmp_path)
    runner._runtime = dataclasses.replace(
        runner._runtime, intraday=IntradayAdmissionSettings(enabled=True, kis_dynamic_slots=("3",))
    )

    with pytest.raises(KrxAlphaError, match="regular_nxt credential slot"):
        runner._ensure_regular_kis_plan(DAY)


def test_intraday_dynamic_slot_outside_nxt_slots_is_accepted(monkeypatch, tmp_path) -> None:
    runner = _runner(monkeypatch, tmp_path)
    runner._runtime = dataclasses.replace(
        runner._runtime, intraday=IntradayAdmissionSettings(enabled=True, kis_dynamic_slots=("5",))
    )

    runner._ensure_regular_kis_plan(DAY)


def test_nxt_plan_without_slots_never_reads_symbol_file(monkeypatch, tmp_path) -> None:
    runner = _runner(monkeypatch, tmp_path, nxt_slots=())
    _ready(runner)

    def _forbidden(*_args, **_kwargs):
        raise AssertionError("symbol file must not be read when NXT has no slots")

    monkeypatch.setattr(daemon_mod, "read_candidate_snapshot", _forbidden)
    runner._ensure_regular_nxt_plan(DAY)

    assert runner.state.regular_nxt_plan == ()
    assert runner.state.regular_nxt_plan_day == DAY


def test_degraded_candidates_block_nxt_plan(monkeypatch, tmp_path, caplog) -> None:
    runner = _runner(monkeypatch, tmp_path)
    _ready(runner)
    runner.state.degraded_active = True

    with caplog.at_level(logging.ERROR, logger=daemon_mod.logger.name):
        runner._ensure_regular_nxt_plan(DAY)

    assert runner.state.regular_nxt_plan == ()
    assert runner.state.regular_nxt_plan_day == DAY
    assert any("reason=nxt_degraded_candidates" in r.getMessage() for r in caplog.records)


def test_nxt_plan_waits_for_today_orchestration(monkeypatch, tmp_path) -> None:
    runner = _runner(monkeypatch, tmp_path)
    _write_nxt_symbols(runner._paths, NXT_SYMBOLS)

    runner._ensure_regular_nxt_plan(DAY)

    assert runner.state.regular_nxt_plan == ()
    assert runner.state.regular_nxt_plan_day is None

    runner.state.orchestrated_for = DAY
    runner._ensure_regular_nxt_plan(DAY)

    assert [shard.symbols for shard in runner.state.regular_nxt_plan] == [NXT_SYMBOLS[0::2], NXT_SYMBOLS[1::2]]


def test_nxt_shard_planning_failure_skips_with_empty_plan(monkeypatch, tmp_path, caplog) -> None:
    runner = _runner(monkeypatch, tmp_path)
    _ready(runner)
    monkeypatch.setenv("KIS_DATA_SLOTS", "1")

    with caplog.at_level(logging.ERROR, logger=daemon_mod.logger.name):
        runner._ensure_regular_nxt_plan(DAY)

    assert runner.state.regular_nxt_plan == ()
    assert runner.children.regular_nxt == {}
    assert any(
        "stage=regular_kis_plan status=SKIP reason=nxt_" in r.getMessage() for r in caplog.records
    )


class _RestartedSupervisor(_FakeSupervisor):
    def ensure_running(self):
        self.last_exit_code = 7
        return "restarted"


class _RaisingSupervisor(_FakeSupervisor):
    def ensure_running(self):
        raise OSError("spawn denied")


def test_restarted_nxt_shard_is_logged_with_exit_code(monkeypatch, tmp_path, caplog) -> None:
    runner = _runner(monkeypatch, tmp_path)
    _ready(runner)
    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _RestartedSupervisor)

    with caplog.at_level(logging.WARNING, logger=daemon_mod.logger.name):
        runner._supervise_regular_nxt(_at(DAY, 9, 0), standard_session_anchors(DAY))

    assert any(
        "status=RESTARTED shard=nxt-00 exit_code=7" in r.getMessage() for r in caplog.records
    )


def test_nxt_spawn_failure_is_contained_as_supervise_fail(monkeypatch, tmp_path, caplog) -> None:
    runner = _runner(monkeypatch, tmp_path)
    _ready(runner)
    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _RaisingSupervisor)

    with caplog.at_level(logging.WARNING, logger=daemon_mod.logger.name):
        runner._supervise_regular_nxt(_at(DAY, 9, 0), standard_session_anchors(DAY))

    assert any(
        "stage=regular_kis_supervise status=FAIL reason=spawn denied" in r.getMessage() for r in caplog.records
    )


def test_nxt_watchdog_io_error_is_contained(monkeypatch, tmp_path, caplog) -> None:
    from src.core.observability import reset_once_per_day

    runner = _runner(monkeypatch, tmp_path)
    _ready(runner)
    anchors = standard_session_anchors(DAY)
    runner._supervise_regular_nxt(_at(DAY, 9, 0), anchors)

    def _unreadable(*_args, **_kwargs):
        raise OSError("journal unreadable")

    monkeypatch.setattr(daemon_mod, "shard_last_journal_wall", _unreadable)
    reset_once_per_day()
    with caplog.at_level(logging.WARNING, logger=daemon_mod.logger.name):
        runner._watch_regular_nxt_shards(_at(DAY, 10, 0), DAY, anchors)

    assert any(
        "stage=shard_watchdog status=ERROR scope=regular_nxt" in r.getMessage() for r in caplog.records
    )
    assert all(child.restarts == [] for child in runner.children.regular_nxt.values())


def test_disabled_watchdog_never_restarts_nxt_shards(monkeypatch, tmp_path) -> None:
    runner = _runner(monkeypatch, tmp_path, watchdog=ShardWatchdogSettings(enabled=False))
    _ready(runner)
    anchors = standard_session_anchors(DAY)
    now = _at(DAY, 10, 0)
    runner._supervise_regular_nxt(_at(DAY, 9, 0), anchors)
    stale = runner.children.regular_nxt["nxt-00"]
    stale.started_at_wall = now.timestamp() - 3600

    monkeypatch.setattr(daemon_mod, "shard_last_journal_wall", lambda *a, **kw: now.timestamp() - 3600)
    runner._watch_regular_nxt_shards(now, DAY, anchors)

    assert stale.restarts == []
    assert stale.is_running()


def test_watchdog_judges_only_planned_nxt_shards(monkeypatch, tmp_path) -> None:
    runner = _runner(monkeypatch, tmp_path)
    _ready(runner)
    runner._ensure_regular_nxt_plan(DAY)
    anchors = standard_session_anchors(DAY)
    now = _at(DAY, 10, 0)
    ghost = _FakeSupervisor(cmd=["collect-regular-kis"])
    ghost._running = True
    ghost.started_at_wall = now.timestamp() - 3600
    runner.children.regular_nxt = {"nxt-09": ghost}

    monkeypatch.setattr(daemon_mod, "shard_last_journal_wall", lambda *a, **kw: now.timestamp() - 3600)
    runner._watch_regular_nxt_shards(now, DAY, anchors)

    assert ghost.restarts == []


def test_stop_regular_children_stops_nxt_shards_too(monkeypatch, tmp_path) -> None:
    runner = _runner(monkeypatch, tmp_path)
    _ready(runner)
    runner._supervise_regular_nxt(_at(DAY, 9, 0), standard_session_anchors(DAY))
    nxt = dict(runner.children.regular_nxt)
    assert sorted(nxt) == ["nxt-00", "nxt-01"]

    runner._stop_regular_kis_children(clear_circuit_alerts=False)

    assert runner.children.regular_nxt == {}
    assert all(sup.stop_calls for sup in nxt.values())
