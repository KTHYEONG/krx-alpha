"""Invariant guard tests for premarket daemon orchestration (part4)."""

from __future__ import annotations

import datetime as dt
import logging
from zoneinfo import ZoneInfo

import pytest

import src.orchestration.daemon as daemon_mod
from src.core.errors import KrxAlphaError
from src.orchestration.trading_day_gate import TradingDayStatus, TradingDayView

KST = ZoneInfo("Asia/Seoul")
S = dt.date(2026, 9, 16)
T = dt.date(2026, 9, 17)
FRI = dt.date(2026, 9, 18)
MON = dt.date(2026, 9, 21)
SAT = dt.date(2026, 9, 19)


@pytest.fixture(autouse=True)
def _kis_data_pool(monkeypatch) -> None:
    monkeypatch.setenv("KIS_DATA_SLOTS", "5")
    monkeypatch.setenv("KIS_DATA_5_APP_KEY", "test-premarket-app-key")
    monkeypatch.setenv("KIS_DATA_5_APP_SECRET", "test-premarket-app-secret")
    monkeypatch.setenv("KIS_DATA_5_HTS_ID", "test-premarket-hts-id")


_CREATED: list = []


class _FakeSupervisor:
    def __init__(self, *, cmd, breaker=None):
        self.cmd = list(cmd)
        self._breaker = breaker
        self._proc = None
        self.last_exit_code = None
        self.stop_calls: list = []
        self.signals: list = []
        self.waits: list = []
        _CREATED.append(self)

    def ensure_running(self):
        if self._proc is not None:
            self.last_exit_code = 1
            if self._breaker is not None and not self._breaker.allow_restart():
                return "circuit_open"
            if self._breaker is not None:
                self._breaker.record_restart()
            return "restarted"
        self._proc = object()
        return "started"

    def stop(self, *, timeout_s=15.0):
        self.stop_calls.append(timeout_s)
        self._proc = None
        return "graceful"

    def request_stop(self):
        _EVENTS.append((id(self), "signal"))
        self.signals.append(1)
        return self._proc is not None

    def wait_stopped(self, timeout_s):
        _EVENTS.append((id(self), "wait"))
        self.waits.append(timeout_s)
        self._proc = None
        return "graceful"


_EVENTS: list = []


@pytest.fixture(autouse=True)
def _reset_fakes():
    _CREATED.clear()
    _EVENTS.clear()
    return


class _FakeGate:
    def __init__(self, status, trading_day=None):
        self.status = status
        self.trading_day = trading_day
        self.calls: list = []

    def view(self, today, now):
        self.calls.append(today)
        return TradingDayView(date=today, status=self.status, trading_day=self.trading_day)


def _trading_day(day, *, business=True, next_day=None, next_anchors=None):
    from src.marketdata.toss_calendar import TradingDay

    delta = dt.timedelta(days=1)
    return TradingDay(
        date=day,
        is_business_day=business,
        previous_business_day=day - delta,
        next_business_day=next_day if next_day is not None else day + delta,
        anchors=None,
        next_anchors=next_anchors,
    )


def _runner(monkeypatch, tmp_path, gate, *, premarket_kwargs=None, enabled=True):
    from src.core.config import CollectorSettings, PremarketSettings, resolve_collector_runtime

    kwargs = {"credential_slot": "5", "pair_capacity_per_connection": 41}
    kwargs.update(premarket_kwargs or {})
    pre = PremarketSettings(enabled=enabled, **kwargs)
    settings = CollectorSettings(data_root=tmp_path / "data")
    runtime = resolve_collector_runtime(collector=settings, premarket=pre)
    runner = daemon_mod.DaemonRunner(runtime=runtime, shutdown=None, now=lambda: _at(S, 8, 0), sleep=lambda s: None)
    runner._gate = gate
    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)
    return runner


def _at(day, hour, minute, second=0):
    return dt.datetime(day.year, day.month, day.day, hour, minute, second, tzinfo=KST)


def _write_pool(paths, target, symbols=("005930", "000660")) -> None:
    from src.universe.ipc import CandidateSnapshot, write_candidate_snapshot

    gen = _at(target - dt.timedelta(days=1), 20, 5)
    eff = _at(target, 7, 58)
    write_candidate_snapshot(
        paths.premarket_candidates(target),
        CandidateSnapshot(
            schema_version=1,
            rev=int(target.strftime("%Y%m%d")),
            session_date=target,
            session="premarket",
            generated_at=gen,
            source_asof=gen,
            effective_from=eff,
            policy_version="premarket_v1",
            capacity=len(symbols),
            eligible_count=len(symbols),
            selected_count=len(symbols),
            candidates=tuple(
                {
                    "symbol": s,
                    "rank": i + 1,
                    "source_ranks": {"trade_amount": i + 1},
                    "metrics": {"trade_value_krw": 10**9 - i, "change_pct": 1.0},
                    "selection_reasons": ["trade_amount"],
                }
                for i, s in enumerate(symbols)
            ),
        ),
    )


class _FakeKisClient:
    def __init__(self, *, fail=False):
        self.calls: list = []
        self.fail = fail

    def get_trade_amount_ranking(self, *, market_div="J"):
        from src.brokers.kis.data import KisRankingRow

        self.calls.append(("ta", market_div))
        if self.fail:
            from src.execution.contracts import KisApiError

            raise KisApiError("EGW00123", "rate limited")
        return tuple(
            KisRankingRow(symbol=s, rank=i + 1, change_pct=1.0, trade_value_krw=10**9 - i)
            for i, s in enumerate(("005930", "000660"))
        )

    def get_fluctuation_ranking(self, *, market_div="J"):
        from src.brokers.kis.data import KisRankingRow

        self.calls.append(("fl", market_div))
        if self.fail:
            from src.execution.contracts import KisApiError

            raise KisApiError("EGW00123", "rate limited")
        return tuple(
            KisRankingRow(symbol=s, rank=i + 1, change_pct=0.5, trade_value_krw=10**9 - i)
            for i, s in enumerate(("005930", "000660"))
        )


def _anchors(day):
    from src.core.session_anchors import standard_session_anchors

    return standard_session_anchors(day)


def test_disabled_feature_is_inert(monkeypatch, tmp_path) -> None:
    from src.core.calendar import calc_sleep_seconds
    from src.core.config import CollectorSettings, resolve_collector_runtime

    settings = CollectorSettings(data_root=tmp_path / "data")
    runtime = resolve_collector_runtime(collector=settings)
    runner = daemon_mod.DaemonRunner(runtime=runtime, shutdown=None, now=lambda: _at(S, 8, 0), sleep=lambda s: None)
    supervise_calls: list = []
    refresh_calls: list = []
    monkeypatch.setattr(
        daemon_mod.DaemonRunner, "_supervise_premarket", lambda self, now, anchors: supervise_calls.append(now)
    )
    monkeypatch.setattr(
        daemon_mod.DaemonRunner, "_maybe_refresh_premarket_pool", lambda self, now, anchors: refresh_calls.append(now)
    )
    monkeypatch.setattr(
        daemon_mod, "_build_kis_client", lambda *a, **k: (_ for _ in ()).throw(AssertionError("no REST"))
    )
    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", lambda **k: (_ for _ in ()).throw(AssertionError("no child")))
    first = runner.step(_at(S, 7, 59))
    assert supervise_calls == []
    assert refresh_calls == []
    assert runner.children.premarket is None
    assert first == min(calc_sleep_seconds(_at(S, 7, 59), runner._sched.streamer_start), 300.0)
    second = runner.step(_at(SAT, 7, 59))
    assert supervise_calls == []
    assert refresh_calls == []
    assert runner.children.premarket is None
    assert second == 3600.0


def test_evening_pool_is_built_once_and_survives_restart(monkeypatch, tmp_path) -> None:
    gate = _FakeGate(TradingDayStatus.BUSINESS, _trading_day(S, next_day=T))
    runner = _runner(monkeypatch, tmp_path, gate)
    client = _FakeKisClient()
    monkeypatch.setattr(daemon_mod, "_build_kis_client", lambda *a, **k: client)
    anchors = _anchors(S)
    runner._maybe_refresh_premarket_pool(_at(S, 20, 5), anchors)
    runner._maybe_refresh_premarket_pool(_at(S, 20, 6), anchors)
    assert len(client.calls) == 2
    assert runner._paths.premarket_candidates(T).exists()
    fresh = _runner(monkeypatch, tmp_path, gate)
    monkeypatch.setattr(daemon_mod, "_build_kis_client", lambda *a, **k: client)
    fresh._maybe_refresh_premarket_pool(_at(S, 20, 7), anchors)
    assert len(client.calls) == 2


def test_friday_evening_targets_monday(monkeypatch, tmp_path) -> None:
    gate = _FakeGate(TradingDayStatus.BUSINESS, _trading_day(FRI, next_day=MON))
    runner = _runner(monkeypatch, tmp_path, gate)
    client = _FakeKisClient()
    monkeypatch.setattr(daemon_mod, "_build_kis_client", lambda *a, **k: client)
    runner._maybe_refresh_premarket_pool(_at(FRI, 20, 5), _anchors(FRI))
    assert runner._paths.premarket_candidates(MON).exists()
    assert not runner._paths.premarket_candidates(FRI + dt.timedelta(days=1)).exists()


def test_shifted_next_day_builds_no_pool(monkeypatch, tmp_path) -> None:
    from src.core.session_anchors import SessionAnchors

    shifted_next = SessionAnchors(
        date=T,
        regular_open=dt.time(10, 0),
        closing_auction_start=dt.time(16, 20),
        regular_close=dt.time(16, 30),
        after_market_end=dt.time(21, 0),
        source="default",
    )
    gate = _FakeGate(TradingDayStatus.BUSINESS, _trading_day(S, next_day=T, next_anchors=shifted_next))
    runner = _runner(monkeypatch, tmp_path, gate)
    client = _FakeKisClient()
    monkeypatch.setattr(daemon_mod, "_build_kis_client", lambda *a, **k: client)
    runner._maybe_refresh_premarket_pool(_at(S, 20, 5), _anchors(S))
    assert client.calls == []
    assert not runner._paths.premarket_candidates(T).exists()
    assert runner.state.premarket_pool_day == S


def test_pool_failure_retries_without_blocking(monkeypatch, tmp_path, caplog) -> None:
    gate = _FakeGate(TradingDayStatus.BUSINESS, _trading_day(S, next_day=T))
    runner = _runner(monkeypatch, tmp_path, gate)
    client = _FakeKisClient(fail=True)
    monkeypatch.setattr(daemon_mod, "_build_kis_client", lambda *a, **k: client)
    anchors = _anchors(S)
    with caplog.at_level(logging.WARNING, logger=daemon_mod.logger.name):
        runner._maybe_refresh_premarket_pool(_at(S, 20, 5), anchors)
    warnings = [r for r in caplog.records if "premarket_pool" in r.getMessage() and r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert runner.state.next_premarket_pool_at == _at(S, 20, 6)
    client.fail = False
    with caplog.at_level(logging.DEBUG, logger=daemon_mod.logger.name):
        runner._maybe_refresh_premarket_pool(_at(S, 20, 6, 5), anchors)
    assert runner._paths.premarket_candidates(T).exists()


def test_pool_is_generated_strictly_before_target_day(monkeypatch, tmp_path) -> None:
    from src.universe.ipc import read_candidate_snapshot

    gate = _FakeGate(TradingDayStatus.BUSINESS, _trading_day(S, next_day=T))
    runner = _runner(monkeypatch, tmp_path, gate)
    monkeypatch.setattr(daemon_mod, "_build_kis_client", lambda *a, **k: _FakeKisClient())
    runner._maybe_refresh_premarket_pool(_at(S, 20, 5), _anchors(S))
    snapshot = read_candidate_snapshot(
        runner._paths.premarket_candidates(T),
        expected_session_date=T,
        expected_session="premarket",
        max_candidates=20,
    )
    assert snapshot.generated_at.date() < T
    assert snapshot.effective_from == _at(T, 7, 58)


def test_collector_starts_at_start_minus_lead(monkeypatch, tmp_path) -> None:
    gate = _FakeGate(TradingDayStatus.BUSINESS, _trading_day(T, next_day=T + dt.timedelta(days=1)))
    runner = _runner(monkeypatch, tmp_path, gate)
    _write_pool(runner._paths, T)
    runner.step(_at(T, 7, 57, 50))
    assert runner.children.premarket is None
    runner.step(_at(T, 7, 58, 0))
    assert runner.children.premarket is not None
    assert "collect-premarket" in runner.children.premarket.cmd


def test_daemon_wakes_for_the_window(monkeypatch, tmp_path) -> None:
    from src.orchestration.premarket import PREMARKET_POLL_S

    gate = _FakeGate(TradingDayStatus.UNKNOWN, None)
    runner = _runner(monkeypatch, tmp_path, gate)
    early = runner.step(_at(T, 7, 0))
    assert early == 300.0
    assert (_at(T, 7, 0) + dt.timedelta(seconds=early)) <= _at(T, 7, 58)
    _write_pool(runner._paths, T)
    inside = runner.step(_at(T, 8, 10))
    assert inside <= PREMARKET_POLL_S


def test_child_is_stopped_at_window_end_and_not_restarted(monkeypatch, tmp_path) -> None:
    gate = _FakeGate(TradingDayStatus.BUSINESS, _trading_day(T, next_day=T + dt.timedelta(days=1)))
    runner = _runner(monkeypatch, tmp_path, gate)
    _write_pool(runner._paths, T)
    anchors = _anchors(T)
    runner._supervise_premarket(_at(T, 8, 10), anchors)
    child = runner.children.premarket
    assert child is not None
    created = len(_CREATED)
    runner._supervise_premarket(_at(T, 8, 50), anchors)
    assert child.stop_calls == [15.0]
    assert runner.children.premarket is None
    runner._supervise_premarket(_at(T, 8, 51), anchors)
    assert runner.children.premarket is None
    assert len(_CREATED) == created


def test_missing_pool_skips_collection_loudly(monkeypatch, tmp_path, caplog) -> None:
    gate = _FakeGate(TradingDayStatus.BUSINESS, _trading_day(T, next_day=T + dt.timedelta(days=1)))
    runner = _runner(monkeypatch, tmp_path, gate)
    anchors = _anchors(T)
    with caplog.at_level(logging.ERROR, logger=daemon_mod.logger.name):
        runner._supervise_premarket(_at(T, 7, 58), anchors)
        runner._supervise_premarket(_at(T, 8, 10), anchors)
    assert runner.children.premarket is None
    errors = [r for r in caplog.records if "premarket_plan" in r.getMessage() and r.levelno == logging.ERROR]
    assert len(errors) == 1


def test_holiday_stops_premarket(monkeypatch, tmp_path) -> None:
    business = _FakeGate(TradingDayStatus.BUSINESS, _trading_day(T, next_day=T + dt.timedelta(days=1)))
    runner = _runner(monkeypatch, tmp_path, business)
    _write_pool(runner._paths, T)
    anchors = _anchors(T)
    runner._supervise_premarket(_at(T, 8, 10), anchors)
    assert runner.children.premarket is not None
    runner._gate = _FakeGate(TradingDayStatus.HOLIDAY, None)
    runner._supervise_premarket(_at(T, 8, 11), anchors)
    assert runner.children.premarket is None
    runner2 = _runner(monkeypatch, tmp_path, _FakeGate(TradingDayStatus.HOLIDAY, None))
    runner2._supervise_premarket(_at(T, 7, 58), anchors)
    assert runner2.children.premarket is None


def test_unknown_calendar_still_collects(monkeypatch, tmp_path) -> None:
    runner = _runner(monkeypatch, tmp_path, _FakeGate(TradingDayStatus.UNKNOWN, None))
    _write_pool(runner._paths, T)
    runner._supervise_premarket(_at(T, 7, 58), _anchors(T))
    assert runner.children.premarket is not None


def test_shifted_session_skips_premarket(monkeypatch, tmp_path) -> None:
    from src.core.session_anchors import SessionAnchors

    shifted = SessionAnchors(
        date=T,
        regular_open=dt.time(10, 0),
        closing_auction_start=dt.time(16, 20),
        regular_close=dt.time(16, 30),
        after_market_end=dt.time(21, 0),
        source="default",
    )
    runner = _runner(monkeypatch, tmp_path, _FakeGate(TradingDayStatus.BUSINESS, _trading_day(T, next_day=T)))
    _write_pool(runner._paths, T)
    runner._supervise_premarket(_at(T, 8, 30), shifted)
    assert runner.children.premarket is None


def test_circuit_open_alerts_once(monkeypatch, tmp_path, caplog) -> None:
    gate = _FakeGate(TradingDayStatus.BUSINESS, _trading_day(T, next_day=T + dt.timedelta(days=1)))
    runner = _runner(monkeypatch, tmp_path, gate)
    _write_pool(runner._paths, T)
    anchors = _anchors(T)
    with caplog.at_level(logging.DEBUG, logger=daemon_mod.logger.name):
        for minute in range(8, 15):
            runner._supervise_premarket(_at(T, 8, minute), anchors)
    restarts = [r for r in caplog.records if "status=RESTARTED" in r.getMessage()]
    circuits = [r for r in caplog.records if "circuit_open" in r.getMessage()]
    assert len(restarts) >= 1
    assert all(r.levelno == logging.WARNING for r in restarts)
    assert len(circuits) == 1
    assert circuits[0].levelno == logging.CRITICAL
    assert runner.children.aftermarket == {}
    assert runner.children.regular is None


def test_premarket_failure_cannot_degrade_other_automation(monkeypatch, tmp_path) -> None:
    gate = _FakeGate(TradingDayStatus.BUSINESS, _trading_day(S, next_day=T))
    runner = _runner(monkeypatch, tmp_path, gate)
    before_children = (runner.children.regular, runner.children.snapshot, dict(runner.children.aftermarket))
    before_plan = runner.state.aftermarket_plan
    monkeypatch.setattr(
        daemon_mod, "plan_premarket_shard", lambda **k: (_ for _ in ()).throw(KrxAlphaError("plan boom"))
    )
    monkeypatch.setattr(daemon_mod, "_build_kis_client", lambda *a, **k: (_ for _ in ()).throw(OSError("rest boom")))
    _write_pool(runner._paths, T)
    runner._supervise_premarket(_at(T, 7, 58), _anchors(T))
    runner._maybe_refresh_premarket_pool(_at(S, 20, 5), _anchors(S))
    assert runner.children.regular is before_children[0]
    assert runner.children.snapshot is before_children[1]
    assert runner.children.aftermarket == before_children[2]
    assert runner.state.aftermarket_plan == before_plan


def test_shutdown_stops_premarket_child_within_shared_deadline(monkeypatch, tmp_path) -> None:
    from src.core.config import CollectorSettings, PremarketSettings, resolve_collector_runtime

    pre = PremarketSettings(enabled=True, credential_slot="5", pair_capacity_per_connection=41)
    settings = CollectorSettings(data_root=tmp_path / "data")
    runtime = resolve_collector_runtime(collector=settings, premarket=pre)
    runner = daemon_mod.DaemonRunner(runtime=runtime, shutdown=None, now=lambda: _at(S, 8, 0), sleep=lambda s: None)
    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)
    gate = _FakeGate(TradingDayStatus.BUSINESS, _trading_day(T, next_day=T + dt.timedelta(days=1)))
    runner._gate = gate
    _write_pool(runner._paths, T)
    runner._supervise_premarket(_at(T, 8, 10), _anchors(T))
    assert runner.children.premarket is not None
    n_supervisors = len(_CREATED)
    assert n_supervisors == 1
    counts = daemon_mod.stop_supervisors(list(_CREATED), deadline_s=20.0)
    assert counts["graceful"] == n_supervisors
    signals = [e for e in _EVENTS if e[1] == "signal"]
    waits = [e for e in _EVENTS if e[1] == "wait"]
    assert len(signals) == n_supervisors
    assert max(_EVENTS.index(e) for e in signals) < min(_EVENTS.index(e) for e in waits)


def _eod_runner(monkeypatch, tmp_path, *, ref_day, manifest_case):
    from src.core.config import CollectorSettings, PremarketSettings, resolve_collector_runtime
    from src.realtime.contracts import MarketVenue
    from src.realtime.kis_sharding import AftermarketShard

    pre = PremarketSettings(enabled=True, credential_slot="5", pair_capacity_per_connection=41)
    settings = CollectorSettings(data_root=tmp_path / "data")
    runtime = resolve_collector_runtime(collector=settings, premarket=pre)
    runner = daemon_mod.DaemonRunner(
        runtime=runtime, shutdown=None, now=lambda: _at(ref_day, 20, 30), sleep=lambda s: None
    )
    runner._gate = _FakeGate(TradingDayStatus.BUSINESS, _trading_day(ref_day, next_day=ref_day + dt.timedelta(days=1)))
    if manifest_case != "no_plan":
        from src.realtime.manifest import SessionManifest

        planned = [("005930", "H0NXCNT0"), ("005930", "H0NXASP0")]
        manifest = SessionManifest(
            session_date=ref_day,
            clock_offset_ns=0,
            started_at_ns=0,
            venue="nxt",
            session="nxt_pre",
            expected_close_ns=10**18,
            writer_closed_at_ns=10**18 if manifest_case == "closed" else None,
            planned_pairs=[{"symbol": s, "tr_id": t} for s, t in planned],
            shard_index=0,
            credential_key_id="fp5",
        )
        for symbol, tr_id in planned:
            manifest.record_ack(vendor="kis", tr_id=tr_id, symbol=symbol, rt_cd="0", accepted=True)
        manifest.save(runner._paths.premarket_manifest_path(ref_day))
        runner.state.premarket_plan_day = ref_day
        runner.state.premarket_shard = AftermarketShard(
            MarketVenue.NXT, 0, ("005930",), ("H0NXCNT0", "H0NXASP0"), "5", "fp5"
        )
    monkeypatch.setattr(
        daemon_mod,
        "_run_eod_housekeeping",
        lambda *a, **k: daemon_mod._EodHousekeeping(
            deleted=0, uploaded=0, purged=0, maintenance_ok=True, offload_ok=True
        ),
    )
    monkeypatch.setattr(daemon_mod, "_check_backup", lambda *a, **k: ([], True, "ok"))
    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", lambda **k: True)
    digests: list = []
    monkeypatch.setattr(daemon_mod, "send_digest", lambda subject, body: digests.append((subject, body)))
    return runner, digests


def test_eod_log_is_informational(monkeypatch, tmp_path, caplog) -> None:
    from src.core.session_anchors import standard_session_anchors
    from src.orchestration.trading_day_gate import TradingDayView

    ref_day = T
    anchors = standard_session_anchors(ref_day)
    day = TradingDayView(
        date=ref_day, status=TradingDayStatus.BUSINESS, trading_day=_trading_day(ref_day, next_day=ref_day)
    )
    runner, digests = _eod_runner(monkeypatch, tmp_path, ref_day=ref_day, manifest_case="closed")
    with caplog.at_level(logging.INFO, logger=daemon_mod.logger.name):
        runner._run_business_eod(_at(ref_day, 20, 30), ref_day, day, anchors)
    lines = [r.getMessage() for r in caplog.records if "stage=premarket_eod" in r.getMessage()]
    assert len(lines) == 1
    assert "status=OK" in lines[0]
    assert "premarket" not in digests[0][1]
    assert "status=OK" in digests[0][1]
    caplog.clear()

    runner, digests = _eod_runner(monkeypatch, tmp_path, ref_day=ref_day, manifest_case="open")
    with caplog.at_level(logging.INFO, logger=daemon_mod.logger.name):
        runner._run_business_eod(_at(ref_day, 20, 30), ref_day, day, anchors)
    lines = [r.getMessage() for r in caplog.records if "stage=premarket_eod" in r.getMessage()]
    assert len(lines) == 1
    assert "status=NOT_CLOSED" in lines[0]
    assert "status=OK" in digests[0][1]
    caplog.clear()

    runner, digests = _eod_runner(monkeypatch, tmp_path, ref_day=ref_day, manifest_case="no_plan")
    with caplog.at_level(logging.INFO, logger=daemon_mod.logger.name):
        runner._run_business_eod(_at(ref_day, 20, 30), ref_day, day, anchors)
    lines = [r.getMessage() for r in caplog.records if "stage=premarket_eod" in r.getMessage()]
    assert len(lines) == 1
    assert "status=SKIPPED" in lines[0]
    assert "status=OK" in digests[0][1]


def test_direct_calls_are_noops_when_disabled(monkeypatch, tmp_path) -> None:
    from src.core.config import CollectorSettings, resolve_collector_runtime

    settings = CollectorSettings(data_root=tmp_path / "data")
    runtime = resolve_collector_runtime(collector=settings)
    runner = daemon_mod.DaemonRunner(runtime=runtime, shutdown=None, now=lambda: _at(S, 8, 0), sleep=lambda s: None)
    runner._maybe_refresh_premarket_pool(_at(S, 20, 5), _anchors(S))
    runner._supervise_premarket(_at(T, 7, 58), _anchors(T))
    assert runner.children.premarket is None
    assert runner.state.premarket_pool_day is None
    assert runner.state.premarket_plan_day is None


def test_pool_retry_window_and_non_business_gate(monkeypatch, tmp_path, caplog) -> None:
    gate = _FakeGate(TradingDayStatus.BUSINESS, _trading_day(S, next_day=T))
    runner = _runner(monkeypatch, tmp_path, gate)
    client = _FakeKisClient(fail=True)
    monkeypatch.setattr(daemon_mod, "_build_kis_client", lambda *a, **k: client)
    anchors = _anchors(S)
    runner._maybe_refresh_premarket_pool(_at(S, 20, 5), anchors)
    assert len(client.calls) == 1
    runner._maybe_refresh_premarket_pool(_at(S, 20, 5, 30), anchors)
    assert len(client.calls) == 1
    with caplog.at_level(logging.DEBUG, logger=daemon_mod.logger.name):
        runner._maybe_refresh_premarket_pool(_at(S, 20, 6, 5), anchors)
    debugs = [r for r in caplog.records if "premarket_pool" in r.getMessage() and r.levelno == logging.DEBUG]
    assert len(debugs) == 1
    assert runner.state.premarket_pool_day is None
    unknown = _runner(monkeypatch, tmp_path, _FakeGate(TradingDayStatus.UNKNOWN, None))
    unknown_client = _FakeKisClient()
    monkeypatch.setattr(daemon_mod, "_build_kis_client", lambda *a, **k: unknown_client)
    unknown._maybe_refresh_premarket_pool(_at(S, 20, 5), anchors)
    assert unknown_client.calls == []
    assert unknown.state.premarket_pool_day is None


def test_supervise_failure_is_isolated(monkeypatch, tmp_path, caplog) -> None:
    gate = _FakeGate(TradingDayStatus.BUSINESS, _trading_day(T, next_day=T + dt.timedelta(days=1)))
    runner = _runner(monkeypatch, tmp_path, gate)
    monkeypatch.setattr(daemon_mod, "premarket_collection_due", lambda *a, **k: (_ for _ in ()).throw(KrxAlphaError("boom")))
    with caplog.at_level(logging.WARNING, logger=daemon_mod.logger.name):
        runner._supervise_premarket(_at(T, 8, 10), _anchors(T))
    failures = [r for r in caplog.records if "premarket_supervise" in r.getMessage()]
    assert len(failures) == 1
    assert runner.children.premarket is None
    assert runner.children.aftermarket == {}


def test_stop_children_includes_premarket(monkeypatch, tmp_path) -> None:
    from src.core.config import CollectorSettings, PremarketSettings, resolve_collector_runtime

    pre = PremarketSettings(enabled=True, credential_slot="5", pair_capacity_per_connection=41)
    settings = CollectorSettings(data_root=tmp_path / "data")
    runtime = resolve_collector_runtime(collector=settings, premarket=pre)
    runner = daemon_mod.DaemonRunner(runtime=runtime, shutdown=None, now=lambda: _at(S, 8, 0), sleep=lambda s: None)
    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)
    runner._children.regular = daemon_mod.ProcessSupervisor(cmd=["regular"], breaker=None)
    runner._children.premarket = daemon_mod.ProcessSupervisor(cmd=["premarket"], breaker=None)
    runner.stop_children()
    assert runner._children.regular is not None
    assert runner._children.premarket is not None
