"""Invariant guards for the evening NXT regular symbol pool step in the daemon."""

from __future__ import annotations

import dataclasses
import datetime as dt
import logging
from zoneinfo import ZoneInfo

import pytest

import src.orchestration.daemon as daemon_mod
from src.core.config import CollectorSettings, RegularKisSettings, resolve_collector_runtime
from src.core.session_anchors import SessionAnchors, standard_session_anchors
from src.marketdata.kiwoom_ranking import KiwoomRankingRow
from src.marketdata.kiwoom_ticks import KiwoomTickTapeError
from src.orchestration.trading_day_gate import TradingDayStatus, TradingDayView
from src.universe.regular_nxt import regular_nxt_pool_ready

KST = ZoneInfo("Asia/Seoul")
FRI = dt.date(2026, 11, 13)
MON = dt.date(2026, 11, 16)
TUE = dt.date(2026, 11, 17)


@pytest.fixture(autouse=True)
def _kis_data_pool(monkeypatch) -> None:
    monkeypatch.setenv("KIS_DATA_SLOTS", "1")
    monkeypatch.setenv("KIS_DATA_1_APP_KEY", "test-nxt-pool-app-key")
    monkeypatch.setenv("KIS_DATA_1_APP_SECRET", "test-nxt-pool-app-secret")
    monkeypatch.setenv("KIS_DATA_1_HTS_ID", "test-nxt-pool-hts-id")


class _FakeGate:
    def __init__(self, status, trading_day=None) -> None:
        self.status = status
        self.trading_day = trading_day

    def view(self, today, now):
        return TradingDayView(date=today, status=self.status, trading_day=self.trading_day)


def _trading_day(day, *, next_day, next_anchors=None):
    from src.marketdata.toss_calendar import TradingDay

    return TradingDay(
        date=day,
        is_business_day=True,
        previous_business_day=day - dt.timedelta(days=1),
        next_business_day=next_day,
        anchors=None,
        next_anchors=next_anchors,
    )


def _at(day: dt.date, hour: int, minute: int, second: int = 0) -> dt.datetime:
    return dt.datetime(day.year, day.month, day.day, hour, minute, second, tzinfo=KST)


def _runner(monkeypatch, tmp_path, gate, *, nxt_slots=("1",)):
    regular = RegularKisSettings(
        enabled=True,
        credential_slots=("2",),
        pair_capacity_per_connection=40,
        nxt_credential_slots=nxt_slots,
    )
    runtime = resolve_collector_runtime(collector=CollectorSettings(data_root=tmp_path / "data"), regular_kis=regular)
    runner = daemon_mod.DaemonRunner(runtime=runtime, shutdown=None, now=lambda: _at(FRI, 8, 0), sleep=lambda s: None)
    runner._gate = gate
    return runner


class _Fetch:
    def __init__(self, *, outcomes) -> None:
        self.outcomes = list(outcomes)
        self.calls = 0

    def __call__(self):
        self.calls += 1
        outcome = self.outcomes.pop(0) if self.outcomes else _rows(3)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


def _rows(count: int) -> list[KiwoomRankingRow]:
    return [
        KiwoomRankingRow(
            rank=i,
            symbol=f"{i:06d}",
            last_price=10_000,
            change_pct=1.0,
            trade_value_krw=10**9 - i,
        )
        for i in range(1, count + 1)
    ]


def _wire(monkeypatch, fetch: _Fetch) -> None:
    monkeypatch.setattr(daemon_mod, "fetch_nxt_trade_amount_ranking_live", fetch)


def test_disabled_without_nxt_slots(monkeypatch, tmp_path) -> None:
    # Given: NXT credential slots are not configured
    gate = _FakeGate(TradingDayStatus.BUSINESS, _trading_day(FRI, next_day=MON))
    runner = _runner(monkeypatch, tmp_path, gate, nxt_slots=())
    fetch = _Fetch(outcomes=[])
    _wire(monkeypatch, fetch)

    # When
    runner._maybe_select_regular_nxt(_at(FRI, 20, 10), standard_session_anchors(FRI))

    # Then
    assert fetch.calls == 0
    assert not runner._paths.regular_nxt_candidates(MON).exists()


def test_before_settle_time_noop_then_fetch_at_settle(monkeypatch, tmp_path) -> None:
    # Given: a business Friday evening with the pool not yet due
    gate = _FakeGate(TradingDayStatus.BUSINESS, _trading_day(FRI, next_day=MON))
    runner = _runner(monkeypatch, tmp_path, gate)
    fetch = _Fetch(outcomes=[])
    _wire(monkeypatch, fetch)
    anchors = standard_session_anchors(FRI)

    # When: 20:09:59 then 20:10:00
    runner._maybe_select_regular_nxt(_at(FRI, 20, 9, 59), anchors)
    assert fetch.calls == 0
    runner._maybe_select_regular_nxt(_at(FRI, 20, 10), anchors)

    # Then
    assert fetch.calls == 1
    assert runner._paths.regular_nxt_candidates(MON).exists()


def test_writes_next_business_day_file_once(monkeypatch, tmp_path, caplog) -> None:
    # Given
    caplog.set_level(logging.INFO)
    gate = _FakeGate(TradingDayStatus.BUSINESS, _trading_day(FRI, next_day=MON))
    runner = _runner(monkeypatch, tmp_path, gate)
    fetch = _Fetch(outcomes=[])
    _wire(monkeypatch, fetch)
    anchors = standard_session_anchors(FRI)

    # When: two evening steps on the same day
    runner._maybe_select_regular_nxt(_at(FRI, 20, 10), anchors)
    runner._maybe_select_regular_nxt(_at(FRI, 20, 11), anchors)

    # Then: one fetch, Monday's file, day marked, one OK milestone with the top value
    assert fetch.calls == 1
    assert regular_nxt_pool_ready(runner._paths.regular_nxt_candidates(MON), target_date=MON, max_candidates=20)
    assert runner.state.regular_nxt_pool_day == FRI
    ok = [r.getMessage() for r in caplog.records if "regular_nxt_pool status=OK" in r.getMessage()]
    assert ok == [f"[DATA] stage=regular_nxt_pool status=OK target={MON.isoformat()} symbols=3 top_value_krw={10**9 - 1}"]


def test_shifted_next_day_skips(monkeypatch, tmp_path, caplog) -> None:
    # Given: the next business day has shifted anchors
    caplog.set_level(logging.INFO)
    shifted = dataclasses.replace(standard_session_anchors(MON), regular_open=dt.time(10, 0))
    gate = _FakeGate(TradingDayStatus.BUSINESS, _trading_day(FRI, next_day=MON, next_anchors=shifted))
    runner = _runner(monkeypatch, tmp_path, gate)
    fetch = _Fetch(outcomes=[])
    _wire(monkeypatch, fetch)

    # When
    runner._maybe_select_regular_nxt(_at(FRI, 20, 10), standard_session_anchors(FRI))

    # Then
    assert fetch.calls == 0
    assert not runner._paths.regular_nxt_candidates(MON).exists()
    assert runner.state.regular_nxt_pool_day == FRI
    assert any("reason=shifted_next_day" in r.getMessage() for r in caplog.records)


def test_existing_ready_file_not_overwritten(monkeypatch, tmp_path, caplog) -> None:
    # Given: a valid Monday pool already on disk
    caplog.set_level(logging.INFO)
    gate = _FakeGate(TradingDayStatus.BUSINESS, _trading_day(FRI, next_day=MON))
    runner = _runner(monkeypatch, tmp_path, gate)
    fetch = _Fetch(outcomes=[])
    _wire(monkeypatch, fetch)
    seed = _Fetch(outcomes=[])
    monkeypatch.setattr(daemon_mod, "fetch_nxt_trade_amount_ranking_live", seed)
    runner._maybe_select_regular_nxt(_at(FRI, 20, 10), standard_session_anchors(FRI))
    path = runner._paths.regular_nxt_candidates(MON)
    before = path.read_text(encoding="utf-8")
    runner.state.regular_nxt_pool_day = None
    _wire(monkeypatch, fetch)

    # When
    runner._maybe_select_regular_nxt(_at(FRI, 20, 20), standard_session_anchors(FRI))

    # Then: no fetch, file untouched, SKIP logged
    assert fetch.calls == 0
    assert path.read_text(encoding="utf-8") == before
    assert any("reason=pool_ready" in r.getMessage() for r in caplog.records)


def test_failure_retries_with_one_warning(monkeypatch, tmp_path, caplog) -> None:
    # Given: the vendor fails twice, then succeeds
    caplog.set_level(logging.DEBUG)
    gate = _FakeGate(TradingDayStatus.BUSINESS, _trading_day(FRI, next_day=MON))
    runner = _runner(monkeypatch, tmp_path, gate)
    fetch = _Fetch(outcomes=[KiwoomTickTapeError("vendor"), KiwoomTickTapeError("vendor"), _rows(3)])
    _wire(monkeypatch, fetch)
    anchors = standard_session_anchors(FRI)

    # When: first failure, a poll inside the retry window, second failure, then the retry
    runner._maybe_select_regular_nxt(_at(FRI, 20, 10), anchors)
    runner._maybe_select_regular_nxt(_at(FRI, 20, 10, 30), anchors)
    runner._maybe_select_regular_nxt(_at(FRI, 20, 11), anchors)
    runner._maybe_select_regular_nxt(_at(FRI, 20, 12), anchors)

    # Then: retry spacing honored, WARNING only on the first failure of the day
    assert fetch.calls == 3
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING and "regular_nxt_pool" in r.getMessage()]
    assert len(warnings) == 1
    assert "status=FAIL" in warnings[0].getMessage()
    assert runner.state.regular_nxt_pool_day == FRI
    assert runner.state.next_regular_nxt_pool_at is None


def test_short_list_is_written_as_available(monkeypatch, tmp_path, caplog) -> None:
    # Given: the ranking returns fewer rows than capacity
    caplog.set_level(logging.INFO)
    gate = _FakeGate(TradingDayStatus.BUSINESS, _trading_day(FRI, next_day=MON))
    runner = _runner(monkeypatch, tmp_path, gate)
    fetch = _Fetch(outcomes=[_rows(2)])
    _wire(monkeypatch, fetch)

    # When
    runner._maybe_select_regular_nxt(_at(FRI, 20, 10), standard_session_anchors(FRI))

    # Then: the file holds what exists and the OK line reports the count
    snapshot = daemon_mod.read_candidate_snapshot(
        runner._paths.regular_nxt_candidates(MON),
        expected_session_date=MON,
        expected_session="regular_nxt",
        max_candidates=20,
    )
    assert snapshot.selected_count == 2 < snapshot.capacity
    assert any("symbols=2" in r.getMessage() for r in caplog.records)


def test_non_business_day_is_noop(monkeypatch, tmp_path) -> None:
    # Given: today is a holiday for the gate
    gate = _FakeGate(TradingDayStatus.HOLIDAY)
    runner = _runner(monkeypatch, tmp_path, gate)
    fetch = _Fetch(outcomes=[])
    _wire(monkeypatch, fetch)

    # When
    runner._maybe_select_regular_nxt(_at(FRI, 20, 10), standard_session_anchors(FRI))

    # Then
    assert fetch.calls == 0
    assert runner.state.regular_nxt_pool_day is None


def test_unresolved_next_anchors_are_resolved_from_calendar(monkeypatch, tmp_path) -> None:
    # Given: the gate did not supply next-day anchors
    gate = _FakeGate(TradingDayStatus.BUSINESS, _trading_day(FRI, next_day=MON, next_anchors=None))
    runner = _runner(monkeypatch, tmp_path, gate)
    fetch = _Fetch(outcomes=[_rows(3)])
    _wire(monkeypatch, fetch)
    resolved: list[dt.date] = []

    def _resolve(directory, day) -> SessionAnchors:
        resolved.append(day)
        return standard_session_anchors(day)

    monkeypatch.setattr(daemon_mod, "resolve_session_anchors", _resolve)

    # When
    runner._maybe_select_regular_nxt(_at(FRI, 20, 10), standard_session_anchors(FRI))

    # Then
    assert resolved == [MON]
    assert fetch.calls == 1


def test_runs_independent_of_premarket_flag(monkeypatch, tmp_path) -> None:
    # Given: premarket is disabled (default) while NXT regular is configured
    gate = _FakeGate(TradingDayStatus.BUSINESS, _trading_day(FRI, next_day=MON))
    runner = _runner(monkeypatch, tmp_path, gate)
    assert runner._premarket_cfg.enabled is False
    fetch = _Fetch(outcomes=[_rows(3)])
    _wire(monkeypatch, fetch)

    # When
    runner._maybe_select_regular_nxt(_at(FRI, 20, 10), standard_session_anchors(FRI))

    # Then
    assert fetch.calls == 1
