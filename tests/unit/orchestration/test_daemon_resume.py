"""Restart resume of SESSION children from on-disk evidence of the day's orchestration; never re-runs orchestration."""

from __future__ import annotations

import datetime as dt
import logging
from zoneinfo import ZoneInfo

import pytest

import src.orchestration.daemon as daemon_mod
from src.core.alerts import PageKind, page_kind
from src.core.calendar import SessionState, get_target_state, schedule_for
from src.core.config import CollectorSettings, RegularKisSettings, resolve_collector_runtime
from src.core.session_anchors import standard_session_anchors
from src.marketdata.toss_calendar import TradingDay
from src.orchestration.daemon import ResumeRefusal, StreamChildKey
from src.orchestration.trading_day_gate import TradingDayStatus, TradingDayView
from src.realtime.manifest import SessionManifest
from src.universe.ipc import write_candidates
from src.universe.membership import read_membership

KST = ZoneInfo("Asia/Seoul")
DAY = dt.date(2026, 9, 16)
PREV = dt.date(2026, 9, 15)
EXPIRED_REV = 20260914
WALL_NS = 1_786_000_000_000_000_000
AFTER_MARKET = SessionState.AFTER_MARKET_ACTIVE


class _FakeSupervisor:
    def __init__(self, *, cmd, breaker=None):
        self.cmd = list(cmd)
        self.last_exit_code = None

    def ensure_running(self):
        return "started"

    def is_running(self):
        return True

    def stop(self, *, timeout_s=15.0):
        return "graceful"


class _OrchestrationSpy:
    def __init__(self) -> None:
        self.calls: list[dt.date] = []

    def __call__(self, **kwargs) -> bool:
        self.calls.append(kwargs["today"])
        return True


@pytest.fixture(autouse=True)
def _kis_data_pool(monkeypatch) -> None:
    monkeypatch.setenv("KIS_DATA_SLOTS", "1,2,3")
    for slot in ("1", "2", "3"):
        monkeypatch.setenv(f"KIS_DATA_{slot}_APP_KEY", f"test-resume-app-key-{slot}")
        monkeypatch.setenv(f"KIS_DATA_{slot}_APP_SECRET", f"test-resume-app-secret-{slot}")
        monkeypatch.setenv(f"KIS_DATA_{slot}_HTS_ID", f"test-resume-hts-id-{slot}")
    monkeypatch.setenv("KRX_ALPHA_REGULAR_KIS_PAIR_CAPACITY_PER_CONNECTION", "41")


@pytest.fixture
def spy(monkeypatch) -> _OrchestrationSpy:
    orchestration = _OrchestrationSpy()
    monkeypatch.setattr(daemon_mod, "run_session_orchestration", orchestration)
    monkeypatch.setattr(daemon_mod, "_kis_token_preflight", lambda *a, **k: {})
    return orchestration


def _at(hour: int, minute: int) -> dt.datetime:
    return dt.datetime(DAY.year, DAY.month, DAY.day, hour, minute, tzinfo=KST)


def _symbols(count: int) -> tuple[str, ...]:
    return tuple(f"{i:06d}" for i in range(1, count + 1))


def _view(*, trading: bool = True) -> TradingDayView:
    trading_day = (
        TradingDay(
            date=DAY,
            is_business_day=True,
            previous_business_day=PREV,
            next_business_day=dt.date(2026, 9, 17),
        )
        if trading
        else None
    )
    return TradingDayView(date=DAY, status=TradingDayStatus.BUSINESS, trading_day=trading_day)


def _runner(tmp_path, monkeypatch, *, ls_active_sessions: int = 2) -> daemon_mod.DaemonRunner:
    collector = CollectorSettings(data_root=tmp_path / "data", ls_active_sessions=ls_active_sessions)
    rk = RegularKisSettings(enabled=True, credential_slots=("1", "2", "3"), pair_capacity_per_connection=41)
    runtime = resolve_collector_runtime(collector=collector, regular_kis=rk)
    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)
    return daemon_mod.DaemonRunner(
        runtime=runtime,
        shutdown=None,
        now=lambda: _at(9, 0),
        sleep=lambda s: None,
        wall_ns=lambda: WALL_NS,
    )


def _write_candidates(paths, *, rev: int, count: int = 150) -> None:
    rows = [{"symbol": symbol, "selection_reasons": ["limit_up"]} for symbol in _symbols(count)]
    paths.candidates.parent.mkdir(parents=True, exist_ok=True)
    write_candidates(paths.candidates, rows, rev=rev)


def _morning(tmp_path, monkeypatch, *, ls_active_sessions: int = 2) -> daemon_mod.DaemonRunner:
    """Model the 08:20 orchestration plus manifest creation of the first daemon process."""
    runner = _runner(tmp_path, monkeypatch, ls_active_sessions=ls_active_sessions)
    paths = runner._paths
    _write_candidates(paths, rev=20260915)
    SessionManifest(session_date=DAY, clock_offset_ns=0, started_at_ns=0, frame_counts={}).save(paths.manifest_path(DAY))
    runner._maybe_orchestrate(_at(8, 20), SessionState.STREAMER_ACTIVE, _view())
    runner._supervise_regular_ls_shard(DAY)
    return runner


def test_resume_after_market_reinstalls_children_with_morning_argv(tmp_path, monkeypatch, spy) -> None:
    first = _morning(tmp_path, monkeypatch)
    primary_argv = list(first.children.regular.cmd)
    shard_argv = list(first.children.get(StreamChildKey.REGULAR_LS_B).cmd)
    candidates_bytes = first._paths.candidates.read_bytes()
    spy.calls.clear()

    restarted = _runner(tmp_path, monkeypatch)
    restarted._maybe_orchestrate(_at(16, 30), AFTER_MARKET, _view())
    restarted._supervise_regular_ls_shard(DAY)

    assert restarted.children.regular.cmd == primary_argv
    assert restarted.children.get(StreamChildKey.REGULAR_LS_B).cmd == shard_argv
    assert restarted.state.orchestrated_for == DAY
    assert restarted.state.ls_shard_plan_day == DAY
    assert spy.calls == []
    assert first._paths.candidates.read_bytes() == candidates_bytes


@pytest.mark.parametrize(("state", "hour", "minute"), [(SessionState.FULL_ACTIVE, 11, 0), (SessionState.STREAMER_ACTIVE, 8, 30)])
def test_resume_in_full_and_streamer_states(tmp_path, monkeypatch, spy, state, hour, minute) -> None:
    first = _morning(tmp_path, monkeypatch)
    primary_argv = list(first.children.regular.cmd)
    spy.calls.clear()

    restarted = _runner(tmp_path, monkeypatch)
    restarted._maybe_orchestrate(_at(hour, minute), state, _view())

    assert restarted.children.regular is not None
    assert restarted.children.regular.cmd == primary_argv
    assert restarted.state.orchestrated_for == DAY
    assert spy.calls == []


def test_morning_flow_orchestrates_with_previous_day_candidates(tmp_path, monkeypatch, spy, caplog) -> None:
    runner = _runner(tmp_path, monkeypatch)
    _write_candidates(runner._paths, rev=EXPIRED_REV)

    with caplog.at_level(logging.INFO, logger=daemon_mod.logger.name):
        runner._maybe_orchestrate(_at(8, 20), SessionState.STREAMER_ACTIVE, _view())

    assert spy.calls == [DAY]
    assert runner.children.regular is not None
    assert runner.state.orchestrated_for == DAY
    assert "stage=session_resume" in caplog.text
    assert "reason=candidates_rev_mismatch" in caplog.text
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING and "session_resume" in r.getMessage()]


def _corrupt_candidates(paths) -> None:
    paths.candidates.write_text("{not-json", encoding="utf-8")


REFUSAL_CASES = [
    pytest.param(ResumeRefusal.CANDIDATES_REV_MISMATCH, lambda paths: _write_candidates(paths, rev=EXPIRED_REV), True, id="rev"),
    pytest.param(ResumeRefusal.MANIFEST_MISSING, lambda paths: paths.manifest_path(DAY).unlink(), True, id="manifest"),
    pytest.param(ResumeRefusal.MEMBERSHIP_MISSING, lambda paths: paths.membership_path(DAY).unlink(), True, id="membership"),
    pytest.param(
        ResumeRefusal.MEMBERSHIP_MISSING,
        lambda paths: paths.membership_path(DAY).write_text("not-json\n", encoding="utf-8"),
        True,
        id="membership_corrupt",
    ),
    pytest.param(ResumeRefusal.CALENDAR_UNKNOWN, lambda paths: None, False, id="calendar"),
    pytest.param(ResumeRefusal.CANDIDATES_UNREADABLE, _corrupt_candidates, True, id="candidates"),
]


@pytest.mark.parametrize(("reason", "break_evidence", "trading"), REFUSAL_CASES)
def test_refusal_after_market_fails_closed_and_pages_once(
    tmp_path, monkeypatch, spy, caplog, reason, break_evidence, trading
) -> None:
    first = _morning(tmp_path, monkeypatch)
    break_evidence(first._paths)
    spy.calls.clear()

    restarted = _runner(tmp_path, monkeypatch)
    with caplog.at_level(logging.INFO, logger=daemon_mod.logger.name):
        for _ in range(3):
            restarted._maybe_orchestrate(_at(16, 30), AFTER_MARKET, _view(trading=trading))

    assert restarted.children.regular is None
    assert restarted.children.get(StreamChildKey.REGULAR_LS_B) is None
    assert restarted.state.orchestrated_for is None
    assert spy.calls == []
    criticals = [
        record
        for record in caplog.records
        if record.levelno == logging.CRITICAL and f"stage=session_resume status=REFUSED reason={reason.value}" in record.getMessage()
    ]
    assert len(criticals) == 1
    assert page_kind(criticals[0]) is PageKind.REGULAR_FEED_DOWN


def test_plan_mismatch_refuses_and_clears_plan(tmp_path, monkeypatch, spy) -> None:
    first = _morning(tmp_path, monkeypatch, ls_active_sessions=2)
    membership = first._paths.membership_path(DAY)
    rows_before = len(read_membership(membership))
    spy.calls.clear()

    restarted = _runner(tmp_path, monkeypatch, ls_active_sessions=1)
    restarted._maybe_orchestrate(_at(16, 30), AFTER_MARKET, _view())

    assert restarted.state.ls_shard_plan == ()
    assert restarted.children.regular is None
    assert restarted.children.get(StreamChildKey.REGULAR_LS_B) is None
    assert len(read_membership(membership)) == rows_before
    assert spy.calls == []


@pytest.mark.parametrize("degraded", [False, True])
def test_live_or_degraded_regular_child_is_never_replaced(tmp_path, monkeypatch, spy, caplog, degraded) -> None:
    if degraded:
        runner = _runner(tmp_path, monkeypatch)
        runner._install_stream_child(StreamChildKey.REGULAR, ["degraded-primary"], degraded=True)
    else:
        runner = _morning(tmp_path, monkeypatch)
    supervisor = runner.children.regular
    runner.state.orchestrated_for = None
    spy.calls.clear()
    caplog.clear()

    with caplog.at_level(logging.INFO, logger=daemon_mod.logger.name):
        runner._maybe_orchestrate(_at(16, 30), AFTER_MARKET, _view())

    assert runner.children.regular is supervisor
    assert "REPLACE_LIVE" not in caplog.text
    assert "session_resume" not in caplog.text
    assert spy.calls == []


def test_any_live_session_child_blocks_resume(tmp_path, monkeypatch, spy, caplog) -> None:
    first = _morning(tmp_path, monkeypatch)
    restarted = _runner(tmp_path, monkeypatch)
    restarted._install_stream_child(StreamChildKey.SNAPSHOT, ["snapshot-child"])
    snapshot = restarted.children.get(StreamChildKey.SNAPSHOT)
    spy.calls.clear()
    caplog.clear()

    with caplog.at_level(logging.INFO, logger=daemon_mod.logger.name):
        restarted._maybe_orchestrate(_at(16, 30), AFTER_MARKET, _view())

    assert restarted.children.get(StreamChildKey.SNAPSHOT) is snapshot
    assert restarted.children.regular is None
    assert restarted.state.orchestrated_for is None
    assert "REPLACE_LIVE" not in caplog.text
    assert "session_resume" not in caplog.text
    assert first.children.regular is not None


def test_plan_mismatch_is_evaluated_once_per_state_and_day(tmp_path, monkeypatch, spy, caplog) -> None:
    _morning(tmp_path, monkeypatch, ls_active_sessions=2)
    restarted = _runner(tmp_path, monkeypatch, ls_active_sessions=1)
    caplog.clear()

    with caplog.at_level(logging.INFO, logger=daemon_mod.logger.name):
        for _ in range(3):
            restarted._maybe_orchestrate(_at(16, 30), AFTER_MARKET, _view())

    assert caplog.text.count("stage=collection_tiers") == 1
    assert caplog.text.count("reason=plan_mismatch") == 1
    assert restarted.state.ls_shard_plan == ()
    assert restarted.children.regular is None


def test_transient_plan_failure_is_retried_not_latched(tmp_path, monkeypatch, spy) -> None:
    _morning(tmp_path, monkeypatch)
    spy.calls.clear()
    restarted = _runner(tmp_path, monkeypatch)
    real_plan = daemon_mod.plan_ls_shards
    attempts: list[int] = []

    def flaky_plan(*args, **kwargs):
        attempts.append(1)
        if len(attempts) == 1:
            raise OSError("transient candidates read failure")
        return real_plan(*args, **kwargs)

    monkeypatch.setattr(daemon_mod, "plan_ls_shards", flaky_plan)

    restarted._maybe_orchestrate(_at(16, 30), AFTER_MARKET, _view())
    assert restarted.children.regular is None
    assert restarted.state.ls_shard_plan_day is None

    restarted._maybe_orchestrate(_at(16, 30), AFTER_MARKET, _view())
    assert restarted.children.regular is not None
    assert restarted.state.orchestrated_for == DAY
    assert spy.calls == []


def test_manifest_oserror_is_a_refusal_not_a_crash(tmp_path, monkeypatch, spy) -> None:
    _morning(tmp_path, monkeypatch)
    restarted = _runner(tmp_path, monkeypatch)
    manifest = restarted._paths.manifest_path(DAY)
    real_exists = type(manifest).exists

    def deny(self, *args, **kwargs):
        if self == manifest:
            raise PermissionError("denied")
        return real_exists(self, *args, **kwargs)

    monkeypatch.setattr(type(manifest), "exists", deny)

    restarted._maybe_orchestrate(_at(16, 30), AFTER_MARKET, _view())

    assert restarted.children.regular is None
    assert restarted.state.orchestrated_for is None


def test_membership_not_duplicated_after_resume(tmp_path, monkeypatch, spy) -> None:
    first = _morning(tmp_path, monkeypatch)
    membership = first._paths.membership_path(DAY)
    before = read_membership(membership)
    assert len(before) == 150

    restarted = _runner(tmp_path, monkeypatch)
    restarted._maybe_orchestrate(_at(16, 30), AFTER_MARKET, _view())

    assert read_membership(membership) == before


def test_post_market_eod_never_resumes(tmp_path, monkeypatch, spy) -> None:
    first = _morning(tmp_path, monkeypatch)
    spy.calls.clear()

    restarted = _runner(tmp_path, monkeypatch)
    now = _at(15, 45)
    anchors = standard_session_anchors(DAY)
    assert get_target_state(now, schedule_for(anchors, restarted._sched)) is SessionState.POST_MARKET_EOD

    eod_calls: list[object] = []
    orchestrate_calls: list[object] = []
    monkeypatch.setattr(restarted, "_resolve_anchors", lambda now: anchors)
    monkeypatch.setattr(restarted, "_resolve_active_day", lambda now, state, anchors: (_view(), anchors))
    monkeypatch.setattr(
        restarted, "_step_post_market_eod", lambda now, day, anchors: eod_calls.append(day) or 10.0
    )
    monkeypatch.setattr(restarted, "_maybe_orchestrate", lambda *a, **k: orchestrate_calls.append(a))
    restarted.step(now)

    assert len(eod_calls) == 1
    assert orchestrate_calls == []
    assert restarted.children.regular is None
    assert restarted.children.get(StreamChildKey.REGULAR_LS_B) is None
    assert first.children.regular is not None
