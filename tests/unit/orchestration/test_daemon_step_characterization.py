"""Characterization net for the DaemonRunner.step decomposition (refactor P3).

The golden timeline pins observable behavior of ``step``: returned sleep,
daemon logger records, supervisor/stub call order, and resulting state.
It is generated on the pre-refactor tree and must pass unchanged after it.
"""

from __future__ import annotations

import ast
import dataclasses
import datetime as dt
import json
import logging
import pathlib
import sys
from zoneinfo import ZoneInfo

import pytest

import src.orchestration.daemon as daemon_mod
from src.core.config import CollectorSettings, SnapshotSettings, resolve_collector_runtime

KST = ZoneInfo("Asia/Seoul")
GOLDEN_PATH = pathlib.Path(__file__).parent / "golden" / "daemon_step_timeline.json"

D = dt.date(2026, 9, 16)
D1 = dt.date(2026, 9, 17)
D2 = dt.date(2026, 9, 18)


@pytest.fixture(autouse=True)
def _verified_aftermarket_capacity(monkeypatch) -> None:
    """Daemon tests simulate a deployed host with verified aftermarket capacity."""
    monkeypatch.setenv("KRX_ALPHA_AFTERMARKET_PAIR_CAPACITY_PER_CONNECTION", "4")


def _trading_day(day: dt.date, *, business: bool):
    from src.marketdata.toss_calendar import TradingDay

    delta = dt.timedelta(days=1)
    return TradingDay(
        date=day,
        is_business_day=business,
        previous_business_day=day - delta,
        next_business_day=day + delta,
    )


def _norm_argv(argv: list[str], tmp_path: pathlib.Path) -> list[str]:
    out: list[str] = []
    for part in argv:
        if part == sys.executable:
            out.append("<PY>")
        else:
            out.append(str(part).replace(str(tmp_path), "<TMP>"))
    return out


def _norm_args(args: object, tmp_path: pathlib.Path) -> object:
    if isinstance(args, (list, tuple)):
        return [_norm_args(a, tmp_path) for a in args]
    if isinstance(args, dict):
        return {k: _norm_args(v, tmp_path) for k, v in args.items()}
    if isinstance(args, (dt.datetime, dt.date, dt.time)):
        return args.isoformat()
    if isinstance(args, pathlib.Path):
        return str(args).replace(str(tmp_path), "<TMP>")
    if isinstance(args, str):
        return args.replace(str(tmp_path), "<TMP>").replace(sys.executable, "<PY>")
    return args


class _TimelineHarness:
    """Stubbed DaemonRunner with a recording supervisor and call log."""

    def __init__(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: pathlib.Path,
        *,
        holidays: set[dt.date],
        orch_outcomes: list[bool],
        ensure_script: dict[str, list[str]] | None = None,
        eod_ready: bool = False,
    ) -> None:
        monkeypatch.setenv("KRX_ALPHA_SNAPSHOT_ENABLED", "true")
        monkeypatch.setenv("KRX_ALPHA_TOSS_PROGRAM_AUTO_BACKFILL_ENABLED", "false")
        monkeypatch.setenv("KRX_ALPHA_AFTERMARKET_PAIR_CAPACITY_PER_CONNECTION", "4")
        self.tmp_path = tmp_path
        self.calls: list[list] = []
        self.orch_outcomes = list(orch_outcomes)
        self.ensure_script = ensure_script or {}
        self._ensure_counts: dict[str, int] = {}
        self.holidays = holidays
        self._snap_rev = 20260915

        def _resolve(day: dt.date, cache: pathlib.Path, anchors_dir: pathlib.Path):
            if day in holidays:
                return _trading_day(day, business=False)
            return _trading_day(day, business=True)

        monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", _resolve)
        monkeypatch.setattr(daemon_mod, "_kis_token_preflight", lambda *a, **k: {})
        monkeypatch.setattr(daemon_mod, "_build_kis_client", lambda *a, **k: object())
        monkeypatch.setattr(daemon_mod, "load_kis_data_credentials", lambda: ())

        def _orch(**kw):
            result = self.orch_outcomes.pop(0) if self.orch_outcomes else True
            self.calls.append(["orchestration", kw["today"].isoformat(), result])
            return result

        monkeypatch.setattr(daemon_mod, "run_session_orchestration", _orch)
        monkeypatch.setattr(daemon_mod, "aftermarket_eod_ready", lambda **kw: eod_ready)
        monkeypatch.setattr(daemon_mod, "check_session_reconciliation", lambda **kw: True)

        def _housekeeping(cfg, paths, ref_day, *, progress):
            self.calls.append(["housekeeping", ref_day.isoformat()])
            return daemon_mod._EodHousekeeping(deleted=1, uploaded=2, purged=3, maintenance_ok=True, offload_ok=True)

        monkeypatch.setattr(daemon_mod, "_run_eod_housekeeping", _housekeeping)
        monkeypatch.setattr(daemon_mod, "_check_backup", lambda *a, **k: ([], True, "ok"))
        monkeypatch.setattr(daemon_mod, "send_digest", lambda s, b: self.calls.append(["digest", s]) or True)

        def _refresh(*, session_date, generated_at, client, out_path, capacity, excluded_symbols):
            from src.universe.ipc import CandidateSnapshot, write_candidate_snapshot

            self.calls.append(["refresh_aftermarket", session_date.isoformat(), len(excluded_symbols)])
            snapshot = CandidateSnapshot(
                schema_version=1,
                rev=int(session_date.strftime("%Y%m%d")),
                session_date=session_date,
                session="aftermarket",
                generated_at=generated_at,
                source_asof=generated_at,
                effective_from=generated_at,
                policy_version="aftermarket_v1",
                capacity=capacity,
                eligible_count=2,
                selected_count=2,
                candidates=(
                    {
                        "symbol": "005930",
                        "rank": 1,
                        "source_ranks": {"trade_amount": 1},
                        "metrics": {"trade_value_krw": 1, "change_pct": 1.0},
                        "selection_reasons": ["trade_amount"],
                    },
                    {
                        "symbol": "000660",
                        "rank": 2,
                        "source_ranks": {"trade_amount": 2},
                        "metrics": {"trade_value_krw": 1, "change_pct": 1.0},
                        "selection_reasons": ["trade_amount"],
                    },
                ),
            )
            write_candidate_snapshot(out_path, snapshot)
            return snapshot

        monkeypatch.setattr(daemon_mod, "refresh_aftermarket_candidates", _refresh)

        def _plan(*, symbols, credentials, pair_capacity_per_connection, krx_streams, nxt_streams):
            from src.realtime.contracts import MarketVenue
            from src.realtime.kis_sharding import AftermarketShard

            self.calls.append(["plan_shards", [str(s) for s in symbols]])
            return (
                AftermarketShard(MarketVenue.NXT, 0, ("005930",), nxt_streams, "0", "id0"),
                AftermarketShard(MarketVenue.KRX, 0, ("000660",), krx_streams, "1", "id1"),
            )

        monkeypatch.setattr(daemon_mod, "plan_aftermarket_shards", _plan)

        harness = self

        class _FakeSupervisor:
            def __init__(self, *, cmd, breaker=None):
                self.cmd = list(cmd)
                self.last_exit_code: int | None = None
                argv = _norm_argv(self.cmd, tmp_path)
                if any("collect-aftermarket" in c for c in self.cmd):
                    venue = self.cmd[self.cmd.index("--venue") + 1]
                    idx = self.cmd[self.cmd.index("--shard-index") + 1]
                    self._key = f"{venue}:{idx}"
                elif any("collect-snapshots" in c for c in self.cmd):
                    self._key = "snapshot"
                else:
                    self._key = "regular"
                harness.calls.append(["new", self._key, argv])

            def ensure_running(self):
                n = harness._ensure_counts.get(self._key, 0)
                harness._ensure_counts[self._key] = n + 1
                script = harness.ensure_script.get(self._key, [])
                result = script[min(n, len(script) - 1)] if script else "running"
                if result == "restarted":
                    self.last_exit_code = -9
                harness.calls.append(["ensure", self._key, result])
                return result

            def stop(self, *, timeout_s: float = 15.0):
                harness.calls.append(["stop", self._key, timeout_s])
                return "graceful"

        monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)
        monkeypatch.setattr(
            daemon_mod.subprocess, "Popen", lambda *a, **k: (_ for _ in ()).throw(AssertionError("no Popen"))
        )

        settings = CollectorSettings(data_root=tmp_path / "data", after_market_enabled=True)
        runtime = resolve_collector_runtime(collector=settings, snapshot=SnapshotSettings(enabled=True))
        self.runner = daemon_mod.DaemonRunner(
            runtime=runtime, shutdown=None, now=lambda: self._now, sleep=lambda s: None
        )
        self._now = dt.datetime(2026, 9, 16, 8, 25, tzinfo=KST)
        self._records: list[logging.LogRecord] = []
        handler = logging.Handler()
        handler.emit = self._records.append  # type: ignore[method-assign]
        self._old_level = daemon_mod.logger.level
        daemon_mod.logger.setLevel(logging.DEBUG)
        daemon_mod.logger.addHandler(handler)
        self._handler = handler
        self._seen = 0

    def close(self) -> None:
        daemon_mod.logger.removeHandler(self._handler)
        daemon_mod.logger.setLevel(self._old_level)

    def step(self, now: dt.datetime):
        self._now = now
        base_calls = len(self.calls)
        base_records = len(self._records)
        sleep = self.runner.step(now)
        new_calls = self.calls[base_calls:]
        new_records = self._records[base_records:]
        logs = [
            [r.levelname, r.msg, _norm_args(r.args, self.tmp_path), bool(getattr(r, "krx_event", False))]
            for r in new_records
            if r.name == daemon_mod.logger.name
        ]
        state = _state_dict(self.runner.state)
        return {"sleep": sleep, "logs": logs, "calls": new_calls, "state": state}


def _state_dict(st) -> dict:
    raw = dataclasses.asdict(st)
    out: dict = {}
    for key in sorted(raw):
        out[key] = _norm_state_value(raw[key])
    return out


def _norm_state_value(value):
    if isinstance(value, dt.datetime):
        return value.isoformat()
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, dt.time):
        return value.isoformat()
    if isinstance(value, (set, frozenset)):
        return sorted(str(v) for v in value)
    if isinstance(value, (list, tuple)):
        return [_norm_state_value(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _norm_state_value(v) for k, v in sorted(value.items(), key=lambda kv: str(kv[0]))}
    return value


def _timeline_instants() -> list[dt.datetime]:
    return [
        dt.datetime(2026, 9, 16, 8, 25, tzinfo=KST),
        dt.datetime(2026, 9, 16, 8, 26, tzinfo=KST),
        dt.datetime(2026, 9, 16, 8, 31, tzinfo=KST),
        dt.datetime(2026, 9, 16, 9, 10, tzinfo=KST),
        dt.datetime(2026, 9, 16, 15, 32, tzinfo=KST),
        dt.datetime(2026, 9, 16, 15, 45, tzinfo=KST),
        dt.datetime(2026, 9, 16, 16, 5, tzinfo=KST),
        dt.datetime(2026, 9, 16, 20, 5, tzinfo=KST),
        dt.datetime(2026, 9, 16, 20, 6, tzinfo=KST),
        dt.datetime(2026, 9, 16, 22, 0, tzinfo=KST),
        dt.datetime(2026, 9, 17, 9, 0, tzinfo=KST),
        dt.datetime(2026, 9, 17, 20, 5, tzinfo=KST),
        dt.datetime(2026, 9, 18, 8, 25, tzinfo=KST),
    ]


def _run_timeline(harness: _TimelineHarness) -> list[dict]:
    from src.universe.ipc import write_candidates

    harness.runner._paths.candidates.parent.mkdir(parents=True, exist_ok=True)
    write_candidates(
        harness.runner._paths.candidates,
        [{"symbol": "005930", "selection_reasons": ["limit_up"]}],
        rev=20260915,
    )
    steps: list[dict] = []
    for instant in _timeline_instants():
        if instant == dt.datetime(2026, 9, 17, 9, 0, tzinfo=KST):
            harness.runner._children.regular = daemon_mod.ProcessSupervisor(
                cmd=["collect-stream", "--session-date", "2026-09-16"], breaker=None
            )
            harness.calls.pop()
            harness.runner._children.snapshot = daemon_mod.ProcessSupervisor(
                cmd=["collect-snapshots", "--session-date", "2026-09-16"], breaker=None
            )
            harness.calls.pop()
            harness.runner._children.aftermarket["krx:9"] = daemon_mod.ProcessSupervisor(
                cmd=["collect-aftermarket", "--venue", "krx", "--shard-index", "9"], breaker=None
            )
            harness.calls.pop()
            harness.runner.state.degraded_active = True
            harness.runner.state.last_supervisor_result = "running"
            harness.runner.state.last_snapshot_result = "running"
            harness.runner.state.aftermarket_results = {"krx:9": "running"}
            harness.runner.state.aftermarket_circuit_alerted = {"krx:9"}
        if instant == dt.datetime(2026, 9, 18, 8, 25, tzinfo=KST):
            harness.runner._children.regular = daemon_mod.ProcessSupervisor(
                cmd=["collect-stream", "--session-date", "2026-09-16"], breaker=None
            )
            harness.calls.pop()
        entry = harness.step(instant)
        entry["instant"] = instant.isoformat()
        steps.append(entry)
    return steps


def _stage_multiset() -> list[str]:
    source = pathlib.Path(daemon_mod.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    found: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == "DaemonRunner":
            found.extend(
                child.value
                for child in ast.walk(node)
                if isinstance(child, ast.Constant) and isinstance(child.value, str) and "stage=" in child.value
            )
    return sorted(found)


def test_golden_timeline_equivalence(tmp_path, monkeypatch) -> None:
    harness = _TimelineHarness(
        monkeypatch,
        tmp_path,
        holidays={D1},
        orch_outcomes=[False, True],
        ensure_script={"nxt:0": ["running", "circuit_open"], "krx:0": ["restarted"]},
        eod_ready=False,
    )
    try:
        steps = _run_timeline(harness)
    finally:
        harness.close()
    golden = json.loads(GOLDEN_PATH.read_text(encoding="utf-8"))
    assert steps == golden["steps"]


def test_holiday_reset_set(tmp_path, monkeypatch, caplog) -> None:
    harness = _TimelineHarness(monkeypatch, tmp_path, holidays={D}, orch_outcomes=[])
    try:
        harness.runner.state.degraded_active = True
        harness.runner.state.last_supervisor_result = "running"
        harness.runner.state.last_snapshot_result = "running"
        harness.runner.state.aftermarket_results = {"krx:0": "running"}
        harness.runner.state.aftermarket_circuit_alerted = {"krx:0"}
        harness.runner._children.regular = daemon_mod.ProcessSupervisor(
            cmd=["collect-stream", "--session-date", "2026-09-16"], breaker=None
        )
        harness.runner._children.snapshot = daemon_mod.ProcessSupervisor(
            cmd=["collect-snapshots", "--session-date", "2026-09-16"], breaker=None
        )
        harness.runner._children.aftermarket["krx:0"] = daemon_mod.ProcessSupervisor(
            cmd=["collect-aftermarket", "--venue", "krx", "--shard-index", "0"], breaker=None
        )
        harness.calls.clear()
        with caplog.at_level(logging.DEBUG, logger=daemon_mod.logger.name):
            sleep = harness.runner.step(dt.datetime(2026, 9, 16, 9, 0, tzinfo=KST))
    finally:
        harness.close()
    kinds = [c[0] for c in harness.calls]
    assert kinds == ["stop", "stop", "stop"]
    assert [c[1] for c in harness.calls] == ["regular", "snapshot", "krx:0"]
    assert all(c[2] == 15.0 for c in harness.calls)
    assert harness.runner.children.regular is None
    assert harness.runner.children.snapshot is None
    assert harness.runner.children.aftermarket == {}
    assert harness.runner.state.last_supervisor_result is None
    assert harness.runner.state.last_snapshot_result is None
    assert harness.runner.state.degraded_active is False
    assert harness.runner.state.aftermarket_results == {}
    assert harness.runner.state.aftermarket_circuit_alerted == {"krx:0"}
    assert not any("streamer_stop" in r.getMessage() or "STOP_STALE_DAY" in r.getMessage() for r in caplog.records)
    assert sleep == min(
        daemon_mod.calc_sleep_seconds(
            dt.datetime(2026, 9, 16, 9, 0, tzinfo=KST),
            harness.runner._sched.market_close,
        ),
        daemon_mod.HOLIDAY_SLEEP_CAP_S,
    )


def test_holiday_without_regular_keeps_degraded_flag(tmp_path, monkeypatch) -> None:
    harness = _TimelineHarness(monkeypatch, tmp_path, holidays={D}, orch_outcomes=[])
    try:
        harness.runner.state.degraded_active = True
        harness.runner.step(dt.datetime(2026, 9, 16, 9, 0, tzinfo=KST))
    finally:
        harness.close()
    assert harness.runner.state.degraded_active is True


def test_rollover_reset_set(tmp_path, monkeypatch, caplog) -> None:
    harness = _TimelineHarness(monkeypatch, tmp_path, holidays=set(), orch_outcomes=[False])
    try:
        st = harness.runner.state
        st.orchestration_day = D - dt.timedelta(days=1)
        st.orchestration_attempts = 3
        st.ingest_stale = True
        st.degraded_active = True
        st.last_ingest_check = dt.datetime(2026, 9, 15, 15, 0, tzinfo=KST)
        st.next_aftermarket_refresh_at = dt.datetime(2026, 9, 15, 15, 31, tzinfo=KST)
        st.aftermarket_plan_day = D - dt.timedelta(days=1)
        st.aftermarket_circuit_alerted = {"krx:0"}
        harness.runner._children.regular = daemon_mod.ProcessSupervisor(
            cmd=["collect-stream", "--session-date", "2026-09-15"], breaker=None
        )
        harness.runner._children.snapshot = daemon_mod.ProcessSupervisor(
            cmd=["collect-snapshots", "--session-date", "2026-09-15"], breaker=None
        )
        harness.runner._children.aftermarket["krx:0"] = daemon_mod.ProcessSupervisor(
            cmd=["collect-aftermarket", "--venue", "krx", "--shard-index", "0"], breaker=None
        )
        harness.calls.clear()
        with caplog.at_level(logging.DEBUG, logger=daemon_mod.logger.name):
            harness.runner.step(dt.datetime(2026, 9, 16, 8, 25, tzinfo=KST))
    finally:
        harness.close()
    stale = [r for r in caplog.records if "STOP_STALE_DAY" in r.getMessage()]
    assert len(stale) == 1
    assert stale[0].levelno == logging.WARNING
    assert getattr(stale[0], "krx_event", False) is False
    assert harness.runner.children.regular is None  # stopped, no degraded replacement without candidates
    assert harness.runner.children.snapshot is None
    assert harness.runner.state.last_supervisor_result is None
    assert harness.runner.state.last_snapshot_result is None
    assert harness.runner.state.orchestration_day == D
    assert harness.runner.state.next_orchestration_at is not None
    assert harness.runner.state.orchestration_attempts == 1
    assert harness.runner.state.degraded_active is False
    assert harness.runner.state.last_ingest_check is None
    assert harness.runner.state.ingest_stale is False
    assert harness.runner.state.next_aftermarket_refresh_at is None
    assert "krx:0" in harness.runner.children.aftermarket
    assert harness.runner.state.aftermarket_plan_day == D - dt.timedelta(days=1)
    assert harness.runner.state.aftermarket_circuit_alerted == {"krx:0"}


def test_eod_reset_set_once_per_date(tmp_path, monkeypatch, caplog) -> None:
    harness = _TimelineHarness(monkeypatch, tmp_path, holidays=set(), orch_outcomes=[], eod_ready=True)
    try:
        harness.runner._children.regular = daemon_mod.ProcessSupervisor(
            cmd=["collect-stream", "--session-date", "2026-09-16"], breaker=None
        )
        harness.runner._children.snapshot = daemon_mod.ProcessSupervisor(
            cmd=["collect-snapshots", "--session-date", "2026-09-16"], breaker=None
        )
        harness.runner._children.aftermarket["krx:0"] = daemon_mod.ProcessSupervisor(
            cmd=["collect-aftermarket", "--venue", "krx", "--shard-index", "0"], breaker=None
        )
        harness.runner.state.aftermarket_results = {"krx:0": "running"}
        harness.runner.state.aftermarket_circuit_alerted = {"krx:0"}
        harness.calls.clear()
        with caplog.at_level(logging.DEBUG, logger=daemon_mod.logger.name):
            first = harness.runner.step(dt.datetime(2026, 9, 16, 20, 5, tzinfo=KST))
            stops_after_first = list(harness.calls)
            digests_after_first = [c for c in harness.calls if c[0] == "digest"]
            second = harness.runner.step(dt.datetime(2026, 9, 16, 20, 6, tzinfo=KST))
    finally:
        harness.close()
    stop_keys = [c[1] for c in stops_after_first if c[0] == "stop"]
    assert stop_keys == ["regular", "snapshot", "krx:0"]
    assert all(c[2] == 15.0 for c in stops_after_first if c[0] == "stop")
    streamer_logs = [r for r in caplog.records if "streamer_stop result=" in r.getMessage()]
    assert len(streamer_logs) == 1
    assert streamer_logs[0].levelno == logging.INFO
    assert getattr(streamer_logs[0], "krx_event", False) is True
    assert harness.runner.children.aftermarket == {}
    assert harness.runner.state.aftermarket_results == {}
    assert harness.runner.state.aftermarket_circuit_alerted == set()
    assert first == 60.0
    assert second == 60.0
    assert len(digests_after_first) == 1
    second_calls = harness.calls[len(stops_after_first) :]
    assert not any(c[0] == "stop" for c in second_calls)
    assert not any(c[0] == "digest" for c in second_calls)


def test_ready_replace_live_non_degraded_stops_and_alerts(tmp_path, monkeypatch, caplog) -> None:
    harness = _TimelineHarness(monkeypatch, tmp_path, holidays=set(), orch_outcomes=[True])
    try:
        old = daemon_mod.ProcessSupervisor(cmd=["old"], breaker=None)
        harness.runner._children.regular = old
        harness.runner.state.orchestration_day = D
        harness.runner.state.orchestrated_for = None
        harness.calls.clear()
        with caplog.at_level(logging.DEBUG, logger=daemon_mod.logger.name):
            harness.runner.step(dt.datetime(2026, 9, 16, 8, 25, tzinfo=KST))
    finally:
        harness.close()
    stops = [c[2] for c in harness.calls if c[0] == "stop" and c[1] == "regular"]
    assert stops == [15.0]
    assert harness.runner.children.regular is not old
    live = [r for r in caplog.records if "REPLACE_LIVE" in r.getMessage()]
    assert len(live) == 1
    assert live[0].levelno == logging.CRITICAL
    assert "slot=regular" in live[0].getMessage()
    assert not any("REPLACE_DEGRADED" in r.getMessage() for r in caplog.records)
    stop_idx = next(i for i, c in enumerate(harness.calls) if c[0] == "stop" and c[1] == "regular")
    new_idx = next(i for i, c in enumerate(harness.calls) if c[0] == "new" and c[1] == "regular")
    assert stop_idx < new_idx


def test_ready_replace_live_snapshot_alerts(tmp_path, monkeypatch, caplog) -> None:
    harness = _TimelineHarness(monkeypatch, tmp_path, holidays=set(), orch_outcomes=[True])
    try:
        harness.runner._children.regular = daemon_mod.ProcessSupervisor(cmd=["old"], breaker=None)
        harness.runner._children.snapshot = daemon_mod.ProcessSupervisor(cmd=["collect-snapshots"], breaker=None)
        harness.runner.state.orchestration_day = D
        harness.runner.state.orchestrated_for = None
        harness.calls.clear()
        with caplog.at_level(logging.DEBUG, logger=daemon_mod.logger.name):
            harness.runner.step(dt.datetime(2026, 9, 16, 8, 25, tzinfo=KST))
    finally:
        harness.close()
    stop_idx = next(i for i, c in enumerate(harness.calls) if c[0] == "stop" and c[1] == "snapshot")
    new_idx = next(i for i, c in enumerate(harness.calls) if c[0] == "new" and c[1] == "snapshot")
    assert stop_idx < new_idx
    snap_live = [r for r in caplog.records if "REPLACE_LIVE" in r.getMessage() and "slot=snapshot" in r.getMessage()]
    assert len(snap_live) == 1
    assert snap_live[0].levelno == logging.CRITICAL


def test_degraded_upgrade_silent_snapshot_replace(tmp_path, monkeypatch, caplog) -> None:
    from src.universe.ipc import write_candidates

    harness = _TimelineHarness(monkeypatch, tmp_path, holidays=set(), orch_outcomes=[False, True])
    try:
        harness.runner._paths.candidates.parent.mkdir(parents=True, exist_ok=True)
        write_candidates(
            harness.runner._paths.candidates,
            [{"symbol": "005930", "selection_reasons": ["limit_up"]}],
            rev=20260915,
        )
        with caplog.at_level(logging.DEBUG, logger=daemon_mod.logger.name):
            harness.step(dt.datetime(2026, 9, 16, 8, 25, tzinfo=KST))
            assert harness.runner.state.degraded_active is True
            harness.step(dt.datetime(2026, 9, 16, 8, 31, tzinfo=KST))
    finally:
        harness.close()
    degraded = [r for r in caplog.records if "REPLACE_DEGRADED" in r.getMessage()]
    assert len(degraded) == 1
    assert degraded[0].levelno == logging.INFO
    assert getattr(degraded[0], "krx_event", False) is True
    assert not any("REPLACE_LIVE" in r.getMessage() for r in caplog.records)
    assert harness.runner.state.degraded_active is False


def test_degraded_derived_by_identity(tmp_path, monkeypatch) -> None:
    from src.universe.ipc import write_candidates

    def _fresh_harness(mp: pytest.MonkeyPatch, outcomes: list[bool], holidays: set[dt.date]) -> _TimelineHarness:
        h = _TimelineHarness(mp, tmp_path, holidays=holidays, orch_outcomes=outcomes)
        h.runner._paths.candidates.parent.mkdir(parents=True, exist_ok=True)
        write_candidates(
            h.runner._paths.candidates,
            [{"symbol": "005930", "selection_reasons": ["limit_up"]}],
            rev=20260915,
        )
        h.runner._install_regular(["collect-stream", "--session-date", "2026-09-16"], degraded=True)
        h.runner._install_snapshot(["collect-snapshots", "--session-date", "2026-09-16"], replaces_degraded=False)
        assert h.runner.children.regular_is_degraded is True
        assert h.runner.state.degraded_active is True
        h.calls.clear()
        return h

    holiday_h = _fresh_harness(monkeypatch, [], {D})
    try:
        holiday_h.step(dt.datetime(2026, 9, 16, 9, 0, tzinfo=KST))
    finally:
        holiday_h.close()
    assert holiday_h.runner.children.regular_is_degraded is False
    assert holiday_h.runner.children.degraded_regular is None
    assert holiday_h.runner.state.degraded_active is False

    eod_h = _fresh_harness(monkeypatch, [], set())
    try:
        eod_h.step(dt.datetime(2026, 9, 16, 20, 5, tzinfo=KST))
    finally:
        eod_h.close()
    assert eod_h.runner.children.regular_is_degraded is False
    assert eod_h.runner.children.degraded_regular is None
    assert eod_h.runner.state.degraded_active is False

    rollover_h = _fresh_harness(monkeypatch, [False], set())
    try:
        rollover_h.runner.state.orchestration_day = D - dt.timedelta(days=1)
        rollover_h.runner._paths.candidates.unlink()
        rollover_h.step(dt.datetime(2026, 9, 16, 8, 25, tzinfo=KST))
    finally:
        rollover_h.close()
    assert rollover_h.runner.children.regular_is_degraded is False
    assert rollover_h.runner.children.degraded_regular is None
    assert rollover_h.runner.state.degraded_active is False


def test_injected_degraded_mirror_ignored_for_decision(tmp_path, monkeypatch, caplog) -> None:
    harness = _TimelineHarness(monkeypatch, tmp_path, holidays=set(), orch_outcomes=[True])
    try:
        old = daemon_mod.ProcessSupervisor(cmd=["old"], breaker=None)
        harness.runner._children.regular = old
        harness.runner.state.degraded_active = True
        harness.runner.state.orchestration_day = D
        harness.runner.state.orchestrated_for = None
        harness.calls.clear()
        with caplog.at_level(logging.DEBUG, logger=daemon_mod.logger.name):
            harness.runner.step(dt.datetime(2026, 9, 16, 8, 25, tzinfo=KST))
    finally:
        harness.close()
    live = [r for r in caplog.records if "REPLACE_LIVE" in r.getMessage()]
    assert len(live) == 1
    assert "slot=regular" in live[0].getMessage()
    assert not any("REPLACE_DEGRADED" in r.getMessage() for r in caplog.records)


def test_aftermarket_key_reinstall_stops(tmp_path, monkeypatch, caplog) -> None:
    harness = _TimelineHarness(monkeypatch, tmp_path, holidays=set(), orch_outcomes=[])
    try:
        cmd = ["collect-aftermarket", "--venue", "krx", "--shard-index", "0"]
        harness.calls.clear()
        with caplog.at_level(logging.DEBUG, logger=daemon_mod.logger.name):
            harness.runner._install_aftermarket("krx:0", cmd)
            first = harness.runner.children.aftermarket["krx:0"]
            harness.runner._install_aftermarket("krx:0", cmd)
    finally:
        harness.close()
    stops = [c for c in harness.calls if c[0] == "stop" and c[1] == "krx:0"]
    assert len(stops) == 1
    assert stops[0][2] == 15.0
    assert harness.runner.children.aftermarket["krx:0"] is not first
    live = [r for r in caplog.records if "REPLACE_LIVE" in r.getMessage()]
    assert len(live) == 1
    assert live[0].levelno == logging.CRITICAL
    assert "slot=aftermarket:krx:0" in live[0].getMessage()


def test_log_format_literal_multiset() -> None:
    golden = json.loads(GOLDEN_PATH.read_text(encoding="utf-8"))
    assert sorted(golden["log_stage_multiset"]) == _stage_multiset()
