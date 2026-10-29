"""Kiwoom crosscheck daemon child invariant tests."""

from __future__ import annotations

import datetime as dt
import logging
import pathlib
from zoneinfo import ZoneInfo

_KST = ZoneInfo("Asia/Seoul")


def _runner(tmp_path, monkeypatch, **settings_kw):
    from src.core.config import CollectorSettings, resolve_collector_runtime
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data", **settings_kw)
    runtime = resolve_collector_runtime(collector=settings)
    return daemon_mod, daemon_mod.DaemonRunner(runtime=runtime, shutdown=None, now=lambda: None, sleep=lambda s: None)


def _anchors(day):
    from src.core.session_anchors import standard_session_anchors

    return standard_session_anchors(day)


def test_kiwoom_crosscheck_not_installed_when_disabled(tmp_path, monkeypatch) -> None:
    daemon_mod, runner = _runner(tmp_path, monkeypatch)
    day = dt.date(2026, 9, 14)
    runner._paths.candidates.parent.mkdir(parents=True, exist_ok=True)
    from src.universe.ipc import write_candidates

    write_candidates(runner._paths.candidates, [{"symbol": "005930", "selection_reasons": ["limit_up"]}], rev=1)
    runner._start_ready_children(day)
    assert runner.children.kiwoom_crosscheck is None
    assert runner.state.last_kiwoom_crosscheck_result is None


def test_kiwoom_crosscheck_installed_with_own_manifest_cmd(tmp_path, monkeypatch) -> None:
    daemon_mod, runner = _runner(tmp_path, monkeypatch, kiwoom_crosscheck_enabled=True)
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
    assert runner.children.kiwoom_crosscheck is not None
    cmd = runner.children.kiwoom_crosscheck.cmd
    assert "collect-stream-kiwoom" in cmd
    assert any(str(c).endswith(".kiwoom.json") for c in cmd)
    direct = daemon_mod._kiwoom_crosscheck_cmd(day, runner._paths)
    assert "collect-stream-kiwoom" in direct
    assert str(runner._paths.kiwoom_crosscheck_manifest_path(day)) in direct


def test_kiwoom_crosscheck_spec_contract(tmp_path, monkeypatch) -> None:
    daemon_mod, runner = _runner(tmp_path, monkeypatch, kiwoom_crosscheck_enabled=True)
    spec = runner._stream_spec(daemon_mod.StreamChildKey.KIWOOM_CROSSCHECK)
    assert spec.role is daemon_mod.StreamRole.CROSSCHECK
    assert spec.requires_primary is True
    assert spec.logs is not None
    assert spec.logs.circuit_level == logging.WARNING
    assert spec.journal_vendor is not None
    assert spec.journal_vendor(runner._cfg) == "kiwoom"
    day = dt.date(2026, 9, 14)
    assert spec.manifest_path is not None
    assert spec.manifest_path(runner._paths, day) == runner._paths.kiwoom_crosscheck_manifest_path(day)
    assert spec.cmd is not None
    assert "collect-stream-kiwoom" in spec.cmd(day, runner._paths, None)
    assert spec.active is not None
    assert spec.active(runner._runtime, dt.datetime(2026, 9, 14, 9, 0, tzinfo=_KST), _anchors(day)) is True
    assert spec.active(runner._runtime, dt.datetime(2026, 9, 14, 16, 0, tzinfo=_KST), _anchors(day)) is False
    assert runner.state.child_result(daemon_mod.StreamChildKey.KIWOOM_CROSSCHECK) is None
    runner.state.set_child_result(daemon_mod.StreamChildKey.KIWOOM_CROSSCHECK, "running")
    assert runner.children.kiwoom_crosscheck is None
    runner.state.set_child_result(daemon_mod.StreamChildKey.KIWOOM_CROSSCHECK, None)


def test_kiwoom_crosscheck_circuit_open_warns_not_critical(tmp_path, monkeypatch, caplog) -> None:
    daemon_mod, runner = _runner(tmp_path, monkeypatch, kiwoom_crosscheck_enabled=True)
    day = dt.date(2026, 9, 14)

    class _Tripping:
        last_exit_code = 3

        def ensure_running(self):
            return "circuit_open"

        def take_circuit_alert(self):
            return True

    runner.children.regular = object()
    runner.children.kiwoom_crosscheck = _Tripping()  # type: ignore[assignment]
    with caplog.at_level(logging.WARNING):
        runner._supervise_stream_child(daemon_mod.StreamChildKey.KIWOOM_CROSSCHECK, dt.datetime(2026, 9, 14, 9, 0, tzinfo=_KST), _anchors(day))
    assert any("stage=kiwoom_crosscheck" in r.getMessage() and r.levelno == logging.WARNING for r in caplog.records)
    assert not any("stage=kiwoom_crosscheck" in r.getMessage() and r.levelno == logging.CRITICAL for r in caplog.records)
    assert runner.state.last_kiwoom_crosscheck_result == "circuit_open"


def test_kiwoom_crosscheck_reconciliation_ok_and_gap(tmp_path, monkeypatch) -> None:
    from src.orchestration.trading_day_gate import TradingDayStatus, TradingDayView

    from tests.unit.orchestration.daemon_fixtures import _business_trading_day

    daemon_mod, runner = _runner(tmp_path, monkeypatch, kiwoom_crosscheck_enabled=True)
    day = dt.date(2026, 9, 14)
    runner._state.orchestrated_for = day
    view = TradingDayView(date=day, status=TradingDayStatus.BUSINESS, trading_day=_business_trading_day(day))
    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", lambda **kw: kw["vendor"] != "kiwoom")
    outcomes = runner._reconcile_crosscheck_sessions(day, view, _anchors(day))
    assert outcomes[daemon_mod.StreamChildKey.KIWOOM_CROSSCHECK] == "gap"
    monkeypatch.setattr(daemon_mod, "check_session_reconciliation", lambda **kw: True)
    outcomes = runner._reconcile_crosscheck_sessions(day, view, _anchors(day))
    assert outcomes[daemon_mod.StreamChildKey.KIWOOM_CROSSCHECK] == "ok"
