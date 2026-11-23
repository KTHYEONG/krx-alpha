"""Daemon supervision of the full-market sweep SESSION child."""

from __future__ import annotations

import datetime as dt
import pathlib
from zoneinfo import ZoneInfo

import pytest

import src.orchestration.daemon as daemon_mod
from src.core.config import CollectorSettings, MarketSweepSettings, resolve_collector_runtime
from src.core.session_anchors import standard_session_anchors

_KST = ZoneInfo("Asia/Seoul")
_DAY = dt.date(2026, 11, 24)


@pytest.fixture(autouse=True)
def _verified_aftermarket_capacity(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KRX_ALPHA_AFTERMARKET_PAIR_CAPACITY_PER_CONNECTION", "4")


class _FakeSupervisor:
    def __init__(self, cmd, breaker=None) -> None:
        self.cmd = cmd
        self.last_exit_code = None
        self.ensured = 0

    def ensure_running(self) -> str:
        self.ensured += 1
        return "started"

    def stop(self, timeout_s: float) -> str:
        return "graceful"


def _runner(tmp_path, monkeypatch, *, enabled: bool) -> daemon_mod.DaemonRunner:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)
    runtime = resolve_collector_runtime(
        collector=CollectorSettings(data_root=pathlib.Path(tmp_path) / "data"),
        market_sweep=MarketSweepSettings(enabled=enabled, kis_data_slot="6" if enabled else None),
    )
    return daemon_mod.DaemonRunner(runtime=runtime, shutdown=None, now=lambda: None, sleep=lambda s: None)


def _keys(runner: daemon_mod.DaemonRunner) -> list[daemon_mod.StreamChildKey]:
    return [spec.key for spec in runner._session_specs()]


def test_disabled_sweep_is_never_installed(tmp_path, monkeypatch) -> None:
    runner = _runner(tmp_path, monkeypatch, enabled=False)

    runner._start_ready_children(_DAY)

    assert daemon_mod.StreamChildKey.MARKET_SWEEP not in _keys(runner)
    assert runner.children.get(daemon_mod.StreamChildKey.MARKET_SWEEP) is None


def test_enabled_sweep_starts_with_session_and_full_listing_command(tmp_path, monkeypatch) -> None:
    runner = _runner(tmp_path, monkeypatch, enabled=True)

    runner._start_ready_children(_DAY)

    sup = runner.children.get(daemon_mod.StreamChildKey.MARKET_SWEEP)
    assert isinstance(sup, _FakeSupervisor)
    assert sup.cmd[3:] == [
        "collect-market-sweep",
        "--session-date",
        _DAY.isoformat(),
        "--market-map-path",
        str(runner._paths.market_map),
    ]
    spec = runner._stream_spec(daemon_mod.StreamChildKey.MARKET_SWEEP)
    assert spec.starts_in_degraded is True
    assert spec.requires_primary is False


def test_sweep_is_supervised_only_before_run_end(tmp_path, monkeypatch) -> None:
    runner = _runner(tmp_path, monkeypatch, enabled=True)
    runner._start_ready_children(_DAY)
    sup = runner.children.get(daemon_mod.StreamChildKey.MARKET_SWEEP)
    anchors = standard_session_anchors(_DAY)

    runner._supervise_stream_child(
        daemon_mod.StreamChildKey.MARKET_SWEEP, dt.datetime.combine(_DAY, dt.time(15, 38, 59), tzinfo=_KST), anchors
    )
    runner._supervise_stream_child(
        daemon_mod.StreamChildKey.MARKET_SWEEP, dt.datetime.combine(_DAY, dt.time(15, 39), tzinfo=_KST), anchors
    )

    assert isinstance(sup, _FakeSupervisor)
    assert sup.ensured == 1


def test_degraded_primary_still_starts_sweep(tmp_path, monkeypatch) -> None:
    runner = _runner(tmp_path, monkeypatch, enabled=True)
    monkeypatch.setattr(daemon_mod, "_degraded_candidates_rev", lambda *args, **kwargs: 20261124)

    runner._handle_orchestration_failure(dt.datetime.combine(_DAY, dt.time(8, 25), tzinfo=_KST), _DAY)

    assert runner.children.get(daemon_mod.StreamChildKey.MARKET_SWEEP) is not None


def test_holiday_and_new_day_stop_the_sweep(tmp_path, monkeypatch) -> None:
    runner = _runner(tmp_path, monkeypatch, enabled=True)
    runner._start_ready_children(_DAY)

    runner._stop_session_children()

    assert runner.children.get(daemon_mod.StreamChildKey.MARKET_SWEEP) is None
