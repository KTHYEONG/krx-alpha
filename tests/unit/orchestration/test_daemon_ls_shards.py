"""Invariant guards for LS shard wiring in the daemon: flag-off legacy, shard-1 child and membership, EOD frame sums."""

from __future__ import annotations

import datetime as dt
import logging
from typing import ClassVar
from zoneinfo import ZoneInfo

import polars as pl
import pytest

import src.orchestration.daemon as daemon_mod
from src.core.config import CollectorSettings, RegularKisSettings, resolve_collector_runtime
from src.core.session_anchors import standard_session_anchors
from src.orchestration.trading_day_gate import TradingDayStatus, TradingDayView
from src.realtime.ls_limits import prioritize_ls_pairs, split_ls_pairs
from src.realtime.manifest import SessionManifest
from src.universe.ipc import write_candidates
from src.universe.membership import read_membership

KST = ZoneInfo("Asia/Seoul")
DAY = dt.date(2026, 9, 16)
WALL_NS = 1_786_000_000_000_000_000


@pytest.fixture(autouse=True)
def _kis_data_pool(monkeypatch) -> None:
    monkeypatch.setenv("KIS_DATA_SLOTS", "1,2,3")
    for slot in ("1", "2", "3"):
        monkeypatch.setenv(f"KIS_DATA_{slot}_APP_KEY", f"test-ls-shard-app-key-{slot}")
        monkeypatch.setenv(f"KIS_DATA_{slot}_APP_SECRET", f"test-ls-shard-app-secret-{slot}")
        monkeypatch.setenv(f"KIS_DATA_{slot}_HTS_ID", f"test-ls-shard-hts-id-{slot}")
    monkeypatch.setenv("KRX_ALPHA_REGULAR_KIS_PAIR_CAPACITY_PER_CONNECTION", "41")


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


class _ScriptedSupervisor(_FakeSupervisor):
    results: ClassVar[list[str]] = []
    alerts: ClassVar[list[bool]] = []

    def __init__(self, *, cmd, breaker=None):
        super().__init__(cmd=cmd, breaker=breaker)
        self.last_exit_code = 137

    def ensure_running(self):
        return self.results.pop(0) if self.results else "started"

    def take_circuit_alert(self):
        return self.alerts.pop(0) if self.alerts else False


def _at(day, hour, minute):
    return dt.datetime(day.year, day.month, day.day, hour, minute, tzinfo=KST)


def _symbols(count: int) -> tuple[str, ...]:
    return tuple(f"{i:06d}" for i in range(1, count + 1))


def _runner(tmp_path, monkeypatch, **collector_kw):
    collector = CollectorSettings(data_root=tmp_path / "data", **collector_kw)
    rk = RegularKisSettings(enabled=True, credential_slots=("1", "2", "3"), pair_capacity_per_connection=41)
    runtime = resolve_collector_runtime(collector=collector, regular_kis=rk)
    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)
    return daemon_mod.DaemonRunner(
        runtime=runtime,
        shutdown=None,
        now=lambda: _at(DAY, 9, 0),
        sleep=lambda s: None,
        wall_ns=lambda: WALL_NS,
    )


def _write_candidates(paths, symbols, tiers=None) -> None:
    rows = [
        {"symbol": symbol, "selection_reasons": ["limit_up"], **({"tier": tiers[symbol]} if tiers else {})}
        for symbol in symbols
    ]
    paths.candidates.parent.mkdir(parents=True, exist_ok=True)
    write_candidates(paths.candidates, rows, rev=20260915)


def test_flag_off_is_legacy(tmp_path, monkeypatch, caplog) -> None:
    runner = _runner(tmp_path, monkeypatch)
    paths = runner._paths
    _write_candidates(paths, _symbols(150))
    with caplog.at_level(logging.INFO, logger=daemon_mod.logger.name):
        runner._start_ready_children(DAY)
    primary = runner.children.regular
    assert primary is not None
    assert "--symbols" not in primary.cmd
    assert str(paths.manifest_path(DAY)) in primary.cmd
    assert paths.ls_manifest_paths(DAY) == (paths.manifest_path(DAY),)
    assert not runner._stream_spec(daemon_mod.StreamChildKey.REGULAR_LS_B).enabled(runner._runtime)
    runner._supervise_regular_ls_shard(DAY)
    assert runner.children.get(daemon_mod.StreamChildKey.REGULAR_LS_B) is None
    rows = read_membership(paths.membership_path(DAY))
    assert {row.tier for row in rows} == {"A", "C"}
    assert sum(row.tier == "A" for row in rows) == 100
    assert sum(row.tier == "C" and row.reason == "overflow" for row in rows) == 50
    assert "stage=collection_tiers" in caplog.text
    assert "tier_b_static=0" in caplog.text


def test_two_sessions_spawn_shard_child_with_plan_symbols(tmp_path, monkeypatch) -> None:
    runner = _runner(tmp_path, monkeypatch, ls_active_sessions=2)
    paths = runner._paths
    symbols = _symbols(150)
    _write_candidates(paths, symbols)
    runner._supervise_regular_ls_shard(DAY)
    assert runner.children.get(daemon_mod.StreamChildKey.REGULAR_LS_B) is None
    universe_values = {symbol: float(index + 1) for index, symbol in enumerate(symbols)}
    paths.universe_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": list(universe_values), "trade_value_100m": list(universe_values.values())}).write_parquet(
        paths.universe_out(DAY)
    )

    runner._start_ready_children(DAY)
    runner._supervise_regular_ls_shard(DAY)

    primary = runner.children.regular
    assert primary is not None
    assert primary.cmd[primary.cmd.index("--symbols") + 1] == ",".join(symbols[:100])
    shard_one = runner.children.get(daemon_mod.StreamChildKey.REGULAR_LS_B)
    assert shard_one is not None
    assert shard_one.cmd[shard_one.cmd.index("--ls-shard-index") + 1] == "1"
    assert shard_one.cmd[shard_one.cmd.index("--symbols") + 1] == ",".join(symbols[100:150])
    assert str(paths.ls_manifest_path(DAY, 1)) in shard_one.cmd

    rows = read_membership(paths.membership_path(DAY))
    assert sum(row.tier == "A" and row.source == "static" for row in rows) == 100
    assert sum(row.tier == "B" and row.source == "static" for row in rows) == 50
    assert all(row.ls_shard == 1 for row in rows if row.tier == "B")
    assert all(row.decided_at_ns == WALL_NS for row in rows)
    by_symbol = {row.symbol: row for row in rows}
    assert by_symbol[symbols[0]].trade_value_krw == 100_000_000
    assert by_symbol[symbols[120]].trade_value_krw == 12_100_000_000

    runner._start_ready_children(DAY)
    assert len(read_membership(paths.membership_path(DAY))) == len(rows)

    runner._stop_stream_child(daemon_mod.StreamChildKey.REGULAR_LS_B)
    assert runner.children.get(daemon_mod.StreamChildKey.REGULAR_LS_B) is None


def test_eod_audit_input_sums_frame_counts_across_shards(tmp_path, monkeypatch) -> None:
    runner = _runner(tmp_path, monkeypatch, ls_active_sessions=2)
    paths = runner._paths
    shard0_counts = {"005930": {"H0STCNT0": 3}}
    shard1_counts = {"000660": {"H0STCNT0": 2}, "005930": {"H0STASP0": 1}}
    SessionManifest(session_date=DAY, clock_offset_ns=0, started_at_ns=0, frame_counts=shard0_counts).save(
        paths.manifest_path(DAY)
    )
    SessionManifest(session_date=DAY, clock_offset_ns=0, started_at_ns=0, frame_counts=shard1_counts).save(
        paths.ls_manifest_path(DAY, 1)
    )
    seen: list[object] = []
    real_derive = daemon_mod.derive_session_audit

    def _spy(*args, **kwargs):
        seen.append(kwargs["manifest_frame_counts"])
        return real_derive(*args, **kwargs)

    monkeypatch.setattr(daemon_mod, "derive_session_audit", _spy)
    view = TradingDayView(date=DAY, status=TradingDayStatus.BUSINESS, trading_day=None)
    runner._derive_session_audit(DAY, view, standard_session_anchors(DAY))
    assert seen == [{"005930": {"H0STCNT0": 3, "H0STASP0": 1}, "000660": {"H0STCNT0": 2}}]


def test_default_settings_ls_coverage_matches_legacy_split(tmp_path, monkeypatch) -> None:
    runner = _runner(tmp_path, monkeypatch)
    symbols = _symbols(150)
    tiers = {symbol: ("core", "event", "follow")[index % 3] for index, symbol in enumerate(symbols)}
    _write_candidates(runner._paths, symbols, tiers)
    collector = runner._runtime.collector
    pairs = [(symbol, stream) for symbol in symbols for stream in collector.streams]
    legacy_pairs, _ = split_ls_pairs(pairs, collector.ls_max_subscriptions_per_session, tier_of=tiers, order=symbols)
    legacy_covered = frozenset(symbol for symbol, _ in legacy_pairs)
    legacy_priority = tuple(dict.fromkeys(s for s, _ in prioritize_ls_pairs(pairs, tier_of=tiers, order=symbols)))

    runner.state.orchestrated_for = DAY
    runner._ensure_regular_kis_plan(DAY)

    new_covered = frozenset(symbol for shard in runner.state.ls_shard_plan for symbol in shard.symbols)
    assert new_covered == legacy_covered
    assert len(legacy_covered) == 100
    expected_kis = daemon_mod.plan_regular_kis_shards(
        symbols=legacy_priority,
        credentials=daemon_mod.load_kis_data_credentials(),
        credential_slots=tuple(runner._runtime.regular_kis.credential_slots),
        pair_capacity_per_connection=41,
        streams=runner._runtime.regular_kis.streams,
        ls_covered_symbols=legacy_covered,
    )
    assert runner.state.regular_kis_plan == expected_kis


def test_eod_audit_skips_manifest_without_frame_counts(tmp_path, monkeypatch) -> None:
    runner = _runner(tmp_path, monkeypatch, ls_active_sessions=2)
    paths = runner._paths
    SessionManifest(session_date=DAY, clock_offset_ns=0, started_at_ns=0, frame_counts=None).save(
        paths.manifest_path(DAY)
    )
    shard1_counts = {"000660": {"H0STCNT0": 2}}
    SessionManifest(session_date=DAY, clock_offset_ns=0, started_at_ns=0, frame_counts=shard1_counts).save(
        paths.ls_manifest_path(DAY, 1)
    )
    seen: list[object] = []
    real_derive = daemon_mod.derive_session_audit

    def _spy(*args, **kwargs):
        seen.append(kwargs["manifest_frame_counts"])
        return real_derive(*args, **kwargs)

    monkeypatch.setattr(daemon_mod, "derive_session_audit", _spy)
    view = TradingDayView(date=DAY, status=TradingDayStatus.BUSINESS, trading_day=None)
    runner._derive_session_audit(DAY, view, standard_session_anchors(DAY))
    assert seen == [{"000660": {"H0STCNT0": 2}}]


def test_static_membership_ignores_non_date_universe_files(tmp_path, monkeypatch) -> None:
    runner = _runner(tmp_path, monkeypatch, ls_active_sessions=2)
    paths = runner._paths
    _write_candidates(paths, _symbols(150))
    paths.universe_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": ["000001"], "trade_value_100m": [1.0]}).write_parquet(paths.universe_out(DAY))
    pl.DataFrame({"x": [1]}).write_parquet(paths.universe_dir / "latest.parquet")
    pl.DataFrame({"x": [1]}).write_parquet(paths.universe_dir / "2026-13-40.parquet")

    runner._start_ready_children(DAY)

    by_symbol = {row.symbol: row for row in read_membership(paths.membership_path(DAY))}
    assert by_symbol["000001"].trade_value_krw == 100_000_000


def test_static_membership_trade_value_is_none_for_null_or_nonfinite(tmp_path, monkeypatch) -> None:
    runner = _runner(tmp_path, monkeypatch, ls_active_sessions=2)
    paths = runner._paths
    symbols = _symbols(150)
    _write_candidates(paths, symbols)
    paths.universe_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {"symbol": list(symbols[:3]), "trade_value_100m": [1.0, None, float("nan")]},
        schema={"symbol": pl.Utf8, "trade_value_100m": pl.Float64},
    ).write_parquet(paths.universe_out(DAY))

    runner._start_ready_children(DAY)

    by_symbol = {row.symbol: row for row in read_membership(paths.membership_path(DAY))}
    assert by_symbol[symbols[0]].trade_value_krw == 100_000_000
    assert by_symbol[symbols[1]].trade_value_krw is None
    assert by_symbol[symbols[2]].trade_value_krw is None


def test_static_membership_dropped_rows_default_reason_when_blank(tmp_path, monkeypatch) -> None:
    runner = _runner(tmp_path, monkeypatch, ls_active_sessions=2)
    paths = runner._paths
    _write_candidates(paths, _symbols(150))
    paths.universe_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": ["000001"], "trade_value_100m": [1.0]}).write_parquet(paths.universe_out(DAY))
    paths.universe_dropped_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": ["900001", "900002", "900003"],
            "trade_value_100m": [2.0, None, 3.0],
            "reasons": ["price_band", None, ""],
        },
        schema={"symbol": pl.Utf8, "trade_value_100m": pl.Float64, "reasons": pl.Utf8},
    ).write_parquet(paths.universe_dropped_out(DAY))

    runner._start_ready_children(DAY)

    by_symbol = {row.symbol: row for row in read_membership(paths.membership_path(DAY))}
    assert by_symbol["900001"].tier == "C"
    assert by_symbol["900001"].reason == "price_band"
    assert by_symbol["900001"].trade_value_krw == 200_000_000
    assert by_symbol["900002"].reason == "policy_dropped"
    assert by_symbol["900003"].reason == "policy_dropped"


def test_static_membership_not_duplicated_after_restart(tmp_path, monkeypatch) -> None:
    first = _runner(tmp_path, monkeypatch, ls_active_sessions=2)
    _write_candidates(first._paths, _symbols(150))
    first._start_ready_children(DAY)
    membership = first._paths.membership_path(DAY)
    before = read_membership(membership)
    assert len(before) == 150

    restarted = _runner(tmp_path, monkeypatch, ls_active_sessions=2)
    restarted._start_ready_children(DAY)

    assert read_membership(membership) == before


def test_corrupt_membership_ledger_logs_fail_and_keeps_shard_plan(tmp_path, monkeypatch, caplog) -> None:
    runner = _runner(tmp_path, monkeypatch, ls_active_sessions=2)
    paths = runner._paths
    _write_candidates(paths, _symbols(150))
    membership = paths.membership_path(DAY)
    membership.parent.mkdir(parents=True, exist_ok=True)
    membership.write_text("not-json\n", encoding="utf-8")

    with caplog.at_level(logging.ERROR, logger=daemon_mod.logger.name):
        runner._start_ready_children(DAY)

    assert "stage=membership status=FAIL" in caplog.text
    assert membership.read_text(encoding="utf-8") == "not-json\n"
    assert [len(shard.symbols) for shard in runner.state.ls_shard_plan] == [100, 50]
    assert runner.children.regular is not None


def test_stop_children_stops_ls_shard_one_child(tmp_path, monkeypatch) -> None:
    runner = _runner(tmp_path, monkeypatch, ls_active_sessions=2)
    _write_candidates(runner._paths, _symbols(150))
    runner._start_ready_children(DAY)
    runner._supervise_regular_ls_shard(DAY)
    shard_one = runner.children.get(daemon_mod.StreamChildKey.REGULAR_LS_B)
    assert shard_one is not None
    regular = runner.children.regular
    assert regular is not None
    selected: list[object] = []

    def _capture(children, *, deadline_s):
        selected.extend(children)
        return {"graceful": len(selected), "killed": 0, "not_running": 0}

    monkeypatch.setattr(daemon_mod, "stop_supervisors", _capture)
    runner.stop_children()

    assert selected.count(shard_one) == 1
    assert selected.count(regular) == 1


def test_ls_shard_one_not_spawned_when_plan_has_no_shard_one_symbols(tmp_path, monkeypatch) -> None:
    runner = _runner(tmp_path, monkeypatch, ls_active_sessions=2)
    _write_candidates(runner._paths, _symbols(50))
    runner._start_ready_children(DAY)
    assert runner.children.regular is not None
    assert not any(shard.shard_index == 1 and shard.symbols for shard in runner.state.ls_shard_plan)

    runner._supervise_regular_ls_shard(DAY)

    assert runner.children.get(daemon_mod.StreamChildKey.REGULAR_LS_B) is None


def test_ls_shard_one_restart_keeps_child_and_logs_exit_code(tmp_path, monkeypatch, caplog) -> None:
    runner = _runner(tmp_path, monkeypatch, ls_active_sessions=2)
    _write_candidates(runner._paths, _symbols(150))
    runner._start_ready_children(DAY)
    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _ScriptedSupervisor)
    monkeypatch.setattr(_ScriptedSupervisor, "results", ["restarted"])

    with caplog.at_level(logging.WARNING, logger=daemon_mod.logger.name):
        runner._supervise_regular_ls_shard(DAY)

    shard_one = runner.children.get(daemon_mod.StreamChildKey.REGULAR_LS_B)
    assert isinstance(shard_one, _ScriptedSupervisor)
    assert shard_one.cmd[shard_one.cmd.index("--ls-shard-index") + 1] == "1"
    assert "[DATA] stage=ls_shard_stream status=RESTARTED shard=1 exit_code=137" in caplog.text
    assert "status=FAIL" not in caplog.text


def test_ls_shard_one_circuit_open_alerts_once_per_episode(tmp_path, monkeypatch, caplog) -> None:
    runner = _runner(tmp_path, monkeypatch, ls_active_sessions=2)
    _write_candidates(runner._paths, _symbols(150))
    runner._start_ready_children(DAY)
    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _ScriptedSupervisor)
    monkeypatch.setattr(_ScriptedSupervisor, "results", ["circuit_open", "circuit_open"])
    monkeypatch.setattr(_ScriptedSupervisor, "alerts", [True, False])

    with caplog.at_level(logging.WARNING, logger=daemon_mod.logger.name):
        runner._supervise_regular_ls_shard(DAY)
        runner._supervise_regular_ls_shard(DAY)

    assert (
        caplog.text.count("[DATA] stage=ls_shard_stream status=FAIL reason=circuit_open shard=1 date=2026-09-16") == 1
    )
    assert runner.children.get(daemon_mod.StreamChildKey.REGULAR_LS_B) is not None


def test_static_membership_carries_universe_direction_and_warning(tmp_path, monkeypatch) -> None:
    runner = _runner(tmp_path, monkeypatch, ls_active_sessions=2)
    paths = runner._paths
    symbols = _symbols(150)
    _write_candidates(paths, symbols)
    paths.universe_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": [symbols[0], symbols[120]],
            "trade_value_100m": [1.0, 2.0],
            "direction": ["down", None],
            "warning_level": ["warning", "none"],
        },
        schema={"symbol": pl.Utf8, "trade_value_100m": pl.Float64, "direction": pl.Utf8, "warning_level": pl.Utf8},
    ).write_parquet(paths.universe_out(DAY))

    runner._start_ready_children(DAY)

    by_symbol = {row.symbol: row for row in read_membership(paths.membership_path(DAY))}
    assert (by_symbol[symbols[0]].direction, by_symbol[symbols[0]].warning_level) == ("down", "warning")
    assert (by_symbol[symbols[120]].direction, by_symbol[symbols[120]].warning_level) == (None, "none")


def test_unexpected_membership_failure_never_blocks_the_shard_plan(tmp_path, monkeypatch, caplog) -> None:
    runner = _runner(tmp_path, monkeypatch, ls_active_sessions=2)
    _write_candidates(runner._paths, _symbols(150))

    def _boom(*args, **kwargs):
        raise ValueError("row validation failed")

    monkeypatch.setattr(daemon_mod, "append_membership", _boom)

    with caplog.at_level(logging.ERROR, logger=daemon_mod.logger.name):
        runner._start_ready_children(DAY)

    assert "stage=membership status=FAIL error_type=ValueError" in caplog.text
    assert [len(shard.symbols) for shard in runner.state.ls_shard_plan] == [100, 50]
    assert runner.children.regular is not None
