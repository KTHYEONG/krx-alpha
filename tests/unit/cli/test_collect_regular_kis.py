"""Invariant guard tests for collect-regular-kis."""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import pathlib
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from src.cli import collect_regular_kis
from src.core.errors import KrxAlphaError, MissingCredentialsError
from src.realtime.kis_sharding import KisDataCredential

_KST = ZoneInfo("Asia/Seoul")


def _write_candidates(path: pathlib.Path, *, day: dt.date, symbols: tuple[str, ...]) -> None:
    from src.marketdata.toss_calendar import TradingDay, save_trading_day_cache
    from src.universe.ipc import write_candidates

    save_trading_day_cache(
        path.parent / "calendar_cache.json",
        TradingDay(date=day, is_business_day=True, previous_business_day=day - dt.timedelta(days=1), next_business_day=day + dt.timedelta(days=1)),
    )
    rev = int((day - dt.timedelta(days=1)).strftime("%Y%m%d"))
    write_candidates(
        path,
        [{"symbol": sym, "selection_reasons": ["trade_amount"]} for sym in symbols],
        rev=rev,
    )


def _runtime(*, enabled=True, slots=("1", "2"), capacity=41, grace=120.0, silence=30.0, nxt_slots=()):
    collector = SimpleNamespace(
        vendor="kis",
        ntp_host="ntp-host",
        ntp_fallback_hosts=(),
        min_free_disk_gb=3.0,
        journal_retain_days=3,
        kis_ack_timeout_s=10.0,
        feed_health_interval_s=5.0,
    )
    regular_kis = SimpleNamespace(
        enabled=enabled,
        credential_slots=slots,
        pair_capacity_per_connection=capacity,
        streams=("H0STCNT0", "H0STASP0"),
        close_grace_s=grace,
        silence_limit_s=silence,
        nxt_credential_slots=nxt_slots,
        nxt_streams=("H0NXCNT0", "H0NXASP0"),
        nxt_max_symbols=len(nxt_slots) * ((capacity or 0) // 2),
    )
    return SimpleNamespace(collector=collector, regular_kis=regular_kis)


def _args(tmp_path: pathlib.Path, *, day: str, symbols: str, shard=0, slot="1", key_id="fp1") -> argparse.Namespace:
    return argparse.Namespace(
        session_date=day,
        journal_root=str(tmp_path / "l0"),
        manifest_path=str(tmp_path / "manifest.json"),
        candidates_path=str(tmp_path / "pool.json"),
        shard_index=shard,
        credential_slot=slot,
        credential_key_id=key_id,
        symbols=symbols,
        ntp_host="x",
        max_clock_offset_ns=2_000_000_000,
        max_cycles=1,
    )


def _creds() -> tuple[KisDataCredential, ...]:
    return (
        KisDataCredential("1", "k1", "s1", "h1", "fp1"),
        KisDataCredential("2", "k2", "s2", "h2", "fp2"),
    )


def test_shard_symbols_must_match_plan_before_socket(monkeypatch, tmp_path) -> None:
    day = dt.date(2026, 9, 16)
    symbols = tuple(f"{i:06d}" for i in range(25))
    _write_candidates(tmp_path / "pool.json", day=day, symbols=symbols)
    monkeypatch.setattr(collect_regular_kis, "resolve_collector_runtime", lambda **kw: _runtime())
    monkeypatch.setattr(collect_regular_kis, "load_kis_data_credentials", lambda: _creds())
    monkeypatch.setattr(
        collect_regular_kis, "bootstrap_session", lambda cfg: (_ for _ in ()).throw(AssertionError("no bootstrap"))
    )
    monkeypatch.setattr(
        collect_regular_kis, "KisRealtimeAdapter", lambda **_: (_ for _ in ()).throw(AssertionError("no socket"))
    )
    wrong = ",".join(symbols[1:21])
    with pytest.raises(MissingCredentialsError):
        asyncio.run(collect_regular_kis._run_stream(_args(tmp_path, day="2026-09-16", symbols=wrong)))


def test_started_after_deadline_writes_nothing(monkeypatch, tmp_path) -> None:
    import time

    import src.realtime.session as session_mod
    from src.realtime.manifest import SessionManifest

    day = dt.date(2026, 9, 16)
    symbols = ("005930", "000660")
    _write_candidates(tmp_path / "pool.json", day=day, symbols=symbols)
    monkeypatch.setattr(collect_regular_kis, "resolve_collector_runtime", lambda **kw: _runtime(grace=120.0))
    monkeypatch.setattr(collect_regular_kis, "load_kis_data_credentials", lambda: _creds())
    close_at = dt.datetime(2026, 9, 16, 15, 30, tzinfo=_KST)
    expected_close_ns = int(close_at.timestamp() * 1_000_000_000)
    monkeypatch.setattr(session_mod, "measure_ntp_offset_ns", lambda *a, **kw: 0)
    monkeypatch.setattr(time, "time_ns", lambda: expected_close_ns + 121_000_000_000)
    monkeypatch.setattr(
        collect_regular_kis, "KisRealtimeAdapter", lambda **_: (_ for _ in ()).throw(AssertionError("no socket"))
    )
    args = _args(tmp_path, day="2026-09-16", symbols="005930,000660")
    assert asyncio.run(collect_regular_kis._run_stream(args)) == 0
    manifest = SessionManifest.load(tmp_path / "manifest.json")
    assert manifest.expected_close_ns == expected_close_ns
    assert manifest.writer_closed_at_ns is None
    assert list((tmp_path / "l0").glob("**/*.jsonl.zst")) == []


def test_revision_matches_calendar_not_age(monkeypatch, tmp_path) -> None:
    from src.core.config import DataPaths
    from src.core.errors import KrxAlphaError
    from src.marketdata.toss_calendar import TradingDay, save_trading_day_cache
    from src.universe.ipc import write_candidates

    day = dt.date(2026, 10, 12)
    path = tmp_path / "pool.json"
    paths = DataPaths(tmp_path)
    write_candidates(path, [{"symbol": "005930"}], rev=20261002)
    with pytest.raises(KrxAlphaError, match="stale"):
        collect_regular_kis._candidate_symbols(path, day, paths)
    save_trading_day_cache(paths.calendar_cache, TradingDay(
        date=day, is_business_day=True, previous_business_day=dt.date(2026, 10, 2), next_business_day=day + dt.timedelta(days=1)
    ))
    assert collect_regular_kis._candidate_symbols(path, day, paths) == ("005930",)
    write_candidates(path, [{"symbol": "005930"}], rev=20261011)
    with pytest.raises(KrxAlphaError, match="stale"):
        collect_regular_kis._candidate_symbols(path, day, paths)
    write_candidates(path, [{}], rev=20261002)
    with pytest.raises(KrxAlphaError, match="symbols"):
        collect_regular_kis._candidate_symbols(path, day, paths)


@pytest.mark.parametrize("offset", [0, -1, 2_000_000_001])
def test_clock_override_cannot_relax_ceiling(monkeypatch, tmp_path, offset) -> None:
    day = dt.date(2026, 9, 16)
    _write_candidates(tmp_path / "pool.json", day=day, symbols=("005930",))
    monkeypatch.setattr(collect_regular_kis, "resolve_collector_runtime", lambda **kw: _runtime())
    monkeypatch.setattr(collect_regular_kis, "load_kis_data_credentials", _creds)
    args = _args(tmp_path, day=day.isoformat(), symbols="005930")
    args.max_clock_offset_ns = offset
    with pytest.raises(ValueError, match="max_clock_offset_ns"):
        asyncio.run(collect_regular_kis._run_stream(args))


def test_journal_path_is_vendor_disjoint(monkeypatch, tmp_path) -> None:
    import time

    import src.realtime.session as session_mod

    day = dt.date(2026, 9, 16)
    symbols = ("005930",)
    _write_candidates(tmp_path / "pool.json", day=day, symbols=symbols)
    monkeypatch.setattr(collect_regular_kis, "resolve_collector_runtime", lambda **kw: _runtime())
    monkeypatch.setattr(collect_regular_kis, "load_kis_data_credentials", lambda: _creds())
    monkeypatch.setattr(session_mod, "measure_ntp_offset_ns", lambda *a, **kw: 0)
    noon = dt.datetime(2026, 9, 16, 10, 0, tzinfo=_KST)
    monkeypatch.setattr(time, "time_ns", lambda: int(noon.timestamp() * 1_000_000_000))

    captured: dict = {}

    class FakeAdapter:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    class FakeStreamer:
        def __init__(self, **kwargs):
            captured["replay_pairs"] = kwargs["replay_pairs"]
            self._sink = kwargs["sink"]

        async def run_forever(self, *a, **kw):
            from src.realtime.contracts import L0Frame, MarketSession, MarketVenue

            now_ns = time.time_ns()
            self._sink.record(
                L0Frame(
                    vendor="kis", stream="H0STCNT0", symbol="005930", raw="005930^090000^100",
                    recv_mono_ns=now_ns, recv_wall_ns=now_ns, conn_seq=1, conn_id="c",
                    venue=MarketVenue.KRX, session=MarketSession.REGULAR, exchange_event_time="090000",
                )
            )
            self._sink.flush()
            return None

    monkeypatch.setattr(collect_regular_kis, "KisRealtimeAdapter", FakeAdapter)
    monkeypatch.setattr(collect_regular_kis, "RealtimeStreamer", FakeStreamer)
    assert asyncio.run(collect_regular_kis._run_stream(_args(tmp_path, day="2026-09-16", symbols="005930"))) == 0
    files = list((tmp_path / "l0").rglob("*.jsonl.zst"))
    assert files, "expected journal files"
    for path in files:
        rel = path.relative_to(tmp_path / "l0").as_posix()
        assert rel.startswith("kis/krx/regular/H0STCNT0/dt=2026-09-16/")
        assert path.name.endswith(".s0.jsonl.zst")
    assert list((tmp_path / "l0" / "ls").glob("**/*")) == []


def test_disabled_refuses_and_disabled_paths_are_inert(monkeypatch, tmp_path) -> None:
    from src.core.errors import KrxAlphaError

    monkeypatch.setattr(collect_regular_kis, "resolve_collector_runtime", lambda **kw: _runtime(enabled=False))
    with pytest.raises(KrxAlphaError, match="disabled"):
        asyncio.run(collect_regular_kis._run_stream(_args(tmp_path, day="2026-09-16", symbols="005930")))


def test_candidate_file_contract_is_fail_closed(monkeypatch, tmp_path) -> None:
    import json

    from src.core.errors import KrxAlphaError

    monkeypatch.setattr(collect_regular_kis, "resolve_collector_runtime", lambda **kw: _runtime())
    monkeypatch.setattr(collect_regular_kis, "load_kis_data_credentials", lambda: _creds())
    args = _args(tmp_path, day="2026-09-16", symbols="005930")
    with pytest.raises(KrxAlphaError, match="missing candidates"):
        asyncio.run(collect_regular_kis._run_stream(args))
    (tmp_path / "pool.json").write_text(json.dumps({"rev": 20250915, "candidates": []}), encoding="utf-8")
    with pytest.raises(KrxAlphaError, match="invalid candidates"):
        asyncio.run(collect_regular_kis._run_stream(args))
    (tmp_path / "pool.json").write_text(json.dumps({"rev": "20250915", "candidates": [{"symbol": "005930"}]}), encoding="utf-8")
    with pytest.raises(KrxAlphaError, match="invalid candidates"):
        asyncio.run(collect_regular_kis._run_stream(args))
    (tmp_path / "pool.json").write_text(json.dumps({"rev": 20250901, "candidates": [{"symbol": "005930"}]}), encoding="utf-8")
    with pytest.raises(KrxAlphaError, match="stale candidates"):
        asyncio.run(collect_regular_kis._run_stream(args))


def test_credential_and_symbol_contracts_fail_before_socket(monkeypatch, tmp_path) -> None:
    from src.core.errors import KrxAlphaError, MissingCredentialsError, SlotBudgetExceededError

    day = dt.date(2026, 9, 16)
    _write_candidates(tmp_path / "pool.json", day=day, symbols=("005930",))
    monkeypatch.setattr(collect_regular_kis, "resolve_collector_runtime", lambda **kw: _runtime())
    monkeypatch.setattr(collect_regular_kis, "load_kis_data_credentials", lambda: _creds())
    with pytest.raises(MissingCredentialsError, match="fingerprint"):
        asyncio.run(collect_regular_kis._run_stream(_args(tmp_path, day="2026-09-16", symbols="005930", key_id="bad")))
    with pytest.raises(KrxAlphaError, match="non-empty"):
        asyncio.run(collect_regular_kis._run_stream(_args(tmp_path, day="2026-09-16", symbols="")))
    monkeypatch.setattr(collect_regular_kis, "resolve_collector_runtime", lambda **kw: _runtime(capacity=None))
    with pytest.raises(SlotBudgetExceededError, match="capacity"):
        asyncio.run(collect_regular_kis._run_stream(_args(tmp_path, day="2026-09-16", symbols="005930")))
    monkeypatch.setattr(collect_regular_kis, "resolve_collector_runtime", lambda **kw: _runtime(slots=("1", "2")))
    with pytest.raises(MissingCredentialsError, match="plan"):
        asyncio.run(
            collect_regular_kis._run_stream(
                _args(tmp_path, day="2026-09-16", symbols="005930", shard=7, slot="1", key_id="fp1")
            )
        )


def test_wrong_slot_for_planned_shard_is_rejected(monkeypatch, tmp_path) -> None:
    from src.core.errors import MissingCredentialsError

    day = dt.date(2026, 9, 16)
    symbols = tuple(f"{i:06d}" for i in range(25))
    _write_candidates(tmp_path / "pool.json", day=day, symbols=symbols)
    creds = (
        KisDataCredential("1", "k1", "s1", "h1", "fp1"),
        KisDataCredential("2", "k2", "s2", "h2", "fp2"),
    )
    monkeypatch.setattr(collect_regular_kis, "resolve_collector_runtime", lambda **kw: _runtime(slots=("1", "2")))
    monkeypatch.setattr(collect_regular_kis, "load_kis_data_credentials", lambda: creds)
    shard0 = ",".join(symbols[:20])
    with pytest.raises(MissingCredentialsError, match="fingerprint"):
        asyncio.run(collect_regular_kis._run_stream(_args(tmp_path, day="2026-09-16", symbols=shard0, slot="2", key_id="fp2")))


def test_malformed_rev_and_over_capacity_are_rejected(monkeypatch, tmp_path) -> None:
    import json

    from src.core.errors import KrxAlphaError, SlotBudgetExceededError

    monkeypatch.setattr(collect_regular_kis, "resolve_collector_runtime", lambda **kw: _runtime())
    monkeypatch.setattr(collect_regular_kis, "load_kis_data_credentials", lambda: _creds())
    (tmp_path / "pool.json").write_text(json.dumps({"rev": 99999999, "candidates": [{"symbol": "005930"}]}), encoding="utf-8")
    with pytest.raises(KrxAlphaError, match="invalid candidates rev"):
        asyncio.run(collect_regular_kis._run_stream(_args(tmp_path, day="2026-09-16", symbols="005930")))
    day = dt.date(2026, 9, 16)
    _write_candidates(tmp_path / "pool.json", day=day, symbols=("005930",))
    symbols = tuple(f"{i:06d}" for i in range(30))
    with pytest.raises(SlotBudgetExceededError, match="exceed capacity"):
        asyncio.run(
            collect_regular_kis._run_stream(
                _args(tmp_path, day="2026-09-16", symbols=",".join(symbols), slot="1", key_id="fp1")
            )
        )


def _write_nxt_snapshot(path: pathlib.Path, *, day: dt.date, symbols: tuple[str, ...], session: str = "regular_nxt") -> None:
    from src.universe.ipc import CandidateSnapshot, write_candidate_snapshot

    stamp = dt.datetime(day.year, day.month, day.day, 8, 0, tzinfo=_KST)
    rows = tuple({"symbol": sym, "rank": rank} for rank, sym in enumerate(symbols, start=1))
    write_candidate_snapshot(
        path,
        CandidateSnapshot(
            schema_version=1, rev=int(day.strftime("%Y%m%d")), session_date=day, session=session,
            generated_at=stamp, source_asof=stamp, effective_from=stamp, policy_version="test",
            capacity=len(rows), eligible_count=len(rows), selected_count=len(rows), candidates=rows,
        ),
    )


def test_nxt_stream_refuses_without_slots_or_on_shifted_day(monkeypatch, tmp_path) -> None:
    from src.core.session_anchors import AnchorSource, SessionAnchors

    day = dt.date(2026, 9, 16)
    symbols = tuple(f"{i:06d}" for i in range(20))
    _write_nxt_snapshot(tmp_path / "nxt.json", day=day, symbols=symbols)
    args = _args(tmp_path, day="2026-09-16", symbols=",".join(symbols[:20]), slot="2", key_id="fp2")
    args.venue = "nxt"
    args.candidates_path = str(tmp_path / "nxt.json")
    monkeypatch.setattr(collect_regular_kis, "load_kis_data_credentials", lambda: _creds())
    monkeypatch.setattr(collect_regular_kis, "resolve_collector_runtime", lambda **kw: _runtime(nxt_slots=()))
    with pytest.raises(KrxAlphaError, match="no configured credential slots"):
        asyncio.run(collect_regular_kis._run_stream(args))

    shifted = SessionAnchors(
        date=day, regular_open=dt.time(9, 5), closing_auction_start=dt.time(15, 25),
        regular_close=dt.time(15, 30), after_market_end=dt.time(18, 0), source=AnchorSource.DEFAULT,
    )
    monkeypatch.setattr(collect_regular_kis, "resolve_collector_runtime", lambda **kw: _runtime(nxt_slots=("2",)))
    monkeypatch.setattr(collect_regular_kis, "resolve_session_anchors", lambda *a, **kw: shifted)
    with pytest.raises(KrxAlphaError, match="shifted session"):
        asyncio.run(collect_regular_kis._run_stream(args))


def test_nxt_stream_rejects_stale_snapshot_and_symbol_mismatch(monkeypatch, tmp_path) -> None:
    day = dt.date(2026, 9, 16)
    symbols = tuple(f"{i:06d}" for i in range(20))
    monkeypatch.setattr(collect_regular_kis, "load_kis_data_credentials", lambda: _creds())
    monkeypatch.setattr(collect_regular_kis, "resolve_collector_runtime", lambda **kw: _runtime(nxt_slots=("2",)))
    monkeypatch.setattr(
        collect_regular_kis, "bootstrap_session", lambda cfg: (_ for _ in ()).throw(AssertionError("no bootstrap"))
    )
    args = _args(tmp_path, day="2026-09-16", symbols=",".join(symbols), slot="2", key_id="fp2")
    args.venue = "nxt"

    args.candidates_path = str(tmp_path / "missing.json")
    with pytest.raises(KrxAlphaError):
        asyncio.run(collect_regular_kis._run_stream(args))

    _write_nxt_snapshot(tmp_path / "stale.json", day=day - dt.timedelta(days=1), symbols=symbols)
    args.candidates_path = str(tmp_path / "stale.json")
    with pytest.raises(KrxAlphaError):
        asyncio.run(collect_regular_kis._run_stream(args))

    _write_nxt_snapshot(tmp_path / "ok.json", day=day, symbols=symbols)
    args.candidates_path = str(tmp_path / "ok.json")
    args.symbols = ",".join(symbols[1:]) + ",000099"
    with pytest.raises(MissingCredentialsError, match="do not match"):
        asyncio.run(collect_regular_kis._run_stream(args))


def test_collect_parser_venue_defaults_to_krx_and_accepts_nxt() -> None:
    import argparse

    root = argparse.ArgumentParser()
    collect_regular_kis.add_parser(root.add_subparsers(dest="command"))
    base = [
        "collect-regular-kis",
        "--session-date", "2026-09-16",
        "--journal-root", "journal",
        "--manifest-path", "manifest.json",
        "--candidates-path", "pool.json",
        "--shard-index", "0",
        "--credential-slot", "2",
        "--credential-key-id", "fp2",
        "--symbols", "000001",
    ]

    assert root.parse_args(base).venue == "krx"
    assert root.parse_args([*base, "--venue", "nxt"]).venue == "nxt"
    with pytest.raises(SystemExit):
        root.parse_args([*base, "--venue", "kosdaq"])


def test_child_recomputes_daemon_plan_with_ls_blocks(monkeypatch, tmp_path) -> None:
    from src.orchestration.regular_kis import ls_shard_blocks, regular_kis_ls_plan_inputs
    from src.realtime.kis_sharding import plan_regular_kis_shards

    day = dt.date(2026, 9, 16)
    symbols = tuple(f"{i:06d}" for i in range(1, 61))
    _write_candidates(tmp_path / "pool.json", day=day, symbols=symbols)
    runtime = _runtime()
    runtime.collector.vendor = "ls"
    runtime.collector.streams = ("H0STCNT0", "H0STASP0")
    runtime.collector.ls_active_sessions = 2
    runtime.collector.ls_max_subscriptions_per_session = 40
    runtime.collector.ls_dynamic_reserve_symbols = 0
    runtime.collector.ls_max_sessions = 3
    monkeypatch.setattr(collect_regular_kis, "resolve_collector_runtime", lambda **kw: runtime)
    monkeypatch.setattr(collect_regular_kis, "load_kis_data_credentials", lambda: _creds())
    monkeypatch.setattr(
        collect_regular_kis, "bootstrap_session", lambda cfg: (_ for _ in ()).throw(AssertionError("no bootstrap"))
    )
    blocks = ls_shard_blocks(tmp_path / "pool.json", runtime.collector)
    assert blocks == (symbols[:20], symbols[20:40])
    covered, ls_blocks = regular_kis_ls_plan_inputs(blocks)
    daemon_plan = plan_regular_kis_shards(
        symbols=symbols, credentials=_creds(), credential_slots=("1", "2"),
        pair_capacity_per_connection=41, streams=("H0STCNT0", "H0STASP0"),
        ls_covered_symbols=covered, ls_shard_symbols=ls_blocks,
    )
    plain_plan = plan_regular_kis_shards(
        symbols=symbols, credentials=_creds(), credential_slots=("1", "2"),
        pair_capacity_per_connection=41, streams=("H0STCNT0", "H0STASP0"),
    )
    assert daemon_plan[1].symbols != plain_plan[1].symbols
    args = _args(tmp_path, day="2026-09-16", symbols=",".join(daemon_plan[1].symbols), shard=1, slot="2", key_id="fp2")
    with pytest.raises(AssertionError, match="no bootstrap"):
        asyncio.run(collect_regular_kis._run_stream(args))
    stale = _args(tmp_path, day="2026-09-16", symbols=",".join(plain_plan[1].symbols), shard=1, slot="2", key_id="fp2")
    with pytest.raises(MissingCredentialsError):
        asyncio.run(collect_regular_kis._run_stream(stale))


def test_ls_shard_blocks_empty_for_non_ls_vendor_and_unreadable_candidates(tmp_path) -> None:
    from src.orchestration.regular_kis import ls_shard_blocks

    kis = SimpleNamespace(vendor="kis")
    ls = SimpleNamespace(
        vendor="ls", streams=("A", "B"), ls_active_sessions=2, ls_max_subscriptions_per_session=40,
        ls_dynamic_reserve_symbols=0, ls_max_sessions=3,
    )
    assert ls_shard_blocks(tmp_path / "pool.json", kis) == ()
    assert ls_shard_blocks(tmp_path / "missing.json", ls) == ()
