"""LS NXT regular spare child invariants: rank slice after the KIS pool, NO_PAIRS exit, spare lease profile, stop deadline."""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import logging
from typing import Any
from zoneinfo import ZoneInfo

from src.cli import collect_stream, collect_stream_ls_nxt_regular
from src.core.config import CollectorSettings, RegularKisSettings
from src.realtime.adapters.ls import LS_NXT_STREAMS
from src.realtime.contracts import MarketSession, MarketVenue
from src.realtime.ws_lease import LeaseRole

DAY = dt.date(2026, 11, 24)
_KST = ZoneInfo("Asia/Seoul")
_CAPACITY_PAIRS = 200


def _ranked(count: int) -> tuple[tuple[str, int], ...]:
    return tuple((f"{rank:06d}", rank) for rank in range(1, count + 1))


class _Snapshot:
    def __init__(self, count: int) -> None:
        self.candidates = tuple({"symbol": f"{rank:06d}", "rank": rank} for rank in range(1, count + 1))


def _child_args(tmp_path, kis_nxt_symbols: int = 60) -> argparse.Namespace:
    return argparse.Namespace(
        session_date=DAY.isoformat(),
        journal_root=str(tmp_path / "l0"),
        manifest_path=str(tmp_path / "m.json"),
        candidates_path=str(tmp_path / "regular_nxt.json"),
        kis_nxt_symbols=kis_nxt_symbols,
    )


def _capture(monkeypatch, tmp_path, pool_size: int) -> dict[str, Any]:
    seen: dict[str, Any] = {}

    def _snapshot(path, **kwargs):
        seen["max_candidates"] = kwargs["max_candidates"]
        seen["expected_session"] = kwargs["expected_session"]
        return _Snapshot(pool_size)

    async def _record(args, settings, profile, **kwargs):
        seen["profile"] = profile
        seen["deadline"] = kwargs.get("stop_deadline_ns")
        return 0

    monkeypatch.setattr(collect_stream_ls_nxt_regular, "read_candidate_snapshot", _snapshot)
    monkeypatch.setattr(collect_stream, "run_regular_stream", _record)
    monkeypatch.setattr(
        collect_stream_ls_nxt_regular,
        "CollectorSettings",
        lambda: CollectorSettings(data_root=tmp_path / "data", ls_capacity_pairs=_CAPACITY_PAIRS),
    )
    return seen


def test_plan_takes_ranks_after_kis() -> None:
    plan = collect_stream_ls_nxt_regular.build_ls_nxt_regular_plan(_ranked(160), 60, _CAPACITY_PAIRS)
    expected = {f"{rank:06d}" for rank in range(61, 161)}
    assert set(plan) == expected
    assert len(plan) * len(LS_NXT_STREAMS) == _CAPACITY_PAIRS
    assert all(streams == LS_NXT_STREAMS for streams in plan.values())
    assert not set(plan) & {f"{rank:06d}" for rank in range(1, 61)}


def test_short_pool_yields_partial_plan() -> None:
    plan = collect_stream_ls_nxt_regular.build_ls_nxt_regular_plan(_ranked(75), 60, _CAPACITY_PAIRS)
    assert set(plan) == {f"{rank:06d}" for rank in range(61, 76)}


def test_pool_within_kis_exits_without_connection(monkeypatch, tmp_path) -> None:
    seen = _capture(monkeypatch, tmp_path, pool_size=60)
    assert asyncio.run(collect_stream_ls_nxt_regular._run_stream(_child_args(tmp_path))) == 0
    assert "profile" not in seen


def test_profile_is_spare_and_requires_lease(monkeypatch, tmp_path) -> None:
    seen = _capture(monkeypatch, tmp_path, pool_size=160)
    assert asyncio.run(collect_stream_ls_nxt_regular._run_stream(_child_args(tmp_path))) == 0
    profile = seen["profile"]
    assert profile.lease_role is LeaseRole.SPARE
    assert profile.require_lease
    assert profile.adapter == "ls-nxt"
    assert profile.route is not None
    assert (profile.route.venue, profile.route.session) == (MarketVenue.NXT, MarketSession.REGULAR)
    assert set(profile.stream_routes) == set(LS_NXT_STREAMS)
    assert all(route == profile.route for route in profile.stream_routes.values())
    assert seen["expected_session"] == "regular_nxt"
    assert seen["max_candidates"] == 60 + _CAPACITY_PAIRS // len(LS_NXT_STREAMS)
    close = dt.datetime.combine(DAY, dt.time(15, 20), tzinfo=_KST)
    grace = RegularKisSettings().close_grace_s
    assert seen["deadline"] == int((close + dt.timedelta(seconds=grace)).timestamp()) * 1_000_000_000


def test_plan_log_reports_first_rank(monkeypatch, tmp_path, caplog) -> None:
    _capture(monkeypatch, tmp_path, pool_size=160)
    with caplog.at_level("INFO"):
        asyncio.run(collect_stream_ls_nxt_regular._run_stream(_child_args(tmp_path)))
    assert any("stage=ls_nxt_regular_child status=START symbols=100 pairs=200 first_rank=61" in r.getMessage() for r in caplog.records)


def test_run_entry_point_starts_child_and_returns_its_exit(monkeypatch, tmp_path) -> None:
    seen = _capture(monkeypatch, tmp_path, pool_size=160)
    assert collect_stream_ls_nxt_regular.run(_child_args(tmp_path)) == 0
    assert seen["profile"].lease_role is LeaseRole.SPARE


def test_unreadable_pool_warns_and_exits_without_connection(monkeypatch, tmp_path, caplog) -> None:
    from src.universe.ipc import CandidateFileError

    seen = _capture(monkeypatch, tmp_path, pool_size=160)

    def _missing(path, **kwargs):
        raise CandidateFileError("missing")

    monkeypatch.setattr(collect_stream_ls_nxt_regular, "read_candidate_snapshot", _missing)
    with caplog.at_level(logging.WARNING):
        assert asyncio.run(collect_stream_ls_nxt_regular._run_stream(_child_args(tmp_path))) == 0
    assert "profile" not in seen
    assert any("CANDIDATES_UNREADABLE" in r.getMessage() for r in caplog.records)
