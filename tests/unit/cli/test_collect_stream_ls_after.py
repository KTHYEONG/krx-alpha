"""LS aftermarket child invariants: pair plan, lease refusal, stop deadline, and LS-shard-0 branch exclusions."""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import logging
import types
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from src.cli import collect_stream, collect_stream_ls_after
from src.core.config import CollectorSettings, DataPaths
from src.realtime.contracts import MarketSession, MarketVenue
from src.realtime.session import StreamRoute
from src.realtime.ws_lease import LeaseRole
from src.universe.intraday import DynamicEntry, DynamicPlan, write_dynamic_admissions

DAY = dt.date(2026, 11, 23)
_KRX = ("H0STCNT0", "H0STASP0")
_NXT = ("H0NXCNT0", "H0NXASP0")
_KST = ZoneInfo("Asia/Seoul")


def _plan(krx, nxt, members, cap=200):
    return collect_stream_ls_after.build_ls_after_pair_plan(krx, nxt, frozenset(members), cap)


def test_subcommand_registered() -> None:
    from src.cli.main import build_parser

    args = build_parser().parse_args(
        [
            "collect-stream-ls-after", "--session-date", "2026-11-23", "--journal-root", "l0",
            "--manifest-path", "m.json", "--market-map", "mm.json", "--krx-candidates-path", "k.json",
            "--nxt-candidates-path", "n.json", "--membership-path", "mb.jsonl", "--dynamic-plan-path", "d.json",
            "--end-time", "20:00",
        ]
    )
    assert args.command == "collect-stream-ls-after"
    assert args.end_time == "20:00"


def test_plan_excludes_ls_members() -> None:
    plan = _plan(["A", "B", "C"], [], {"B"})
    assert list(plan) == ["A", "C"]
    assert all(streams == _KRX for streams in plan.values())


def test_plan_merges_both_families() -> None:
    plan = _plan(["A"], ["A", "D"], set())
    assert plan["A"] == (*_KRX, *_NXT)
    assert plan["D"] == _NXT


def test_plan_caps_krx_first() -> None:
    plan = _plan(["A", "B"], ["D"], set(), cap=4)
    assert list(plan) == ["A", "B"]


def test_unreadable_membership_keeps_all_krx(tmp_path, caplog) -> None:
    garbage = tmp_path / "membership.jsonl"
    garbage.write_text("not-json\n", encoding="utf-8")
    with caplog.at_level(logging.WARNING):
        members = collect_stream_ls_after._ls_member_symbols(garbage, tmp_path / "missing.json", 2)
    assert members == frozenset()
    assert any("MEMBERSHIP_UNREADABLE" in record.getMessage() for record in caplog.records)
    assert list(_plan(["A", "B"], [], members)) == ["A", "B"]


def test_ls_members_from_dynamic_plan_are_krx_admissions_only(tmp_path) -> None:
    plan_path = tmp_path / "dyn.json"
    rows = (("000010", "ls:0", "krx"), ("000020", "ls:1", "krx"), ("000030", "ls:1", "nxt"), ("000040", "ls:2", "krx"))
    write_dynamic_admissions(plan_path, DynamicPlan(rev=1, entries=tuple(DynamicEntry(s, f, 1, v) for s, f, v in rows)))
    members = collect_stream_ls_after._ls_member_symbols(tmp_path / "none.jsonl", plan_path, 2)
    assert members == frozenset({"000010", "000020"})


def _session(pairs):
    return types.SimpleNamespace(
        manifest=types.SimpleNamespace(subscription_acks=[], gaps=[], boots=[]),
        replay_pairs=lambda: list(pairs),
    )


class _Creds:
    app_key = "ls-app-key"


class _Entry:
    requires_market_map = False

    def load_credentials(self) -> _Creds:
        return _Creds()


class _Ledger:
    def __init__(self) -> None:
        self.acquired: list[tuple[str, LeaseRole]] = []

    def acquire(self, owner: str, role: LeaseRole) -> object:
        self.acquired.append((owner, role))
        return object()

    def renew(self, lease: object) -> bool:
        return True

    def release(self, lease: object) -> None:
        return None


def _profile(plan: dict[str, tuple[str, ...]], *, require_lease: bool = True) -> collect_stream.RegularStreamProfile:
    return collect_stream.RegularStreamProfile(
        vendor="ls",
        adapter="ls-after",
        slot_budget=200,
        symbol_limit=None,
        route=StreamRoute(MarketVenue.NXT, MarketSession.NXT_AFTER),
        shutdown_log="[DATA] stage=stream_shutdown vendor=ls-after manifest=%s acks=%d gaps=%d boots=%d",
        session_index=0,
        hot_symbols=0,
        pair_plan=plan,
        stream_routes=collect_stream_ls_after._stream_routes(plan),
        lease_role=LeaseRole.STANDBY,
        require_lease=require_lease,
        feed_role="after",
    )


def _args(tmp_path, dyn_path=None) -> argparse.Namespace:
    return argparse.Namespace(
        session_date=DAY.isoformat(),
        journal_root=str(tmp_path / "l0"),
        manifest_path=str(tmp_path / "m.json"),
        candidates_path=str(tmp_path / "ignored.json"),
        market_map=None,
        ntp_host=None,
        max_clock_offset_ns=None,
        max_cycles=1,
        degraded_reason=None,
        dynamic_plan_path=str(dyn_path or tmp_path / "dyn.json"),
    )


@pytest.fixture
def child_env(monkeypatch, tmp_path):
    seen: dict[str, Any] = {"streaming": None, "bootstrap": None}

    def _child(**kwargs):
        seen["streaming"] = kwargs

        async def _go() -> None:
            return None

        return _go()

    class _Tokens:
        token_cache_dir = tmp_path / "tokens"

    monkeypatch.setattr(collect_stream, "regular_vendor", lambda vendor: _Entry())
    monkeypatch.setattr(collect_stream, "KisTokenSettings", _Tokens)
    monkeypatch.setattr(collect_stream, "run_streaming_child", _child)
    monkeypatch.setattr(collect_stream, "build_feed_health_tracker", lambda **kwargs: None)
    return seen


def _run_profile(tmp_path, settings, profile, seen, *, stop_deadline_ns=None) -> int:
    def _bootstrap(cfg):
        seen["cfg"] = cfg
        return _session([(s, st) for s, sts in cfg.pair_plan.items() for st in sts])

    return asyncio.run(
        collect_stream.run_regular_stream(
            _args(tmp_path),
            settings,
            profile,
            bootstrap=_bootstrap,
            streamer_factory=None,
            stop_deadline_ns=stop_deadline_ns,
        )
    )


def test_no_pairs_exits_zero_without_connect(monkeypatch, tmp_path, child_env) -> None:
    def _fail(*args, **kwargs):
        raise AssertionError("must not run the child")

    monkeypatch.setattr(collect_stream, "run_regular_stream", _fail)
    monkeypatch.setattr(collect_stream_ls_after, "CollectorSettings", lambda: CollectorSettings(data_root=tmp_path))
    args = argparse.Namespace(
        session_date=DAY.isoformat(),
        journal_root=str(tmp_path / "l0"),
        manifest_path=str(tmp_path / "m.json"),
        market_map=str(tmp_path / "mm.json"),
        krx_candidates_path=str(tmp_path / "missing-k.json"),
        nxt_candidates_path=str(tmp_path / "missing-n.json"),
        membership_path=str(tmp_path / "mb.jsonl"),
        dynamic_plan_path=str(tmp_path / "dyn.json"),
        end_time=None,
    )
    assert asyncio.run(collect_stream_ls_after._run_stream(args)) == 0
    assert child_env["streaming"] is None


def test_no_ledger_refuses_without_connect(monkeypatch, tmp_path, child_env) -> None:
    monkeypatch.setattr(collect_stream, "_lease_ledger", lambda *a, **k: None)
    plan = {"A": _KRX}
    code = _run_profile(tmp_path, CollectorSettings(data_root=tmp_path / "d", ls_active_sessions=1), _profile(plan), child_env)
    assert code == collect_stream.SHARD_DENIED_EXIT
    assert child_env["streaming"] is None


def test_standby_role_requested_on_strict_lease(monkeypatch, tmp_path, child_env) -> None:
    ledger = _Ledger()
    monkeypatch.setattr(collect_stream, "_lease_ledger", lambda *a, **k: ledger)
    plan = {"A": _KRX}
    code = _run_profile(tmp_path, CollectorSettings(data_root=tmp_path / "d", ls_active_sessions=2), _profile(plan), child_env)
    assert code == 0
    assert [role for _, role in ledger.acquired] == [LeaseRole.STANDBY]


def test_stop_deadline_is_passed_to_streamer(monkeypatch, tmp_path, child_env) -> None:
    ledger = _Ledger()
    monkeypatch.setattr(collect_stream, "_lease_ledger", lambda *a, **k: ledger)
    deadline = int(dt.datetime(2026, 11, 23, 20, 0, tzinfo=_KST).timestamp()) * 1_000_000_000
    plan = {"A": _KRX}
    code = _run_profile(
        tmp_path, CollectorSettings(data_root=tmp_path / "d", ls_active_sessions=2), _profile(plan), child_env,
        stop_deadline_ns=deadline,
    )
    assert code == 0
    assert child_env["streaming"]["stop_deadline_ns"] == deadline


def test_end_time_resolves_to_kst_wall_clock() -> None:
    settings = CollectorSettings()
    args = argparse.Namespace(end_time="20:00")
    expected = int(dt.datetime(2026, 11, 23, 20, 0, tzinfo=_KST).timestamp()) * 1_000_000_000
    assert collect_stream_ls_after._stop_deadline_ns(args, settings, DAY) == expected


def test_ls_shard_zero_behaviours_are_excluded(monkeypatch, tmp_path, child_env) -> None:
    ledger = _Ledger()
    monkeypatch.setattr(collect_stream, "_lease_ledger", lambda *a, **k: ledger)
    plan = {"A": _KRX, "B": (*_KRX, *_NXT)}
    dyn = tmp_path / "dyn.json"
    write_dynamic_admissions(dyn, DynamicPlan(rev=1, entries=(DynamicEntry("Z", "ls:1", 1, "krx"),)))
    settings = CollectorSettings(data_root=tmp_path / "d", ls_active_sessions=2)
    code = _run_profile(tmp_path, settings, _profile(plan), child_env)
    assert code == 0
    cfg = child_env["cfg"]
    streaming = child_env["streaming"]
    assert cfg.ls_guard is None
    assert streaming["handover"] is None
    assert streaming["market_state_path"] is None
    assert streaming["live_watch"] is None
    assert streaming["replay_pairs"] == [("A", s) for s in _KRX] + [("B", s) for s in (*_KRX, *_NXT)]
    assert cfg.route == StreamRoute(MarketVenue.NXT, MarketSession.NXT_AFTER)
    assert set(cfg.stream_routes) == set(_KRX) | set(_NXT)
    assert cfg.desired_streams == (*_KRX, *_NXT)
    assert DataPaths(tmp_path).ls_after_manifest_path(DAY).name == "2026-11-23.ls_after.json"


def test_capacity_break_drops_the_rest_of_the_krx_family() -> None:
    plan = _plan(["A", "B", "C"], [], set(), cap=4)
    assert list(plan) == ["A", "B"]


def test_end_time_defaults_to_session_after_market_end(tmp_path) -> None:
    settings = CollectorSettings(data_root=tmp_path / "data")
    expected = int(dt.datetime(2026, 11, 23, 20, 0, tzinfo=_KST).timestamp()) * 1_000_000_000
    assert collect_stream_ls_after._stop_deadline_ns(argparse.Namespace(end_time=None), settings, DAY) == expected


def test_unreadable_admissions_keep_members_empty(tmp_path, caplog) -> None:
    plan_path = tmp_path / "dyn.json"
    plan_path.write_text("{not json", encoding="utf-8")
    with caplog.at_level(logging.WARNING):
        members = collect_stream_ls_after._ls_member_symbols(tmp_path / "none.jsonl", plan_path, 2)
    assert members == frozenset()
    assert any("ADMISSIONS_UNREADABLE" in record.getMessage() for record in caplog.records)


class _Snapshot:
    def __init__(self, symbols) -> None:
        self.candidates = tuple({"symbol": symbol} for symbol in symbols)


def _capture_child(monkeypatch, tmp_path, *, krx, nxt, settings_kw=None):
    seen: dict[str, Any] = {}

    def _snapshot(path, **kwargs):
        if path.name == "krx.json":
            return _Snapshot(krx)
        return _Snapshot(nxt)

    async def _record(args, settings, profile, **kwargs):
        seen["profile"] = profile
        seen["args"] = args
        seen["deadline"] = kwargs.get("stop_deadline_ns")
        return 0

    monkeypatch.setattr(collect_stream_ls_after, "read_candidate_snapshot", _snapshot)
    monkeypatch.setattr(collect_stream, "run_regular_stream", _record)
    settings_kw = settings_kw or {}
    monkeypatch.setattr(
        collect_stream_ls_after,
        "CollectorSettings",
        lambda: CollectorSettings(data_root=tmp_path / "data", **settings_kw),
    )
    return seen


def _child_args(tmp_path, *, end_time=None) -> argparse.Namespace:
    return argparse.Namespace(
        session_date=DAY.isoformat(),
        journal_root=str(tmp_path / "l0"),
        manifest_path=str(tmp_path / "m.json"),
        market_map=str(tmp_path / "mm.json"),
        krx_candidates_path=str(tmp_path / "krx.json"),
        nxt_candidates_path=str(tmp_path / "nxt.json"),
        membership_path=str(tmp_path / "mb.jsonl"),
        dynamic_plan_path=str(tmp_path / "dyn.json"),
        end_time=end_time,
    )


def test_happy_path_builds_profile_from_candidates(monkeypatch, tmp_path) -> None:
    seen = _capture_child(monkeypatch, tmp_path, krx=["000010", "000020"], nxt=["000020", "000030"])
    assert asyncio.run(collect_stream_ls_after._run_stream(_child_args(tmp_path, end_time="20:00"))) == 0
    profile = seen["profile"]
    assert profile.pair_plan == {
        "000010": _KRX,
        "000020": (*_KRX, *_NXT),
        "000030": _NXT,
    }
    assert profile.lease_role is LeaseRole.STANDBY
    assert profile.require_lease
    assert profile.feed_role == "after"
    assert profile.adapter == "ls-after"
    assert seen["args"].candidates_path == str(tmp_path / "krx.json")
    assert seen["deadline"] == int(dt.datetime(2026, 11, 23, 20, 0, tzinfo=_KST).timestamp()) * 1_000_000_000


def test_truncated_plan_still_starts_within_capacity(monkeypatch, tmp_path, caplog) -> None:
    seen = _capture_child(
        monkeypatch, tmp_path, krx=["000010", "000020", "000040"], nxt=["000050"], settings_kw={"ls_capacity_pairs": 4}
    )
    with caplog.at_level(logging.WARNING):
        assert asyncio.run(collect_stream_ls_after._run_stream(_child_args(tmp_path))) == 0
    assert list(seen["profile"].pair_plan) == ["000010", "000020"]
    assert any("TRUNCATED dropped=2" in record.getMessage() for record in caplog.records)


def test_run_entry_point_returns_zero_without_candidates(monkeypatch, tmp_path) -> None:
    def _snapshot(path, **kwargs):
        raise collect_stream_ls_after.CandidateFileError("missing")

    monkeypatch.setattr(collect_stream_ls_after, "read_candidate_snapshot", _snapshot)
    monkeypatch.setattr(
        collect_stream_ls_after,
        "CollectorSettings",
        lambda: CollectorSettings(data_root=tmp_path / "data"),
    )
    assert collect_stream_ls_after.run(_child_args(tmp_path)) == 0


def test_all_candidates_on_ls_exit_zero_without_child(monkeypatch, tmp_path) -> None:
    write_dynamic_admissions(
        tmp_path / "dyn.json", DynamicPlan(rev=1, entries=(DynamicEntry("000010", "ls:0", 1, "krx"),))
    )
    seen = _capture_child(monkeypatch, tmp_path, krx=["000010"], nxt=[])
    assert asyncio.run(collect_stream_ls_after._run_stream(_child_args(tmp_path))) == 0
    assert "profile" not in seen
