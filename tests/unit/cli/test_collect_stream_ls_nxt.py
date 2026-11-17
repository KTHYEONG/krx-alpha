"""LS-NXT on-demand child invariants: arguments, NXT profile, strict LS lease, dynamic plan filtering."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import datetime as dt
import types
from typing import Any

import pytest

from src.cli import collect_stream, collect_stream_ls_nxt
from src.core.config import CollectorSettings
from src.core.paths import DataPaths
from src.realtime.contracts import MarketSession, MarketVenue
from src.realtime.ws_lease import LeaseRole
from src.universe.intraday import DynamicEntry, DynamicPlan, write_dynamic_admissions

_NX_CNT = "H0NXCNT0"
_NX_ASP = "H0NXASP0"
DAY = dt.date(2026, 11, 17)


def test_subcommand_registered_with_plan_path() -> None:
    from src.cli.main import build_parser

    args = build_parser().parse_args(
        [
            "collect-stream-ls-nxt", "--session-date", "2026-11-17", "--journal-root", "l0",
            "--manifest-path", "m.ls_nxt.json", "--candidates-path", "c.json", "--dynamic-plan-path", "dyn.json",
        ]
    )
    assert args.command == "collect-stream-ls-nxt"
    assert callable(args.handler)


def test_child_runs_a_warm_nxt_profile_journaled_as_ls(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, Any] = {}

    async def _run_regular(args: Any, settings: Any, profile: Any, **kwargs: Any) -> int:
        seen["profile"] = profile
        return 0

    monkeypatch.setattr(collect_stream, "run_regular_stream", _run_regular)
    assert asyncio.run(collect_stream_ls_nxt._run_stream(argparse.Namespace())) == 0
    profile = seen["profile"]
    assert (profile.vendor, profile.adapter) == ("ls", "ls-nxt")
    assert profile.warm_standby
    assert profile.hot_symbols == 0
    assert profile.session_index == 0
    assert (profile.route.venue, profile.route.session) == (MarketVenue.NXT, MarketSession.REGULAR)


def test_streams_feed_id_and_manifest_path(tmp_path) -> None:
    from src.realtime.feed_health import feed_id_for

    assert collect_stream.vendor_streams(CollectorSettings(), "ls", "nxt") == (_NX_CNT, _NX_ASP)
    assert collect_stream.vendor_streams(CollectorSettings(), "ls") == ("H0STCNT0", "H0STASP0")
    assert feed_id_for("ls", "nxt", 0) == "ls-nxt:0"
    paths = DataPaths(tmp_path)
    assert paths.ls_nxt_manifest_path(DAY) == paths.manifest_dir / "2026-11-17.ls_nxt.json"
    assert paths.feed_health_path("ls-nxt:0").name == "ls-nxt__0.json"


def test_watcher_takes_only_ls_nxt_admissions_of_the_nxt_venue(tmp_path) -> None:
    plan_path = tmp_path / "dyn.json"
    rows = (
        ("005930", "ls-nxt:0", "nxt"),
        ("000660", "ls-nxt:0", "krx"),
        ("035420", "kis:5", "nxt"),
        ("051910", "ls:1", "krx"),
    )
    write_dynamic_admissions(
        plan_path, DynamicPlan(rev=1, entries=tuple(DynamicEntry(s, f, 1, v) for s, f, v in rows))
    )
    watch = collect_stream._warm_dynamic_watch(
        session=types.SimpleNamespace(),  # type: ignore[arg-type]
        session_date=DAY,
        settings=CollectorSettings(data_root=tmp_path / "data"),
        vendor="ls",
        session_index=0,
        plan_path=plan_path,
        static_pairs=[],
        venue="nxt",
    )
    assert watch.pairs() == [("005930", _NX_CNT), ("005930", _NX_ASP)]


class _Creds:
    app_key = "ls-app-key"


class _Entry:
    requires_market_map = False

    def load_credentials(self) -> _Creds:
        return _Creds()


def _session() -> Any:
    return types.SimpleNamespace(
        manifest=types.SimpleNamespace(subscription_acks=[], gaps=[], boots=[]),
        replay_pairs=lambda: [],
        note_ack=lambda **kw: None,
    )


def _run(monkeypatch, tmp_path, seen: dict, settings: CollectorSettings, *, sleep_s: float = 0.0) -> int:
    class _Tokens:
        token_cache_dir = tmp_path / "tokens"

    def _child(**kwargs):
        seen.update(kwargs)

        async def _go() -> None:
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.sleep(sleep_s)

        return _go()

    async def _profile_run() -> int:
        captured: dict[str, Any] = {}

        async def _record(args, settings_, profile, **kw):
            captured["profile"] = profile
            return await real(args, settings_, profile, **kw)

        return await _record(
            argparse.Namespace(
                session_date="2026-11-17", journal_root=str(tmp_path / "l0"), manifest_path=str(tmp_path / "m.json"),
                candidates_path=str(tmp_path / "c.json"), dynamic_plan_path=str(tmp_path / "dyn.json"),
                ntp_host=None, max_clock_offset_ns=None, max_cycles=1, degraded_reason=None,
            ),
            settings,
            _nxt_profile(),
            bootstrap=lambda cfg: _session(),
            streamer_factory=None,
        )

    real = collect_stream.run_regular_stream
    monkeypatch.setattr(collect_stream, "regular_vendor", lambda vendor: _Entry())
    monkeypatch.setattr(collect_stream, "KisTokenSettings", _Tokens)
    monkeypatch.setattr(collect_stream, "run_streaming_child", _child)
    monkeypatch.setattr(collect_stream, "build_feed_health_tracker", lambda **kwargs: None)
    return asyncio.run(_profile_run())


def _nxt_profile() -> collect_stream.RegularStreamProfile:
    from src.realtime.session import StreamRoute

    return collect_stream.RegularStreamProfile(
        vendor="ls", adapter="ls-nxt", slot_budget=200, symbol_limit=1,
        route=StreamRoute(MarketVenue.NXT, MarketSession.REGULAR), shutdown_log="stop %s %d %d %d",
        session_index=0, warm_standby=True,
    )


def test_lease_denied_while_handover_holds_the_slots(tmp_path, monkeypatch) -> None:
    settings = CollectorSettings(data_root=tmp_path / "data", ls_active_sessions=2)
    ledger = collect_stream._lease_ledger("ls", settings, tmp_path / "tokens", "ls-app-key")
    assert ledger is not None
    for index in range(settings.ls_max_sessions):
        ledger.acquire(f"ls-shard-{index}", LeaseRole.PRIMARY)
    seen: dict[str, Any] = {}
    assert _run(monkeypatch, tmp_path, seen, settings) == collect_stream.SHARD_DENIED_EXIT
    assert seen == {}


def test_nxt_child_runs_with_a_free_slot_and_arms_silence_only_when_subscribed(tmp_path, monkeypatch) -> None:
    settings = CollectorSettings(data_root=tmp_path / "data", ls_active_sessions=2)
    seen: dict[str, Any] = {}
    assert _run(monkeypatch, tmp_path, seen, settings) == 0
    assert seen["replay_pairs"] == []
    assert seen["silence_limit"]() is None


def test_run_returns_the_stream_exit_code(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _stream(args: Any) -> int:
        return 3

    monkeypatch.setattr(collect_stream_ls_nxt, "_run_stream", _stream)
    assert collect_stream_ls_nxt.run(argparse.Namespace()) == 3
