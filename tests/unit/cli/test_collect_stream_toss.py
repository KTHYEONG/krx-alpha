"""Toss warm-standby streamer CLI invariants: arguments, leases, paths, unified route, dynamic plan."""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import pathlib
from types import SimpleNamespace
from typing import Any

import pytest

from src.cli import collect_stream, collect_stream_toss
from src.core.config import CollectorSettings
from src.core.errors import KrxAlphaError
from src.core.paths import DataPaths
from src.realtime.adapters.toss import TossRealtimeAdapter
from src.realtime.contracts import MarketSession, MarketVenue
from src.realtime.ws_lease import LeaseRole, LeaseUnavailable
from src.universe.intraday import DynamicEntry, DynamicPlan, read_dynamic_admissions, write_dynamic_admissions

_CNT = "H0STCNT0"
_ASP = "H0STASP0"
DAY = dt.date(2026, 11, 17)


class _Session:
    def note_ack(self, **kwargs: Any) -> None:
        return None


def _publish(plan_path: pathlib.Path, *entries: tuple[str, str, str]) -> None:
    rev = read_dynamic_admissions(plan_path).rev + 1 if plan_path.exists() else 1
    write_dynamic_admissions(
        plan_path, DynamicPlan(rev=rev, entries=tuple(DynamicEntry(s, f, 1, venue) for s, f, venue in entries))
    )


def test_subcommand_takes_toss_session_index_and_hot_symbols() -> None:
    from src.cli.main import build_parser

    args = build_parser().parse_args([
        "collect-stream-toss", "--session-date", "2026-11-17", "--journal-root", "l0",
        "--manifest-path", "m.toss.1.json", "--candidates-path", "c.json",
        "--dynamic-plan-path", "dyn.json", "--toss-session-index", "1", "--hot-symbols", "10",
    ])
    assert args.command == "collect-stream-toss"
    assert callable(args.handler)
    assert (args.toss_session_index, args.hot_symbols) == (1, 10)
    assert not hasattr(args, "market_map")


def test_hot_symbols_are_bounded_by_topic_capacity_not_symbol_streams(tmp_path: pathlib.Path) -> None:
    def args(hot: int) -> argparse.Namespace:
        return argparse.Namespace(toss_session_index=0, hot_symbols=hot)

    async def _run_regular(*a: Any, **k: Any) -> int:
        return 0

    from unittest import mock

    with mock.patch.object(collect_stream, "run_regular_stream", _run_regular):
        assert asyncio.run(collect_stream_toss._run_stream(args(100))) == 0  # trade-only: 100 symbols fit one connection
    with pytest.raises(KrxAlphaError):
        asyncio.run(collect_stream_toss._run_stream(args(101)))


def test_child_runs_a_warm_unified_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, Any] = {}

    async def _run_regular(args: Any, settings: Any, profile: Any, **kwargs: Any) -> int:
        seen["profile"] = profile
        return 0

    monkeypatch.setattr(collect_stream, "run_regular_stream", _run_regular)
    assert asyncio.run(collect_stream_toss._run_stream(argparse.Namespace(toss_session_index=1, hot_symbols=0))) == 0
    profile = seen["profile"]
    assert profile.vendor == "toss"
    assert profile.warm_standby
    assert profile.session_index == 1
    assert profile.route.venue is MarketVenue.UNIFIED
    assert profile.route.session is MarketSession.REGULAR
    assert profile.slot_budget == 100


def test_toss_default_streams_are_trade_only_and_book_is_configurable() -> None:
    assert collect_stream.vendor_streams(CollectorSettings(), "toss") == (_CNT,)
    assert collect_stream.vendor_streams(CollectorSettings(toss_streams=(_CNT, _ASP)), "toss") == (_CNT, _ASP)
    assert collect_stream.vendor_streams(CollectorSettings(), "db") == (_CNT, _ASP)
    replay = [("005930", _CNT), ("000660", _CNT)]
    assert collect_stream._warm_static_pairs(replay, (_CNT,), 2) == replay


def test_toss_lease_ledger_allows_two_sessions_and_denies_the_third(tmp_path: pathlib.Path) -> None:
    ledger = collect_stream._lease_ledger("toss", CollectorSettings(), tmp_path, "app-key")
    assert ledger is not None
    assert ledger.acquire("toss-shard-0", LeaseRole.PRIMARY) is not None
    assert ledger.acquire("toss-shard-1", LeaseRole.PRIMARY) is not None
    with pytest.raises(LeaseUnavailable):
        asyncio.run(collect_stream._claim_primary(ledger, owner="toss-shard-2", shard_index=2, ttl_s=30.0, strict=True))


def test_manifest_paths_per_session_index(tmp_path: pathlib.Path) -> None:
    paths = DataPaths(tmp_path)
    assert paths.toss_manifest_path(DAY) == paths.manifest_dir / "2026-11-17.toss.json"
    assert paths.toss_manifest_path(DAY, 1) == paths.manifest_dir / "2026-11-17.toss.1.json"
    with pytest.raises(ValueError, match="session_index"):
        paths.toss_manifest_path(DAY, 2)


def test_warm_watch_consumes_admissions_of_both_venues_and_sends_full_state(tmp_path: pathlib.Path) -> None:
    class _WS:
        def __init__(self) -> None:
            self.inbox: asyncio.Queue[Any] = asyncio.Queue()
            self.sent: list[str] = []

        async def send_str(self, text: str) -> None:
            self.sent.append(text)
            if text.startswith("["):
                topics = [f"{i['type']}:{c}" for i in json.loads(text) for c in i["codes"]]
                data = json.dumps({"type": "subscriptions", "subscribed": topics, "rejected": []})
                self.inbox.put_nowait(SimpleNamespace(type=__import__("aiohttp").WSMsgType.TEXT, data=data))

        async def receive(self, timeout: float | None = None) -> Any:  # noqa: ASYNC109 - mirrors aiohttp's receive signature
            return await asyncio.wait_for(self.inbox.get(), timeout)

    class _Http:
        def __init__(self, ws: _WS) -> None:
            self.ws = ws

        async def ws_connect(self, url: str, **kwargs: Any) -> _WS:
            return self.ws

    class _Store:
        async def aget_or_issue(self, issue: Any) -> str:
            return "TOK"

    async def scenario() -> tuple[_WS, list[tuple[str, str]]]:
        ws = _WS()
        adapter = TossRealtimeAdapter(http=_Http(ws), token_store=_Store(), declare_spacing_s=0.001)  # type: ignore[arg-type]
        await adapter.connect()
        plan_path = tmp_path / "dynamic.json"
        watch = collect_stream._warm_dynamic_watch(
            session=_Session(),  # type: ignore[arg-type]
            session_date=DAY,
            settings=CollectorSettings(data_root=tmp_path / "data"),
            vendor="toss",
            session_index=0,
            plan_path=plan_path,
            static_pairs=[],
        )
        assert watch.pairs() == []

        async def drain() -> None:
            while True:
                await adapter.recv()

        receiver = asyncio.create_task(drain())
        _publish(plan_path, ("005930", "toss:0", "krx"), ("000660", "toss:0", "nxt"), ("035420", "toss:1", "krx"))
        assert await watch.tick(adapter) == 2
        _publish(plan_path, ("005930", "toss:0", "krx"), ("000660", "toss:0", "nxt"), ("009150", "toss:0", "krx"))
        assert await watch.tick(adapter) == 1
        receiver.cancel()
        return ws, watch.pairs()

    ws, pairs = asyncio.run(scenario())
    declarations = [json.loads(s) for s in ws.sent if s.startswith("[")]
    assert declarations == [
        [{"type": "trade:kr", "codes": ["005930", "000660"]}],
        [{"type": "trade:kr", "codes": ["005930", "000660", "009150"]}],
    ]
    assert pairs == [("005930", _CNT), ("000660", _CNT), ("009150", _CNT)]


def test_run_returns_the_stream_exit_code(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _stream(args: Any) -> int:
        return 3

    monkeypatch.setattr(collect_stream_toss, "_run_stream", _stream)
    assert collect_stream_toss.run(argparse.Namespace()) == 3
