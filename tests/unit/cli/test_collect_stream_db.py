"""DB warm-standby streamer CLI invariant tests: arguments, static pairs, dynamic plan, leases, paths."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import datetime as dt
import logging
import pathlib
import types

import pytest

from src.cli import collect_stream
from src.cli import collect_stream_db
from src.core.config import CollectorSettings
from src.core.errors import KrxAlphaError
from src.core.paths import DataPaths
from src.realtime.contracts import VendorAck
from src.realtime.feed_health import feed_id_for
from src.realtime.session import DynamicFeed, DynamicPairWatcher
from src.realtime.ws_lease import LeaseRole, LeaseUnavailable, WsLeaseLedger
from src.universe.intraday import DynamicEntry, DynamicPlan, read_dynamic_admissions, write_dynamic_admissions

_CNT = "H0STCNT0"
_ASP = "H0STASP0"
DAY = dt.date(2026, 9, 8)


def test_collect_stream_db_subcommand_takes_warm_standby_arguments() -> None:
    from src.cli.main import build_parser

    args = build_parser().parse_args([
        "collect-stream-db", "--session-date", "2026-09-08", "--journal-root", "l0",
        "--manifest-path", "s.db.1.json", "--candidates-path", "c.json",
        "--dynamic-plan-path", "dyn.json", "--db-session-index", "1", "--hot-symbols", "10",
    ])
    assert args.command == "collect-stream-db"
    assert callable(args.handler)
    assert args.db_session_index == 1
    assert args.hot_symbols == 10
    assert not hasattr(args, "market_map")


def test_collect_stream_db_rejects_hot_symbols_beyond_pair_capacity(tmp_path) -> None:
    settings = CollectorSettings()
    too_many = settings.db_capacity_pairs // len(settings.streams) + 1
    args = argparse.Namespace(
        session_date="2026-09-08", journal_root=str(tmp_path / "l0"), manifest_path=str(tmp_path / "s.json"),
        candidates_path=str(tmp_path / "c.json"), dynamic_plan_path=str(tmp_path / "d.json"),
        db_session_index=0, hot_symbols=too_many, ntp_host=None, max_clock_offset_ns=None,
        max_cycles=1, degraded_reason=None,
    )
    with pytest.raises(KrxAlphaError):
        asyncio.run(collect_stream_db._run_stream(args))


def test_hot_symbols_are_top_priority_candidates_times_streams() -> None:
    replay = [("005930", _CNT), ("005930", _ASP), ("000660", _CNT), ("000660", _ASP), ("035420", _CNT)]
    assert collect_stream._warm_static_pairs(replay, (_CNT, _ASP), 2) == [
        ("005930", _CNT), ("005930", _ASP), ("000660", _CNT), ("000660", _ASP),
    ]
    assert collect_stream._warm_static_pairs(replay, (_CNT, _ASP), 0) == []


class _Session:
    def note_ack(self, **kwargs) -> None:
        return None


class _Live:
    """Adapter fake whose subscribe_live accepts every requested pair."""

    def __init__(self) -> None:
        self.calls: list[list[tuple[str, str]]] = []
        self.subscribed: list[tuple[str, str]] = []

    def subscribed_pairs(self) -> tuple[tuple[str, str], ...]:
        return tuple(self.subscribed)

    async def subscribe_live(self, pairs):
        self.calls.append(list(pairs))
        self.subscribed.extend(pairs)
        return [VendorAck(symbol, stream, True, "00000") for symbol, stream in pairs]


def _publish(plan_path: pathlib.Path, *entries: tuple[str, str]) -> None:
    rev = read_dynamic_admissions(plan_path).rev + 1 if plan_path.exists() else 1
    write_dynamic_admissions(
        plan_path, DynamicPlan(rev=rev, entries=tuple(DynamicEntry(s, f, 1) for s, f in entries))
    )


def test_warm_child_starts_idle_and_subscribes_admitted_pairs_from_plan(tmp_path) -> None:
    settings = CollectorSettings(data_root=tmp_path / "data")
    plan_path = tmp_path / "dynamic.json"
    watch = collect_stream._warm_dynamic_watch(
        session=_Session(),  # type: ignore[arg-type]
        session_date=DAY,
        settings=settings,
        vendor="db",
        session_index=0,
        plan_path=plan_path,
        static_pairs=[],
    )
    live = _Live()
    assert watch.pairs() == []
    assert asyncio.run(watch.tick(live)) == 0  # type: ignore[arg-type]
    assert live.calls == []

    _publish(plan_path, ("005930", "db:0"), ("000660", "db:1"))
    assert asyncio.run(watch.tick(live)) == 2  # type: ignore[arg-type]
    assert live.calls == [[("005930", _CNT), ("005930", _ASP)]]
    assert watch.pairs() == [("005930", _CNT), ("005930", _ASP)]


def test_warm_watch_feed_identity_is_db_session_feed() -> None:
    assert feed_id_for("db", "krx", 0) == "db:0"
    assert feed_id_for("db", "krx", 1) == "db:1"


def test_db_session_lease_denies_third_connection(tmp_path) -> None:
    ledger = WsLeaseLedger(tmp_path, vendor="db", key_id="abc123abc123", max_connections=2, ttl_s=30.0)
    first = ledger.acquire("db-shard-0", LeaseRole.PRIMARY)
    second = ledger.acquire("db-shard-1", LeaseRole.PRIMARY)
    assert first is not None
    assert second is not None
    with pytest.raises(LeaseUnavailable):
        asyncio.run(collect_stream._claim_primary(ledger, owner="db-shard-2", shard_index=2, ttl_s=30.0, strict=True))


def test_db_session_zero_is_strict_when_lease_full(tmp_path) -> None:
    ledger = WsLeaseLedger(tmp_path, vendor="db", key_id="abc123abc123", max_connections=1, ttl_s=30.0)
    ledger.acquire("db-shard-1", LeaseRole.PRIMARY)
    with pytest.raises(LeaseUnavailable):
        asyncio.run(collect_stream._claim_primary(ledger, owner="db-shard-0", shard_index=0, ttl_s=0.0, strict=True))


def test_db_session_index_paths_and_command_wiring(tmp_path) -> None:
    from src.orchestration import daemon as daemon_mod

    paths = DataPaths(tmp_path)
    assert paths.db_crosscheck_manifest_path(DAY) == paths.manifest_dir / "2026-09-08.db.json"
    assert paths.db_crosscheck_manifest_path(DAY, 1) == paths.manifest_dir / "2026-09-08.db.1.json"
    assert paths.db_crosscheck_manifest_path(DAY) != paths.manifest_path(DAY)
    with pytest.raises(ValueError, match="session_index"):
        paths.db_crosscheck_manifest_path(DAY, 2)

    cmd = daemon_mod._db_crosscheck_cmd(DAY, paths, session_index=1, hot_symbols=5)
    assert cmd[cmd.index("--db-session-index") + 1] == "1"
    assert cmd[cmd.index("--hot-symbols") + 1] == "5"
    assert cmd[cmd.index("--manifest-path") + 1] == str(paths.manifest_dir / "2026-09-08.db.1.json")
    assert cmd[cmd.index("--dynamic-plan-path") + 1] == str(paths.dynamic_admissions(DAY))


def test_db_crosscheck_flag_is_gone_from_source_and_docs() -> None:
    root = pathlib.Path(__file__).resolve().parents[3]
    needle = "db_crosscheck" + "_enabled"
    hits = []
    for base in ("src", "tests", "docs/flags.toml"):
        target = root / base
        files = [target] if target.is_file() else [p for p in target.rglob("*") if p.is_file()]
        for path in files:
            scanned = path.suffix in {".py", ".toml", ".md"} and path.name != pathlib.Path(__file__).name
            if scanned and needle in path.read_text(encoding="utf-8", errors="ignore"):
                hits.append(str(path))
    assert hits == []


def test_dynamic_feed_for_warm_child_is_db_vendor(tmp_path) -> None:
    watch = collect_stream._warm_dynamic_watch(
        session=_Session(),  # type: ignore[arg-type]
        session_date=DAY,
        settings=CollectorSettings(data_root=tmp_path / "data"),
        vendor="db",
        session_index=1,
        plan_path=tmp_path / "dynamic.json",
        static_pairs=[],
    )
    assert isinstance(watch, DynamicPairWatcher)
    assert watch._feed == DynamicFeed(  # noqa: SLF001 - identity of the admissions this child consumes
        vendor="db",
        feed="db:1",
        streams=(_CNT, _ASP),
        ipc_path=tmp_path / "dynamic.json",
        membership_path=DataPaths(tmp_path / "data").membership_path(DAY),
    )


def test_run_stream_hands_a_warm_profile_to_the_regular_stream(tmp_path, monkeypatch) -> None:
    seen: dict[str, object] = {}

    async def _run_regular(args, settings, profile, **kwargs) -> int:
        seen["profile"] = profile
        return 0

    monkeypatch.setattr(collect_stream, "run_regular_stream", _run_regular)
    args = argparse.Namespace(db_session_index=1, hot_symbols=0)
    assert asyncio.run(collect_stream_db._run_stream(args)) == 0
    profile = seen["profile"]
    assert profile.warm_standby
    assert profile.session_index == 1
    assert profile.hot_symbols == 0
    assert profile.symbol_limit == 1


@pytest.mark.parametrize("index", [0, 1])
def test_db_child_start_log_has_session_index(monkeypatch, caplog, index) -> None:
    async def _run_regular(args, settings, profile, **kwargs) -> int:
        return 0

    monkeypatch.setattr(collect_stream, "run_regular_stream", _run_regular)
    args = argparse.Namespace(db_session_index=index, hot_symbols=0)
    with caplog.at_level(logging.INFO):
        asyncio.run(collect_stream_db._run_stream(args))
    starts = [r.getMessage() for r in caplog.records if "stage=db_child status=START" in r.getMessage()]
    assert len(starts) == 1
    assert f"session={index}" in starts[0]


class _FakeLedger:
    def __init__(self, renewals: list[object]) -> None:
        self._renewals = renewals

    def renew(self, lease):
        outcome = self._renewals.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class _Lease:
    owner = "db-shard-0"


def test_lease_guard_returns_true_when_stream_ends_first() -> None:
    async def scenario() -> bool:
        async def stream() -> None:
            await asyncio.sleep(0)

        return await collect_stream._run_lease_guarded(stream(), _FakeLedger([]), _Lease(), every_s=5.0)  # type: ignore[arg-type]

    assert asyncio.run(scenario()) is True


@pytest.mark.parametrize("outcome", [False, LeaseUnavailable("ledger unreadable")])
def test_lease_guard_cancels_stream_when_lease_is_lost(outcome) -> None:
    cancelled: list[bool] = []

    async def scenario() -> bool:
        async def stream() -> None:
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                cancelled.append(True)
                raise

        return await collect_stream._run_lease_guarded(
            stream(), _FakeLedger([outcome]), _Lease(), every_s=0.01  # type: ignore[arg-type]
        )

    assert asyncio.run(scenario()) is False
    assert cancelled == [True]


class _WarmSession:
    def __init__(self, replay: list[tuple[str, str]]) -> None:
        self._replay = replay
        self.manifest = types.SimpleNamespace(subscription_acks=[], gaps=[], boots=[])

    def replay_pairs(self) -> list[tuple[str, str]]:
        return self._replay

    def note_ack(self, **kwargs) -> None:
        return None


class _Creds:
    app_key = "db-app-key"


class _Entry:
    requires_market_map = False

    def load_credentials(self) -> _Creds:
        return _Creds()


def _warm_args(tmp_path, *, hot: int) -> argparse.Namespace:
    return argparse.Namespace(
        session_date="2026-09-08", journal_root=str(tmp_path / "l0"), manifest_path=str(tmp_path / "s.db.1.json"),
        candidates_path=str(tmp_path / "c.json"), dynamic_plan_path=str(tmp_path / "dyn.json"),
        db_session_index=1, hot_symbols=hot, ntp_host=None, max_clock_offset_ns=None, max_cycles=1,
        degraded_reason=None,
    )


def _patch_warm_runtime(monkeypatch, tmp_path, session, seen: dict, *, child_sleep_s: float = 0.0):
    class _Tokens:
        token_cache_dir = tmp_path / "tokens"

    def _child(**kwargs):
        seen.update(kwargs)

        async def _run() -> None:
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.sleep(child_sleep_s)

        return _run()

    monkeypatch.setattr(collect_stream, "regular_vendor", lambda vendor: _Entry())
    monkeypatch.setattr(collect_stream, "KisTokenSettings", _Tokens)
    monkeypatch.setattr(collect_stream, "run_streaming_child", _child)
    monkeypatch.setattr(collect_stream, "build_feed_health_tracker", lambda **kwargs: None)
    return lambda cfg: session


def _profile(hot: int) -> collect_stream.RegularStreamProfile:
    from src.realtime.contracts import MarketSession, MarketVenue
    from src.realtime.session import StreamRoute

    return collect_stream.RegularStreamProfile(
        vendor="db", slot_budget=50, symbol_limit=max(hot, 1), route=StreamRoute(MarketVenue.KRX, MarketSession.REGULAR),
        shutdown_log="[DATA] stage=stream_shutdown vendor=db manifest=%s acks=%d gaps=%d boots=%d",
        session_index=1, warm_standby=True, hot_symbols=hot,
    )


def test_warm_child_arms_silence_watchdog_only_after_a_pair_is_subscribed(tmp_path, monkeypatch) -> None:
    seen: dict = {}
    replay = [("005930", _CNT), ("005930", _ASP), ("000660", _CNT), ("000660", _ASP)]
    bootstrap = _patch_warm_runtime(monkeypatch, tmp_path, _WarmSession(replay), seen)
    settings = CollectorSettings(data_root=tmp_path / "data")
    code = asyncio.run(
        collect_stream.run_regular_stream(
            _warm_args(tmp_path, hot=0), settings, _profile(0), bootstrap=bootstrap, streamer_factory=None  # type: ignore[arg-type]
        )
    )
    assert code == 0
    assert seen["replay_pairs"] == []
    assert seen["silence_limit"]() is None

    seen.clear()
    bootstrap = _patch_warm_runtime(monkeypatch, tmp_path, _WarmSession(replay), seen)
    asyncio.run(
        collect_stream.run_regular_stream(
            _warm_args(tmp_path, hot=1), settings, _profile(1), bootstrap=bootstrap, streamer_factory=None  # type: ignore[arg-type]
        )
    )
    assert seen["replay_pairs"] == [("005930", _CNT), ("005930", _ASP)]
    assert collect_stream._armed_when_subscribed(lambda: 5.0, lambda: True)() == 5.0
    assert collect_stream._armed_when_subscribed(lambda: 5.0, lambda: False)() is None


def test_warm_child_exits_with_shard_denied_code_when_lease_is_lost(tmp_path, monkeypatch) -> None:
    seen: dict = {}
    bootstrap = _patch_warm_runtime(monkeypatch, tmp_path, _WarmSession([]), seen, child_sleep_s=60.0)
    monkeypatch.setattr(WsLeaseLedger, "renew", lambda self, lease: False)
    settings = CollectorSettings(data_root=tmp_path / "data", ws_lease_ttl_s=0.03)
    code = asyncio.run(
        collect_stream.run_regular_stream(
            _warm_args(tmp_path, hot=0), settings, _profile(0), bootstrap=bootstrap, streamer_factory=None  # type: ignore[arg-type]
        )
    )
    assert code == collect_stream.SHARD_DENIED_EXIT
