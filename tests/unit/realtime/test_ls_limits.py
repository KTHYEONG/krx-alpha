"""Invariant guard tests for the LS session/subscription limit guard."""

from __future__ import annotations

import asyncio
import logging

from src.core.config import (
    LS_MAX_SESSIONS,
    LS_MAX_SUBSCRIPTIONS_PER_SESSION,
    CollectorSettings,
)
from src.realtime import ls_limits
from src.realtime.connection_handover import Handover, SwapRequest
from src.realtime.ls_limits import (
    LsGuardConfig,
    apply_ls_subscription_guard,
    classify_ls_close_reason,
    is_vendor_close_detail,
    ls_limit_status,
    prioritize_ls_pairs,
    split_ls_pairs,
    stream_disconnect_reason,
)

_STREAMS = ("H0STCNT0", "H0STASP0")


def _pairs(symbols: list[str]) -> list[tuple[str, str]]:
    return [(symbol, stream) for symbol in symbols for stream in _STREAMS]


def test_settings_carry_ls_ceilings_with_env_override(monkeypatch) -> None:
    assert LS_MAX_SESSIONS == 3
    assert LS_MAX_SUBSCRIPTIONS_PER_SESSION == 200
    assert CollectorSettings().ls_max_sessions == 3
    assert CollectorSettings().ls_max_subscriptions_per_session == 200
    monkeypatch.setenv("KRX_ALPHA_LS_MAX_SESSIONS", "5")
    monkeypatch.setenv("KRX_ALPHA_LS_MAX_SUBSCRIPTIONS_PER_SESSION", "150")
    assert CollectorSettings().ls_max_sessions == 5
    assert CollectorSettings().ls_max_subscriptions_per_session == 150


def test_under_cap_logs_ok(caplog) -> None:
    pairs = _pairs([f"{i:06d}" for i in range(70)])
    assert ls_limit_status(len(pairs), 200) == "OK"
    with caplog.at_level(logging.INFO):
        kept = apply_ls_subscription_guard(pairs, guard=LsGuardConfig())
    assert kept == pairs
    assert "stage=ls_limits status=OK pairs=140 cap=200 sessions_peak_planned=2 cap_sessions=3" in caplog.text


def test_near_cap_logs_warn(caplog) -> None:
    pairs = _pairs([f"{i:06d}" for i in range(85)])
    assert ls_limit_status(len(pairs), 200) == "WARN"
    with caplog.at_level(logging.INFO):
        kept = apply_ls_subscription_guard(pairs, guard=LsGuardConfig())
    assert kept == pairs
    assert "stage=ls_limits status=WARN pairs=170 cap=200" in caplog.text


def _tiered_universe() -> tuple[list[tuple[str, str]], dict[str, str]]:
    core = [f"10{i:04d}" for i in range(60)]
    events = [f"20{i:04d}" for i in range(30)]
    follow = [f"30{i:04d}" for i in range(20)]
    tiers = dict.fromkeys(core, "core") | dict.fromkeys(events, "event") | dict.fromkeys(follow, "follow")
    return _pairs(follow + events + core), tiers


def test_over_cap_is_loud_and_ordered(caplog) -> None:
    from types import SimpleNamespace

    pairs, tiers = _tiered_universe()
    manifest = SimpleNamespace(note=None)
    persisted: list[bool] = []
    session = SimpleNamespace(manifest=manifest, persist=lambda: persisted.append(True))
    with caplog.at_level(logging.INFO):
        kept = apply_ls_subscription_guard(pairs, session=session, guard=LsGuardConfig(tiers=tiers))
    assert len(kept) == 200
    kept_symbols = [symbol for symbol, _ in kept]
    tiers_in_order = [tiers[s] for s in dict.fromkeys(kept_symbols)]
    assert tiers_in_order == ["core"] * 60 + ["event"] * 30 + ["follow"] * 10
    dropped = [s for s in dict.fromkeys(s for s, _ in pairs) if s not in set(kept_symbols)]
    assert len(dropped) == 10
    assert "stage=ls_limits status=VIOLATION" in caplog.text
    assert "CRITICAL" in caplog.text
    assert set(kept_symbols) | set(dropped) == {s for s, _ in pairs}
    assert manifest.note is not None
    for symbol in dropped:
        assert symbol in manifest.note
    assert persisted == [True]


def test_over_cap_subscribes_two_hundred_in_tier_order(monkeypatch, caplog, tmp_path) -> None:
    import argparse
    import json

    from src.cli import collect_stream
    from src.realtime.adapters import registry
    from src.realtime.contracts import VendorAck, VendorDisconnected
    from src.realtime.manifest import SessionManifest
    from src.universe.ipc import write_candidates

    pairs, tiers = _tiered_universe()
    symbols = list(dict.fromkeys(s for s, _ in pairs))
    candidates = tmp_path / "c.json"
    write_candidates(candidates, [{"symbol": s, "tier": tiers[s]} for s in symbols], rev=20261026)
    market_map = tmp_path / "m.json"
    market_map.write_text(json.dumps(dict.fromkeys(symbols, "KOSPI")), encoding="utf-8")
    subscribed = []

    class _Adapter:
        name = "ls"
        capacity_pairs = 200

        def __init__(self, **kwargs):
            return None

        async def connect(self):
            return None

        async def subscribe(self, pairs):
            subscribed.append(pairs)
            return [VendorAck(s, t, True, "00000") for s, t in pairs]

        async def recv(self):
            raise VendorDisconnected("closed")

        async def aclose(self):
            return None

    class _Http:
        async def close(self):
            return None

    monkeypatch.setenv("LS_APP_KEY", "k")
    monkeypatch.setenv("LS_APP_SECRET", "s")
    monkeypatch.setenv("KRX_ALPHA_VENDOR", "ls")
    monkeypatch.setattr("src.realtime.session.measure_ntp_offset_ns", lambda *a, **k: 0)
    monkeypatch.setattr(registry, "LsRealtimeAdapter", _Adapter)
    monkeypatch.setattr("aiohttp.ClientSession", _Http)
    args = argparse.Namespace(
        session_date="2026-10-26", journal_root=str(tmp_path / "l0"),
        manifest_path=str(tmp_path / "manifest.json"), candidates_path=str(candidates),
        market_map=str(market_map), max_cycles=1,
    )
    with caplog.at_level(logging.INFO):
        assert collect_stream.run(args) == 0
    assert len(subscribed) == 1
    assert len(subscribed[0]) == 200
    kept = list(dict.fromkeys(s for s, _ in subscribed[0]))
    assert [tiers[s] for s in kept] == ["core"] * 60 + ["event"] * 30 + ["follow"] * 10
    manifest = SessionManifest.load(tmp_path / "manifest.json")
    dropped = set(symbols) - set(kept)
    assert len(dropped) == 10
    assert all(s in manifest.note for s in dropped)
    assert "stage=ls_limits status=VIOLATION pairs=220" in caplog.text
    assert "stage=ls_limits status=OK" not in caplog.text


def test_standby_blocked_at_session_cap(caplog) -> None:
    from src.realtime.contracts import VendorAck

    calls: list[bool] = []

    class _Adapter:
        name = "ls"

        async def connect(self) -> None:
            return None

        async def subscribe(self, pairs):
            return [VendorAck(s, t, True, "00000") for s, t in pairs]

        async def recv(self):
            raise AssertionError("standby must never connect at the session cap")

        async def aclose(self) -> None:
            return None

    def _factory():
        calls.append(True)
        return _Adapter()

    request = SwapRequest()
    handover = Handover(
        adapter_factory=_factory, pairs=[("005930", "H0STCNT0")], request=request, max_s=60, cooldown_s=0,
        max_sessions=3,
    )
    request.request()
    with caplog.at_level(logging.WARNING):
        handover.maybe_start(eligible=True, live_connections=3)
    assert calls == []
    assert not handover.active
    assert "stage=stream_handover status=OPEN_SKIPPED reason=session_cap" in caplog.text


def test_standby_allowed_below_cap() -> None:
    from src.realtime.contracts import L0Frame, VendorAck

    calls: list[bool] = []

    class _Adapter:
        name = "ls"

        async def connect(self) -> None:
            return None

        async def subscribe(self, pairs):
            return [VendorAck(s, t, True, "00000") for s, t in pairs]

        async def recv(self) -> L0Frame:
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

        async def aclose(self) -> None:
            return None

    def _factory():
        calls.append(True)
        return _Adapter()

    async def _run() -> list[bool]:
        request = SwapRequest()
        handover = Handover(
            adapter_factory=_factory, pairs=[("005930", "H0STCNT0")], request=request, max_s=60, cooldown_s=0,
            max_sessions=3,
        )
        request.request()
        handover.maybe_start(eligible=True, live_connections=1)
        await asyncio.sleep(0.05)
        await handover.aclose()
        return calls

    assert asyncio.run(_run()) == [True]


def test_unobserved_vendor_text_stays_unknown(monkeypatch) -> None:
    assert classify_ls_close_reason("policy-violation whatever 42") == "unknown"
    assert classify_ls_close_reason("") == "unknown"
    assert classify_ls_close_reason(None) == "unknown"
    assert not is_vendor_close_detail("closed")
    assert not is_vendor_close_detail(None)
    assert is_vendor_close_detail("WSMsgType.CLOSE:code=1000:reason=bye")
    assert is_vendor_close_detail("gone:code=1:reason=x")
    assert stream_disconnect_reason("disconnect", "closed") == "disconnect"
    assert stream_disconnect_reason("watchdog", "WSMsgType.CLOSE:code=1000:reason=bye") == "watchdog"
    assert stream_disconnect_reason("disconnect", "WSMsgType.CLOSE:code=1000:reason=bye") == "unknown"
    monkeypatch.setattr(ls_limits, "LS_LIMIT_OBSERVED_MARKERS", ("policy-violation",))
    assert classify_ls_close_reason("CLOSE:code=1:reason=policy-violation") == "ls_limit"


def test_prioritize_and_split_helpers() -> None:
    pairs = [("b", "H0STCNT0"), ("a", "H0STCNT0"), ("c", "H0STCNT0")]
    assert prioritize_ls_pairs(pairs) == pairs
    assert prioritize_ls_pairs(pairs, tier_of={"a": "core", "b": "follow", "c": "event"}) == [
        ("a", "H0STCNT0"),
        ("c", "H0STCNT0"),
        ("b", "H0STCNT0"),
    ]
    assert prioritize_ls_pairs(pairs, order=("c", "b", "a")) == [
        ("c", "H0STCNT0"),
        ("b", "H0STCNT0"),
        ("a", "H0STCNT0"),
    ]
    assert prioritize_ls_pairs(
        pairs, tier_of={"a": "core", "b": "core", "c": "core"}, order=("c", "b", "a")
    ) == [("c", "H0STCNT0"), ("b", "H0STCNT0"), ("a", "H0STCNT0")]
    kept, dropped = split_ls_pairs(_pairs(["a", "b"]), 4)
    assert len(kept) == 4
    assert dropped == []
    kept, dropped = split_ls_pairs(_pairs(["a", "b"]), 3)
    assert kept == [("a", "H0STCNT0"), ("a", "H0STASP0")]
    assert dropped == ["b"]


def test_guard_without_session_never_blocks(caplog) -> None:
    pairs = _pairs(["005930"])
    with caplog.at_level(logging.INFO):
        assert apply_ls_subscription_guard(pairs) == pairs
    assert "stage=ls_limits status=OK pairs=2 cap=200" in caplog.text


def test_guard_persists_verdict_and_preserves_existing_note(caplog) -> None:
    from types import SimpleNamespace

    session = SimpleNamespace(manifest=SimpleNamespace(note="existing coverage issue"))
    pairs = _pairs(["a", "b"])
    with caplog.at_level(logging.INFO):
        apply_ls_subscription_guard(pairs, session=session, guard=LsGuardConfig(max_subscriptions=2))
        note = session.manifest.note
        apply_ls_subscription_guard(pairs, session=session, guard=LsGuardConfig(max_subscriptions=2))
    assert session.manifest.note == note
    assert note.startswith("existing coverage issue;")
    assert "symbols=b" in note
    assert all(getattr(record, "krx_event", False) for record in caplog.records if record.levelno == logging.INFO)


def test_session_cap_window_does_not_log_on_every_poll(caplog) -> None:
    request = SwapRequest()
    handover = Handover(
        adapter_factory=lambda: None, pairs=[], request=request, max_s=60, cooldown_s=300,
        window=lambda: True, monotonic=lambda: 100.0, max_sessions=1,
    )
    with caplog.at_level(logging.WARNING):
        for _ in range(20):
            handover.maybe_start(eligible=True, live_connections=1)
    assert not handover.active
    assert caplog.text.count("status=OPEN_SKIPPED reason=session_cap") == 1


def test_vendor_close_maps_to_unknown_in_disconnect_log(caplog) -> None:
    import asyncio
    import logging

    from src.realtime.contracts import VendorDisconnected
    from src.realtime.streamer import RealtimeStreamer

    class _Adapter:
        name = "ls"
        capacity_pairs = 200
        sent = 0

        async def connect(self):
            return None

        async def subscribe(self, pairs):
            return []

        async def recv(self):
            raise VendorDisconnected("WSMsgType.CLOSE:code=1009:reason=going-away")

        async def aclose(self):
            return None

    class _Sink:
        def record(self, f):
            return None

        def note_ack(self, v, a):
            return None

        def note_gap(self, symbol, start, end, reason):
            self.reasons = [*getattr(self, "reasons", []), reason]

        def flush(self):
            return 0

    async def _nosleep(_):
        return None

    sink = _Sink()
    streamer = RealtimeStreamer(
        adapter=_Adapter(), sink=sink, replay_pairs=[("005930", "H0STCNT0")],
        wall_ns=lambda: 1_000, rng=lambda: 1.0,
    )
    with caplog.at_level(logging.WARNING):
        asyncio.run(streamer.run_forever(asyncio.Event(), max_cycles=1, sleep=_nosleep))
    assert "stage=stream_disconnect reason=unknown" in caplog.text
    assert getattr(sink, "reasons", []) == ["disconnect"]


def test_actual_ls_subscriptions_count_unique_accepted_acks(caplog) -> None:
    from src.realtime.contracts import VendorAck, VendorDisconnected
    from src.realtime.streamer import RealtimeStreamer

    class _Adapter:
        name = "ls"
        capacity_pairs = 200

        async def connect(self):
            return None

        async def subscribe(self, pairs):
            return [
                VendorAck("005930", "H0STCNT0", True, "00000"),
                VendorAck("005930", "H0STCNT0", True, "00000"),
                VendorAck("005930", "H0STASP0", False, "reject"),
            ]

        async def recv(self):
            raise VendorDisconnected("closed")

        async def aclose(self):
            return None

    class _Sink:
        def note_ack(self, vendor, ack):
            return None

        def flush(self):
            return 0

    streamer = RealtimeStreamer(adapter=_Adapter(), sink=_Sink(), replay_pairs=_pairs(["005930"]))
    with caplog.at_level(logging.INFO):
        assert asyncio.run(streamer.pump(asyncio.Event())) == "disconnect"
    assert "stage=ls_subscriptions status=PARTIAL pairs=2 accepted_pairs=1 rejected_pairs=1" in caplog.text


def test_handover_open_failure_carries_unknown_reason(caplog) -> None:
    from src.realtime.contracts import VendorDisconnected

    class _Failing:
        async def connect(self):
            raise VendorDisconnected("refused")

        async def aclose(self):
            return None

    async def _run() -> None:
        request = SwapRequest()
        handover = Handover(
            adapter_factory=_Failing, pairs=[("005930", "H0STCNT0")], request=request, max_s=60, cooldown_s=0,
        )
        request.request()
        handover.maybe_start(eligible=True)
        await asyncio.sleep(0.05)
        await handover.aclose()

    with caplog.at_level(logging.WARNING):
        asyncio.run(_run())
    assert "stage=stream_handover status=OPEN_FAILED" in caplog.text
    assert "reason=unknown" in caplog.text


def test_manifest_note_roundtrip(tmp_path) -> None:
    import json

    from src.realtime.manifest import SessionManifest
    import datetime as dt

    manifest = SessionManifest(session_date=dt.date(2026, 10, 26), clock_offset_ns=0, started_at_ns=1)
    manifest.note = "ls_limit cap=200 dropped=1 symbols=000660"
    path = tmp_path / "m.json"
    manifest.save(path)
    assert SessionManifest.load(path).note == manifest.note
    payload = json.loads(path.read_text(encoding="utf-8"))
    del payload["note"]
    path.write_text(json.dumps(payload), encoding="utf-8")
    assert SessionManifest.load(path).note is None


def test_ls_adapter_capacity_follows_settings(tmp_path) -> None:
    from src.realtime.adapters.registry import RegularAdapterContext, regular_vendor
    from src.realtime.adapters.registry import AppCredentials

    settings = CollectorSettings(ls_max_subscriptions_per_session=150)
    ctx = RegularAdapterContext(
        http=object(), settings=settings, token_cache_dir=tmp_path, market_of={"005930": "KOSPI"}
    )
    adapter = regular_vendor("ls").build(AppCredentials(app_key="k", app_secret="s"), ctx)
    assert adapter.capacity_pairs == 150


def test_ls_guard_config_from_candidates(tmp_path) -> None:
    from src.cli.collect_stream import ls_guard_config
    from src.universe.ipc import emit_candidates, write_candidates

    settings = CollectorSettings()
    missing = ls_guard_config(settings, tmp_path / "absent.json")
    assert (missing.max_subscriptions, missing.max_sessions, missing.tiers, missing.order) == (200, 3, None, None)
    corrupt = tmp_path / "corrupt.json"
    corrupt.write_text("{not json", encoding="utf-8")
    assert ls_guard_config(settings, corrupt).tiers is None
    tiered = tmp_path / "tiered.json"
    emit_candidates(
        tiered,
        [
            {"symbol": "005930", "selection_reasons": ["core"], "tier": "core"},
            {"symbol": "000660", "selection_reasons": ["surge"], "tier": "event"},
        ],
        rev=20261026,
    )
    cfg = ls_guard_config(settings, tiered)
    assert cfg.tiers == {"005930": "core", "000660": "event"}
    assert cfg.order == ("005930", "000660")
    plain = tmp_path / "plain.json"
    write_candidates(plain, [{"symbol": "005930", "selection_reasons": ["x"]}], rev=20261026)
    cfg = ls_guard_config(settings, plain)
    assert cfg.tiers is None
    assert cfg.order == ("005930",)


def test_streaming_child_skips_guard_for_non_ls_adapter(monkeypatch) -> None:
    import pytest

    from src.core.errors import SlotBudgetExceededError
    from src.realtime import streaming_child
    from types import SimpleNamespace

    class _FakeHttp:
        async def close(self) -> None:
            return None

    monkeypatch.setattr("aiohttp.ClientSession", lambda *a, **k: _FakeHttp())

    def _streamer(**kwargs):
        pytest.fail("guard must not reshape non-LS plans")

    with pytest.raises(SlotBudgetExceededError, match="planned pairs 3 exceed capacity 2"):
        asyncio.run(
            streaming_child.run_streaming_child(
                session=SimpleNamespace(persist=lambda: None),
                adapter_factory=lambda http: SimpleNamespace(name="db", capacity_pairs=2),
                replay_pairs=[("005930", "a"), ("005930", "b"), ("000660", "a")],
                silence_limit=lambda: None,
                max_cycles=1,
                streamer_factory=_streamer,
            )
        )
