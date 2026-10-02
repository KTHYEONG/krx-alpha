"""Invariant guard tests for collect-premarket (part3)."""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import pathlib
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from src.cli import collect_premarket
from src.core.errors import KrxAlphaError, MissingCredentialsError, SlotBudgetExceededError
from src.realtime.kis_sharding import KisDataCredential

_KST = ZoneInfo("Asia/Seoul")


def _stamp(day: dt.date) -> dt.datetime:
    return dt.datetime(day.year, day.month, day.day, 7, 58, tzinfo=_KST)


def _write_pool(path: pathlib.Path, *, day: dt.date, session: str, symbols: tuple[str, ...]) -> None:
    from src.universe.ipc import CandidateSnapshot, write_candidate_snapshot

    st = _stamp(day - dt.timedelta(days=1))
    st = st.replace(tzinfo=_KST)
    eff = dt.datetime(day.year, day.month, day.day, 7, 58, tzinfo=_KST)
    gen = dt.datetime((day - dt.timedelta(days=1)).year, (day - dt.timedelta(days=1)).month, (day - dt.timedelta(days=1)).day, 20, 5, tzinfo=_KST)
    candidates = tuple(
        {
            "symbol": sym,
            "rank": idx + 1,
            "source_ranks": {"trade_amount": idx + 1},
            "metrics": {"trade_value_krw": 10**9 - idx, "change_pct": 1.0},
            "selection_reasons": ["trade_amount"],
        }
        for idx, sym in enumerate(symbols)
    )
    write_candidate_snapshot(
        path,
        CandidateSnapshot(
            schema_version=1,
            rev=int(day.strftime("%Y%m%d")),
            session_date=day,
            session=session,
            generated_at=gen,
            source_asof=gen,
            effective_from=eff,
            policy_version="premarket_v1",
            capacity=len(symbols),
            eligible_count=len(symbols),
            selected_count=len(symbols),
            candidates=candidates,
        ),
    )


def _runtime(*, enabled=True, capacity=41, max_symbols=20, extra_disk=2.0, silence=30.0, grace=60.0):
    collector = SimpleNamespace(
        ntp_host="ntp-host",
        ntp_fallback_hosts=(),
        min_free_disk_gb=3.0,
        journal_retain_days=3,
    )
    premarket = SimpleNamespace(
        enabled=enabled,
        credential_slot="5",
        pair_capacity_per_connection=capacity,
        nxt_streams=("H0NXCNT0", "H0NXASP0"),
        max_symbols=max_symbols,
        close_grace_s=grace,
        extra_free_disk_gb=extra_disk,
        silence_limit_s=silence,
    )
    return SimpleNamespace(collector=collector, premarket=premarket, aftermarket=SimpleNamespace(), snapshot=SimpleNamespace(), paths=SimpleNamespace())


def _args(tmp_path: pathlib.Path, *, day: str, symbols: str, slot="5", key_id="fp5") -> argparse.Namespace:
    return argparse.Namespace(
        session_date=day,
        journal_root=str(tmp_path / "l0"),
        manifest_path=str(tmp_path / "manifest.json"),
        candidates_path=str(tmp_path / "pool.json"),
        credential_slot=slot,
        credential_key_id=key_id,
        symbols=symbols,
        ntp_host="x",
        max_clock_offset_ns=2_000_000_000,
        max_cycles=1,
    )


def test_disabled_premarket_refuses_to_run(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(collect_premarket, "resolve_collector_runtime", lambda **kw: _runtime(enabled=False))
    monkeypatch.setattr(collect_premarket, "KisRealtimeAdapter", lambda **_: (_ for _ in ()).throw(AssertionError("no adapter")))
    args = _args(tmp_path, day="2026-09-16", symbols="005930")
    with pytest.raises(KrxAlphaError):
        asyncio.run(collect_premarket._run_stream(args))
    assert list((tmp_path / "l0").glob("**/*")) == []


def test_fingerprint_mismatch_is_fatal(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(collect_premarket, "resolve_collector_runtime", lambda **kw: _runtime())
    monkeypatch.setattr(collect_premarket, "load_kis_data_credentials", lambda: (KisDataCredential("5", "k", "s", "h", "actual"),))
    monkeypatch.setattr(collect_premarket, "KisRealtimeAdapter", lambda **_: (_ for _ in ()).throw(AssertionError("no adapter")))
    args = _args(tmp_path, day="2026-09-16", symbols="005930", key_id="wrong")
    with pytest.raises(MissingCredentialsError, match="fingerprint"):
        asyncio.run(collect_premarket._run_stream(args))


def test_symbols_must_match_pool_order(monkeypatch, tmp_path) -> None:
    day = dt.date(2026, 9, 16)
    _write_pool(tmp_path / "pool.json", day=day, session="premarket", symbols=("005930", "000660"))
    monkeypatch.setattr(collect_premarket, "resolve_collector_runtime", lambda **kw: _runtime())
    monkeypatch.setattr(collect_premarket, "load_kis_data_credentials", lambda: (KisDataCredential("5", "k", "s", "h", "fp5"),))

    def _boom(cfg):
        raise AssertionError("must not bootstrap on contract violation")

    monkeypatch.setattr(collect_premarket, "bootstrap_session", _boom)
    args = _args(tmp_path, day="2026-09-16", symbols="000660,005930")
    with pytest.raises(KrxAlphaError):
        asyncio.run(collect_premarket._run_stream(args))


def test_wrong_session_pool_is_rejected(monkeypatch, tmp_path) -> None:
    day = dt.date(2026, 9, 16)
    _write_pool(tmp_path / "pool.json", day=day, session="aftermarket", symbols=("005930",))
    monkeypatch.setattr(collect_premarket, "resolve_collector_runtime", lambda **kw: _runtime())
    monkeypatch.setattr(collect_premarket, "load_kis_data_credentials", lambda: (KisDataCredential("5", "k", "s", "h", "fp5"),))
    args = _args(tmp_path, day="2026-09-16", symbols="005930")
    with pytest.raises(KrxAlphaError):
        asyncio.run(collect_premarket._run_stream(args))


def test_capacity_overflow_is_rejected(monkeypatch, tmp_path) -> None:
    day = dt.date(2026, 9, 16)
    _write_pool(tmp_path / "pool.json", day=day, session="premarket", symbols=("005930", "000660"))
    monkeypatch.setattr(collect_premarket, "resolve_collector_runtime", lambda **kw: _runtime(capacity=2))
    monkeypatch.setattr(collect_premarket, "load_kis_data_credentials", lambda: (KisDataCredential("5", "k", "s", "h", "fp5"),))
    args = _args(tmp_path, day="2026-09-16", symbols="005930,000660")
    with pytest.raises(SlotBudgetExceededError):
        asyncio.run(collect_premarket._run_stream(args))


def _run_with_real_bootstrap(monkeypatch, tmp_path, *, day: dt.date, symbols: tuple[str, ...], sink_fn=None):
    import time

    import src.realtime.session as session_mod

    _write_pool(tmp_path / "pool.json", day=day, session="premarket", symbols=symbols)
    monkeypatch.setattr(collect_premarket, "resolve_collector_runtime", lambda **kw: _runtime())
    monkeypatch.setattr(collect_premarket, "load_kis_data_credentials", lambda: (KisDataCredential("5", "k", "s", "h", "fp5"),))
    monkeypatch.setattr(session_mod, "measure_ntp_offset_ns", lambda *a, **kw: 0)
    # Keep the run inside the window regardless of wall-clock date.
    import src.core.session_anchors as anchors_mod

    real_resolve = anchors_mod.resolve_session_anchors
    anchors = real_resolve(tmp_path / "cal", day)
    close_at = dt.datetime.combine(day, anchors.shift_pre_open(__import__("src.core.session_anchors", fromlist=["STANDARD_NXT_PREMARKET_END"]).STANDARD_NXT_PREMARKET_END), tzinfo=_KST)
    deadline_ns = int(close_at.timestamp() * 1_000_000_000) + 60_000_000_000
    monkeypatch.setattr(time, "time_ns", lambda: deadline_ns - 3_600_000_000_000)

    captured: dict = {}

    class FakeAdapter:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    class FakeStreamer:
        def __init__(self, **kwargs):
            captured["replay_pairs"] = kwargs["replay_pairs"]
            captured["silence_limit"] = kwargs["silence_limit"]
            self._sink = kwargs["sink"]

        async def run_forever(self, *a, **kw):
            if sink_fn is not None:
                sink_fn(self._sink)
            return None

    monkeypatch.setattr(collect_premarket, "KisRealtimeAdapter", FakeAdapter)
    monkeypatch.setattr(collect_premarket, "RealtimeStreamer", FakeStreamer)
    args = _args(tmp_path, day=day.isoformat(), symbols=",".join(symbols))
    rc = asyncio.run(collect_premarket._run_stream(args))
    return rc, captured


def test_route_and_partition_are_premarket(monkeypatch, tmp_path) -> None:
    import time

    from src.realtime.contracts import L0Frame, MarketSession, MarketVenue

    day = dt.date(2026, 9, 16)

    def _write_two(sink) -> None:
        now_ns = time.time_ns()
        for stream in ("H0NXCNT0", "H0NXASP0"):
            sink.record(
                L0Frame(
                    vendor="kis",
                    stream=stream,
                    symbol="005930",
                    raw="005930^080000^100",
                    recv_mono_ns=now_ns,
                    recv_wall_ns=now_ns,
                    conn_seq=1,
                    conn_id="c",
                    venue=MarketVenue.NXT,
                    session=MarketSession.NXT_PRE,
                    exchange_event_time="080000",
                )
            )
        sink.flush()

    rc, captured = _run_with_real_bootstrap(monkeypatch, tmp_path, day=day, symbols=("005930",), sink_fn=_write_two)
    assert rc == 0
    assert captured["route"].venue.value == "nxt"
    assert captured["route"].session.value == "nxt_pre"
    assert captured["capacity_pairs"] == 41
    files = list((tmp_path / "l0").rglob("*.jsonl.zst"))
    assert files, "expected journal files"
    for path in files:
        rel = path.relative_to(tmp_path / "l0").as_posix()
        assert "kis/nxt/nxt_pre/" in rel
        assert path.name.endswith(".s0.jsonl.zst")
    assert {path.parent.parent.name for path in files} <= {"H0NXCNT0", "H0NXASP0"}


def test_manifest_closure_is_recorded(monkeypatch, tmp_path) -> None:
    import json

    day = dt.date(2026, 9, 16)
    rc, _ = _run_with_real_bootstrap(monkeypatch, tmp_path, day=day, symbols=("005930",))
    assert rc == 0
    data = json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8"))
    assert data["venue"] == "nxt"
    assert data["session"] == "nxt_pre"
    assert data["shard_index"] == 0
    anchors_day = day
    from src.core.session_anchors import STANDARD_NXT_PREMARKET_END, standard_session_anchors

    anchors = standard_session_anchors(anchors_day)
    close_at = dt.datetime.combine(day, anchors.shift_pre_open(STANDARD_NXT_PREMARKET_END), tzinfo=_KST)
    assert data["expected_close_ns"] == int(close_at.timestamp() * 1_000_000_000)
    assert data["writer_closed_at_ns"] is not None


def test_late_start_exits_without_socket(monkeypatch, tmp_path) -> None:
    import time

    from src.realtime.manifest import SessionManifest

    day = dt.date(2026, 9, 16)
    _write_pool(tmp_path / "pool.json", day=day, session="premarket", symbols=("005930",))
    monkeypatch.setattr(collect_premarket, "resolve_collector_runtime", lambda **kw: _runtime(grace=60.0))
    monkeypatch.setattr(collect_premarket, "load_kis_data_credentials", lambda: (KisDataCredential("5", "k", "s", "h", "fp5"),))
    close_at = dt.datetime(2026, 9, 16, 8, 50, tzinfo=_KST)
    expected_close_ns = int(close_at.timestamp() * 1_000_000_000)
    manifest = SessionManifest(
        session_date=day, clock_offset_ns=0, started_at_ns=expected_close_ns - 10**9,
        venue="nxt", session="nxt_pre", expected_close_ns=expected_close_ns,
    )
    fake_session = SimpleNamespace(manifest=manifest, replay_pairs=lambda: [], persist=lambda: None)
    monkeypatch.setattr(collect_premarket, "bootstrap_session", lambda cfg: fake_session)
    monkeypatch.setattr(time, "time_ns", lambda: expected_close_ns + 61_000_000_000)
    monkeypatch.setattr(collect_premarket, "KisRealtimeAdapter", lambda **_: (_ for _ in ()).throw(AssertionError("no socket")))
    args = _args(tmp_path, day="2026-09-16", symbols="005930")
    assert asyncio.run(collect_premarket._run_stream(args)) == 0
    assert manifest.writer_closed_at_ns is None


def test_disk_watermark_uses_premarket_floor(monkeypatch, tmp_path) -> None:
    import src.realtime.session as session_mod
    from src.storage.retention import StorageExhaustedError

    day = dt.date(2026, 9, 16)
    captured: dict = {}
    real_bootstrap = session_mod.bootstrap_session

    def _capture(cfg):
        captured["min_free_disk_gb"] = cfg.min_free_disk_gb
        return real_bootstrap(cfg)

    _write_pool(tmp_path / "pool.json", day=day, session="premarket", symbols=("005930",))
    monkeypatch.setattr(collect_premarket, "resolve_collector_runtime", lambda **kw: _runtime(extra_disk=2.0))
    monkeypatch.setattr(collect_premarket, "load_kis_data_credentials", lambda: (KisDataCredential("5", "k", "s", "h", "fp5"),))
    monkeypatch.setattr(session_mod, "measure_ntp_offset_ns", lambda *a, **kw: 0)
    monkeypatch.setattr(collect_premarket, "bootstrap_session", _capture)
    monkeypatch.setattr(session_mod, "check_disk_watermark", lambda *a, **kw: False)

    class FakeAdapter:
        def __init__(self, **kwargs):
            return None

    class FakeStreamer:
        def __init__(self, **kwargs):
            self._sink = kwargs["sink"]

        async def run_forever(self, *a, **kw):
            import time

            from src.realtime.contracts import L0Frame, MarketSession, MarketVenue

            now_ns = time.time_ns()
            with pytest.raises(StorageExhaustedError):
                self._sink.record(
                    L0Frame(
                        vendor="kis", stream="H0NXCNT0", symbol="005930", raw="r",
                        recv_mono_ns=now_ns, recv_wall_ns=now_ns, conn_seq=1, conn_id="c",
                        venue=MarketVenue.NXT, session=MarketSession.NXT_PRE, exchange_event_time="080000",
                    )
                )
            raise StorageExhaustedError("free disk below watermark")

    monkeypatch.setattr(collect_premarket, "KisRealtimeAdapter", FakeAdapter)
    monkeypatch.setattr(collect_premarket, "RealtimeStreamer", FakeStreamer)
    # Freeze time inside the window so the deadline path does not trigger.
    import time as _time

    close_at = dt.datetime(2026, 9, 16, 8, 50, tzinfo=_KST)
    monkeypatch.setattr(_time, "time_ns", lambda: int(close_at.timestamp() * 1_000_000_000) - 3_600_000_000_000)
    args = _args(tmp_path, day="2026-09-16", symbols="005930")
    with pytest.raises(StorageExhaustedError):
        asyncio.run(collect_premarket._run_stream(args))
    assert captured["min_free_disk_gb"] == pytest.approx(5.0)


def test_clock_gate_is_fail_closed(monkeypatch, tmp_path) -> None:
    import src.realtime.session as session_mod
    from src.realtime.clock import ClockUnsyncedError

    day = dt.date(2026, 9, 16)
    _write_pool(tmp_path / "pool.json", day=day, session="premarket", symbols=("005930",))
    monkeypatch.setattr(collect_premarket, "resolve_collector_runtime", lambda **kw: _runtime())
    monkeypatch.setattr(collect_premarket, "load_kis_data_credentials", lambda: (KisDataCredential("5", "k", "s", "h", "fp5"),))
    monkeypatch.setattr(session_mod, "measure_ntp_offset_ns", lambda *a, **kw: 5_000_000_000)
    args = argparse.Namespace(
        session_date="2026-09-16",
        journal_root=str(tmp_path / "l0"),
        manifest_path=str(tmp_path / "manifest.json"),
        candidates_path=str(tmp_path / "pool.json"),
        credential_slot="5",
        credential_key_id="fp5",
        symbols="005930",
        ntp_host="x",
        max_clock_offset_ns=1_000_000_000,
        max_cycles=1,
    )
    with pytest.raises(ClockUnsyncedError):
        asyncio.run(collect_premarket._run_stream(args))
    assert list((tmp_path / "l0").glob("**/*.jsonl.zst")) == []


def test_no_secrets_in_logs(monkeypatch, tmp_path, caplog) -> None:
    day = dt.date(2026, 9, 16)
    app_key, app_secret, hts_id = "APPKEY_SECRET_X1", "APPSECRET_SECRET_X2", "HTSID_SECRET_X3"
    _write_pool(tmp_path / "pool.json", day=day, session="premarket", symbols=("005930",))
    monkeypatch.setattr(collect_premarket, "resolve_collector_runtime", lambda **kw: _runtime())
    monkeypatch.setattr(
        collect_premarket,
        "load_kis_data_credentials",
        lambda: (KisDataCredential("5", app_key, app_secret, hts_id, "fp5"),),
    )

    class FakeSession:
        def __init__(self):
            from src.realtime.manifest import SessionManifest

            close_at = dt.datetime(2026, 9, 16, 8, 50, tzinfo=_KST)
            self.manifest = SessionManifest(
                session_date=day, clock_offset_ns=0, started_at_ns=0,
                venue="nxt", session="nxt_pre",
                expected_close_ns=int(close_at.timestamp() * 1_000_000_000) + 10**12,
            )

        def replay_pairs(self):
            return [("005930", "H0NXCNT0")]

        def persist(self):
            return None

    monkeypatch.setattr(collect_premarket, "bootstrap_session", lambda cfg: FakeSession())
    monkeypatch.setattr(collect_premarket, "KisRealtimeAdapter", lambda **kwargs: SimpleNamespace(acloses=None))
    import logging

    class FakeStreamer:
        def __init__(self, **kwargs):
            return None

        async def run_forever(self, *a, **kw):
            return None

    monkeypatch.setattr(collect_premarket, "RealtimeStreamer", FakeStreamer)
    args = _args(tmp_path, day="2026-09-16", symbols="005930")
    with caplog.at_level(logging.INFO, logger="src.cli.collect_premarket"):
        assert asyncio.run(collect_premarket._run_stream(args)) == 0
    text = caplog.text
    assert app_key not in text
    assert app_secret not in text
    assert hts_id not in text
    assert "fp5" in text


def test_registration_reads_no_environment(monkeypatch) -> None:
    import os

    monkeypatch.delenv("KRX_ALPHA_PREMARKET_ENABLED", raising=False)
    monkeypatch.delenv("KRX_ALPHA_PREMARKET_CREDENTIAL_SLOT", raising=False)
    accessed: list[str] = []
    real_getenv = os.getenv
    monkeypatch.setattr(os, "getenv", lambda *a, **kw: (accessed.append(a[0]), real_getenv(*a, **kw))[1])
    from src.cli.main import build_parser

    parser = build_parser()
    args = parser.parse_args([
        "collect-premarket", "--session-date", "2026-09-16", "--journal-root", "l0",
        "--manifest-path", "m.json", "--candidates-path", "c.json",
        "--credential-slot", "5", "--credential-key-id", "fp5", "--symbols", "005930",
    ])
    assert args.command == "collect-premarket" if hasattr(args, "command") else True
    assert (args.credential_slot, args.symbols) == ("5", "005930")
    assert not any(name.startswith("KRX_ALPHA") for name in accessed)


def test_empty_symbols_rejected_before_bootstrap(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(collect_premarket, "resolve_collector_runtime", lambda **kw: _runtime())
    monkeypatch.setattr(collect_premarket, "load_kis_data_credentials", lambda: (KisDataCredential("5", "k", "s", "h", "fp5"),))
    monkeypatch.setattr(collect_premarket, "bootstrap_session", lambda cfg: (_ for _ in ()).throw(AssertionError("no bootstrap")))
    args = _args(tmp_path, day="2026-09-16", symbols="")
    with pytest.raises(KrxAlphaError, match="non-empty"):
        asyncio.run(collect_premarket._run_stream(args))


def test_symbols_outside_pool_rejected(monkeypatch, tmp_path) -> None:
    day = dt.date(2026, 9, 16)
    _write_pool(tmp_path / "pool.json", day=day, session="premarket", symbols=("005930",))
    monkeypatch.setattr(collect_premarket, "resolve_collector_runtime", lambda **kw: _runtime())
    monkeypatch.setattr(collect_premarket, "load_kis_data_credentials", lambda: (KisDataCredential("5", "k", "s", "h", "fp5"),))
    monkeypatch.setattr(collect_premarket, "bootstrap_session", lambda cfg: (_ for _ in ()).throw(AssertionError("no bootstrap")))
    args = _args(tmp_path, day="2026-09-16", symbols="999999")
    with pytest.raises(MissingCredentialsError, match="subset"):
        asyncio.run(collect_premarket._run_stream(args))


def test_missing_pair_capacity_rejected(monkeypatch, tmp_path) -> None:
    day = dt.date(2026, 9, 16)
    _write_pool(tmp_path / "pool.json", day=day, session="premarket", symbols=("005930",))
    monkeypatch.setattr(collect_premarket, "resolve_collector_runtime", lambda **kw: _runtime(capacity=None))
    monkeypatch.setattr(collect_premarket, "load_kis_data_credentials", lambda: (KisDataCredential("5", "k", "s", "h", "fp5"),))
    args = _args(tmp_path, day="2026-09-16", symbols="005930")
    with pytest.raises(SlotBudgetExceededError, match="capacity"):
        asyncio.run(collect_premarket._run_stream(args))


def test_self_bounded_deadline_stops_without_sigterm(monkeypatch, tmp_path) -> None:
    import time

    from src.realtime.manifest import SessionManifest

    day = dt.date(2026, 9, 16)
    _write_pool(tmp_path / "pool.json", day=day, session="premarket", symbols=("005930",))
    monkeypatch.setattr(collect_premarket, "resolve_collector_runtime", lambda **kw: _runtime(grace=0.05))
    monkeypatch.setattr(collect_premarket, "load_kis_data_credentials", lambda: (KisDataCredential("5", "k", "s", "h", "fp5"),))
    expected_close_ns = time.time_ns() + 50_000_000
    manifest = SessionManifest(
        session_date=day, clock_offset_ns=0, started_at_ns=expected_close_ns,
        venue="nxt", session="nxt_pre", expected_close_ns=expected_close_ns,
    )
    seen: dict = {}
    fake_session = SimpleNamespace(manifest=manifest, replay_pairs=lambda: [("005930", "H0NXCNT0")], persist=lambda: seen.update(calls=seen.get("calls", 0) + 1))
    monkeypatch.setattr(collect_premarket, "bootstrap_session", lambda cfg: fake_session)

    class FakeAdapter:
        def __init__(self, **kwargs):
            return None

    class FakeStreamer:
        def __init__(self, **kwargs):
            return None

        async def run_forever(self, stop, **kw):
            await stop.wait()
            return None

    monkeypatch.setattr(collect_premarket, "KisRealtimeAdapter", FakeAdapter)
    monkeypatch.setattr(collect_premarket, "RealtimeStreamer", FakeStreamer)
    args = _args(tmp_path, day="2026-09-16", symbols="005930")
    assert asyncio.run(collect_premarket._run_stream(args)) == 0
    assert manifest.writer_closed_at_ns is not None
