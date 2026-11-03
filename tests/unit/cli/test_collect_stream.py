import pytest


def test_collect_stream_subcommand_registered() -> None:
    from src.cli.main import build_parser

    parser = build_parser()
    args = parser.parse_args([
        'collect-stream', '--session-date', '2026-09-08', '--journal-root', 'l0',
        '--manifest-path', 's.json', '--candidates-path', 'c.json', '--market-map', 'm.json',
    ])

    assert args.command == 'collect-stream'
    assert callable(args.handler)
def test_collect_stream_run_drives_streamer_one_cycle(tmp_path, monkeypatch) -> None:
    import argparse
    import json
    from src.cli import collect_stream
    from src.realtime.adapters import registry
    from src.universe.ipc import write_candidates
    from src.realtime.contracts import VendorAck, VendorDisconnected

    monkeypatch.setenv('LS_APP_KEY', 'k')
    monkeypatch.setenv('LS_APP_SECRET', 's')
    monkeypatch.setattr('src.realtime.session.measure_ntp_offset_ns', lambda *a, **k: 0)

    cp = tmp_path / 'c.json'
    write_candidates(cp, [{'symbol': '005930', 'selection_reasons': ['limit_up']}], rev=1)
    mm = tmp_path / 'm.json'
    mm.write_text(json.dumps({'005930': 'KOSPI'}), encoding='utf-8')

    class _FakeAdapter:
        name = 'ls'
        capacity_pairs = 200
        def __init__(self, **kw):
            self.kw = kw
        async def connect(self):
            self.connected = True
        async def subscribe(self, pairs):
            return [VendorAck(s, t, True, '00000') for s, t in pairs]
        async def recv(self):
            raise VendorDisconnected('closed')
        async def aclose(self):
            self.closed = True

    class _FakeHttp:
        async def close(self):
            self.closed = True

    monkeypatch.setattr(registry, "LsRealtimeAdapter", _FakeAdapter)
    monkeypatch.setattr('aiohttp.ClientSession', lambda *a, **k: _FakeHttp())

    args = argparse.Namespace(
        session_date='2026-09-08', journal_root=str(tmp_path / 'l0'),
        manifest_path=str(tmp_path / 's.json'), candidates_path=str(cp),
        market_map=str(mm), ntp_host='h', max_clock_offset_ns=2_000_000_000, max_cycles=1,
    )

    rc = collect_stream.run(args)

    assert rc == 0
    assert (tmp_path / 's.json').exists()

def test_collect_stream_installs_sigterm_handler(tmp_path, monkeypatch) -> None:
    import argparse
    import asyncio
    import json
    import signal
    from src.cli import collect_stream
    from src.realtime.adapters import registry
    from src.universe.ipc import write_candidates
    from src.realtime.contracts import VendorAck, VendorDisconnected

    monkeypatch.setenv('LS_APP_KEY', 'k')
    monkeypatch.setenv('LS_APP_SECRET', 's')
    monkeypatch.setattr('src.realtime.session.measure_ntp_offset_ns', lambda *a, **k: 0)

    cp = tmp_path / 'c.json'
    write_candidates(cp, [{'symbol': '005930', 'selection_reasons': ['limit_up']}], rev=1)
    mm = tmp_path / 'm.json'
    mm.write_text(json.dumps({'005930': 'KOSPI'}), encoding='utf-8')

    import contextlib as _ctxlib

    registered: list[tuple[int, object]] = []
    real_add_signal_handler = asyncio.unix_events._UnixSelectorEventLoop.add_signal_handler

    def _spy_add_signal_handler(self, sig, callback, *cb_args):
        registered.append((sig, callback))
        return real_add_signal_handler(self, sig, callback, *cb_args)

    monkeypatch.setattr(asyncio.AbstractEventLoop, 'add_signal_handler', _spy_add_signal_handler)
    with _ctxlib.suppress(AttributeError):
        monkeypatch.setattr(asyncio.BaseEventLoop, 'add_signal_handler', _spy_add_signal_handler)
    with _ctxlib.suppress(AttributeError):
        monkeypatch.setattr(
            asyncio.unix_events._UnixSelectorEventLoop,
            'add_signal_handler',
            _spy_add_signal_handler,
        )

    class _FakeAdapter:
        name = 'ls'
        capacity_pairs = 200
        def __init__(self, **kw):
            self.kw = kw
        async def connect(self):
            self.connected = True
        async def subscribe(self, pairs):
            return [VendorAck(s, t, True, '00000') for s, t in pairs]
        async def recv(self):
            raise VendorDisconnected('closed')
        async def aclose(self):
            self.closed = True

    class _FakeHttp:
        async def close(self):
            self.closed = True

    monkeypatch.setattr(registry, "LsRealtimeAdapter", _FakeAdapter)
    monkeypatch.setattr('aiohttp.ClientSession', lambda *a, **k: _FakeHttp())

    args = argparse.Namespace(
        session_date='2026-09-08', journal_root=str(tmp_path / 'l0'),
        manifest_path=str(tmp_path / 's.json'), candidates_path=str(cp),
        market_map=str(mm), ntp_host='h', max_clock_offset_ns=2_000_000_000, max_cycles=1,
    )

    rc = collect_stream.run(args)

    assert rc == 0
    assert registered and registered[0][0] == signal.SIGTERM  # noqa: PT018 - verbatim contract skeleton


def test_collect_stream_forwards_degraded_reason_to_session_config(tmp_path, monkeypatch) -> None:
    import argparse

    import pytest

    from src.cli import collect_stream
    from src.cli.main import build_parser

    monkeypatch.setenv("LS_APP_KEY", "k")
    monkeypatch.setenv("LS_APP_SECRET", "s")
    seen = []

    def _capture(cfg, **kw):
        seen.append(cfg)
        raise RuntimeError("stop-after-bootstrap")

    monkeypatch.setattr(collect_stream, "bootstrap_session", _capture)
    base = [
        "collect-stream", "--session-date", "2026-09-14", "--journal-root", str(tmp_path / "l0"),
        "--manifest-path", str(tmp_path / "s.json"), "--candidates-path", str(tmp_path / "c.json"), "--market-map", str(tmp_path / "m.json"),
    ]
    args = build_parser().parse_args([*base, "--degraded-reason", "orchestration_failed"])
    with pytest.raises(RuntimeError, match="stop-after-bootstrap"):
        collect_stream.run(args)

    legacy = argparse.Namespace(
        session_date="2026-09-14", journal_root=str(tmp_path / "l0"), manifest_path=str(tmp_path / "s.json"),
        candidates_path=str(tmp_path / "c.json"), market_map=str(tmp_path / "m.json"), ntp_host="h",
        max_clock_offset_ns=2_000_000_000, max_cycles=1,
    )
    with pytest.raises(RuntimeError, match="stop-after-bootstrap"):
        collect_stream.run(legacy)

    assert args.degraded_reason == "orchestration_failed"
    assert [cfg.degraded_reason for cfg in seen] == ["orchestration_failed", None]


def test_collect_stream_wires_watchdog_ntp_fallback_and_shutdown_event(tmp_path, monkeypatch, caplog) -> None:
    import argparse
    import json
    import logging

    from src.cli import collect_stream
    from src.realtime.adapters import registry
    from src.universe.ipc import write_candidates

    monkeypatch.setenv("LS_APP_KEY", "k")
    monkeypatch.setenv("LS_APP_SECRET", "s")
    monkeypatch.setattr("src.realtime.session.measure_ntp_offset_ns", lambda *a, **k: 0)
    cp = tmp_path / "c.json"
    write_candidates(cp, [{"symbol": "005930", "selection_reasons": ["limit_up"]}], rev=1)
    mm = tmp_path / "m.json"
    mm.write_text(json.dumps({"005930": "KOSPI"}), encoding="utf-8")
    captured: dict[str, object] = {}
    real_bootstrap = collect_stream.bootstrap_session

    def _bootstrap(cfg, **kw):
        captured["cfg"] = cfg
        return real_bootstrap(cfg, **kw)

    class _FakeStreamer:
        def __init__(self, **kw):
            captured["streamer_kwargs"] = kw

        async def run_forever(self, stop, *, max_cycles=None):
            captured["max_cycles"] = max_cycles

    class _FakeHttp:
        async def close(self):
            return None

    monkeypatch.setattr(collect_stream, "bootstrap_session", _bootstrap)
    monkeypatch.setattr(collect_stream, "RealtimeStreamer", _FakeStreamer)
    monkeypatch.setattr(registry, "LsRealtimeAdapter", lambda **kw: object())
    monkeypatch.setattr("aiohttp.ClientSession", lambda *a, **k: _FakeHttp())
    args = argparse.Namespace(session_date="2026-09-14", journal_root=str(tmp_path / "l0"), manifest_path=str(tmp_path / "s.json"),
                              candidates_path=str(cp), market_map=str(mm), ntp_host="h", max_clock_offset_ns=2_000_000_000, max_cycles=1)

    with caplog.at_level(logging.INFO):
        assert collect_stream.run(args) == 0

    assert captured["cfg"].ntp_fallback_hosts == ("time.google.com", "time.cloudflare.com")
    silence_limit = captured["streamer_kwargs"]["silence_limit"]
    assert callable(silence_limit)
    limit = silence_limit()
    assert limit is None or limit >= 30.0
    assert captured["max_cycles"] == 1
    assert "[DATA] stage=stream_shutdown" in caplog.text
    assert (tmp_path / "s.json").exists()


def test_collect_stream_aborts_before_connect_when_clock_unsynced(tmp_path, monkeypatch) -> None:
    import argparse
    import json

    import pytest

    from src.cli import collect_stream
    from src.realtime.adapters import registry
    from src.realtime.clock import ClockUnsyncedError
    from src.universe.ipc import write_candidates

    monkeypatch.setenv("LS_APP_KEY", "k")
    monkeypatch.setenv("LS_APP_SECRET", "s")
    monkeypatch.setattr("src.realtime.session.measure_ntp_offset_ns", lambda *a, **k: (_ for _ in ()).throw(ClockUnsyncedError("ntp unreachable: all 3 hosts failed")))

    cp = tmp_path / "c.json"
    write_candidates(cp, [{"symbol": "005930", "selection_reasons": ["limit_up"]}], rev=1)
    mm = tmp_path / "m.json"
    mm.write_text(json.dumps({"005930": "KOSPI"}), encoding="utf-8")

    connected: list[bool] = []

    class _NeverAdapter:
        def __init__(self, **kw):
            connected.append(True)

    monkeypatch.setattr(registry, "LsRealtimeAdapter", _NeverAdapter)

    args = argparse.Namespace(
        session_date="2026-09-08", journal_root=str(tmp_path / "l0"),
        manifest_path=str(tmp_path / "s.json"), candidates_path=str(cp),
        market_map=str(mm), ntp_host="h", max_clock_offset_ns=2_000_000_000, max_cycles=1,
    )

    with pytest.raises(ClockUnsyncedError):
        collect_stream.run(args)

    assert connected == []
    assert not (tmp_path / "s.json").exists()


def test_collect_stream_uses_settings_vendor_adapter(tmp_path, monkeypatch) -> None:
    import argparse

    from src.cli import collect_stream
    from src.realtime.adapters import registry
    from src.universe.ipc import write_candidates

    monkeypatch.setenv("KRX_ALPHA_VENDOR", "db")
    monkeypatch.setenv("DB_APP_KEY", "k")
    monkeypatch.setenv("DB_APP_SECRET", "s")
    monkeypatch.setattr("src.realtime.session.measure_ntp_offset_ns", lambda *a, **k: 0)
    cp = tmp_path / "c.json"
    write_candidates(cp, [{"symbol": "005930", "selection_reasons": ["limit_up"]}], rev=1)
    seen: dict[str, object] = {}
    real_bootstrap = collect_stream.bootstrap_session

    def _capture(cfg, **kw):
        seen["cfg"] = cfg
        return real_bootstrap(cfg, **kw)

    class _FakeDbAdapter:
        def __init__(self, **kw):
            seen["adapter_kw"] = kw

    class _FakeStreamer:
        def __init__(self, **kw):
            seen["adapter"] = kw.get("adapter")

        async def run_forever(self, stop, *, max_cycles=None):
            return None

    class _FakeHttp:
        async def close(self):
            return None

    monkeypatch.setattr(collect_stream, "bootstrap_session", _capture)
    monkeypatch.setattr(collect_stream, "RealtimeStreamer", _FakeStreamer)
    monkeypatch.setattr(registry, "DbRealtimeAdapter", _FakeDbAdapter)
    monkeypatch.setattr("aiohttp.ClientSession", lambda *a, **k: _FakeHttp())
    args = argparse.Namespace(session_date="2026-09-08", journal_root=str(tmp_path / "l0"),
                              manifest_path=str(tmp_path / "s.json"), candidates_path=str(cp),
                              ntp_host="h", max_clock_offset_ns=2_000_000_000, max_cycles=1)
    assert collect_stream.run(args) == 0
    assert seen["cfg"].vendor == "db"
    assert isinstance(seen["adapter"], _FakeDbAdapter)


def test_collect_stream_missing_market_map_fails_closed(tmp_path, monkeypatch) -> None:
    import argparse

    import pytest

    from src.cli import collect_stream
    from src.core.errors import KrxAlphaError

    monkeypatch.delenv("KRX_ALPHA_VENDOR", raising=False)
    monkeypatch.setenv("LS_APP_KEY", "k")
    monkeypatch.setenv("LS_APP_SECRET", "s")
    monkeypatch.setattr(collect_stream, "bootstrap_session", lambda cfg: object())
    args = argparse.Namespace(session_date="2026-09-08", journal_root=str(tmp_path / "l0"),
                              manifest_path=str(tmp_path / "s.json"), candidates_path=str(tmp_path / "c.json"),
                              ntp_host="h", max_clock_offset_ns=2_000_000_000, max_cycles=1)
    with pytest.raises(KrxAlphaError, match="market-map"):
        collect_stream.run(args)


def _run_with_captured_streamer(tmp_path, monkeypatch, env: dict[str, str]):
    import argparse
    import json

    from src.cli import collect_stream
    from src.realtime.adapters import registry
    from src.universe.ipc import write_candidates

    monkeypatch.setenv("LS_APP_KEY", "k")
    monkeypatch.setenv("LS_APP_SECRET", "s")
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr("src.realtime.session.measure_ntp_offset_ns", lambda *a, **k: 0)
    cp = tmp_path / "c.json"
    write_candidates(cp, [{"symbol": "005930", "selection_reasons": ["limit_up"]}], rev=1)
    mm = tmp_path / "m.json"
    mm.write_text(json.dumps({"005930": "KOSPI"}), encoding="utf-8")
    captured: dict[str, object] = {}

    class _FakeStreamer:
        def __init__(self, **kw):
            captured.update(kw)

        async def run_forever(self, stop, *, max_cycles=None):
            return None

    class _FakeHttp:
        async def close(self):
            return None

    monkeypatch.setattr(collect_stream, "RealtimeStreamer", _FakeStreamer)
    monkeypatch.setattr(registry, "LsRealtimeAdapter", lambda **kw: object())
    monkeypatch.setattr("aiohttp.ClientSession", lambda *a, **k: _FakeHttp())
    args = argparse.Namespace(session_date="2026-09-14", journal_root=str(tmp_path / "l0"), manifest_path=str(tmp_path / "s.json"),
                              candidates_path=str(cp), market_map=str(mm), ntp_host="h", max_clock_offset_ns=2_000_000_000, max_cycles=1)
    assert collect_stream.run(args) == 0
    return captured


def test_collect_stream_enables_proactive_handover_for_ls_by_default(tmp_path, monkeypatch) -> None:
    from src.realtime.connection_handover import Handover

    captured = _run_with_captured_streamer(tmp_path, monkeypatch, {})

    assert isinstance(captured["handover"], Handover)


def test_collect_stream_handover_can_be_disabled(tmp_path, monkeypatch) -> None:
    captured = _run_with_captured_streamer(tmp_path, monkeypatch, {"KRX_ALPHA_LAG_HANDOVER_ENABLED": "false"})

    assert "handover" not in captured


def _ls_key_id() -> str:
    import hashlib

    return hashlib.sha256(b"k").hexdigest()[:12]


def _ledger_rows(path) -> list:
    import json

    if not path.exists():
        return []
    return json.loads(path.read_text(encoding="utf-8"))["leases"]


def _fill_ls_ledger(token_dir, owners: tuple[str, ...]) -> None:
    from src.realtime.ws_lease import LeaseRole, WsLeaseLedger

    ledger = WsLeaseLedger(token_dir, vendor="ls", key_id=_ls_key_id(), max_connections=3, ttl_s=600.0)
    for owner in owners:
        ledger.acquire(owner, LeaseRole.PRIMARY)


def _run_shard(tmp_path, monkeypatch, *, index, symbols, env):
    import argparse
    import json

    from src.cli import collect_stream
    from src.realtime import session as session_module
    from src.realtime.adapters import registry
    from src.universe.ipc import write_candidates

    monkeypatch.setenv("LS_APP_KEY", "k")
    monkeypatch.setenv("LS_APP_SECRET", "s")
    monkeypatch.setenv("KRX_ALPHA_KIS_TOKEN_CACHE_DIR", str(tmp_path / "tokens"))
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr("src.realtime.session.measure_ntp_offset_ns", lambda *a, **k: 0)
    cp = tmp_path / "c.json"
    candidates = ("005930", "000660")
    write_candidates(cp, [{"symbol": s, "selection_reasons": ["limit_up"]} for s in candidates], rev=1)
    mm = tmp_path / "m.json"
    mm.write_text(json.dumps(dict.fromkeys(candidates, "KOSPI")), encoding="utf-8")
    ledger_file = tmp_path / "tokens" / f"wslease_ls_{_ls_key_id()}.json"
    captured: dict[str, object] = {}
    journal_tags: list[object] = []
    real_writer = session_module.L0JournalWriter

    def _writer(**kwargs):
        journal_tags.append(kwargs.get("file_tag"))
        return real_writer(**kwargs)

    class _FakeStreamer:
        def __init__(self, **kw):
            captured.update(kw)

        async def run_forever(self, stop, *, max_cycles=None):
            captured["ledger_during"] = _ledger_rows(ledger_file)

    class _FakeHttp:
        async def close(self):
            return None

    monkeypatch.setattr(session_module, "L0JournalWriter", _writer)
    monkeypatch.setattr(collect_stream, "RealtimeStreamer", _FakeStreamer)
    monkeypatch.setattr(registry, "LsRealtimeAdapter", lambda **kw: object())
    monkeypatch.setattr("aiohttp.ClientSession", lambda *a, **k: _FakeHttp())
    args = argparse.Namespace(
        session_date="2026-09-14",
        journal_root=str(tmp_path / "l0"),
        manifest_path=str(tmp_path / "s.json"),
        candidates_path=str(cp),
        market_map=str(mm),
        ntp_host="h",
        max_clock_offset_ns=2_000_000_000,
        max_cycles=1,
        ls_shard_index=index,
        symbols=symbols,
    )
    rc = collect_stream.run(args)
    return rc, captured, journal_tags, ledger_file


def test_collect_stream_shard_arguments_default_to_the_legacy_shard() -> None:
    from src.cli.main import build_parser

    args = build_parser().parse_args(
        [
            "collect-stream",
            "--session-date",
            "2026-09-14",
            "--journal-root",
            "l0",
            "--manifest-path",
            "s.json",
            "--candidates-path",
            "c.json",
            "--market-map",
            "m.json",
        ]
    )

    assert args.ls_shard_index == 0
    assert args.symbols is None


def test_shard_index_one_requires_symbols(tmp_path) -> None:
    import argparse

    from src.cli import collect_stream
    from src.core.errors import KrxAlphaError

    args = argparse.Namespace(
        session_date="2026-09-14",
        journal_root=str(tmp_path / "l0"),
        manifest_path=str(tmp_path / "s.json"),
        candidates_path=str(tmp_path / "c.json"),
        market_map=str(tmp_path / "m.json"),
        ls_shard_index=1,
        symbols=None,
    )

    with pytest.raises(KrxAlphaError, match="symbols is required"):
        collect_stream.run(args)
    assert not (tmp_path / "s.json").exists()


@pytest.mark.parametrize("symbols", ["005930,005930", "005930,", "00593", "005930;000660", "005930, "])
def test_malformed_shard_symbols_fail_closed(tmp_path, symbols: str) -> None:
    import argparse

    from src.cli import collect_stream
    from src.core.errors import KrxAlphaError

    args = argparse.Namespace(
        session_date="2026-09-14",
        journal_root=str(tmp_path / "l0"),
        manifest_path=str(tmp_path / "s.json"),
        candidates_path=str(tmp_path / "c.json"),
        market_map=str(tmp_path / "m.json"),
        ls_shard_index=1,
        symbols=symbols,
    )

    with pytest.raises(KrxAlphaError, match="symbols must be"):
        collect_stream.run(args)


def test_single_session_builds_no_lease_ledger(tmp_path, monkeypatch) -> None:
    from src.cli import collect_stream
    from src.realtime.connection_handover import Handover

    def _forbidden(*args, **kwargs):
        raise AssertionError("single-session collection must not build a lease ledger")

    monkeypatch.setattr(collect_stream, "WsLeaseLedger", _forbidden)

    captured = _run_with_captured_streamer(tmp_path, monkeypatch, {})

    assert isinstance(captured["handover"], Handover)


@pytest.mark.parametrize(("index", "expected_tag"), [(0, None), (1, "s1")])
def test_ls_shard_holds_its_primary_slot_while_streaming(tmp_path, monkeypatch, caplog, index, expected_tag) -> None:
    caplog.set_level("INFO", logger="src.cli.collect_stream")
    env = {"KRX_ALPHA_LS_ACTIVE_SESSIONS": "2", "KRX_ALPHA_LS_MAX_SESSIONS": "3"}

    rc, captured, journal_tags, ledger_file = _run_shard(tmp_path, monkeypatch, index=index, symbols="005930", env=env)

    assert rc == 0
    held = captured["ledger_during"]
    assert [(row["owner"].startswith(f"ls-shard-{index}-"), row["role"]) for row in held] == [(True, "primary")]
    assert _ledger_rows(ledger_file) == []
    assert {symbol for symbol, _ in captured["replay_pairs"]} == {"005930"}
    assert journal_tags
    assert set(journal_tags) == {expected_tag}
    assert f"[DATA] stage=ls_shard index={index} symbols=1" in caplog.text


def test_shard_one_exits_3_when_its_primary_slot_is_denied(tmp_path, monkeypatch) -> None:
    _fill_ls_ledger(tmp_path / "tokens", ("a", "b", "c"))
    env = {"KRX_ALPHA_LS_ACTIVE_SESSIONS": "2", "KRX_ALPHA_LS_MAX_SESSIONS": "3"}

    rc, captured, _, ledger_file = _run_shard(tmp_path, monkeypatch, index=1, symbols="005930", env=env)

    assert rc == 3
    assert "replay_pairs" not in captured
    assert not (tmp_path / "s.json").exists()
    assert len(_ledger_rows(ledger_file)) == 3


def test_shard_zero_proceeds_unleased_after_the_retry_window(tmp_path, monkeypatch, caplog) -> None:
    from src.cli import collect_stream

    monkeypatch.setattr(collect_stream, "_LEASE_RETRY_S", 0.01)
    _fill_ls_ledger(tmp_path / "tokens", ("a", "b", "c"))
    env = {"KRX_ALPHA_LS_ACTIVE_SESSIONS": "2", "KRX_ALPHA_LS_MAX_SESSIONS": "3", "KRX_ALPHA_WS_LEASE_TTL_S": "0.05"}

    rc, captured, _, ledger_file = _run_shard(tmp_path, monkeypatch, index=0, symbols="005930", env=env)

    assert rc == 0
    assert "replay_pairs" in captured
    assert [row["owner"] for row in captured["ledger_during"]] == ["a", "b", "c"]
    assert any(record.levelname == "CRITICAL" and "PRIMARY_FORCED" in record.getMessage() for record in caplog.records)
    assert [row["owner"] for row in _ledger_rows(ledger_file)] == ["a", "b", "c"]


def test_shard_index_must_be_non_negative(tmp_path) -> None:
    import argparse

    from src.cli import collect_stream
    from src.core.errors import KrxAlphaError

    args = argparse.Namespace(
        session_date="2026-09-14",
        journal_root=str(tmp_path / "l0"),
        manifest_path=str(tmp_path / "s.json"),
        candidates_path=str(tmp_path / "c.json"),
        market_map=str(tmp_path / "m.json"),
        ls_shard_index=-1,
        symbols=None,
    )

    with pytest.raises(KrxAlphaError, match="must be non-negative"):
        collect_stream.run(args)
    assert not (tmp_path / "s.json").exists()


def test_ls_guard_config_keeps_only_the_shard_tiers_and_priority_order(tmp_path) -> None:
    from src.cli.collect_stream import ls_guard_config
    from src.core.config import CollectorSettings
    from src.universe.ipc import write_candidates

    cp = tmp_path / "c.json"
    write_candidates(cp, [{"symbol": "005930", "tier": "A"}, {"symbol": "000660", "tier": "B"}], rev=1)

    shard = ls_guard_config(CollectorSettings(), cp, shard_symbols=("000660",))
    assert shard.order == ("000660",)
    assert shard.tiers == {"000660": "B"}

    empty = ls_guard_config(CollectorSettings(), cp, shard_symbols=("123456",))
    assert empty.order is None
    assert empty.tiers is None


def test_shard_primary_release_failure_is_logged_and_the_run_completes(tmp_path, monkeypatch, caplog) -> None:
    from src.realtime import ws_lease

    caplog.set_level("WARNING", logger="src.cli.collect_stream")

    def _unreleasable(self, lease):
        raise ws_lease.LeaseUnavailable("ledger_unavailable")

    monkeypatch.setattr(ws_lease.WsLeaseLedger, "release", _unreleasable)
    env = {"KRX_ALPHA_LS_ACTIVE_SESSIONS": "2", "KRX_ALPHA_LS_MAX_SESSIONS": "3"}

    rc, captured, _, ledger_file = _run_shard(tmp_path, monkeypatch, index=1, symbols="005930", env=env)

    assert rc == 0
    assert "replay_pairs" in captured
    assert [row["role"] for row in _ledger_rows(ledger_file)] == ["primary"]
    assert "status=RELEASE_FAILED reason=ledger_unavailable" in caplog.text


@pytest.mark.parametrize(("index", "requests_standby"), [(0, True), (1, False)])
def test_only_shard_zero_requests_lag_standbys(tmp_path, monkeypatch, index, requests_standby) -> None:
    from src.cli import collect_stream

    seen: list[object] = []
    real_monitor = collect_stream.LagMonitor

    def _monitor(**kwargs):
        seen.append(kwargs.get("on_exceeded"))
        return real_monitor(**kwargs)

    monkeypatch.setattr(collect_stream, "LagMonitor", _monitor)
    env = {"KRX_ALPHA_LS_ACTIVE_SESSIONS": "2", "KRX_ALPHA_LS_MAX_SESSIONS": "3"}

    rc, _captured, _tags, _ledger = _run_shard(tmp_path, monkeypatch, index=index, symbols="005930", env=env)

    assert rc == 0
    assert len(seen) == 1
    assert (seen[0] is not None) is requests_standby
