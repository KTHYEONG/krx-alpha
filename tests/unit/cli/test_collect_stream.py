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

    monkeypatch.setattr(collect_stream, 'LsRealtimeAdapter', _FakeAdapter)
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

    monkeypatch.setattr(collect_stream, 'LsRealtimeAdapter', _FakeAdapter)
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
    monkeypatch.setattr(collect_stream, "LsRealtimeAdapter", lambda **kw: object())
    monkeypatch.setattr("aiohttp.ClientSession", lambda *a, **k: _FakeHttp())
    args = argparse.Namespace(session_date="2026-09-14", journal_root=str(tmp_path / "l0"), manifest_path=str(tmp_path / "s.json"),
                              candidates_path=str(cp), market_map=str(mm), ntp_host="h", max_clock_offset_ns=2_000_000_000, max_cycles=1)

    with caplog.at_level(logging.INFO):
        assert collect_stream.run(args) == 0

    assert captured["cfg"].ntp_fallback_hosts == ("time.google.com", "time.cloudflare.com")
    silence_limit = captured["streamer_kwargs"]["silence_limit"]
    assert callable(silence_limit)
    assert silence_limit() in (None, 30.0)
    assert captured["max_cycles"] == 1
    assert "[DATA] stage=stream_shutdown" in caplog.text
    assert (tmp_path / "s.json").exists()


def test_collect_stream_aborts_before_connect_when_clock_unsynced(tmp_path, monkeypatch) -> None:
    import argparse
    import json

    import pytest

    from src.cli import collect_stream
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

    monkeypatch.setattr(collect_stream, "LsRealtimeAdapter", _NeverAdapter)

    args = argparse.Namespace(
        session_date="2026-09-08", journal_root=str(tmp_path / "l0"),
        manifest_path=str(tmp_path / "s.json"), candidates_path=str(cp),
        market_map=str(mm), ntp_host="h", max_clock_offset_ns=2_000_000_000, max_cycles=1,
    )

    with pytest.raises(ClockUnsyncedError):
        collect_stream.run(args)

    assert connected == []
    assert not (tmp_path / "s.json").exists()
