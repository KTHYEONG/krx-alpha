"""Kiwoom crosscheck CLI + config + paths contract tests."""

from __future__ import annotations

import argparse
import datetime as dt
import logging


def test_collect_stream_kiwoom_subcommand_registered() -> None:
    from src.cli.main import build_parser

    parser = build_parser()
    args = parser.parse_args([
        "collect-stream-kiwoom", "--session-date", "2026-09-08", "--journal-root", "l0",
        "--manifest-path", "s.kiwoom.json", "--candidates-path", "c.json",
    ])
    assert args.command == "collect-stream-kiwoom"
    assert callable(args.handler)
    assert not hasattr(args, "market_map")


def test_collect_stream_kiwoom_skips_when_disabled(tmp_path, monkeypatch, caplog) -> None:
    from src.cli import collect_stream_kiwoom
    from src.realtime.adapters import registry

    monkeypatch.setenv("KRX_ALPHA_KIWOOM_CROSSCHECK_ENABLED", "false")
    constructed: list[bool] = []
    monkeypatch.setattr(registry, "KiwoomRealtimeAdapter", lambda **kw: constructed.append(True))
    args = argparse.Namespace(session_date="2026-09-08", journal_root=str(tmp_path / "l0"),
                              manifest_path=str(tmp_path / "s.kiwoom.json"), candidates_path=str(tmp_path / "c.json"),
                              ntp_host=None, max_clock_offset_ns=None, max_cycles=1, degraded_reason=None)
    with caplog.at_level(logging.INFO):
        assert collect_stream_kiwoom.run(args) == 0
    assert constructed == []
    assert "stage=kiwoom_crosscheck status=SKIP reason=disabled" in caplog.text


def test_collect_stream_kiwoom_uses_kiwoom_vendor_and_own_manifest(tmp_path, monkeypatch) -> None:
    import json

    from src.cli import collect_stream_kiwoom
    from src.realtime.adapters import registry
    from src.realtime.contracts import MarketSession, MarketVenue
    from src.universe.ipc import write_candidates

    monkeypatch.setenv("KRX_ALPHA_KIWOOM_CROSSCHECK_ENABLED", "true")
    monkeypatch.setenv("KIWOOM_APP_KEY", "k")
    monkeypatch.setenv("KIWOOM_SECRET_KEY", "s")
    monkeypatch.setattr("src.realtime.session.measure_ntp_offset_ns", lambda *a, **k: 0)
    cp = tmp_path / "c.json"
    write_candidates(cp, [{"symbol": "005930", "selection_reasons": ["limit_up"]}], rev=1)
    seen: dict[str, object] = {}

    real_bootstrap = collect_stream_kiwoom.bootstrap_session

    def _capture(cfg, **kw):
        seen["cfg"] = cfg
        return real_bootstrap(cfg, **kw)

    class _FakeStreamer:
        def __init__(self, **kw):
            seen["adapter"] = kw.get("adapter")

        async def run_forever(self, stop, *, max_cycles=None):
            return None

    class _FakeAdapter:
        def __init__(self, **kw):
            seen["adapter_kw"] = kw

    class _FakeHttp:
        async def close(self):
            return None

    monkeypatch.setattr(collect_stream_kiwoom, "bootstrap_session", _capture)
    monkeypatch.setattr(collect_stream_kiwoom, "RealtimeStreamer", _FakeStreamer)
    monkeypatch.setattr(registry, "KiwoomRealtimeAdapter", _FakeAdapter)
    monkeypatch.setattr("aiohttp.ClientSession", lambda *a, **k: _FakeHttp())
    manifest = str(tmp_path / "2026-09-08.kiwoom.json")
    args = argparse.Namespace(session_date="2026-09-08", journal_root=str(tmp_path / "l0"),
                              manifest_path=manifest, candidates_path=str(cp),
                              ntp_host="h", max_clock_offset_ns=2_000_000_000, max_cycles=1, degraded_reason=None)
    assert collect_stream_kiwoom.run(args) == 0
    cfg = seen["cfg"]
    assert cfg.vendor == "kiwoom"
    assert cfg.route.venue is MarketVenue.KRX
    assert cfg.route.session is MarketSession.REGULAR
    assert str(cfg.manifest_path).endswith(".kiwoom.json")
    from src.core.config import CollectorSettings

    assert cfg.symbol_limit == CollectorSettings().kiwoom_capacity_pairs // len(CollectorSettings().streams)
    assert cfg.slot_budget == CollectorSettings().kiwoom_capacity_pairs
    assert json.loads((tmp_path / "2026-09-08.kiwoom.json").read_text(encoding="utf-8"))["session_date"] == "2026-09-08"


def test_kiwoom_config_and_paths_contract(tmp_path) -> None:
    from src.core.config import CollectorSettings
    from src.core.paths import DataPaths

    settings = CollectorSettings(kiwoom_crosscheck_enabled=True)
    assert settings.kiwoom_crosscheck_enabled is True
    assert settings.kiwoom_capacity_pairs == 200
    assert settings.kiwoom_ack_timeout_s == 10.0
    assert settings.kiwoom_user_agent == "krx-alpha"
    assert settings.kiwoom_crosscheck_symbol_budget == settings.kiwoom_capacity_pairs // len(settings.streams)
    assert CollectorSettings().kiwoom_crosscheck_enabled is False
    paths = DataPaths(tmp_path)
    day = dt.date(2026, 9, 8)
    assert paths.kiwoom_crosscheck_manifest_path(day) == paths.manifest_dir / "2026-09-08.kiwoom.json"
    assert paths.kiwoom_crosscheck_manifest_path(day) != paths.manifest_path(day)
    assert paths.kiwoom_crosscheck_manifest_path(day) != paths.db_crosscheck_manifest_path(day)
