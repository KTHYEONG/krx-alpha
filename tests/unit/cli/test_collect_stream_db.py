"""DB crosscheck streamer CLI invariant tests."""

from __future__ import annotations


def test_collect_stream_db_subcommand_registered_without_market_map() -> None:
    from src.cli.main import build_parser

    parser = build_parser()
    args = parser.parse_args([
        "collect-stream-db", "--session-date", "2026-09-08", "--journal-root", "l0",
        "--manifest-path", "s.db.json", "--candidates-path", "c.json",
    ])
    assert args.command == "collect-stream-db"
    assert callable(args.handler)
    assert not hasattr(args, "market_map")


def test_collect_stream_db_skips_when_disabled(tmp_path, monkeypatch, caplog) -> None:
    import argparse
    import logging

    from src.cli import collect_stream_db

    monkeypatch.setenv("KRX_ALPHA_DB_CROSSCHECK_ENABLED", "false")
    monkeypatch.delenv("DB_APP_KEY", raising=False)
    monkeypatch.delenv("DB_APP_SECRET", raising=False)
    constructed: list[bool] = []
    monkeypatch.setattr(collect_stream_db, "DbRealtimeAdapter", lambda **kw: constructed.append(True))
    args = argparse.Namespace(session_date="2026-09-08", journal_root=str(tmp_path / "l0"),
                              manifest_path=str(tmp_path / "s.db.json"), candidates_path=str(tmp_path / "c.json"),
                              ntp_host=None, max_clock_offset_ns=None, max_cycles=1, degraded_reason=None)
    with caplog.at_level(logging.INFO):
        assert collect_stream_db.run(args) == 0
    assert constructed == []
    assert "stage=db_crosscheck status=SKIP reason=disabled" in caplog.text


def test_collect_stream_db_uses_db_vendor_route_and_own_manifest(tmp_path, monkeypatch) -> None:
    import argparse
    import json

    from src.cli import collect_stream_db
    from src.realtime.contracts import MarketSession, MarketVenue
    from src.universe.ipc import write_candidates

    monkeypatch.setenv("KRX_ALPHA_DB_CROSSCHECK_ENABLED", "true")
    monkeypatch.setenv("DB_APP_KEY", "k")
    monkeypatch.setenv("DB_APP_SECRET", "s")
    monkeypatch.setattr("src.realtime.session.measure_ntp_offset_ns", lambda *a, **k: 0)
    cp = tmp_path / "c.json"
    write_candidates(cp, [{"symbol": "005930", "selection_reasons": ["limit_up"]}], rev=1)
    seen: dict[str, object] = {}

    real_bootstrap = collect_stream_db.bootstrap_session

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

    monkeypatch.setattr(collect_stream_db, "bootstrap_session", _capture)
    monkeypatch.setattr(collect_stream_db, "RealtimeStreamer", _FakeStreamer)
    monkeypatch.setattr(collect_stream_db, "DbRealtimeAdapter", _FakeAdapter)
    monkeypatch.setattr("aiohttp.ClientSession", lambda *a, **k: _FakeHttp())
    manifest = str(tmp_path / "2026-09-08.db.json")
    args = argparse.Namespace(session_date="2026-09-08", journal_root=str(tmp_path / "l0"),
                              manifest_path=manifest, candidates_path=str(cp),
                              ntp_host="h", max_clock_offset_ns=2_000_000_000, max_cycles=1, degraded_reason=None)
    assert collect_stream_db.run(args) == 0
    cfg = seen["cfg"]
    assert cfg.vendor == "db"
    assert cfg.route.venue is MarketVenue.KRX
    assert cfg.route.session is MarketSession.REGULAR
    assert str(cfg.manifest_path).endswith(".db.json")
    from src.core.config import CollectorSettings

    assert cfg.symbol_limit == CollectorSettings().db_capacity_pairs // len(CollectorSettings().streams)
    assert cfg.slot_budget == CollectorSettings().db_capacity_pairs
    assert json.loads((tmp_path / "2026-09-08.db.json").read_text(encoding="utf-8"))["session_date"] == "2026-09-08"


def test_collect_stream_db_missing_credentials_raises(tmp_path, monkeypatch) -> None:
    import argparse

    import pytest

    from src.cli import collect_stream_db
    from src.core.errors import MissingCredentialsError

    monkeypatch.setenv("KRX_ALPHA_DB_CROSSCHECK_ENABLED", "true")
    monkeypatch.delenv("DB_APP_KEY", raising=False)
    monkeypatch.delenv("DB_APP_SECRET", raising=False)
    args = argparse.Namespace(session_date="2026-09-08", journal_root=str(tmp_path / "l0"),
                              manifest_path=str(tmp_path / "s.db.json"), candidates_path=str(tmp_path / "c.json"),
                              ntp_host=None, max_clock_offset_ns=None, max_cycles=1, degraded_reason=None)
    with pytest.raises(MissingCredentialsError):
        collect_stream_db.run(args)


def test_collect_stream_db_shutdown_log_uses_db_vendor(tmp_path, monkeypatch, caplog) -> None:
    import argparse
    import logging

    from src.cli import collect_stream_db
    from src.universe.ipc import write_candidates

    monkeypatch.setenv("KRX_ALPHA_DB_CROSSCHECK_ENABLED", "true")
    monkeypatch.setenv("DB_APP_KEY", "k")
    monkeypatch.setenv("DB_APP_SECRET", "s")
    monkeypatch.setattr("src.realtime.session.measure_ntp_offset_ns", lambda *a, **k: 0)
    cp = tmp_path / "c.json"
    write_candidates(cp, [{"symbol": "005930", "selection_reasons": ["limit_up"]}], rev=1)

    class _FakeStreamer:
        def __init__(self, **kw):
            pass

        async def run_forever(self, stop, *, max_cycles=None):
            return None

    class _FakeHttp:
        async def close(self):
            return None

    monkeypatch.setattr(collect_stream_db, "RealtimeStreamer", _FakeStreamer)
    monkeypatch.setattr(collect_stream_db, "DbRealtimeAdapter", lambda **kw: object())
    monkeypatch.setattr("aiohttp.ClientSession", lambda *a, **k: _FakeHttp())
    args = argparse.Namespace(session_date="2026-09-08", journal_root=str(tmp_path / "l0"),
                              manifest_path=str(tmp_path / "s.db.json"), candidates_path=str(cp),
                              ntp_host="h", max_clock_offset_ns=2_000_000_000, max_cycles=1, degraded_reason=None)
    with caplog.at_level(logging.INFO):
        assert collect_stream_db.run(args) == 0
    assert "stage=stream_shutdown vendor=db" in caplog.text


def test_db_crosscheck_config_and_paths_contract(tmp_path) -> None:
    import datetime as dt

    from src.core.config import CollectorSettings
    from src.core.paths import DataPaths

    settings = CollectorSettings(db_crosscheck_enabled=True)
    assert settings.db_crosscheck_enabled is True
    assert settings.db_crosscheck_symbol_budget == settings.db_capacity_pairs // len(settings.streams)
    assert CollectorSettings().db_crosscheck_enabled is False
    paths = DataPaths(tmp_path)
    day = dt.date(2026, 9, 8)
    assert paths.db_crosscheck_manifest_path(day) == paths.manifest_dir / "2026-09-08.db.json"
    assert paths.db_crosscheck_manifest_path(day) != paths.manifest_path(day)
