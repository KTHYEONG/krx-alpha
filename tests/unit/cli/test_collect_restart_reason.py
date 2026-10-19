"""Invariant guards for `--restart-reason` plumbing and KIS ack-timeout wiring."""

from __future__ import annotations


def test_aftermarket_restart_reason_plumbing(monkeypatch, tmp_path) -> None:
    import argparse
    import asyncio

    from src.cli import collect_aftermarket
    from src.realtime.kis_sharding import KisDataCredential

    captured = {}

    class FakeAdapter:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    class FakeStreamer:
        def __init__(self, **kwargs):
            pass

        async def run_forever(self, *args, **kwargs):
            return None

    monkeypatch.setattr(collect_aftermarket, "KisRealtimeAdapter", FakeAdapter)
    monkeypatch.setattr(collect_aftermarket, "RealtimeStreamer", FakeStreamer)
    monkeypatch.setattr(
        collect_aftermarket, "load_kis_data_credentials",
        lambda: (KisDataCredential("1", "k", "s", "h", "fp1"),),
    )
    cfgs = {}

    def _bootstrap(cfg):
        cfgs["cfg"] = cfg
        return type("S", (), {
            "replay_pairs": lambda self: [("005930", "H0NXCNT0"), ("005930", "H0NXASP0")],
            "persist": lambda self: None,
        })()

    monkeypatch.setattr(collect_aftermarket, "bootstrap_session", _bootstrap)
    import datetime as _dt
    from zoneinfo import ZoneInfo as _ZI
    from src.universe.ipc import CandidateSnapshot as _CS, write_candidate_snapshot as _wcs

    _st = _dt.datetime(2026, 9, 15, 15, 31, tzinfo=_ZI("Asia/Seoul"))
    _wcs(
        tmp_path / "candidates.json",
        _CS(schema_version=1, rev=20260915, session_date=_dt.date(2026, 9, 15), session="aftermarket_nxt",
            generated_at=_st, source_asof=_st, effective_from=_st, policy_version="aftermarket_nxt_v1",
            capacity=1, eligible_count=1, selected_count=1, candidates=(
                {"symbol": "005930", "rank": 1, "source_ranks": {"trade_amount": 1},
                 "metrics": {"trade_value_krw": 1, "change_pct": 1.0}, "selection_reasons": ["trade_amount"]},)),
    )

    def _args(**over):
        base = {
            "session_date": "2026-09-15", "journal_root": str(tmp_path / "l0"),
            "manifest_path": str(tmp_path / "nxt.json"), "candidates_path": str(tmp_path / "candidates.json"),
            "venue": "nxt", "shard_index": 0, "credential_slot": "1", "credential_key_id": "fp1",
            "symbols": "005930", "ntp_host": "x", "max_clock_offset_ns": 2, "max_cycles": 1,
            "degraded_reason": None,
        }
        base.update(over)
        base.setdefault("restart_reason", None)
        return argparse.Namespace(**base)

    from src.core.config import CollectorSettings

    monkeypatch.setattr(
        collect_aftermarket, "resolve_collector_runtime",
        lambda **kw: type("R", (), {"collector": CollectorSettings(data_root=tmp_path / "d"),
                                    "aftermarket": type("A", (), {
                                        "nxt_streams": ("H0NXCNT0", "H0NXASP0"),
                                        "krx_streams": ("H0STCNT0", "H0STASP0"),
                                        "pair_capacity_per_connection": 4, "max_symbols": 40,
                                        "silence_limit_s": 10.0})()})(),
    )
    asyncio.run(collect_aftermarket._run_stream(_args(restart_reason="stale_child")))
    assert cfgs["cfg"].restart_reason == "stale_child"
    assert captured["ack_timeout_s"] == 10.0
    asyncio.run(collect_aftermarket._run_stream(_args()))
    assert cfgs["cfg"].restart_reason is None


def test_aftermarket_restart_reason_parser() -> None:
    import argparse

    import pytest

    from src.cli import collect_aftermarket

    parser = argparse.ArgumentParser()
    subs = parser.add_subparsers()
    collect_aftermarket.add_parser(subs)
    args = parser.parse_args([
        "collect-aftermarket", "--session-date", "2026-09-15", "--journal-root", "j",
        "--manifest-path", "m", "--candidates-path", "c", "--venue", "nxt",
        "--shard-index", "0", "--credential-slot", "1", "--credential-key-id", "k",
        "--symbols", "005930", "--restart-reason", "stale_child",
    ])
    assert args.restart_reason == "stale_child"
    with pytest.raises(SystemExit):
        parser.parse_args([
            "collect-aftermarket", "--session-date", "2026-09-15", "--journal-root", "j",
            "--manifest-path", "m", "--candidates-path", "c", "--venue", "nxt",
            "--shard-index", "0", "--credential-slot", "1", "--credential-key-id", "k",
            "--symbols", "005930", "--restart-reason", "bogus",
        ])


def test_premarket_restart_reason_parser_and_timeout() -> None:
    import argparse

    from src.cli import collect_premarket

    parser = argparse.ArgumentParser()
    subs = parser.add_subparsers()
    collect_premarket.add_parser(subs)
    args = parser.parse_args([
        "collect-premarket", "--session-date", "2026-09-15", "--journal-root", "j",
        "--manifest-path", "m", "--candidates-path", "c", "--credential-slot", "1",
        "--credential-key-id", "k", "--symbols", "005930",
    ])
    assert args.restart_reason is None


def test_regular_parser_untouched() -> None:
    import argparse

    from src.cli import collect_stream

    parser = argparse.ArgumentParser()
    subs = parser.add_subparsers()
    collect_stream.add_parser(subs)
    actions = [a.dest for sub in subs.choices.values() for a in sub._actions]
    assert "restart_reason" not in actions
