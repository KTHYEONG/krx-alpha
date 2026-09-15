

def test_collect_aftermarket_builds_nxt_kis_route(monkeypatch, tmp_path):
    import argparse
    import asyncio
    from src.cli import collect_aftermarket
    from src.realtime.kis_sharding import KisDataCredential
    captured = {}
    class FakeAdapter:
        def __init__(self, **kwargs): captured.update(kwargs)
    class FakeStreamer:
        def __init__(self, **kwargs): captured['replay_pairs'] = kwargs['replay_pairs']
        async def run_forever(self, *args, **kwargs): return None
    monkeypatch.setattr(collect_aftermarket, 'KisRealtimeAdapter', FakeAdapter)
    monkeypatch.setattr(collect_aftermarket, 'RealtimeStreamer', FakeStreamer)
    monkeypatch.setattr(collect_aftermarket, 'load_kis_data_credentials', lambda: (KisDataCredential('1','k','s','h','fp1'),))
    monkeypatch.setattr(collect_aftermarket, 'CollectorSettings', lambda: type('Settings', (), {'after_market_enabled': True, 'subscription_pair_budget': 4, 'ntp_fallback_hosts': ()})())
    monkeypatch.setattr(collect_aftermarket, 'AftermarketSettings', lambda **kwargs: type('Aftermarket', (), {'nxt_streams': ('H0NXCNT0', 'H0NXASP0'), 'krx_streams': ('H0STCNT0', 'H0STASP0'), 'pair_capacity_per_connection': 4})())
    monkeypatch.setattr(collect_aftermarket, 'bootstrap_session', lambda cfg: type('S', (), {'replay_pairs': lambda self: [('005930','H0NXCNT0'),('005930','H0NXASP0')], 'persist': lambda self: None})())
    (tmp_path/'candidates.json').write_text('{"candidates":[{"symbol":"005930"}]}')
    args = argparse.Namespace(session_date='2026-09-15', journal_root=str(tmp_path/'l0'), manifest_path=str(tmp_path/'nxt.json'), candidates_path=str(tmp_path/'candidates.json'), venue='nxt', shard_index=0, credential_slot='1', credential_key_id='fp1', symbols='005930', ntp_host='x', max_clock_offset_ns=2, max_cycles=1, degraded_reason=None)
    asyncio.run(collect_aftermarket._run_stream(args))
    assert captured['route'].venue.value == 'nxt'
    assert {stream for _, stream in captured['replay_pairs']} == {'H0NXCNT0', 'H0NXASP0'}


def test_collect_aftermarket_rejects_missing_verified_capacity(monkeypatch, tmp_path):
    import argparse
    import asyncio
    import pytest

    from src.cli import collect_aftermarket
    from src.core.errors import SlotBudgetExceededError
    from src.realtime.kis_sharding import KisDataCredential

    monkeypatch.setattr(collect_aftermarket, 'load_kis_data_credentials', lambda: (KisDataCredential('1','k','s','h','fp1'),))
    monkeypatch.setattr(collect_aftermarket, 'CollectorSettings', lambda: type('Settings', (), {'after_market_enabled': True, 'subscription_pair_budget': 4, 'ntp_fallback_hosts': ()})())
    monkeypatch.setattr(collect_aftermarket, 'AftermarketSettings', lambda **kwargs: type('Aftermarket', (), {'nxt_streams': ('H0NXCNT0', 'H0NXASP0'), 'krx_streams': ('H0STCNT0', 'H0STASP0'), 'pair_capacity_per_connection': None})())
    monkeypatch.setattr(collect_aftermarket, 'bootstrap_session', lambda cfg: type('S', (), {'replay_pairs': lambda self: [('005930', 'H0NXCNT0')]})())
    (tmp_path/'candidates.json').write_text('{"candidates":[{"symbol":"005930"}]}')
    args = argparse.Namespace(session_date='2026-09-15', journal_root=str(tmp_path/'l0'), manifest_path=str(tmp_path/'nxt.json'), candidates_path=str(tmp_path/'candidates.json'), venue='nxt', shard_index=0, credential_slot='1', credential_key_id='fp1', symbols='005930', ntp_host='x', max_clock_offset_ns=2, max_cycles=1, degraded_reason=None)
    with pytest.raises(SlotBudgetExceededError, match='capacity'):
        asyncio.run(collect_aftermarket._run_stream(args))


def test_collect_aftermarket_rejects_fingerprint_mismatch_before_adapter(monkeypatch, tmp_path) -> None:
    import argparse
    import asyncio

    import pytest
    from src.cli import collect_aftermarket
    from src.core.errors import MissingCredentialsError
    from src.realtime.kis_sharding import KisDataCredential
    monkeypatch.setattr(collect_aftermarket, 'load_kis_data_credentials', lambda: (KisDataCredential('1','key','secret','hts','actual'),))
    monkeypatch.setattr(collect_aftermarket, 'KisRealtimeAdapter', lambda **_: (_ for _ in ()).throw(AssertionError('must not create adapter')))
    args = argparse.Namespace(session_date='2026-09-15', journal_root=str(tmp_path/'l0'), manifest_path=str(tmp_path/'m.json'), candidates_path=str(tmp_path/'c.json'), venue='nxt', shard_index=0, credential_slot='1', credential_key_id='wrong', symbols='005930', ntp_host='x', max_clock_offset_ns=2, max_cycles=1, degraded_reason=None)
    with pytest.raises(MissingCredentialsError, match='fingerprint'):
        asyncio.run(collect_aftermarket._run_stream(args))


def test_collect_aftermarket_parser_requires_shard_arguments() -> None:
    import argparse
    from src.cli import collect_aftermarket
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    collect_aftermarket.add_parser(subparsers)
    args = parser.parse_args(["collect-aftermarket", "--session-date", "2026-09-15", "--journal-root", "l0", "--manifest-path", "m.json", "--candidates-path", "c.json", "--venue", "nxt", "--shard-index", "0", "--credential-slot", "1", "--credential-key-id", "id1", "--symbols", "005930"])
    assert (args.shard_index, args.credential_slot, args.credential_key_id, args.symbols) == (0, "1", "id1", "005930")


def test_collect_aftermarket_rejects_symbols_outside_candidates(monkeypatch, tmp_path) -> None:
    import argparse
    import asyncio

    import pytest
    from src.cli import collect_aftermarket
    from src.core.errors import MissingCredentialsError
    from src.realtime.kis_sharding import KisDataCredential
    monkeypatch.setattr(collect_aftermarket, "load_kis_data_credentials", lambda: (KisDataCredential("1", "k", "s", "h", "fp1"),))
    (tmp_path / "candidates.json").write_text('{"candidates":[{"symbol":"005930"}]}')
    args = argparse.Namespace(session_date="2026-09-15", journal_root=str(tmp_path / "l0"), manifest_path=str(tmp_path / "m.json"), candidates_path=str(tmp_path / "candidates.json"), venue="nxt", shard_index=0, credential_slot="1", credential_key_id="fp1", symbols="999999", ntp_host="x", max_clock_offset_ns=2, max_cycles=1, degraded_reason=None)
    with pytest.raises(MissingCredentialsError, match="subset"):
        asyncio.run(collect_aftermarket._run_stream(args))


def test_collect_aftermarket_rejects_duplicate_or_reordered_symbols(monkeypatch, tmp_path) -> None:
    import argparse
    import asyncio
    import pytest

    from src.cli import collect_aftermarket
    from src.core.errors import MissingCredentialsError
    from src.realtime.kis_sharding import KisDataCredential

    monkeypatch.setattr(collect_aftermarket, "load_kis_data_credentials", lambda: (KisDataCredential("1", "k", "s", "h", "fp1"),))
    (tmp_path / "candidates.json").write_text('{"candidates":[{"symbol":"005930"},{"symbol":"000660"}]}')
    args = argparse.Namespace(session_date="2026-09-15", journal_root=str(tmp_path / "l0"), manifest_path=str(tmp_path / "m.json"), candidates_path=str(tmp_path / "candidates.json"), venue="nxt", shard_index=0, credential_slot="1", credential_key_id="fp1", symbols="000660,000660", ntp_host="x", max_clock_offset_ns=2, max_cycles=1, degraded_reason=None)
    with pytest.raises(MissingCredentialsError, match="exactly match"):
        asyncio.run(collect_aftermarket._run_stream(args))
