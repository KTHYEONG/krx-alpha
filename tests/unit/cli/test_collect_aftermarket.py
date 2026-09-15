

def test_collect_aftermarket_builds_nxt_kis_route(monkeypatch, tmp_path):
    import argparse
    import asyncio
    from src.cli import collect_aftermarket
    captured = {}
    class FakeAdapter:
        def __init__(self, **kwargs): captured.update(kwargs)
    class FakeStreamer:
        def __init__(self, **kwargs): captured['replay_pairs'] = kwargs['replay_pairs']
        async def run_forever(self, *args, **kwargs): return None
    monkeypatch.setattr(collect_aftermarket, 'KisRealtimeAdapter', FakeAdapter)
    monkeypatch.setattr(collect_aftermarket, 'RealtimeStreamer', FakeStreamer)
    monkeypatch.setattr(collect_aftermarket, 'load_credentials', lambda _: object())
    monkeypatch.setattr(collect_aftermarket, 'CollectorSettings', lambda: type('Settings', (), {'after_market_enabled': True, 'subscription_pair_budget': 4, 'ntp_fallback_hosts': ()})())
    monkeypatch.setattr(collect_aftermarket, 'AftermarketSettings', lambda **kwargs: type('Aftermarket', (), {'nxt_streams': ('H0NXCNT0', 'H0NXASP0'), 'krx_streams': ('H0STCNT0', 'H0STASP0'), 'pair_capacity_per_connection': 4})())
    monkeypatch.setattr(collect_aftermarket, 'bootstrap_session', lambda cfg: type('S', (), {'replay_pairs': lambda self: [('005930','H0NXCNT0'),('005930','H0NXASP0')], 'persist': lambda self: None})())
    (tmp_path/'candidates.json').write_text('{"candidates":[{"symbol":"005930"}]}')
    args = argparse.Namespace(session_date='2026-09-15', journal_root=str(tmp_path/'l0'), manifest_path=str(tmp_path/'nxt.json'), candidates_path=str(tmp_path/'candidates.json'), venue='nxt', ntp_host='x', max_clock_offset_ns=2, max_cycles=1, degraded_reason=None)
    asyncio.run(collect_aftermarket._run_stream(args))
    assert captured['route'].venue.value == 'nxt'
    assert {stream for _, stream in captured['replay_pairs']} == {'H0NXCNT0', 'H0NXASP0'}


def test_collect_aftermarket_rejects_missing_verified_capacity(monkeypatch, tmp_path):
    import argparse
    import asyncio
    import pytest

    from src.cli import collect_aftermarket
    from src.core.errors import SlotBudgetExceededError

    monkeypatch.setattr(collect_aftermarket, 'CollectorSettings', lambda: type('Settings', (), {'after_market_enabled': True, 'subscription_pair_budget': 4, 'ntp_fallback_hosts': ()})())
    monkeypatch.setattr(collect_aftermarket, 'AftermarketSettings', lambda **kwargs: type('Aftermarket', (), {'nxt_streams': ('H0NXCNT0', 'H0NXASP0'), 'krx_streams': ('H0STCNT0', 'H0STASP0'), 'pair_capacity_per_connection': None})())
    monkeypatch.setattr(collect_aftermarket, 'bootstrap_session', lambda cfg: type('S', (), {'replay_pairs': lambda self: [('005930', 'H0NXCNT0')]})())
    args = argparse.Namespace(session_date='2026-09-15', journal_root=str(tmp_path/'l0'), manifest_path=str(tmp_path/'nxt.json'), candidates_path=str(tmp_path/'candidates.json'), venue='nxt', ntp_host='x', max_clock_offset_ns=2, max_cycles=1, degraded_reason=None)
    with pytest.raises(SlotBudgetExceededError, match='capacity'):
        asyncio.run(collect_aftermarket._run_stream(args))
