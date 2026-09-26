"""Daemon aftermarket cases split from test_daemon.py (aftermarket reselection, shard planning, and NXT/KRX session startup)."""

from __future__ import annotations

import logging

import pytest
from tests.unit.orchestration.daemon_fixtures import _business_trading_day, _holiday_trading_day


@pytest.fixture(autouse=True)
def _verified_aftermarket_capacity(monkeypatch) -> None:
    """Daemon tests simulate a deployed host with verified aftermarket capacity."""
    monkeypatch.setenv("KRX_ALPHA_AFTERMARKET_PAIR_CAPACITY_PER_CONNECTION", "4")


@pytest.fixture(autouse=True)


def _verified_aftermarket_capacity(monkeypatch) -> None:
    """Daemon tests simulate a deployed host with verified aftermarket capacity."""
    monkeypatch.setenv("KRX_ALPHA_AFTERMARKET_PAIR_CAPACITY_PER_CONNECTION", "4")


def test_after_market_active_keeps_streamer_and_defers_eod(tmp_path, monkeypatch):
    import datetime as dt
    import pathlib
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data", after_market_enabled=True)
    monkeypatch.setattr(daemon_mod, "resolve_trading_day", lambda ref_date: None)
    monkeypatch.setattr(daemon_mod, "run_session_orchestration", lambda **kwargs: True)
    monkeypatch.setattr(daemon_mod, "plan_aftermarket_shards", lambda **kwargs: ())
    eod_maintenance = MagicMock(return_value=0)
    monkeypatch.setattr(daemon_mod, "run_eod_maintenance", eod_maintenance)
    calls: list[str] = []

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            calls.append("constructed")

        def ensure_running(self):
            calls.append("ensure_running")
            return "started"

    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)
    kst = ZoneInfo("Asia/Seoul")
    times = iter([
        dt.datetime(2026, 9, 10, 9, 0, tzinfo=kst),
        dt.datetime(2026, 9, 10, 18, 0, tzinfo=kst),
    ])

    daemon_mod.run_collector_daemon(
        settings=settings,
        sleep_fn=lambda _: None,
        max_cycles=2,
        now_fn=lambda: next(times),
    )

    assert calls.count("constructed") >= 1
    assert calls.count("ensure_running") >= 2
    eod_maintenance.assert_not_called()


def test_daemon_starts_nxt_then_krx_without_stopping_ls(monkeypatch, tmp_path):
    import datetime as dt
    from zoneinfo import ZoneInfo
    from src.core.config import CollectorSettings
    from src.realtime.contracts import MarketVenue
    from src.realtime.kis_sharding import AftermarketShard
    import src.orchestration.daemon as daemon
    commands = []
    class Supervisor:
        def __init__(self, cmd, breaker): commands.append(cmd)
        def ensure_running(self): return 'running'
        def stop(self, timeout_s): return 'stopped'
    plan = tuple(AftermarketShard(venue, index, ('000001',), streams, str(index), f'id{index}') for venue, streams, index in ((MarketVenue.NXT,('H0NXCNT0','H0NXASP0'),0),(MarketVenue.NXT,('H0NXCNT0','H0NXASP0'),1),(MarketVenue.KRX,('H0STCNT0','H0STASP0'),2),(MarketVenue.KRX,('H0STCNT0','H0STASP0'),3)))
    times = iter([dt.datetime(2026,9,15,15,40,tzinfo=ZoneInfo('Asia/Seoul')), dt.datetime(2026,9,15,16,0,tzinfo=ZoneInfo('Asia/Seoul'))])
    monkeypatch.setattr(daemon, 'plan_aftermarket_shards', lambda **_: plan)
    monkeypatch.setattr(daemon, 'ProcessSupervisor', Supervisor)
    monkeypatch.setattr(daemon, 'run_session_orchestration', lambda **_: True)
    monkeypatch.setattr(daemon, '_resolve_trading_day_with_cache', lambda *_: None)
    cfg = CollectorSettings(data_root=tmp_path, after_market_enabled=True, universe_slot_budget=1, ls_capacity_pairs=2)
    from src.universe.ipc import CandidateSnapshot as _CS, write_candidate_snapshot as _wcs
    import datetime as _dt
    from zoneinfo import ZoneInfo as _ZI
    _stamp = _dt.datetime(2026, 9, 15, 15, 31, tzinfo=_ZI('Asia/Seoul'))
    _wcs(cfg.paths.aftermarket_candidates(_dt.date(2026, 9, 15)), _CS(schema_version=1, rev=20260915, session_date=_dt.date(2026, 9, 15), session='aftermarket', generated_at=_stamp, source_asof=_stamp, effective_from=_stamp, policy_version='aftermarket_v1', capacity=40, eligible_count=1, selected_count=1, candidates=({'symbol': '000001', 'rank': 1, 'source_ranks': {'trade_amount': 1}, 'metrics': {'trade_value_krw': 1, 'change_pct': 1.0}, 'selection_reasons': ['trade_amount']},)))
    daemon.run_collector_daemon(settings=cfg, now_fn=lambda: next(times), sleep_fn=lambda _: None, max_cycles=2)
    assert sum('collect-aftermarket' in cmd for cmd in commands) == 4
    venues = [cmd[cmd.index('--venue') + 1] for cmd in commands]
    assert venues == ['nxt', 'nxt', 'krx', 'krx']


def test_daemon_starts_four_sharded_aftermarket_commands(monkeypatch, tmp_path) -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo
    from src.core.config import CollectorSettings
    from src.orchestration import daemon
    from src.realtime.contracts import MarketVenue
    from src.realtime.kis_sharding import AftermarketShard
    plan = tuple(AftermarketShard(venue, index, ('000001',), streams, str(index), f'id{index}') for venue, streams, index in ((MarketVenue.NXT,('H0NXCNT0','H0NXASP0'),0),(MarketVenue.NXT,('H0NXCNT0','H0NXASP0'),1),(MarketVenue.KRX,('H0STCNT0','H0STASP0'),2),(MarketVenue.KRX,('H0STCNT0','H0STASP0'),3)))
    commands=[]
    class Sup:
        def __init__(self, *, cmd, breaker): commands.append(cmd)
        def ensure_running(self): return 'running'
        def stop(self, timeout_s): return 'stopped'
    monkeypatch.setattr(daemon, 'plan_aftermarket_shards', lambda **_: plan)
    monkeypatch.setattr(daemon, 'ProcessSupervisor', Sup)
    monkeypatch.setattr(daemon, 'run_session_orchestration', lambda **_: True)
    monkeypatch.setattr(daemon, '_resolve_trading_day_with_cache', lambda *_: None)
    times=iter([dt.datetime(2026,9,15,15,40,tzinfo=ZoneInfo('Asia/Seoul')),dt.datetime(2026,9,15,16,0,tzinfo=ZoneInfo('Asia/Seoul'))])
    _cfg2=CollectorSettings(data_root=tmp_path,after_market_enabled=True,universe_slot_budget=1,ls_capacity_pairs=2)
    from src.universe.ipc import CandidateSnapshot as _CS2, write_candidate_snapshot as _wcs2
    _stamp2=dt.datetime(2026,9,15,15,31,tzinfo=ZoneInfo('Asia/Seoul'))
    _wcs2(_cfg2.paths.aftermarket_candidates(dt.date(2026,9,15)),_CS2(schema_version=1,rev=20260915,session_date=dt.date(2026,9,15),session='aftermarket',generated_at=_stamp2,source_asof=_stamp2,effective_from=_stamp2,policy_version='aftermarket_v1',capacity=40,eligible_count=1,selected_count=1,candidates=({'symbol':'000001','rank':1,'source_ranks':{'trade_amount':1},'metrics':{'trade_value_krw':1,'change_pct':1.0},'selection_reasons':['trade_amount']},)))
    daemon.run_collector_daemon(settings=_cfg2,now_fn=lambda:next(times),sleep_fn=lambda _:None,max_cycles=2)
    assert len(commands) == 4
    assert [x[x.index('--credential-slot')+1] for x in commands] == ['0','1','2','3']


def test_daemon_refreshes_isolated_aftermarket_snapshot_before_nxt_start(monkeypatch, tmp_path) -> None:
    import datetime as dt
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon
    from src.realtime.contracts import MarketVenue
    from src.realtime.kis_sharding import AftermarketShard
    from src.universe.ipc import CandidateSnapshot, write_candidate_snapshot

    kst = ZoneInfo('Asia/Seoul')
    settings = CollectorSettings(data_root=tmp_path, after_market_enabled=True, universe_slot_budget=1, ls_capacity_pairs=2)
    calls: list[list[str]] = []
    refreshed: list[dict[str, object]] = []

    def refresh(**kwargs):
        refreshed.append(kwargs)
        stamp = kwargs['generated_at']
        snapshot = CandidateSnapshot(
            schema_version=1, rev=20260916, session_date=kwargs['session_date'], session='aftermarket',
            generated_at=stamp, source_asof=stamp, effective_from=stamp, policy_version='aftermarket_v1',
            capacity=40, eligible_count=1, selected_count=1,
            candidates=({'symbol': '005930', 'rank': 1, 'source_ranks': {'trade_amount': 1}, 'metrics': {'trade_value_krw': 1, 'change_pct': 1.0}, 'selection_reasons': ['trade_amount']},),
        )
        write_candidate_snapshot(kwargs['out_path'], snapshot)
        return snapshot

    class Supervisor:
        def __init__(self, *, cmd, breaker):
            calls.append(cmd)
        def ensure_running(self):
            return 'running'
        def stop(self, *, timeout_s=15.0):
            return 'stopped'

    plan = (AftermarketShard(MarketVenue.NXT, 0, ('005930',), ('H0NXCNT0', 'H0NXASP0'), '1', 'id1'),)
    monkeypatch.setattr(daemon, 'refresh_aftermarket_candidates', refresh)
    monkeypatch.setattr(daemon, '_build_kis_client', lambda _: object())
    monkeypatch.setattr(daemon, 'plan_aftermarket_shards', lambda **_: plan)
    monkeypatch.setattr(daemon, 'load_kis_data_credentials', lambda: ())
    monkeypatch.setattr(daemon, 'run_session_orchestration', lambda **_: True)
    monkeypatch.setattr(daemon, '_resolve_trading_day_with_cache', lambda *_: None)
    monkeypatch.setattr(daemon, 'ProcessSupervisor', Supervisor)
    times = iter([dt.datetime(2026, 9, 16, 15, 31, tzinfo=kst), dt.datetime(2026, 9, 16, 15, 40, tzinfo=kst)])

    daemon.run_collector_daemon(settings=settings, now_fn=lambda: next(times), sleep_fn=MagicMock(), max_cycles=2)

    assert len(refreshed) == 1
    assert refreshed[0]['out_path'] == settings.paths.aftermarket_candidates(dt.date(2026, 9, 16))
    aftermarket = [cmd for cmd in calls if 'collect-aftermarket' in cmd]
    assert len(aftermarket) == 1
    assert aftermarket[0][aftermarket[0].index('--candidates-path') + 1] == str(settings.paths.aftermarket_candidates(dt.date(2026, 9, 16)))


def test_daemon_blocks_aftermarket_when_reselection_fails(monkeypatch, tmp_path, caplog) -> None:
    import datetime as dt
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.execution.contracts import KisApiError
    from src.orchestration import daemon

    settings = CollectorSettings(data_root=tmp_path, after_market_enabled=True, universe_slot_budget=1, ls_capacity_pairs=2)
    commands: list[list[str]] = []

    class Supervisor:
        def __init__(self, *, cmd, breaker):
            commands.append(cmd)
        def ensure_running(self):
            return 'running'
        def stop(self, *, timeout_s=15.0):
            return 'stopped'

    def fail(**kwargs):
        raise KisApiError('SCHEMA', 'bad ranking')

    monkeypatch.setattr(daemon, 'refresh_aftermarket_candidates', fail)
    monkeypatch.setattr(daemon, '_build_kis_client', lambda _: object())
    monkeypatch.setattr(daemon, 'run_session_orchestration', lambda **_: True)
    monkeypatch.setattr(daemon, '_resolve_trading_day_with_cache', lambda *_: None)
    monkeypatch.setattr(daemon, 'ProcessSupervisor', Supervisor)
    kst = ZoneInfo('Asia/Seoul')
    times = iter([dt.datetime(2026, 9, 16, 15, 31, tzinfo=kst), dt.datetime(2026, 9, 16, 15, 40, tzinfo=kst)])

    daemon.run_collector_daemon(settings=settings, now_fn=lambda: next(times), sleep_fn=MagicMock(), max_cycles=2)

    assert any('collect-stream' in cmd for cmd in commands)
    assert not any('collect-aftermarket' in cmd for cmd in commands)
    assert any('aftermarket' in record.getMessage().lower() for record in caplog.records)


def test_daemon_retries_aftermarket_reselection_on_failure(monkeypatch, tmp_path) -> None:
    import datetime as dt
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.execution.contracts import KisApiError
    from src.orchestration import daemon
    from src.realtime.contracts import MarketVenue
    from src.realtime.kis_sharding import AftermarketShard

    settings = CollectorSettings(data_root=tmp_path, after_market_enabled=True, universe_slot_budget=1, ls_capacity_pairs=2)
    commands: list[list[str]] = []

    class Supervisor:
        def __init__(self, *, cmd, breaker):
            commands.append(cmd)
        def ensure_running(self):
            return 'running'
        def stop(self, *, timeout_s=15.0):
            return 'stopped'

    attempts = 0
    refreshed: list[dict] = []

    def mock_refresh(**kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise KisApiError('SCHEMA', 'temporary ranking schema failure')
        refreshed.append(kwargs)
        # Write dummy snapshot
        from src.universe.ipc import CandidateSnapshot, write_candidate_snapshot
        write_candidate_snapshot(
            kwargs['out_path'],
            CandidateSnapshot(
                schema_version=1,
                rev=20260916,
                session_date=kwargs['session_date'],
                session='aftermarket',
                generated_at=kwargs['generated_at'],
                source_asof=kwargs['generated_at'],
                effective_from=kwargs['generated_at'],
                policy_version='aftermarket_v1',
                capacity=40,
                eligible_count=1,
                selected_count=1,
                candidates=({'symbol': '005930', 'rank': 1, 'source_ranks': {'trade_amount': 1}, 'metrics': {'trade_value_krw': 1, 'change_pct': 1.0}, 'selection_reasons': ['trade_amount']},),
            ),
        )

    plan = (AftermarketShard(MarketVenue.NXT, 0, ('005930',), ('H0NXCNT0', 'H0NXASP0'), '1', 'id1'),)
    monkeypatch.setattr(daemon, 'refresh_aftermarket_candidates', mock_refresh)
    monkeypatch.setattr(daemon, '_build_kis_client', lambda _: object())
    monkeypatch.setattr(daemon, 'plan_aftermarket_shards', lambda **_: plan)
    monkeypatch.setattr(daemon, 'load_kis_data_credentials', lambda: ())
    monkeypatch.setattr(daemon, 'run_session_orchestration', lambda **_: True)
    monkeypatch.setattr(daemon, '_resolve_trading_day_with_cache', lambda *_: None)
    monkeypatch.setattr(daemon, 'ProcessSupervisor', Supervisor)

    kst = ZoneInfo('Asia/Seoul')
    times = iter([
        dt.datetime(2026, 9, 16, 15, 31, 0, tzinfo=kst),  # 1st attempt: fails
        dt.datetime(2026, 9, 16, 15, 31, 30, tzinfo=kst), # skipped: cooldown
        dt.datetime(2026, 9, 16, 15, 32, 5, tzinfo=kst),  # 2nd attempt: succeeds
        dt.datetime(2026, 9, 16, 15, 40, 0, tzinfo=kst),  # aftermarket starts
    ])

    daemon.run_collector_daemon(settings=settings, now_fn=lambda: next(times), sleep_fn=MagicMock(), max_cycles=4)

    assert attempts == 2
    assert len(refreshed) == 1
    aftermarket = [cmd for cmd in commands if 'collect-aftermarket' in cmd]
    assert len(aftermarket) == 1


def test_daemon_reselection_passes_classification_exclusions(monkeypatch, tmp_path) -> None:
    import datetime as dt
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo

    import polars as pl

    from src.core.config import CollectorSettings
    from src.execution.kis_client import KisRankingRow
    from src.orchestration import daemon
    from src.universe.ipc import read_candidate_snapshot

    settings = CollectorSettings(data_root=tmp_path, after_market_enabled=True, universe_slot_budget=1, ls_capacity_pairs=2)
    settings.paths.bars_daily_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({
        "date": [dt.date(2026, 9, 16)],
        "symbol": ["005935"],
        "stock_cert_kind": ["구형우선주"],
        "section": [None],
    }).write_parquet(settings.paths.bars_daily_dir / "2026-09.parquet")

    class FakeClient:
        def get_trade_amount_ranking(self):
            return (
                KisRankingRow(symbol='005935', rank=1, change_pct=1.0, trade_value_krw=100),
                KisRankingRow(symbol='005930', rank=2, change_pct=1.0, trade_value_krw=90),
            )

        def get_fluctuation_ranking(self):
            return (
                KisRankingRow(symbol='005935', rank=1, change_pct=1.0, trade_value_krw=100),
                KisRankingRow(symbol='005930', rank=2, change_pct=1.0, trade_value_krw=90),
            )

    class Supervisor:
        def __init__(self, *, cmd, breaker):
            pass
        def ensure_running(self):
            return 'running'
        def stop(self, *, timeout_s=15.0):
            return 'stopped'

    monkeypatch.setattr(daemon, '_build_kis_client', lambda _: FakeClient())
    monkeypatch.setattr(daemon, 'run_session_orchestration', lambda **_: True)
    monkeypatch.setattr(daemon, '_resolve_trading_day_with_cache', lambda *_: None)
    monkeypatch.setattr(daemon, 'ProcessSupervisor', Supervisor)
    kst = ZoneInfo('Asia/Seoul')
    times = iter([dt.datetime(2026, 9, 16, 15, 31, tzinfo=kst)])

    daemon.run_collector_daemon(settings=settings, now_fn=lambda: next(times), sleep_fn=MagicMock(), max_cycles=1)

    snapshot = read_candidate_snapshot(
        settings.paths.aftermarket_candidates(dt.date(2026, 9, 16)),
        expected_session_date=dt.date(2026, 9, 16), expected_session='aftermarket', max_candidates=40,
    )
    assert [row['symbol'] for row in snapshot.candidates] == ['005930']


def test_daemon_reselection_degrades_once_without_bar_store(monkeypatch, tmp_path, caplog) -> None:
    import datetime as dt
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.execution.contracts import KisApiError
    from src.execution.kis_client import KisRankingRow
    from src.orchestration import daemon

    settings = CollectorSettings(data_root=tmp_path, after_market_enabled=True, universe_slot_budget=1, ls_capacity_pairs=2)

    class FakeClient:
        def get_trade_amount_ranking(self):
            return (KisRankingRow(symbol='005930', rank=1, change_pct=1.0, trade_value_krw=100),)

        def get_fluctuation_ranking(self):
            return (KisRankingRow(symbol='005930', rank=1, change_pct=1.0, trade_value_krw=100),)

    attempts = 0

    def fail_once(**kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise KisApiError('SCHEMA', 'temporary failure')
        from src.universe import aftermarket as aftermarket_mod
        return aftermarket_mod.refresh_aftermarket_candidates(**kwargs)

    class Supervisor:
        def __init__(self, *, cmd, breaker):
            pass
        def ensure_running(self):
            return 'running'
        def stop(self, *, timeout_s=15.0):
            return 'stopped'

    monkeypatch.setattr(daemon, '_build_kis_client', lambda _: FakeClient())
    monkeypatch.setattr(daemon, 'refresh_aftermarket_candidates', fail_once)
    monkeypatch.setattr(daemon, 'run_session_orchestration', lambda **_: True)
    monkeypatch.setattr(daemon, '_resolve_trading_day_with_cache', lambda *_: None)
    monkeypatch.setattr(daemon, 'ProcessSupervisor', Supervisor)
    kst = ZoneInfo('Asia/Seoul')
    times = iter([
        dt.datetime(2026, 9, 16, 15, 31, 0, tzinfo=kst),
        dt.datetime(2026, 9, 16, 15, 32, 5, tzinfo=kst),
    ])

    with caplog.at_level(logging.WARNING):
        daemon.run_collector_daemon(settings=settings, now_fn=lambda: next(times), sleep_fn=MagicMock(), max_cycles=2)

    assert attempts == 2
    assert settings.paths.aftermarket_candidates(dt.date(2026, 9, 16)).exists()
    degraded = [record for record in caplog.records if 'stage=aftermarket_eligibility' in record.getMessage()]
    assert len(degraded) == 1
    assert 'reason=no_bars_store' in degraded[0].getMessage()


def test_daemon_reselection_degrades_on_unreadable_bar_store(monkeypatch, tmp_path, caplog) -> None:
    import datetime as dt
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.execution.kis_client import KisRankingRow
    from src.orchestration import daemon

    settings = CollectorSettings(data_root=tmp_path, after_market_enabled=True, universe_slot_budget=1, ls_capacity_pairs=2)
    settings.paths.bars_daily_dir.mkdir(parents=True, exist_ok=True)
    (settings.paths.bars_daily_dir / "2026-09.parquet").write_bytes(b'\x00\x01broken-parquet')

    class FakeClient:
        def get_trade_amount_ranking(self):
            return (KisRankingRow(symbol='005930', rank=1, change_pct=1.0, trade_value_krw=100),)

        def get_fluctuation_ranking(self):
            return (KisRankingRow(symbol='005930', rank=1, change_pct=1.0, trade_value_krw=100),)

    class Supervisor:
        def __init__(self, *, cmd, breaker):
            pass
        def ensure_running(self):
            return 'running'
        def stop(self, *, timeout_s=15.0):
            return 'stopped'

    monkeypatch.setattr(daemon, '_build_kis_client', lambda _: FakeClient())
    monkeypatch.setattr(daemon, 'run_session_orchestration', lambda **_: True)
    monkeypatch.setattr(daemon, '_resolve_trading_day_with_cache', lambda *_: None)
    monkeypatch.setattr(daemon, 'ProcessSupervisor', Supervisor)
    kst = ZoneInfo('Asia/Seoul')
    times = iter([dt.datetime(2026, 9, 16, 15, 31, tzinfo=kst)])

    with caplog.at_level(logging.WARNING):
        daemon.run_collector_daemon(settings=settings, now_fn=lambda: next(times), sleep_fn=MagicMock(), max_cycles=1)

    assert settings.paths.aftermarket_candidates(dt.date(2026, 9, 16)).exists()
    degraded = [record for record in caplog.records if 'stage=aftermarket_eligibility' in record.getMessage()]
    assert len(degraded) == 1
    assert 'reason=bars_unreadable' in degraded[0].getMessage()


def test_daemon_restart_aftermarket_on_holiday_plans_nothing(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data", after_market_enabled=True)
    kst = ZoneInfo("Asia/Seoul")
    holiday = _holiday_trading_day(dt.date(2026, 9, 24))
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c, _a: holiday)

    constructed: list[list[str]] = []

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            constructed.append(cmd)

        def ensure_running(self):
            return "started"

        def stop(self, *, timeout_s=15.0):
            return "graceful"

    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)
    now = dt.datetime(2026, 9, 24, 19, 31, tzinfo=kst)
    with caplog.at_level(logging.INFO):
        daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=1, now_fn=lambda: now)
    assert not any("collect-aftermarket" in " ".join(cmd) for cmd in constructed)
    assert not any("stage=aftermarket_plan" in r.getMessage() for r in caplog.records if r.levelno == logging.CRITICAL)
    skips = [r for r in caplog.records if "reason=market_holiday" in r.getMessage()]
    assert len(skips) == 1
    assert skips[0].levelno == logging.INFO


def test_daemon_holiday_active_stops_aftermarket_supervisors(tmp_path, monkeypatch) -> None:
    import datetime as dt
    import pathlib
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.orchestration import daemon as daemon_mod
    from src.realtime.contracts import MarketVenue
    from src.realtime.kis_sharding import AftermarketShard
    from src.universe.ipc import CandidateSnapshot, write_candidate_snapshot

    monkeypatch.chdir(tmp_path)
    settings = CollectorSettings(data_root=pathlib.Path(tmp_path) / "data", after_market_enabled=True)
    kst = ZoneInfo("Asia/Seoul")
    business = _business_trading_day(dt.date(2026, 9, 14))
    holiday = _holiday_trading_day(dt.date(2026, 9, 15))

    def _resolve(day, cache, _anchors_dir):
        return business if day == dt.date(2026, 9, 14) else holiday

    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", _resolve)
    monkeypatch.setattr(daemon_mod, "run_session_orchestration", lambda **kw: True)
    plan = (AftermarketShard(MarketVenue.NXT, 0, ("000001",), ("H0NXCNT0", "H0NXASP0"), "0", "id0"),)
    monkeypatch.setattr(daemon_mod, "plan_aftermarket_shards", lambda **kw: plan)
    monkeypatch.setattr(daemon_mod, "load_kis_data_credentials", lambda: ())
    stamp = dt.datetime(2026, 9, 14, 15, 31, tzinfo=kst)
    write_candidate_snapshot(
        settings.paths.aftermarket_candidates(dt.date(2026, 9, 14)),
        CandidateSnapshot(schema_version=1, rev=20260914, session_date=dt.date(2026, 9, 14), session="aftermarket", generated_at=stamp, source_asof=stamp, effective_from=stamp, policy_version="aftermarket_v1", capacity=40, eligible_count=1, selected_count=1, candidates=({"symbol": "000001", "rank": 1, "source_ranks": {"trade_amount": 1}, "metrics": {"trade_value_krw": 1, "change_pct": 1.0}, "selection_reasons": ["trade_amount"]},)),
    )
    stops: list[list[str]] = []

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            self.cmd = cmd

        def ensure_running(self):
            return "started"

        def stop(self, *, timeout_s=15.0):
            stops.append(self.cmd)
            return "graceful"

    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _FakeSupervisor)
    times = iter([dt.datetime(2026, 9, 14, 15, 40, tzinfo=kst), dt.datetime(2026, 9, 15, 16, 30, tzinfo=kst)])
    daemon_mod.run_collector_daemon(settings=settings, sleep_fn=lambda s: None, max_cycles=2, now_fn=lambda: next(times))
    assert any("collect-aftermarket" in " ".join(cmd) for cmd in stops)




def _aftermarket_runner(tmp_path, monkeypatch):
    import datetime as dt
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings, resolve_collector_runtime
    from src.orchestration import daemon as daemon_mod

    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda day, cache, _a: None)
    settings = CollectorSettings(data_root=tmp_path, after_market_enabled=True)
    runtime = resolve_collector_runtime(collector=settings)
    now = dt.datetime(2026, 9, 16, 16, 30, tzinfo=ZoneInfo("Asia/Seoul"))
    runner = daemon_mod.DaemonRunner(runtime=runtime, shutdown=None, now=lambda: now, sleep=lambda _: None)
    runner.state.aftermarket_plan_day = dt.date(2026, 9, 16)
    runner.state.aftermarket_plan = ()
    return runner, now


class _ScriptedSupervisor:
    def __init__(self, results, *, exit_code=-9):
        self._results = list(results)
        self.last_exit_code = exit_code

    def ensure_running(self):
        if len(self._results) > 1:
            return self._results.pop(0)
        return self._results[0]

    def stop(self, *, timeout_s=15.0):
        return "graceful"


def test_aftermarket_restart_logged_and_counted(tmp_path, monkeypatch, caplog) -> None:
    import logging

    runner, now = _aftermarket_runner(tmp_path, monkeypatch)
    runner.children.aftermarket["nxt:0"] = _ScriptedSupervisor(["restarted"])

    with caplog.at_level(logging.WARNING):
        runner.step(now)

    assert runner.state.aftermarket_results == {"nxt:0": "restarted"}
    assert runner.state.aftermarket_restarts == 1
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert warnings.count("[DAEMON] stage=aftermarket_stream status=RESTARTED key=nxt:0 exit_code=-9") == 1


def test_aftermarket_circuit_open_alerted_once_per_key(tmp_path, monkeypatch, caplog) -> None:
    import logging

    runner, now = _aftermarket_runner(tmp_path, monkeypatch)
    runner.children.aftermarket["krx:2"] = _ScriptedSupervisor(["circuit_open"])

    with caplog.at_level(logging.CRITICAL):
        for _ in range(3):
            runner.step(now)

    criticals = [
        r.getMessage()
        for r in caplog.records
        if r.levelno == logging.CRITICAL and "stage=aftermarket_stream" in r.getMessage()
    ]
    assert criticals == ["[DAEMON] stage=aftermarket_stream status=FAIL reason=circuit_open key=krx:2"]
    assert runner.state.aftermarket_results == {"krx:2": "circuit_open"}


def test_daemon_reselection_degrades_on_monthless_bar_store(monkeypatch, tmp_path, caplog) -> None:
    import datetime as dt
    import logging
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings
    from src.execution.kis_client import KisRankingRow
    from src.orchestration import daemon

    settings = CollectorSettings(data_root=tmp_path, after_market_enabled=True, universe_slot_budget=1, ls_capacity_pairs=2)
    settings.paths.bars_daily_dir.mkdir(parents=True, exist_ok=True)
    (settings.paths.bars_daily_dir / "notes.parquet").write_bytes(b"x")

    class FakeClient:
        def get_trade_amount_ranking(self):
            return (KisRankingRow(symbol='005930', rank=1, change_pct=1.0, trade_value_krw=100),)

        def get_fluctuation_ranking(self):
            return (KisRankingRow(symbol='005930', rank=1, change_pct=1.0, trade_value_krw=100),)

    class Supervisor:
        def __init__(self, *, cmd, breaker):
            pass
        def ensure_running(self):
            return 'running'
        def stop(self, *, timeout_s=15.0):
            return 'stopped'

    monkeypatch.setattr(daemon, '_build_kis_client', lambda _: FakeClient())
    monkeypatch.setattr(daemon, 'run_session_orchestration', lambda **_: True)
    monkeypatch.setattr(daemon, '_resolve_trading_day_with_cache', lambda *_: None)
    monkeypatch.setattr(daemon, 'ProcessSupervisor', Supervisor)
    kst = ZoneInfo('Asia/Seoul')
    times = iter([dt.datetime(2026, 9, 16, 15, 31, tzinfo=kst)])

    with caplog.at_level(logging.WARNING):
        daemon.run_collector_daemon(settings=settings, now_fn=lambda: next(times), sleep_fn=MagicMock(), max_cycles=1)

    assert settings.paths.aftermarket_candidates(dt.date(2026, 9, 16)).exists()
    degraded = [record for record in caplog.records if 'stage=aftermarket_eligibility' in record.getMessage()]
    assert len(degraded) == 1
    assert 'reason=bars_unreadable' in degraded[0].getMessage()


def test_aftermarket_circuit_open_not_realerted_after_breaker_recovers(tmp_path, monkeypatch, caplog) -> None:
    import logging

    runner, now = _aftermarket_runner(tmp_path, monkeypatch)
    # 차단기가 열렸다가 30분 창이 지나 재시작 후 다시 열리는 같은 날의 반복
    runner.children.aftermarket["krx:2"] = _ScriptedSupervisor(
        ["circuit_open", "restarted", "running", "circuit_open"]
    )

    with caplog.at_level(logging.CRITICAL):
        for _ in range(4):
            runner.step(now)

    criticals = [
        r.getMessage()
        for r in caplog.records
        if r.levelno == logging.CRITICAL and "reason=circuit_open" in r.getMessage()
    ]
    assert criticals == ["[DAEMON] stage=aftermarket_stream status=FAIL reason=circuit_open key=krx:2"]
