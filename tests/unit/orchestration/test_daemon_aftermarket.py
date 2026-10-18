"""Daemon aftermarket cases split from test_daemon.py (aftermarket reselection, shard planning, and NXT/KRX session startup)."""

from __future__ import annotations

import datetime as _dt
import logging

import pytest
from tests.unit.orchestration.daemon_fixtures import _business_trading_day, _holiday_trading_day

_ARM_DAY = _dt.date(2026, 9, 16)


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


def _arm_plan():
    from src.realtime.contracts import MarketVenue
    from src.realtime.kis_sharding import AftermarketShard

    return (
        AftermarketShard(MarketVenue.NXT, 0, ("005930",), ("H0NXCNT0", "H0NXASP0"), "0", "id0"),
        AftermarketShard(MarketVenue.KRX, 0, ("000660",), ("H0STCNT0", "H0STASP0"), "1", "id1"),
    )


def _arm_candidate_snapshot(day=None):
    import datetime as dt
    from zoneinfo import ZoneInfo

    from src.universe.ipc import CandidateSnapshot

    session_date = day if day is not None else _ARM_DAY
    stamp = dt.datetime.combine(session_date, dt.time(15, 31), tzinfo=ZoneInfo("Asia/Seoul"))
    return CandidateSnapshot(
        schema_version=1,
        rev=int(session_date.strftime("%Y%m%d")),
        session_date=session_date,
        session="aftermarket",
        generated_at=stamp,
        source_asof=stamp,
        effective_from=stamp,
        policy_version="aftermarket_v1",
        capacity=40,
        eligible_count=1,
        selected_count=1,
        candidates=(
            {
                "symbol": "005930",
                "rank": 1,
                "source_ranks": {"trade_amount": 1},
                "metrics": {"trade_value_krw": 1, "change_pct": 1.0},
                "selection_reasons": ["trade_amount"],
            },
        ),
    )


def _arm_daemon(
    tmp_path,
    monkeypatch,
    *,
    refresh_day=_ARM_DAY,
    candidate=True,
    enabled=True,
    holiday=False,
    refresh=None,
):
    import datetime as dt
    from zoneinfo import ZoneInfo

    from src.core.config import CollectorSettings, resolve_collector_runtime
    from src.orchestration import daemon as daemon_mod
    from src.universe.ipc import write_candidate_snapshot

    kst = ZoneInfo("Asia/Seoul")
    commands: list[list[str]] = []
    ensures: list[str] = []

    def _default_refresh(*, out_path, **_kwargs):
        snapshot = _arm_candidate_snapshot()
        write_candidate_snapshot(out_path, snapshot)
        return snapshot

    monkeypatch.setattr(daemon_mod, "_kis_token_preflight", lambda *a, **k: {})
    monkeypatch.setattr(daemon_mod, "_build_kis_client", lambda _paths: object())
    monkeypatch.setattr(daemon_mod, "load_kis_data_credentials", lambda: ())
    monkeypatch.setattr(daemon_mod, "plan_aftermarket_shards", lambda **_kw: _arm_plan())
    monkeypatch.setattr(daemon_mod, "run_session_orchestration", lambda **_kw: True)
    monkeypatch.setattr(daemon_mod, "refresh_aftermarket_candidates", refresh or _default_refresh)
    monkeypatch.setattr(
        daemon_mod,
        "_resolve_trading_day_with_cache",
        lambda day, cache, _a: _holiday_trading_day(day) if holiday else _business_trading_day(day),
    )

    class _Sup:
        def __init__(self, *, cmd, breaker=None):
            self.cmd = list(cmd)
            commands.append(self.cmd)

        def ensure_running(self):
            if "collect-aftermarket" in self.cmd:
                ensures.append(self.cmd[self.cmd.index("--venue") + 1])
            return "running"

        def stop(self, *, timeout_s=15.0):
            return "graceful"

    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _Sup)

    settings = CollectorSettings(
        data_root=tmp_path,
        after_market_enabled=enabled,
        universe_slot_budget=1,
        ls_capacity_pairs=2,
    )
    if candidate:
        write_candidate_snapshot(settings.paths.aftermarket_candidates(_ARM_DAY), _arm_candidate_snapshot())
    runtime = resolve_collector_runtime(collector=settings)
    holder = {"now": dt.datetime(2026, 9, 16, 15, 30, tzinfo=kst)}
    runner = daemon_mod.DaemonRunner(runtime=runtime, shutdown=None, now=lambda: holder["now"], sleep=lambda _: None)
    runner.state.aftermarket_refresh_day = refresh_day
    return runner, commands, ensures


def _arm_at(runner, hour, minute, second=0):
    import datetime as dt
    from zoneinfo import ZoneInfo

    now = dt.datetime(2026, 9, 16, hour, minute, second, tzinfo=ZoneInfo("Asia/Seoul"))
    runner._now = lambda: now  # type: ignore[method-assign]
    return runner.step(now)


def _installed_venues(commands) -> list[str]:
    return [cmd[cmd.index("--venue") + 1] for cmd in commands if "collect-aftermarket" in cmd]


def test_aftermarket_arm_at_standard_day() -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo

    from src.core.session_anchors import standard_session_anchors
    from src.orchestration import daemon
    from src.realtime.contracts import MarketVenue

    kst = ZoneInfo("Asia/Seoul")
    anchors = standard_session_anchors(_ARM_DAY)
    assert daemon.aftermarket_arm_at(
        _ARM_DAY, MarketVenue.NXT, anchors=anchors, prearm_lead_s=60.0
    ) == dt.datetime(2026, 9, 16, 15, 39, 0, tzinfo=kst)
    assert daemon.aftermarket_arm_at(
        _ARM_DAY, MarketVenue.KRX, anchors=anchors, prearm_lead_s=60.0
    ) == dt.datetime(2026, 9, 16, 15, 59, 0, tzinfo=kst)


def test_aftermarket_arm_at_shifted_day(tmp_path) -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo

    from src.core.paths import DataPaths
    from src.core.session_anchors import AnchorSource, SessionAnchors, resolve_session_anchors, save_session_anchors
    from src.orchestration import daemon
    from src.realtime.contracts import MarketVenue

    kst = ZoneInfo("Asia/Seoul")
    day = dt.date(2026, 11, 19)
    paths = DataPaths(tmp_path)
    save_session_anchors(
        paths.session_calendar_dir,
        SessionAnchors(
            date=day,
            regular_open=dt.time(10, 0),
            closing_auction_start=dt.time(16, 20),
            regular_close=dt.time(16, 30),
            after_market_end=dt.time(20, 0),
            source=AnchorSource.VENDOR,
        ),
    )
    anchors = resolve_session_anchors(paths.session_calendar_dir, day)
    assert daemon.aftermarket_arm_at(day, MarketVenue.NXT, anchors=anchors, prearm_lead_s=60.0) == dt.datetime(
        2026, 11, 19, 16, 39, 0, tzinfo=kst
    )
    assert daemon.aftermarket_arm_at(day, MarketVenue.KRX, anchors=anchors, prearm_lead_s=60.0) == dt.datetime(
        2026, 11, 19, 16, 59, 0, tzinfo=kst
    )


def test_aftermarket_arm_at_rejects_non_positive_lead() -> None:
    import pytest

    from src.core.session_anchors import standard_session_anchors
    from src.orchestration import daemon
    from src.realtime.contracts import MarketVenue

    anchors = standard_session_anchors(_ARM_DAY)
    with pytest.raises(ValueError, match="prearm_lead_s must be positive"):
        daemon.aftermarket_arm_at(_ARM_DAY, MarketVenue.NXT, anchors=anchors, prearm_lead_s=0.0)


def test_nxt_shard_launches_inside_full_active_before_open(tmp_path, monkeypatch) -> None:
    from src.core.calendar import SessionState

    runner, commands, _ = _arm_daemon(tmp_path, monkeypatch, refresh_day=_ARM_DAY)

    _arm_at(runner, 15, 38, 55)
    assert runner.children.aftermarket == {}
    assert runner.state.aftermarket_plan_day is None

    _arm_at(runner, 15, 39, 5)
    assert sorted(runner.children.aftermarket) == ["nxt:0"]
    assert runner.state.prev_state == SessionState.FULL_ACTIVE
    assert _installed_venues(commands) == ["nxt"]


def test_krx_shard_launches_before_krx_open(tmp_path, monkeypatch) -> None:
    runner, commands, _ = _arm_daemon(tmp_path, monkeypatch, refresh_day=_ARM_DAY)

    _arm_at(runner, 15, 58, 55)
    assert sorted(runner.children.aftermarket) == ["nxt:0"]

    _arm_at(runner, 15, 59, 5)
    assert sorted(runner.children.aftermarket) == ["krx:0", "nxt:0"]
    assert _installed_venues(commands) == ["nxt", "krx"]


def test_shard_start_precedes_open_for_every_venue(tmp_path, monkeypatch) -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo

    from src.core.session_anchors import standard_session_anchors
    from src.orchestration import daemon
    from src.realtime.contracts import MarketVenue

    kst = ZoneInfo("Asia/Seoul")
    runner, _commands, _ = _arm_daemon(tmp_path, monkeypatch, refresh_day=_ARM_DAY)
    anchors = standard_session_anchors(_ARM_DAY)
    install_at: dict[str, dt.datetime] = {}

    instant = dt.datetime(2026, 9, 16, 15, 30, 0, tzinfo=kst)
    end = dt.datetime(2026, 9, 16, 16, 5, 0, tzinfo=kst)
    while instant <= end:
        before = set(runner.children.aftermarket)
        _arm_at(runner, instant.hour, instant.minute, instant.second)
        for key in set(runner.children.aftermarket) - before:
            install_at[key] = instant
        instant += dt.timedelta(seconds=10)

    assert set(install_at) == {"nxt:0", "krx:0"}
    assert daemon.aftermarket_arm_at(_ARM_DAY, MarketVenue.NXT, anchors=anchors, prearm_lead_s=60.0) <= install_at[
        "nxt:0"
    ] < dt.datetime(2026, 9, 16, 15, 40, 0, tzinfo=kst)
    assert daemon.aftermarket_arm_at(_ARM_DAY, MarketVenue.KRX, anchors=anchors, prearm_lead_s=60.0) <= install_at[
        "krx:0"
    ] < dt.datetime(2026, 9, 16, 16, 0, 0, tzinfo=kst)


def test_shifted_day_does_not_arm_at_standard_times(tmp_path, monkeypatch) -> None:
    import datetime as dt

    from src.core.config import CollectorSettings, resolve_collector_runtime
    from src.core.paths import DataPaths
    from src.core.session_anchors import AnchorSource, SessionAnchors, save_session_anchors
    from src.orchestration import daemon as daemon_mod
    from tests.unit.orchestration.daemon_fixtures import _business_trading_day

    day = dt.date(2026, 11, 19)
    settings = CollectorSettings(
        data_root=tmp_path, after_market_enabled=True, universe_slot_budget=1, ls_capacity_pairs=2
    )
    save_session_anchors(
        DataPaths(tmp_path).session_calendar_dir,
        SessionAnchors(
            date=day,
            regular_open=dt.time(10, 0),
            closing_auction_start=dt.time(16, 20),
            regular_close=dt.time(16, 30),
            after_market_end=dt.time(20, 0),
            source=AnchorSource.VENDOR,
        ),
    )
    commands: list[list[str]] = []

    class _Sup:
        def __init__(self, *, cmd, breaker=None):
            commands.append(list(cmd))

        def ensure_running(self):
            return "running"

        def stop(self, *, timeout_s=15.0):
            return "graceful"

    monkeypatch.setattr(daemon_mod, "_kis_token_preflight", lambda *a, **k: {})
    monkeypatch.setattr(daemon_mod, "plan_aftermarket_shards", lambda **_kw: _arm_plan())
    monkeypatch.setattr(daemon_mod, "load_kis_data_credentials", lambda: ())
    monkeypatch.setattr(daemon_mod, "run_session_orchestration", lambda **_kw: True)
    monkeypatch.setattr(daemon_mod, "_resolve_trading_day_with_cache", lambda d, c, _a: _business_trading_day(d))
    monkeypatch.setattr(daemon_mod, "ProcessSupervisor", _Sup)
    from src.universe.ipc import write_candidate_snapshot

    write_candidate_snapshot(settings.paths.aftermarket_candidates(day), _arm_candidate_snapshot(day))
    runtime = resolve_collector_runtime(collector=settings)
    runner = daemon_mod.DaemonRunner(runtime=runtime, shutdown=None, now=lambda: None, sleep=lambda _: None)
    runner.state.aftermarket_refresh_day = day

    for hour, minute, second in ((15, 39, 5), (15, 59, 5)):
        now = dt.datetime(2026, 11, 19, hour, minute, second, tzinfo=dt.timezone(dt.timedelta(hours=9)))
        runner._now = lambda now=now: now  # type: ignore[method-assign]
        runner.step(now)
    assert runner.children.aftermarket == {}

    _arm_at_shifted(runner, dt.datetime(2026, 11, 19, 16, 39, 5, tzinfo=dt.timezone(dt.timedelta(hours=9))))
    assert sorted(runner.children.aftermarket) == ["nxt:0"]


def _arm_at_shifted(runner, now) -> None:
    runner._now = lambda: now  # type: ignore[method-assign]
    runner.step(now)


def test_pending_reselection_defers_plan_without_latching(tmp_path, monkeypatch) -> None:
    import datetime as dt

    from src.execution.contracts import KisApiError
    from src.universe.ipc import write_candidate_snapshot

    attempts = []

    def refresh(*, out_path, generated_at, **_kwargs):
        attempts.append(generated_at)
        if len(attempts) == 1:
            raise KisApiError("SCHEMA", "temporary ranking failure")
        write_candidate_snapshot(out_path, _arm_candidate_snapshot())

    runner, commands, _ = _arm_daemon(
        tmp_path,
        monkeypatch,
        refresh_day=None,
        candidate=False,
        refresh=refresh,
    )
    _arm_at(runner, 15, 38, 30)
    assert len(attempts) == 1
    assert runner.state.next_aftermarket_refresh_at == attempts[0] + dt.timedelta(seconds=60)

    _arm_at(runner, 15, 39, 5)
    assert runner.state.aftermarket_plan_day is None
    assert runner.children.aftermarket == {}
    assert _installed_venues(commands) == []
    assert len(attempts) == 1

    _arm_at(runner, 15, 39, 35)
    assert len(attempts) == 2
    assert runner.state.aftermarket_plan_day == _ARM_DAY
    assert sorted(runner.children.aftermarket) == ["nxt:0"]


def test_reselection_failing_through_open_keeps_existing_fail_path(tmp_path, monkeypatch, caplog) -> None:
    from src.execution.contracts import KisApiError

    def fail(**_kwargs):
        raise KisApiError("SCHEMA", "ranking unavailable")

    runner, commands, _ = _arm_daemon(tmp_path, monkeypatch, refresh_day=None, candidate=False, refresh=fail)

    with caplog.at_level(logging.CRITICAL):
        _arm_at(runner, 15, 38, 30)
        _arm_at(runner, 15, 39, 35)
        assert runner.state.aftermarket_plan_day is None
        _arm_at(runner, 15, 40, 5)

    assert runner.children.aftermarket == {}
    assert _installed_venues(commands) == []
    criticals = [r.getMessage() for r in caplog.records if r.levelno == logging.CRITICAL]
    assert any("stage=aftermarket_plan status=FAIL" in msg for msg in criticals)
    assert runner.state.aftermarket_plan == ()


def test_restart_after_nxt_open_arms_remaining_venue_on_time(tmp_path, monkeypatch, caplog) -> None:
    runner, _commands, _ = _arm_daemon(tmp_path, monkeypatch, refresh_day=None)

    with caplog.at_level(logging.INFO):
        _arm_at(runner, 15, 50, 0)
    armed = [r.getMessage() for r in caplog.records if "stage=aftermarket_stream status=ARMED" in r.getMessage()]
    assert armed == ["[DAEMON] stage=aftermarket_stream status=ARMED key=nxt:0 open=15:40:00 lead_s=-600.0"]
    assert sorted(runner.children.aftermarket) == ["nxt:0"]

    _arm_at(runner, 15, 59, 5)
    assert sorted(runner.children.aftermarket) == ["krx:0", "nxt:0"]


def test_repeated_cycles_are_idempotent(tmp_path, monkeypatch, caplog) -> None:
    runner, _commands, ensures = _arm_daemon(tmp_path, monkeypatch, refresh_day=_ARM_DAY)

    with caplog.at_level(logging.INFO):
        _arm_at(runner, 15, 59, 5)
        for second in (5, 15, 25, 35, 45):
            _arm_at(runner, 16, 0, second)

    armed = [r.getMessage() for r in caplog.records if "stage=aftermarket_stream status=ARMED" in r.getMessage()]
    assert len(armed) == 2
    assert len(set(armed)) == 2
    replace_live = [
        r.getMessage()
        for r in caplog.records
        if r.levelno == logging.CRITICAL and "REPLACE_LIVE" in r.getMessage()
    ]
    assert replace_live == []
    assert ensures.count("nxt") == 6
    assert ensures.count("krx") == 6


def test_holiday_never_arms(tmp_path, monkeypatch, caplog) -> None:
    runner, commands, _ = _arm_daemon(tmp_path, monkeypatch, refresh_day=None, holiday=True)

    with caplog.at_level(logging.CRITICAL):
        _arm_at(runner, 15, 39, 5)

    assert runner.children.aftermarket == {}
    assert _installed_venues(commands) == []
    assert runner.state.aftermarket_plan_day is None
    assert not any("stage=aftermarket_plan" in r.getMessage() for r in caplog.records)


def test_aftermarket_disabled_never_arms(tmp_path, monkeypatch, caplog) -> None:
    runner, commands, _ = _arm_daemon(tmp_path, monkeypatch, refresh_day=_ARM_DAY, enabled=False)

    with caplog.at_level(logging.INFO):
        _arm_at(runner, 15, 39, 5)

    assert runner.children.aftermarket == {}
    assert _installed_venues(commands) == []
    assert runner.state.aftermarket_plan_day is None
    assert not any("stage=aftermarket_stream status=ARMED" in r.getMessage() for r in caplog.records)


def test_pending_reselection_never_installs_previous_day_plan(tmp_path, monkeypatch) -> None:
    from src.execution.contracts import KisApiError
    from src.universe.ipc import write_candidate_snapshot

    attempts = []

    def refresh(*, out_path, generated_at, **_kwargs):
        attempts.append(generated_at)
        if len(attempts) == 1:
            raise KisApiError("SCHEMA", "temporary ranking failure")
        write_candidate_snapshot(out_path, _arm_candidate_snapshot())

    runner, commands, _ = _arm_daemon(
        tmp_path, monkeypatch, refresh_day=None, candidate=False, refresh=refresh
    )
    yesterday = _ARM_DAY - _dt.timedelta(days=1)
    runner.state.aftermarket_plan_day = yesterday
    runner.state.aftermarket_plan = _arm_plan()

    _arm_at(runner, 15, 38, 30)
    _arm_at(runner, 15, 39, 5)
    assert runner.state.aftermarket_plan_day == yesterday
    assert _installed_venues(commands) == []

    _arm_at(runner, 15, 39, 35)
    assert runner.state.aftermarket_plan_day == _ARM_DAY
    assert _installed_venues(commands) == ["nxt"]


@pytest.mark.parametrize("minute", [38, 39])
def test_existing_aftermarket_children_supervised_while_plan_gate_closed(tmp_path, monkeypatch, minute) -> None:
    from src.core.calendar import SessionState
    from src.core.session_anchors import standard_session_anchors

    runner, commands, _ = _arm_daemon(tmp_path, monkeypatch, refresh_day=None, candidate=False)
    runner.children.aftermarket["nxt:0"] = _ScriptedSupervisor(["restarted"])
    now = _dt.datetime.combine(
        _ARM_DAY, _dt.time(15, minute, 5), tzinfo=_dt.timezone(_dt.timedelta(hours=9))
    )
    runner._supervise_aftermarket(now, SessionState.FULL_ACTIVE, _ARM_DAY, standard_session_anchors(_ARM_DAY))

    assert runner.state.aftermarket_results == {"nxt:0": "restarted"}
    assert runner.state.aftermarket_restarts == 1
    assert runner.state.aftermarket_plan_day is None
    assert _installed_venues(commands) == []
