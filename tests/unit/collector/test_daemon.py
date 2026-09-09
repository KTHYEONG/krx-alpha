"""Unit tests for Collector 24/7 Daemon scheduling logic."""

from __future__ import annotations


def test_get_target_state_weekend() -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo
    from src.collector.daemon import get_target_state

    kst = ZoneInfo('Asia/Seoul')
    # 2026-09-12 is Saturday
    saturday_noon = dt.datetime(2026, 9, 12, 12, 0, 0, tzinfo=kst)
    assert get_target_state(saturday_noon) == 'WEEKEND_SLEEP'
    # 2026-09-13 is Sunday
    sunday_morning = dt.datetime(2026, 9, 13, 9, 0, 0, tzinfo=kst)
    assert get_target_state(sunday_morning) == 'WEEKEND_SLEEP'


def test_get_target_state_weekday_schedule() -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo
    from src.collector.daemon import get_target_state

    kst = ZoneInfo('Asia/Seoul')
    # 2026-09-08 is Tuesday
    assert get_target_state(dt.datetime(2026, 9, 8, 7, 0, 0, tzinfo=kst)) == 'PRE_MARKET_SLEEP'
    assert get_target_state(dt.datetime(2026, 9, 8, 8, 25, 0, tzinfo=kst)) == 'STREAMER_ACTIVE'
    assert get_target_state(dt.datetime(2026, 9, 8, 9, 30, 0, tzinfo=kst)) == 'FULL_ACTIVE'
    assert get_target_state(dt.datetime(2026, 9, 8, 15, 45, 0, tzinfo=kst)) == 'POST_MARKET_EOD'
    assert get_target_state(dt.datetime(2026, 9, 8, 19, 0, 0, tzinfo=kst)) == 'NIGHT_SLEEP'


def test_calc_sleep_seconds_same_day_and_next_day() -> None:
    import datetime as dt
    from zoneinfo import ZoneInfo
    from src.collector.daemon import calc_sleep_seconds

    kst = ZoneInfo('Asia/Seoul')
    now = dt.datetime(2026, 9, 8, 8, 0, 0, tzinfo=kst)
    target_same_day = dt.time(8, 20, 0)
    # 20 minutes = 1200 seconds
    assert calc_sleep_seconds(now, target_same_day) == 1200.0

    # Target past -> next day 08:20
    now_after = dt.datetime(2026, 9, 8, 16, 0, 0, tzinfo=kst)
    target_next_day = dt.time(8, 20, 0)
    # From 16:00 to 08:20 next day is 16 hours 20 minutes = 58800 seconds
    assert calc_sleep_seconds(now_after, target_next_day) == 58800.0


def test_run_eod_maintenance_threads_archive_root(tmp_path) -> None:
    import datetime as dt
    import json
    import zstandard as zstd
    from src.collector.daemon import run_eod_maintenance

    # Given: 만료 파티션에 유효 프레임
    part = tmp_path / 'l0' / 'kis' / 'H0STCNT0' / 'dt=2026-09-01'
    part.mkdir(parents=True, exist_ok=True)
    rec = {'raw': 'a', 'recv_mono_ns': 1, 'recv_wall_ns': 2, 'conn_id': 'c1', 'conn_seq': 1, 'vendor': 'kis', 'tr_id': 'H0STCNT0'}
    (part / '09.jsonl.zst').write_bytes(zstd.ZstdCompressor(level=3).compress((json.dumps(rec) + '\n').encode('utf-8')))
    archive_root = tmp_path / 'l1'

    # When
    deleted = run_eod_maintenance(tmp_path / 'l0', retain_days=3, today=dt.date(2026, 9, 8), archive_root=archive_root)

    # Then
    assert deleted == 1
    assert not part.exists()
    assert (archive_root / 'kis' / 'H0STCNT0' / 'dt=2026-09-01.parquet').exists()


def test_run_eod_maintenance_invokes_prune(tmp_path) -> None:
    import datetime as dt
    from src.collector.daemon import run_eod_maintenance

    # Given: 만료 + 최근 파티션, archive_root 미지정
    root = tmp_path / 'l0' / 'kis' / 'H0STCNT0'
    old_part = root / 'dt=2026-09-01'
    recent_part = root / 'dt=2026-09-07'
    old_part.mkdir(parents=True, exist_ok=True)
    recent_part.mkdir(parents=True, exist_ok=True)
    (old_part / '09.jsonl.zst').write_text('dummy')
    (recent_part / '09.jsonl.zst').write_text('dummy')

    # When
    deleted = run_eod_maintenance(tmp_path / 'l0', retain_days=3, today=dt.date(2026, 9, 8))

    # Then: offload 대상 없음 -> 삭제 0, 원본 보존
    assert deleted == 0
    assert old_part.exists()
    assert recent_part.exists()


def test_run_collector_daemon_single_cycle() -> None:
    import datetime as dt
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo
    from src.collector.daemon import run_collector_daemon

    mock_sleep = MagicMock()
    night = dt.datetime(2026, 9, 8, 20, 0, 0, tzinfo=ZoneInfo('Asia/Seoul'))

    run_collector_daemon(sleep_fn=mock_sleep, max_cycles=1, now_fn=lambda: night)

    mock_sleep.assert_called_once()

def test_run_eod_offload_syncs_and_prunes_with_injected_archiver(tmp_path) -> None:
    import datetime as dt
    from src.collector.daemon import run_eod_offload

    root = tmp_path / 'l1' / 'kis' / 'H0STCNT0'
    root.mkdir(parents=True)
    old_pq = root / 'dt=2026-07-01.parquet'
    old_pq.write_bytes(b'x')

    class _Arc:
        def sync_l1_tree(self, archive_root):
            return {'uploaded': 1, 'skipped': 0, 'failed': 0}
        def remote_files(self, prefix):
            return {'l1/kis/H0STCNT0/dt=2026-07-01.parquet'}

    stats = run_eod_offload(tmp_path / 'l1', archiver=_Arc(), reference_date=dt.date(2026, 9, 8))

    assert stats['uploaded'] == 1
    assert stats['purged'] == 1
    assert not old_pq.exists()


def test_run_eod_offload_returns_zeros_when_archiver_missing(tmp_path, caplog, monkeypatch) -> None:
    import logging
    from src.collector.daemon import run_eod_offload

    root = tmp_path / 'l1' / 'kis' / 'H0STCNT0'
    root.mkdir(parents=True)
    old_pq = root / 'dt=2026-07-01.parquet'
    old_pq.write_bytes(b'x')

    monkeypatch.setattr('src.collector.remote_archive.HfDatasetArchiver.try_from_env', staticmethod(lambda: None))

    with caplog.at_level(logging.CRITICAL):
        stats = run_eod_offload(tmp_path / 'l1')

    assert stats == {'uploaded': 0, 'skipped': 0, 'failed': 0, 'purged': 0}
    assert old_pq.exists()
    assert any(r.levelno == logging.CRITICAL for r in caplog.records)

def test_run_session_orchestration_returns_true_when_candidates_ready(tmp_path, monkeypatch) -> None:
    import datetime as dt
    import polars as pl
    from src.collector.daemon import run_session_orchestration
    from src.collector.ipc import write_candidates

    bars_store = tmp_path / 'bars.parquet'
    pl.DataFrame({'date': [dt.date(2026, 9, 7)], 'symbol': ['005930'], 'close': [1.0],
                 'volume': [1], 'trade_value_100m': [1.0], 'daily_change_pct': [0.1]}).write_parquet(bars_store)
    candidates_path = tmp_path / 'candidates.json'
    write_candidates(candidates_path, [{'symbol': '005930', 'selection_reasons': ['limit_up']}], rev=1)

    calls: dict[str, object] = {}

    def _fake_bars_run(args):
        calls['bars'] = args
        return 0

    def _fake_universe_run(args):
        calls['universe'] = args
        return 0

    monkeypatch.setattr('src.cli.bars_refresh.run', _fake_bars_run)
    monkeypatch.setattr('src.cli.universe_plan.run', _fake_universe_run)

    ready = run_session_orchestration(
        today=dt.date(2026, 9, 8),
        bars_store=bars_store,
        market_map_path=tmp_path / 'm.json',
        candidates_path=candidates_path,
        universe_out_path=tmp_path / 'u.parquet',
    )

    assert ready is True
    assert calls['universe'].decision_date == '2026-09-07'
    assert calls['universe'].slot_budget == 100

def test_run_session_orchestration_returns_false_when_bars_store_missing(tmp_path, monkeypatch) -> None:
    import datetime as dt
    from src.collector.daemon import run_session_orchestration

    def _fake_bars_run(args):
        return 4

    monkeypatch.setattr('src.cli.bars_refresh.run', _fake_bars_run)

    ready = run_session_orchestration(
        today=dt.date(2026, 9, 8),
        bars_store=tmp_path / 'missing.parquet',
        market_map_path=tmp_path / 'm.json',
        candidates_path=tmp_path / 'candidates.json',
        universe_out_path=tmp_path / 'u.parquet',
    )

    assert ready is False

def test_run_session_orchestration_reuses_existing_candidates_when_universe_plan_fails(tmp_path, monkeypatch) -> None:
    import datetime as dt
    import polars as pl
    from src.collector.daemon import run_session_orchestration
    from src.collector.ipc import write_candidates

    bars_store = tmp_path / 'bars.parquet'
    pl.DataFrame({'date': [dt.date(2026, 9, 7)], 'symbol': ['005930'], 'close': [1.0],
                 'volume': [1], 'trade_value_100m': [1.0], 'daily_change_pct': [0.1]}).write_parquet(bars_store)
    candidates_path = tmp_path / 'candidates.json'
    write_candidates(candidates_path, [{'symbol': '005930', 'selection_reasons': ['limit_up']}], rev=1)

    monkeypatch.setattr('src.cli.bars_refresh.run', lambda args: 0)
    monkeypatch.setattr('src.cli.universe_plan.run', lambda args: 1)

    ready = run_session_orchestration(
        today=dt.date(2026, 9, 8),
        bars_store=bars_store,
        market_map_path=tmp_path / 'm.json',
        candidates_path=candidates_path,
        universe_out_path=tmp_path / 'u.parquet',
    )

    assert ready is True

def test_run_collector_daemon_streamer_active_spawns_supervised_process(tmp_path, monkeypatch) -> None:
    import datetime as dt
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo
    import src.collector.daemon as daemon_mod
    from src.collector.daemon import run_collector_daemon

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(daemon_mod, 'run_session_orchestration', lambda **kw: True)

    calls: list[str] = []

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            calls.append('constructed')
        def ensure_running(self):
            calls.append('ensure_running')
            return 'started'

    monkeypatch.setattr(daemon_mod, 'ProcessSupervisor', _FakeSupervisor)

    active = dt.datetime(2026, 9, 8, 9, 0, 0, tzinfo=ZoneInfo('Asia/Seoul'))

    run_collector_daemon(sleep_fn=MagicMock(), max_cycles=1, now_fn=lambda: active)

    assert calls == ['constructed', 'ensure_running']

def test_run_collector_daemon_eod_stops_supervised_process(tmp_path, monkeypatch) -> None:
    import datetime as dt
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo
    import src.collector.daemon as daemon_mod
    from src.collector.daemon import run_collector_daemon

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(daemon_mod, 'run_session_orchestration', lambda **kw: True)

    stop_calls: list[float] = []

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            self.cmd = cmd
        def ensure_running(self):
            self.checked = True
            return 'started'
        def stop(self, *, timeout_s=15.0):
            stop_calls.append(timeout_s)
            return 'graceful'

    monkeypatch.setattr(daemon_mod, 'ProcessSupervisor', _FakeSupervisor)

    kst = ZoneInfo('Asia/Seoul')
    times = iter([
        dt.datetime(2026, 9, 8, 9, 0, 0, tzinfo=kst),
        dt.datetime(2026, 9, 8, 15, 45, 0, tzinfo=kst),
    ])

    run_collector_daemon(sleep_fn=MagicMock(), max_cycles=2, now_fn=lambda: next(times))

    assert stop_calls == [15.0]


def test_run_collector_daemon_streamer_active_skips_spawn_when_not_ready(tmp_path, monkeypatch) -> None:
    import datetime as dt
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo
    import src.collector.daemon as daemon_mod
    from src.collector.daemon import run_collector_daemon

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(daemon_mod, 'run_session_orchestration', lambda **kw: False)

    constructed: list[str] = []

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            constructed.append('constructed')
        def ensure_running(self):
            constructed.append('ensure_running')
            return 'started'

    monkeypatch.setattr(daemon_mod, 'ProcessSupervisor', _FakeSupervisor)

    active = dt.datetime(2026, 9, 8, 9, 0, 0, tzinfo=ZoneInfo('Asia/Seoul'))

    run_collector_daemon(sleep_fn=MagicMock(), max_cycles=1, now_fn=lambda: active)

    assert constructed == []


def test_run_collector_daemon_streamer_active_logs_circuit_open(tmp_path, monkeypatch, caplog) -> None:
    import datetime as dt
    import logging
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo
    import src.collector.daemon as daemon_mod
    from src.collector.daemon import run_collector_daemon

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(daemon_mod, 'run_session_orchestration', lambda **kw: True)

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            self.cmd = cmd
        def ensure_running(self):
            return 'circuit_open'

    monkeypatch.setattr(daemon_mod, 'ProcessSupervisor', _FakeSupervisor)

    active = dt.datetime(2026, 9, 8, 9, 0, 0, tzinfo=ZoneInfo('Asia/Seoul'))

    with caplog.at_level(logging.CRITICAL):
        run_collector_daemon(sleep_fn=MagicMock(), max_cycles=1, now_fn=lambda: active)

    assert any('circuit_open' in r.message for r in caplog.records)


def test_run_collector_daemon_streamer_active_survives_orchestration_exception(tmp_path, monkeypatch) -> None:
    import datetime as dt
    from unittest.mock import MagicMock
    from zoneinfo import ZoneInfo
    import src.collector.daemon as daemon_mod
    from src.collector.daemon import run_collector_daemon

    monkeypatch.chdir(tmp_path)

    def _raise(**kw):
        raise FileNotFoundError("data/universe/2026-09-08.parquet")

    monkeypatch.setattr(daemon_mod, 'run_session_orchestration', _raise)

    constructed: list[str] = []

    class _FakeSupervisor:
        def __init__(self, *, cmd, breaker=None):
            constructed.append('constructed')
        def ensure_running(self):
            return 'started'

    monkeypatch.setattr(daemon_mod, 'ProcessSupervisor', _FakeSupervisor)

    active = dt.datetime(2026, 9, 8, 9, 0, 0, tzinfo=ZoneInfo('Asia/Seoul'))

    run_collector_daemon(sleep_fn=MagicMock(), max_cycles=1, now_fn=lambda: active)

    assert constructed == []
