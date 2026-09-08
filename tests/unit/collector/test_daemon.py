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
    from unittest.mock import MagicMock
    from src.collector.daemon import run_collector_daemon

    mock_sleep = MagicMock()
    run_collector_daemon(sleep_fn=mock_sleep, max_cycles=1)
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
