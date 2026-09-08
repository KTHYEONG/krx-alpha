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


def test_run_eod_maintenance_invokes_prune(tmp_path) -> None:
    import datetime as dt
    from src.collector.daemon import run_eod_maintenance

    root = tmp_path / 'l0' / 'kis' / 'H0STCNT0'
    old_part = root / 'dt=2026-09-01'
    recent_part = root / 'dt=2026-09-07'
    old_part.mkdir(parents=True, exist_ok=True)
    recent_part.mkdir(parents=True, exist_ok=True)
    (old_part / '09.jsonl.zst').write_text('dummy')
    (recent_part / '09.jsonl.zst').write_text('dummy')

    today = dt.date(2026, 9, 8)
    deleted = run_eod_maintenance(tmp_path / 'l0', retain_days=3, today=today)
    assert deleted == 1
    assert not old_part.exists()
    assert recent_part.exists()


def test_run_collector_daemon_single_cycle() -> None:
    from unittest.mock import MagicMock
    from src.collector.daemon import run_collector_daemon

    mock_sleep = MagicMock()
    run_collector_daemon(sleep_fn=mock_sleep, max_cycles=1)
    mock_sleep.assert_called_once()
