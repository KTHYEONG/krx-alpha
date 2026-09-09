def test_get_target_state_honors_configured_schedule() -> None:
    # Given: 동일 시각에 대해 기본 스케줄과 이동된 스케줄
    import datetime as dt
    from zoneinfo import ZoneInfo

    from src.core.calendar import SessionSchedule, SessionState, get_target_state

    kst = ZoneInfo("Asia/Seoul")
    weekday_0830 = dt.datetime(2026, 9, 10, 8, 30, 0, tzinfo=kst)
    default = SessionSchedule()
    shifted = SessionSchedule(
        streamer_start=dt.time(9, 30),
        scanner_start=dt.time(10, 0),
        market_close=dt.time(15, 40),
        eod_done=dt.time(16, 0),
    )

    # When / Then: 스케줄 값 이동이 상태전이를 실제로 이동시켜야 한다
    assert get_target_state(weekday_0830, schedule=default) == SessionState.STREAMER_ACTIVE
    assert get_target_state(weekday_0830, schedule=shifted) == SessionState.PRE_MARKET_SLEEP


def test_session_schedule_rejects_non_monotonic_times() -> None:
    # Given: 역순 스케줄
    import datetime as dt

    import pytest

    from src.core.calendar import SessionSchedule
    from src.core.errors import ScheduleOrderError

    # When / Then: fail-closed
    with pytest.raises(ScheduleOrderError, match="monotonic"):
        SessionSchedule(
            streamer_start=dt.time(9, 0),
            scanner_start=dt.time(8, 0),
            market_close=dt.time(15, 40),
            eod_done=dt.time(16, 0),
        )


def test_get_target_state_covers_all_session_boundaries() -> None:
    # Given: 기본 스케줄과 주말/평일 경계 시각
    import datetime as dt
    from zoneinfo import ZoneInfo

    from src.core.calendar import SessionSchedule, SessionState, get_target_state

    kst = ZoneInfo("Asia/Seoul")
    sched = SessionSchedule()

    # When / Then
    assert get_target_state(dt.datetime(2026, 9, 12, 12, 0, tzinfo=kst), schedule=sched) == SessionState.WEEKEND_SLEEP
    assert get_target_state(dt.datetime(2026, 9, 13, 12, 0, tzinfo=kst), schedule=sched) == SessionState.WEEKEND_SLEEP
    assert get_target_state(dt.datetime(2026, 9, 10, 7, 0, tzinfo=kst), schedule=sched) == SessionState.PRE_MARKET_SLEEP
    assert get_target_state(dt.datetime(2026, 9, 10, 8, 25, tzinfo=kst), schedule=sched) == SessionState.STREAMER_ACTIVE
    assert get_target_state(dt.datetime(2026, 9, 10, 9, 30, tzinfo=kst), schedule=sched) == SessionState.FULL_ACTIVE
    assert get_target_state(dt.datetime(2026, 9, 10, 15, 45, tzinfo=kst), schedule=sched) == SessionState.POST_MARKET_EOD
    assert get_target_state(dt.datetime(2026, 9, 10, 19, 0, tzinfo=kst), schedule=sched) == SessionState.NIGHT_SLEEP


def test_calc_sleep_seconds_same_day_and_next_day() -> None:
    # Given: 목표시각 이전/이후 두 시점
    import datetime as dt
    from zoneinfo import ZoneInfo

    from src.core.calendar import calc_sleep_seconds

    kst = ZoneInfo("Asia/Seoul")

    # When / Then: 같은 날 남은 초, 지난 경우 익일로 롤오버
    assert calc_sleep_seconds(dt.datetime(2026, 9, 10, 8, 0, 0, tzinfo=kst), dt.time(8, 20, 0)) == 1200.0
    assert calc_sleep_seconds(dt.datetime(2026, 9, 10, 16, 0, 0, tzinfo=kst), dt.time(8, 20, 0)) == 58800.0
