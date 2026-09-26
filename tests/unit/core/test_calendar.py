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


def _kst(t: str):
    import datetime as dt
    from zoneinfo import ZoneInfo

    kst = ZoneInfo("Asia/Seoul")
    hh, mm, ss = (int(x) for x in t.split(":"))
    return dt.datetime(2026, 9, 10, hh, mm, ss, tzinfo=kst)


def test_after_market_disabled_preserves_regular_eod_boundaries():
    from src.core.calendar import SessionSchedule, SessionState, get_target_state

    schedule = SessionSchedule(after_market_enabled=False)
    assert get_target_state(_kst('15:39:59'), schedule) is SessionState.FULL_ACTIVE
    assert get_target_state(_kst('15:40:00'), schedule) is SessionState.POST_MARKET_EOD
    assert get_target_state(_kst('16:00:00'), schedule) is SessionState.NIGHT_IDLE


def test_after_market_enabled_uses_1540_2000_2030_boundaries():
    from src.core.calendar import SessionSchedule, SessionState, get_target_state

    schedule = SessionSchedule(after_market_enabled=True)
    assert get_target_state(_kst('15:40:00'), schedule) is SessionState.AFTER_MARKET_ACTIVE
    assert get_target_state(_kst('19:59:59'), schedule) is SessionState.AFTER_MARKET_ACTIVE
    assert get_target_state(_kst('20:00:00'), schedule) is SessionState.POST_MARKET_EOD
    assert get_target_state(_kst('20:30:00'), schedule) is SessionState.NIGHT_IDLE


def test_after_market_schedule_rejects_non_monotonic_boundaries():
    from datetime import time

    import pytest

    from src.core.calendar import SessionSchedule

    with pytest.raises(ValueError, match="monotonic"):  # noqa: PT011 - skeleton requires ValueError
        SessionSchedule(after_market_close=time(15, 30))


def test_schedule_for_standard_anchors_returns_base() -> None:
    import datetime as dt

    from src.core.calendar import SessionSchedule, schedule_for
    from src.core.session_anchors import standard_session_anchors

    base = SessionSchedule(after_market_enabled=True)

    assert schedule_for(standard_session_anchors(dt.date(2026, 10, 1)), base) == base


def test_schedule_for_csat_anchors_shifts_close_transitions() -> None:
    import datetime as dt

    from src.core.calendar import SessionSchedule, schedule_for
    from src.core.session_anchors import AnchorSource, SessionAnchors

    base = SessionSchedule(after_market_enabled=True)
    anchors = SessionAnchors(
        date=dt.date(2026, 11, 19),
        regular_open=dt.time(10, 0),
        closing_auction_start=dt.time(16, 20),
        regular_close=dt.time(16, 30),
        after_market_end=dt.time(20, 0),
        source=AnchorSource.VENDOR,
    )

    shifted = schedule_for(anchors, base)

    assert shifted.market_close == dt.time(16, 40)
    assert shifted.streamer_start == base.streamer_start
    assert shifted.after_market_close == dt.time(20, 0)
    assert shifted.after_market_eod_done == dt.time(20, 30)
