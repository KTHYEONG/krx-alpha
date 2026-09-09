"""세션 시각 단일 불변식: 상태전이와 대기시간이 동일 스케줄을 참조한다."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from enum import StrEnum
from zoneinfo import ZoneInfo

from src.core.errors import ScheduleOrderError

_KST = ZoneInfo("Asia/Seoul")


class SessionState(StrEnum):
    """세션 상태 (StrEnum 이므로 기존 문자열 비교와 호환된다)."""

    WEEKEND_SLEEP = "WEEKEND_SLEEP"
    PRE_MARKET_SLEEP = "PRE_MARKET_SLEEP"
    STREAMER_ACTIVE = "STREAMER_ACTIVE"
    FULL_ACTIVE = "FULL_ACTIVE"
    POST_MARKET_EOD = "POST_MARKET_EOD"
    NIGHT_SLEEP = "NIGHT_SLEEP"


@dataclass(frozen=True)
class SessionSchedule:
    """세션 시각표. 상태전이와 대기시간 계산이 공유하는 단일 소스."""

    streamer_start: dt.time = dt.time(8, 20)
    scanner_start: dt.time = dt.time(8, 50)
    market_close: dt.time = dt.time(15, 40)
    eod_done: dt.time = dt.time(16, 0)

    def __post_init__(self) -> None:
        if not (self.streamer_start < self.scanner_start < self.market_close < self.eod_done):
            raise ScheduleOrderError(
                "schedule must be strictly monotonic: "
                f"{self.streamer_start!r} < {self.scanner_start!r} < "
                f"{self.market_close!r} < {self.eod_done!r}"
            )


def get_target_state(now_dt: dt.datetime, *, schedule: SessionSchedule) -> SessionState:
    """주어진 스케줄 기준으로 목표 세션 상태를 반환한다 (내부 시각 리터럴 금지)."""
    kst_dt = now_dt.astimezone(_KST)
    if kst_dt.weekday() in (5, 6):
        return SessionState.WEEKEND_SLEEP
    now_time = kst_dt.time()
    if now_time < schedule.streamer_start:
        return SessionState.PRE_MARKET_SLEEP
    if now_time < schedule.scanner_start:
        return SessionState.STREAMER_ACTIVE
    if now_time < schedule.market_close:
        return SessionState.FULL_ACTIVE
    if now_time < schedule.eod_done:
        return SessionState.POST_MARKET_EOD
    return SessionState.NIGHT_SLEEP


def calc_sleep_seconds(now_dt: dt.datetime, target_time: dt.time) -> float:
    """목표시각까지 남은 초를 반환한다 (경과 시 익일로 롤오버)."""
    kst_dt = now_dt.astimezone(_KST)
    target_dt = kst_dt.replace(
        hour=target_time.hour,
        minute=target_time.minute,
        second=target_time.second,
        microsecond=0,
    )
    if target_dt <= kst_dt:
        target_dt += dt.timedelta(days=1)
    return float((target_dt - kst_dt).total_seconds())
