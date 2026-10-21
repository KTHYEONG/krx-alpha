from __future__ import annotations

import datetime as dt
import logging
from zoneinfo import ZoneInfo

import pytest

from src.core.session_anchors import standard_session_anchors
from src.realtime.contracts import L0Frame
from src.realtime.regular_open_watch import RegularOpenWatch

_KST = ZoneInfo("Asia/Seoul")
_DAY = dt.date(2026, 10, 21)


class _Clock:
    def __init__(self, hh: int, mm: int, ss: int) -> None:
        self.at = dt.datetime(_DAY.year, _DAY.month, _DAY.day, hh, mm, ss, tzinfo=_KST)

    def __call__(self) -> dt.datetime:
        return self.at

    def advance(self, seconds: float) -> None:
        self.at += dt.timedelta(seconds=seconds)


def _watch(clock: _Clock) -> RegularOpenWatch:
    return RegularOpenWatch(
        anchors=standard_session_anchors(_DAY), silence_limit_s=30.0, open_grace_s=600.0, alert_after_s=30.0, now=clock
    )


def _frame(at: dt.datetime, stream: str = "H0STCNT0") -> L0Frame:
    return L0Frame("ls", stream, "005930", "{}", 1, int(at.timestamp() * 1_000_000_000), 1, "c")


def test_outside_regular_session_has_no_limit() -> None:
    assert _watch(_Clock(8, 59, 59)).silence_limit() is None
    assert _watch(_Clock(15, 30, 0)).silence_limit() is None


def test_grace_before_first_trade_then_normal_limit(caplog: pytest.LogCaptureFixture) -> None:
    clock = _Clock(9, 0, 10)
    watch = _watch(clock)
    assert watch.silence_limit() == 590.0
    with caplog.at_level(logging.CRITICAL):
        clock.advance(30)
        assert watch.silence_limit() == 560.0
        watch.silence_limit()
    assert caplog.text.count("stage=open_delay status=CRITICAL") == 1
    clock.advance(60)
    watch.observe(_frame(clock.at))
    assert watch.silence_limit() == 30.0


def test_grace_expires_back_to_normal_limit() -> None:
    clock = _Clock(9, 10, 0)
    assert _watch(clock).silence_limit() == 30.0


def test_only_trade_frames_after_open_end_grace() -> None:
    clock = _Clock(9, 0, 5)
    watch = _watch(clock)
    watch.observe(_frame(clock.at, "H0STASP0"))
    watch.observe(_frame(clock.at - dt.timedelta(seconds=10)))
    assert watch.silence_limit() == 595.0
    watch.observe(_frame(clock.at))
    assert watch.silence_limit() == 30.0


def test_resolved_is_logged_only_after_an_alert(caplog: pytest.LogCaptureFixture) -> None:
    clock = _Clock(9, 0, 40)
    watch = _watch(clock)
    watch.silence_limit()
    clock.advance(20)
    with caplog.at_level(logging.WARNING):
        watch.observe(_frame(clock.at))
    assert "stage=open_delay status=RESOLVED first_trade_after_s=60" in caplog.text
    quiet = _watch(_Clock(9, 0, 1))
    with caplog.at_level(logging.WARNING):
        caplog.clear()
        quiet.observe(_frame(quiet._now()))
    assert "open_delay" not in caplog.text


def test_invalid_arguments_and_bad_timestamps() -> None:
    clock = _Clock(9, 0, 0)
    anchors = standard_session_anchors(_DAY)
    with pytest.raises(ValueError, match="positive"):
        RegularOpenWatch(anchors=anchors, silence_limit_s=0, open_grace_s=600, alert_after_s=30, now=clock)
    with pytest.raises(ValueError, match="open_grace_s"):
        RegularOpenWatch(anchors=anchors, silence_limit_s=30, open_grace_s=10, alert_after_s=30, now=clock)
    watch = _watch(clock)
    watch.observe(L0Frame("ls", "H0STCNT0", "005930", "{}", 1, 2**70, 1, "c"))
    assert watch.silence_limit() == 600.0
