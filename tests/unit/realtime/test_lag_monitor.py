from __future__ import annotations

import datetime as dt
import logging
from zoneinfo import ZoneInfo

import pytest

from src.realtime.contracts import L0Frame
from src.realtime.lag_monitor import LagMonitor
from src.realtime.streaming_child import LagMonitoringSink

_KST = ZoneInfo("Asia/Seoul")
_DAY = dt.date(2026, 10, 20)


def _ns(second_of_day: float) -> int:
    base = dt.datetime(_DAY.year, _DAY.month, _DAY.day, tzinfo=_KST).timestamp()
    return int((base + second_of_day) * 1_000_000_000)


def _hms(second_of_day: int) -> str:
    return f"{second_of_day // 3600:02d}{second_of_day % 3600 // 60:02d}{second_of_day % 60:02d}"


def _frame(recv_s: float, event_s: int, *, field: bool = False) -> L0Frame:
    raw = f'{{"body":{{"chetime":"{_hms(event_s)}"}}}}'
    return L0Frame(
        "ls", "H0STCNT0", "005930", raw, 1, _ns(recv_s), 1, "c",
        exchange_event_time=_hms(event_s) if field else "",
    )


def _feed(monitor: LagMonitor, start_s: int, seconds: int, lag_s: int, per_second: int = 6) -> None:
    for sec in range(start_s, start_s + seconds):
        for _ in range(per_second):
            monitor.observe(_frame(sec + 0.5, sec - lag_s))


def test_sustained_lag_alerts_once_then_recovers(caplog: pytest.LogCaptureFixture) -> None:
    monitor = LagMonitor(threshold_s=10.0, sample_every=1)
    with caplog.at_level(logging.INFO):
        _feed(monitor, 32400, 20, lag_s=1)
        _feed(monitor, 32420, 20, lag_s=30)
        _feed(monitor, 32440, 20, lag_s=40)
        assert monitor.alerted
        _feed(monitor, 32460, 20, lag_s=1)
    critical = [r for r in caplog.records if r.levelno == logging.CRITICAL]
    assert len(critical) == 1
    assert "stage=ingest_lag status=CRITICAL reason=live_lag_exceeded" in critical[0].getMessage()
    assert "threshold_s=10.0" in critical[0].getMessage()
    assert any("status=RECOVERED" in r.getMessage() for r in caplog.records)
    assert not monitor.alerted


def test_healthy_feed_and_sparse_windows_stay_silent(caplog: pytest.LogCaptureFixture) -> None:
    monitor = LagMonitor(threshold_s=10.0, sample_every=1, min_samples=5)
    with caplog.at_level(logging.INFO):
        _feed(monitor, 32400, 30, lag_s=2)
        monitor.observe(_frame(32431.2, 32431 - 60))
        monitor.observe(_frame(32436.2, 32436 - 60))
    assert caplog.records == []
    assert not monitor.alerted


def test_event_time_field_is_preferred_and_unparsable_frames_are_ignored() -> None:
    monitor = LagMonitor(threshold_s=10.0, window_s=3600.0, sample_every=1, min_samples=1)
    plain = L0Frame("ls", "H0STCNT0", "005930", "not json", 1, _ns(32400.5), 1, "c")
    monitor.observe(plain)
    assert monitor._lags == []
    monitor.observe(_frame(32410.5, 32400, field=True))
    assert monitor._lags == [10.5]
    future = _frame(32400.5, 32580)
    monitor.observe(future)
    assert monitor._lags == [10.5]


def test_sampling_skips_frames_and_invalid_arguments_raise() -> None:
    monitor = LagMonitor(threshold_s=10.0, sample_every=4)
    for i in range(7):
        monitor.observe(_frame(32400.5, 32400 - i))
    assert len(monitor._lags) == 1
    with pytest.raises(ValueError, match="positive"):
        LagMonitor(threshold_s=0)
    with pytest.raises(ValueError, match=">= 1"):
        LagMonitor(threshold_s=1.0, sample_every=0)


def test_sink_decorator_delegates_and_observes() -> None:
    calls: list[tuple[str, object]] = []

    class _Inner:
        def record(self, frame: L0Frame) -> None:
            calls.append(("record", frame.symbol))

        def note_ack(self, vendor: str, ack: object) -> None:
            calls.append(("ack", vendor))

        def note_gap(self, symbol: str, start_ns: int, end_ns: int, reason: str) -> None:
            calls.append(("gap", reason))

        def flush(self) -> int:
            return 7

    monitor = LagMonitor(threshold_s=10.0, sample_every=1, min_samples=1)
    sink = LagMonitoringSink(_Inner(), monitor)
    sink.record(_frame(32400.5, 32400))
    sink.note_ack("ls", object())  # type: ignore[arg-type]
    sink.note_gap("005930", 1, 2, "disconnect")
    assert sink.flush() == 7
    assert calls == [("record", "005930"), ("ack", "ls"), ("gap", "disconnect")]
    assert monitor._lags == [0.5]


def test_standby_frames_are_recorded_but_not_observed_when_excluded() -> None:
    calls: list[str] = []

    class _Inner:
        def record(self, frame: L0Frame) -> None:
            calls.append("record")

        def record_standby(self, frame: L0Frame) -> None:
            calls.append("standby")

        def note_ack(self, vendor: str, ack: object) -> None:
            return None

        def note_gap(self, symbol: str, start_ns: int, end_ns: int, reason: str) -> None:
            return None

        def flush(self) -> int:
            return 0

    monitor = LagMonitor(threshold_s=10.0, sample_every=1, min_samples=1)
    sink = LagMonitoringSink(_Inner(), monitor, observe_standby=False)
    sink.record_standby(_frame(32400.5, 32350))
    assert calls == ["standby"]
    assert monitor._lags == []
    sink.record(_frame(32400.5, 32400))
    assert monitor._lags == [0.5]

    observing = LagMonitoringSink(_Inner(), monitor)
    observing.record_standby(_frame(32400.5, 32400))
    assert monitor._lags == [0.5, 0.5]


def test_standby_recorded_through_plain_sink_without_record_standby() -> None:
    calls: list[str] = []

    class _Plain:
        def record(self, frame: L0Frame) -> None:
            calls.append("record")

        def note_ack(self, vendor: str, ack: object) -> None:
            return None

        def note_gap(self, symbol: str, start_ns: int, end_ns: int, reason: str) -> None:
            return None

        def flush(self) -> int:
            return 0

    sink = LagMonitoringSink(_Plain(), LagMonitor(threshold_s=10.0), observe_standby=False)
    sink.record_standby(_frame(32400.5, 32400))
    assert calls == ["record"]


def test_observe_swallows_unexpected_errors() -> None:
    monitor = LagMonitor(threshold_s=10.0, sample_every=1)
    bad = L0Frame("ls", "H0STCNT0", "005930", '{"chetime":"999999"}', 1, _ns(32400.5), 1, "c")
    monitor.observe(bad)
    assert monitor._lags == []


def test_on_exceeded_fires_once_per_episode() -> None:
    calls: list[int] = []
    monitor = LagMonitor(threshold_s=10.0, sample_every=1, on_exceeded=lambda: calls.append(1))
    _feed(monitor, 32400, 20, lag_s=1)
    _feed(monitor, 32420, 20, lag_s=30)
    _feed(monitor, 32440, 20, lag_s=40)
    assert calls == [1]
