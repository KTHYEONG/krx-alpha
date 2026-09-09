"""Clock NTP unit tests."""

from __future__ import annotations


def test_measure_ntp_offset_returns_median_ns():
    # Given: 고정 offset(초) 3개를 반환하는 가짜 NTP 클라이언트
    from src.realtime.clock import measure_ntp_offset_ns

    class _FakeStat:
        def __init__(self, off):
            self.offset = off

    class _FakeClient:
        def __init__(self):
            self._vals = [1.088, 1.090, 1.089]
            self._i = 0

        def request(self, host, version=3, timeout=5):
            v = self._vals[self._i % len(self._vals)]
            self._i += 1
            return _FakeStat(v)

    # When
    off_ns = measure_ntp_offset_ns("pool.ntp.org", samples=3, timeout_s=3.0, client=_FakeClient())

    # Then: 중앙값 1.089s 를 ns 정수로
    assert off_ns == 1_089_000_000


def test_measure_ntp_offset_raises_on_ntp_failure():
    # Given: 항상 NTPException 을 던지는 클라이언트
    import ntplib
    import pytest

    from src.realtime.clock import ClockUnsyncedError, measure_ntp_offset_ns

    class _DeadClient:
        def request(self, host, version=3, timeout=5):
            raise ntplib.NTPException("no response")

    # When / Then: NTP 불통은 ClockUnsyncedError 로 fail-closed
    with pytest.raises(ClockUnsyncedError):
        measure_ntp_offset_ns("pool.ntp.org", samples=3, timeout_s=1.0, client=_DeadClient())
