"""KIS REST request spacing (per-key rate limiter)."""

from __future__ import annotations

import time
from collections.abc import Callable


class RateLimiter:
    """토큰버킷 대체: 요청 간 최소 간격을 보장한다."""

    def __init__(
        self,
        rate_per_s: float,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._interval = 1.0 / rate_per_s
        self._clock = clock
        self._sleep = sleep
        self._next_allowed = float("-inf")

    def acquire(self) -> None:
        """Wait until this configured KIS key may send its next REST request."""
        now = self._clock()
        if now < self._next_allowed:
            self._sleep(self._next_allowed - now)
            now = self._next_allowed
        self._next_allowed = now + self._interval
