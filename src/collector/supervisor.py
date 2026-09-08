"""collect-stream subprocess 생존감독 + 재시작 서킷브레이커."""

from __future__ import annotations

import subprocess
import time
from typing import Any


class RestartCircuitBreaker:
    def __init__(self, *, max_restarts: int = 5, window_s: float = 1800.0) -> None:
        self._max_restarts = max_restarts
        self._window_s = window_s
        self._timestamps: list[float] = []

    def _prune(self, now: float) -> None:
        cutoff = now - self._window_s
        self._timestamps = [t for t in self._timestamps if t > cutoff]

    def allow_restart(self, *, now: float | None = None) -> bool:
        now = now if now is not None else time.monotonic()
        self._prune(now)
        return len(self._timestamps) < self._max_restarts

    def record_restart(self, *, now: float | None = None) -> None:
        now = now if now is not None else time.monotonic()
        self._prune(now)
        self._timestamps.append(now)


class ProcessSupervisor:
    def __init__(
        self,
        *,
        cmd: list[str],
        breaker: RestartCircuitBreaker | None = None,
        popen: Any = subprocess.Popen,
    ) -> None:
        self._cmd = cmd
        self._breaker = breaker
        self._popen = popen
        self._proc: Any = None

    def is_running(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    def ensure_running(self) -> str:
        if self._proc is not None and self._proc.poll() is None:
            return "running"
        was_started_before = self._proc is not None
        if was_started_before and self._breaker is not None and not self._breaker.allow_restart():
            return "circuit_open"
        if was_started_before and self._breaker is not None:
            self._breaker.record_restart()
        self._proc = self._popen(self._cmd)
        return "restarted" if was_started_before else "started"

    def stop(self, *, timeout_s: float = 15.0) -> str:
        if self._proc is None or self._proc.poll() is not None:
            return "not_running"
        self._proc.terminate()
        try:
            self._proc.wait(timeout=timeout_s)
            return "graceful"
        except subprocess.TimeoutExpired:
            self._proc.kill()
            self._proc.wait()
            return "killed"
