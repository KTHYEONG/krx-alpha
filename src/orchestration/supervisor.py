"""collect-stream subprocess 생존감독 + 재시작 서킷브레이커."""

from __future__ import annotations

import subprocess
import time
from collections.abc import Iterable
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
        self._last_exit_code: int | None = None

    @property
    def last_exit_code(self) -> int | None:
        return self._last_exit_code

    def is_running(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    def ensure_running(self) -> str:
        if self._proc is not None and self._proc.poll() is None:
            return "running"
        was_started_before = self._proc is not None
        if was_started_before:
            self._last_exit_code = self._proc.poll()
        if was_started_before and self._breaker is not None and not self._breaker.allow_restart():
            return "circuit_open"
        if was_started_before and self._breaker is not None:
            self._breaker.record_restart()
        self._proc = self._popen(self._cmd)
        return "restarted" if was_started_before else "started"

    def request_stop(self) -> bool:
        """Send SIGTERM to the child if it is running. Non-blocking.

        Returns:
            True when a signal was sent, False when no child was running.
        """
        proc = self._proc
        if proc is None or proc.poll() is not None:
            return False
        proc.terminate()
        return True

    def wait_stopped(self, timeout_s: float) -> str:
        """Wait up to ``timeout_s`` for the child to exit, SIGKILL it on expiry.

        Returns:
            ``"graceful"``, ``"killed"`` or ``"not_running"``.
        """
        proc = self._proc
        if proc is None or proc.poll() is not None:
            return "not_running"
        try:
            ret = proc.wait(timeout=timeout_s)
            code = proc.poll()
            self._last_exit_code = code if code is not None else ret
            return "graceful"
        except subprocess.TimeoutExpired:
            proc.kill()
            ret = proc.wait()
            code = proc.poll()
            self._last_exit_code = code if code is not None else ret
            return "killed"

    def stop(self, *, timeout_s: float = 15.0) -> str:
        if not self.request_stop():
            return "not_running"
        return self.wait_stopped(timeout_s)


def stop_supervisors(supervisors: Iterable[ProcessSupervisor], *, deadline_s: float) -> dict[str, int]:
    """Stop all supervised children concurrently under one shared deadline.

    Children are signalled first and awaited afterwards, so N children cost one grace window rather
    than N sequential ones — required to finish inside the container ``stop_grace_period``.

    Returns:
        Counts keyed ``graceful``, ``killed``, ``not_running``.
    """
    sups = list(supervisors)
    counts = {"graceful": 0, "killed": 0, "not_running": 0}
    deadline = time.monotonic() + deadline_s
    for sup in sups:
        sup.request_stop()
    for sup in sups:
        remaining = deadline - time.monotonic()
        if remaining < 0.0:
            remaining = 0.0
        result = sup.wait_stopped(remaining)
        counts[result] += 1
    return counts
