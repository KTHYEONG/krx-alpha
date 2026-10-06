"""collect-stream subprocess 생존감독 + 재시작 서킷브레이커."""

from __future__ import annotations

import subprocess
import time
from collections.abc import Iterable
from typing import Any

PROBE_BASE_INTERVAL_S: float = 60.0
PROBE_MAX_INTERVAL_S: float = 300.0


class RestartCircuitBreaker:
    def __init__(
        self,
        *,
        max_restarts: int = 5,
        window_s: float = 1800.0,
        probe_base_interval_s: float = PROBE_BASE_INTERVAL_S,
        probe_max_interval_s: float = PROBE_MAX_INTERVAL_S,
    ) -> None:
        """Bound fast restarts, then allow spaced probe restarts.

        Every restart attempt (fast or probe) is recorded in one sliding window. While fewer than
        ``max_restarts`` attempts lie in the window restarts are immediate. Once the budget is spent
        the breaker is *open*: a further attempt is a probe, allowed only after
        ``min(probe_max_interval_s, probe_base_interval_s * 2**p)`` seconds since the latest attempt,
        where ``p`` is the number of attempts beyond the budget still inside the window. A child that
        stays up stops adding attempts, so the window drains and the breaker closes on its own.

        Raises:
            ValueError: ``max_restarts < 1``, non-positive ``window_s`` or probe interval, or
                ``probe_max_interval_s < probe_base_interval_s``.
        """
        if max_restarts < 1:
            raise ValueError(f"max_restarts must be >= 1, got {max_restarts}")
        if window_s <= 0:
            raise ValueError(f"window_s must be positive, got {window_s}")
        if probe_base_interval_s <= 0:
            raise ValueError(f"probe_base_interval_s must be positive, got {probe_base_interval_s}")
        if probe_max_interval_s <= 0:
            raise ValueError(f"probe_max_interval_s must be positive, got {probe_max_interval_s}")
        if probe_max_interval_s < probe_base_interval_s:
            raise ValueError(
                "probe_max_interval_s must be >= probe_base_interval_s, "
                f"got {probe_max_interval_s} < {probe_base_interval_s}"
            )
        self._max_restarts = max_restarts
        self._window_s = window_s
        self._probe_base_interval_s = probe_base_interval_s
        self._probe_max_interval_s = probe_max_interval_s
        self._timestamps: list[float] = []

    def _prune(self, now: float) -> None:
        cutoff = now - self._window_s
        self._timestamps = [t for t in self._timestamps if t > cutoff]

    def allow_restart(self, *, now: float | None = None) -> bool:
        now = now if now is not None else time.monotonic()
        self._prune(now)
        return len(self._timestamps) < self._max_restarts

    def allow_probe(self, *, now: float | None = None) -> bool:
        now = now if now is not None else time.monotonic()
        self._prune(now)
        if len(self._timestamps) < self._max_restarts:
            return False
        beyond = len(self._timestamps) - self._max_restarts
        spacing = min(self._probe_max_interval_s, self._probe_base_interval_s * (2**beyond))
        latest: float = max(self._timestamps)
        return bool((now - latest) >= spacing)

    def is_open(self, *, now: float | None = None) -> bool:
        now = now if now is not None else time.monotonic()
        self._prune(now)
        return len(self._timestamps) >= self._max_restarts

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
        self._open_alerted: bool = False

    @property
    def last_exit_code(self) -> int | None:
        return self._last_exit_code

    def is_running(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    def ensure_running(self) -> str:
        """Return ``"started"``, ``"running"``, ``"restarted"`` or ``"circuit_open"``.

        A dead child is restarted when the breaker allows a fast restart or a due probe (both reported
        as ``"restarted"``); ``"circuit_open"`` means the child is dead, the breaker is open and no
        probe is due yet. A supervisor without a breaker always restarts.
        """
        now = time.monotonic()
        if self._breaker is not None and not self._breaker.is_open(now=now):
            self._open_alerted = False
        if self._proc is not None and self._proc.poll() is None:
            return "running"
        was_started_before = self._proc is not None
        if was_started_before:
            self._last_exit_code = self._proc.poll()
        if not was_started_before:
            self._proc = self._popen(self._cmd)
            return "started"
        if self._breaker is None:
            self._proc = self._popen(self._cmd)
            return "restarted"
        if self._breaker.allow_restart(now=now):
            self._breaker.record_restart(now=now)
            self._proc = self._popen(self._cmd)
            return "restarted"
        if self._breaker.allow_probe(now=now):
            self._breaker.record_restart(now=now)
            self._proc = self._popen(self._cmd)
            return "restarted"
        return "circuit_open"

    def take_circuit_alert(self) -> bool:
        """Return True exactly once per open episode.

        An episode starts when the breaker first reports open and ends when it closes; the latch makes
        callers alert once per episode even though probe restarts interleave ``"restarted"`` and
        ``"circuit_open"`` results. Always False without a breaker.
        """
        if self._breaker is None:
            return False
        if not self._breaker.is_open():
            self._open_alerted = False
            return False
        if self._open_alerted:
            return False
        self._open_alerted = True
        return True

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
