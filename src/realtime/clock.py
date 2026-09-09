"""세션 시작 시 NTP offset 실측 (fail-closed 클럭 게이트)."""

from __future__ import annotations

import statistics
from typing import Any

import ntplib

from src.core.errors import KrxAlphaError


class ClockUnsyncedError(KrxAlphaError):
    """NTP 불통 또는 허용 오차 초과."""


def measure_ntp_offset_ns(host: str, *, samples: int = 5, timeout_s: float = 3.0, client: object | None = None) -> int:
    cli: Any = client if client is not None else ntplib.NTPClient()
    offsets: list[float] = []
    for _ in range(samples):
        try:
            resp = cli.request(host, version=3, timeout=timeout_s)
        except (ntplib.NTPException, OSError):
            continue
        offsets.append(float(resp.offset))
    if not offsets:
        raise ClockUnsyncedError(f"ntp unreachable: {host}")
    return round(statistics.median(offsets) * 1_000_000_000)
