"""KIS 종목 상태 기반 수집 제외 및 경고 판정."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Final

logger = logging.getLogger(__name__)


class WarningLevel(StrEnum):
    NONE = "none"
    CAUTION = "caution"
    WARNING = "warning"
    RISK = "risk"
    UNKNOWN = "unknown"


EXCLUSION_FLAG_SOURCES: Final[Mapping[str, str]] = {
    "managed": "managed",
    "halted": "trading_halted",
    "liquidation": "liquidation_trading",
    "short_overheated": "short_overheated",
}
EXCLUSION_FLAGS: Final[tuple[str, ...]] = tuple(EXCLUSION_FLAG_SOURCES)
_WARNING_CODE_LEVELS: Final[Mapping[str, WarningLevel]] = {
    "00": WarningLevel.NONE,
    "01": WarningLevel.CAUTION,
    "02": WarningLevel.WARNING,
    "03": WarningLevel.RISK,
}
_MARGIN_100_STATUS_CODE: Final[str] = "57"


@dataclass(frozen=True)
class StatusVerdict:
    exclude: bool
    warning_level: WarningLevel
    flags: tuple[str, ...]


def classify_security_status(status: Mapping[str, object]) -> StatusVerdict:
    """Map one security status row to an exclusion verdict and warning level; unknown codes never raise.

    Raises: None. Side effects: one ``[DATA]`` WARNING when ``market_warning_code`` is unmapped.
    """
    flags = {flag for flag, key in EXCLUSION_FLAG_SOURCES.items() if status.get(key) is True}
    exclude = bool(flags)
    if status.get("status_code") == _MARGIN_100_STATUS_CODE:
        flags.add("margin_100")
    if status.get("credit_available") is False:
        flags.add("credit_unavailable")
    code = status.get("market_warning_code")
    level = _WARNING_CODE_LEVELS.get(code) if isinstance(code, str) else None
    if level is None:
        level = WarningLevel.UNKNOWN
        logger.warning("[DATA] stage=universe_status warning_code=%s status=UNMAPPED", code)
    return StatusVerdict(exclude=exclude, warning_level=level, flags=tuple(sorted(flags)))
