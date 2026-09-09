"""교차 컨텍스트 공용 예외 계층 루트."""

from __future__ import annotations


class KrxAlphaError(RuntimeError):
    """krx-alpha 공용 예외 루트."""


class SlotBudgetExceededError(KrxAlphaError):
    """선정/구독 슬롯 예산 초과 fail-closed 신호."""


class MissingCredentialsError(KrxAlphaError):
    """필수 자격증명 누락 fail-closed 신호."""


class ScheduleOrderError(KrxAlphaError):
    """세션 스케줄 단조성 위반 fail-closed 신호."""
