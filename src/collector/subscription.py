"""WS 구독 레지스트리 (재접속 replay + 슬롯 예산 강제)."""

from __future__ import annotations

from dataclasses import dataclass

from src.universe.policy import SlotBudgetExceededError


@dataclass(frozen=True)
class SubscriptionDiff:
    to_add: frozenset[tuple[str, str]]
    to_remove: frozenset[tuple[str, str]]


class SubscriptionRegistry:
    """활성 (symbol, tr_id) 페어 집합을 관리한다."""

    def __init__(self, *, slot_budget: int) -> None:
        self._slot_budget = slot_budget
        self._active: set[tuple[str, str]] = set()

    def plan(self, desired: dict[str, tuple[str, ...]]) -> SubscriptionDiff:
        want = frozenset((sym, tr) for sym, trs in desired.items() for tr in trs)
        if len(want) > self._slot_budget:
            raise SlotBudgetExceededError(f"want {len(want)} pairs exceeds slot_budget {self._slot_budget}")
        return SubscriptionDiff(to_add=want - frozenset(self._active), to_remove=frozenset(self._active) - want)

    def apply(self, diff: SubscriptionDiff) -> None:
        self._active -= set(diff.to_remove)
        self._active |= set(diff.to_add)

    def replay_pairs(self) -> list[tuple[str, str]]:
        return sorted(self._active)
