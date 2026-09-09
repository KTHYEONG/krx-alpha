"""Subscription registry unit tests."""

from __future__ import annotations


def test_subscription_first_plan_adds_all_pairs():
    # Given: 슬롯 예산 41, 최초 desired 2종목
    from src.realtime.subscription import SubscriptionRegistry

    reg = SubscriptionRegistry(slot_budget=41)
    desired = {"005930": ("H0STCNT0", "H0STASP0"), "000660": ("H0STCNT0",)}

    # When
    diff = reg.plan(desired)
    reg.apply(diff)

    # Then: 전 페어가 to_add, to_remove 없음, replay 는 정렬된 활성 페어
    assert diff.to_add == frozenset({("005930", "H0STCNT0"), ("005930", "H0STASP0"), ("000660", "H0STCNT0")})
    assert diff.to_remove == frozenset()
    assert reg.replay_pairs() == [("000660", "H0STCNT0"), ("005930", "H0STASP0"), ("005930", "H0STCNT0")]


def test_subscription_plan_computes_add_and_remove_delta():
    # Given: 이미 005930(체결) 구독 중, 새 desired 는 000660(체결) 로 교체
    from src.realtime.subscription import SubscriptionRegistry

    reg = SubscriptionRegistry(slot_budget=41)
    reg.apply(reg.plan({"005930": ("H0STCNT0",)}))

    # When
    diff = reg.plan({"000660": ("H0STCNT0",)})
    reg.apply(diff)

    # Then: 005930 제거 + 000660 추가
    assert diff.to_add == frozenset({("000660", "H0STCNT0")})
    assert diff.to_remove == frozenset({("005930", "H0STCNT0")})
    assert reg.replay_pairs() == [("000660", "H0STCNT0")]


def test_subscription_over_budget_raises_slot_budget_exceeded():
    # Given: 슬롯 예산 2, desired 페어 3개
    import pytest

    from src.realtime.subscription import SubscriptionRegistry
    from src.universe.policy import SlotBudgetExceededError

    reg = SubscriptionRegistry(slot_budget=2)

    # When / Then: 무음 절단 대신 예외
    with pytest.raises(SlotBudgetExceededError):
        reg.plan({"005930": ("H0STCNT0", "H0STASP0"), "000660": ("H0STCNT0",)})
