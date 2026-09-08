def test_symbol_budget_derives_from_capacity_and_stream_count() -> None:
    from src.collector.vendor import SubscriptionPlanner, VendorCapacity

    planner = SubscriptionPlanner(streams=('H0STCNT0', 'H0STASP0'))

    budget = planner.symbol_budget([VendorCapacity('ls', 200), VendorCapacity('kis', 41)])

    assert budget == 120
def test_plan_fills_primary_vendor_with_all_symbol_stream_pairs() -> None:
    from src.collector.vendor import SubscriptionPlanner, VendorCapacity

    planner = SubscriptionPlanner(streams=('H0STCNT0', 'H0STASP0'))

    out = planner.plan(['005930', '000660'], primary=[VendorCapacity('ls', 200)])

    assert out == {'ls': [('000660', 'H0STASP0'), ('000660', 'H0STCNT0'), ('005930', 'H0STASP0'), ('005930', 'H0STCNT0')]}
def test_plan_raises_slot_budget_exceeded_when_primary_capacity_too_small() -> None:
    import pytest
    from src.collector.vendor import SubscriptionPlanner, VendorCapacity
    from src.universe.policy import SlotBudgetExceededError

    planner = SubscriptionPlanner(streams=('H0STCNT0', 'H0STASP0'))

    with pytest.raises(SlotBudgetExceededError):
        planner.plan(['005930', '000660'], primary=[VendorCapacity('ls', 2)])
def test_plan_assigns_top_n_symbols_to_crosscheck_vendor() -> None:
    from src.collector.vendor import SubscriptionPlanner, VendorCapacity

    planner = SubscriptionPlanner(streams=('H0STCNT0', 'H0STASP0'))

    out = planner.plan(
        ['005930', '000660', '035420'],
        primary=[VendorCapacity('ls', 200)],
        crosscheck=[(VendorCapacity('kis', 41), 1)],
    )

    assert out['kis'] == [('005930', 'H0STASP0'), ('005930', 'H0STCNT0')]
    assert len(out['ls']) == 6
