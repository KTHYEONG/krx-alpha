
def test_transition_allows_forward_and_blocks_terminal_reversal() -> None:
    # Given
    import pytest

    from src.execution.contracts import IllegalTransitionError, OrderStatus, transition

    # When / Then: 전방 전이와 멱등 전이
    assert transition(OrderStatus.PENDING_SUBMIT, OrderStatus.ACCEPTED) is OrderStatus.ACCEPTED
    assert transition(OrderStatus.PENDING_SUBMIT, OrderStatus.UNKNOWN) is OrderStatus.UNKNOWN
    assert transition(OrderStatus.UNKNOWN, OrderStatus.FILLED) is OrderStatus.FILLED
    assert transition(OrderStatus.ACCEPTED, OrderStatus.REJECTED) is OrderStatus.REJECTED
    assert transition(OrderStatus.PARTIALLY_FILLED, OrderStatus.PARTIALLY_FILLED) is OrderStatus.PARTIALLY_FILLED
    assert transition(OrderStatus.FILLED, OrderStatus.FILLED) is OrderStatus.FILLED

    # Then: 역전이/도약 전이 금지
    for bad_from, bad_to in [
        (OrderStatus.FILLED, OrderStatus.ACCEPTED),
        (OrderStatus.CANCELLED, OrderStatus.FILLED),
        (OrderStatus.REJECTED, OrderStatus.ACCEPTED),
        (OrderStatus.PARTIALLY_FILLED, OrderStatus.ACCEPTED),
        (OrderStatus.PENDING_SUBMIT, OrderStatus.FILLED),
    ]:
        with pytest.raises(IllegalTransitionError):
            transition(bad_from, bad_to)

def test_derive_status_maps_broker_cumulative_fields() -> None:
    # Given: 10주 지정가 매수의 브로커 행
    import dataclasses

    import pytest

    from src.execution.contracts import BrokerIntegrityError, BrokerOrderStatus, OrderStatus, Side, derive_status

    base = BrokerOrderStatus(
        broker_order_no="0000000001", original_order_no="", org_no="91252", symbol="005930", side=Side.BUY,
        ordered_qty=10, price=10_000, filled_qty=0, filled_amount_krw=0, remaining_qty=10, rejected_qty=0,
        cancelled=False, order_time="090001",
    )

    def status_of(**changes):
        return derive_status(dataclasses.replace(base, **changes), has_cancel_child=False)

    # When / Then
    assert status_of() is OrderStatus.ACCEPTED
    assert status_of(filled_qty=4, filled_amount_krw=40_000, remaining_qty=6) is OrderStatus.PARTIALLY_FILLED
    assert status_of(filled_qty=10, filled_amount_krw=100_000, remaining_qty=0) is OrderStatus.FILLED
    assert status_of(filled_qty=4, filled_amount_krw=40_000, remaining_qty=0, cancelled=True) is OrderStatus.CANCELLED
    assert status_of(rejected_qty=10, remaining_qty=0) is OrderStatus.REJECTED
    assert status_of(remaining_qty=0) is OrderStatus.CANCELLED
    assert derive_status(base, has_cancel_child=True) is OrderStatus.CANCELLED
    with pytest.raises(BrokerIntegrityError):
        status_of(filled_qty=11, filled_amount_krw=110_000, remaining_qty=0)
