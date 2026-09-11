
import pytest


@pytest.mark.parametrize(
    ("intent_kw", "quote_kw", "limits_kw", "ctx_kw", "code"),
    [
        ({}, {}, {}, {"kill_switch": True}, "kill_switch"),
        ({}, {}, {}, {"unknown_pending": True}, "unknown_pending"),
        ({}, {}, {"symbol_whitelist": frozenset({"000660"})}, {}, "symbol_not_allowed"),
        ({"qty": 0}, {}, {}, {}, "qty_invalid"),
        ({"order_type": "market"}, {}, {}, {}, "price_invalid"),
        ({}, {"halted": True}, {}, {}, "trading_halted"),
        ({}, {"tick": 5}, {}, {}, "tick_table_mismatch"),
        ({"limit_price": 10_005}, {}, {}, {}, "price_off_tick"),
        ({"limit_price": 13_010}, {}, {}, {}, "price_out_of_band"),
        ({}, {}, {}, {"orders_last_minute": 10}, "order_rate_limit"),
        ({}, {}, {}, {"realized_pnl_krw": -300_000}, "daily_loss_limit"),
        ({"qty": 101}, {}, {}, {}, "notional_limit"),
        ({}, {}, {}, {"position_qty": 295}, "position_limit"),
        ({}, {}, {}, {"orderable_cash_krw": 100_010}, "insufficient_cash"),
        ({"side": "sell"}, {}, {}, {"position_qty": 12, "open_sell_qty": 3}, "insufficient_position"),
    ],
)
def test_check_intent_rejects_each_violation_with_code(intent_kw, quote_kw, limits_kw, ctx_kw, code) -> None:
    # Given: 통과하는 기준 주문(10주 x 10,000원 지정가 매수)에서 한 항목만 위반시킨다
    import dataclasses

    from src.execution.contracts import OrderIntent, OrderType, RejectCode, RiskRejectedError, Side
    from src.execution.risk import RiskContext, RiskLimits, check_intent
    from tests.unit.execution.fakes import make_quote

    changes = dict(intent_kw)
    if "side" in changes:
        changes["side"] = Side(changes["side"])
    if "order_type" in changes:
        changes["order_type"] = OrderType(changes["order_type"])
    intent = dataclasses.replace(
        OrderIntent("005930", Side.BUY, 10, OrderType.LIMIT, 10_000), **changes
    )
    quote = make_quote(**quote_kw)
    limits = dataclasses.replace(
        RiskLimits(
            max_order_notional_krw=1_000_000, max_position_notional_krw=3_000_000,
            max_orders_per_minute=10, max_daily_loss_krw=300_000,
        ),
        **limits_kw,
    )
    ctx = dataclasses.replace(
        RiskContext(
            kill_switch=False, unknown_pending=False, orders_last_minute=0, realized_pnl_krw=0, position_qty=0,
            open_buy_qty=0, open_sell_qty=0, orderable_cash_krw=10_000_000, est_commission_krw=15,
        ),
        **ctx_kw,
    )

    # When / Then
    with pytest.raises(RiskRejectedError) as excinfo:
        check_intent(intent, quote, limits=limits, ctx=ctx)
    assert excinfo.value.code is RejectCode(code)

def test_check_intent_passes_valid_orders_and_allows_risk_reducing_sell_after_loss() -> None:
    # Given
    import dataclasses

    from src.execution.contracts import OrderIntent, OrderType, Side
    from src.execution.risk import RiskContext, RiskLimits, check_intent, reference_price
    from tests.unit.execution.fakes import make_quote

    quote = make_quote()
    limits = RiskLimits(
        max_order_notional_krw=1_000_000, max_position_notional_krw=3_000_000,
        max_orders_per_minute=10, max_daily_loss_krw=300_000,
    )
    ctx = RiskContext(
        kill_switch=False, unknown_pending=False, orders_last_minute=0, realized_pnl_krw=0, position_qty=0,
        open_buy_qty=0, open_sell_qty=0, orderable_cash_krw=10_000_000, est_commission_krw=15,
    )
    buy = OrderIntent("005930", Side.BUY, 10, OrderType.LIMIT, 10_000)
    sell = OrderIntent("005930", Side.SELL, 5, OrderType.MARKET)

    # When / Then: 정상 매수 통과
    assert check_intent(buy, quote, limits=limits, ctx=ctx) is None

    # Then: 일손실 한도 도달 후에도 보유분 매도(위험 축소)는 허용
    loss_ctx = dataclasses.replace(ctx, realized_pnl_krw=-500_000, position_qty=10)
    assert check_intent(sell, quote, limits=limits, ctx=loss_ctx) is None

    # Then: 시장가 매수 기준가 = 상한가(KIS 주문금액 산정 규칙), 시장가 매도 = 현재가
    assert reference_price(OrderIntent("005930", Side.BUY, 1, OrderType.MARKET), quote) == 13_000
    assert reference_price(sell, quote) == 10_000
    assert reference_price(buy, quote) == 10_000
