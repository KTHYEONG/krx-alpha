
def test_ledger_round_trip_accounts_commission_and_sell_tax() -> None:
    # Given: 수수료 1.5bp, 매도 거래세 20bp
    from src.execution.contracts import Side
    from src.execution.ledger import CostModel, Ledger

    ledger = Ledger(cash_krw=1_000_000, costs=CostModel(commission_bps=1.5, sell_tax_bps=20.0))

    # When: 10주 100,000원 매수
    assert ledger.apply_fill("005930", Side.BUY, 10, 100_000) == 0

    # Then: 현금 = 1,000,000 - 100,000 - 15
    assert ledger.cash_krw == 899_985
    assert ledger.qty("005930") == 10
    assert ledger.positions["005930"].cost_krw == 100_015

    # When: 전량 110,000원 매도 (수수료 16, 거래세 220)
    pnl = ledger.apply_fill("005930", Side.SELL, 10, 110_000)

    # Then
    assert pnl == 9_749
    assert ledger.realized_pnl_krw == 9_749
    assert ledger.cash_krw == 1_009_749
    assert ledger.qty("005930") == 0
    assert "005930" not in ledger.positions

def test_ledger_rejects_oversell_and_nonpositive_fill() -> None:
    # Given: 3주 보유(원가 30,000)
    import pytest

    from src.execution.contracts import BrokerIntegrityError, Side
    from src.execution.ledger import CostModel, Ledger, Position

    ledger = Ledger(
        cash_krw=0,
        costs=CostModel(commission_bps=0.0, sell_tax_bps=20.0),
        positions={"005930": Position(qty=3, cost_krw=30_000)},
    )

    # When / Then: 불가능한 체결은 원장을 오염시키지 않고 거부
    with pytest.raises(BrokerIntegrityError):
        ledger.apply_fill("005930", Side.SELL, 4, 40_000)
    with pytest.raises(BrokerIntegrityError):
        ledger.apply_fill("005930", Side.BUY, 0, 0)
    with pytest.raises(BrokerIntegrityError):
        ledger.apply_fill("005930", Side.BUY, 1, -1)

    # When: 1주 12,000원 매도 (거래세 24, 원가 30,000*1//3=10,000)
    pnl = ledger.apply_fill("005930", Side.SELL, 1, 12_000)

    # Then
    assert pnl == 12_000 - 24 - 10_000
    assert ledger.positions["005930"].cost_krw == 20_000
    assert ledger.cash_krw == 11_976
