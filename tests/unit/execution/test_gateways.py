
def test_simulate_paper_fills_sweeps_levels_within_limit() -> None:
    # Given
    from src.execution.contracts import OrderIntent, OrderType, Side
    from src.execution.gateways import simulate_paper_fills
    from tests.unit.execution.fakes import make_quote

    quote = make_quote(asks=((10_010, 3), (10_020, 5)), bids=((10_000, 2), (9_990, 10)))

    # When / Then
    assert simulate_paper_fills(OrderIntent("005930", Side.BUY, 5, OrderType.MARKET), quote) == (5, 3 * 10_010 + 2 * 10_020)
    assert simulate_paper_fills(OrderIntent("005930", Side.BUY, 5, OrderType.LIMIT, 10_010), quote) == (3, 30_030)
    assert simulate_paper_fills(OrderIntent("005930", Side.BUY, 5, OrderType.LIMIT, 10_000), quote) == (0, 0)
    assert simulate_paper_fills(OrderIntent("005930", Side.SELL, 5, OrderType.LIMIT, 9_990), quote) == (5, 2 * 10_000 + 3 * 9_990)
    assert simulate_paper_fills(OrderIntent("005930", Side.SELL, 20, OrderType.MARKET), quote) == (12, 2 * 10_000 + 10 * 9_990)
    assert simulate_paper_fills(OrderIntent("005930", Side.BUY, 1, OrderType.MARKET), make_quote(asks=())) == (0, 0)

def test_paper_gateway_never_posts_and_journals_would_send_payload(tmp_path) -> None:
    # Given: 클라이언트/세션을 아예 주입받지 않는 paper 게이트웨이
    import json

    from src.core.config import ExecutionMode
    from src.execution.contracts import Order, OrderIntent, OrderStatus, OrderType, OutcomeKind, Side
    from src.execution.gateways import PaperGateway
    from src.execution.journal import OrderJournal
    from src.execution.ledger import CostModel, Ledger
    from tests.unit.execution.fakes import T0, FixedClock, make_creds, make_quote

    journal = OrderJournal(root=tmp_path, mode=ExecutionMode.PAPER, now=FixedClock(T0))
    ledger = Ledger(cash_krw=1_000_000, costs=CostModel(commission_bps=1.5, sell_tax_bps=20.0))
    gateway = PaperGateway(creds=make_creds(), journal=journal, ledger=ledger)
    order = Order(
        client_id="paper-20260911-000001", intent=OrderIntent("005930", Side.BUY, 7, OrderType.MARKET),
        status=OrderStatus.PENDING_SUBMIT, created_at=T0,
    )

    # When
    out = gateway.submit(order, make_quote())

    # Then: 가상 주문번호로 접수
    assert gateway.mode is ExecutionMode.PAPER
    assert out.kind is OutcomeKind.ACCEPTED
    assert out.broker_order_no == "P00000001"
    assert out.broker_org_no == "PAPER"

    # Then: 실전과 동일한 TR/바디를 계좌 마스킹 후 기록
    rec = json.loads((tmp_path / "2026-09-11.jsonl").read_text(encoding="utf-8").splitlines()[0])
    assert rec["event"] == "paper_would_send"
    assert rec["client_id"] == "paper-20260911-000001"
    assert rec["tr_id"] == "TTTC0012U"
    assert (rec["body"]["CANO"], rec["body"]["ORD_DVSN"], rec["body"]["ORD_QTY"]) == ("***", "01", "7")
    assert rec["simulated_fill_qty"] == 7
    assert rec["simulated_fill_amount_krw"] == 5 * 10_010 + 2 * 10_020
    assert rec["fill_model"] == "paper_l10_sweep_v1"

    # Then: 모의체결은 브로커 누적체결 형식으로만 노출
    [status] = gateway.fetch_statuses()
    assert (status.broker_order_no, status.filled_qty, status.remaining_qty) == ("P00000001", 7, 0)

def test_paper_gateway_cancel_reserves_cash_and_reports_status(tmp_path) -> None:
    # Given
    import json

    import pytest

    from src.core.config import ExecutionMode
    from src.execution.contracts import ExecutionError, Order, OrderIntent, OrderStatus, OrderType, OutcomeKind, Side
    from src.execution.gateways import PaperGateway
    from src.execution.journal import OrderJournal
    from src.execution.ledger import CostModel, Ledger
    from tests.unit.execution.fakes import T0, FixedClock, make_creds, make_quote

    journal = OrderJournal(root=tmp_path, mode=ExecutionMode.PAPER, now=FixedClock(T0))
    ledger = Ledger(cash_krw=1_000_000, costs=CostModel(commission_bps=1.5, sell_tax_bps=20.0))
    gateway = PaperGateway(creds=make_creds(), journal=journal, ledger=ledger)
    resting = Order(
        client_id="c1", intent=OrderIntent("005930", Side.BUY, 2, OrderType.LIMIT, 9_000),
        status=OrderStatus.PENDING_SUBMIT, created_at=T0,
    )

    # When: 비시장성 지정가 매수 접수
    out = gateway.submit(resting, make_quote())
    resting.broker_order_no = out.broker_order_no
    resting.broker_org_no = out.broker_org_no

    # Then: 미체결 잔량 x 지정가만큼 가용현금 예약
    assert gateway.orderable_cash("005930", 9_000, OrderType.LIMIT) == 1_000_000 - 2 * 9_000
    [status] = gateway.fetch_statuses()
    assert (status.filled_qty, status.remaining_qty, status.cancelled) == (0, 2, False)

    # When / Then: 취소 시 예약 해제
    assert gateway.cancel(resting).kind is OutcomeKind.ACCEPTED
    [status] = gateway.fetch_statuses()
    assert (status.cancelled, status.remaining_qty) == (True, 0)
    assert gateway.orderable_cash("005930", 9_000, OrderType.LIMIT) == 1_000_000

    # When / Then: 전량 체결 주문 취소는 거부, 미지 주문번호는 예외
    filled = Order(
        client_id="c2", intent=OrderIntent("005930", Side.BUY, 1, OrderType.MARKET),
        status=OrderStatus.PENDING_SUBMIT, created_at=T0,
    )
    filled_out = gateway.submit(filled, make_quote())
    filled.broker_order_no = filled_out.broker_order_no
    filled.broker_org_no = filled_out.broker_org_no
    rejected = gateway.cancel(filled)
    assert rejected.kind is OutcomeKind.REJECTED
    assert rejected.code == "ALREADY_FILLED"
    ghost = Order(
        client_id="c3", intent=OrderIntent("005930", Side.BUY, 1, OrderType.MARKET),
        status=OrderStatus.ACCEPTED, created_at=T0, broker_order_no="P99999999", broker_org_no="PAPER",
    )
    with pytest.raises(ExecutionError):
        gateway.cancel(ghost)

    # Then: 기록은 접수된 취소만
    lines = (tmp_path / "2026-09-11.jsonl").read_text(encoding="utf-8").splitlines()
    assert [json.loads(line)["event"] for line in lines] == ["paper_would_send", "paper_would_cancel", "paper_would_send"]

def test_live_gateway_delegates_to_client_and_journals_send(tmp_path) -> None:
    # Given: 네트워크 경계를 대체한 클라이언트
    import datetime as dt
    import json

    from src.core.config import ExecutionMode
    from src.execution.contracts import BrokerOutcome, Order, OrderIntent, OrderStatus, OrderType, OutcomeKind, Side
    from src.execution.gateways import LiveGateway
    from src.execution.journal import OrderJournal
    from tests.unit.execution.fakes import T0, FixedClock, make_creds, make_quote

    class _Client:
        def __init__(self) -> None:
            self.posts: list[tuple[str, dict[str, str]]] = []
            self.days: list[dt.date] = []

        def post_order(self, tr_id, body):
            self.posts.append((tr_id, body))
            return BrokerOutcome(
                kind=OutcomeKind.ACCEPTED, broker_order_no="0000117057", broker_org_no="91252", order_time="090001"
            )

        def get_daily_orders(self, day):
            self.days.append(day)
            return []

        def get_orderable_cash(self, symbol, price, order_type):
            return 850_000

    client = _Client()
    journal = OrderJournal(root=tmp_path, mode=ExecutionMode.LIVE, now=FixedClock(T0))
    gateway = LiveGateway(client=client, creds=make_creds(), journal=journal, today=lambda: dt.date(2026, 9, 11))
    order = Order(
        client_id="live-20260911-000001", intent=OrderIntent("005930", Side.SELL, 2, OrderType.LIMIT, 10_000),
        status=OrderStatus.PENDING_SUBMIT, created_at=T0,
    )

    # When
    out = gateway.submit(order, make_quote())
    order.broker_order_no = out.broker_order_no
    order.broker_org_no = out.broker_org_no
    gateway.cancel(order)

    # Then
    assert gateway.mode is ExecutionMode.LIVE
    assert client.posts[0][0] == "TTTC0011U"
    assert client.posts[0][1]["SLL_TYPE"] == "01"
    assert client.posts[1][0] == "TTTC0013U"
    assert client.posts[1][1]["ORGN_ODNO"] == "0000117057"
    assert gateway.fetch_statuses() == []
    assert client.days == [dt.date(2026, 9, 11)]
    assert gateway.orderable_cash("005930", 10_000, OrderType.LIMIT) == 850_000
    events = [json.loads(line) for line in (tmp_path / "2026-09-11.jsonl").read_text(encoding="utf-8").splitlines()]
    assert [e["event"] for e in events] == ["live_send", "live_send"]
    assert events[0]["tr_id"] == "TTTC0011U"
    assert events[0]["body"]["CANO"] == "***"
