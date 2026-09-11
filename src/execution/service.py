"""주문집행 조립: 설정·자격증명·세션으로 OrderManager 를 구성한다."""

from __future__ import annotations

import datetime as dt
import logging
import time
from collections.abc import Callable
from typing import Any

from src.core.config import ExecutionMode, ExecutionSettings, KisCredentials
from src.execution.contracts import AccountCheckError, KisApiError, OrderGateway
from src.execution.gateways import LiveGateway, PaperGateway
from src.execution.journal import OrderJournal
from src.execution.kis_client import KisRestClient, RateLimiter
from src.execution.ledger import CostModel, Ledger, Position
from src.execution.oms import OrderManager
from src.execution.risk import RiskLimits

logger = logging.getLogger(__name__)


def build_order_manager(
    *,
    settings: ExecutionSettings,
    creds: KisCredentials,
    session: Any,
    now: Callable[[], dt.datetime],
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> OrderManager:
    """실계좌 자기검증 후 모드별 게이트웨이로 매니저를 조립한다."""
    paths = settings.paths
    journal = OrderJournal(root=paths.order_journal_dir, mode=settings.mode, now=now)
    limiter = RateLimiter(settings.rest_rate_per_s, clock=clock, sleep=sleep)
    client = KisRestClient(
        creds=creds,
        session=session,
        token_cache_path=paths.kis_token_cache,
        limiter=limiter,
        now=now,
        timeout_s=settings.request_timeout_s,
    )
    try:
        holdings = client.get_holdings()
    except KisApiError as exc:
        raise AccountCheckError(f"account self-check failed: {exc.msg_cd}") from exc
    costs = CostModel(commission_bps=settings.commission_bps, sell_tax_bps=settings.sell_tax_bps)
    if settings.mode is ExecutionMode.PAPER:
        ledger = Ledger(cash_krw=settings.paper_initial_cash_krw, costs=costs)
        gateway: OrderGateway = PaperGateway(creds=creds, journal=journal, ledger=ledger)
    else:
        ledger = Ledger(
            cash_krw=0,
            costs=costs,
            positions={h.symbol: Position(qty=h.qty, cost_krw=h.cost_krw) for h in holdings},
        )
        gateway = LiveGateway(client=client, creds=creds, journal=journal, today=lambda: now().date())
    limits = RiskLimits(
        max_order_notional_krw=settings.max_order_notional_krw,
        max_position_notional_krw=settings.max_position_notional_krw,
        max_orders_per_minute=settings.max_orders_per_minute,
        max_daily_loss_krw=settings.max_daily_loss_krw,
        symbol_whitelist=frozenset(settings.symbol_whitelist),
    )
    journal.append("session_start", holdings_count=len(holdings), limits=limits)
    logger.info("[EXEC] stage=session_start mode=%s holdings=%d", settings.mode.value, len(holdings))
    return OrderManager(
        gateway=gateway,
        quotes=client,
        ledger=ledger,
        limits=limits,
        costs=costs,
        journal=journal,
        now=now,
        kill_switch=paths.kill_switch_file.exists,
    )
