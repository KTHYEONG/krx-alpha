"""주문집행 조립: 설정·자격증명·세션으로 OrderManager 를 구성한다."""

from __future__ import annotations

import datetime as dt
import fcntl
import logging
import time
from collections.abc import Callable
from typing import Any

from src.brokers.kis.auth import KisAppAuth
from src.brokers.kis.stack import build_kis_rest_stack
from src.core.config import ExecutionMode, ExecutionSettings, KisCredentials, KisTokenSettings
from src.execution.contracts import (
    TERMINAL_STATUSES,
    AccountCheckError,
    BrokerIntegrityError,
    ExecutionError,
    Holding,
    KisApiError,
    OrderGateway,
    SessionLockError,
)
from src.execution.gateways import LiveGateway, PaperGateway
from src.execution.journal import OrderJournal
from src.execution.ledger import CostModel, Ledger, Position
from src.execution.oms import OrderManager
from src.execution.recovery import position_diffs, restore_session
from src.execution.risk import RiskLimits

logger = logging.getLogger(__name__)


def build_order_manager(
    *,
    settings: ExecutionSettings,
    creds: KisCredentials,
    session: Any,
    now: Callable[[], dt.datetime],
    clock: Callable[[], float] = time.time,
    sleep: Callable[[float], None] = time.sleep,
) -> OrderManager:
    """Assemble a session-locked, journal-restored manager.

    The pacing clock is wall time (see `build_kis_rest_stack`).
    """
    paths = settings.paths
    lock_path = paths.order_session_lock_file
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = open(lock_path, "w", encoding="utf-8")  # noqa: SIM115 - lock handle outlives the builder; released by OrderManager.close
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        handle.close()
        raise SessionLockError(f"order session already active: {lock_path}") from None
    def _release() -> None:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()

    try:
        journal = OrderJournal(root=paths.order_journal_dir, mode=settings.mode, now=now)
        costs = CostModel(commission_bps=settings.commission_bps, sell_tax_bps=settings.sell_tax_bps)
        token_settings = KisTokenSettings()
        stack = build_kis_rest_stack(
            auth=KisAppAuth(app_key=creds.kis_app_key, app_secret=creds.kis_app_secret),
            cache_dir=token_settings.token_cache_dir,
            session=session,
            now=now,
            rate_per_s=settings.venue_rest_rate_per_s,
            max_lead_s=settings.rest_max_lead_s,
            timeout_s=settings.venue_request_timeout_s,
            allow_issue=token_settings.allow_issue,
            venue=settings.venue,
            clock=clock,
            sleep=sleep,
        )
        quotes = stack.data
        trading = stack.trading_client(creds)
        recovered = restore_session(
            journal.read_day(now().date()), mode=settings.mode, costs=costs
        )
        limits = RiskLimits(
            max_order_notional_krw=settings.max_order_notional_krw,
            max_position_notional_krw=settings.max_position_notional_krw,
            max_orders_per_minute=settings.max_orders_per_minute,
            max_daily_loss_krw=settings.max_daily_loss_krw,
            symbol_whitelist=frozenset(settings.symbol_whitelist),
        )
        holdings: list[Holding] = []
        if settings.mode is ExecutionMode.PAPER:
            holdings = trading.get_holdings()
            ledger = Ledger(cash_krw=settings.paper_initial_cash_krw, costs=costs)
            gateway: OrderGateway = PaperGateway(creds=creds, journal=journal, ledger=ledger)
            manager = OrderManager(
                gateway=gateway,
                quotes=quotes,
                ledger=ledger,
                limits=limits,
                costs=costs,
                journal=journal,
                now=now,
                kill_switch=paths.kill_switch_file.exists,
                recovered=recovered,
                on_close=_release,
            )
        else:
            gateway = LiveGateway(
                client=trading, creds=creds, journal=journal, today=lambda: now().date(), mode=settings.mode
            )
            if recovered.ledger is None:
                holdings = trading.get_holdings()
                ledger = Ledger(
                    cash_krw=0,
                    costs=costs,
                    positions={h.symbol: Position(qty=h.qty, cost_krw=h.cost_krw) for h in holdings},
                )
                manager = OrderManager(
                    gateway=gateway,
                    quotes=quotes,
                    ledger=ledger,
                    limits=limits,
                    costs=costs,
                    journal=journal,
                    now=now,
                    kill_switch=paths.kill_switch_file.exists,
                    recovered=recovered,
                    on_close=_release,
                )
            else:
                manager = OrderManager(
                    gateway=gateway,
                    quotes=quotes,
                    ledger=recovered.ledger,
                    limits=limits,
                    costs=costs,
                    journal=journal,
                    now=now,
                    kill_switch=paths.kill_switch_file.exists,
                    recovered=recovered,
                    on_close=_release,
                )
                diffs: list[tuple[str, int, int]] = []
                for _ in range(settings.position_parity_attempts):
                    if any(o.status not in TERMINAL_STATUSES for o in manager.orders.values()):
                        manager.reconcile()
                    holdings = trading.get_holdings()
                    diffs = position_diffs(manager.ledger, holdings)
                    if not diffs:
                        break
                if diffs:
                    journal.append(
                        "position_mismatch",
                        diffs=[
                            {"symbol": s, "ledger_qty": lq, "broker_qty": bq} for s, lq, bq in diffs
                        ],
                    )
                    detail = ", ".join(f"{s} ledger={lq} broker={bq}" for s, lq, bq in diffs)
                    logger.critical("[EXEC] stage=position_mismatch diffs=%s", detail)
                    manager.block_submits(f"position mismatch: {detail}")
        journal.append(
            "session_start",
            venue=settings.venue.value,
            holdings_count=len(holdings),
            holdings=[{"symbol": h.symbol, "qty": h.qty, "cost_krw": h.cost_krw} for h in holdings],
            restored_orders=len(recovered.orders),
            limits=limits,
        )
        logger.info("[EXEC] stage=session_start mode=%s holdings=%d", settings.mode.value, len(holdings))
        return manager
    except (KisApiError, BrokerIntegrityError) as exc:
        _release()
        raise AccountCheckError(f"account self-check failed: {exc}") from exc
    except ExecutionError:
        _release()
        raise
    except BaseException:
        _release()
        raise
