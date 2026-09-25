"""KIS 실전 REST 클라이언트: 시세·잔고 조회와 주문 POST 경계 (분리 파사드)."""

from __future__ import annotations

import datetime as dt
import pathlib
from collections.abc import Callable
from typing import Any

from src.brokers.kis.auth import (
    KisAppAuth,
    KisTokenProvider,
    TokenSource,
    kis_app_key_fingerprint,
    kis_token_cache_path,
)
from src.brokers.kis.data import (
    TR_ASKING,
    TR_DAILY_CHART,
    TR_FLUCTUATION,
    TR_INDEX_MINUTE_CHART,
    TR_INDEX_PRICE,
    TR_INVESTOR_ESTIMATE,
    TR_MINUTE_CHART,
    TR_NEWS_TITLE,
    TR_PRICE,
    TR_PROGRAM_TRADE,
    TR_TRADE_AMOUNT,
    KisDataClient,
    KisRankingRow,
)
from src.brokers.kis.http import KisGetTransport
from src.brokers.kis.rate import RateLimiter
from src.brokers.kis.trading import (
    KIS_LIVE_BASE_URL,
    TR_BALANCE,
    TR_BUY,
    TR_CANCEL,
    TR_DAILY_ORDERS,
    TR_ORDERABLE,
    TR_SELL,
    KisTradingClient,
    build_cancel_body,
    build_order_body,
    order_tr_id,
)
from src.core.config import KisCredentials
from src.execution.contracts import (
    BrokerOrderStatus,
    BrokerOutcome,
    Holding,
    OrderType,
    Quote,
)

__all__ = [
    "KIS_LIVE_BASE_URL",
    "TR_ASKING",
    "TR_BALANCE",
    "TR_BUY",
    "TR_CANCEL",
    "TR_DAILY_CHART",
    "TR_DAILY_ORDERS",
    "TR_FLUCTUATION",
    "TR_INDEX_MINUTE_CHART",
    "TR_INDEX_PRICE",
    "TR_INVESTOR_ESTIMATE",
    "TR_MINUTE_CHART",
    "TR_NEWS_TITLE",
    "TR_ORDERABLE",
    "TR_PRICE",
    "TR_PROGRAM_TRADE",
    "TR_SELL",
    "TR_TRADE_AMOUNT",
    "KisDataClient",
    "KisRankingRow",
    "KisRestClient",
    "KisTradingClient",
    "RateLimiter",
    "TokenSource",
    "build_cancel_body",
    "build_order_body",
    "kis_app_key_fingerprint",
    "kis_token_cache_path",
    "order_tr_id",
]


class KisRestClient:
    """구 호출 호환 파사드: 읽기/주문을 두 전용 클라이언트에 위임한다."""

    def __init__(
        self,
        *,
        creds: KisCredentials,
        session: Any,
        token_cache_path: pathlib.Path,
        limiter: RateLimiter,
        now: Callable[[], dt.datetime],
        timeout_s: float,
        base_url: str = KIS_LIVE_BASE_URL,
        allow_token_issue: bool = True,
    ) -> None:
        self._creds = creds
        self._session = session
        self._token_cache_path = token_cache_path
        self._limiter = limiter
        self._now = now
        self._timeout_s = timeout_s
        self._base_url = base_url
        self._allow_token_issue = allow_token_issue
        auth = KisAppAuth(creds.kis_app_key, creds.kis_app_secret)
        self._tokens = KisTokenProvider(
            auth=auth,
            session=session,
            cache_path=token_cache_path,
            limiter=limiter,
            now=now,
            timeout_s=timeout_s,
            base_url=base_url,
            allow_issue=allow_token_issue,
        )
        self._get_transport = KisGetTransport(
            auth=auth,
            tokens=self._tokens,
            session=session,
            limiter=limiter,
            timeout_s=timeout_s,
            base_url=base_url,
        )
        self._data = KisDataClient(transport=self._get_transport)
        self._trading = KisTradingClient(
            transport=self._get_transport,
            session=session,
            credentials=creds,
            limiter=limiter,
            timeout_s=timeout_s,
            base_url=base_url,
        )

    def ensure_token(self) -> TokenSource:
        """Make a usable access token available and report where it came from.

        Used by the pre-session preflight so a missing token is detected (and,
        when issuance is allowed, repaired) before collection starts instead of
        at the first data request.

        Returns:
            ``TokenSource.CACHE`` when a valid token was already cached or held
            in memory; ``TokenSource.ISSUED`` when this call issued a new one.

        Raises:
            KisApiError: ``TOKEN_CACHE`` when no valid token exists and issuance is
                disabled; ``TOKEN_BACKOFF`` while a previous issuance failure for
                this key is inside the retry backoff; ``TOKEN_DAILY_LIMIT`` when
                the cache shows an issuance today but the token is unusable; or
                the vendor error code when issuance is rejected.
        """
        return self._tokens.ensure_token()

    def access_token(self, *, force: bool = False) -> str:
        """캐시 토큰을 반환하고 만료 임박 시에만 재발급한다."""
        return self._tokens.access_token(force=force)

    def _headers(self, tr_id: str, tr_cont: str) -> dict[str, str]:
        return self._get_transport.headers(tr_id, tr_cont)

    def _get(
        self, path: str, tr_id: str, params: dict[str, str], tr_cont: str = ""
    ) -> tuple[dict[str, Any], str]:
        return self._get_transport.get(path, tr_id, params, tr_cont)

    def _get_paged(
        self, path: str, tr_id: str, params: dict[str, str], list_key: str = "output1"
    ) -> list[dict[str, Any]]:
        return self._get_transport.get_paged(path, tr_id, params, list_key)

    def get_quote(self, symbol: str) -> Quote:
        return self._data.get_quote(symbol)

    def get_daily_bar(self, symbol: str, day: dt.date) -> dict[str, str] | None:
        return self._data.get_daily_bar(symbol, day)

    def get_holdings(self) -> list[Holding]:
        return self._trading.get_holdings()

    def get_orderable_cash(self, symbol: str, price: int, order_type: OrderType) -> int:
        return self._trading.get_orderable_cash(symbol, price, order_type)

    def get_daily_orders(self, day: dt.date) -> list[BrokerOrderStatus]:
        return self._trading.get_daily_orders(day)

    def post_order(self, tr_id: str, body: dict[str, str]) -> BrokerOutcome:
        return self._trading.post_order(tr_id, body)

    def get_trade_amount_ranking(self) -> tuple[KisRankingRow, ...]:
        return self._data.get_trade_amount_ranking()

    def get_fluctuation_ranking(self) -> tuple[KisRankingRow, ...]:
        return self._data.get_fluctuation_ranking()

    def get_security_status(self, symbol: str) -> dict[str, object]:
        return self._data.get_security_status(symbol)

    def get_auction_book(self, symbol: str) -> dict[str, object]:
        return self._data.get_auction_book(symbol)

    def get_investor_estimate(self, symbol: str) -> tuple[dict[str, object], ...]:
        return self._data.get_investor_estimate(symbol)

    def get_program_trade_latest(self, symbol: str) -> dict[str, object] | None:
        return self._data.get_program_trade_latest(symbol)

    def get_index_snapshot(self, index_code: str) -> dict[str, object]:
        return self._data.get_index_snapshot(index_code)

    def get_index_minute_bars(
        self, index_code: str, *, session_date: dt.date
    ) -> tuple[dict[str, object], ...]:
        return self._data.get_index_minute_bars(index_code, session_date=session_date)

    def get_stock_minute_bars(
        self, symbol: str, *, session_date: dt.date, session_open: dt.time, session_close: dt.time
    ) -> tuple[dict[str, object], ...]:
        return self._data.get_stock_minute_bars(
            symbol, session_date=session_date, session_open=session_open, session_close=session_close
        )

    def get_news_titles(
        self, *, before: tuple[str, str] | None = None
    ) -> tuple[dict[str, object], ...]:
        return self._data.get_news_titles(before=before)
