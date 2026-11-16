"""NXT regular-session ranking fetch for the evening symbol pool (Kiwoom boundary, host-shared token)."""

from __future__ import annotations

import datetime as dt

import requests

from deploy.shared.host_broker_contract import AsyncHostPacer, IssuedToken, admission_state_path, rate_for
from src.core.config import KisTokenSettings, KiwoomCredentials, load_credentials
from src.marketdata.host_token_store import STORE_FAILURE_TYPES, kiwoom_issued, make_kiwoom_store
from src.marketdata.kiwoom_ranking import KiwoomRankingRow, fetch_trade_amount_ranking
from src.marketdata.kiwoom_ticks import KiwoomTickTapeError, issue_kiwoom_token

_KIWOOM_VENDOR: str = "kiwoom"
_KIWOOM_SCOPE: str = "ka10032"
_NXT_STEX_TP: str = "2"


def fetch_nxt_trade_amount_ranking_live(*, max_pages: int = 3, timeout_s: float = 10.0) -> tuple[KiwoomRankingRow, ...]:
    """Return the NXT trade-amount ranking rows (vendor order) using the host-shared Kiwoom token and pacer scope.

    Raises:
        KiwoomTickTapeError: Token store failure, or any vendor failure from the ranking pages.
    """
    kiwoom = load_credentials(KiwoomCredentials)
    token_settings = KisTokenSettings()
    pacer = AsyncHostPacer(
        admission_state_path(token_settings.token_cache_dir, _KIWOOM_VENDOR, kiwoom.kiwoom_app_key, _KIWOOM_SCOPE),
        rate_for(_KIWOOM_VENDOR, _KIWOOM_SCOPE),
    )
    token_store = make_kiwoom_store(
        token_settings.token_cache_dir, kiwoom.kiwoom_app_key, clock=lambda: dt.datetime.now(dt.UTC)
    )

    def _issue_token() -> IssuedToken:
        return kiwoom_issued(
            issue_kiwoom_token(
                app_key=kiwoom.kiwoom_app_key,
                secret_key=kiwoom.kiwoom_secret_key,
                session=requests,
                timeout_s=timeout_s,
            )
        )

    try:
        token = token_store.get_or_issue(_issue_token).access_token
    except STORE_FAILURE_TYPES as exc:
        raise KiwoomTickTapeError("kiwoom token store failed") from exc
    return fetch_trade_amount_ranking(
        session=requests,
        access_token=token,
        app_key=kiwoom.kiwoom_app_key,
        pacer=pacer,
        max_pages=max_pages,
        min_trade_value_krw=1,
        timeout_s=timeout_s,
        stex_tp=_NXT_STEX_TP,
    )
