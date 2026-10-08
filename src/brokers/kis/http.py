"""KIS safe GET transport (bounded retries, token refresh, pagination)."""

from __future__ import annotations

from enum import StrEnum
from typing import Any

import requests

from src.brokers.kis.auth import KisAppAuth, KisTokenProvider, TokenSource
from src.brokers.kis.rate import Pacer
from src.execution.contracts import KisApiError

_RATE_LIMIT_CODES: frozenset[str] = frozenset({"EGW00201"})
_EXPIRED_TOKEN_CODES: frozenset[str] = frozenset({"EGW00121", "EGW00123"})
MAX_SAFE_RETRIES: int = 2
"""Retry budget for requests the vendor provably did not execute."""
_MAX_PAGES = 100


class KisRetryDecision(StrEnum):
    RETRY_RATE_LIMITED = "retry_rate_limited"
    RETRY_AFTER_TOKEN_REFRESH = "retry_after_token_refresh"  # noqa: S105 - retry decision label, not a secret
    FINAL = "final"


def classify_kis_retry(msg_cd: str, *, retries: int, refreshed: bool) -> KisRetryDecision:
    """Classify a KIS response code into a safe-retry decision."""
    if msg_cd in _RATE_LIMIT_CODES and retries < MAX_SAFE_RETRIES:
        return KisRetryDecision.RETRY_RATE_LIMITED
    if msg_cd in _EXPIRED_TOKEN_CODES and not refreshed:
        return KisRetryDecision.RETRY_AFTER_TOKEN_REFRESH
    return KisRetryDecision.FINAL


class KisGetTransport:
    """Bounded safe-retry GET transport over an authenticated KIS session."""

    def __init__(
        self,
        *,
        auth: KisAppAuth,
        tokens: KisTokenProvider,
        session: Any,
        limiter: Pacer,
        timeout_s: float,
        base_url: str,
    ) -> None:
        self._auth = auth
        self._tokens = tokens
        self._session = session
        self._limiter = limiter
        self._timeout_s = timeout_s
        self._base_url = base_url

    def authorized_headers(self, tr_id: str, tr_cont: str = "") -> tuple[dict[str, str], str]:
        """Build signed KIS headers and return (headers, bearer_token)."""
        token = self._tokens.access_token()
        return (
            {
                "content-type": "application/json; charset=utf-8",
                "authorization": f"Bearer {token}",
                "appkey": self._auth.app_key,
                "appsecret": self._auth.app_secret,
                "tr_id": tr_id,
                "custtype": "P",
                "tr_cont": tr_cont,
            },
            token,
        )

    def headers(self, tr_id: str, tr_cont: str = "") -> dict[str, str]:
        """Signed KIS headers (secrets are never logged); see `authorized_headers`."""
        headers, _ = self.authorized_headers(tr_id, tr_cont)
        return headers

    def refresh_token(self, rejected_token: str) -> str:
        """Resolve a replacement token after vendor rejects the provided token."""
        return self._tokens.access_token(force=True, rejected_token=rejected_token)

    def ensure_token(self) -> TokenSource:
        """Ensure a valid token exists (preflight entry point for data stacks)."""
        return self._tokens.ensure_token()

    def access_token(self, *, force: bool = False, rejected_token: str | None = None) -> str:
        """Return a usable token, refreshing only when expiring."""
        return self._tokens.access_token(force=force, rejected_token=rejected_token)

    def get(
        self,
        path: str,
        tr_id: str,
        params: dict[str, str],
        tr_cont: str = "",
    ) -> tuple[dict[str, Any], str]:
        """Fetch one KIS page with bounded safe retries and token refresh."""
        refreshed = False
        retries = 0
        while True:
            self._limiter.acquire()
            try:
                sent_headers, sent_token = self.authorized_headers(tr_id, tr_cont)
                resp = self._session.get(
                    self._base_url + path,
                    headers=sent_headers,
                    params=params,
                    timeout=self._timeout_s,
                )
                body = resp.json()
            except (requests.RequestException, ValueError) as exc:
                raise KisApiError("TRANSPORT", type(exc).__name__) from exc
            msg_cd = str(body.get("msg_cd", ""))
            decision = classify_kis_retry(msg_cd, retries=retries, refreshed=refreshed)
            if decision is KisRetryDecision.RETRY_RATE_LIMITED:
                retries += 1
                continue
            if decision is KisRetryDecision.RETRY_AFTER_TOKEN_REFRESH:
                self.refresh_token(sent_token)
                refreshed = True
                continue
            if body.get("rt_cd") != "0":
                raise KisApiError(msg_cd, str(body.get("msg1", "")).strip())
            return body, str(resp.headers.get("tr_cont", ""))

    def get_paged(
        self,
        path: str,
        tr_id: str,
        params: dict[str, str],
        list_key: str = "output1",
    ) -> list[dict[str, Any]]:
        """Follow KIS continuation headers within the existing page cap."""
        rows: list[dict[str, Any]] = []
        req_cont = ""
        for _ in range(_MAX_PAGES):
            body, cont = self.get(path, tr_id, params, req_cont)
            rows.extend(body.get(list_key) or [])
            if cont not in ("M", "F"):
                return rows
            params = {
                **params,
                "CTX_AREA_FK100": str(body.get("ctx_area_fk100", "")),
                "CTX_AREA_NK100": str(body.get("ctx_area_nk100", "")),
            }
            req_cont = "N"
        raise KisApiError("PAGINATION", f"exceeded {_MAX_PAGES} pages")
