"""KIS safe GET transport (bounded retries, token refresh, pagination)."""

from __future__ import annotations

from typing import Any

import requests

from src.brokers.kis.auth import KisAppAuth, KisTokenProvider, TokenSource
from src.brokers.kis.rate import RateLimiter
from src.execution.contracts import KisApiError

_RATE_LIMIT_CODES: frozenset[str] = frozenset({"EGW00201"})
_EXPIRED_TOKEN_CODES: frozenset[str] = frozenset({"EGW00121", "EGW00123"})
_MAX_SAFE_RETRIES = 2
_MAX_PAGES = 100


class KisGetTransport:
    """Bounded safe-retry GET transport over an authenticated KIS session."""

    def __init__(
        self,
        *,
        auth: KisAppAuth,
        tokens: KisTokenProvider,
        session: Any,
        limiter: RateLimiter,
        timeout_s: float,
        base_url: str,
    ) -> None:
        self._auth = auth
        self._tokens = tokens
        self._session = session
        self._limiter = limiter
        self._timeout_s = timeout_s
        self._base_url = base_url

    def headers(self, tr_id: str, tr_cont: str = "") -> dict[str, str]:
        """Build authenticated KIS request headers without logging secrets."""
        token = self._tokens.access_token()
        return {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {token}",
            "appkey": self._auth.app_key,
            "appsecret": self._auth.app_secret,
            "tr_id": tr_id,
            "custtype": "P",
            "tr_cont": tr_cont,
        }

    def refresh_token(self) -> str:
        """Force a token refresh for unsafe POST retry paths."""
        return self._tokens.access_token(force=True)

    def ensure_token(self) -> TokenSource:
        """Ensure a valid token exists (preflight entry point for data stacks)."""
        return self._tokens.ensure_token()

    def access_token(self, *, force: bool = False) -> str:
        """Return a usable token, refreshing only when expiring."""
        return self._tokens.access_token(force=force)

    def get(
        self, path: str, tr_id: str, params: dict[str, str],
        tr_cont: str = "",
    ) -> tuple[dict[str, Any], str]:
        """Fetch one KIS page with bounded safe retries and token refresh."""
        refreshed = False
        retries = 0
        while True:
            self._limiter.acquire()
            try:
                resp = self._session.get(
                    self._base_url + path,
                    headers=self.headers(tr_id, tr_cont),
                    params=params,
                    timeout=self._timeout_s,
                )
                body = resp.json()
            except (requests.RequestException, ValueError) as exc:
                raise KisApiError("TRANSPORT", type(exc).__name__) from exc
            msg_cd = str(body.get("msg_cd", ""))
            if msg_cd in _RATE_LIMIT_CODES and retries < _MAX_SAFE_RETRIES:
                retries += 1
                continue
            if msg_cd in _EXPIRED_TOKEN_CODES and not refreshed:
                self._tokens.access_token(force=True)
                refreshed = True
                continue
            if body.get("rt_cd") != "0":
                raise KisApiError(msg_cd, str(body.get("msg1", "")).strip())
            return body, str(resp.headers.get("tr_cont", ""))

    def get_paged(
        self, path: str, tr_id: str, params: dict[str, str],
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
