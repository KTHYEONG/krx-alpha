"""Single assembly point for a host-paced KIS REST stack over one app key."""

from __future__ import annotations

import datetime as dt
import pathlib
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from src.brokers.kis.auth import KisAppAuth, KisTokenProvider, kis_token_cache_path
from src.brokers.kis.data import KisDataClient
from src.brokers.kis.http import KisGetTransport
from src.brokers.kis.rate import HostPacedRateLimiter, kis_state_path
from src.brokers.kis.trading import KIS_LIVE_BASE_URL, KisTradingClient
from src.core.config import KisCredentials, KisTokenSettings, SnapshotSettings
from src.core.errors import MissingCredentialsError
from src.realtime.kis_sharding import KisDataCredential


@dataclass(frozen=True)
class KisRestStack:
    """One app key's token provider, host pacer, and GET transport.

    The data client and (on demand) the trading client share these so every
    request and token issuance for the key passes the same pacer and token cache.
    """

    app_key: str
    tokens: KisTokenProvider
    limiter: HostPacedRateLimiter
    transport: KisGetTransport
    data: KisDataClient
    session: Any
    timeout_s: float
    base_url: str

    def trading_client(self, credentials: KisCredentials) -> KisTradingClient:
        """Account/order client over this stack's transport, pacer, session, timeout, and base URL.

        Raises:
            ValueError: When ``credentials.kis_app_key`` differs from the stack's key;
                orders would otherwise carry one key's token under another key's account.
        """
        if credentials.kis_app_key != self.app_key:
            raise ValueError("trading credentials app key does not match the stack app key")
        return KisTradingClient(
            transport=self.transport,
            session=self.session,
            credentials=credentials,
            limiter=self.limiter,
            timeout_s=self.timeout_s,
            base_url=self.base_url,
        )


def build_kis_rest_stack(
    *,
    auth: KisAppAuth,
    cache_dir: pathlib.Path,
    session: Any,
    now: Callable[[], dt.datetime],
    rate_per_s: float,
    max_lead_s: float | None,
    timeout_s: float,
    allow_issue: bool,
    base_url: str = KIS_LIVE_BASE_URL,
    clock: Callable[[], float] = time.time,
    sleep: Callable[[float], None] = time.sleep,
) -> KisRestStack:
    """Assemble the stack with on-disk paths derived from ``auth.app_key`` under ``cache_dir``.

    The token cache (``kis_token_cache_path``) and pacing state (``kis_state_path``)
    names are a protocol shared with KCA. ``cache_dir`` and ``allow_issue`` are
    explicit so the builder never reads environment settings. ``clock`` must be wall
    time (epoch seconds) because the pacing file is a host-wide ledger.

    Raises:
        ValueError: Non-positive rate/lead or empty app key.
        HostPacingNotSharedError: Container without the host admission marker.
    """
    if not auth.app_key:
        raise ValueError("app_key must be non-empty")
    cache_path = kis_token_cache_path(cache_dir, auth.app_key)
    state_path = kis_state_path(cache_dir, auth.app_key)
    limiter = HostPacedRateLimiter(state_path, rate_per_s, max_lead_s=max_lead_s, clock=clock, sleep=sleep)
    tokens = KisTokenProvider(
        auth=auth,
        session=session,
        cache_path=cache_path,
        limiter=limiter,
        now=now,
        timeout_s=timeout_s,
        base_url=base_url,
        allow_issue=allow_issue,
    )
    transport = KisGetTransport(
        auth=auth,
        tokens=tokens,
        session=session,
        limiter=limiter,
        timeout_s=timeout_s,
        base_url=base_url,
    )
    data = KisDataClient(transport=transport)
    return KisRestStack(
        app_key=auth.app_key,
        tokens=tokens,
        limiter=limiter,
        transport=transport,
        data=data,
        session=session,
        timeout_s=timeout_s,
        base_url=base_url,
    )


def build_kis_data_slot_stack(
    *,
    snapshot: SnapshotSettings,
    credentials: Sequence[KisDataCredential],
    token_settings: KisTokenSettings,
    session: Any,
    now: Callable[[], dt.datetime],
) -> tuple[KisRestStack, str]:
    """Assemble the account-free REST stack for the snapshot data slot.

    Selects the credential whose ``slot`` equals ``snapshot.kis_data_slot`` and builds it
    with the snapshot pacing (``rest_rate_per_s``, ``rest_max_lead_s``) and
    ``request_timeout_s``, the token cache directory and ``allow_issue`` from
    ``token_settings``. Credentials and settings are explicit so the builder never reads
    environment settings.

    Returns:
        The stack and the selected credential's ``key_id`` (app-key fingerprint).

    Raises:
        MissingCredentialsError: No credential for the configured slot.
        ValueError: Non-positive rate/lead or empty app key (from build_kis_rest_stack).
    """
    cred = next((c for c in credentials if c.slot == snapshot.kis_data_slot), None)
    if cred is None:
        raise MissingCredentialsError(f"no data credential for slot {snapshot.kis_data_slot}")
    stack = build_kis_rest_stack(
        auth=KisAppAuth(cred.app_key, cred.app_secret),
        cache_dir=token_settings.token_cache_dir,
        session=session,
        now=now,
        rate_per_s=snapshot.rest_rate_per_s,
        max_lead_s=snapshot.rest_max_lead_s,
        timeout_s=snapshot.request_timeout_s,
        allow_issue=token_settings.allow_issue,
    )
    return stack, cred.key_id
