"""Regular-session vendor adapter registry."""

from __future__ import annotations

import asyncio
import datetime as dt
import pathlib
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

import aiohttp

from deploy.shared.host_broker_contract import token_record_path
from src.core.config import (
    CollectorSettings,
    DbCredentials,
    KisTokenSettings,
    KiwoomCredentials,
    LsCredentials,
    TossCredentials,
    load_credentials,
)
from src.core.errors import KrxAlphaError
from src.marketdata.host_token_store import AsyncRotatingTokenStore, kiwoom_issued, make_kiwoom_store
from src.marketdata.toss_token_store import TossTokenStore, toss_token_path
from src.realtime.adapters.db import DbRealtimeAdapter
from src.realtime.adapters.kiwoom import KiwoomRealtimeAdapter
from src.realtime.adapters.ls import LsNxtRealtimeAdapter, LsRealtimeAdapter
from src.realtime.adapters.toss import TossRealtimeAdapter
from src.realtime.contracts import VendorAdapter
from src.storage.layout import L0_VENDOR_DB, L0_VENDOR_KIWOOM, L0_VENDOR_TOSS

# Registry key of the NXT-venue LS adapter; its journal vendor stays "ls".
LS_NXT_ADAPTER = "ls-nxt"


@dataclass(frozen=True)
class AppCredentials:
    app_key: str
    app_secret: str = field(repr=False)


@dataclass(frozen=True)
class RegularAdapterContext:
    http: aiohttp.ClientSession
    settings: CollectorSettings
    token_cache_dir: pathlib.Path
    market_of: Mapping[str, str] | None
    connect_gate: Callable[[], Awaitable[None]] | None = None


@dataclass(frozen=True)
class RegularVendor:
    vendor: str
    load_credentials: Callable[[], AppCredentials]
    build: Callable[[AppCredentials, RegularAdapterContext], VendorAdapter]
    requires_market_map: bool


def _load_ls_credentials() -> AppCredentials:
    creds = load_credentials(LsCredentials)
    return AppCredentials(app_key=creds.ls_app_key, app_secret=creds.ls_app_secret)


def _load_db_credentials() -> AppCredentials:
    creds = load_credentials(DbCredentials)
    return AppCredentials(app_key=creds.db_app_key, app_secret=creds.db_app_secret)


def _load_kiwoom_credentials() -> AppCredentials:
    creds = load_credentials(KiwoomCredentials)
    return AppCredentials(app_key=creds.kiwoom_app_key, app_secret=creds.kiwoom_secret_key)


def _load_toss_credentials() -> AppCredentials:
    creds = load_credentials(TossCredentials)
    return AppCredentials(app_key=creds.toss_app_key, app_secret=creds.toss_app_secret)


def _ls_token_store(creds: AppCredentials, ctx: RegularAdapterContext) -> AsyncRotatingTokenStore:
    return AsyncRotatingTokenStore(
        token_record_path(ctx.token_cache_dir, "ls", creds.app_key),
        vendor="ls",
        rotation_cooldown_s=ctx.settings.ls_token_rotation_cooldown_s,
        clock=lambda: dt.datetime.now(dt.UTC),
    )


def _build_ls(creds: AppCredentials, ctx: RegularAdapterContext) -> VendorAdapter:
    if ctx.market_of is None:
        raise ValueError("market_of is required for ls adapter")
    return LsRealtimeAdapter(
        app_key=creds.app_key,
        app_secret=creds.app_secret,
        http=ctx.http,
        market_of=ctx.market_of,
        token_store=_ls_token_store(creds, ctx),
        capacity_pairs=ctx.settings.ls_max_subscriptions_per_session,
    )


def _build_ls_nxt(creds: AppCredentials, ctx: RegularAdapterContext) -> VendorAdapter:
    # Same host-shared token store as the KRX LS sessions: one issuer per key.
    return LsNxtRealtimeAdapter(
        app_key=creds.app_key,
        app_secret=creds.app_secret,
        http=ctx.http,
        token_store=_ls_token_store(creds, ctx),
        capacity_pairs=ctx.settings.ls_max_subscriptions_per_session,
    )


def _build_db(creds: AppCredentials, ctx: RegularAdapterContext) -> VendorAdapter:
    store = AsyncRotatingTokenStore(
        token_record_path(ctx.token_cache_dir, "dbsec", creds.app_key),
        vendor="dbsec",
        rotation_cooldown_s=ctx.settings.db_token_rotation_cooldown_s,
        clock=lambda: dt.datetime.now(dt.UTC),
    )
    return DbRealtimeAdapter(
        app_key=creds.app_key,
        app_secret=creds.app_secret,
        http=ctx.http,
        token_store=store,
        capacity_pairs=ctx.settings.db_capacity_pairs,
        ack_timeout_s=ctx.settings.db_ack_timeout_s,
        subscribe_spacing_s=ctx.settings.db_subscribe_spacing_s,
        connect_gate=ctx.connect_gate,
    )


def _build_toss(creds: AppCredentials, ctx: RegularAdapterContext) -> VendorAdapter:
    # Same cache dir and path derivation as the kca REST clients, so one file serves the host-wide single issuer.
    return TossRealtimeAdapter(
        http=ctx.http,
        token_store=TossTokenStore(toss_token_path(ctx.token_cache_dir, creds.app_key)),
        capacity_pairs=ctx.settings.toss_capacity_pairs,
        ack_timeout_s=ctx.settings.toss_ack_timeout_s,
        declare_spacing_s=ctx.settings.toss_declare_spacing_s,
    )


def _build_kiwoom(creds: AppCredentials, ctx: RegularAdapterContext) -> VendorAdapter:
    from src.marketdata.kiwoom_ticks import issue_kiwoom_token

    app_key, secret_key, timeout_s = creds.app_key, creds.app_secret, ctx.settings.kiwoom_ack_timeout_s

    store = make_kiwoom_store(KisTokenSettings().token_cache_dir, creds.app_key, clock=lambda: dt.datetime.now(dt.UTC))

    async def issue_token() -> str:
        def _call() -> str:
            import requests

            with requests.Session() as session:
                return store.get_or_issue(
                    lambda: kiwoom_issued(
                        issue_kiwoom_token(
                            app_key=app_key, secret_key=secret_key, session=session, timeout_s=timeout_s
                        )
                    )
                ).access_token

        return await asyncio.to_thread(_call)

    return KiwoomRealtimeAdapter(
        app_key=creds.app_key,
        secret_key=creds.app_secret,
        http=ctx.http,
        streams=ctx.settings.streams,
        capacity_pairs=ctx.settings.kiwoom_capacity_pairs,
        user_agent=ctx.settings.kiwoom_user_agent,
        ack_timeout_s=ctx.settings.kiwoom_ack_timeout_s,
        issue_token=issue_token,
    )


ADAPTERS: Mapping[str, RegularVendor] = MappingProxyType(
    {
        "ls": RegularVendor(vendor="ls", load_credentials=_load_ls_credentials, build=_build_ls, requires_market_map=True),
        LS_NXT_ADAPTER: RegularVendor(vendor="ls", load_credentials=_load_ls_credentials, build=_build_ls_nxt, requires_market_map=False),
        L0_VENDOR_DB: RegularVendor(vendor=L0_VENDOR_DB, load_credentials=_load_db_credentials, build=_build_db, requires_market_map=False),
        L0_VENDOR_TOSS: RegularVendor(vendor=L0_VENDOR_TOSS, load_credentials=_load_toss_credentials, build=_build_toss, requires_market_map=False),
        L0_VENDOR_KIWOOM: RegularVendor(vendor=L0_VENDOR_KIWOOM, load_credentials=_load_kiwoom_credentials, build=_build_kiwoom, requires_market_map=False),
    }
)


def regular_vendor(vendor: str) -> RegularVendor:
    """Return the registry entry for a regular-session vendor id."""
    try:
        return ADAPTERS[vendor]
    except KeyError:
        raise KrxAlphaError(f"unsupported realtime vendor: {vendor}") from None


__all__: list[str] = ["ADAPTERS", "LS_NXT_ADAPTER", "AppCredentials", "RegularAdapterContext", "RegularVendor", "regular_vendor"]
