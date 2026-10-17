"""Regular-session vendor adapter registry."""

from __future__ import annotations

import pathlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

import aiohttp

from src.core.config import CollectorSettings, DbCredentials, LsCredentials, load_credentials
from src.core.errors import KrxAlphaError
from src.marketdata.toss_token_store import TossTokenStore, db_token_path, ls_token_path
from src.realtime.adapters.db import DbRealtimeAdapter
from src.realtime.adapters.ls import LsRealtimeAdapter
from src.realtime.contracts import VendorAdapter
from src.storage.layout import L0_VENDOR_DB


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


def _build_ls(creds: AppCredentials, ctx: RegularAdapterContext) -> VendorAdapter:
    if ctx.market_of is None:
        raise ValueError("market_of is required for ls adapter")
    store = TossTokenStore(
        ls_token_path(ctx.token_cache_dir, creds.app_key),
        rotation_cooldown_s=ctx.settings.ls_token_rotation_cooldown_s,
    )
    return LsRealtimeAdapter(
        app_key=creds.app_key,
        app_secret=creds.app_secret,
        http=ctx.http,
        market_of=ctx.market_of,
        token_store=store,
    )


def _build_db(creds: AppCredentials, ctx: RegularAdapterContext) -> VendorAdapter:
    store = TossTokenStore(
        db_token_path(ctx.token_cache_dir, creds.app_key),
        rotation_cooldown_s=ctx.settings.db_token_rotation_cooldown_s,
    )
    return DbRealtimeAdapter(
        app_key=creds.app_key,
        app_secret=creds.app_secret,
        http=ctx.http,
        token_store=store,
        capacity_pairs=ctx.settings.db_capacity_pairs,
        ack_timeout_s=ctx.settings.db_ack_timeout_s,
        subscribe_spacing_s=ctx.settings.db_subscribe_spacing_s,
    )


ADAPTERS: Mapping[str, RegularVendor] = MappingProxyType(
    {
        "ls": RegularVendor(vendor="ls", load_credentials=_load_ls_credentials, build=_build_ls, requires_market_map=True),
        L0_VENDOR_DB: RegularVendor(vendor=L0_VENDOR_DB, load_credentials=_load_db_credentials, build=_build_db, requires_market_map=False),
    }
)


def regular_vendor(vendor: str) -> RegularVendor:
    """Return the registry entry for a regular-session vendor id."""
    try:
        return ADAPTERS[vendor]
    except KeyError:
        raise KrxAlphaError(f"unsupported realtime vendor: {vendor}") from None


__all__: list[str] = ["ADAPTERS", "AppCredentials", "RegularAdapterContext", "RegularVendor", "regular_vendor"]
