"""Invariant guard tests for the regular-session adapter registry."""

from __future__ import annotations

import pytest

from src.core.config import CollectorSettings
from src.core.errors import KrxAlphaError
from src.realtime.adapters import registry
from src.realtime.adapters.registry import (
    ADAPTERS,
    AppCredentials,
    RegularAdapterContext,
    regular_vendor,
)


def test_registry_keys_exact() -> None:
    assert set(ADAPTERS.keys()) == {"ls", "db", "kiwoom"}
    with pytest.raises(TypeError):
        ADAPTERS["xx"] = None  # type: ignore[index]


def test_unknown_vendor_fails_closed() -> None:
    with pytest.raises(KrxAlphaError, match="unsupported realtime vendor: kis"):
        regular_vendor("kis")


def test_ls_requires_market_map(tmp_path) -> None:
    entry = regular_vendor("ls")
    assert entry.requires_market_map is True
    assert regular_vendor("db").requires_market_map is False
    ctx = RegularAdapterContext(
        http=object(),  # type: ignore[arg-type]
        settings=CollectorSettings(),
        token_cache_dir=tmp_path,
        market_of=None,
    )
    with pytest.raises(ValueError, match="market_of"):
        entry.build(AppCredentials(app_key="k", app_secret="s"), ctx)


def test_credentials_repr_redacted() -> None:
    assert "s3cret" not in repr(AppCredentials("k", "s3cret"))
    assert "k" in repr(AppCredentials("k", "s3cret"))


@pytest.mark.parametrize("vendor", ["ls", "db", "kiwoom"])
def test_registry_patch_points_are_module_globals(monkeypatch, tmp_path, vendor) -> None:
    from src.marketdata.toss_token_store import db_token_path, ls_token_path

    seen = {}
    adapter = object()
    store = object()
    http = object()
    market_of = {"005930": "KOSPI"}
    settings = CollectorSettings(
        ls_token_rotation_cooldown_s=123,
        db_token_rotation_cooldown_s=456,
        db_capacity_pairs=12,
        db_ack_timeout_s=3,
        db_subscribe_spacing_s=0.2,
    )

    def _store(path, **kwargs):
        seen["store"] = (path, kwargs)
        return store

    def _adapter(**kwargs):
        seen["adapter"] = kwargs
        return adapter

    monkeypatch.setattr(registry, "TossTokenStore", _store)
    if vendor == "ls":
        monkeypatch.setattr(registry, "LsRealtimeAdapter", _adapter)
    elif vendor == "db":
        monkeypatch.setattr(registry, "DbRealtimeAdapter", _adapter)
    else:
        monkeypatch.setattr(registry, "KiwoomRealtimeAdapter", _adapter)
    if vendor == "ls":
        monkeypatch.setenv("LS_APP_KEY", "key")
        monkeypatch.setenv("LS_APP_SECRET", "secret")
    elif vendor == "db":
        monkeypatch.setenv("DB_APP_KEY", "key")
        monkeypatch.setenv("DB_APP_SECRET", "secret")
    else:
        monkeypatch.setenv("KIWOOM_APP_KEY", "key")
        monkeypatch.setenv("KIWOOM_SECRET_KEY", "secret")
    entry = regular_vendor(vendor)
    creds = entry.load_credentials()
    assert creds == AppCredentials("key", "secret")
    assert entry.build(creds, RegularAdapterContext(http, settings, tmp_path, market_of)) is adapter
    if vendor == "kiwoom":
        assert entry.requires_market_map is False
        assert seen["adapter"]["capacity_pairs"] == settings.kiwoom_capacity_pairs
        assert seen["adapter"]["ack_timeout_s"] == settings.kiwoom_ack_timeout_s
        assert seen["adapter"]["user_agent"] == settings.kiwoom_user_agent
        return
    assert seen["store"] == (
        (ls_token_path if vendor == "ls" else db_token_path)(tmp_path, "key"),
        {"rotation_cooldown_s": 123 if vendor == "ls" else 456},
    )
    expected = {"app_key": "key", "app_secret": "secret", "http": http, "token_store": store}
    if vendor == "ls":
        expected["market_of"] = market_of
        expected["capacity_pairs"] = settings.ls_max_subscriptions_per_session
    else:
        expected.update(capacity_pairs=12, ack_timeout_s=3, subscribe_spacing_s=0.2)
    assert seen["adapter"] == expected
