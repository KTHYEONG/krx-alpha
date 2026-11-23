"""설정 단일 소스: 경로·자격증명·용량/예산 파라미터의 타입드 스키마."""

from __future__ import annotations

import datetime as dt
import os
import pathlib
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from itertools import pairwise
from typing import Any, Final, TypeVar

from pydantic import AliasChoices, Field, ValidationError, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.core.calendar import SessionSchedule
from src.core.errors import LiveNotArmedError, MissingCredentialsError, SlotBudgetExceededError, VenueCredentialError
from src.core.paths import (
    DEFAULT_DATA_ROOT,
    DEFAULT_HOST_BACKUP_STATUS_PATH,
    DEFAULT_KIS_TOKEN_CACHE_DIR,
    DataPaths,
)
from src.core.session_anchors import (
    STANDARD_CLOSING_AUCTION,
    STANDARD_NXT_AFTER_MARKET_OPEN,
    STANDARD_REGULAR_CLOSE,
)

__all__ = ["DataPaths"]

SettingsT = TypeVar("SettingsT", bound=BaseSettings)

RUN_ID_ENV: str = "KRX_ALPHA_RUN_ID"

# LS vendor session/subscription ceilings (notices 2026-09-23 and 2026-10-16).
# Single source for the defaults; env may only override via the settings fields below.
LS_MAX_SESSIONS: int = 3
LS_MAX_SUBSCRIPTIONS_PER_SESSION: int = 200
# LS closes the whole socket after one rejected subscribe (2026-11-23 probe); a rejected pair waits this long before a resend.
LS_REJECTED_PAIR_HOLD_S: float = 1800.0
# Market-state frames (JIF status, VI_ volatility interruption) ride one symbol on this LS shard; the symbol is the
# only subscription budget the market lane costs.
LS_MARKET_STATE_SHARD: Final[int] = 1
LS_MARKET_STATE_PAIRS: Final[tuple[tuple[str, str], ...]] = (("0", "LS_JIF"), ("000000", "LS_VI"))
LS_MARKET_STATE_STREAMS: Final[frozenset[str]] = frozenset({stream for _, stream in LS_MARKET_STATE_PAIRS})
# Per-symbol silence watch (observe-only): cross-source gap thresholds in seconds, by stream kind.
SYMBOL_SILENCE_CROSS_GAP_S: Final[Mapping[str, float]] = {"trade": 15.0, "quote": 20.0}
SYMBOL_SILENCE_SINGLE_MIN_S: Final[float] = 90.0
SYMBOL_SILENCE_SINGLE_K: Final[float] = 12.0
SYMBOL_SILENCE_OPEN_GRACE_S: Final[float] = 60.0
SYMBOL_SILENCE_FEED_STALE_S: Final[float] = 30.0
SYMBOL_SILENCE_REPORT_S: Final[float] = 60.0
SYMBOL_SILENCE_SUSPECT_CAP: Final[int] = 20
# REST confirmation of quiet suspects on the market-sweep child (observe-only).
QUIET_CHECK_BATCH: Final[int] = 10
QUIET_CHECK_COOLDOWN_S: Final[float] = 120.0
QUIET_CHECK_FILE_STALE_S: Final[float] = 30.0
QUIET_CHECK_MARGIN_S: Final[float] = 3.0
# 2026-09-14 L0 measurement: off-regular silence reached 450 s on normal days.
OFF_REGULAR_MAX_NORMAL_SILENCE_S: float = 450.0


def ls_market_state_symbols(stream_count: int) -> int:
    """Symbol slots the market-state pairs occupy in one LS session carrying ``stream_count`` streams per symbol."""
    if stream_count < 1:
        raise ValueError(f"stream_count {stream_count} must be >= 1")
    return -(-len(LS_MARKET_STATE_PAIRS) // stream_count)


class KisVenue(StrEnum):
    """KIS server environment an app key is issued for."""

    LIVE = "live"
    MOCK = "mock"


class ExecutionMode(StrEnum):
    """주문집행 모드 (기본 paper fail-closed)."""

    PAPER = "paper"
    MOCK = "mock"
    LIVE = "live"


TOSS_LOGICAL_STREAMS: Final[frozenset[str]] = frozenset({"H0STCNT0", "H0STASP0"})


class CollectorSettings(BaseSettings):
    """수집기 전역 설정 (env_prefix='KRX_ALPHA_')."""

    model_config = SettingsConfigDict(env_prefix="KRX_ALPHA_", extra="ignore")

    feed_health_interval_s: float = Field(default=5.0, ge=1.0)

    # The host backup script is the sole writer, and the container mounts this path
    # read-only. It must not live under root-owned data_root, which the host user
    # cannot write.
    host_backup_status_path: pathlib.Path = DEFAULT_HOST_BACKUP_STATUS_PATH
    data_root: pathlib.Path = DEFAULT_DATA_ROOT
    ntp_host: str = "kr.pool.ntp.org"
    ntp_fallback_hosts: tuple[str, ...] = ("time.google.com", "time.cloudflare.com")
    # Ceiling enforces the fail-closed clock gate (drift <= 2.0s); env may only tighten it now that streamer children honor it.
    max_clock_offset_ns: int = Field(default=2_000_000_000, gt=0, le=2_000_000_000)
    vendor: str = "ls"
    streams: tuple[str, ...] = ("H0STCNT0", "H0STASP0")
    ls_capacity_pairs: int = Field(default=LS_MAX_SUBSCRIPTIONS_PER_SESSION, ge=1)
    # LS vendor ceilings below; env-overridable, never literals in stream logic.
    ls_max_sessions: int = Field(default=LS_MAX_SESSIONS, ge=1)
    ls_max_subscriptions_per_session: int = Field(default=LS_MAX_SUBSCRIPTIONS_PER_SESSION, ge=1)
    ls_active_sessions: int = Field(default=1, ge=1)
    ls_dynamic_reserve_symbols: int = Field(default=0, ge=0)
    ws_lease_ttl_s: float = Field(default=30.0, gt=0)
    # Rotation of a fresh token is a transport problem, not expiry.
    ls_token_rotation_cooldown_s: float = Field(default=300.0, ge=0)
    regular_open_grace_s: float = Field(default=600.0, gt=0)
    regular_open_alert_after_s: float = Field(default=30.0, gt=0)
    regular_open_standby_lead_s: float = Field(default=120.0, gt=0)
    regular_open_standby_hold_s: float = Field(default=300.0, gt=0)
    handover_state_interval_s: float = Field(default=60.0, gt=0)
    lag_handover_enabled: bool = True
    lag_handover_max_s: float = Field(default=120.0, gt=0)
    lag_handover_cooldown_s: float = Field(default=300.0, ge=0)
    lag_handover_standby_refresh_enabled: bool = True
    lag_handover_refresh_cooldown_s: float = Field(default=30.0, ge=0)
    # Per-session pair cap of one DB connection (probe-verified 50 pairs); raise only with a new probe report.
    db_capacity_pairs: int = Field(default=50, ge=1)
    db_max_sessions: int = Field(default=2, ge=1, le=2)
    db_token_rotation_cooldown_s: float = Field(default=300.0, ge=0)
    db_subscribe_spacing_s: float = Field(default=0.12, ge=0.1)
    db_ack_timeout_s: float = Field(default=10.0, gt=0)
    # Vendor allows 6 DB websocket connects/min per account; the guard keeps headroom below that cap across both DB children.
    db_connects_per_window: int = Field(default=4, ge=1, le=6)
    db_connect_window_s: float = Field(default=60.0, gt=0)
    # Toss unified streams: trade-only by default (the unified book is not an exchange book); 100 topics per connection, 2 connections per account.
    toss_streams: tuple[str, ...] = ("H0STCNT0",)
    toss_capacity_pairs: int = Field(default=100, ge=1)
    toss_max_sessions: int = Field(default=2, ge=1, le=2)
    # Vendor limit is 5 declarations per second; the 0.2 s floor keeps the adapter at or below it.
    toss_declare_spacing_s: float = Field(default=0.25, ge=0.2)
    toss_ack_timeout_s: float = Field(default=10.0, gt=0)
    # Kiwoom REST WebSocket crosscheck (conflated, volume-lossless 0B stream).
    kiwoom_crosscheck_enabled: bool = False
    kiwoom_capacity_pairs: int = Field(default=200, ge=1)
    kiwoom_ack_timeout_s: float = Field(default=10.0, gt=0)
    kiwoom_user_agent: str = "krx-alpha"
    # Per-subscribe-ack deadline (seconds) of the KIS adapter; mirrors `db_ack_timeout_s`.
    kis_ack_timeout_s: float = Field(default=10.0, gt=0)
    # Symbols per session universe. Unset derives ``ls_capacity_pairs // len(streams)`` so the cap cannot drift from vendor capacity (a literal caused the 2026-09-10 and 2026-10-02 incidents). An explicit value (env ``KRX_ALPHA_UNIVERSE_SLOT_BUDGET`` or kwarg) may only tighten it.
    universe_slot_budget: int = Field(default=0, ge=0)
    bars_window_days: int = 90
    # 61거래행 + 최장 KRX 휴장(약 10일)을 여유 있게 cover: 150역일은 약 100거래일.
    selection_lookback_calendar_days: int = Field(default=150, ge=100)
    journal_retain_days: int = 3
    l0_recompress_level: int = Field(default=9, ge=1, le=19)
    l0_recompress_seal_grace_s: float = Field(default=600.0, gt=0)
    archive_retain_days: int = 30
    min_free_disk_gb: float = 3.0
    orchestration_retry_s: float = 300.0
    degraded_candidates_max_age_days: int = 7
    # 야간 01:30 KST 백업은 20:00 EOD 점검 시점에 보통 18.5시간 전 성공이다.
    # 36시간이면 하룻밤 누락을 감지하면서 다음날 아침까지 끝나는 재시도는 허용한다.
    host_backup_max_age_h: float = 36.0
    # A one-night miss self-heals on the next run; only a multi-day failure pages the operator.
    backup_page_stale_days: int = Field(default=2, ge=1)
    stale_bars_max_calendar_days: int = 4
    normalize_timeout_s: float = 900.0
    schedule: SessionSchedule = SessionSchedule()
    after_market_enabled: bool = False
    # LS child reconnects on silence after the regular close; the in-window limit is regular_silence_limit_s.
    after_close_silence_enabled: bool = True
    after_close_silence_limit_s: float = Field(default=900.0, gt=0)
    regular_silence_limit_s: float = Field(default=10.0, gt=0)
    # Host-wide KIS pacing reservation lead for daemon data usage (standard work).
    # Critical execution work books without bound; this only bounds the daemon's
    # own data-slot bookings so a critical arrival waits at most this lead.
    kis_rest_max_lead_s: float = Field(default=1.0, gt=0)

    @property
    def paths(self) -> DataPaths:
        return DataPaths(self.data_root)

    @property
    def subscription_pair_budget(self) -> int:
        return self.ls_capacity_pairs

    @property
    def subscription_symbol_budget(self) -> int:
        market_reserve = ls_market_state_symbols(len(self.streams)) if self.ls_active_sessions >= 2 else 0
        return (
            self.ls_capacity_pairs * self.ls_active_sessions // len(self.streams)
            - self.ls_dynamic_reserve_symbols
            - market_reserve
        )

    @property
    def kiwoom_crosscheck_symbol_budget(self) -> int:
        return self.kiwoom_capacity_pairs // len(self.streams)

    @model_validator(mode="after")
    def check_toss_streams(self) -> CollectorSettings:
        """Toss streams are a non-empty subset of the logical trade and book ids."""
        if not self.toss_streams or set(self.toss_streams) - TOSS_LOGICAL_STREAMS or len(set(self.toss_streams)) != len(self.toss_streams):
            raise ValueError(f"toss_streams {self.toss_streams!r} must be unique, non-empty and within {sorted(TOSS_LOGICAL_STREAMS)!r}")
        return self

    @model_validator(mode="after")
    def check_ls_sharding_capacity(self) -> CollectorSettings:
        """Keep one LS session for the shard-0 standby when sharding, and the static budget non-negative."""
        if self.ls_active_sessions >= 2 and self.ls_active_sessions > self.ls_max_sessions - 1:
            raise ValueError(
                f"ls_active_sessions {self.ls_active_sessions} must be <= ls_max_sessions - 1 ({self.ls_max_sessions - 1})"
            )
        if self.subscription_symbol_budget < 0:
            raise ValueError(
                f"ls_dynamic_reserve_symbols {self.ls_dynamic_reserve_symbols} exceeds LS static symbol capacity"
            )
        return self

    @model_validator(mode="after")
    def check_after_close_silence_limit(self) -> CollectorSettings:
        # The enabled limit must exceed the 30 s in-window limit, which the 450 s floor already implies.
        if self.after_close_silence_enabled and self.after_close_silence_limit_s < OFF_REGULAR_MAX_NORMAL_SILENCE_S:
            raise ValueError(
                f"after_close_silence_limit_s {self.after_close_silence_limit_s} must be >= "
                f"{OFF_REGULAR_MAX_NORMAL_SILENCE_S} when after_close_silence_enabled"
            )
        return self

    @model_validator(mode="after")
    def check_regular_silence_limit_below_after_close(self) -> CollectorSettings:
        if self.after_close_silence_enabled and self.regular_silence_limit_s >= self.after_close_silence_limit_s:
            raise ValueError(
                f"regular_silence_limit_s {self.regular_silence_limit_s} must be below "
                f"after_close_silence_limit_s {self.after_close_silence_limit_s}"
            )
        return self

    @model_validator(mode="after")
    def check_universe_budget_within_technical_capacity(self) -> CollectorSettings:
        if "universe_slot_budget" not in self.model_fields_set:
            self.universe_slot_budget = self.subscription_symbol_budget
        elif self.universe_slot_budget < 1:
            raise ValueError(f"universe_slot_budget {self.universe_slot_budget} must be >= 1")
        if self.universe_slot_budget > self.subscription_symbol_budget:
            raise SlotBudgetExceededError(
                f"universe_slot_budget {self.universe_slot_budget} exceeds "
                f"subscription_symbol_budget {self.subscription_symbol_budget}"
            )
        return self


class AftermarketSettings(BaseSettings):
    """애프터마켓 수집 설정 (env_prefix='KRX_ALPHA_AFTERMARKET_')."""

    model_config = SettingsConfigDict(env_prefix="KRX_ALPHA_AFTERMARKET_", extra="ignore")

    enabled: bool = False
    pair_capacity_per_connection: int | None = None
    krx_streams: tuple[str, str] = ("H0STCNT0", "H0STASP0")
    nxt_streams: tuple[str, str] = ("H0NXCNT0", "H0NXASP0")
    selection_time: dt.time = dt.time(15, 31)
    # Seconds between per-venue selection retries and plan re-evaluations.
    selection_retry_s: float = Field(default=60.0, gt=0)
    prearm_lead_s: float = Field(default=60.0, gt=0)
    max_symbols: int = Field(default=40, ge=1)
    # 2026-09 probe: max normal intra-connection silence 3.84s over ~30 conn-hours; 10s (~2.6x)
    # bounds half-dead socket loss without false reconnects.
    silence_limit_s: float = Field(default=10.0, gt=0)

    @model_validator(mode="after")
    def check_verified_capacity_when_enabled(self) -> AftermarketSettings:
        if self.enabled and (self.pair_capacity_per_connection is None or self.pair_capacity_per_connection <= 0):
            raise ValueError("pair_capacity_per_connection must be a positive verified value when enabled")
        return self

    @model_validator(mode="after")
    def check_prearm_after_selection(self) -> AftermarketSettings:
        """Reject a lead that would arm NXT shards at or before the selection time."""
        base = dt.date(2000, 1, 1)
        selection_at = dt.datetime.combine(base, self.selection_time)
        nxt_open = dt.datetime.combine(base, STANDARD_NXT_AFTER_MARKET_OPEN)
        if not self.prearm_lead_s < (nxt_open - selection_at).total_seconds():
            raise ValueError(
                f"prearm_lead_s {self.prearm_lead_s} must arm NXT shards strictly after "
                f"selection_time {self.selection_time} and before open {nxt_open.time()}"
            )
        return self


class PremarketSettings(BaseSettings):
    """NXT premarket collection settings (env_prefix='KRX_ALPHA_PREMARKET_').

    Disabled by default; when disabled every other field is inert. Enabling requires a verified
    per-connection pair capacity and an explicit data slot, because premarket must never share
    the snapshot REST slot or silently borrow a slot another automation owns.
    """

    model_config = SettingsConfigDict(env_prefix="KRX_ALPHA_PREMARKET_", extra="ignore")

    enabled: bool = False
    credential_slot: str | None = None
    pair_capacity_per_connection: int | None = None
    nxt_streams: tuple[str, str] = ("H0NXCNT0", "H0NXASP0")
    max_symbols: int = Field(default=20, ge=1)
    start_lead_s: float = Field(default=120.0, gt=0)
    close_grace_s: float = Field(default=60.0, gt=0)
    pool_settle_s: float = Field(default=300.0, ge=0)
    pool_retry_s: float = Field(default=60.0, gt=0)
    silence_limit_s: float = Field(default=30.0, gt=0)
    extra_free_disk_gb: float = Field(default=2.0, ge=0)

    @model_validator(mode="after")
    def check_verified_capacity_when_enabled(self) -> PremarketSettings:
        if not self.enabled:
            return self
        if not self.credential_slot:
            raise ValueError("credential_slot must be set when enabled")
        if self.pair_capacity_per_connection is None or self.pair_capacity_per_connection <= 0:
            raise ValueError("pair_capacity_per_connection must be a positive verified value when enabled")
        if self.max_symbols > self.pair_capacity_per_connection // len(self.nxt_streams):
            raise ValueError(
                f"max_symbols {self.max_symbols} exceeds connection capacity "
                f"{self.pair_capacity_per_connection // len(self.nxt_streams)}"
            )
        return self


class RegularKisSettings(BaseSettings):
    """KRX regular-session KIS co-primary shard settings; inert when disabled."""

    model_config = SettingsConfigDict(env_prefix="KRX_ALPHA_REGULAR_KIS_", extra="ignore")

    enabled: bool = False
    credential_slots: tuple[str, ...] = ()
    pair_capacity_per_connection: int | None = None
    streams: tuple[str, str] = ("H0STCNT0", "H0STASP0")
    start_lead_s: float = Field(default=1920.0, gt=0)
    close_grace_s: float = Field(default=120.0, ge=0)
    silence_limit_s: float = Field(default=30.0, gt=0)
    nxt_credential_slots: tuple[str, ...] = ()
    nxt_streams: tuple[str, str] = ("H0NXCNT0", "H0NXASP0")
    nxt_start_lead_s: float = Field(default=120.0, gt=0)
    nxt_pool_settle_s: float = Field(default=600.0, ge=0)
    nxt_pool_retry_s: float = Field(default=60.0, gt=0)

    @model_validator(mode="after")
    def check_verified_capacity_when_enabled(self) -> RegularKisSettings:
        if not self.enabled:
            return self
        if not self.credential_slots or len(set(self.credential_slots)) != len(self.credential_slots):
            raise ValueError("credential_slots must be non-empty and duplicate-free when enabled")
        if self.pair_capacity_per_connection is None or self.pair_capacity_per_connection <= 0:
            raise ValueError("pair_capacity_per_connection must be a positive verified value when enabled")
        if self.pair_capacity_per_connection // len(self.streams) < 1:
            raise ValueError("pair_capacity_per_connection must hold at least one symbol across streams")
        return self

    @model_validator(mode="after")
    def check_nxt_slots_when_enabled(self) -> RegularKisSettings:
        if not self.enabled or not self.nxt_credential_slots:
            return self
        if len(set(self.nxt_credential_slots)) != len(self.nxt_credential_slots):
            raise ValueError(f"nxt_credential_slots must be duplicate-free: {self.nxt_credential_slots}")
        invalid = [slot for slot in self.nxt_credential_slots if slot not in _KIS_CREDENTIAL_SLOTS]
        if invalid:
            raise ValueError(f"nxt_credential_slots entries must be within '1'..'6': {invalid}")
        overlap = sorted(set(self.nxt_credential_slots) & set(self.credential_slots))
        if overlap:
            raise ValueError(f"nxt_credential_slots must not overlap credential_slots: {overlap}")
        return self

    @property
    def max_symbols(self) -> int:
        if not self.enabled:
            return 0
        if self.pair_capacity_per_connection is None:
            return 0
        return len(self.credential_slots) * (self.pair_capacity_per_connection // len(self.streams))

    @property
    def nxt_max_symbols(self) -> int:
        if not self.enabled or not self.nxt_credential_slots or self.pair_capacity_per_connection is None:
            return 0
        return len(self.nxt_credential_slots) * (self.pair_capacity_per_connection // len(self.nxt_streams))


_KIS_CREDENTIAL_SLOTS: frozenset[str] = frozenset({"1", "2", "3", "4", "5", "6"})


class IntradayAdmissionSettings(BaseSettings):
    """Intraday discovery/admission (env_prefix='KRX_ALPHA_INTRADAY_'); inert when disabled."""

    model_config = SettingsConfigDict(env_prefix="KRX_ALPHA_INTRADAY_", extra="ignore")

    enabled: bool = False
    poll_interval_s: float = Field(default=60.0, ge=15.0)
    start_after_open_s: float = Field(default=30.0, ge=0)
    admission_cutoff: dt.time = dt.time(15, 15)
    post_close_screen_at: dt.time = dt.time(15, 31)
    min_abs_change_pct: float = Field(default=5.0, gt=0)
    min_trade_value_krw: int = Field(default=10_000_000_000, gt=0)
    max_pages: int = Field(default=3, ge=1)
    kis_dynamic_slots: tuple[str, ...] = ("5",)
    status_check: bool = True

    @model_validator(mode="after")
    def check_cutoff_before_closing_auction(self) -> IntradayAdmissionSettings:
        """Keep admissions clear of the 15:20 closing-auction ka10027 window and its slot-5 REST."""
        if not self.admission_cutoff < STANDARD_CLOSING_AUCTION:
            raise ValueError(
                f"admission_cutoff {self.admission_cutoff} must precede closing auction start {STANDARD_CLOSING_AUCTION}"
            )
        if not self.admission_cutoff < self.post_close_screen_at:
            raise ValueError("post_close_screen_at must follow admission_cutoff")
        return self

    @model_validator(mode="after")
    def check_kis_dynamic_slots(self) -> IntradayAdmissionSettings:
        """Each dynamic KIS shard needs its own distinct credential slot 1..6."""
        if len(set(self.kis_dynamic_slots)) != len(self.kis_dynamic_slots):
            raise ValueError(f"kis_dynamic_slots must be unique: {self.kis_dynamic_slots}")
        invalid = [slot for slot in self.kis_dynamic_slots if slot not in _KIS_CREDENTIAL_SLOTS]
        if invalid:
            raise ValueError(f"kis_dynamic_slots entries must be within '1'..'6': {invalid}")
        return self


class KrxCredentials(BaseSettings):
    """KRX OpenAPI 자격증명 (필수, 빈 기본값 금지)."""

    model_config = SettingsConfigDict(extra="ignore")

    krx_openapi_key: str


class TossCredentials(BaseSettings):
    """토스증권 자격증명 (필수, 빈 기본값 금지)."""

    model_config = SettingsConfigDict(extra="ignore")

    toss_app_key: str
    toss_app_secret: str


class TossProgramTradesSettings(BaseSettings):
    """Toss program-trade backfill throttling (env_prefix='KRX_ALPHA_TOSS_PROGRAM_').

    The vendor's STOCK_TRADING_TREND group caps at 10 req/s; the default
    leaves headroom so a long backfill run never trips the vendor's own
    rate-limit rejection mid-run.
    """

    model_config = SettingsConfigDict(env_prefix="KRX_ALPHA_TOSS_PROGRAM_", extra="ignore")

    rate_per_s: float = 8.0
    request_timeout_s: float = 10.0
    auto_backfill_enabled: bool = True
    auto_backfill_lookback_days: int = 120
    max_stale_ratio: float = 0.05
    sync_timeout_s: float = 2400.0
    gap_check_business_days: int = Field(default=20, ge=1)
    # A refetched symbol-day the vendor still omits is accepted as vendor-empty only while a day's share stays below this.
    gap_vendor_empty_max_day_ratio: float = Field(default=0.02, gt=0, lt=1)
    # Host-wide Toss pacing reservation lead for program-trades sync/backfill (bulk work).
    rest_max_lead_s: float = Field(default=0.25, gt=0)

    @model_validator(mode="after")
    def check_positive(self) -> TossProgramTradesSettings:
        if self.rate_per_s <= 0 or self.request_timeout_s <= 0 or self.auto_backfill_lookback_days <= 0:
            raise ValueError("rate_per_s, request_timeout_s, and auto_backfill_lookback_days must be positive")
        if not 0 < self.max_stale_ratio < 1:
            raise ValueError("max_stale_ratio must be in (0, 1)")
        if self.sync_timeout_s <= 0:
            raise ValueError("sync_timeout_s must be positive")
        return self


class LsCredentials(BaseSettings):
    """LS증권 자격증명 (필수, 빈 기본값 금지)."""

    model_config = SettingsConfigDict(extra="ignore")

    ls_app_key: str
    ls_app_secret: str


class KiwoomCredentials(BaseSettings):
    """Kiwoom REST credentials (required, no empty defaults)."""

    model_config = SettingsConfigDict(extra="ignore")

    kiwoom_app_key: str = Field(min_length=1, repr=False)
    kiwoom_secret_key: str = Field(min_length=1, repr=False)


class TickTapeBackfillSettings(BaseSettings):
    """Kiwoom tick-tape backfill pacing, budget and window; inert when `enabled` is false."""

    model_config = SettingsConfigDict(env_prefix="KRX_ALPHA_TICK_TAPE_", extra="ignore")

    enabled: bool = False
    rate_per_s: float = Field(default=2.0, gt=0, le=5.0)
    max_pages_per_run: int = Field(default=20000, ge=1)
    window_start_kst: dt.time = dt.time(23, 30)
    window_end_kst: dt.time = dt.time(5, 30)
    vendor_window_sessions: int = Field(default=19, ge=1)
    request_timeout_s: float = Field(default=10.0, gt=0)
    max_ticks_per_day: int = Field(default=1_200_000, ge=1)
    progress_every_pages: int = Field(default=500, ge=1)
    transient_retries: int = Field(default=3, ge=0, le=10)
    transient_backoff_s: float = Field(default=2.0, ge=0, le=60)
    retry_passes: int = Field(default=1, ge=0, le=3)
    retry_pass_delay_s: float = Field(default=60.0, ge=0, le=600)

    @model_validator(mode="after")
    def check_window_spans(self) -> TickTapeBackfillSettings:
        if self.window_start_kst == self.window_end_kst:
            raise ValueError("window_start_kst must differ from window_end_kst")
        return self



class TickTapeOverflowSettings(BaseSettings):
    """Tier C same-night tick-tape budget; inert when `enabled` is false."""

    model_config = SettingsConfigDict(env_prefix="KRX_ALPHA_TICK_TAPE_C_", extra="ignore")

    enabled: bool = False
    max_symbols: int = Field(default=60, ge=0)
    deadline: dt.time = dt.time(4, 30)

    @model_validator(mode="after")
    def check_deadline_before_window_end(self) -> TickTapeOverflowSettings:
        window_end = TickTapeBackfillSettings.model_fields["window_end_kst"].default
        if self.deadline >= window_end:
            raise ValueError("deadline must precede the tick-tape window end")
        return self


class MinuteBackfillSettings(BaseSettings):
    """KIS historical 1m backfill pacing, slot, budget and range for the nightly MINUTE_BARS child."""

    model_config = SettingsConfigDict(env_prefix="KRX_ALPHA_MINUTE_BACKFILL_", extra="ignore")

    kis_data_slot: int = Field(default=5, ge=1, le=5)
    rate_per_s: float = Field(default=8.0, gt=0, le=15.0)
    # rate_per_s x the 23:30-05:30 nightly window (21,600 s); the nightly deadline still stops a run earlier.
    max_calls_per_run: int = Field(default=172_800, ge=1)
    history_start: dt.date = dt.date(2025, 10, 27)
    request_timeout_s: float = Field(default=10.0, gt=0)
    freeze_start_kst: dt.time = dt.time(8, 10)
    freeze_end_kst: dt.time = dt.time(22, 0)


class DbCredentials(BaseSettings):
    """DB증권 자격증명 (필수, 빈 기본값 금지)."""

    model_config = SettingsConfigDict(extra="ignore")

    db_app_key: str
    db_app_secret: str


class RcloneArchiveSettings(BaseSettings):
    """Rclone 원격 아카이브 설정 (기본값 존재, ValidationError 불가)."""

    model_config = SettingsConfigDict(env_prefix="KRX_ALPHA_BACKUP_", extra="ignore")

    remote_name: str = "gdrive"
    remote_path: str = "quant-lake/live/krx-alpha/data"
    rclone_timeout_s: float = 600.0
    # Kept well under rclone_timeout_s so a stalled transfer is retried by rclone itself before the call is killed.
    rclone_io_idle_timeout_s: float = Field(default=60.0, gt=0)
    # Parallel uploads per batched copy; matches the host backup's transfer count.
    rclone_transfers: int = Field(default=4, ge=1, le=16)


class KisCredentials(BaseSettings):
    """KIS 실전 자격증명 (필수, 빈 기본값 금지)."""

    model_config = SettingsConfigDict(extra="ignore")

    kis_app_key: str
    kis_app_secret: str
    kis_account_no: str
    kis_account_product_code: str


class KisMockCredentials(BaseSettings):
    """KIS mock-trading (VTS) credentials read from KIS_MOCK_* only."""

    model_config = SettingsConfigDict(extra="ignore")

    kis_mock_app_key: str
    kis_mock_app_secret: str
    kis_mock_account_no: str
    kis_mock_account_product_code: str = "01"


class KisTokenSettings(BaseSettings):
    """KIS 토큰 캐시 설정 (env_prefix='KRX_ALPHA_KIS_TOKEN_')."""

    model_config = SettingsConfigDict(env_prefix="KRX_ALPHA_KIS_TOKEN_", extra="ignore")

    token_cache_dir: pathlib.Path = Field(
        default=DEFAULT_KIS_TOKEN_CACHE_DIR,
        validation_alias=AliasChoices("KRX_ALPHA_KIS_TOKEN_CACHE_DIR", "token_cache_dir"),
    )
    # kca 07:05 warm-up is the first token issuer; krx issues only when the shared cache has no valid token, under the shared lock and day guard.
    allow_issue: bool = True


def validate_snapshot_order(settings: SnapshotSettings, *, market_close: dt.time) -> None:
    """Check that snapshot polling ends before the day's regular-session close transition.

    ``market_close`` is a parameter because shifted sessions (CSAT day) move the
    close transition; the settings validator applies the standard close.

    Raises:
        ValueError: If ``news_start < intraday_start < intraday_end <=
            eod_collect_time < run_end < market_close`` does not hold.
    """
    if not (
        settings.news_start
        < settings.intraday_start
        < settings.intraday_end
        <= settings.eod_collect_time
        < settings.run_end
        < market_close
    ):
        raise ValueError(
            "session time order must satisfy news_start < intraday_start < intraday_end <= eod_collect_time < run_end < market_close"
        )


class SnapshotSettings(BaseSettings):
    """Intraday REST snapshot collection contract (env_prefix='KRX_ALPHA_SNAPSHOT_').

    All times are KST wall-clock times on the session date. The collector must
    finish before the session EOD offload starts, because EOD uploads and later
    prunes the same L1 partitions.
    """

    model_config = SettingsConfigDict(env_prefix="KRX_ALPHA_SNAPSHOT_", extra="ignore")

    enabled: bool = False
    kis_data_slot: str = "1"
    rest_rate_per_s: float = 18.0
    request_timeout_s: float = 5.0
    transport_retry_backoff_s: float = Field(default=0.5, ge=0, allow_inf_nan=False)
    auction_open_times: tuple[dt.time, ...] = (
        dt.time(8, 40),
        dt.time(8, 50),
        dt.time(8, 55),
        dt.time(8, 58),
        dt.time(8, 59, 30),
    )
    auction_open_deadline: dt.time = dt.time(9, 0)
    auction_close_times: tuple[dt.time, ...] = (
        dt.time(15, 21),
        dt.time(15, 25),
        dt.time(15, 28),
        dt.time(15, 29, 30),
    )
    auction_close_deadline: dt.time = dt.time(15, 30)
    intraday_start: dt.time = dt.time(9, 0)
    intraday_end: dt.time = dt.time(15, 30)
    ranking_interval_s: int = 60
    index_interval_s: int = 300
    index_minute_interval_s: int = 4800
    index_codes: tuple[str, ...] = ("0001", "1001", "2001")
    news_start: dt.time = dt.time(8, 0)
    news_interval_s: int = 30
    news_max_pages: int = 30
    investor_estimate_times: tuple[dt.time, ...] = (
        dt.time(9, 35),
        dt.time(10, 5),
        dt.time(11, 25),
        dt.time(13, 25),
        dt.time(14, 35),
    )
    program_trade_interval_s: int = 1800
    eod_collect_time: dt.time = dt.time(15, 35)
    run_end: dt.time = dt.time(15, 39)
    stock_minute_max_symbols: int = 60
    eod_minute_resweeps: int = Field(default=1, ge=0, le=3)
    idle_sleep_cap_s: float = 5.0
    # Host-wide KIS pacing reservation lead for snapshot collection (standard work).
    rest_max_lead_s: float = Field(default=1.0, gt=0)

    @model_validator(mode="after")
    def check_snapshot_contract(self) -> SnapshotSettings:
        for name in ("auction_open_times", "auction_close_times", "investor_estimate_times"):
            times = getattr(self, name)
            if len(times) == 0:
                raise ValueError(f"{name} must not be empty")
            for idx in range(1, len(times)):
                if times[idx] <= times[idx - 1]:
                    raise ValueError(f"{name} must be strictly ascending")
        if not (max(self.auction_open_times) < self.auction_open_deadline <= self.intraday_start):
            raise ValueError("auction_open window must satisfy max(times) < deadline <= intraday_start")
        if not (self.intraday_start < min(self.auction_close_times)):
            raise ValueError("intraday_start must precede auction_close_times")
        if not (max(self.auction_close_times) < self.auction_close_deadline <= self.intraday_end):
            raise ValueError("auction_close window must satisfy max(times) < deadline <= intraday_end")
        if not (self.intraday_start < min(self.investor_estimate_times)):
            raise ValueError("intraday_start must precede investor_estimate_times")
        if not (max(self.investor_estimate_times) < self.intraday_end):
            raise ValueError("investor_estimate_times must precede intraday_end")
        validate_snapshot_order(self, market_close=SessionSchedule().market_close)
        for name in (
            "ranking_interval_s",
            "index_interval_s",
            "index_minute_interval_s",
            "news_interval_s",
            "program_trade_interval_s",
        ):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        if not 0 < self.index_minute_interval_s <= 5400:
            raise ValueError("index_minute_interval_s must be in (0, 5400] seconds")
        if self.news_max_pages < 1:
            raise ValueError("news_max_pages must be >= 1")
        if self.stock_minute_max_symbols < 0:
            raise ValueError("stock_minute_max_symbols must be >= 0")
        if self.rest_rate_per_s <= 0:
            raise ValueError("rest_rate_per_s must be positive")
        if self.idle_sleep_cap_s <= 0:
            raise ValueError("idle_sleep_cap_s must be positive")
        if len(self.index_codes) == 0:
            raise ValueError("index_codes must not be empty")
        if any(len(c) != 4 or not c.isdigit() for c in self.index_codes):
            raise ValueError("index_codes must each be a 4-digit numeric string")
        return self


# kca and kse own KIS slots 3, 4 and 5 for REST inside both auction windows
# (deploy/shared/kis_slot_assignments.toml); a sweep there would exceed the per-key TPS cap.
MARKET_SWEEP_RESERVED_SLOTS: Final[frozenset[str]] = frozenset({"3", "4", "5"})


def _check_ascending(name: str, times: tuple[dt.time, ...]) -> None:
    if len(times) == 0:
        raise ValueError(f"{name} must not be empty")
    if any(later <= earlier for earlier, later in pairwise(times)):
        raise ValueError(f"{name} must be strictly ascending")


def validate_market_sweep_order(settings: MarketSweepSettings, *, market_close: dt.time) -> None:
    """Check that sweep windows are disjoint and polling ends before the day's close transition.

    Raises:
        ValueError: If the open auction, intraday investor estimates, closing auction,
            close-time estimate and run end are not ordered before ``market_close``.
    """
    if not (
        max(settings.auction_open_times) < settings.auction_open_deadline <= min(settings.investor_estimate_times)
        and max(settings.investor_estimate_times) < min(settings.auction_close_times)
        and max(settings.auction_close_times)
        < settings.auction_close_deadline
        <= settings.investor_estimate_close_time
        < settings.run_end
        < market_close
    ):
        raise ValueError(
            "market sweep order must satisfy auction_open < open_deadline <= investor_estimate < auction_close"
            " < close_deadline <= investor_estimate_close_time < run_end < market_close"
        )


class MarketSweepSettings(BaseSettings):
    """Full-market auction book and investor-estimate REST sweep (env_prefix='KRX_ALPHA_MARKET_SWEEP_').

    Disabled by default; enabling requires an explicit data slot outside
    ``MARKET_SWEEP_RESERVED_SLOTS``. Times are KST wall-clock times on the session
    date and the sweep ends before the session EOD offload, like snapshots.
    """

    model_config = SettingsConfigDict(env_prefix="KRX_ALPHA_MARKET_SWEEP_", extra="ignore")

    enabled: bool = False
    kis_data_slot: str | None = None
    rest_rate_per_s: float = Field(default=18.0, gt=0, allow_inf_nan=False)
    request_timeout_s: float = Field(default=5.0, gt=0, allow_inf_nan=False)
    transport_retry_backoff_s: float = Field(default=0.5, ge=0, allow_inf_nan=False)
    # Host-wide KIS pacing reservation lead (standard work, same as snapshots).
    rest_max_lead_s: float = Field(default=1.0, gt=0)
    markets: tuple[str, ...] = ("KOSPI", "KOSDAQ")
    auction_open_times: tuple[dt.time, ...] = (
        dt.time(8, 40),
        dt.time(8, 43),
        dt.time(8, 46),
        dt.time(8, 49),
        dt.time(8, 52),
        dt.time(8, 55),
    )
    auction_open_deadline: dt.time = dt.time(9, 0)
    auction_close_times: tuple[dt.time, ...] = (
        dt.time(15, 20, 30),
        dt.time(15, 23),
        dt.time(15, 25, 30),
        dt.time(15, 28),
    )
    auction_close_deadline: dt.time = dt.time(15, 30)
    investor_estimate_times: tuple[dt.time, ...] = (
        dt.time(9, 35),
        dt.time(10, 5),
        dt.time(11, 25),
        dt.time(13, 25),
        dt.time(14, 35),
    )
    investor_estimate_close_time: dt.time = dt.time(15, 35)
    run_end: dt.time = dt.time(15, 39)
    idle_sleep_cap_s: float = Field(default=5.0, gt=0, allow_inf_nan=False)

    @model_validator(mode="after")
    def check_market_sweep_contract(self) -> MarketSweepSettings:
        if self.kis_data_slot in MARKET_SWEEP_RESERVED_SLOTS:
            raise ValueError(
                f"kis_data_slot {self.kis_data_slot} is reserved for other REST users in the auction windows"
            )
        if self.enabled and not self.kis_data_slot:
            raise ValueError("kis_data_slot must be set when enabled")
        if len(self.markets) == 0:
            raise ValueError("markets must not be empty")
        for name in ("auction_open_times", "auction_close_times", "investor_estimate_times"):
            _check_ascending(name, getattr(self, name))
        validate_market_sweep_order(self, market_close=SessionSchedule().market_close)
        return self


class ExecutionSettings(BaseSettings):
    """주문집행 설정 (env_prefix='KRX_ALPHA_EXEC_')."""

    model_config = SettingsConfigDict(env_prefix="KRX_ALPHA_EXEC_", extra="ignore")

    mode: ExecutionMode = ExecutionMode.PAPER
    live_armed: bool = False
    data_root: pathlib.Path = DEFAULT_DATA_ROOT
    rest_rate_per_s: float = 18.0
    mock_rest_rate_per_s: float = Field(default=1.0, allow_inf_nan=False)
    request_timeout_s: float = 5.0
    mock_request_timeout_s: float = Field(default=20.0, allow_inf_nan=False)
    commission_bps: float = 0.36396
    sell_tax_bps: float = 20.0
    paper_initial_cash_krw: int = 10_000_000
    max_order_notional_krw: int = 1_000_000
    max_position_notional_krw: int = 3_000_000
    max_orders_per_minute: int = 10
    max_daily_loss_krw: int = 300_000
    symbol_whitelist: tuple[str, ...] = ()
    position_parity_attempts: int = 2
    fill_poll_interval_s: float = 1.0
    cancel_confirm_wait_s: float = 5.0
    # Host-wide KIS pacing reservation lead for order execution (critical work:
    # None books without bound so execution never waits behind bulk bookings).
    rest_max_lead_s: float | None = None

    @property
    def paths(self) -> DataPaths:
        return DataPaths(self.data_root)

    @property
    def venue(self) -> KisVenue:
        return KisVenue.MOCK if self.mode is ExecutionMode.MOCK else KisVenue.LIVE

    @property
    def venue_rest_rate_per_s(self) -> float:
        return self.mock_rest_rate_per_s if self.venue is KisVenue.MOCK else self.rest_rate_per_s

    @property
    def venue_request_timeout_s(self) -> float:
        return self.mock_request_timeout_s if self.venue is KisVenue.MOCK else self.request_timeout_s

    @model_validator(mode="before")
    @classmethod
    def ignore_mock_arming(cls, values: Any) -> Any:
        if isinstance(values, Mapping) and values.get("mode") == ExecutionMode.MOCK:
            return {**values, "live_armed": False}
        return values

    @model_validator(mode="after")
    def check_live_armed(self) -> ExecutionSettings:
        if self.mode is ExecutionMode.LIVE and not self.live_armed:
            raise LiveNotArmedError("live mode requires KRX_ALPHA_EXEC_LIVE_ARMED=true")
        if self.rest_max_lead_s is not None and self.rest_max_lead_s <= 0:
            raise ValueError("rest_max_lead_s must be positive or None")
        if self.mock_rest_rate_per_s <= 0:
            raise ValueError("mock_rest_rate_per_s must be positive")
        if self.mock_request_timeout_s <= 0:
            raise ValueError("mock_request_timeout_s must be positive")
        if self.position_parity_attempts < 1:
            raise ValueError("position_parity_attempts must be >= 1")
        if self.fill_poll_interval_s <= 0:
            raise ValueError("fill_poll_interval_s must be positive")
        if self.cancel_confirm_wait_s <= 0:
            raise ValueError("cancel_confirm_wait_s must be positive")
        return self


class ObservabilitySettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="KRX_ALPHA_", extra="ignore")

    persistent_logs: bool = False
    run_id: str = ""


class DataQualitySettings(BaseSettings):
    """Stream data-quality verdict thresholds (env_prefix='KRX_ALPHA_DQ_').

    Ratios are violating rows divided by decoded stream rows of one L1 partition.
    Defaults sit well above the measured daily vendor noise floor so that FAIL
    means a structural feed defect, not routine vendor jitter.
    ``price_band_ratio`` is the symmetric daily price limit around the reference price. On a symbol's listing day the reference price is the offering price and the allowed range is ``[listing_day_lower_ratio, listing_day_upper_ratio] x reference`` (KRX new-listing rule since June 2023: 60%~400% of the offering price).
    """

    model_config = SettingsConfigDict(env_prefix="KRX_ALPHA_DQ_", extra="ignore")

    max_decode_fail_ratio: float = Field(default=0.001, ge=0.0, le=1.0)
    max_invariant_violation_ratio: float = Field(default=0.001, ge=0.0, le=1.0)
    max_total_remain_short_ratio: float = Field(default=0.05, ge=0.0, le=1.0)
    price_band_ratio: float = Field(default=0.30, gt=0.0, lt=1.0)
    listing_day_lower_ratio: float = Field(default=0.60, gt=0.0, lt=1.0)
    listing_day_upper_ratio: float = Field(default=4.00, gt=1.0, allow_inf_nan=False)


class AlertSettings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")

    alert_gmail_user: str = ""
    alert_gmail_app_password: str = ""
    alert_gmail_to: str = ""

    @property
    def enabled(self) -> bool:
        return bool(self.alert_gmail_user and self.alert_gmail_app_password and self.alert_gmail_to)


class LivenessSettings(BaseSettings):
    """External liveness signalling (env_prefix='KRX_ALPHA_LIVENESS_').

    ``healthcheck_url`` is a healthchecks.io ping URL. The URL alone authorizes
    pings, so it is treated as a credential and never logged.
    """

    model_config = SettingsConfigDict(env_prefix="KRX_ALPHA_LIVENESS_", extra="ignore")

    healthcheck_url: str = ""
    ping_interval_s: float = 60.0
    ping_timeout_s: float = 5.0
    crash_alert_daily_budget: int = 3
    # A single crash self-heals via the container restart; the Nth crash of one KST day pages.
    crash_page_min_daily: int = Field(default=2, ge=1)

    @property
    def enabled(self) -> bool:
        return self.healthcheck_url.startswith("https://")

    @model_validator(mode="after")
    def check_liveness_bounds(self) -> LivenessSettings:
        if not (
            self.ping_interval_s > 0
            and self.ping_timeout_s > 0
            and self.ping_timeout_s < self.ping_interval_s
            and self.crash_alert_daily_budget >= 1
        ):
            raise ValueError(
                "liveness requires ping_interval_s > 0, 0 < ping_timeout_s < ping_interval_s, "
                "and crash_alert_daily_budget >= 1"
            )
        return self


class ShardWatchdogSettings(BaseSettings):
    """Daemon-side stale detection for aftermarket/premarket shards."""

    model_config = SettingsConfigDict(env_prefix="KRX_ALPHA_SHARD_WATCHDOG_", extra="ignore")

    enabled: bool = True
    aftermarket_stale_limit_s: float = Field(default=120.0, gt=0)
    premarket_stale_limit_s: float = Field(default=120.0, gt=0)
    max_restarts_per_day: int = Field(default=3, ge=0)
    ls_after_close_enabled: bool = True
    ls_after_close_stale_limit_s: float = Field(default=1200.0, gt=0)
    ls_after_close_recovery_margin_s: float = Field(default=300.0, gt=0)
    ls_after_close_venue_wide_page_s: float = Field(default=1800.0, gt=0)

    @model_validator(mode="after")
    def check_ls_after_close(self) -> ShardWatchdogSettings:
        if self.ls_after_close_venue_wide_page_s < self.ls_after_close_stale_limit_s:
            raise ValueError("ls_after_close_venue_wide_page_s must be >= ls_after_close_stale_limit_s")
        return self


_FEED_CONTROL_GROUP_RE = re.compile(r"(ls|kis-krx|kis-nxt):[0-9]+")
_FEED_CONTROL_SECONDARY_FEEDS = frozenset({"db:0", "db:1", "toss:0", "toss:1", "kis:5", "ls-nxt:0"})


class FeedControlSettings(BaseSettings):
    """Feed failover controller thresholds (env_prefix='KRX_ALPHA_FEED_CONTROL_'); inert while watch_groups is empty."""

    model_config = SettingsConfigDict(env_prefix="KRX_ALPHA_FEED_CONTROL_", extra="ignore")

    watch_groups: tuple[str, ...] = ()
    secondary_feeds: tuple[str, ...] = ()
    health_max_age_s: float = Field(default=15.0, gt=0)
    unknown_grace_s: float = Field(default=30.0, gt=0)
    ls_silence_s: float = Field(default=10.0, gt=0)
    kis_silence_s: float = Field(default=30.0, gt=0)
    other_silence_s: float = Field(default=30.0, gt=0)
    first_frame_grace_s: float = Field(default=30.0, ge=0)
    lag_limit_s: float = Field(default=5.0, gt=0)
    pairs_shortfall_after_s: float = Field(default=20.0, gt=0)
    degrade_after_s: float = Field(default=3.0, ge=0)
    heal_after_s: float = Field(default=15.0, gt=0)
    failover_after_s: float = Field(default=30.0, gt=0)
    failback_hold_s: float = Field(default=600.0, gt=0)
    failback_confirm_s: float = Field(default=60.0, gt=0)
    demote_relapses: int = Field(default=2, ge=1)
    demote_degraded_s: float = Field(default=900.0, gt=0)
    page_after_s: float = Field(default=300.0, gt=0)
    quality_floor_availability: float = Field(default=0.995, gt=0, le=1)
    quality_floor_days: int = Field(default=3, ge=1)
    # (secondary feed id, top-priority symbols subscribed from session start); empty or 0 is pure warm standby.
    # Hot pairs (count x streams) are reserved out of the feed's pair capacity; the rest admits on failover.
    hot_symbols: tuple[tuple[str, int], ...] = ()

    @model_validator(mode="after")
    def check_feed_control_topology(self) -> FeedControlSettings:
        if len(set(self.watch_groups)) != len(self.watch_groups):
            raise ValueError("watch_groups must not contain duplicates")
        for group_id in self.watch_groups:
            if _FEED_CONTROL_GROUP_RE.fullmatch(group_id) is None:
                raise ValueError(f"watch_groups entry {group_id!r} must be ls:<n>, kis-krx:<n> or kis-nxt:<n>")
        unknown = set(self.secondary_feeds) - _FEED_CONTROL_SECONDARY_FEEDS
        if unknown:
            raise ValueError(f"secondary_feeds entries {sorted(unknown)!r} are not supported feed ids")
        if self.secondary_feeds and not self.watch_groups:
            raise ValueError("secondary_feeds requires non-empty watch_groups")
        hot_ids = [feed for feed, _ in self.hot_symbols]
        if len(set(hot_ids)) != len(hot_ids):
            raise ValueError("hot_symbols must not repeat a feed id")
        for feed, count in self.hot_symbols:
            if feed not in self.secondary_feeds:
                raise ValueError(f"hot_symbols feed {feed!r} is not in secondary_feeds")
            if count < 0:
                raise ValueError(f"hot_symbols count for {feed!r} must be non-negative")
        return self

    @model_validator(mode="after")
    def check_feed_control_windows(self) -> FeedControlSettings:
        if not self.failback_hold_s > self.failover_after_s:
            raise ValueError("failback_hold_s must exceed failover_after_s")
        if not self.demote_degraded_s > self.failback_hold_s:
            raise ValueError("demote_degraded_s must exceed failback_hold_s")
        if not self.page_after_s > self.failover_after_s:
            raise ValueError("page_after_s must exceed failover_after_s")
        return self


class AuditMode(StrEnum):
    """EOD session-audit rollout mode."""

    OFF = "off"
    SHADOW = "shadow"
    ENFORCE = "enforce"


class RepairSettings(BaseSettings):
    """Hold, vendor scope and expiry of the L0 repair gate; inert while hold_enabled is false."""

    model_config = SettingsConfigDict(env_prefix="KRX_ALPHA_REPAIR_", extra="ignore")

    hold_enabled: bool = False
    job_enabled: bool = False
    merge_required: bool = False
    hold_max_extra_days: int = Field(default=7, ge=0, le=10)
    gate_vendors: tuple[str, ...] = ("ls", "kis")
    repair_vendors: tuple[str, ...] = ("ls",)
    xvendor_peers: tuple[str, ...] = ("kis",)
    xvendor_conflating_peers: tuple[str, ...] = ("kiwoom",)
    overdue_days: int = Field(default=2, ge=1, le=7)
    overdue_attempts: int = Field(default=2, ge=1, le=5)
    # Days before hold expiry at which an unrepaired L0 day pages the operator.
    expiry_page_lead_days: int = Field(default=2, ge=1, le=7)

    @model_validator(mode="after")
    def check_repair_vendor_scope(self) -> RepairSettings:
        for name in ("gate_vendors", "repair_vendors"):
            vendors = getattr(self, name)
            if not vendors:
                raise ValueError(f"{name} must be non-empty")
            for vendor in vendors:
                if not vendor.strip() or vendor != vendor.strip() or vendor != vendor.lower():
                    raise ValueError(f"{name} entries must be non-empty lowercase: {vendor!r}")
        if set(self.repair_vendors) - set(self.gate_vendors):
            raise ValueError(
                f"repair_vendors {self.repair_vendors!r} must be a subset of gate_vendors {self.gate_vendors!r}"
            )
        peers = self.xvendor_peers
        if any(not entry.strip() or entry != entry.strip() or entry != entry.lower() for entry in peers):
            raise ValueError(f"xvendor_peers entries must be non-empty lowercase: {peers!r}")
        if len(set(peers)) != len(peers):
            raise ValueError(f"xvendor_peers must not contain duplicates: {peers!r}")
        conflating = self.xvendor_conflating_peers
        if any(not entry.strip() or entry != entry.strip() or entry != entry.lower() for entry in conflating):
            raise ValueError(f"xvendor_conflating_peers entries must be non-empty lowercase: {conflating!r}")
        if len(set(conflating)) != len(conflating):
            raise ValueError(f"xvendor_conflating_peers must not contain duplicates: {conflating!r}")
        return self


class AuditSettings(BaseSettings):
    """Open-latency alert threshold, trade-loss budget and derivation bounds for the EOD session audit; every threshold is configuration, never a literal in business logic."""

    model_config = SettingsConfigDict(env_prefix="KRX_ALPHA_AUDIT_", extra="ignore")

    mode: AuditMode = AuditMode.ENFORCE
    latency_alert_threshold_s: float = Field(default=10.0, gt=0)
    latency_streams: tuple[str, ...] = ("H0STCNT0", "H0STASP0")
    loss_warn_ratio: float = Field(default=1e-4, gt=0, lt=1)
    loss_fail_ratio: float = Field(default=5e-3, gt=0, lt=1)
    symbol_loss_fail_ratio: float = Field(default=5e-2, gt=0, lt=1)
    symbol_min_trades: int = Field(default=100, ge=1)
    tail_silence_max_s: float = Field(default=600.0, gt=0)
    time_budget_s: float = Field(default=1800.0, gt=0)
    latency_sample_budget_bytes: int = Field(default=402_653_184, ge=1_048_576)
    counter_reset_confirm_frames: int = Field(default=3, ge=2)
    max_intervals_per_symbol: int = Field(default=500, ge=1)
    event_hole_min_s: int = Field(default=10, ge=1)
    max_event_holes: int = Field(default=20, ge=1)

    @model_validator(mode="after")
    def check_audit_budget_order(self) -> AuditSettings:
        if not self.loss_warn_ratio < self.loss_fail_ratio:
            raise ValueError(f"loss_warn_ratio {self.loss_warn_ratio} must be < loss_fail_ratio {self.loss_fail_ratio}")
        allowed = ("H0STCNT0", "H0STASP0")
        if not self.latency_streams or any(stream not in allowed for stream in self.latency_streams):
            raise ValueError(f"latency_streams {self.latency_streams!r} must be a non-empty subset of {allowed!r}")
        return self


class UniverseTierSettings(BaseSettings):
    """Core-panel and follow-up layer parameters for the regular universe; inert when core_enabled is false."""

    model_config = SettingsConfigDict(env_prefix="KRX_ALPHA_UNIVERSE_", extra="ignore")

    core_enabled: bool = False
    core_enter_rank: int = Field(default=60, ge=1)
    core_exit_rank: int = Field(default=75, ge=1)
    core_warmup_sessions: int = Field(default=20, ge=1)
    follow_sessions: int = Field(default=3, ge=0)
    downside_enabled: bool = False

    @model_validator(mode="after")
    def check_core_rank_order(self) -> UniverseTierSettings:
        if self.core_exit_rank < self.core_enter_rank:
            raise ValueError(f"core_exit_rank {self.core_exit_rank} must be >= core_enter_rank {self.core_enter_rank}")
        return self


@dataclass(frozen=True)
class CollectorRuntime:
    collector: CollectorSettings
    aftermarket: AftermarketSettings
    snapshot: SnapshotSettings
    paths: DataPaths
    premarket: PremarketSettings = field(default_factory=lambda: PremarketSettings())
    shard_watchdog: ShardWatchdogSettings = field(default_factory=lambda: ShardWatchdogSettings())
    audit: AuditSettings = field(default_factory=lambda: AuditSettings())
    regular_kis: RegularKisSettings = field(default_factory=lambda: RegularKisSettings())
    universe_tiers: UniverseTierSettings = field(default_factory=lambda: UniverseTierSettings())
    repair: RepairSettings = field(default_factory=lambda: RepairSettings())
    intraday: IntradayAdmissionSettings = field(default_factory=lambda: IntradayAdmissionSettings())
    feed_control: FeedControlSettings = field(default_factory=lambda: FeedControlSettings())
    market_sweep: MarketSweepSettings = field(default_factory=lambda: MarketSweepSettings())


def resolve_collector_runtime(
    *,
    collector: CollectorSettings | None = None,
    aftermarket: AftermarketSettings | None = None,
    snapshot: SnapshotSettings | None = None,
    premarket: PremarketSettings | None = None,
    shard_watchdog: ShardWatchdogSettings | None = None,
    audit: AuditSettings | None = None,
    regular_kis: RegularKisSettings | None = None,
    universe_tiers: UniverseTierSettings | None = None,
    repair: RepairSettings | None = None,
    intraday: IntradayAdmissionSettings | None = None,
    feed_control: FeedControlSettings | None = None,
    market_sweep: MarketSweepSettings | None = None,
) -> CollectorRuntime:
    """Resolve one coherent collector configuration before child processes start.

    Args:
        collector: Optional prevalidated collector settings.
        aftermarket: Optional prevalidated aftermarket settings.
        snapshot: Optional prevalidated snapshot settings.
        premarket: Optional prevalidated premarket settings.
        shard_watchdog: Optional prevalidated shard watchdog settings.
        audit: Optional prevalidated session-audit settings.
        regular_kis: Optional prevalidated regular KIS shard settings.

    Returns:
        Settings and data paths from one resolved process configuration.

    Raises:
        ValueError: Explicit aftermarket enablement conflicts with collector enablement.
        ValueError: Enabled premarket shares the snapshot REST data slot.
        ValueError: Enabled market sweep shares the snapshot REST data slot.
        ValueError: Enabled watchdog stale limit does not exceed the in-process silence limit.
        ValueError: Enabled regular KIS overlaps the premarket window or the aftermarket prearm.
    """
    resolved_collector = collector if collector is not None else CollectorSettings()
    resolved_snapshot = snapshot if snapshot is not None else SnapshotSettings()
    resolved_premarket = premarket if premarket is not None else PremarketSettings()
    resolved_watchdog = shard_watchdog if shard_watchdog is not None else ShardWatchdogSettings()
    resolved_audit = audit if audit is not None else AuditSettings()
    resolved_regular_kis = regular_kis if regular_kis is not None else RegularKisSettings()
    resolved_intraday = intraday if intraday is not None else IntradayAdmissionSettings()
    if aftermarket is None:
        resolved_aftermarket = AftermarketSettings(enabled=resolved_collector.after_market_enabled)
    else:
        if aftermarket.enabled != resolved_collector.after_market_enabled:
            raise ValueError("aftermarket enabled conflicts with collector after_market_enabled")
        resolved_aftermarket = aftermarket
    if resolved_premarket.enabled and resolved_premarket.credential_slot == resolved_snapshot.kis_data_slot:
        raise ValueError("premarket credential_slot must not equal snapshot kis_data_slot")
    resolved_market_sweep = market_sweep if market_sweep is not None else MarketSweepSettings()
    if resolved_market_sweep.enabled and resolved_market_sweep.kis_data_slot == resolved_snapshot.kis_data_slot:
        raise ValueError("market_sweep kis_data_slot must not equal snapshot kis_data_slot")
    if resolved_watchdog.enabled:
        if resolved_watchdog.aftermarket_stale_limit_s <= resolved_aftermarket.silence_limit_s:
            raise ValueError("aftermarket_stale_limit_s must exceed aftermarket silence_limit_s")
        if resolved_watchdog.premarket_stale_limit_s <= resolved_premarket.silence_limit_s:
            raise ValueError("premarket_stale_limit_s must exceed premarket silence_limit_s")
    if resolved_regular_kis.enabled:
        if resolved_premarket.enabled and resolved_premarket.credential_slot in resolved_regular_kis.credential_slots:
            raise ValueError("regular_kis credential_slots must not overlap the premarket credential slot")
        if resolved_regular_kis.nxt_credential_slots:
            if resolved_premarket.enabled and resolved_premarket.credential_slot in resolved_regular_kis.nxt_credential_slots:
                raise ValueError("regular_kis nxt_credential_slots must not overlap the premarket credential slot")
            if resolved_intraday.enabled and set(resolved_intraday.kis_dynamic_slots) & set(
                resolved_regular_kis.nxt_credential_slots
            ):
                raise ValueError("regular_kis nxt_credential_slots must not overlap intraday kis_dynamic_slots")
            nxt_end = dt.datetime.combine(dt.date(2000, 1, 1), STANDARD_CLOSING_AUCTION) + dt.timedelta(
                seconds=float(resolved_regular_kis.close_grace_s)
            )
            nxt_prearm = dt.datetime.combine(dt.date(2000, 1, 1), STANDARD_NXT_AFTER_MARKET_OPEN) - dt.timedelta(
                seconds=float(resolved_aftermarket.prearm_lead_s)
            )
            if not nxt_end < nxt_prearm:
                raise ValueError("NXT regular window must end strictly before the aftermarket prearm moment")
        if resolved_aftermarket.enabled:
            base = dt.date(2000, 1, 1)
            regular_end = dt.datetime.combine(base, STANDARD_REGULAR_CLOSE) + dt.timedelta(
                seconds=float(resolved_regular_kis.close_grace_s)
            )
            prearm = dt.datetime.combine(base, STANDARD_NXT_AFTER_MARKET_OPEN) - dt.timedelta(
                seconds=float(resolved_aftermarket.prearm_lead_s)
            )
            if not regular_end < prearm:
                raise ValueError("regular_kis window must end strictly before the aftermarket prearm moment")
    resolved_tiers = universe_tiers if universe_tiers is not None else UniverseTierSettings()
    resolved_repair = repair if repair is not None else RepairSettings()
    resolved_feed_control = feed_control if feed_control is not None else FeedControlSettings()
    if "kis:5" in resolved_feed_control.secondary_feeds and resolved_intraday.enabled:
        raise ValueError("feed_control secondary kis:5 conflicts with intraday admission on the shared slot 5")
    for hot_feed, hot_count in resolved_feed_control.hot_symbols:
        if hot_feed.startswith("db:") and hot_count * len(resolved_collector.streams) > resolved_collector.db_capacity_pairs:
            raise ValueError(
                f"hot_symbols {hot_feed!r} x streams exceeds db_capacity_pairs {resolved_collector.db_capacity_pairs}"
            )
        if hot_feed.startswith("toss:") and hot_count * len(resolved_collector.toss_streams) > resolved_collector.toss_capacity_pairs:
            raise ValueError(
                f"hot_symbols {hot_feed!r} x toss_streams exceeds toss_capacity_pairs {resolved_collector.toss_capacity_pairs}"
            )
    if resolved_tiers.core_enabled and resolved_tiers.core_exit_rank > resolved_collector.universe_slot_budget:
        raise SlotBudgetExceededError(
            f"core_exit_rank {resolved_tiers.core_exit_rank} exceeds "
            f"universe_slot_budget {resolved_collector.universe_slot_budget}"
        )
    return CollectorRuntime(
        collector=resolved_collector,
        aftermarket=resolved_aftermarket,
        snapshot=resolved_snapshot,
        paths=DataPaths(resolved_collector.data_root),
        premarket=resolved_premarket,
        shard_watchdog=resolved_watchdog,
        audit=resolved_audit,
        regular_kis=resolved_regular_kis,
        universe_tiers=resolved_tiers,
        repair=resolved_repair,
        intraday=resolved_intraday,
        feed_control=resolved_feed_control,
        market_sweep=resolved_market_sweep,
    )


def child_process_env(overrides: Mapping[str, str]) -> dict[str, str]:
    return {**os.environ, **overrides}


def kis_data_env() -> dict[str, str]:
    """KIS 데이터 키 풀 원천 env 매핑 (프로세스 환경, layering 단일 진입점)."""
    return dict(os.environ)


def export_run_id(run_id: str) -> None:
    os.environ[RUN_ID_ENV] = run_id


def load_credentials(model: type[SettingsT]) -> SettingsT:
    """자격증명 모델을 로드하고 ValidationError 를 MissingCredentialsError 로 변환한다."""
    try:
        return model()
    except ValidationError as exc:
        fields = sorted(str(err["loc"][0]) for err in exc.errors() if err.get("loc"))
        raise MissingCredentialsError(f"missing credentials for {model.__name__}: {', '.join(fields)}") from exc


def load_venue_credentials(venue: KisVenue) -> KisCredentials:
    """Load the trading credentials for one venue; raises MissingCredentialsError or VenueCredentialError."""
    if venue is KisVenue.MOCK:
        mock = load_credentials(KisMockCredentials)
        creds = KisCredentials(
            kis_app_key=mock.kis_mock_app_key,
            kis_app_secret=mock.kis_mock_app_secret,
            kis_account_no=mock.kis_mock_account_no,
            kis_account_product_code=mock.kis_mock_account_product_code,
        )
    else:
        creds = load_credentials(KisCredentials)
    live_key = os.environ.get("KIS_APP_KEY")
    mock_key = os.environ.get("KIS_MOCK_APP_KEY")
    if live_key is not None and mock_key is not None and live_key == mock_key:
        raise VenueCredentialError("KIS_APP_KEY and KIS_MOCK_APP_KEY must differ across venues")
    live_acct = os.environ.get("KIS_ACCOUNT_NO")
    mock_acct = os.environ.get("KIS_MOCK_ACCOUNT_NO")
    if live_acct is not None and mock_acct is not None and live_acct == mock_acct:
        raise VenueCredentialError("KIS_ACCOUNT_NO and KIS_MOCK_ACCOUNT_NO must differ across venues")
    return creds
