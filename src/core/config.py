"""설정 단일 소스: 경로·자격증명·용량/예산 파라미터의 타입드 스키마."""

from __future__ import annotations

import datetime as dt
import os
import pathlib
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, TypeVar

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


class KisVenue(StrEnum):
    """KIS server environment an app key is issued for."""

    LIVE = "live"
    MOCK = "mock"


class ExecutionMode(StrEnum):
    """주문집행 모드 (기본 paper fail-closed)."""

    PAPER = "paper"
    MOCK = "mock"
    LIVE = "live"


class CollectorSettings(BaseSettings):
    """수집기 전역 설정 (env_prefix='KRX_ALPHA_')."""

    model_config = SettingsConfigDict(env_prefix="KRX_ALPHA_", extra="ignore")

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
    # Rotation of a fresh token is a transport problem, not expiry.
    ls_token_rotation_cooldown_s: float = Field(default=300.0, ge=0)
    regular_open_grace_s: float = Field(default=600.0, gt=0)
    regular_open_alert_after_s: float = Field(default=30.0, gt=0)
    regular_open_standby_lead_s: float = Field(default=120.0, gt=0)
    regular_open_standby_hold_s: float = Field(default=300.0, gt=0)
    lag_handover_enabled: bool = True
    lag_handover_max_s: float = Field(default=120.0, gt=0)
    lag_handover_cooldown_s: float = Field(default=300.0, ge=0)
    lag_handover_standby_refresh_enabled: bool = True
    lag_handover_refresh_cooldown_s: float = Field(default=30.0, ge=0)
    db_capacity_pairs: int = Field(default=50, ge=1)
    db_token_rotation_cooldown_s: float = Field(default=300.0, ge=0)
    db_subscribe_spacing_s: float = Field(default=0.12, ge=0.1)
    db_ack_timeout_s: float = Field(default=10.0, gt=0)
    # Kiwoom REST WebSocket crosscheck (conflated, volume-lossless 0B stream).
    kiwoom_crosscheck_enabled: bool = False
    kiwoom_capacity_pairs: int = Field(default=200, ge=1)
    kiwoom_ack_timeout_s: float = Field(default=10.0, gt=0)
    kiwoom_user_agent: str = "krx-alpha"
    # Per-subscribe-ack deadline (seconds) of the KIS adapter; mirrors `db_ack_timeout_s`.
    kis_ack_timeout_s: float = Field(default=10.0, gt=0)
    db_crosscheck_enabled: bool = False
    # Symbols per session universe. Unset derives ``ls_capacity_pairs // len(streams)`` so the cap cannot drift from vendor capacity (a literal caused the 2026-09-10 and 2026-10-02 incidents). An explicit value (env ``KRX_ALPHA_UNIVERSE_SLOT_BUDGET`` or kwarg) may only tighten it.
    universe_slot_budget: int = Field(default=0, ge=0)
    bars_window_days: int = 90
    # 61거래행 + 최장 KRX 휴장(약 10일)을 여유 있게 cover: 150역일은 약 100거래일.
    selection_lookback_calendar_days: int = Field(default=150, ge=100)
    journal_retain_days: int = 3
    archive_retain_days: int = 30
    min_free_disk_gb: float = 3.0
    orchestration_retry_s: float = 300.0
    degraded_candidates_max_age_days: int = 7
    # 야간 23:30 KST 백업은 20:00 EOD 점검 시점에 보통 20.5시간 전 성공이다.
    # 36시간이면 하룻밤 누락을 감지하면서 다음날 아침까지 끝나는 재시도는 허용한다.
    host_backup_max_age_h: float = 36.0
    stale_bars_max_calendar_days: int = 4
    normalize_timeout_s: float = 900.0
    schedule: SessionSchedule = SessionSchedule()
    after_market_enabled: bool = False
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
        return self.ls_capacity_pairs // len(self.streams)

    @property
    def db_crosscheck_symbol_budget(self) -> int:
        return self.db_capacity_pairs // len(self.streams)

    @property
    def kiwoom_crosscheck_symbol_budget(self) -> int:
        return self.kiwoom_capacity_pairs // len(self.streams)

    @model_validator(mode="after")
    def check_universe_budget_within_technical_capacity(self) -> CollectorSettings:
        if "universe_slot_budget" not in self.model_fields_set:
            self.universe_slot_budget = self.ls_capacity_pairs // len(self.streams)
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

    @property
    def max_symbols(self) -> int:
        if not self.enabled:
            return 0
        if self.pair_capacity_per_connection is None:
            return 0
        return len(self.credential_slots) * (self.pair_capacity_per_connection // len(self.streams))


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

    @model_validator(mode="after")
    def check_window_spans(self) -> TickTapeBackfillSettings:
        if self.window_start_kst == self.window_end_kst:
            raise ValueError("window_start_kst must differ from window_end_kst")
        return self


class MinuteBackfillSettings(BaseSettings):
    """KIS historical 1m backfill pacing, slot, budget and range; inert when `enabled` is false."""

    model_config = SettingsConfigDict(env_prefix="KRX_ALPHA_MINUTE_BACKFILL_", extra="ignore")

    enabled: bool = False
    kis_data_slot: int = Field(default=5, ge=1, le=5)
    rate_per_s: float = Field(default=8.0, gt=0, le=15.0)
    max_calls_per_run: int = Field(default=60000, ge=1)
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
    news_max_pages: int = 5
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
        ValueError: Enabled watchdog stale limit does not exceed the in-process silence limit.
        ValueError: Enabled regular KIS overlaps the premarket window or the aftermarket prearm.
    """
    resolved_collector = collector if collector is not None else CollectorSettings()
    resolved_snapshot = snapshot if snapshot is not None else SnapshotSettings()
    resolved_premarket = premarket if premarket is not None else PremarketSettings()
    resolved_watchdog = shard_watchdog if shard_watchdog is not None else ShardWatchdogSettings()
    resolved_audit = audit if audit is not None else AuditSettings()
    resolved_regular_kis = regular_kis if regular_kis is not None else RegularKisSettings()
    if aftermarket is None:
        resolved_aftermarket = AftermarketSettings(enabled=resolved_collector.after_market_enabled)
    else:
        if aftermarket.enabled != resolved_collector.after_market_enabled:
            raise ValueError("aftermarket enabled conflicts with collector after_market_enabled")
        resolved_aftermarket = aftermarket
    if resolved_premarket.enabled and resolved_premarket.credential_slot == resolved_snapshot.kis_data_slot:
        raise ValueError("premarket credential_slot must not equal snapshot kis_data_slot")
    if resolved_watchdog.enabled:
        if resolved_watchdog.aftermarket_stale_limit_s <= resolved_aftermarket.silence_limit_s:
            raise ValueError("aftermarket_stale_limit_s must exceed aftermarket silence_limit_s")
        if resolved_watchdog.premarket_stale_limit_s <= resolved_premarket.silence_limit_s:
            raise ValueError("premarket_stale_limit_s must exceed premarket silence_limit_s")
    if resolved_regular_kis.enabled:
        if resolved_premarket.enabled and resolved_premarket.credential_slot in resolved_regular_kis.credential_slots:
            raise ValueError("regular_kis credential_slots must not overlap the premarket credential slot")
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
