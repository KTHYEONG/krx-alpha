"""설정 단일 소스: 경로·자격증명·용량/예산 파라미터의 타입드 스키마."""

from __future__ import annotations

import datetime as dt
import os
import pathlib
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import TypeVar

from pydantic import AliasChoices, Field, ValidationError, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.core.calendar import SessionSchedule
from src.core.errors import LiveNotArmedError, MissingCredentialsError, SlotBudgetExceededError

SettingsT = TypeVar("SettingsT", bound=BaseSettings)

RUN_ID_ENV: str = "KRX_ALPHA_RUN_ID"


class ExecutionMode(StrEnum):
    """주문집행 모드 (기본 paper fail-closed)."""

    PAPER = "paper"
    LIVE = "live"


@dataclass(frozen=True)
class DataPaths:
    """data_root 하나에서 파생되는 전 파일시스템 경로."""

    root: pathlib.Path

    @property
    def bars_store(self) -> pathlib.Path:
        return self.root / "bars" / "daily.parquet"

    @property
    def market_map(self) -> pathlib.Path:
        return self.root / "market_map.json"

    @property
    def program_trades_store(self) -> pathlib.Path:
        return self.root / "bars" / "program_trades.parquet"

    @property
    def candidates(self) -> pathlib.Path:
        return self.root / "candidates.json"

    @property
    def universe_dir(self) -> pathlib.Path:
        return self.root / "universe"

    @property
    def manifest_dir(self) -> pathlib.Path:
        return self.root / "manifest"

    @property
    def journal_root(self) -> pathlib.Path:
        return self.root / "l0"

    @property
    def archive_root(self) -> pathlib.Path:
        return self.root / "l1"

    @property
    def quarantine_root(self) -> pathlib.Path:
        return self.root / "quarantine"

    @property
    def work_root(self) -> pathlib.Path:
        return self.root / "work"

    @property
    def kis_ws_lease_dir(self) -> pathlib.Path:
        return self.work_root / "kis_ws_leases"

    @property
    def logs_dir(self) -> pathlib.Path:
        return self.root / "logs"

    @property
    def calendar_cache(self) -> pathlib.Path:
        return self.root / "calendar_cache.json"

    def universe_out(self, day: dt.date) -> pathlib.Path:
        return self.universe_dir / f"{day.isoformat()}.parquet"

    def snapshot_partition(self, dataset: str, day: dt.date) -> pathlib.Path:
        """Return the L1 day partition for a REST snapshot dataset.

        Snapshot partitions live under the L1 archive root so the existing remote
        offload and verified local pruning apply unchanged; the file name must keep
        the ``dt=YYYY-MM-DD`` form that retention parses.
        """
        return self.archive_root / "snapshot" / dataset / f"dt={day.isoformat()}.parquet"

    def aftermarket_candidates(self, day: dt.date) -> pathlib.Path:
        return self.universe_dir / "aftermarket" / f"{day.isoformat()}.json"

    def manifest_path(self, day: dt.date) -> pathlib.Path:
        return self.manifest_dir / f"{day.isoformat()}.json"

    def aftermarket_manifest_path(self, day: dt.date, venue: object, shard_index: int) -> pathlib.Path:
        venue_s = str(getattr(venue, "value", venue))
        return self.manifest_dir / "aftermarket" / f"{day.isoformat()}.{venue_s}.shard-{shard_index:02d}.json"

    @property
    def execution_dir(self) -> pathlib.Path:
        return self.root / "execution"

    @property
    def order_journal_dir(self) -> pathlib.Path:
        return self.execution_dir / "journal"

    @property
    def kis_token_cache(self) -> pathlib.Path:
        return self.execution_dir / "kis_token.json"

    @property
    def kill_switch_file(self) -> pathlib.Path:
        return self.execution_dir / "KILL_SWITCH"


class CollectorSettings(BaseSettings):
    """수집기 전역 설정 (env_prefix='KRX_ALPHA_')."""

    model_config = SettingsConfigDict(env_prefix="KRX_ALPHA_", extra="ignore")

    data_root: pathlib.Path = pathlib.Path("data")
    ntp_host: str = "kr.pool.ntp.org"
    ntp_fallback_hosts: tuple[str, ...] = ("time.google.com", "time.cloudflare.com")
    max_clock_offset_ns: int = 2_000_000_000
    vendor: str = "ls"
    streams: tuple[str, ...] = ("H0STCNT0", "H0STASP0")
    ls_capacity_pairs: int = 200
    universe_slot_budget: int = 90
    bars_window_days: int = 90
    journal_retain_days: int = 3
    archive_retain_days: int = 30
    min_free_disk_gb: float = 3.0
    orchestration_retry_s: float = 300.0
    degraded_candidates_max_age_days: int = 7
    stale_bars_max_calendar_days: int = 4
    schedule: SessionSchedule = SessionSchedule()
    after_market_enabled: bool = False

    @property
    def paths(self) -> DataPaths:
        return DataPaths(self.data_root)

    @property
    def subscription_pair_budget(self) -> int:
        return self.ls_capacity_pairs

    @property
    def subscription_symbol_budget(self) -> int:
        return self.ls_capacity_pairs // len(self.streams)

    @model_validator(mode="after")
    def check_universe_budget_within_technical_capacity(self) -> CollectorSettings:
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
    max_symbols: int = Field(default=40, ge=1)

    @model_validator(mode="after")
    def check_verified_capacity_when_enabled(self) -> AftermarketSettings:
        if self.enabled and (self.pair_capacity_per_connection is None or self.pair_capacity_per_connection <= 0):
            raise ValueError("pair_capacity_per_connection must be a positive verified value when enabled")
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

    @model_validator(mode="after")
    def check_positive(self) -> TossProgramTradesSettings:
        if self.rate_per_s <= 0 or self.request_timeout_s <= 0 or self.auto_backfill_lookback_days <= 0:
            raise ValueError("rate_per_s, request_timeout_s, and auto_backfill_lookback_days must be positive")
        return self


class LsCredentials(BaseSettings):
    """LS증권 자격증명 (필수, 빈 기본값 금지)."""

    model_config = SettingsConfigDict(extra="ignore")

    ls_app_key: str
    ls_app_secret: str


class RcloneArchiveSettings(BaseSettings):
    """Rclone 원격 아카이브 설정 (기본값 존재, ValidationError 불가)."""

    model_config = SettingsConfigDict(env_prefix="KRX_ALPHA_BACKUP_", extra="ignore")

    remote_name: str = "gdrive"
    remote_path: str = "quant-lake/live/krx-alpha/data"


class KisCredentials(BaseSettings):
    """KIS 실전 자격증명 (필수, 빈 기본값 금지)."""

    model_config = SettingsConfigDict(extra="ignore")

    kis_app_key: str
    kis_app_secret: str
    kis_account_no: str
    kis_account_product_code: str


class KisTokenSettings(BaseSettings):
    """KIS 토큰 캐시 설정 (env_prefix='KRX_ALPHA_KIS_TOKEN_')."""

    model_config = SettingsConfigDict(env_prefix="KRX_ALPHA_KIS_TOKEN_", extra="ignore")

    token_cache_dir: pathlib.Path = Field(
        default=pathlib.Path("data/execution/kis_tokens"),
        validation_alias=AliasChoices("KRX_ALPHA_KIS_TOKEN_CACHE_DIR", "token_cache_dir"),
    )
    allow_issue: bool = True


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
    idle_sleep_cap_s: float = 5.0

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
        if not (
            self.news_start < self.intraday_start < self.intraday_end <= self.eod_collect_time < self.run_end < SessionSchedule().market_close
        ):
            raise ValueError("session time order must satisfy news_start < intraday_start < intraday_end <= eod_collect_time < run_end < market_close")
        for name in ("ranking_interval_s", "index_interval_s", "index_minute_interval_s", "news_interval_s", "program_trade_interval_s"):
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
    data_root: pathlib.Path = pathlib.Path("data")
    rest_rate_per_s: float = 18.0
    request_timeout_s: float = 5.0
    commission_bps: float = 0.36396
    sell_tax_bps: float = 20.0
    paper_initial_cash_krw: int = 10_000_000
    max_order_notional_krw: int = 1_000_000
    max_position_notional_krw: int = 3_000_000
    max_orders_per_minute: int = 10
    max_daily_loss_krw: int = 300_000
    symbol_whitelist: tuple[str, ...] = ()

    @property
    def paths(self) -> DataPaths:
        return DataPaths(self.data_root)

    @model_validator(mode="after")
    def check_live_armed(self) -> ExecutionSettings:
        if self.mode is ExecutionMode.LIVE and not self.live_armed:
            raise LiveNotArmedError("live mode requires KRX_ALPHA_EXEC_LIVE_ARMED=true")
        return self


class ObservabilitySettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="KRX_ALPHA_", extra="ignore")

    persistent_logs: bool = False
    run_id: str = ""


class AlertSettings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")

    alert_gmail_user: str = ""
    alert_gmail_app_password: str = ""
    alert_gmail_to: str = ""

    @property
    def enabled(self) -> bool:
        return bool(self.alert_gmail_user and self.alert_gmail_app_password and self.alert_gmail_to)


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
