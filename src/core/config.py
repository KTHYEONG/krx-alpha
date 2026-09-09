"""설정 단일 소스: 경로·자격증명·용량/예산 파라미터의 타입드 스키마."""

from __future__ import annotations

import datetime as dt
import pathlib
from dataclasses import dataclass
from typing import TypeVar

from pydantic import ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.core.calendar import SessionSchedule
from src.core.errors import MissingCredentialsError

SettingsT = TypeVar("SettingsT", bound=BaseSettings)


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

    def universe_out(self, day: dt.date) -> pathlib.Path:
        return self.universe_dir / f"{day.isoformat()}.parquet"

    def manifest_path(self, day: dt.date) -> pathlib.Path:
        return self.manifest_dir / f"{day.isoformat()}.json"


class CollectorSettings(BaseSettings):
    """수집기 전역 설정 (env_prefix='KRX_ALPHA_')."""

    model_config = SettingsConfigDict(env_prefix="KRX_ALPHA_", extra="ignore")

    data_root: pathlib.Path = pathlib.Path("data")
    ntp_host: str = "kr.pool.ntp.org"
    max_clock_offset_ns: int = 2_000_000_000
    vendor: str = "ls"
    streams: tuple[str, ...] = ("H0STCNT0", "H0STASP0")
    ls_capacity_pairs: int = 200
    universe_slot_budget: int = 40
    bars_window_days: int = 90
    journal_retain_days: int = 3
    archive_retain_days: int = 30
    min_free_disk_gb: float = 3.0
    schedule: SessionSchedule = SessionSchedule()

    @property
    def paths(self) -> DataPaths:
        return DataPaths(self.data_root)

    @property
    def subscription_pair_budget(self) -> int:
        return self.ls_capacity_pairs

    @property
    def subscription_symbol_budget(self) -> int:
        return self.ls_capacity_pairs // len(self.streams)


class KrxCredentials(BaseSettings):
    """KRX OpenAPI 자격증명 (필수, 빈 기본값 금지)."""

    model_config = SettingsConfigDict(extra="ignore")

    krx_openapi_key: str


class LsCredentials(BaseSettings):
    """LS증권 자격증명 (필수, 빈 기본값 금지)."""

    model_config = SettingsConfigDict(extra="ignore")

    ls_app_key: str
    ls_app_secret: str


class HfArchiveSettings(BaseSettings):
    """HuggingFace 아카이브 자격증명 (필수, 빈 기본값 금지)."""

    model_config = SettingsConfigDict(extra="ignore", case_sensitive=False)

    hf_token: str
    hf_dataset_repo: str


def load_credentials(model: type[SettingsT]) -> SettingsT:
    """자격증명 모델을 로드하고 ValidationError 를 MissingCredentialsError 로 변환한다."""
    try:
        return model()
    except ValidationError as exc:
        fields = sorted(str(err["loc"][0]) for err in exc.errors() if err.get("loc"))
        raise MissingCredentialsError(f"missing credentials for {model.__name__}: {', '.join(fields)}") from exc
