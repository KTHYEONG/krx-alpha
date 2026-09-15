"""KIS 애프터마켓 데이터 키 풀과 샤드 플래너 (연결당 20종목/40쌍 고정 배정)."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass

from src.core.config import kis_data_env
from src.core.errors import KrxAlphaError, SlotBudgetExceededError
from src.realtime.contracts import MarketVenue


def credential_key_id_for(app_key: str) -> str:
    """앱키 지문을 토큰 캐시 파일명과 동일한 규칙으로 계산한다."""
    return hashlib.sha256(app_key.encode("utf-8")).hexdigest()[:12]


@dataclass(frozen=True)
class KisDataCredential:
    slot: str
    app_key: str
    app_secret: str
    hts_id: str
    key_id: str


@dataclass(frozen=True)
class AftermarketShard:
    venue: MarketVenue
    shard_index: int
    symbols: tuple[str, ...]
    streams: tuple[str, str]
    credential_slot: str
    credential_key_id: str


def load_kis_data_credentials(env: Mapping[str, str] | None = None) -> tuple[KisDataCredential, ...]:
    """KIS_DATA_SLOTS 순서대로 데이터 키 풀을 로드하고 worker 시작 전 fail-closed 검증한다."""
    source = env if env is not None else kis_data_env()
    raw = (source.get("KIS_DATA_SLOTS", "") or "").strip()
    if not raw:
        return ()
    slots = [part.strip() for part in raw.split(",")]
    if any(not slot for slot in slots):
        raise KrxAlphaError("empty data slot in KIS_DATA_SLOTS")
    if len(set(slots)) != len(slots):
        raise KrxAlphaError(f"duplicate data slots in KIS_DATA_SLOTS: {raw}")
    trade_key = (source.get("KIS_TRADE_APP_KEY", "") or "").strip()
    primary_key = (source.get("KIS_APP_KEY", "") or "").strip()
    credentials: list[KisDataCredential] = []
    seen: set[str] = set()
    for slot in slots:
        app_key = (source.get(f"KIS_DATA_{slot}_APP_KEY", "") or "").strip()
        app_secret = (source.get(f"KIS_DATA_{slot}_APP_SECRET", "") or "").strip()
        hts_id = (source.get(f"KIS_DATA_{slot}_HTS_ID", "") or "").strip()
        if not app_key or not app_secret or not hts_id:
            raise KrxAlphaError(f"empty credentials for data slot {slot}")
        fingerprint = credential_key_id_for(app_key)
        if fingerprint in seen:
            raise KrxAlphaError(f"duplicate data app_key fingerprint in slot {slot}")
        seen.add(fingerprint)
        if primary_key and app_key == primary_key:
            raise KrxAlphaError(f"data slot {slot} app_key collides with primary KIS_APP_KEY")
        if trade_key and app_key == trade_key:
            raise KrxAlphaError(f"data slot {slot} app_key collides with primary KIS_TRADE_APP_KEY")
        credentials.append(KisDataCredential(slot, app_key, app_secret, hts_id, fingerprint))
    return tuple(credentials)


def plan_aftermarket_shards(
    *,
    symbols: tuple[str, ...],
    credentials: tuple[KisDataCredential, ...],
    pair_capacity_per_connection: int,
    krx_streams: tuple[str, str],
    nxt_streams: tuple[str, str],
) -> tuple[AftermarketShard, ...]:
    """후보 순서를 보존해 NXT 샤드 then KRX 샤드로 고정 배정한다 (키 부족·축소 수집 거부)."""
    ordered = tuple(symbols)
    if not ordered:
        raise KrxAlphaError("empty aftermarket symbols")
    if len(set(ordered)) != len(ordered):
        raise KrxAlphaError("duplicate aftermarket symbols")
    nxt_per_shard = pair_capacity_per_connection // len(nxt_streams)
    krx_per_shard = pair_capacity_per_connection // len(krx_streams)
    if nxt_per_shard < 1 or krx_per_shard < 1:
        raise SlotBudgetExceededError(
            f"aftermarket pair capacity {pair_capacity_per_connection} cannot hold one symbol"
        )
    nxt_shard_count = (len(ordered) + nxt_per_shard - 1) // nxt_per_shard
    krx_shard_count = (len(ordered) + krx_per_shard - 1) // krx_per_shard
    required = nxt_shard_count + krx_shard_count
    available = len(credentials)
    if required > available:
        raise SlotBudgetExceededError(f"aftermarket shards required={required} available={available}")
    keys = list(credentials)
    plan: list[AftermarketShard] = []
    for index in range(nxt_shard_count):
        credential = keys.pop(0)
        plan.append(
            AftermarketShard(
                MarketVenue.NXT,
                index,
                ordered[index * nxt_per_shard : (index + 1) * nxt_per_shard],
                nxt_streams,
                credential.slot,
                credential.key_id,
            )
        )
    for index in range(krx_shard_count):
        credential = keys.pop(0)
        plan.append(
            AftermarketShard(
                MarketVenue.KRX,
                index,
                ordered[index * krx_per_shard : (index + 1) * krx_per_shard],
                krx_streams,
                credential.slot,
                credential.key_id,
            )
        )
    return tuple(plan)
