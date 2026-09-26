"""Collector 24/7 daemon 오케스트레이션 (service 직접 호출, CLI 역방향 의존 제거)."""

from __future__ import annotations

import datetime as dt
import logging
import pathlib
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl
import requests

from src.brokers.kis.auth import KisAppAuth, KisTokenProvider, kis_app_key_fingerprint, kis_token_cache_path
from src.brokers.kis.data import KisDataClient
from src.brokers.kis.http import KisGetTransport
from src.brokers.kis.rate import RateLimiter
from src.brokers.kis.trading import KIS_LIVE_BASE_URL
from src.core.calendar import SessionState, calc_sleep_seconds, get_target_state, schedule_for
from src.core.config import (
    CollectorRuntime,
    CollectorSettings,
    DataPaths,
    ExecutionSettings,
    KisCredentials,
    KisTokenSettings,
    KrxCredentials,
    LivenessSettings,
    ObservabilitySettings,
    SnapshotSettings,
    TossCredentials,
    TossProgramTradesSettings,
    child_process_env,
    load_credentials,
    resolve_collector_runtime,
)
from src.core.errors import KrxAlphaError, MissingCredentialsError
from src.core.healthcheck import HealthcheckPinger, NoopPinger
from src.core.lifecycle import DaemonLifecycleRecord, crash_alert_allowed, read_lifecycle, write_lifecycle
from src.core.observability import EVENT, configure_logging, send_digest, shutdown_logging
from src.core.session_anchors import AnchorSource, SessionAnchors, resolve_session_anchors, save_session_anchors
from src.execution.contracts import KisApiError
from src.marketdata.krx_bars import KrxBarsError
from src.marketdata.partitioned_store import (
    PartitionedStoreError,
    latest_partition_date,
    migrate_single_file_store,
    scan_month_partitions,
)
from src.marketdata.service import BarsRefreshResult as BarsRefreshResult
from src.marketdata.service import KisFallbackError as KisFallbackError
from src.marketdata.service import refresh_bars, refresh_bars_via_kis_fallback
from src.marketdata.snapshot_plan import shift_snapshot_settings
from src.marketdata.toss_calendar import (
    TossCalendarError,
    TradingDay,
    fetch_trading_day,
    load_trading_day_cache,
    save_trading_day_cache,
    trading_day_from_cache,
)
from src.orchestration.eod import (
    aftermarket_eod_ready,
    check_backup_freshness,
    check_host_backup_freshness,
    check_session_reconciliation,
    classify_remote_failure,
    run_eod_maintenance,
    run_eod_offload,
    run_eod_remote_l0_purge,
)
from src.orchestration.supervisor import ProcessSupervisor, RestartCircuitBreaker, stop_supervisors
from src.orchestration.trading_day_gate import TradingDayGate, TradingDayStatus
from src.realtime.contracts import MarketSession, MarketVenue
from src.realtime.kis_sharding import AftermarketShard, load_kis_data_credentials, plan_aftermarket_shards
from src.storage.remote import RemoteArchiveError
from src.storage.retention import check_disk_watermark
from src.storage.snapshot_store import SnapshotStore
from src.universe.aftermarket import AftermarketUniverseError, refresh_aftermarket_candidates
from src.universe.ipc import CandidateFileError, read_candidate_snapshot, read_candidates
from src.universe.policy import ineligible_security_symbols
from src.universe.service import UniversePlanResult as UniversePlanResult
from src.universe.service import plan_universe

logger = logging.getLogger(__name__)
_KST = ZoneInfo("Asia/Seoul")
_BASE_DATE: dt.date = dt.date(2000, 1, 1)
HEARTBEAT_SUMMARY_S: float = 600.0
INGEST_STALE_S: float = 300.0
INGEST_CHECK_S: float = 60.0
INGEST_WATCH_START_OFFSET: dt.timedelta = dt.timedelta(minutes=5)
INGEST_WATCH_END_OFFSET: dt.timedelta = dt.timedelta(minutes=5)
NXT_AFTERMARKET_START: dt.time = dt.time(15, 40)
KRX_AFTERMARKET_START: dt.time = dt.time(16, 0)
# 컴포즈 stop_grace_period(30s) 안에 자식 정상종료 + 로그 flush 를 끝내기 위한 공유 데드라인.
SHUTDOWN_CHILD_DEADLINE_S: float = 20.0
HOLIDAY_SLEEP_CAP_S: float = 3600.0
PROGRAM_SYNC_POLL_S: float = 60.0


def _shifted_time(base: dt.time, offset: dt.timedelta) -> dt.time:
    return (dt.datetime.combine(_BASE_DATE, base) + offset).time()


def _journal_age_s(journal_root: pathlib.Path, vendor: str, day: dt.date, now: dt.datetime) -> float | None:
    files = list(journal_root.glob(f"{vendor}/**/dt={day.isoformat()}/*.jsonl.zst"))
    if not files:
        return None
    return now.timestamp() - max(f.stat().st_mtime for f in files)


def resolve_trading_day(ref_date: dt.date) -> TradingDay | None:
    try:
        creds = load_credentials(TossCredentials)
        return fetch_trading_day(ref_date, app_key=creds.toss_app_key, app_secret=creds.toss_app_secret)
    except (MissingCredentialsError, TossCalendarError) as exc:
        logger.warning("[DATA] stage=trading_day_probe status=DEGRADED reason=%s", str(exc))
        return None


def _build_data_client(
    *,
    app_key: str,
    app_secret: str,
    rate_per_s: float,
    timeout_s: float,
    allow_issue: bool,
) -> tuple[KisTokenProvider, KisDataClient]:
    """Build a data-only KIS stack from an app key pair (no account fields)."""
    token_settings = KisTokenSettings()
    auth = KisAppAuth(app_key=app_key, app_secret=app_secret)
    limiter = RateLimiter(rate_per_s)
    tokens = KisTokenProvider(
        auth=auth,
        session=requests,
        cache_path=kis_token_cache_path(token_settings.token_cache_dir, app_key),
        limiter=limiter,
        now=lambda: dt.datetime.now(_KST),
        timeout_s=timeout_s,
        base_url=KIS_LIVE_BASE_URL,
        allow_issue=allow_issue,
    )
    transport = KisGetTransport(
        auth=auth,
        tokens=tokens,
        session=requests,
        limiter=limiter,
        timeout_s=timeout_s,
        base_url=KIS_LIVE_BASE_URL,
    )
    return tokens, KisDataClient(transport=transport)


def _build_kis_client(paths: DataPaths) -> KisDataClient:
    """execution 모듈과 동일한 토큰 캐시를 공유하는 KIS 데이터 클라이언트를 생성한다."""
    creds = load_credentials(KisCredentials)
    execution = ExecutionSettings(data_root=paths.root)
    token_settings = KisTokenSettings()
    _, client = _build_data_client(
        app_key=creds.kis_app_key,
        app_secret=creds.kis_app_secret,
        rate_per_s=execution.rest_rate_per_s,
        timeout_s=execution.request_timeout_s,
        allow_issue=token_settings.allow_issue,
    )
    return client


def _build_snapshot_preflight_client(paths: DataPaths, snapshot_settings: SnapshotSettings | None = None) -> tuple[KisDataClient, str]:
    """collect-snapshots와 동일한 규칙으로 데이터 슬롯 KIS 클라이언트를 생성한다."""
    snapshot_cfg = snapshot_settings if snapshot_settings is not None else SnapshotSettings()
    credentials = load_kis_data_credentials()
    cred = next((c for c in credentials if c.slot == snapshot_cfg.kis_data_slot), None)
    if cred is None:
        raise MissingCredentialsError(f"no data credential for slot {snapshot_cfg.kis_data_slot}")
    token_settings = KisTokenSettings()
    _, client = _build_data_client(
        app_key=cred.app_key,
        app_secret=cred.app_secret,
        rate_per_s=snapshot_cfg.rest_rate_per_s,
        timeout_s=snapshot_cfg.request_timeout_s,
        allow_issue=token_settings.allow_issue,
    )
    return client, cred.key_id


def _client_app_key(client: object) -> str:
    creds = getattr(client, "_creds", None)
    if creds is not None:
        key = getattr(creds, "kis_app_key", "")
        if key:
            return str(key)
    transport = getattr(client, "_transport", None)
    auth = getattr(transport, "_auth", None)
    key = getattr(auth, "app_key", "")
    return str(key) if key else ""


def _kis_token_preflight(paths: DataPaths, today: dt.date, snapshot_settings: SnapshotSettings | None = None) -> dict[str, str]:
    """Ensure krx's KIS REST keys hold a token before the session starts.

    Covers the primary key (security status, aftermarket reselection, daily
    bar fallback) and the snapshot data slot. The aftermarket WebSocket uses
    per-connection approval keys and is intentionally not covered. Failures
    never block orchestration: LS tick collection does not depend on KIS.

    Returns:
        Mapping of key fingerprint to outcome (``"cache"``, ``"issued"`` or
        ``"fail:<msg_cd>"``), for logging and tests.
    """
    del today
    outcomes: dict[str, str] = {}
    try:
        primary = _build_kis_client(paths)
    except MissingCredentialsError:
        logger.critical("[SYS] stage=kis_token_preflight key_id=unknown result=fail reason=missing_credentials")
        outcomes["unknown"] = "fail:missing_credentials"
        primary = None
    if primary is not None:
        app_key = _client_app_key(primary)
        fingerprint = kis_app_key_fingerprint(app_key) if app_key else "unknown"
        try:
            source = primary.ensure_token()
        except KisApiError as exc:
            logger.critical(
                "[SYS] stage=kis_token_preflight key_id=%s result=fail reason=%s",
                fingerprint,
                exc.msg_cd,
            )
            outcomes[fingerprint] = f"fail:{exc.msg_cd}"
        except Exception as exc:  # noqa: BLE001 - 프리플라이트 실패가 오케스트레이션을 막지 않도록 격리
            logger.critical(
                "[SYS] stage=kis_token_preflight key_id=%s result=fail reason=%s",
                fingerprint,
                type(exc).__name__,
            )
            outcomes[fingerprint] = f"fail:{type(exc).__name__}"
        else:
            logger.info(
                "[SYS] stage=kis_token_preflight key_id=%s result=%s reason=-",
                fingerprint,
                source.value,
            )
            outcomes[fingerprint] = source.value
    try:
        snapshot_client, snapshot_fp = _build_snapshot_preflight_client(paths, snapshot_settings)
    except MissingCredentialsError:
        fallback_key = "unknown-data"
        logger.critical(
            "[SYS] stage=kis_token_preflight key_id=%s result=fail reason=missing_credentials",
            fallback_key,
        )
        outcomes[fallback_key] = "fail:missing_credentials"
    else:
        try:
            source = snapshot_client.ensure_token()
        except KisApiError as exc:
            logger.critical(
                "[SYS] stage=kis_token_preflight key_id=%s result=fail reason=%s",
                snapshot_fp,
                exc.msg_cd,
            )
            outcomes[snapshot_fp] = f"fail:{exc.msg_cd}"
        except Exception as exc:  # noqa: BLE001 - 프리플라이트 실패가 오케스트레이션을 막지 않도록 격리
            logger.critical(
                "[SYS] stage=kis_token_preflight key_id=%s result=fail reason=%s",
                snapshot_fp,
                type(exc).__name__,
            )
            outcomes[snapshot_fp] = f"fail:{type(exc).__name__}"
        else:
            logger.info(
                "[SYS] stage=kis_token_preflight key_id=%s result=%s reason=-",
                snapshot_fp,
                source.value,
            )
            outcomes[snapshot_fp] = source.value
    return outcomes


def run_session_orchestration(
    *,
    today: dt.date,
    settings: CollectorSettings,
    trading_day: TradingDay | None = None,
    progress: Callable[[], None] | None = None,
) -> bool:
    """bars 갱신 + 유니버스 선정을 타입드 인자로 직접 호출하고 후보 준비 여부를 반환한다."""
    paths = settings.paths
    # 단계마다 생존 신호를 보내 장전 준비가 길어져도 외부 감시가 멈춤으로 오판하지 않게 한다.
    tick = progress if progress is not None else (lambda: None)
    # 이관 전 legacy 파일이 남아 있으면 빈 파티션에 90일 재백필이 조용히 진행되어
    # 이력이 잘린 채 선정되고 다음 기동부터 모호 상태가 되므로 fail-closed 한다.
    if paths.legacy_bars_file.exists():
        logger.critical("[DAEMON] stage=orchestration status=FAIL reason=store_migration_pending")
        return False
    # bars 갱신 실패는 기존 store 로 진행 가능하므로 도메인 예외만 흡수한다 (광역 포획 금지).
    try:
        auth_key = load_credentials(KrxCredentials).krx_openapi_key
        refresh_bars(
            store_path=paths.bars_daily_dir,
            market_map_path=paths.market_map,
            ref_date=today,
            window_days=settings.bars_window_days,
            auth_key=auth_key,
        )
    except (MissingCredentialsError, KrxBarsError) as exc:
        logger.error("[DAEMON] stage=orchestration status=FAIL step=bars_refresh reason=%s", str(exc))
    tick()
    decision_date = latest_partition_date(paths.bars_daily_dir)
    if decision_date is None:
        logger.critical("[DAEMON] stage=orchestration status=FAIL reason=no_bars_store")
        return False
    if trading_day is None and (today - decision_date).days > settings.stale_bars_max_calendar_days:
        logger.critical(
            "[DAEMON] stage=orchestration status=FAIL reason=stale_bars_calendar_unknown decision_date=%s today=%s",
            decision_date.isoformat(),
            today.isoformat(),
        )
        return False
    if trading_day is not None and decision_date != trading_day.previous_business_day:
        try:
            fallback = refresh_bars_via_kis_fallback(
                store_path=paths.bars_daily_dir,
                market_map_path=paths.market_map,
                target_date=trading_day.previous_business_day,
                kis_client=_build_kis_client(paths),
            )
        except (MissingCredentialsError, KisFallbackError, KisApiError, KrxBarsError) as exc:
            logger.critical(
                "[DAEMON] stage=orchestration status=FAIL reason=stale_bars_fallback_failed decision_date=%s expected=%s error=%s",
                decision_date.isoformat(),
                trading_day.previous_business_day.isoformat(),
                str(exc),
            )
            return False
        decision_date = fallback.trading_day
        logger.warning(
            "[DAEMON] stage=orchestration status=OK reason=kis_fallback_used decision_date=%s",
            decision_date.isoformat(),
        )
    # 선정 실패는 흡수하지 않는다: 직전 세션의 stale candidates 로 스트리밍하는 fail-open 을 차단.
    try:
        status_source: KisDataClient | None = _build_kis_client(paths)
    except MissingCredentialsError as exc:
        logger.warning("[DAEMON] stage=orchestration status=DEGRADED step=security_status reason=%s", str(exc))
        status_source = None
    plan_universe(
        bars_root=paths.bars_daily_dir,
        decision_date=decision_date,
        lookback_calendar_days=settings.selection_lookback_calendar_days,
        out_path=paths.universe_out(decision_date),
        slot_budget=settings.universe_slot_budget,
        candidates_path=paths.candidates,
        session_date=today,
        status_source=status_source,
        snapshot_store=SnapshotStore(paths=paths, session_date=today),
    )
    tick()
    ready = _candidates_ready(paths.candidates)
    logger.info(
        "[DAEMON] stage=orchestration status=OK decision_date=%s ready=%s",
        decision_date.isoformat(),
        ready,
    )
    return ready


def _candidates_ready(path: pathlib.Path) -> bool:
    data = read_candidates(path)
    return bool(data and data.get("candidates"))


def _degraded_candidates_rev(path: pathlib.Path, today: dt.date, *, max_age_days: int) -> int | None:
    try:
        data = read_candidates(path)
    except CandidateFileError:
        return None
    if data is None:
        return None
    candidates = data.get("candidates")
    rev = data.get("rev")
    if not candidates or not isinstance(rev, int):
        return None
    try:
        rev_date = dt.datetime.strptime(str(rev), "%Y%m%d").date()
    except ValueError:
        return None
    age = (today - rev_date).days
    if 0 <= age <= max_age_days:
        return rev
    return None


@dataclass(frozen=True)
class _EodHousekeeping:
    deleted: int
    uploaded: int
    purged: int
    maintenance_ok: bool
    offload_ok: bool


def _run_eod_housekeeping(
    cfg: CollectorSettings, paths: DataPaths, ref_day: dt.date, *, progress: Callable[[], None]
) -> _EodHousekeeping:
    """Run the date-agnostic EOD storage sequence shared by business days and holidays.

    Order matters: L0 is only deleted after the offload has verified its L1 copy
    remotely, so maintenance runs once unverified (normalize only), then again
    with the verified set, followed by the remote L0 purge.
    """
    deleted = 0
    maintenance_ok = True
    disk_ok = check_disk_watermark(paths.root, min_free_gb=cfg.min_free_disk_gb)
    if not disk_ok:
        logger.critical("[DATA] stage=eod_disk_guard status=FAIL min_free_gb=%s", cfg.min_free_disk_gb)
        maintenance_ok = False
    try:
        if disk_ok:
            deleted = run_eod_maintenance(
                paths.journal_root,
                retain_days=cfg.journal_retain_days,
                today=ref_day,
                archive_root=paths.archive_root,
                quarantine_root=paths.quarantine_root,
                work_root=paths.work_root,
                verified_remote_l1=None,
                progress=progress,
            )
            if getattr(deleted, "failed", 0) > 0:
                maintenance_ok = False
    except (KrxAlphaError, OSError) as e:
        maintenance_ok = False
        logger.critical("[DAEMON] stage=eod_maintenance status=FAIL reason=maintenance_error error=%s", str(e))
    offload: Any = {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0}
    offload_ok = True
    try:
        offload = run_eod_offload(
            paths.archive_root,
            paths.manifest_dir,
            retain_days=cfg.archive_retain_days,
            reference_date=ref_day,
            progress=progress,
        )
    except RemoteArchiveError as e:
        offload_ok = False
        reason = classify_remote_failure(str(e))
        logger.critical(
            "[DAEMON] stage=eod_offload status=FAIL reason=%s hint=%s error=%s",
            reason,
            "rclone_config_reconnect_gdrive" if reason == "auth_expired" else "check_remote",
            str(e),
        )
    except Exception as e:  # noqa: BLE001
        offload_ok = False
        logger.error("[DAEMON] stage=eod_maintenance error=%s", str(e), exc_info=True)
    if offload_ok:
        try:
            verified = offload.verified_remote_l1 if hasattr(offload, "verified_remote_l1") else frozenset()
            # 디스크 부족 시에도 원격 검증된 L0 는 정규화 없이 지워야 교착(수집 중단 + 공간 미확보)에서 스스로 회복한다.
            post_deleted = run_eod_maintenance(
                paths.journal_root,
                retain_days=cfg.journal_retain_days,
                today=ref_day,
                archive_root=paths.archive_root,
                quarantine_root=paths.quarantine_root,
                work_root=paths.work_root,
                verified_remote_l1=verified,
                progress=progress,
                normalize=disk_ok,
            )
            if getattr(post_deleted, "failed", 0) > 0:
                maintenance_ok = False
            deleted = int(deleted) + int(post_deleted)
            try:
                run_eod_remote_l0_purge(paths.journal_root, verified, progress=progress)
            except Exception as exc:  # noqa: BLE001 - purge failure never fails EOD
                logger.error("[DAEMON] stage=eod_l0_remote_purge status=FAIL error=%s", str(exc), exc_info=True)
        except (KrxAlphaError, OSError) as e:
            maintenance_ok = False
            logger.critical("[DAEMON] stage=eod_maintenance status=FAIL reason=maintenance_error error=%s", str(e))
    uploaded = offload.l1.uploaded if hasattr(offload, "l1") else offload["uploaded"]
    purged = offload.purged if hasattr(offload, "purged") else offload["purged"]
    return _EodHousekeeping(
        deleted=int(deleted),
        uploaded=int(uploaded),
        purged=int(purged),
        maintenance_ok=maintenance_ok,
        offload_ok=offload_ok,
    )


def _check_backup(
    status_path: pathlib.Path,
    paths: DataPaths,
    ref_day: dt.date,
    *,
    now: dt.datetime,
    max_age: dt.timedelta,
) -> tuple[list[str], bool, str]:
    host_reason = check_host_backup_freshness(
        status_path=status_path, now=now, max_age=max_age
    )
    host_label = "ok" if host_reason is None else host_reason
    if host_reason is not None:
        logger.critical("[SYS] stage=host_backup_freshness status=STALE reason=%s", host_reason)
    try:
        missing = check_backup_freshness(manifest_dir=paths.manifest_dir, today=ref_day)
    except RemoteArchiveError as e:
        logger.critical(
            "[DAEMON] stage=backup_freshness status=FAIL reason=%s error=%s",
            classify_remote_failure(str(e)),
            str(e),
        )
        return [], False, host_label
    if missing:
        logger.critical("[DAEMON] stage=backup_freshness status=STALE missing=%d oldest=%s", len(missing), missing[0])
        return missing, False, host_label
    if host_reason is not None:
        return [], False, host_label
    return [], True, host_label


def _stream_cmd(today: dt.date, paths: DataPaths, *, degraded_reason: str | None) -> list[str]:
    cmd = [
        sys.executable,
        "-m",
        "src.cli.main",
        "collect-stream",
        "--session-date",
        today.isoformat(),
        "--journal-root",
        str(paths.journal_root),
        "--manifest-path",
        str(paths.manifest_path(today)),
        "--candidates-path",
        str(paths.candidates),
        "--market-map",
        str(paths.market_map),
    ]
    if degraded_reason is not None:
        cmd += ["--degraded-reason", degraded_reason]
    return cmd


def _snapshot_cmd(today: dt.date, paths: DataPaths) -> list[str]:
    return [
        sys.executable,
        "-m",
        "src.cli.main",
        "collect-snapshots",
        "--session-date",
        today.isoformat(),
        "--candidates-path",
        str(paths.candidates),
    ]


def _aftermarket_stream_cmd(today: dt.date, paths: DataPaths, *, shard: AftermarketShard) -> list[str]:
    return [
        sys.executable,
        "-m",
        "src.cli.main",
        "collect-aftermarket",
        "--session-date",
        today.isoformat(),
        "--journal-root",
        str(paths.journal_root),
        "--manifest-path",
        str(paths.aftermarket_manifest_path(today, shard.venue, shard.shard_index)),
        "--candidates-path",
        str(paths.aftermarket_candidates(today)),
        "--venue",
        shard.venue.value,
        "--shard-index",
        str(shard.shard_index),
        "--credential-slot",
        shard.credential_slot,
        "--credential-key-id",
        shard.credential_key_id,
        "--symbols",
        ",".join(shard.symbols),
    ]


def _resolve_trading_day_with_cache(
    today: dt.date, cache_path: pathlib.Path, anchors_dir: pathlib.Path
) -> TradingDay | None:
    day = resolve_trading_day(today)
    if day is not None:
        # 캐시·당일 앵커·익일 앵커 저장은 서로 독립: 하나의 쓰기 실패가 나머지를 막지 않는다.
        writes: list[tuple[Callable[[], None], str]] = [(lambda: save_trading_day_cache(cache_path, day), "trading_day_cache")]
        if day.anchors is not None:
            today_anchors = day.anchors
            writes.append((lambda: save_session_anchors(anchors_dir, today_anchors), "session_anchors"))
        if day.next_anchors is not None:
            next_anchors = day.next_anchors
            writes.append((lambda: save_session_anchors(anchors_dir, next_anchors), "session_anchors"))
        for write, stage in writes:
            try:
                write()
            except OSError as exc:
                logger.warning("[DATA] stage=%s status=WRITE_FAIL reason=%s", stage, type(exc).__name__)
        if day.is_business_day and day.anchors is None:
            logger.warning(
                "[DATA] stage=session_anchors status=DEFAULT date=%s reason=vendor_missing",
                today.isoformat(),
            )
        return day
    cached = load_trading_day_cache(cache_path)
    derived = trading_day_from_cache(cached, today) if cached is not None else None
    if derived is not None:
        logger.warning(
            "[DATA] stage=trading_day_probe status=CACHE date=%s is_business_day=%s",
            today.isoformat(),
            derived.is_business_day,
        )
        return derived
    return None


@dataclass
class DaemonState:
    cycle: int = 0
    prev_state: SessionState | None = None
    last_summary: dt.datetime | None = None
    orchestrated_for: dt.date | None = None
    preflight_day: dt.date | None = None
    holiday_skip_logged_for: dt.date | None = None
    possible_holiday_warned_for: dt.date | None = None
    eod_attempted_for: dt.date | None = None
    program_sync_day: dt.date | None = None
    orchestration_day: dt.date | None = None
    next_orchestration_at: dt.datetime | None = None
    orchestration_attempts: int = 0
    degraded_active: bool = False
    last_ingest_check: dt.datetime | None = None
    ingest_stale: bool = False
    streamer_restarts: int = 0
    last_supervisor_result: str | None = None
    last_snapshot_result: str | None = None
    aftermarket_plan: tuple[AftermarketShard, ...] = ()
    aftermarket_plan_day: dt.date | None = None
    aftermarket_refresh_day: dt.date | None = None
    aftermarket_eligibility_day: dt.date | None = None
    next_aftermarket_refresh_at: dt.datetime | None = None
    anchors_reloaded_for: dt.date | None = None
    aftermarket_results: dict[str, str] = field(default_factory=dict)
    aftermarket_circuit_alerted: set[str] = field(default_factory=set)
    aftermarket_restarts: int = 0


@dataclass
class DaemonChildren:
    regular: ProcessSupervisor | None = None
    snapshot: ProcessSupervisor | None = None
    aftermarket: dict[str, ProcessSupervisor] = field(default_factory=dict)
    program_sync: Any | None = None
    program_sync_started_at: dt.datetime | None = None


def _program_sync_cmd(session_date: dt.date, complete_through: dt.date) -> list[str]:
    """Build the one-shot nightly program-trades sync child command."""
    return [
        sys.executable,
        "-m",
        "src.cli.toss_program_trades_sync",
        "--session-date",
        session_date.isoformat(),
        "--complete-through",
        complete_through.isoformat(),
    ]


def _stop_sync_child(proc: Any, *, grace_s: float, already_signalled: bool = False) -> None:
    """Terminate a nightly sync child and SIGKILL it when the grace window expires."""
    if not already_signalled:
        proc.terminate()
    try:
        proc.wait(timeout=grace_s)
    except Exception as exc:  # noqa: BLE001 - SIGKILL fallback stays inside the deadline
        logger.warning("[DAEMON] stage=program_trades_sync status=WAIT_TIMEOUT reason=%s", str(exc))
        try:
            proc.kill()
            proc.wait()
        except Exception as kill_exc:  # noqa: BLE001 - shutdown proceeds regardless
            logger.warning("[DAEMON] stage=program_trades_sync status=KILL_FAIL reason=%s", str(kill_exc))


class DaemonRunner:
    """Own one daemon process's date-scoped state and supervised children.

    The runner keeps state transitions explicit so a retry, holiday, shutdown, and
    EOD cycle cannot accidentally reuse a prior day's process or status.
    """

    def __init__(
        self,
        *,
        runtime: CollectorRuntime,
        shutdown: threading.Event | None,
        now: Callable[[], dt.datetime],
        sleep: Callable[[float], None],
    ) -> None:
        self._runtime = runtime
        self._shutdown = shutdown
        self._now = now
        self._sleep = sleep
        self._cfg = runtime.collector
        self._paths = runtime.paths
        self._after_cfg = runtime.aftermarket
        self._snapshot_cfg = runtime.snapshot
        self._sched = replace(
            runtime.collector.schedule,
            after_market_enabled=runtime.collector.after_market_enabled,
        )
        self._state = DaemonState()
        self._children = DaemonChildren()
        self._anchors: SessionAnchors | None = None
        self._anchors_date: dt.date | None = None
        self._pinger: HealthcheckPinger | NoopPinger = NoopPinger()
        self._ping_interval_s: float = 60.0
        self._liveness_enabled: bool = False
        self._last_ping_mono: float | None = None
        self._gate = TradingDayGate(
            resolver=lambda d: _resolve_trading_day_with_cache(
                d, runtime.paths.calendar_cache, runtime.paths.session_calendar_dir
            )
        )
        self._run_id: str | None = None

    def _maybe_ping(self, now_mono: float) -> None:
        """Send a liveness ``success`` ping at most once per interval.

        Runs on the main thread only, so a hung ``step`` stops pings and the
        external dead-man's switch fires. Attempts are rate-limited to one per
        interval; the next scheduled ping is the retry.
        """
        if not self._liveness_enabled:
            return
        if self._last_ping_mono is not None and now_mono - self._last_ping_mono < self._ping_interval_s:
            return
        self._last_ping_mono = now_mono
        self._pinger.success()

    @property
    def state(self) -> DaemonState:
        return self._state

    @property
    def children(self) -> DaemonChildren:
        return self._children

    def stop_children(self) -> None:
        """Stop all owned child processes within the existing shutdown deadline."""
        targets: list[ProcessSupervisor] = []
        if self._children.regular is not None:
            targets.append(self._children.regular)
        targets.extend(self._children.aftermarket.values())
        if self._children.snapshot is not None:
            targets.append(self._children.snapshot)
        sync = self._children.program_sync
        sync_running = sync is not None and sync.poll() is None
        # 모든 자식에 먼저 SIGTERM 을 보내 대기를 겹쳐야 compose 30s 유예 안에 끝난다 (순차 대기 시 최대 40s).
        started = time.monotonic()
        if sync_running:
            assert sync is not None
            sync.terminate()
        counts = stop_supervisors(targets, deadline_s=SHUTDOWN_CHILD_DEADLINE_S)
        if sync_running:
            assert sync is not None
            remaining = max(1.0, SHUTDOWN_CHILD_DEADLINE_S - (time.monotonic() - started))
            _stop_sync_child(sync, grace_s=remaining, already_signalled=True)
            self._children.program_sync = None
            self._children.program_sync_started_at = None
        raw = getattr(self._shutdown, "signal_name", "UNKNOWN") if self._shutdown is not None else "UNKNOWN"
        sig = str(raw) if raw else "UNKNOWN"
        logger.info(
            "[DAEMON] stage=shutdown status=STOPPED signal=%s graceful=%d killed=%d not_running=%d",
            sig,
            counts["graceful"],
            counts["killed"],
            counts["not_running"],
            extra=EVENT,
        )

    def _poll_program_sync(self, now: dt.datetime) -> None:
        """Reap the nightly sync child without blocking the daemon loop."""
        proc = self._children.program_sync
        if proc is None:
            return
        code = proc.poll()
        if code is None:
            started = self._children.program_sync_started_at
            if started is not None:
                timeout_s = TossProgramTradesSettings().sync_timeout_s
                if (now - started).total_seconds() > timeout_s:
                    _stop_sync_child(proc, grace_s=10.0)
                    logger.critical("[DAEMON] stage=program_trades_sync status=FAIL reason=timeout")
                    self._children.program_sync = None
                    self._children.program_sync_started_at = None
            return
        self._children.program_sync = None
        self._children.program_sync_started_at = None
        if code == 0:
            logger.info("[DAEMON] stage=program_trades_sync status=OK exit_code=0")
        elif code == 2:
            return
        else:
            logger.critical("[DAEMON] stage=program_trades_sync status=FAIL exit_code=%s", code)

    def _maybe_launch_program_sync(self, now: dt.datetime) -> None:
        """Launch the nightly sync once per business day after EOD completes."""
        st = self._state
        ch = self._children
        if ch.program_sync is not None and ch.program_sync.poll() is None:
            return
        if st.eod_attempted_for is None or st.program_sync_day == st.eod_attempted_for:
            return
        today = st.eod_attempted_for
        view = self._gate.view(today, now)
        if view.status is not TradingDayStatus.BUSINESS or view.trading_day is None:
            return
        sync_settings = TossProgramTradesSettings()
        if not sync_settings.auto_backfill_enabled:
            return
        cmd = _program_sync_cmd(today, view.trading_day.previous_business_day)
        try:
            ch.program_sync = subprocess.Popen(cmd, env=child_process_env({}))  # noqa: S603 - fixed argv, no shell
        except OSError as exc:
            logger.critical("[DAEMON] stage=program_trades_sync status=FAIL exit_code=launch reason=%s", str(exc))
            ch.program_sync = None
            return
        ch.program_sync_started_at = now
        st.program_sync_day = today
        logger.info("[DAEMON] stage=program_trades_sync status=STARTED date=%s", today.isoformat())

    def step(self, now: dt.datetime) -> float:
        """Advance one scheduled daemon cycle and return the next delay.

        Args:
            now: Current timezone-aware instant.

        Returns:
            Seconds to wait before the next cycle.

        Raises:
            KrxAlphaError: For failures already propagated by the current loop.
        """
        st = self._state
        ch = self._children
        cfg = self._cfg
        paths = self._paths
        after_cfg = self._after_cfg
        snapshot_cfg = self._snapshot_cfg
        sched = self._sched
        st.cycle += 1
        today_kst = now.astimezone(_KST).date()
        anchors = self._anchors
        if self._anchors_date != today_kst or anchors is None:
            anchors = resolve_session_anchors(paths.session_calendar_dir, today_kst)
            self._anchors = anchors
            self._anchors_date = today_kst
        # 전일 prefetch 가 없는 채로 시각 이동일 장중에 재기동하면 표준 앵커로 상태가 먼저 정해져
        # 장중 EOD 나 세션 누락이 생긴다. 앵커를 알 수 있는 시각이면 게이트를 먼저 풀어 앵커를 확정한다.
        if (
            anchors.source is AnchorSource.DEFAULT
            and now.astimezone(_KST).weekday() < 5
            and now.astimezone(_KST).time() >= sched.streamer_start
            and st.anchors_reloaded_for != today_kst
        ):
            early_day = self._gate.view(today_kst, now)
            if early_day.status is TradingDayStatus.BUSINESS:
                st.anchors_reloaded_for = today_kst
                anchors = resolve_session_anchors(paths.session_calendar_dir, today_kst)
                self._anchors = anchors
        state = get_target_state(now, schedule_for(anchors, sched))
        day = None
        if state in (
            SessionState.STREAMER_ACTIVE,
            SessionState.FULL_ACTIVE,
            SessionState.AFTER_MARKET_ACTIVE,
            SessionState.POST_MARKET_EOD,
        ):
            day = self._gate.view(now.astimezone(_KST).date(), now)
            if day.status is TradingDayStatus.BUSINESS and st.anchors_reloaded_for != day.date:
                st.anchors_reloaded_for = day.date
                anchors = resolve_session_anchors(paths.session_calendar_dir, day.date)
                self._anchors = anchors
                self._anchors_date = day.date
            if day.status is TradingDayStatus.HOLIDAY and st.holiday_skip_logged_for != day.date:
                logger.info(
                    "[DAEMON] stage=session status=SKIP reason=market_holiday date=%s",
                    day.date.isoformat(),
                    extra=EVENT,
                )
                st.holiday_skip_logged_for = day.date
        logger.debug("[DAEMON] cycle=%d now=%s state=%s", st.cycle, now.strftime("%Y-%m-%d %H:%M:%S"), state)
        if state != st.prev_state:
            logger.info(
                "[DAEMON] stage=state_change from=%s to=%s cycle=%d", st.prev_state, state, st.cycle, extra=EVENT
            )
            st.prev_state = state
        streamer_alive = st.last_supervisor_result in ("started", "running", "restarted")
        if st.last_summary is None or (now - st.last_summary).total_seconds() >= HEARTBEAT_SUMMARY_S:
            logger.info(
                "[DAEMON] stage=heartbeat state=%s cycle=%d streamer_alive=%s streamer_restarts=%d",
                state,
                st.cycle,
                streamer_alive,
                st.streamer_restarts,
            )
            st.last_summary = now

        if state == SessionState.WEEKEND_SLEEP:
            sleep_sec = 3600.0  # 주말엔 1시간씩 대기
        elif state == SessionState.PRE_MARKET_SLEEP:
            sleep_sec = min(calc_sleep_seconds(now, sched.streamer_start), 300.0)
        elif state in (SessionState.STREAMER_ACTIVE, SessionState.FULL_ACTIVE, SessionState.AFTER_MARKET_ACTIVE):
            assert day is not None
            today = day.date
            if day.status is TradingDayStatus.HOLIDAY:
                if ch.regular is not None:
                    ch.regular.stop(timeout_s=15.0)
                    ch.regular = None
                    st.last_supervisor_result = None
                    st.degraded_active = False
                if ch.snapshot is not None:
                    ch.snapshot.stop(timeout_s=15.0)
                    ch.snapshot = None
                    st.last_snapshot_result = None
                for _sup in ch.aftermarket.values():
                    _sup.stop(timeout_s=15.0)
                ch.aftermarket.clear()
                st.aftermarket_results.clear()
                if state is SessionState.STREAMER_ACTIVE:
                    _holiday_target = sched.scanner_start
                elif state is SessionState.FULL_ACTIVE:
                    _holiday_target = sched.market_close
                else:
                    _holiday_target = sched.after_market_close
                sleep_sec = min(calc_sleep_seconds(now, _holiday_target), HOLIDAY_SLEEP_CAP_S)
            else:
                if state is not SessionState.AFTER_MARKET_ACTIVE and today != st.orchestration_day:
                    # EOD를 거치지 못한 전일 스트리머가 남아 있으면 전일 session-date로 재기동되므로 먼저 정리한다
                    if ch.regular is not None:
                        stale_stop = ch.regular.stop(timeout_s=15.0)
                        logger.warning("[DAEMON] stage=streamer status=STOP_STALE_DAY stop_result=%s", stale_stop)
                        ch.regular = None
                        st.last_supervisor_result = None
                    if ch.snapshot is not None:
                        ch.snapshot.stop(timeout_s=15.0)
                        ch.snapshot = None
                        st.last_snapshot_result = None
                    st.orchestration_day = today
                    st.next_orchestration_at = None
                    st.orchestration_attempts = 0
                    st.degraded_active = False
                    st.last_ingest_check = None
                    st.ingest_stale = False
                    st.next_aftermarket_refresh_at = None
                if (
                    state is not SessionState.AFTER_MARKET_ACTIVE
                    and st.orchestrated_for != today
                    and (st.next_orchestration_at is None or now >= st.next_orchestration_at)
                ):
                    trading_day = day.trading_day
                    if st.preflight_day != today:
                        st.preflight_day = today
                        _kis_token_preflight(paths, today, snapshot_cfg)
                        self._maybe_ping(time.monotonic())
                    st.orchestration_attempts += 1
                    try:
                        ready = run_session_orchestration(
                            today=today,
                            settings=cfg,
                            trading_day=trading_day,
                            progress=lambda: self._maybe_ping(time.monotonic()),
                        )
                    except Exception as e:  # noqa: BLE001 - 오케스트레이션 실패가 데몬 전체를 죽이지 않도록 격리
                        logger.critical(
                            "[DAEMON] stage=session status=FAIL reason=orchestration_error error=%s",
                            str(e),
                            exc_info=True,
                        )
                        ready = False
                    if ready:
                        st.orchestrated_for = today
                        st.next_orchestration_at = None
                        if ch.regular is not None and st.degraded_active:
                            stop_result = ch.regular.stop(timeout_s=15.0)
                            logger.info(
                                "[DAEMON] stage=streamer status=REPLACE_DEGRADED stop_result=%s",
                                stop_result,
                                extra=EVENT,
                            )
                        ch.regular = ProcessSupervisor(
                            cmd=_stream_cmd(today, paths, degraded_reason=None),
                            breaker=RestartCircuitBreaker(),
                        )
                        st.degraded_active = False
                        st.last_supervisor_result = None
                        if snapshot_cfg.enabled:
                            if ch.snapshot is not None:
                                ch.snapshot.stop(timeout_s=15.0)
                            ch.snapshot = ProcessSupervisor(
                                cmd=_snapshot_cmd(today, paths),
                                breaker=RestartCircuitBreaker(),
                            )
                            st.last_snapshot_result = None
                    else:
                        st.next_orchestration_at = now + dt.timedelta(seconds=cfg.orchestration_retry_s)
                        log_fn = logger.critical if st.orchestration_attempts == 1 else logger.warning
                        log_fn(
                            "[DAEMON] stage=session status=FAIL reason=candidates_not_ready date=%s attempt=%d next_retry=%s",
                            today.isoformat(),
                            st.orchestration_attempts,
                            st.next_orchestration_at.isoformat(),
                        )
                        if ch.regular is None:
                            rev = _degraded_candidates_rev(
                                paths.candidates, today, max_age_days=cfg.degraded_candidates_max_age_days
                            )
                            if rev is not None:
                                ch.regular = ProcessSupervisor(
                                    cmd=_stream_cmd(today, paths, degraded_reason="orchestration_failed"),
                                    breaker=RestartCircuitBreaker(),
                                )
                                if snapshot_cfg.enabled and ch.snapshot is None:
                                    ch.snapshot = ProcessSupervisor(
                                        cmd=_snapshot_cmd(today, paths),
                                        breaker=RestartCircuitBreaker(),
                                    )
                                    st.last_snapshot_result = None
                                st.degraded_active = True
                                st.last_supervisor_result = None
                                logger.critical(
                                    "[DAEMON] stage=streamer status=DEGRADED reason=orchestration_failed candidates_rev=%d",
                                    rev,
                                )
                if ch.regular is not None:
                    result = ch.regular.ensure_running()
                    if result == "started":
                        logger.info("[DAEMON] stage=streamer status=STARTED", extra=EVENT)
                    elif result == "restarted":
                        st.streamer_restarts += 1
                        logger.warning(
                            "[DAEMON] stage=streamer status=RESTARTED exit_code=%s restarts=%d",
                            ch.regular.last_exit_code,
                            st.streamer_restarts,
                        )
                    elif result == "circuit_open" and st.last_supervisor_result != "circuit_open":
                        logger.critical("[DAEMON] stage=streamer status=FAIL reason=circuit_open")
                    st.last_supervisor_result = result
                snapshot_run_end = shift_snapshot_settings(snapshot_cfg, anchors).run_end
                if ch.snapshot is not None and now.astimezone(_KST).time() < snapshot_run_end:
                    snapshot_result = ch.snapshot.ensure_running()
                    if snapshot_result == "restarted":
                        logger.warning(
                            "[DAEMON] stage=snapshots status=RESTARTED exit_code=%s",
                            ch.snapshot.last_exit_code,
                        )
                    elif snapshot_result == "circuit_open" and st.last_snapshot_result != "circuit_open":
                        logger.critical("[DAEMON] stage=snapshots status=FAIL reason=circuit_open")
                    st.last_snapshot_result = snapshot_result
                watch_start = _shifted_time(anchors.regular_open, INGEST_WATCH_START_OFFSET)
                watch_end = _shifted_time(anchors.closing_auction_start, INGEST_WATCH_END_OFFSET)
                if (
                    state == SessionState.FULL_ACTIVE
                    and watch_start <= now.astimezone(_KST).time() < watch_end
                    and (st.last_ingest_check is None or (now - st.last_ingest_check).total_seconds() >= INGEST_CHECK_S)
                ):
                    st.last_ingest_check = now
                    age = _journal_age_s(paths.journal_root, cfg.vendor, today, now)
                    stale = age is None or age > INGEST_STALE_S
                    if stale and not st.ingest_stale and age is None and day.status is TradingDayStatus.UNKNOWN:
                        if st.possible_holiday_warned_for != today:
                            logger.warning(
                                "[DAEMON] stage=ingest_watchdog status=POSSIBLE_HOLIDAY date=%s",
                                today.isoformat(),
                            )
                            st.possible_holiday_warned_for = today
                    elif stale and not st.ingest_stale:
                        logger.critical(
                            "[DAEMON] stage=ingest_watchdog status=STALE date=%s age_s=%s",
                            today.isoformat(),
                            "none" if age is None else str(int(age)),
                        )
                    elif not stale and st.ingest_stale and age is not None:
                        logger.warning(
                            "[DAEMON] stage=ingest_watchdog status=RECOVERED date=%s age_s=%d",
                            today.isoformat(),
                            int(age),
                        )
                    st.ingest_stale = stale and not (
                        age is None and day.status is TradingDayStatus.UNKNOWN
                    )
                if (
                    state == SessionState.FULL_ACTIVE
                    and cfg.after_market_enabled
                    and now.astimezone(_KST).time() >= anchors.shift_post_close(after_cfg.selection_time)
                    and st.aftermarket_refresh_day != today
                    and (st.next_aftermarket_refresh_at is None or now >= st.next_aftermarket_refresh_at)
                ):
                    excluded_symbols: frozenset[str]
                    if any(paths.bars_daily_dir.glob("*.parquet")):
                        try:
                            latest_bars_date = latest_partition_date(paths.bars_daily_dir)
                            if latest_bars_date is None:
                                raise PartitionedStoreError(
                                    f"no month partitions in {paths.bars_daily_dir}"
                                )
                            eligibility_bars = (
                                scan_month_partitions(
                                    paths.bars_daily_dir,
                                    min_date=latest_bars_date
                                    - dt.timedelta(days=cfg.selection_lookback_calendar_days),
                                )
                                .select(["date", "symbol", "stock_cert_kind", "section"])
                                .collect()
                            )
                        except (OSError, pl.exceptions.PolarsError, PartitionedStoreError):
                            excluded_symbols = frozenset()
                            if st.aftermarket_eligibility_day != today:
                                st.aftermarket_eligibility_day = today
                                logger.warning(
                                    "[ALGO] stage=aftermarket_eligibility status=DEGRADED reason=bars_unreadable"
                                )
                        else:
                            excluded_symbols = ineligible_security_symbols(eligibility_bars)
                    else:
                        excluded_symbols = frozenset()
                        if st.aftermarket_eligibility_day != today:
                            st.aftermarket_eligibility_day = today
                            logger.warning(
                                "[ALGO] stage=aftermarket_eligibility status=DEGRADED reason=no_bars_store"
                            )
                    try:
                        refresh_aftermarket_candidates(
                            session_date=today,
                            generated_at=now,
                            client=_build_kis_client(paths),
                            out_path=paths.aftermarket_candidates(today),
                            capacity=after_cfg.max_symbols,
                            excluded_symbols=excluded_symbols,
                        )
                        st.aftermarket_refresh_day = today
                        st.next_aftermarket_refresh_at = None
                    except (MissingCredentialsError, KisApiError, AftermarketUniverseError, CandidateFileError) as exc:
                        st.next_aftermarket_refresh_at = now + dt.timedelta(seconds=60.0)
                        logger.critical(
                            "[DAEMON] stage=aftermarket_reselection status=FAIL reason=%s next_retry=%s",
                            str(exc),
                            st.next_aftermarket_refresh_at.isoformat(),
                        )
                if state == SessionState.AFTER_MARKET_ACTIVE and cfg.after_market_enabled:
                    if st.aftermarket_plan_day != today:
                        try:
                            snapshot = read_candidate_snapshot(paths.aftermarket_candidates(today), expected_session_date=today, expected_session="aftermarket", max_candidates=after_cfg.max_symbols)
                            st.aftermarket_plan = plan_aftermarket_shards(symbols=tuple(str(row["symbol"]) for row in snapshot.candidates), credentials=load_kis_data_credentials(), pair_capacity_per_connection=int(after_cfg.pair_capacity_per_connection or 0), krx_streams=after_cfg.krx_streams, nxt_streams=after_cfg.nxt_streams)
                        except (CandidateFileError, KrxAlphaError) as exc:
                            logger.critical("[DAEMON] stage=aftermarket_plan status=FAIL reason=%s", str(exc))
                            st.aftermarket_plan = ()
                        st.aftermarket_plan_day = today
                    kst_time = now.astimezone(_KST).time()
                    due: list[MarketVenue] = []
                    if kst_time >= anchors.shift_post_close(NXT_AFTERMARKET_START):
                        due.append(MarketVenue.NXT)
                    if kst_time >= anchors.shift_post_close(KRX_AFTERMARKET_START):
                        due.append(MarketVenue.KRX)
                    for shard in st.aftermarket_plan:
                        if shard.venue in due:
                            supervisor_key = f"{shard.venue.value}:{shard.shard_index}"
                            if supervisor_key not in ch.aftermarket:
                                ch.aftermarket[supervisor_key] = ProcessSupervisor(
                                    cmd=_aftermarket_stream_cmd(today, paths, shard=shard),
                                    breaker=RestartCircuitBreaker(),
                                )
                    for supervisor_key, sup in ch.aftermarket.items():
                        result = sup.ensure_running()
                        st.aftermarket_results[supervisor_key] = result
                        if result == "restarted":
                            st.aftermarket_restarts += 1
                            logger.warning(
                                "[DAEMON] stage=aftermarket_stream status=RESTARTED key=%s exit_code=%s",
                                supervisor_key,
                                sup.last_exit_code,
                            )
                        elif result == "circuit_open" and supervisor_key not in st.aftermarket_circuit_alerted:
                            logger.critical(
                                "[DAEMON] stage=aftermarket_stream status=FAIL reason=circuit_open key=%s",
                                supervisor_key,
                            )
                            st.aftermarket_circuit_alerted.add(supervisor_key)
                sleep_sec = 10.0
        elif state == SessionState.POST_MARKET_EOD:
            if ch.regular is not None:
                stop_result = ch.regular.stop(timeout_s=15.0)
                logger.info("[DAEMON] stage=streamer_stop result=%s", stop_result, extra=EVENT)
                ch.regular = None
                st.last_supervisor_result = None
                st.degraded_active = False
            if ch.snapshot is not None:
                ch.snapshot.stop(timeout_s=15.0)
                ch.snapshot = None
                st.last_snapshot_result = None
            # 15:40 EOD 유지보수 (정규화 및 오래된 저널 prune + L1 오프로드)
            # 같은 거래일에 60초마다 재실행하지 않도록 날짜당 1회만 시도한다
            ref_day = now.astimezone(_KST).date()
            if st.eod_attempted_for != ref_day:
                st.eod_attempted_for = ref_day
                for sup in ch.aftermarket.values(): sup.stop(timeout_s=15.0)  # noqa: E701 - 20:00 KIS 수집기 종료
                ch.aftermarket.clear()
                st.aftermarket_results.clear()
                st.aftermarket_circuit_alerted.clear()
                assert day is not None
                if day.status is TradingDayStatus.HOLIDAY:
                    housekeeping = _run_eod_housekeeping(cfg, paths, ref_day, progress=lambda: self._maybe_ping(time.monotonic()))
                    _check_backup(
                        cfg.host_backup_status_path,
                        paths,
                        ref_day,
                        now=now,
                        max_age=dt.timedelta(hours=cfg.host_backup_max_age_h),
                    )
                    logger.info(
                        "[DAEMON] stage=eod_maintenance status=HOLIDAY deleted_partitions=%d uploaded=%d purged=%d date=%s",
                        housekeeping.deleted,
                        housekeeping.uploaded,
                        housekeeping.purged,
                        ref_day.isoformat(),
                        extra=EVENT,
                    )
                else:
                    aftermarket_manifests = (
                        [
                            paths.aftermarket_manifest_path(ref_day, shard.venue, shard.shard_index)
                            for shard in st.aftermarket_plan
                        ]
                        if cfg.after_market_enabled
                        else []
                    )
                    aftermarket_blocked = cfg.after_market_enabled and not aftermarket_eod_ready(
                        manifests=aftermarket_manifests,
                        date=ref_day,
                        now=now,
                        expected_shards=tuple(st.aftermarket_plan),
                        after_market_end=anchors.after_market_end,
                    )
                    if aftermarket_blocked: logger.critical("[DAEMON] stage=eod_maintenance status=DEGRADED reason=aftermarket_not_ready date=%s", ref_day.isoformat())  # noqa: E701
                    housekeeping = _run_eod_housekeeping(cfg, paths, ref_day, progress=lambda: self._maybe_ping(time.monotonic()))
                    reconcile_ok = True
                    reconciled = False
                    if day.status is TradingDayStatus.BUSINESS:
                        try:
                            reconciled = check_session_reconciliation(
                                manifest_path=paths.manifest_path(ref_day),
                                date=ref_day,
                                journal_root=paths.journal_root,
                                streams=cfg.streams,
                                vendor=cfg.vendor,
                                venue=MarketVenue.KRX.value,
                                session=MarketSession.REGULAR.value,
                                regular_open=anchors.regular_open,
                                regular_close=anchors.regular_close,
                            )
                            if not reconciled:
                                reconcile_ok = False
                                logger.critical(
                                    "[DAEMON] stage=eod_maintenance status=FAIL reason=session_data_gap date=%s",
                                    ref_day.isoformat(),
                                )
                        except OSError as e:
                            reconcile_ok = False
                            reconciled = False
                            logger.critical(
                                "[DAEMON] stage=eod_reconciliation status=FAIL error=%s", str(e), exc_info=True
                            )
                    else:
                        logger.warning("[DAEMON] stage=eod_reconciliation status=SKIP reason=calendar_unknown")
                    backup_missing, backup_ok, host_backup = _check_backup(
                        cfg.host_backup_status_path,
                        paths,
                        ref_day,
                        now=now,
                        max_age=dt.timedelta(hours=cfg.host_backup_max_age_h),
                    )
                    eod_status = (
                        "OK"
                        if (
                            housekeeping.maintenance_ok
                            and housekeeping.offload_ok
                            and reconcile_ok
                            and backup_ok
                            and not aftermarket_blocked
                        )
                        else "DEGRADED"
                    )
                    logger.info(
                        "[DAEMON] stage=eod_maintenance deleted_partitions=%d uploaded=%d purged=%d status=%s",
                        housekeeping.deleted,
                        housekeeping.uploaded,
                        housekeeping.purged,
                        eod_status,
                        extra=EVENT,
                    )
                    digest_body = "\n".join(
                        [
                            f"run_id={self._run_id}",
                            f"status={eod_status}",
                            f"deleted_partitions={housekeeping.deleted}",
                            f"uploaded={housekeeping.uploaded}",
                            f"purged={housekeeping.purged}",
                            f"reconciled={reconciled}",
                            f"aftermarket_ready={not aftermarket_blocked}",
                            f"backup_missing={len(backup_missing)}",
                            f"host_backup={host_backup}",
                            f"streamer_restarts={st.streamer_restarts}",
                            f"aftermarket_restarts={st.aftermarket_restarts}",
                            f"orchestration_attempts={st.orchestration_attempts}",
                        ]
                    )
                    send_digest(f"[krx-alpha] EOD {ref_day.isoformat()} {eod_status}", digest_body)
                    st.aftermarket_restarts = 0
            sleep_sec = 60.0
        else:  # NIGHT_SLEEP
            sleep_sec = min(calc_sleep_seconds(now, sched.streamer_start), 1800.0)
        self._poll_program_sync(now)
        self._maybe_launch_program_sync(now)
        if self._children.program_sync is not None:
            # 야간 수면(최대 30분) 동안 타임아웃 점검이 밀리지 않도록 sync 자식이 도는 동안은 짧게 깨어난다.
            sleep_sec = min(sleep_sec, PROGRAM_SYNC_POLL_S)
        return sleep_sec


def _migrate_market_stores(paths: DataPaths) -> None:
    """Migrate legacy single-file bars/program-trade stores to month partitions.

    A failed migration only logs; orchestration then fails closed on the
    missing partitioned store.
    """
    for name, legacy, root, keys in (
        ("bars", paths.legacy_bars_file, paths.bars_daily_dir, ("date", "symbol")),
        (
            "program_trades",
            paths.legacy_program_trades_file,
            paths.program_trades_dir,
            ("symbol", "date"),
        ),
    ):
        try:
            migrate_single_file_store(legacy, root, key_columns=keys, sort_columns=("symbol", "date"))
        except PartitionedStoreError as exc:
            logger.critical(
                "[DATA] stage=store_migration status=FAIL store=%s reason=%s",
                name,
                str(exc),
            )


def run_collector_daemon(
    *,
    settings: CollectorSettings | None = None,
    sleep_fn: Callable[[float], None] | None = None,
    max_cycles: int | None = None,
    now_fn: Callable[[], dt.datetime] | None = None,
    shutdown: threading.Event | None = None,
) -> None:
    """Collector daemon main loop.

    Shutdown: when ``shutdown`` is set the loop stops starting work,
    stops all supervised children via ``stop_supervisors`` within ``SHUTDOWN_CHILD_DEADLINE_S`` and
    returns. A cycle already inside EOD finishes that call first; the deploy session gate keeps
    deploys out of the EOD window.
    """
    sleeper = sleep_fn if sleep_fn is not None else time.sleep
    runtime = resolve_collector_runtime(collector=settings)

    run_id = configure_logging("daemon", log_dir=runtime.paths.logs_dir if ObservabilitySettings().persistent_logs else None)
    logger.info("[DAEMON] stage=start status=ONLINE timezone=Asia/Seoul run_id=%s", run_id, extra=EVENT)

    liveness = LivenessSettings()
    if liveness.enabled:
        pinger: HealthcheckPinger | NoopPinger = HealthcheckPinger(
            liveness.healthcheck_url,
            timeout_s=liveness.ping_timeout_s,
            run_id=run_id,
        )
    else:
        pinger = NoopPinger()
        logger.warning("[SYS] stage=healthcheck status=DISABLED")

    lifecycle_path = runtime.paths.daemon_lifecycle

    def _write_record(record: DaemonLifecycleRecord) -> None:
        try:
            write_lifecycle(lifecycle_path, record)
        except OSError as exc:
            logger.warning("[SYS] stage=daemon_lifecycle status=WRITE_FAIL reason=%s", type(exc).__name__)

    clock = now_fn if now_fn is not None else (lambda: dt.datetime.now(_KST))
    previous = read_lifecycle(lifecycle_path)
    alert_day = previous.crash_alert_day if previous is not None else None
    alerts_sent = previous.crash_alerts_sent if previous is not None else 0
    if previous is not None and not previous.clean_exit and previous.crash_error is None:
        today = clock().astimezone(_KST).date()
        if crash_alert_allowed(previous, today, liveness.crash_alert_daily_budget):
            logger.critical(
                "[SYS] stage=daemon_restart status=UNCLEAN reason=killed previous_run_id=%s",
                previous.run_id,
            )
            # OOM/SIGKILL 루프는 재기동마다 여기로 오므로 크래시와 같은 일일 예산을 영속 차감한다.
            alerts_sent = alerts_sent + 1 if alert_day == today else 1
            alert_day = today
        else:
            logger.error(
                "[SYS] stage=daemon_restart status=UNCLEAN reason=killed previous_run_id=%s",
                previous.run_id,
            )
        pinger.fail("unclean_restart:killed")
    elif previous is not None and not previous.clean_exit:
        logger.warning(
            "[SYS] stage=daemon_restart status=RESTARTED_AFTER_CRASH previous_run_id=%s",
            previous.run_id,
        )
    record = DaemonLifecycleRecord(
        run_id=run_id,
        started_at=dt.datetime.now(_KST),
        clean_exit=False,
        crash_error=None,
        crash_alert_day=alert_day,
        crash_alerts_sent=alerts_sent,
    )
    _write_record(record)
    pinger.start()

    def _finish_graceful() -> None:
        nonlocal record
        record = replace(record, clean_exit=True)
        _write_record(record)
        pinger.success()
        shutdown_logging()

    # lifecycle 기록 직후부터 경계를 둔다: 이관·러너 생성 실패도 크래시로 보고되어야 한다.
    try:
        _migrate_market_stores(runtime.paths)

        runner = DaemonRunner(runtime=runtime, shutdown=shutdown, now=clock, sleep=sleeper)
        runner._run_id = run_id
        runner._pinger = pinger
        runner._ping_interval_s = liveness.ping_interval_s
        runner._liveness_enabled = liveness.enabled
        runner._last_ping_mono = time.monotonic()

        while True:
            if shutdown is not None and shutdown.is_set():
                runner.stop_children()
                _finish_graceful()
                return
            delay = runner.step(clock())
            runner._maybe_ping(time.monotonic())
            if liveness.enabled:
                delay = min(delay, liveness.ping_interval_s)
            logger.debug("[DAEMON] sleeping for %.1f seconds...", delay)
            if shutdown is not None:
                if sleep_fn is not None:
                    sleeper(delay)
                else:
                    shutdown.wait(delay)
                if shutdown.is_set():
                    runner.stop_children()
                    _finish_graceful()
                    return
            else:
                sleeper(delay)

            if max_cycles is not None and runner.state.cycle >= max_cycles:
                logger.info("[DAEMON] reached max_cycles=%d, exiting gracefully.", max_cycles)
                _finish_graceful()
                break
    except Exception as exc:
        error_type = type(exc).__name__
        crash_day = clock().astimezone(_KST).date()
        if crash_alert_allowed(record, crash_day, liveness.crash_alert_daily_budget):
            logger.critical("[SYS] stage=daemon_crash error=%s", error_type, exc_info=True)
            sent = record.crash_alerts_sent + 1 if record.crash_alert_day == crash_day else 1
            record = replace(
                record, crash_error=error_type, crash_alert_day=crash_day, crash_alerts_sent=sent
            )
        else:
            logger.error("[SYS] stage=daemon_crash error=%s", error_type, exc_info=True)
            record = replace(record, crash_error=error_type)
        _write_record(record)
        pinger.fail(f"daemon_crash:{error_type}")
        shutdown_logging()
        raise
    except BaseException:
        # SystemExit·KeyboardInterrupt 는 의도된 중지다: 다음 기동이 "killed" 로 오분류하지 않게 정상 종료로 남긴다.
        _finish_graceful()
        raise



def main() -> None:
    """Run the daemon and make every termination observable.

    Crashes are reported through a CRITICAL log line (email) and a healthcheck
    ``fail`` before re-raising so Docker still restarts the process; a previous
    run that ended without either a clean exit or a recorded crash (SIGKILL,
    OOM) is reported at the next start.
    """
    shutdown = threading.Event()

    def _handle(signum: int, _frame: Any) -> None:
        try:
            shutdown.signal_name = signal.Signals(signum).name  # type: ignore[attr-defined]
        except Exception:
            shutdown.signal_name = str(signum)  # type: ignore[attr-defined]
        shutdown.set()

    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT, _handle)
    run_collector_daemon(shutdown=shutdown)


if __name__ == "__main__":  # pragma: no cover
    main()
