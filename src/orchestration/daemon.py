"""Collector 24/7 daemon 오케스트레이션 (service 직접 호출, CLI 역방향 의존 제거)."""

from __future__ import annotations

import datetime as dt
import importlib
import logging
import pathlib
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from enum import StrEnum
from typing import Any, ClassVar
from zoneinfo import ZoneInfo

import polars as pl
import requests

from src.brokers.kis.auth import KisAppAuth, kis_app_key_fingerprint
from src.brokers.kis.data import KisDataClient
from src.brokers.kis.stack import build_kis_data_slot_stack, build_kis_rest_stack
from src.core.calendar import SessionState, calc_sleep_seconds, get_target_state, schedule_for
from src.core.config import (
    AuditMode,
    AuditSettings,
    CollectorRuntime,
    CollectorSettings,
    DataPaths,
    ExecutionSettings,
    KisCredentials,
    KisTokenSettings,
    KrxCredentials,
    LivenessSettings,
    ObservabilitySettings,
    RegularKisSettings,
    ShardWatchdogSettings,
    SnapshotSettings,
    TickTapeBackfillSettings,
    TossCredentials,
    TossProgramTradesSettings,
    UniverseTierSettings,
    child_process_env,
    load_credentials,
    resolve_collector_runtime,
)
from src.core.errors import KrxAlphaError, MissingCredentialsError
from src.core.healthcheck import HealthcheckPinger, NoopPinger
from src.core.lifecycle import DaemonLifecycleRecord, crash_alert_allowed, read_lifecycle, write_lifecycle
from src.core.observability import EVENT, configure_logging, send_digest, shutdown_logging
from src.core.session_anchors import (
    STANDARD_NXT_PREMARKET_OPEN,
    AnchorSource,
    SessionAnchors,
    resolve_session_anchors,
    save_session_anchors,
)
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
    PrearmVerdict,
    aftermarket_eod_ready,
    check_aftermarket_prearm,
    check_aftermarket_symbol_coverage,
    check_backup_freshness,
    check_host_backup_freshness,
    check_session_reconciliation,
    classify_remote_failure,
    log_session_audit,
    run_eod_maintenance,
    run_eod_offload,
    run_eod_remote_l0_purge,
)
from src.orchestration.premarket import (
    premarket_collection_due,
    premarket_eod_report,
    premarket_pool_due,
    premarket_stream_cmd,
    premarket_wake_cap_s,
)
from src.orchestration.regular_kis import regular_kis_collection_due, regular_kis_stream_cmd
from src.orchestration.shard_watchdog import (
    ShardDecision,
    ShardObservation,
    ShardVerdict,
    ShardWatchdog,
    shard_last_journal_wall,
    shard_window,
)
from src.orchestration.supervisor import ProcessSupervisor, RestartCircuitBreaker, stop_supervisors
from src.orchestration.trading_day_gate import TradingDayGate, TradingDayStatus, TradingDayView
from src.realtime.contracts import MarketSession, MarketVenue
from src.realtime.kis_sharding import (
    AftermarketShard,
    load_kis_data_credentials,
    plan_aftermarket_shards,
    plan_premarket_shard,
    plan_regular_kis_shards,
)
from src.realtime.manifest import SessionManifest
from src.realtime.streamer import aftermarket_open_time
from src.storage.l0_audit import AuditStatus, SessionAudit, derive_session_audit, write_session_audit
from src.storage.layout import L0_VENDOR_DB, l0_day_journal_glob
from src.storage.remote import RemoteArchiveError
from src.storage.retention import check_disk_watermark
from src.storage.snapshot_store import SnapshotStore
from src.universe.aftermarket import (
    AftermarketUniverseError,
    aftermarket_candidate_session,
    refresh_aftermarket_candidates,
)
from src.universe.ipc import CandidateFileError, read_candidate_snapshot, read_candidates
from src.universe.policy import ineligible_security_symbols
from src.universe.premarket import premarket_pool_ready, refresh_premarket_pool
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
FIRST_FRAME_LEAD: dt.timedelta = dt.timedelta(minutes=30)
FIRST_FRAME_GRACE: dt.timedelta = dt.timedelta(minutes=5)
# 컴포즈 stop_grace_period(30s) 안에 자식 정상종료 + 로그 flush 를 끝내기 위한 공유 데드라인.
SHUTDOWN_CHILD_DEADLINE_S: float = 20.0
HOLIDAY_SLEEP_CAP_S: float = 3600.0
PROGRAM_SYNC_POLL_S: float = 60.0
NIGHTLY_OVERRUN_GRACE_S: float = 120.0
CHILD_STOP_TIMEOUT_S: float = 15.0


def _shifted_time(base: dt.time, offset: dt.timedelta) -> dt.time:
    return (dt.datetime.combine(_BASE_DATE, base) + offset).time()


def _journal_age_s(journal_root: pathlib.Path, vendor: str, day: dt.date, now: dt.datetime) -> float | None:
    files = list(journal_root.glob(l0_day_journal_glob(vendor, day)))
    if not files:
        return None
    return now.timestamp() - max(f.stat().st_mtime for f in files)


def _journal_has_frames(journal_root: pathlib.Path, vendor: str, day: dt.date) -> bool:
    """True when ``vendor`` has at least one non-empty L0 journal file for ``day``.

    Pre-open liveness only needs existence of data: prints before 09:00 are sparse, so a staleness
    age (as in ``_journal_age_s``) would false-alarm between prints.
    """
    for path in journal_root.glob(l0_day_journal_glob(vendor, day)):
        try:
            if path.is_file() and path.stat().st_size > 0:
                return True
        except OSError:
            continue
    return False


def resolve_trading_day(ref_date: dt.date) -> TradingDay | None:
    try:
        creds = load_credentials(TossCredentials)
        return fetch_trading_day(ref_date, app_key=creds.toss_app_key, app_secret=creds.toss_app_secret)
    except (MissingCredentialsError, TossCalendarError) as exc:
        logger.warning("[DATA] stage=trading_day_probe status=DEGRADED reason=%s", str(exc))
        return None


def _build_kis_client(paths: DataPaths) -> KisDataClient:
    """Primary-key KIS data client sharing the execution key's token cache and host pacing file."""
    creds = load_credentials(KisCredentials)
    execution = ExecutionSettings(data_root=paths.root)
    collector = CollectorSettings(data_root=paths.root)
    token_settings = KisTokenSettings()
    return build_kis_rest_stack(
        auth=KisAppAuth(creds.kis_app_key, creds.kis_app_secret),
        cache_dir=token_settings.token_cache_dir,
        session=requests,
        now=lambda: dt.datetime.now(_KST),
        rate_per_s=execution.rest_rate_per_s,
        max_lead_s=collector.kis_rest_max_lead_s,
        timeout_s=execution.request_timeout_s,
        allow_issue=token_settings.allow_issue,
    ).data


def _build_snapshot_preflight_client(
    paths: DataPaths, snapshot_settings: SnapshotSettings | None = None
) -> tuple[KisDataClient, str]:
    """collect-snapshots와 동일한 규칙으로 데이터 슬롯 KIS 클라이언트를 생성한다."""
    snapshot_cfg = snapshot_settings if snapshot_settings is not None else SnapshotSettings()
    stack, key_id = build_kis_data_slot_stack(
        snapshot=snapshot_cfg,
        credentials=load_kis_data_credentials(),
        token_settings=KisTokenSettings(),
        session=requests,
        now=lambda: dt.datetime.now(_KST),
    )
    return stack.data, key_id


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


def _kis_token_preflight(
    paths: DataPaths, today: dt.date, snapshot_settings: SnapshotSettings | None = None
) -> dict[str, str]:
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
    tiers: UniverseTierSettings | None = None,
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
        tiers=tiers,
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


def _tick_open_max_latency_s(audit: SessionAudit, anchors: SessionAnchors) -> str:
    peak: float | None = None
    open_end = _shifted_time(anchors.regular_open, dt.timedelta(minutes=5))
    for entry in audit.latency:
        if entry.stream != "H0STCNT0":
            continue
        for minute in entry.minutes:
            moment = dt.time(int(minute.minute_kst[0:2]), int(minute.minute_kst[3:5]))
            if anchors.regular_open <= moment < open_end and (peak is None or minute.max_s > peak):
                peak = minute.max_s
    return f"{peak:.1f}" if peak is not None else "-"


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
            journal_root=paths.journal_root,
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
                reuse_fresh_l1=True,
            )
            if getattr(post_deleted, "failed", 0) > 0:
                maintenance_ok = False
            deleted = int(deleted) + int(post_deleted)
            verified = verified - getattr(post_deleted, "invalidated_remote_l1", frozenset())
            try:
                run_eod_remote_l0_purge(
                    paths.journal_root,
                    verified,
                    progress=progress,
                    quarantine_root=paths.quarantine_root,
                )
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


def _archive_eod_digest(paths: DataPaths, ref_day: dt.date, subject: str, body: str) -> pathlib.Path | None:
    """Persist the full EOD digest (mail only carries warnings); returns None when the write fails."""
    target = paths.eod_digest_path(ref_day)
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_suffix(".tmp")
        tmp.write_text(f"{subject}\n\n{body}\n", encoding="utf-8")
        tmp.replace(target)
    except OSError as exc:
        logger.warning("[DAEMON] stage=eod_digest_archive status=FAIL reason=%s", type(exc).__name__)
        return None
    return target


def _check_backup(
    status_path: pathlib.Path,
    paths: DataPaths,
    ref_day: dt.date,
    *,
    now: dt.datetime,
    max_age: dt.timedelta,
) -> tuple[list[str], bool, str]:
    host_reason = check_host_backup_freshness(status_path=status_path, now=now, max_age=max_age)
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


def _db_crosscheck_cmd(today: dt.date, paths: DataPaths) -> list[str]:
    return [
        sys.executable,
        "-m",
        "src.cli.main",
        "collect-stream-db",
        "--session-date",
        today.isoformat(),
        "--journal-root",
        str(paths.journal_root),
        "--manifest-path",
        str(paths.db_crosscheck_manifest_path(today)),
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
        str(paths.aftermarket_candidates(today, shard.venue)),
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


def aftermarket_arm_at(
    day: dt.date, venue: MarketVenue, *, anchors: SessionAnchors, prearm_lead_s: float
) -> dt.datetime:
    """KST instant at which ``venue``'s shards for ``day`` must be launched (shifted open minus lead)."""
    if prearm_lead_s <= 0:
        raise ValueError("prearm_lead_s must be positive")
    open_at = dt.datetime.combine(day, aftermarket_open_time(venue, anchors), tzinfo=_KST)
    return open_at - dt.timedelta(seconds=prearm_lead_s)


def _resolve_trading_day_with_cache(
    today: dt.date, cache_path: pathlib.Path, anchors_dir: pathlib.Path
) -> TradingDay | None:
    day = resolve_trading_day(today)
    if day is not None:
        # 캐시·당일 앵커·익일 앵커 저장은 서로 독립: 하나의 쓰기 실패가 나머지를 막지 않는다.
        writes: list[tuple[Callable[[], None], str]] = [
            (lambda: save_trading_day_cache(cache_path, day), "trading_day_cache")
        ]
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


class StreamRole(StrEnum):
    PRIMARY = "primary"
    CROSSCHECK = "crosscheck"
    SNAPSHOT = "snapshot"
    AFTERMARKET = "aftermarket"
    PREMARKET = "premarket"
    REGULAR_KIS = "regular_kis"


class StreamChildKey(StrEnum):
    REGULAR = "regular"
    SNAPSHOT = "snapshot"
    DB_CROSSCHECK = "db_crosscheck"
    AFTERMARKET = "aftermarket"
    PREMARKET = "premarket"
    REGULAR_KIS = "regular_kis"


class ChildLifecycle(StrEnum):
    SESSION = "session"
    SHARDED = "sharded"
    WINDOWED = "windowed"


@dataclass(frozen=True)
class SupervisionLogs:
    """Literal log templates for one SESSION child's supervise outcomes."""

    restarted: str
    circuit_open: str
    circuit_level: int
    started: str | None = None


@dataclass(frozen=True)
class StreamChildSpec:
    """Declarative contract of one daemon-owned streaming child."""

    key: StreamChildKey
    role: StreamRole
    lifecycle: ChildLifecycle
    enabled: Callable[[CollectorRuntime], bool]
    cmd: Callable[[dt.date, DataPaths, str | None], list[str]] | None = None
    active: Callable[[CollectorRuntime, dt.datetime, SessionAnchors], bool] | None = None
    logs: SupervisionLogs | None = None
    requires_primary: bool = False
    starts_in_degraded: bool = False
    silent_on_degraded_upgrade: bool = False
    journal_vendor: Callable[[CollectorSettings], str] | None = None
    manifest_path: Callable[[DataPaths, dt.date], pathlib.Path] | None = None

    def __post_init__(self) -> None:
        is_session = self.lifecycle is ChildLifecycle.SESSION
        callables = (self.cmd, self.active, self.logs)
        if is_session and not all(c is not None for c in callables):
            raise ValueError(f"SESSION child {self.key.value} requires cmd, active and logs")
        if not is_session and any(c is not None for c in callables):
            raise ValueError(f"{self.lifecycle.value} child {self.key.value} must not define cmd, active or logs")
        if self.logs is not None and self.logs.started is not None and self.role is not StreamRole.PRIMARY:
            raise ValueError(f"only PRIMARY children may log a started event: {self.key.value}")
        if self.role is StreamRole.CROSSCHECK:
            if self.logs is None:
                raise ValueError(f"CROSSCHECK child {self.key.value} requires session supervision logs")
            if not self.requires_primary:
                raise ValueError(f"CROSSCHECK child {self.key.value} requires a primary child")
            if self.logs.circuit_level > logging.WARNING:
                raise ValueError(f"CROSSCHECK child {self.key.value} must not alert above WARNING")
        if (self.role is StreamRole.PRIMARY or not is_session) and (
            self.requires_primary or self.starts_in_degraded or self.silent_on_degraded_upgrade
        ):
            raise ValueError(f"{self.key.value} must not set primary-only supervision flags")
        journals_session = self.role in (StreamRole.PRIMARY, StreamRole.CROSSCHECK)
        has_journal_fields = self.journal_vendor is not None and self.manifest_path is not None
        if journals_session and not has_journal_fields:
            raise ValueError(f"{self.key.value} must define journal_vendor and manifest_path")
        if not journals_session and (self.journal_vendor is not None or self.manifest_path is not None):
            raise ValueError(f"{self.key.value} must not define journal_vendor or manifest_path")


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
    nightly_done: dict[NightlyJob, dt.date] = field(default_factory=dict)
    orchestration_day: dt.date | None = None
    next_orchestration_at: dt.datetime | None = None
    orchestration_attempts: int = 0
    degraded_active: bool = False
    last_ingest_check: dt.datetime | None = None
    ingest_stale: bool = False
    streamer_restarts: int = 0
    last_supervisor_result: str | None = None
    last_snapshot_result: str | None = None
    last_db_crosscheck_result: str | None = None
    aftermarket_plan: tuple[AftermarketShard, ...] = ()
    aftermarket_plan_day: dt.date | None = None
    aftermarket_refresh_day: dt.date | None = None
    aftermarket_nxt_refresh_day: dt.date | None = None
    aftermarket_eligibility_day: dt.date | None = None
    next_aftermarket_refresh_at: dt.datetime | None = None
    next_aftermarket_nxt_refresh_at: dt.datetime | None = None
    anchors_reloaded_for: dt.date | None = None
    aftermarket_results: dict[str, str] = field(default_factory=dict)
    aftermarket_circuit_alerted: set[str] = field(default_factory=set)
    aftermarket_restarts: int = 0
    shard_stale_restarts: int = 0
    shard_venue_wide_warned: set[tuple[str, dt.date]] = field(default_factory=set)
    shard_budget_alerted: set[tuple[str, dt.date]] = field(default_factory=set)
    premarket_pool_day: dt.date | None = None
    next_premarket_pool_at: dt.datetime | None = None
    premarket_pool_failed_day: dt.date | None = None
    premarket_plan_day: dt.date | None = None
    premarket_shard: AftermarketShard | None = None
    premarket_circuit_alerted_day: dt.date | None = None
    regular_kis_plan: tuple[AftermarketShard, ...] = ()
    regular_kis_plan_day: dt.date | None = None
    regular_kis_circuit_alerted: set[str] = field(default_factory=set)
    aftermarket_deferred_slots: set[str] = field(default_factory=set)

    _CHILD_RESULT_FIELDS: ClassVar[Mapping[StreamChildKey, str]] = {
        StreamChildKey.REGULAR: "last_supervisor_result",
        StreamChildKey.SNAPSHOT: "last_snapshot_result",
        StreamChildKey.DB_CROSSCHECK: "last_db_crosscheck_result",
    }

    def child_result(self, key: StreamChildKey) -> str | None:
        """Return the last supervise result of a SESSION child."""
        value: str | None = getattr(self, self._CHILD_RESULT_FIELDS[key])
        return value

    def set_child_result(self, key: StreamChildKey, value: str | None) -> None:
        """Store the last supervise result of a SESSION child."""
        setattr(self, self._CHILD_RESULT_FIELDS[key], value)


@dataclass
class DaemonChildren:
    streams: dict[StreamChildKey, ProcessSupervisor] = field(default_factory=dict)
    aftermarket: dict[str, ProcessSupervisor] = field(default_factory=dict)
    regular_kis: dict[str, ProcessSupervisor] = field(default_factory=dict)
    program_sync: Any | None = None
    program_sync_started_at: dt.datetime | None = None
    nightly: subprocess.Popen[bytes] | None = None
    nightly_job: NightlyJob | None = None
    nightly_started_at: dt.datetime | None = None
    degraded_regular: ProcessSupervisor | None = None

    def get(self, key: StreamChildKey) -> ProcessSupervisor | None:
        """Return the live supervisor for a single-instance child or None."""
        return self.streams.get(key)

    def set(self, key: StreamChildKey, supervisor: ProcessSupervisor | None) -> None:
        """Install (or with None remove) a single-instance child."""
        if key is StreamChildKey.AFTERMARKET:
            raise ValueError("aftermarket shards live in the aftermarket mapping")
        if key is StreamChildKey.REGULAR_KIS:
            raise ValueError("regular_kis shards live in the regular_kis mapping")
        if supervisor is None:
            self.streams.pop(key, None)
        else:
            self.streams[key] = supervisor

    @property
    def regular(self) -> ProcessSupervisor | None:
        return self.get(StreamChildKey.REGULAR)

    @regular.setter
    def regular(self, supervisor: ProcessSupervisor | None) -> None:
        self.set(StreamChildKey.REGULAR, supervisor)

    @property
    def snapshot(self) -> ProcessSupervisor | None:
        return self.get(StreamChildKey.SNAPSHOT)

    @snapshot.setter
    def snapshot(self, supervisor: ProcessSupervisor | None) -> None:
        self.set(StreamChildKey.SNAPSHOT, supervisor)

    @property
    def db_crosscheck(self) -> ProcessSupervisor | None:
        return self.get(StreamChildKey.DB_CROSSCHECK)

    @db_crosscheck.setter
    def db_crosscheck(self, supervisor: ProcessSupervisor | None) -> None:
        self.set(StreamChildKey.DB_CROSSCHECK, supervisor)

    @property
    def premarket(self) -> ProcessSupervisor | None:
        return self.get(StreamChildKey.PREMARKET)

    @premarket.setter
    def premarket(self, supervisor: ProcessSupervisor | None) -> None:
        self.set(StreamChildKey.PREMARKET, supervisor)

    @property
    def regular_is_degraded(self) -> bool:
        """True iff the current regular supervisor is the one installed in degraded mode.

        Derived by identity, so clearing or replacing `regular` can never leave a stale degraded flag.
        """
        return self.regular is not None and self.degraded_regular is self.regular


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


def _stop_sync_child(proc: Any, *, grace_s: float, already_signalled: bool = False, stage: str = "program_trades_sync") -> None:
    """Terminate a nightly sync child and SIGKILL it when the grace window expires."""
    if not already_signalled:
        proc.terminate()
    try:
        proc.wait(timeout=grace_s)
    except Exception as exc:  # noqa: BLE001 - SIGKILL fallback stays inside the deadline
        logger.warning("[DAEMON] stage=%s status=WAIT_TIMEOUT reason=%s", stage, str(exc))
        try:
            proc.kill()
            proc.wait()
        except Exception as kill_exc:  # noqa: BLE001 - shutdown proceeds regardless
            logger.warning("[DAEMON] stage=%s status=KILL_FAIL reason=%s", stage, str(kill_exc))


class NightlyJob(StrEnum):
    TICK_TAPE = "tick_tape"
    MINUTE_BARS = "minute_bars"


_NIGHTLY_JOB_ORDER: tuple[NightlyJob, ...] = (NightlyJob.TICK_TAPE, NightlyJob.MINUTE_BARS)
_NIGHTLY_BLOCKED_STATES: tuple[SessionState, ...] = (
    SessionState.STREAMER_ACTIVE,
    SessionState.FULL_ACTIVE,
    SessionState.AFTER_MARKET_ACTIVE,
)


def nightly_window_open(now: dt.datetime, *, start: dt.time, end: dt.time) -> bool:
    """True iff now (KST) is inside [start, end), where the window may cross midnight."""
    wall = now.astimezone(_KST).time()
    if start <= end:
        return start <= wall < end
    return wall >= start or wall < end


def _nightly_window_identity(now: dt.datetime, *, start: dt.time, end: dt.time) -> dt.date:
    """KST date of the window start containing now; a 23:30-05:30 window belongs to the 23:30 date."""
    today = now.astimezone(_KST).date()
    if start <= end:
        return today
    return today if now.astimezone(_KST).time() >= start else today - dt.timedelta(days=1)


def _nightly_window_end(identity: dt.date, *, start: dt.time, end: dt.time) -> dt.datetime:
    """KST instant the window starting on identity ends."""
    if start <= end:
        return dt.datetime.combine(identity, end, tzinfo=_KST)
    return dt.datetime.combine(identity + dt.timedelta(days=1), end, tzinfo=_KST)


def _nightly_job_cmd(job: NightlyJob, *, deadline: dt.datetime) -> list[str]:
    """Argv for the one-shot backfill child (backfill-tick-tape / backfill-minute-bars) with --deadline."""
    module = "backfill-tick-tape" if job is NightlyJob.TICK_TAPE else "backfill-minute-bars"
    return [
        sys.executable,
        "-m",
        "src.cli.main",
        module,
        "--deadline",
        deadline.isoformat(),
    ]


def _minute_backfill_settings(explicit: Any | None) -> Any | None:
    """Minute-bars settings override, else the Part 4 class when it exists (None until then)."""
    if explicit is not None:
        return explicit
    cls = getattr(importlib.import_module("src.core.config"), "MinuteBackfillSettings", None)
    return cls() if cls is not None else None


def _child_alive(proc: Any) -> bool:
    """True while a raw Popen or a ProcessSupervisor child is still running."""
    poll = getattr(proc, "poll", None)
    if callable(poll):
        return proc.poll() is None
    running = getattr(proc, "is_running", None)
    if callable(running):
        return bool(proc.is_running())
    return proc is not None


@dataclass
class CrosscheckWatch:
    """Per-child liveness latch for one CROSSCHECK stream (runner-private, not part of DaemonState)."""

    last_ingest_check: dt.datetime | None = None
    stale_for: dt.date | None = None
    last_first_frame_check: dt.datetime | None = None
    first_frame_alerted_for: dt.date | None = None


def _crosscheck_ingest_tick(
    watch: CrosscheckWatch,
    *,
    child: StreamChildKey,
    vendor: str,
    journal_root: pathlib.Path,
    today: dt.date,
    now: dt.datetime,
) -> None:
    """Rate-limited staleness check of one crosscheck journal; logs at most WARNING."""
    last = watch.last_ingest_check
    if last is not None and (now - last).total_seconds() < INGEST_CHECK_S:
        return
    watch.last_ingest_check = now
    age = _journal_age_s(journal_root, vendor, today, now)
    stale = age is None or age > INGEST_STALE_S
    if stale and watch.stale_for != today:
        logger.warning(
            "[DATA] stage=crosscheck_watchdog status=STALE child=%s vendor=%s date=%s age_s=%s",
            child.value,
            vendor,
            today.isoformat(),
            "none" if age is None else str(int(age)),
        )
        watch.stale_for = today
    elif not stale and watch.stale_for == today:
        assert age is not None
        logger.info(
            "[DATA] stage=crosscheck_watchdog status=RECOVERED child=%s vendor=%s date=%s age_s=%d",
            child.value,
            vendor,
            today.isoformat(),
            int(age),
        )
        watch.stale_for = None


def _crosscheck_first_frame_tick(
    watch: CrosscheckWatch,
    *,
    child: StreamChildKey,
    vendor: str,
    journal_root: pathlib.Path,
    today: dt.date,
    now: dt.datetime,
    due: dt.time,
) -> None:
    """Rate-limited pre-open first-frame check of one crosscheck journal; logs at most WARNING."""
    last = watch.last_first_frame_check
    if last is not None and (now - last).total_seconds() < INGEST_CHECK_S:
        return
    watch.last_first_frame_check = now
    if not _journal_has_frames(journal_root, vendor, today):
        if watch.first_frame_alerted_for != today:
            logger.warning(
                "[DATA] stage=crosscheck_watchdog status=NO_FIRST_FRAME child=%s vendor=%s date=%s due=%s",
                child.value,
                vendor,
                today.isoformat(),
                due.isoformat(),
            )
            watch.first_frame_alerted_for = today
        return
    if watch.first_frame_alerted_for == today:
        logger.info(
            "[DATA] stage=crosscheck_watchdog status=FIRST_FRAME_RECOVERED child=%s vendor=%s date=%s",
            child.value,
            vendor,
            today.isoformat(),
        )
        watch.first_frame_alerted_for = None


def _log_crosscheck_reconciliation_failure(child: str, exc: OSError) -> None:
    """Emit the WARNING for one crosscheck reconciliation OSError outside DaemonRunner."""
    logger.warning(
        "[DATA] stage=crosscheck_reconciliation status=FAIL child=%s error=%s",
        child,
        str(exc),
        exc_info=True,
    )


class DaemonRunner:
    """Own one daemon process's date-scoped state and supervised children.

    The runner keeps state transitions explicit so a retry, holiday, shutdown, and
    EOD cycle cannot accidentally reuse a prior day's process or status.
    ``program_sync_settings`` is resolved once here (None
    reads env) so the nightly sync gate and timeout cannot drift between cycles
    and invalid program-sync env fails at daemon start inside the crash boundary,
    not mid-evening.
    """

    _REPLACE_LIVE_LOG = "[DAEMON] stage=child_ownership status=REPLACE_LIVE slot=%s stop_result=%s"

    _STREAM_SPECS: ClassVar[tuple[StreamChildSpec, ...]] = (
        StreamChildSpec(
            key=StreamChildKey.REGULAR,
            role=StreamRole.PRIMARY,
            lifecycle=ChildLifecycle.SESSION,
            enabled=lambda runtime: True,
            cmd=lambda today, paths, reason: _stream_cmd(today, paths, degraded_reason=reason),
            active=lambda runtime, now, anchors: True,
            logs=SupervisionLogs(
                started="[DAEMON] stage=streamer status=STARTED",
                restarted="[DAEMON] stage=streamer status=RESTARTED exit_code=%s restarts=%d",
                circuit_open="[DAEMON] stage=streamer status=FAIL reason=circuit_open",
                circuit_level=logging.CRITICAL,
            ),
            journal_vendor=lambda cfg: cfg.vendor,
            manifest_path=lambda paths, day: paths.manifest_path(day),
        ),
        StreamChildSpec(
            key=StreamChildKey.SNAPSHOT,
            role=StreamRole.SNAPSHOT,
            lifecycle=ChildLifecycle.SESSION,
            enabled=lambda runtime: runtime.snapshot.enabled,
            cmd=lambda today, paths, reason: _snapshot_cmd(today, paths),
            active=lambda runtime, now, anchors: (
                now.astimezone(_KST).time() < shift_snapshot_settings(runtime.snapshot, anchors).run_end
            ),
            logs=SupervisionLogs(
                restarted="[DAEMON] stage=snapshots status=RESTARTED exit_code=%s",
                circuit_open="[DAEMON] stage=snapshots status=FAIL reason=circuit_open",
                circuit_level=logging.CRITICAL,
            ),
            starts_in_degraded=True,
            silent_on_degraded_upgrade=True,
        ),
        StreamChildSpec(
            key=StreamChildKey.DB_CROSSCHECK,
            role=StreamRole.CROSSCHECK,
            lifecycle=ChildLifecycle.SESSION,
            enabled=lambda runtime: runtime.collector.db_crosscheck_enabled,
            cmd=lambda today, paths, reason: _db_crosscheck_cmd(today, paths),
            active=lambda runtime, now, anchors: now.astimezone(_KST).time() < anchors.regular_close,
            logs=SupervisionLogs(
                restarted="[DAEMON] stage=db_crosscheck status=RESTARTED exit_code=%s",
                circuit_open="[DAEMON] stage=db_crosscheck status=FAIL reason=circuit_open",
                circuit_level=logging.WARNING,
            ),
            requires_primary=True,
            journal_vendor=lambda cfg: L0_VENDOR_DB,
            manifest_path=lambda paths, day: paths.db_crosscheck_manifest_path(day),
        ),
        StreamChildSpec(
            key=StreamChildKey.AFTERMARKET,
            role=StreamRole.AFTERMARKET,
            lifecycle=ChildLifecycle.SHARDED,
            enabled=lambda runtime: runtime.collector.after_market_enabled,
        ),
        StreamChildSpec(
            key=StreamChildKey.PREMARKET,
            role=StreamRole.PREMARKET,
            lifecycle=ChildLifecycle.WINDOWED,
            enabled=lambda runtime: runtime.premarket.enabled,
        ),
        StreamChildSpec(
            key=StreamChildKey.REGULAR_KIS,
            role=StreamRole.REGULAR_KIS,
            lifecycle=ChildLifecycle.SHARDED,
            enabled=lambda runtime: bool(getattr(getattr(runtime, "regular_kis", None), "enabled", False)),
        ),
    )

    @classmethod
    def _stream_spec(cls, key: StreamChildKey) -> StreamChildSpec:
        """Return the table row for key; raises KeyError when absent."""
        for spec in cls._STREAM_SPECS:
            if spec.key is key:
                return spec
        raise KeyError(key)

    def _session_specs(self) -> tuple[StreamChildSpec, ...]:
        """SESSION-lifecycle rows in table order whose enabled(runtime) is True."""
        return tuple(
            spec
            for spec in self._STREAM_SPECS
            if spec.lifecycle is ChildLifecycle.SESSION and spec.enabled(self._runtime)
        )

    def __init__(
        self,
        *,
        runtime: CollectorRuntime,
        shutdown: threading.Event | None,
        now: Callable[[], dt.datetime],
        sleep: Callable[[float], None],
        program_sync_settings: TossProgramTradesSettings | None = None,
        tick_tape_settings: TickTapeBackfillSettings | None = None,
        minute_backfill_settings: Any | None = None,
    ) -> None:
        self._runtime = runtime
        self._shutdown = shutdown
        self._now = now
        self._sleep = sleep
        # Kept off DaemonState: a monotonic reading is not part of the deterministic state snapshot.
        self._last_heartbeat_mono: float | None = None
        self._cfg = runtime.collector
        self._paths = runtime.paths
        self._after_cfg = runtime.aftermarket
        self._snapshot_cfg = runtime.snapshot
        self._premarket_cfg = runtime.premarket
        _tiers = getattr(runtime, "universe_tiers", None)
        self._universe_tiers = _tiers if _tiers is not None else UniverseTierSettings()
        _wd = getattr(runtime, "shard_watchdog", None)
        if _wd is None:
            _wd = ShardWatchdogSettings()
        self._aftermarket_watchdog = ShardWatchdog(
            limit_s=_wd.aftermarket_stale_limit_s, max_restarts_per_day=_wd.max_restarts_per_day
        )
        self._premarket_watchdog = ShardWatchdog(
            limit_s=_wd.premarket_stale_limit_s, max_restarts_per_day=_wd.max_restarts_per_day
        )
        self._regular_kis_watchdog = ShardWatchdog(
            limit_s=_wd.aftermarket_stale_limit_s, max_restarts_per_day=_wd.max_restarts_per_day
        )
        self._program_sync_cfg = (
            program_sync_settings if program_sync_settings is not None else TossProgramTradesSettings()
        )
        self._tick_tape_cfg = (
            tick_tape_settings if tick_tape_settings is not None else TickTapeBackfillSettings()
        )
        self._minute_cfg = _minute_backfill_settings(minute_backfill_settings)
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
        self._last_first_frame_check: dt.datetime | None = None
        self._first_frame_alerted_for: dt.date | None = None
        self._crosscheck_watch: dict[StreamChildKey, CrosscheckWatch] = {}
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
        ch = self._children
        regular = ch.regular
        if regular is not None:
            targets.append(regular)
        targets.extend(ch.aftermarket.values())
        targets.extend(ch.regular_kis.values())
        for spec in self._STREAM_SPECS:
            if spec.role is StreamRole.PRIMARY or spec.lifecycle is ChildLifecycle.SHARDED:
                continue
            supervisor = ch.get(spec.key)
            if supervisor is not None:
                targets.append(supervisor)
        sync = self._children.program_sync
        sync_running = sync is not None and sync.poll() is None
        nightly = self._children.nightly
        nightly_running = nightly is not None and _child_alive(nightly)
        nightly_stage = (
            f"nightly_{self._children.nightly_job.value}"
            if self._children.nightly_job is not None
            else "nightly_unknown"
        )
        # 모든 자식에 먼저 SIGTERM 을 보내 대기를 겹쳐야 compose 30s 유예 안에 끝난다 (순차 대기 시 최대 40s).
        started = time.monotonic()
        if sync_running:
            assert sync is not None
            sync.terminate()
        if nightly_running:
            assert nightly is not None
            nightly.terminate()
        counts = stop_supervisors(targets, deadline_s=SHUTDOWN_CHILD_DEADLINE_S)
        if sync_running:
            assert sync is not None
            remaining = max(1.0, SHUTDOWN_CHILD_DEADLINE_S - (time.monotonic() - started))
            _stop_sync_child(sync, grace_s=remaining, already_signalled=True)
            self._children.program_sync = None
            self._children.program_sync_started_at = None
        if nightly_running:
            assert nightly is not None
            remaining = max(1.0, SHUTDOWN_CHILD_DEADLINE_S - (time.monotonic() - started))
            _stop_sync_child(nightly, grace_s=remaining, already_signalled=True, stage=nightly_stage)
        self._children.nightly = None
        self._children.nightly_job = None
        self._children.nightly_started_at = None
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
                timeout_s = self._program_sync_cfg.sync_timeout_s
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
        if ch.nightly is not None and ch.nightly.poll() is None:
            return
        if ch.program_sync is not None and ch.program_sync.poll() is None:
            return
        if st.eod_attempted_for is None or st.program_sync_day == st.eod_attempted_for:
            return
        today = st.eod_attempted_for
        view = self._gate.view(today, now)
        if view.status is not TradingDayStatus.BUSINESS or view.trading_day is None:
            return
        sync_settings = self._program_sync_cfg
        if not sync_settings.auto_backfill_enabled:
            return
        cmd = _program_sync_cmd(today, view.trading_day.previous_business_day)
        try:
            ch.program_sync = subprocess.Popen(cmd, env=child_process_env({}))  # noqa: S603 - fixed argv, no shell
        except OSError as exc:
            logger.critical("[DAEMON] stage=program_trades_sync status=FAIL exit_code=launch reason=%s", str(exc))
            ch.program_sync = None
            return
        # `now` is the cycle-start instant; EOD maintenance may have blocked this cycle for tens of minutes.
        ch.program_sync_started_at = self._now()
        st.program_sync_day = today
        logger.info("[DAEMON] stage=program_trades_sync status=STARTED date=%s", today.isoformat())

    def _nightly_job_enabled(self, job: NightlyJob) -> bool:
        """Per-job enable gate; minute bars stay inert until the Part 4 settings land."""
        if job is NightlyJob.TICK_TAPE:
            return bool(self._tick_tape_cfg.enabled)
        return self._minute_cfg is not None and bool(self._minute_cfg.enabled)

    def _nightly_session_live(self) -> bool:
        """True while any session state or supervised session child is active."""
        if self._state.prev_state in _NIGHTLY_BLOCKED_STATES:
            return True
        ch = self._children
        singles: list[Any | None] = [ch.regular, ch.snapshot, ch.db_crosscheck, ch.premarket]
        if any(sup is not None and _child_alive(sup) for sup in singles):
            return True
        return any(any(_child_alive(sup) for sup in mapping.values()) for mapping in (ch.aftermarket, ch.regular_kis))

    def _stop_nightly_child(self, *, reason: str) -> None:
        """Terminate the nightly backfill child when present and drop its handle."""
        ch = self._children
        proc = ch.nightly
        if proc is None:
            return
        job = ch.nightly_job
        stage = f"nightly_{job.value}" if job is not None else "nightly_unknown"
        if proc.poll() is None:
            _stop_sync_child(proc, grace_s=CHILD_STOP_TIMEOUT_S, stage=stage)
        ch.nightly = None
        ch.nightly_job = None
        ch.nightly_started_at = None
        logger.info("[DAEMON] stage=%s status=STOPPED reason=%s", stage, reason)

    def _poll_nightly(self, now: dt.datetime) -> None:
        """Reap the nightly backfill child without blocking; enforce window end + 120 s grace."""
        ch = self._children
        proc = ch.nightly
        if proc is None:
            return
        job = ch.nightly_job
        stage = f"nightly_{job.value}" if job is not None else "nightly_unknown"
        code = proc.poll()
        if code is None:
            started = ch.nightly_started_at
            anchor = started if started is not None else now
            window = (self._tick_tape_cfg.window_start_kst, self._tick_tape_cfg.window_end_kst)
            identity = _nightly_window_identity(anchor, start=window[0], end=window[1])
            window_end = _nightly_window_end(identity, start=window[0], end=window[1])
            if (now - window_end).total_seconds() > NIGHTLY_OVERRUN_GRACE_S:
                _stop_sync_child(proc, grace_s=CHILD_STOP_TIMEOUT_S, stage=stage)
                ch.nightly = None
                ch.nightly_job = None
                ch.nightly_started_at = None
                logger.warning("[DAEMON] stage=%s status=FAIL reason=deadline", stage)
            return
        ch.nightly = None
        ch.nightly_job = None
        ch.nightly_started_at = None
        if code == 0:
            logger.info("[DAEMON] stage=%s status=OK", stage)
        elif code == 2:
            logger.info("[DAEMON] stage=%s status=NOOP", stage)
        else:
            logger.warning("[DAEMON] stage=%s status=FAIL exit_code=%s", stage, code)

    def _eod_done_for(self, day: dt.date) -> bool:
        """True when this process attempted EOD for `day` or an earlier process archived its digest (EOD state is in-memory, a restart must not skip the night)."""
        return self._state.eod_attempted_for == day or self._paths.eod_digest_path(day).exists()

    def _maybe_launch_nightly(self, now: dt.datetime) -> None:
        """Launch at most one backfill child at a time, each job at most once per window."""
        st = self._state
        ch = self._children
        if not any(self._nightly_job_enabled(job) for job in _NIGHTLY_JOB_ORDER):
            return
        now = max(now, self._now())
        if ch.nightly is not None:
            return
        sync = ch.program_sync
        if sync is not None and _child_alive(sync):
            return
        if self._nightly_session_live():
            return
        window = (self._tick_tape_cfg.window_start_kst, self._tick_tape_cfg.window_end_kst)
        if not nightly_window_open(now, start=window[0], end=window[1]):
            return
        identity = _nightly_window_identity(now, start=window[0], end=window[1])
        view = self._gate.view(identity, now)
        if view.status is TradingDayStatus.UNKNOWN:
            return
        if view.status is TradingDayStatus.BUSINESS and not self._eod_done_for(identity):
            return
        for job in _NIGHTLY_JOB_ORDER:
            if st.nightly_done.get(job) == identity:
                continue
            if not self._nightly_job_enabled(job):
                continue
            deadline = _nightly_window_end(identity, start=window[0], end=window[1])
            try:
                ch.nightly = subprocess.Popen(  # noqa: S603 - fixed argv, no shell
                    _nightly_job_cmd(job, deadline=deadline), env=child_process_env({})
                )
            except OSError as exc:
                logger.critical(
                    "[DAEMON] stage=nightly_%s status=FAIL exit_code=launch reason=%s",
                    job.value,
                    str(exc),
                )
                st.nightly_done[job] = identity
                return
            ch.nightly_job = job
            ch.nightly_started_at = self._now()
            st.nightly_done[job] = identity
            logger.info(
                "[DAEMON] stage=nightly_%s status=STARTED deadline=%s", job.value, deadline.isoformat()
            )
            return

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
        st.cycle += 1
        anchors = self._resolve_anchors(now)
        state = get_target_state(now, schedule_for(anchors, self._sched))
        day, anchors = self._resolve_active_day(now, state, anchors)
        self._log_cycle(now, state)
        if state in _NIGHTLY_BLOCKED_STATES:
            self._stop_nightly_child(reason="session")
        if state == SessionState.WEEKEND_SLEEP:
            sleep_sec = 3600.0  # 주말엔 1시간씩 대기
        elif state == SessionState.PRE_MARKET_SLEEP:
            sleep_sec = min(calc_sleep_seconds(now, self._sched.streamer_start), 300.0)
        elif state in (SessionState.STREAMER_ACTIVE, SessionState.FULL_ACTIVE, SessionState.AFTER_MARKET_ACTIVE):
            assert day is not None
            sleep_sec = self._step_active_session(now, state, day, anchors)
        elif state == SessionState.POST_MARKET_EOD:
            sleep_sec = self._step_post_market_eod(now, day, anchors)
        else:  # NIGHT_SLEEP
            sleep_sec = min(calc_sleep_seconds(now, self._sched.streamer_start), 1800.0)
        if self._premarket_cfg.enabled:
            self._supervise_premarket(now, anchors)
            self._maybe_refresh_premarket_pool(now, anchors)
            cap = premarket_wake_cap_s(now, anchors=anchors, start_lead_s=self._premarket_cfg.start_lead_s)
            if cap is not None:
                sleep_sec = min(sleep_sec, cap)
        self._poll_program_sync(now)
        self._maybe_launch_program_sync(now)
        self._poll_nightly(now)
        self._maybe_launch_nightly(now)
        if self._children.program_sync is not None or self._children.nightly is not None:
            # A running nightly child must not wait out a long night sleep before its deadline is checked.
            sleep_sec = min(sleep_sec, PROGRAM_SYNC_POLL_S)
        if any(self._nightly_job_enabled(job) for job in _NIGHTLY_JOB_ORDER):
            sleep_sec = min(sleep_sec, PROGRAM_SYNC_POLL_S)
        return sleep_sec

    def _resolve_anchors(self, now: dt.datetime) -> SessionAnchors:
        """Return today's session anchors, reloading them before state resolution when a vendor anchor may exist.

        A restart on a shifted-session day (e.g. CSAT) without the prior-day prefetch would otherwise
        derive the state from DEFAULT anchors and run EOD mid-session or miss a session; on weekdays at
        or after `streamer_start` the trading-day gate is consulted once per day to pin real anchors.
        """
        st = self._state
        today_kst = now.astimezone(_KST).date()
        anchors = self._anchors
        if self._anchors_date != today_kst or anchors is None:
            anchors = resolve_session_anchors(self._paths.session_calendar_dir, today_kst)
            self._anchors = anchors
            self._anchors_date = today_kst
        if (
            anchors.source is AnchorSource.DEFAULT
            and now.astimezone(_KST).weekday() < 5
            and now.astimezone(_KST).time() >= self._sched.streamer_start
            and st.anchors_reloaded_for != today_kst
        ):
            early_day = self._gate.view(today_kst, now)
            if early_day.status is TradingDayStatus.BUSINESS:
                st.anchors_reloaded_for = today_kst
                anchors = resolve_session_anchors(self._paths.session_calendar_dir, today_kst)
                self._anchors = anchors
        return anchors

    def _resolve_active_day(
        self, now: dt.datetime, state: SessionState, anchors: SessionAnchors
    ) -> tuple[TradingDayView | None, SessionAnchors]:
        """Resolve the trading-day view for session-bearing states and reload anchors once per business day.

        Returns `(None, anchors)` for sleep states without consulting the gate. Logs the holiday SKIP
        event at most once per date.
        """
        if state not in (
            SessionState.STREAMER_ACTIVE,
            SessionState.FULL_ACTIVE,
            SessionState.AFTER_MARKET_ACTIVE,
            SessionState.POST_MARKET_EOD,
        ):
            return None, anchors
        st = self._state
        day = self._gate.view(now.astimezone(_KST).date(), now)
        if day.status is TradingDayStatus.BUSINESS and st.anchors_reloaded_for != day.date:
            st.anchors_reloaded_for = day.date
            anchors = resolve_session_anchors(self._paths.session_calendar_dir, day.date)
            self._anchors = anchors
            self._anchors_date = day.date
        if day.status is TradingDayStatus.HOLIDAY and st.holiday_skip_logged_for != day.date:
            logger.info(
                "[DAEMON] stage=session status=SKIP reason=market_holiday date=%s",
                day.date.isoformat(),
                extra=EVENT,
            )
            st.holiday_skip_logged_for = day.date
        return day, anchors

    def _log_cycle(self, now: dt.datetime, state: SessionState) -> None:
        st = self._state
        logger.debug("[DAEMON] cycle=%d now=%s state=%s", st.cycle, now.strftime("%Y-%m-%d %H:%M:%S"), state)
        if state != st.prev_state:
            logger.info(
                "[DAEMON] stage=state_change from=%s to=%s cycle=%d", st.prev_state, state, st.cycle, extra=EVENT
            )
            st.prev_state = state
        if st.last_summary is None or (now - st.last_summary).total_seconds() >= HEARTBEAT_SUMMARY_S:
            self._log_heartbeat(state)
            st.last_summary = now

    def _log_heartbeat(self, state: SessionState, mono: float | None = None) -> None:
        st = self._state
        streamer_alive = st.last_supervisor_result in ("started", "running", "restarted")
        logger.info(
            "[DAEMON] stage=heartbeat state=%s cycle=%d streamer_alive=%s streamer_restarts=%d",
            state,
            st.cycle,
            streamer_alive,
            st.streamer_restarts,
        )
        self._last_heartbeat_mono = mono if mono is not None else time.monotonic()

    def _eod_progress(self) -> None:
        """Liveness callback for EOD housekeeping, which blocks the daemon loop for tens of minutes.

        Pings the external dead-man's switch and keeps the ten-minute heartbeat going so observers can tell
        a busy EOD from a hung daemon. Uses the monotonic clock only: it must not consume the injected wall clock.
        """
        mono = time.monotonic()
        self._maybe_ping(mono)
        last = self._last_heartbeat_mono
        if last is None or mono - last >= HEARTBEAT_SUMMARY_S:
            self._log_heartbeat(SessionState.POST_MARKET_EOD, mono)

    def _step_active_session(
        self, now: dt.datetime, state: SessionState, day: TradingDayView, anchors: SessionAnchors
    ) -> float:
        """Run one STREAMER/FULL/AFTER_MARKET cycle and return the next delay.

        Holidays stop every session child and sleep to the next state boundary (capped by
        `HOLIDAY_SLEEP_CAP_S`); business/unknown days run rollover, orchestration, supervision,
        watchdog and aftermarket phases in that fixed order and return 10.0 s.
        """
        if day.status is TradingDayStatus.HOLIDAY:
            self._stop_holiday_children()
            return self._holiday_sleep_s(now, state)
        today = day.date
        st = self._state
        if state is not SessionState.AFTER_MARKET_ACTIVE and today != st.orchestration_day:
            self._reset_for_new_session_day(today)
        self._maybe_orchestrate(now, state, day)
        self._supervise_session_children(now, anchors)
        self._supervise_regular_kis(now, anchors)
        self._run_ingest_watchdog(now, state, day, anchors)
        self._run_first_frame_watchdog(now, state, day, anchors)
        self._run_crosscheck_watchdogs(now, state, day, anchors)
        self._maybe_reselect_aftermarket(now, state, today, anchors)
        if state in (SessionState.FULL_ACTIVE, SessionState.AFTER_MARKET_ACTIVE) and self._cfg.after_market_enabled:
            self._supervise_aftermarket(now, state, today, anchors)
        return 10.0

    def _stop_holiday_children(self) -> None:
        self._stop_session_children()
        self._stop_aftermarket_children(clear_circuit_alerts=False)
        self._stop_regular_kis_children(clear_circuit_alerts=False)
        self._stop_nightly_child(reason="holiday")

    def _holiday_sleep_s(self, now: dt.datetime, state: SessionState) -> float:
        if state is SessionState.STREAMER_ACTIVE:
            _holiday_target = self._sched.scanner_start
        elif state is SessionState.FULL_ACTIVE:
            _holiday_target = self._sched.market_close
        else:
            _holiday_target = self._sched.after_market_close
        return min(calc_sleep_seconds(now, _holiday_target), HOLIDAY_SLEEP_CAP_S)

    def _reset_for_new_session_day(self, today: dt.date) -> None:
        """Drop the previous day's regular/snapshot children and per-day orchestration state.

        A streamer that missed EOD would otherwise keep running with the previous session date.
        Aftermarket children and plan state are intentionally untouched (EOD owns them).
        """
        st = self._state
        ch = self._children
        if ch.regular is not None:
            stale_stop = self._stop_stream_child(StreamChildKey.REGULAR)
            logger.warning("[DAEMON] stage=streamer status=STOP_STALE_DAY stop_result=%s", stale_stop)
        self._stop_session_children()
        self._stop_regular_kis_children(clear_circuit_alerts=True)
        self._stop_nightly_child(reason="new_session_day")
        st.orchestration_day = today
        st.next_orchestration_at = None
        st.orchestration_attempts = 0
        self._sync_degraded_mirror()
        st.last_ingest_check = None
        st.ingest_stale = False
        st.next_aftermarket_refresh_at = None
        st.next_aftermarket_nxt_refresh_at = None
        st.regular_kis_plan = ()
        st.regular_kis_plan_day = None
        st.regular_kis_circuit_alerted.clear()
        st.aftermarket_deferred_slots.clear()

    def _maybe_orchestrate(self, now: dt.datetime, state: SessionState, day: TradingDayView) -> None:
        """Run session orchestration at most once per day, retrying on backoff and falling back to a degraded streamer.

        The KIS token preflight runs once per date before the first attempt and is outside the
        failure-isolation boundary; only `run_session_orchestration` is isolated so an orchestration
        crash never kills the daemon.
        """
        st = self._state
        cfg = self._cfg
        paths = self._paths
        snapshot_cfg = self._snapshot_cfg
        today = day.date
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
                    tiers=self._universe_tiers,
                )
            except Exception as e:  # noqa: BLE001 - isolate orchestration crashes so they never kill the daemon
                logger.critical(
                    "[DAEMON] stage=session status=FAIL reason=orchestration_error error=%s",
                    str(e),
                    exc_info=True,
                )
                ready = False
            if ready:
                self._start_ready_children(today)
            else:
                self._handle_orchestration_failure(now, today)

    def _sync_degraded_mirror(self) -> None:
        self._state.degraded_active = self._children.regular_is_degraded

    def _replace_supervisor(
        self, predecessor: ProcessSupervisor | None, cmd: list[str]
    ) -> tuple[ProcessSupervisor, str | None]:
        """Stop predecessor then build the successor; returns (successor, stop_result or None)."""
        stop_result: str | None = None
        if predecessor is not None:
            stop_result = predecessor.stop(timeout_s=CHILD_STOP_TIMEOUT_S)
        successor = ProcessSupervisor(cmd=cmd, breaker=RestartCircuitBreaker())
        return successor, stop_result

    def _install_stream_child(
        self, key: StreamChildKey, cmd: list[str], *, degraded: bool = False, replaces_degraded: bool = False
    ) -> None:
        """Install a SESSION child supervisor with ownership logging."""
        spec = self._stream_spec(key)
        if spec.lifecycle is not ChildLifecycle.SESSION:
            raise ValueError(f"cannot install a non-session child: {key.value}")
        if degraded and spec.role is not StreamRole.PRIMARY:
            raise ValueError(f"only the PRIMARY child may be installed degraded: {key.value}")
        ch = self._children
        is_primary = spec.role is StreamRole.PRIMARY
        was_degraded = ch.regular_is_degraded if is_primary else False
        predecessor = ch.get(key)
        successor, stop_result = self._replace_supervisor(predecessor, cmd)
        if predecessor is not None:
            if is_primary and was_degraded:
                logger.info(
                    "[DAEMON] stage=streamer status=REPLACE_DEGRADED stop_result=%s",
                    stop_result,
                    extra=EVENT,
                )
            elif not is_primary and replaces_degraded and spec.silent_on_degraded_upgrade:
                pass
            else:
                logger.critical(self._REPLACE_LIVE_LOG, key.value, stop_result)
        ch.set(key, successor)
        if is_primary:
            ch.degraded_regular = successor if degraded else None
        self._state.set_child_result(key, None)
        if is_primary:
            self._sync_degraded_mirror()

    def _install_aftermarket(self, key: str, cmd: list[str]) -> None:
        """Install the aftermarket supervisor for `key`, stopping and CRITICAL-logging any predecessor."""
        ch = self._children
        predecessor = ch.aftermarket.get(key)
        successor, stop_result = self._replace_supervisor(predecessor, cmd)
        if predecessor is not None:
            logger.critical(
                self._REPLACE_LIVE_LOG,
                f"aftermarket:{key}",
                stop_result,
            )
        ch.aftermarket[key] = successor

    def _start_ready_children(self, today: dt.date) -> None:
        st = self._state
        st.orchestrated_for = today
        st.next_orchestration_at = None
        replacing_degraded = self._children.regular_is_degraded
        if replacing_degraded:
            st.regular_kis_plan_day = None
        for spec in self._session_specs():
            assert spec.cmd is not None
            self._install_stream_child(
                spec.key, spec.cmd(today, self._paths, None), replaces_degraded=replacing_degraded
            )

    def _handle_orchestration_failure(self, now: dt.datetime, today: dt.date) -> None:
        """Schedule the next orchestration retry and start a degraded streamer from recent candidates.

        The first failed attempt of a day is CRITICAL, later retries WARNING. A degraded streamer is
        started only when no regular child exists and candidates are within
        `degraded_candidates_max_age_days`.
        """
        st = self._state
        ch = self._children
        cfg = self._cfg
        paths = self._paths
        st.next_orchestration_at = now + dt.timedelta(seconds=cfg.orchestration_retry_s)
        log_fn = logger.critical if st.orchestration_attempts == 1 else logger.warning
        log_fn(
            "[DAEMON] stage=session status=FAIL reason=candidates_not_ready date=%s attempt=%d next_retry=%s",
            today.isoformat(),
            st.orchestration_attempts,
            st.next_orchestration_at.isoformat(),
        )
        if ch.regular is None:
            rev = _degraded_candidates_rev(paths.candidates, today, max_age_days=cfg.degraded_candidates_max_age_days)
            if rev is not None:
                primary_spec = self._stream_spec(StreamChildKey.REGULAR)
                assert primary_spec.cmd is not None
                self._install_stream_child(
                    StreamChildKey.REGULAR,
                    primary_spec.cmd(today, paths, "orchestration_failed"),
                    degraded=True,
                )
                for spec in self._session_specs():
                    if not spec.starts_in_degraded or ch.get(spec.key) is not None:
                        continue
                    assert spec.cmd is not None
                    self._install_stream_child(spec.key, spec.cmd(today, paths, None))
                logger.critical(
                    "[DAEMON] stage=streamer status=DEGRADED reason=orchestration_failed candidates_rev=%d",
                    rev,
                )

    def _supervise_stream_child(self, key: StreamChildKey, now: dt.datetime, anchors: SessionAnchors) -> None:
        """Run one ensure_running for a SESSION child inside its active window and log the outcome."""
        spec = self._stream_spec(key)
        assert spec.lifecycle is ChildLifecycle.SESSION
        assert spec.active is not None
        assert spec.logs is not None
        ch = self._children
        sup = ch.get(key)
        if sup is None:
            return
        if spec.requires_primary and ch.regular is None:
            return
        if not spec.active(self._runtime, now, anchors):
            return
        result = sup.ensure_running()
        logs = spec.logs
        if result == "started" and logs.started is not None:
            logger.info(logs.started, extra=EVENT)
        elif result == "restarted":
            if spec.role is StreamRole.PRIMARY:
                self._state.streamer_restarts += 1
                logger.warning(logs.restarted, sup.last_exit_code, self._state.streamer_restarts)
            else:
                logger.warning(logs.restarted, sup.last_exit_code)
        elif result == "circuit_open" and sup.take_circuit_alert():
            logger.log(logs.circuit_level, logs.circuit_open)
        self._state.set_child_result(key, result)

    def _supervise_session_children(self, now: dt.datetime, anchors: SessionAnchors) -> None:
        """Supervise every enabled SESSION child in table order."""
        for spec in self._session_specs():
            self._supervise_stream_child(spec.key, now, anchors)

    def _stop_stream_child(self, key: StreamChildKey) -> str | None:
        """Stop and drop a single-instance child; returns the stop result or None when absent."""
        if key is StreamChildKey.AFTERMARKET:
            raise ValueError("aftermarket shards are stopped via _stop_aftermarket_children")
        if key is StreamChildKey.REGULAR_KIS:
            raise ValueError("regular_kis shards are stopped via _stop_regular_kis_children")
        ch = self._children
        sup = ch.get(key)
        if sup is None:
            return None
        result = sup.stop(timeout_s=CHILD_STOP_TIMEOUT_S)
        ch.set(key, None)
        if key in DaemonState._CHILD_RESULT_FIELDS:
            self._state.set_child_result(key, None)
        if key is StreamChildKey.REGULAR:
            ch.degraded_regular = None
            self._sync_degraded_mirror()
        return result

    def _stop_session_children(self) -> None:
        """Stop every SESSION child in table order (absent children are no-ops)."""
        for spec in self._STREAM_SPECS:
            if spec.lifecycle is ChildLifecycle.SESSION:
                self._stop_stream_child(spec.key)

    def _run_ingest_watchdog(
        self, now: dt.datetime, state: SessionState, day: TradingDayView, anchors: SessionAnchors
    ) -> None:
        """Alert on a stale regular journal inside the shifted continuous-session window.

        Missing journals on an UNKNOWN calendar day are treated as a possible holiday (one WARNING per
        date) and never latch `ingest_stale`, so a later real stall still raises CRITICAL.
        """
        st = self._state
        cfg = self._cfg
        paths = self._paths
        journal_vendor = self._stream_spec(StreamChildKey.REGULAR).journal_vendor
        assert journal_vendor is not None
        vendor = journal_vendor(cfg)
        today = day.date
        watch_start = _shifted_time(anchors.regular_open, INGEST_WATCH_START_OFFSET)
        watch_end = _shifted_time(anchors.closing_auction_start, INGEST_WATCH_END_OFFSET)
        if (
            state == SessionState.FULL_ACTIVE
            and watch_start <= now.astimezone(_KST).time() < watch_end
            and (st.last_ingest_check is None or (now - st.last_ingest_check).total_seconds() >= INGEST_CHECK_S)
        ):
            st.last_ingest_check = now
            age = _journal_age_s(paths.journal_root, vendor, today, now)
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
            st.ingest_stale = stale and not (age is None and day.status is TradingDayStatus.UNKNOWN)

    def _run_first_frame_watchdog(
        self, now: dt.datetime, state: SessionState, day: TradingDayView, anchors: SessionAnchors
    ) -> None:
        """Alert once per day when no regular-stream frame exists after the first pre-open print is due.

        The first KRX print precedes ``regular_open`` by ``FIRST_FRAME_LEAD`` (08:30:00 on every
        observed business day); the check is due ``FIRST_FRAME_GRACE`` later and ends where
        ``_run_ingest_watchdog`` takes over (``regular_open + INGEST_WATCH_START_OFFSET``). Anchors are
        shift-aware, so shifted-open days move the window with the open.
        """
        if state not in (SessionState.STREAMER_ACTIVE, SessionState.FULL_ACTIVE):
            return
        if day.status is not TradingDayStatus.BUSINESS:
            return
        if self._children.regular is None:
            return
        today = day.date
        due = _shifted_time(anchors.regular_open, -FIRST_FRAME_LEAD + FIRST_FRAME_GRACE)
        handover = _shifted_time(anchors.regular_open, INGEST_WATCH_START_OFFSET)
        now_time = now.astimezone(_KST).time()
        if not (due <= now_time < handover):
            return
        if (
            self._last_first_frame_check is not None
            and (now - self._last_first_frame_check).total_seconds() < INGEST_CHECK_S
        ):
            return
        self._last_first_frame_check = now
        cfg = self._cfg
        paths = self._paths
        journal_vendor = self._stream_spec(StreamChildKey.REGULAR).journal_vendor
        assert journal_vendor is not None
        vendor = journal_vendor(cfg)
        if _journal_has_frames(paths.journal_root, vendor, today):
            if self._first_frame_alerted_for == today:
                logger.warning(
                    "[DAEMON] stage=ingest_watchdog status=FIRST_FRAME_RECOVERED date=%s",
                    today.isoformat(),
                )
                self._first_frame_alerted_for = None
            return
        if self._first_frame_alerted_for != today:
            logger.critical(
                "[DAEMON] stage=ingest_watchdog status=NO_FIRST_FRAME date=%s due=%s",
                today.isoformat(),
                due.isoformat(),
            )
            self._first_frame_alerted_for = today

    def _run_crosscheck_watchdogs(
        self, now: dt.datetime, state: SessionState, day: TradingDayView, anchors: SessionAnchors
    ) -> None:
        """Run ingest and first-frame checks for every live CROSSCHECK child; never touches primary state."""
        if day.status is not TradingDayStatus.BUSINESS:
            return
        now_time = now.astimezone(_KST).time()
        ingest_start = _shifted_time(anchors.regular_open, INGEST_WATCH_START_OFFSET)
        ingest_end = _shifted_time(anchors.closing_auction_start, INGEST_WATCH_END_OFFSET)
        ingest_due = state == SessionState.FULL_ACTIVE and ingest_start <= now_time < ingest_end
        due = _shifted_time(anchors.regular_open, -FIRST_FRAME_LEAD + FIRST_FRAME_GRACE)
        handover = _shifted_time(anchors.regular_open, INGEST_WATCH_START_OFFSET)
        frame_due = state in (SessionState.STREAMER_ACTIVE, SessionState.FULL_ACTIVE) and due <= now_time < handover
        if not (ingest_due or frame_due):
            return
        cfg = self._cfg
        paths = self._paths
        today = day.date
        for spec in self._session_specs():
            if spec.role is not StreamRole.CROSSCHECK:
                continue
            if self._children.get(spec.key) is None:
                continue
            assert spec.journal_vendor is not None
            watch = self._crosscheck_watch.setdefault(spec.key, CrosscheckWatch())
            vendor = spec.journal_vendor(cfg)
            if ingest_due:
                _crosscheck_ingest_tick(
                    watch,
                    child=spec.key,
                    vendor=vendor,
                    journal_root=paths.journal_root,
                    today=today,
                    now=now,
                )
            if frame_due:
                _crosscheck_first_frame_tick(
                    watch,
                    child=spec.key,
                    vendor=vendor,
                    journal_root=paths.journal_root,
                    today=today,
                    now=now,
                    due=due,
                )

    def _maybe_reselect_aftermarket(
        self, now: dt.datetime, state: SessionState, today: dt.date, anchors: SessionAnchors
    ) -> None:
        """Refresh each venue's isolated aftermarket snapshot once per day after the shifted selection time.

        Vendor or candidate-file failures schedule a per-venue 60 s retry with a CRITICAL alert instead of
        reusing the regular-session universe; one venue's failure never blocks or re-triggers the other.
        """
        st = self._state
        cfg = self._cfg
        paths = self._paths
        after_cfg = self._after_cfg
        if not (
            state == SessionState.FULL_ACTIVE
            and cfg.after_market_enabled
            and now.astimezone(_KST).time() >= anchors.shift_post_close(after_cfg.selection_time)
        ):
            return
        due_venues = tuple(
            venue
            for venue, refresh_day, retry_at in (
                (MarketVenue.KRX, st.aftermarket_refresh_day, st.next_aftermarket_refresh_at),
                (MarketVenue.NXT, st.aftermarket_nxt_refresh_day, st.next_aftermarket_nxt_refresh_at),
            )
            if refresh_day != today and (retry_at is None or now >= retry_at)
        )
        if not due_venues:
            return
        excluded_symbols = self._aftermarket_excluded_symbols(today)
        for venue in due_venues:
            try:
                snapshot = refresh_aftermarket_candidates(
                    session_date=today,
                    generated_at=now,
                    client=_build_kis_client(paths),
                    out_path=paths.aftermarket_candidates(today, venue),
                    capacity=after_cfg.max_symbols,
                    venue=venue,
                    excluded_symbols=excluded_symbols,
                )
            except (MissingCredentialsError, KisApiError, AftermarketUniverseError, CandidateFileError) as exc:
                if venue is MarketVenue.NXT:
                    st.next_aftermarket_nxt_refresh_at = now + dt.timedelta(seconds=60.0)
                    retry_at = st.next_aftermarket_nxt_refresh_at
                else:
                    st.next_aftermarket_refresh_at = now + dt.timedelta(seconds=60.0)
                    retry_at = st.next_aftermarket_refresh_at
                logger.critical(
                    "[DAEMON] stage=aftermarket_reselection status=FAIL venue=%s reason=%s next_retry=%s",
                    venue.value,
                    str(exc),
                    retry_at.isoformat(),
                )
                continue
            if venue is MarketVenue.NXT:
                st.aftermarket_nxt_refresh_day = today
                st.next_aftermarket_nxt_refresh_at = None
            else:
                st.aftermarket_refresh_day = today
                st.next_aftermarket_refresh_at = None
            logger.info(
                "[DATA] stage=aftermarket_reselection status=OK venue=%s symbols=%d",
                venue.value,
                snapshot.selected_count,
            )

    def _aftermarket_excluded_symbols(self, today: dt.date) -> frozenset[str]:
        """Return symbols ineligible for aftermarket selection from the latest daily-bar window.

        Missing or unreadable bar stores degrade to no exclusions with one WARNING per date rather
        than blocking reselection.
        """
        st = self._state
        cfg = self._cfg
        paths = self._paths
        excluded_symbols: frozenset[str]
        if any(paths.bars_daily_dir.glob("*.parquet")):
            try:
                latest_bars_date = latest_partition_date(paths.bars_daily_dir)
                if latest_bars_date is None:
                    raise PartitionedStoreError(f"no month partitions in {paths.bars_daily_dir}")
                eligibility_bars = (
                    scan_month_partitions(
                        paths.bars_daily_dir,
                        min_date=latest_bars_date - dt.timedelta(days=cfg.selection_lookback_calendar_days),
                    )
                    .select(["date", "symbol", "stock_cert_kind", "section"])
                    .collect()
                )
            except (OSError, pl.exceptions.PolarsError, PartitionedStoreError):
                excluded_symbols = frozenset()
                if st.aftermarket_eligibility_day != today:
                    st.aftermarket_eligibility_day = today
                    logger.warning("[ALGO] stage=aftermarket_eligibility status=DEGRADED reason=bars_unreadable")
            else:
                excluded_symbols = ineligible_security_symbols(eligibility_bars)
        else:
            excluded_symbols = frozenset()
            if st.aftermarket_eligibility_day != today:
                st.aftermarket_eligibility_day = today
                logger.warning("[ALGO] stage=aftermarket_eligibility status=DEGRADED reason=no_bars_store")
        return excluded_symbols

    def _ensure_aftermarket_plan(self, today: dt.date) -> None:
        st = self._state
        paths = self._paths
        after_cfg = self._after_cfg
        if st.aftermarket_plan_day != today:
            venue_symbols: dict[MarketVenue, tuple[str, ...]] = {}
            for venue in (MarketVenue.KRX, MarketVenue.NXT):
                try:
                    snapshot = read_candidate_snapshot(
                        paths.aftermarket_candidates(today, venue),
                        expected_session_date=today,
                        expected_session=aftermarket_candidate_session(venue),
                        max_candidates=after_cfg.max_symbols,
                    )
                except CandidateFileError as exc:
                    logger.critical(
                        "[DAEMON] stage=aftermarket_plan status=EMPTY venue=%s reason=%s",
                        venue.value,
                        str(exc),
                    )
                    venue_symbols[venue] = ()
                else:
                    venue_symbols[venue] = tuple(str(row["symbol"]) for row in snapshot.candidates)
            try:
                st.aftermarket_plan = plan_aftermarket_shards(
                    krx_symbols=venue_symbols[MarketVenue.KRX],
                    nxt_symbols=venue_symbols[MarketVenue.NXT],
                    credentials=load_kis_data_credentials(),
                    pair_capacity_per_connection=int(after_cfg.pair_capacity_per_connection or 0),
                    krx_streams=after_cfg.krx_streams,
                    nxt_streams=after_cfg.nxt_streams,
                )
            except (CandidateFileError, KrxAlphaError) as exc:
                logger.critical("[DAEMON] stage=aftermarket_plan status=FAIL reason=%s", str(exc))
                st.aftermarket_plan = ()
            st.aftermarket_plan_day = today

    def _watch_aftermarket_shards(self, now: dt.datetime, today: dt.date, anchors: SessionAnchors) -> None:
        """Judge each running shard's journal freshness inside its window and stop a stale one."""
        try:
            self._watch_aftermarket_shards_guarded(now, today, anchors)
        except (OSError, ValueError) as exc:
            logger.warning("[DATA] stage=shard_watchdog status=ERROR reason=%s", type(exc).__name__)

    def _watch_aftermarket_shards_guarded(self, now: dt.datetime, today: dt.date, anchors: SessionAnchors) -> None:
        wd = getattr(self._runtime, "shard_watchdog", None)
        if wd is None or not wd.enabled:
            return
        st = self._state
        ch = self._children
        if not ch.aftermarket or st.aftermarket_plan_day != today:
            return
        plan = {f"{s.venue.value}:{s.shard_index}": s for s in st.aftermarket_plan}
        live = {
            key: sup
            for key, sup in ch.aftermarket.items()
            if key in plan and hasattr(sup, "is_running") and hasattr(sup, "stop_for_restart")
        }
        if not live:
            return
        by_venue: dict[str, list[tuple[str, Any, AftermarketShard]]] = {}
        for key, sup in live.items():
            shard = plan[key]
            by_venue.setdefault(shard.venue.value, []).append((key, sup, shard))
        for venue, items in by_venue.items():
            first = items[0][2]
            session = MarketSession.NXT_AFTER if first.venue is MarketVenue.NXT else MarketSession.KRX_AFTER
            open_dt, end_dt = shard_window(first.venue, session, day=today, anchors=anchors)
            if not open_dt <= now < end_dt:
                continue
            observations = tuple(
                ShardObservation(
                    key=key,
                    venue=venue,
                    running=bool(sup.is_running()),
                    started_at_wall=getattr(sup, "started_at_wall", None),
                    last_journal_wall=shard_last_journal_wall(
                        self._paths.journal_root,
                        vendor="kis",
                        venue=shard.venue.value,
                        session=session.value,
                        streams=shard.streams,
                        day=today,
                        shard_index=shard.shard_index,
                    ),
                )
                for key, sup, shard in items
            )
            decisions = self._aftermarket_watchdog.evaluate(
                observations,
                now_wall=now.timestamp(),
                window_open_wall=open_dt.timestamp(),
                in_window=bool(open_dt <= now < end_dt),
                day=today,
            )
            if self._apply_shard_decisions(
                decisions,
                watchdog=self._aftermarket_watchdog,
                venue=venue,
                today=today,
                supervisors={key: sup for key, sup, _ in items},
                max_restarts=wd.max_restarts_per_day,
            ):
                return

    def _watch_premarket_shard(
        self, now: dt.datetime, today: dt.date, anchors: SessionAnchors, shard: AftermarketShard
    ) -> None:
        """Judge the running premarket shard's journal freshness inside its window."""
        try:
            self._watch_premarket_shard_guarded(now, today, anchors, shard)
        except (OSError, ValueError) as exc:
            logger.warning("[DATA] stage=shard_watchdog status=ERROR reason=%s", type(exc).__name__)

    def _watch_premarket_shard_guarded(
        self, now: dt.datetime, today: dt.date, anchors: SessionAnchors, shard: AftermarketShard
    ) -> None:
        wd = getattr(self._runtime, "shard_watchdog", None)
        if wd is None or not wd.enabled:
            return
        sup = self._children.premarket
        if sup is None or not hasattr(sup, "is_running") or not hasattr(sup, "stop_for_restart"):
            return
        open_dt, end_dt = shard_window(MarketVenue.NXT, MarketSession.NXT_PRE, day=today, anchors=anchors)
        if not open_dt <= now < end_dt:
            return
        observations = (
            ShardObservation(
                key="premarket",
                venue="nxt",
                running=bool(sup.is_running()),
                started_at_wall=getattr(sup, "started_at_wall", None),
                last_journal_wall=shard_last_journal_wall(
                    self._paths.journal_root,
                    vendor="kis",
                    venue="nxt",
                    session=MarketSession.NXT_PRE.value,
                    streams=shard.streams,
                    day=today,
                    shard_index=shard.shard_index,
                ),
            ),
        )
        decisions = self._premarket_watchdog.evaluate(
            observations,
            now_wall=now.timestamp(),
            window_open_wall=open_dt.timestamp(),
            in_window=bool(open_dt <= now < end_dt),
            day=today,
        )
        self._apply_shard_decisions(
            decisions,
            watchdog=self._premarket_watchdog,
            venue="nxt",
            today=today,
            supervisors={"premarket": sup},
            max_restarts=wd.max_restarts_per_day,
        )

    def _apply_shard_decisions(
        self,
        decisions: tuple[ShardDecision, ...],
        *,
        watchdog: ShardWatchdog,
        venue: str,
        today: dt.date,
        supervisors: Mapping[str, ProcessSupervisor],
        max_restarts: int,
    ) -> bool:
        """Log verdicts and stop at most one stale shard; returns True when stopped."""
        st = self._state
        wide = [d for d in decisions if d.verdict is ShardVerdict.VENUE_WIDE_SILENCE]
        if wide and (venue, today) not in st.shard_venue_wide_warned:
            st.shard_venue_wide_warned.add((venue, today))
            peak = max(d.silent_s or 0.0 for d in wide)
            logger.warning(
                "[DATA] stage=shard_watchdog status=WARN reason=venue_wide_silence venue=%s shards=%d silent_s=%.0f limit_s=%.0f",
                venue,
                len(wide),
                peak,
                wide[0].limit_s,
            )
        for d in decisions:
            if d.verdict is ShardVerdict.BUDGET_EXHAUSTED and (d.key, today) not in st.shard_budget_alerted:
                st.shard_budget_alerted.add((d.key, today))
                logger.critical(
                    "[DATA] stage=shard_watchdog status=FAIL reason=stale_budget_exhausted key=%s silent_s=%.0f limit_s=%.0f max_restarts=%d",
                    d.key,
                    d.silent_s or 0.0,
                    d.limit_s,
                    max_restarts,
                )
        for d in decisions:
            if not d.restart:
                continue
            sup = supervisors.get(d.key)
            if sup is None:
                continue
            stop_result = sup.stop_for_restart(reason="stale_child", timeout_s=CHILD_STOP_TIMEOUT_S)
            restarts_today = watchdog.record_restart(d.key, day=today)
            st.shard_stale_restarts += 1
            logger.warning(
                "[SYS] stage=child_restart status=STALE reason=stale_child key=%s silent_s=%.0f limit_s=%.0f stop_result=%s restarts_today=%d",
                d.key,
                d.silent_s or 0.0,
                d.limit_s,
                stop_result,
                restarts_today,
            )
            return True
        return False

    def _supervise_aftermarket(
        self, now: dt.datetime, state: SessionState, today: dt.date, anchors: SessionAnchors
    ) -> None:
        """Launch shards of venues whose arm instant has passed and supervise every aftermarket child.

        Each venue becomes due ``prearm_lead_s`` before its shifted open; a circuit-open alert is
        emitted once per supervisor key until EOD clears the alert set.
        """
        st = self._state
        ch = self._children
        paths = self._paths
        prearm_lead_s = self._after_cfg.prearm_lead_s
        due = tuple(
            venue
            for venue in (MarketVenue.NXT, MarketVenue.KRX)
            if now >= aftermarket_arm_at(today, venue, anchors=anchors, prearm_lead_s=prearm_lead_s)
        )
        if due and not (
            state is SessionState.FULL_ACTIVE
            and (st.aftermarket_refresh_day != today or st.aftermarket_nxt_refresh_day != today)
        ):
            # A pending reselection in FULL_ACTIVE must not latch an empty plan for the day.
            self._ensure_aftermarket_plan(today)
        plan_slots = {f"krx-{s.shard_index:02d}": s.credential_slot for s in st.regular_kis_plan}
        live_regular_slots = {
            plan_slots[key]
            for key, sup in ch.regular_kis.items()
            if key in plan_slots and hasattr(sup, "is_running") and bool(sup.is_running())
        }
        for shard in st.aftermarket_plan:
            if st.aftermarket_plan_day != today or shard.venue not in due:
                continue
            supervisor_key = f"{shard.venue.value}:{shard.shard_index}"
            if supervisor_key in ch.aftermarket:
                continue
            slot = getattr(shard, "credential_slot", None)
            if slot is not None and slot in live_regular_slots:
                if slot not in st.aftermarket_deferred_slots:
                    st.aftermarket_deferred_slots.add(slot)
                    logger.warning(
                        "[DATA] stage=aftermarket_stream status=DEFERRED reason=slot_busy_regular_kis slot=%s",
                        slot,
                    )
                continue
            self._install_aftermarket(supervisor_key, _aftermarket_stream_cmd(today, paths, shard=shard))
            open_time = aftermarket_open_time(shard.venue, anchors)
            logger.info(
                "[DAEMON] stage=aftermarket_stream status=ARMED key=%s open=%s lead_s=%.1f",
                supervisor_key,
                open_time.isoformat(),
                (dt.datetime.combine(today, open_time, tzinfo=_KST) - now).total_seconds(),
            )
        self._watch_aftermarket_shards(now, today, anchors)
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

    def _maybe_refresh_premarket_pool(self, now: dt.datetime, anchors: SessionAnchors) -> None:
        """Build the next business day's premarket pool once per business evening.

        Runs only when enabled and ``premarket_pool_due``; needs a BUSINESS view of today with a resolved
        ``next_business_day``. Skips (INFO, marks done) when the next day's anchors are shifted or a valid
        pool for the target already exists, so restarts are idempotent. Vendor/file failures retry every
        ``pool_retry_s``; the first failure per source day logs WARNING, later retries DEBUG. Never raises.
        """
        cfg = self._premarket_cfg
        if not cfg.enabled:
            return
        st = self._state
        paths = self._paths
        today = now.astimezone(_KST).date()
        target: dt.date | None = None
        try:
            if st.premarket_pool_day == today:
                return
            if st.next_premarket_pool_at is not None and now < st.next_premarket_pool_at:
                return
            if not premarket_pool_due(now, anchors=anchors, settle_s=cfg.pool_settle_s):
                return
            view = self._gate.view(today, now)
            if view.status is not TradingDayStatus.BUSINESS or view.trading_day is None:
                return
            target = view.trading_day.next_business_day
            next_anchors = view.trading_day.next_anchors
            if next_anchors is None:
                next_anchors = resolve_session_anchors(paths.session_calendar_dir, target)
            if next_anchors.open_shift != dt.timedelta(0):
                st.premarket_pool_day = today
                logger.info(
                    "[DATA] stage=premarket_pool status=SKIP reason=shifted_next_day target=%s",
                    target.isoformat(),
                )
                return
            if premarket_pool_ready(
                paths.premarket_candidates(target), target_date=target, max_candidates=cfg.max_symbols
            ):
                st.premarket_pool_day = today
                logger.info(
                    "[DATA] stage=premarket_pool status=SKIP reason=pool_ready target=%s",
                    target.isoformat(),
                )
                return
            effective_from = dt.datetime.combine(target, STANDARD_NXT_PREMARKET_OPEN, tzinfo=_KST) - dt.timedelta(
                seconds=cfg.start_lead_s
            )
            excluded = self._aftermarket_excluded_symbols(today)
            snapshot = refresh_premarket_pool(
                target_date=target,
                generated_at=now,
                effective_from=effective_from,
                client=_build_kis_client(paths),
                out_path=paths.premarket_candidates(target),
                capacity=cfg.max_symbols,
                excluded_symbols=excluded,
            )
            st.premarket_pool_day = today
            st.next_premarket_pool_at = None
            logger.info(
                "[DATA] stage=premarket_pool status=OK target=%s symbols=%d",
                target.isoformat(),
                len(snapshot.candidates),
            )
        except (KrxAlphaError, KisApiError, OSError, ValueError) as exc:
            st.next_premarket_pool_at = now + dt.timedelta(seconds=cfg.pool_retry_s)
            label = target.isoformat() if target is not None else "unknown"
            if st.premarket_pool_failed_day != today:
                st.premarket_pool_failed_day = today
                logger.warning(
                    "[DATA] stage=premarket_pool status=FAIL reason=%s target=%s next_retry=%s",
                    str(exc),
                    label,
                    st.next_premarket_pool_at.isoformat(),
                )
            else:
                logger.debug(
                    "[DATA] stage=premarket_pool status=FAIL reason=%s target=%s next_retry=%s",
                    str(exc),
                    label,
                    st.next_premarket_pool_at.isoformat(),
                )

    def _ensure_premarket_plan(self, today: dt.date) -> None:
        """Once per date: read today's pool and plan the single shard; no pool/invalid pool → shard None with one ERROR log naming the reason."""
        st = self._state
        if st.premarket_plan_day == today:
            return
        st.premarket_plan_day = today
        cfg = self._premarket_cfg
        paths = self._paths
        try:
            snapshot = read_candidate_snapshot(
                paths.premarket_candidates(today),
                expected_session_date=today,
                expected_session="premarket",
                max_candidates=cfg.max_symbols,
            )
            st.premarket_shard = plan_premarket_shard(
                symbols=tuple(str(row["symbol"]) for row in snapshot.candidates),
                credentials=load_kis_data_credentials(),
                credential_slot=str(cfg.credential_slot),
                pair_capacity_per_connection=int(cfg.pair_capacity_per_connection or 0),
                nxt_streams=cfg.nxt_streams,
            )
        except (KrxAlphaError, OSError, ValueError) as exc:
            st.premarket_shard = None
            logger.error(
                "[DATA] stage=premarket_plan status=SKIP reason=%s date=%s",
                str(exc),
                today.isoformat(),
            )

    def _supervise_premarket(self, now: dt.datetime, anchors: SessionAnchors) -> None:
        """Start, supervise and stop the premarket child; never raises and never touches any other child."""
        try:
            cfg = self._premarket_cfg
            if not cfg.enabled:
                return
            st = self._state
            ch = self._children
            paths = self._paths
            today = now.astimezone(_KST).date()
            due = premarket_collection_due(now, anchors=anchors, start_lead_s=cfg.start_lead_s)
            if due:
                view = self._gate.view(today, now)
                if view.status is TradingDayStatus.HOLIDAY:
                    due = False
            if not due:
                self._stop_stream_child(StreamChildKey.PREMARKET)
                return
            self._ensure_premarket_plan(today)
            shard = st.premarket_shard
            if shard is None:
                return
            if ch.premarket is None:
                self._stop_nightly_child(reason="premarket")
                ch.premarket = ProcessSupervisor(
                    cmd=premarket_stream_cmd(today, paths, shard=shard),
                    breaker=RestartCircuitBreaker(),
                )
            self._watch_premarket_shard(now, today, anchors, shard)
            result = ch.premarket.ensure_running()
            if result == "restarted":
                logger.warning(
                    "[DATA] stage=premarket_stream status=RESTARTED exit_code=%s",
                    ch.premarket.last_exit_code,
                )
            elif result == "circuit_open" and st.premarket_circuit_alerted_day != today:
                st.premarket_circuit_alerted_day = today
                logger.critical(
                    "[DATA] stage=premarket_stream status=FAIL reason=circuit_open date=%s",
                    today.isoformat(),
                )
        except (KrxAlphaError, KisApiError, OSError, ValueError) as exc:
            logger.warning("[DATA] stage=premarket_supervise status=FAIL reason=%s", str(exc))

    def _ensure_regular_kis_plan(self, today: dt.date) -> None:
        """Plan today's ready candidate prefix once, containing file and credential failures."""
        st = self._state
        if st.regular_kis_plan_day == today:
            return
        if st.degraded_active:
            st.regular_kis_plan_day = today
            st.regular_kis_plan = ()
            logger.error(
                "[DATA] stage=regular_kis_plan status=SKIP reason=degraded_candidates date=%s",
                today.isoformat(),
            )
            return
        if st.orchestrated_for != today:
            return
        st.regular_kis_plan_day = today
        try:
            data = read_candidates(self._paths.candidates)
            if data is None or (isinstance(data, dict) and not data.get("candidates")):
                st.regular_kis_plan_day = None
                return
            rows = data.get("candidates") if isinstance(data, dict) else None
            if (
                not isinstance(rows, list)
                or not rows
                or not all(isinstance(row, dict) and isinstance(row.get("symbol"), str) and row["symbol"] for row in rows)
            ):
                raise KrxAlphaError(f"invalid candidates file: {self._paths.candidates}")
            symbols = tuple(str(row["symbol"]) for row in rows if isinstance(row, dict))
            cfg = getattr(self._runtime, "regular_kis", None) or RegularKisSettings()
            st.regular_kis_plan = plan_regular_kis_shards(
                symbols=symbols,
                credentials=load_kis_data_credentials(),
                credential_slots=tuple(cfg.credential_slots),
                pair_capacity_per_connection=int(cfg.pair_capacity_per_connection or 0),
                streams=cfg.streams,
            )
        except (KrxAlphaError, OSError, ValueError) as exc:
            st.regular_kis_plan = ()
            logger.error(
                "[DATA] stage=regular_kis_plan status=SKIP reason=%s date=%s",
                str(exc),
                today.isoformat(),
            )

    def _supervise_regular_kis(self, now: dt.datetime, anchors: SessionAnchors) -> None:
        """Start, supervise, watchdog and stop KIS regular shards; never raises and never touches any other child."""
        try:
            cfg = getattr(self._runtime, "regular_kis", None) or RegularKisSettings()
            st = self._state
            ch = self._children
            paths = self._paths
            today = now.astimezone(_KST).date()
            due = bool(cfg.enabled)
            if due:
                view = self._gate.view(today, now)
                if view.status is TradingDayStatus.HOLIDAY or not regular_kis_collection_due(
                    now,
                    anchors=anchors,
                    start_lead_s=cfg.start_lead_s,
                    close_grace_s=cfg.close_grace_s,
                ):
                    due = False
            if not due:
                for live in ch.regular_kis.values():
                    live.stop(timeout_s=CHILD_STOP_TIMEOUT_S)
                ch.regular_kis.clear()
                return
            self._ensure_regular_kis_plan(today)
            for shard in st.regular_kis_plan:
                key = f"krx-{shard.shard_index:02d}"
                sup = ch.regular_kis.get(key)
                if sup is None:
                    sup = ProcessSupervisor(
                        cmd=regular_kis_stream_cmd(today, paths, shard=shard),
                        breaker=RestartCircuitBreaker(),
                    )
                    ch.regular_kis[key] = sup
                result = sup.ensure_running()
                if result == "restarted":
                    logger.warning(
                        "[DATA] stage=regular_kis_stream status=RESTARTED shard=%s exit_code=%s",
                        key,
                        sup.last_exit_code,
                    )
                elif result == "circuit_open" and key not in st.regular_kis_circuit_alerted:
                    st.regular_kis_circuit_alerted.add(key)
                    logger.critical(
                        "[DATA] stage=regular_kis_stream status=FAIL reason=circuit_open shard=%s date=%s",
                        key,
                        today.isoformat(),
                    )
            self._watch_regular_kis_shards(now, today, anchors)
        except (KrxAlphaError, KisApiError, OSError, ValueError) as exc:
            logger.warning("[DATA] stage=regular_kis_supervise status=FAIL reason=%s", str(exc))

    def _watch_regular_kis_shards(self, now: dt.datetime, today: dt.date, anchors: SessionAnchors) -> None:
        """Judge each running regular shard's journal freshness inside its window and stop a stale one."""
        try:
            self._watch_regular_kis_shards_guarded(now, today, anchors)
        except (OSError, ValueError) as exc:
            logger.warning("[DATA] stage=shard_watchdog status=ERROR reason=%s", type(exc).__name__)

    def _watch_regular_kis_shards_guarded(
        self, now: dt.datetime, today: dt.date, anchors: SessionAnchors
    ) -> None:
        wd = getattr(self._runtime, "shard_watchdog", None)
        if wd is None or not wd.enabled:
            return
        st = self._state
        ch = self._children
        if not ch.regular_kis or st.regular_kis_plan_day != today:
            return
        plan = {f"krx-{s.shard_index:02d}": s for s in st.regular_kis_plan}
        live = {
            key: sup
            for key, sup in ch.regular_kis.items()
            if key in plan and hasattr(sup, "is_running") and hasattr(sup, "stop_for_restart")
        }
        if not live:
            return
        open_dt, end_dt = shard_window(MarketVenue.KRX, MarketSession.REGULAR, day=today, anchors=anchors)
        if not open_dt <= now < end_dt:
            return
        observations = tuple(
            ShardObservation(
                key=key,
                venue="krx",
                running=bool(sup.is_running()),
                started_at_wall=getattr(sup, "started_at_wall", None),
                last_journal_wall=shard_last_journal_wall(
                    self._paths.journal_root,
                    vendor="kis",
                    venue="krx",
                    session=MarketSession.REGULAR.value,
                    streams=plan[key].streams,
                    day=today,
                    shard_index=plan[key].shard_index,
                ),
            )
            for key, sup in live.items()
        )
        decisions = self._regular_kis_watchdog.evaluate(
            observations,
            now_wall=now.timestamp(),
            window_open_wall=open_dt.timestamp(),
            in_window=True,
            day=today,
        )
        self._apply_shard_decisions(
            decisions,
            watchdog=self._regular_kis_watchdog,
            venue="krx",
            today=today,
            supervisors=live,
            max_restarts=wd.max_restarts_per_day,
        )

    def _log_premarket_eod(self, ref_day: dt.date) -> None:
        cfg = self._premarket_cfg
        if not cfg.enabled:
            return
        st = self._state
        paths = self._paths
        if st.premarket_plan_day != ref_day or st.premarket_shard is None:
            logger.info(
                "[DATA] stage=premarket_eod status=SKIPPED date=%s accepted=%d planned=%d",
                ref_day.isoformat(),
                0,
                0,
            )
            return
        report = premarket_eod_report(paths.premarket_manifest_path(ref_day), date=ref_day)
        status = "OK" if report.closed else "NOT_CLOSED"
        logger.info(
            "[DATA] stage=premarket_eod status=%s date=%s accepted=%d planned=%d",
            status,
            ref_day.isoformat(),
            report.accepted_pairs,
            report.planned_pairs,
        )

    def _log_aftermarket_prearm(self, ref_day: dt.date, anchors: SessionAnchors) -> int:
        """Log pre-arm verdicts for today's aftermarket plan; return the count of non-ARMED shards."""
        st = self._state
        if not self._cfg.after_market_enabled or not st.aftermarket_plan:
            return 0
        expected = tuple(
            (shard, self._paths.aftermarket_manifest_path(ref_day, shard.venue, shard.shard_index))
            for shard in st.aftermarket_plan
        )
        findings = check_aftermarket_prearm(expected=expected, date=ref_day, anchors=anchors)
        late = 0
        for finding in findings:
            if finding.verdict is PrearmVerdict.ARMED:
                continue
            late += 1
            open_time = dt.datetime.fromtimestamp(finding.open_at_ns / 1e9, tz=_KST).time()
            open_ns = finding.open_at_ns

            def _lead(t: int | None, *, _open_ns: int = open_ns) -> str:
                return "na" if t is None else f"{(_open_ns - t) / 1e9:.3f}"

            logger.error(
                "[DATA] stage=aftermarket_prearm status=%s venue=%s shard=%d open=%s start_lead_s=%s ack_lead_s=%s reason=%s",
                finding.verdict.value,
                finding.venue,
                finding.shard_index,
                open_time.isoformat(),
                _lead(finding.started_at_ns),
                _lead(finding.initial_ack_ns),
                finding.reason,
            )
        armed = len(findings) - late
        logger.info(
            "[DATA] stage=aftermarket_prearm status=%s armed=%d shards=%d date=%s",
            "OK" if late == 0 else "DEGRADED",
            armed,
            len(findings),
            ref_day.isoformat(),
        )
        return late

    def _log_aftermarket_coverage(self, ref_day: dt.date) -> int:
        """Log per-shard zero-frame symbols for today's aftermarket plan; return the total zero-frame symbol count."""
        st = self._state
        if not self._cfg.after_market_enabled or not st.aftermarket_plan:
            return 0
        expected = tuple(
            (shard, self._paths.aftermarket_manifest_path(ref_day, shard.venue, shard.shard_index))
            for shard in st.aftermarket_plan
        )
        findings = check_aftermarket_symbol_coverage(expected=expected, date=ref_day)
        zero_total = 0
        for finding in findings:
            if finding.zero_frame_symbols is None:
                logger.info(
                    "[DATA] stage=aftermarket_coverage status=UNVERIFIABLE venue=%s shard=%d reason=%s",
                    finding.venue,
                    finding.shard_index,
                    finding.reason,
                )
            elif finding.zero_frame_symbols:
                zero_total += len(finding.zero_frame_symbols)
                logger.warning(
                    "[DATA] stage=aftermarket_coverage status=ZERO_FRAMES venue=%s shard=%d accepted=%d zero=%d symbols=%s",
                    finding.venue,
                    finding.shard_index,
                    finding.accepted_symbols,
                    len(finding.zero_frame_symbols),
                    ",".join(finding.zero_frame_symbols),
                )
        logger.info(
            "[DATA] stage=aftermarket_coverage status=%s zero=%d shards=%d date=%s",
            "GAPS" if zero_total else "OK",
            zero_total,
            len(findings),
            ref_day.isoformat(),
        )
        return zero_total

    def _step_post_market_eod(self, now: dt.datetime, day: TradingDayView | None, anchors: SessionAnchors) -> float:
        """Stop session children and run EOD maintenance once per KST date; return 60.0 s.

        Aftermarket collectors are stopped only inside the once-per-date gate, after
        `eod_attempted_for` is recorded, so a failing EOD is never retried every cycle.
        """
        st = self._state
        ch = self._children
        if ch.regular is not None:
            result = self._stop_stream_child(StreamChildKey.REGULAR)
            logger.info("[DAEMON] stage=streamer_stop result=%s", result, extra=EVENT)
        self._stop_session_children()
        self._stop_regular_kis_children(clear_circuit_alerts=False)
        ref_day = now.astimezone(_KST).date()
        if st.eod_attempted_for != ref_day:
            st.eod_attempted_for = ref_day
            self._stop_aftermarket_children(clear_circuit_alerts=True)
            assert day is not None
            if day.status is TradingDayStatus.HOLIDAY:
                self._run_holiday_eod(now, ref_day)
            else:
                self._run_business_eod(now, ref_day, day, anchors)
        return 60.0

    def _run_holiday_eod(self, now: dt.datetime, ref_day: dt.date) -> None:
        cfg = self._cfg
        paths = self._paths
        housekeeping = _run_eod_housekeeping(cfg, paths, ref_day, progress=self._eod_progress)
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

    def _run_business_eod(
        self, now: dt.datetime, ref_day: dt.date, day: TradingDayView, anchors: SessionAnchors
    ) -> None:
        """Run business/unknown-day EOD: aftermarket readiness, housekeeping, reconciliation, backup, digest.

        Housekeeping always runs even when aftermarket manifests are not ready; every failed check
        degrades the status instead of aborting so the digest is always sent once per date.
        `aftermarket_restarts` is reset only after the digest.
        """
        st = self._state
        cfg = self._cfg
        paths = self._paths
        aftermarket_blocked = self._aftermarket_eod_blocked(now, ref_day, anchors)
        prearm_late = self._log_aftermarket_prearm(ref_day, anchors)
        zero_frame_symbols = self._log_aftermarket_coverage(ref_day)
        self._log_premarket_eod(ref_day)
        audit = self._derive_session_audit(ref_day, day, anchors)
        self._derive_kis_regular_audit(ref_day, day, anchors, ls_audit=audit)
        housekeeping = _run_eod_housekeeping(cfg, paths, ref_day, progress=self._eod_progress)
        reconcile_ok, reconciled = self._reconcile_regular_session(ref_day, day, anchors, audit=audit)
        crosscheck = self._reconcile_crosscheck_sessions(ref_day, day, anchors)
        backup_missing, backup_ok, host_backup = _check_backup(
            cfg.host_backup_status_path,
            paths,
            ref_day,
            now=now,
            max_age=dt.timedelta(hours=cfg.host_backup_max_age_h),
        )
        degraded_reasons = [
            name
            for name, failed in (
                ("maintenance", not housekeeping.maintenance_ok),
                ("offload", not housekeeping.offload_ok),
                ("reconcile", not reconcile_ok),
                ("backup", not backup_ok),
                ("aftermarket_not_ready", aftermarket_blocked),
            )
            if failed
        ]
        eod_status = "DEGRADED" if degraded_reasons else "OK"
        logger.info(
            "[DAEMON] stage=eod_maintenance deleted_partitions=%d uploaded=%d purged=%d status=%s",
            housekeeping.deleted,
            housekeeping.uploaded,
            housekeeping.purged,
            eod_status,
            extra=EVENT,
        )
        runtime_audit = getattr(self._runtime, "audit", None) or AuditSettings()
        if audit is None:
            audit_status = "OFF" if runtime_audit.mode is AuditMode.OFF else "UNAVAILABLE"
            trade_loss = "-"
            open_latency = "-"
        else:
            audit_status = audit.status.value
            trade_loss = (
                f"{audit.totals.confirmed_missing}/{audit.totals.trades_observed}"
                if audit.status is not AuditStatus.UNAVAILABLE else "-"
            )
            open_latency = (
                _tick_open_max_latency_s(audit, anchors) if audit.status is not AuditStatus.UNAVAILABLE else "-"
            )
        digest_body = "\n".join(
            [
                f"run_id={self._run_id}",
                f"status={eod_status}",
                f"deleted_partitions={housekeeping.deleted}",
                f"uploaded={housekeeping.uploaded}",
                f"purged={housekeeping.purged}",
                f"reconciled={reconciled}",
                f"audit_status={audit_status}",
                f"regular_trade_loss={trade_loss}",
                f"open_max_latency_s={open_latency}",
                f"aftermarket_ready={not aftermarket_blocked}",
                f"aftermarket_zero_frame_symbols={zero_frame_symbols}",
                f"aftermarket_prearm_late={prearm_late}",
                f"backup_missing={len(backup_missing)}",
                f"host_backup={host_backup}",
                f"streamer_restarts={st.streamer_restarts}",
                f"aftermarket_restarts={st.aftermarket_restarts}",
                f"shard_stale_restarts={st.shard_stale_restarts}",
                f"orchestration_attempts={st.orchestration_attempts}",
                *[f"crosscheck_{k.value}={v}" for k, v in crosscheck.items()],
                f"degraded_reasons={','.join(degraded_reasons) or 'none'}",
            ]
        )
        subject = f"[krx-alpha] EOD {ref_day.isoformat()} {eod_status}"
        digest_path = _archive_eod_digest(paths, ref_day, subject, digest_body)
        if digest_path is not None:
            digest_body = f"{digest_body}\ndigest_path={digest_path}"
        send_digest(subject, digest_body)
        st.aftermarket_restarts = 0
        st.shard_stale_restarts = 0

    def _aftermarket_eod_blocked(self, now: dt.datetime, ref_day: dt.date, anchors: SessionAnchors) -> bool:
        """Return whether missing aftermarket manifests degrade this EOD."""
        st = self._state
        cfg = self._cfg
        paths = self._paths
        aftermarket_manifests = (
            [paths.aftermarket_manifest_path(ref_day, shard.venue, shard.shard_index) for shard in st.aftermarket_plan]
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
        if aftermarket_blocked:
            logger.critical(
                "[DAEMON] stage=eod_maintenance status=DEGRADED reason=aftermarket_not_ready date=%s",
                ref_day.isoformat(),
            )
        if cfg.after_market_enabled and st.aftermarket_plan:
            planned_venues = {shard.venue for shard in st.aftermarket_plan}
            for venue in (MarketVenue.NXT, MarketVenue.KRX):
                if venue not in planned_venues:
                    logger.critical(
                        "[DAEMON] stage=eod_maintenance status=DEGRADED reason=aftermarket_venue_unplanned venue=%s date=%s",
                        venue.value,
                        ref_day.isoformat(),
                    )
                    aftermarket_blocked = True
        return aftermarket_blocked

    def _derive_session_audit(self, ref_day: dt.date, day: TradingDayView, anchors: SessionAnchors) -> SessionAudit | None:
        """Derive, persist and log the regular-session audit before EOD housekeeping; returns None when disabled, not a business day, or on any contained failure (EOD never aborts)."""
        runtime_audit = getattr(self._runtime, "audit", None) or AuditSettings()
        if runtime_audit.mode is AuditMode.OFF or day.status is not TradingDayStatus.BUSINESS:
            return None
        cfg = self._cfg
        paths = self._paths
        try:
            manifest_frame_counts = SessionManifest.load(paths.manifest_path(ref_day)).frame_counts
        except FileNotFoundError:
            manifest_frame_counts = None
        except (OSError, KrxAlphaError, ValueError, KeyError, TypeError, AttributeError) as exc:
            manifest_frame_counts = None
            logger.error(
                "[DATA] stage=session_audit status=ERROR date=%s error=%s",
                ref_day.isoformat(),
                str(exc),
                exc_info=True,
            )
        try:
            audit = derive_session_audit(
                paths.journal_root,
                vendor=cfg.vendor,
                venue=MarketVenue.KRX.value,
                session=MarketSession.REGULAR.value,
                day=ref_day,
                anchors=anchors,
                settings=runtime_audit,
                manifest_frame_counts=manifest_frame_counts,
                progress=self._eod_progress,
            )
        except (OSError, KrxAlphaError, ValueError) as exc:
            logger.error(
                "[DATA] stage=session_audit status=ERROR date=%s error=%s",
                ref_day.isoformat(),
                str(exc),
                exc_info=True,
            )
            return None
        try:
            write_session_audit(audit, paths.session_audit_path(ref_day, cfg.vendor))
        except (OSError, KrxAlphaError, ValueError) as exc:
            logger.error(
                "[DATA] stage=session_audit status=WRITE_FAIL date=%s error=%s",
                ref_day.isoformat(),
                str(exc),
                exc_info=True,
            )
        log_session_audit(audit, settings=runtime_audit, anchors=anchors, date=ref_day)
        return audit

    def _derive_kis_regular_audit(
        self,
        ref_day: dt.date,
        day: TradingDayView,
        anchors: SessionAnchors,
        *,
        ls_audit: SessionAudit | None = None,
    ) -> SessionAudit | None:
        """Derive and persist the KIS regular audit when the feature is enabled; returns None on disabled or contained failure; never alters EOD status."""
        cfg = getattr(self._runtime, "regular_kis", None) or RegularKisSettings()
        if not cfg.enabled:
            return None
        runtime_audit = getattr(self._runtime, "audit", None) or AuditSettings()
        if runtime_audit.mode is AuditMode.OFF or day.status is not TradingDayStatus.BUSINESS:
            return None
        try:
            kis_audit = derive_session_audit(
                self._paths.journal_root,
                vendor="kis",
                venue=MarketVenue.KRX.value,
                session=MarketSession.REGULAR.value,
                day=ref_day,
                anchors=anchors,
                settings=runtime_audit,
                manifest_frame_counts=None,
                progress=self._eod_progress,
            )
        except (OSError, KrxAlphaError, ValueError) as exc:
            logger.error(
                "[DATA] stage=regular_kis_audit status=ERROR date=%s error=%s",
                ref_day.isoformat(),
                str(exc),
                exc_info=True,
            )
            return None
        try:
            write_session_audit(kis_audit, self._paths.session_audit_path(ref_day, "kis"))
        except (OSError, KrxAlphaError, ValueError) as exc:
            logger.error(
                "[DATA] stage=regular_kis_audit status=WRITE_FAIL date=%s error=%s",
                ref_day.isoformat(),
                str(exc),
                exc_info=True,
            )
        log_session_audit(kis_audit, settings=runtime_audit, anchors=anchors, date=ref_day)
        kis_loss = kis_audit.totals.loss_ratio
        ls_loss = ls_audit.totals.loss_ratio if ls_audit is not None else float("nan")
        logger.info(
            "[DATA] stage=regular_kis_compare date=%s kis_loss_ratio=%.6g ls_loss_ratio=%.6g kis_open_max_lag_s=%s ls_open_max_lag_s=%s kis_symbols=%d",
            ref_day.isoformat(),
            kis_loss,
            ls_loss,
            _tick_open_max_latency_s(kis_audit, anchors),
            _tick_open_max_latency_s(ls_audit, anchors) if ls_audit is not None else "-",
            len(kis_audit.symbols),
        )
        return kis_audit

    def _reconcile_regular_session(
        self, ref_day: dt.date, day: TradingDayView, anchors: SessionAnchors, *, audit: SessionAudit | None = None
    ) -> tuple[bool, bool]:
        """Return `(reconcile_ok, reconciled)` for the KRX regular session.

        UNKNOWN calendar days skip reconciliation with a WARNING and report `(True, False)`;
        `OSError` is CRITICAL with traceback and reports `(False, False)`.
        """
        cfg = self._cfg
        paths = self._paths
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
                    audit=audit,
                    audit_settings=getattr(self._runtime, "audit", None) or AuditSettings(),
                )
                if not reconciled:
                    logger.critical(
                        "[DAEMON] stage=eod_maintenance status=FAIL reason=session_data_gap date=%s",
                        ref_day.isoformat(),
                    )
                    return False, False
                return True, True
            except OSError as e:
                logger.critical("[DAEMON] stage=eod_reconciliation status=FAIL error=%s", str(e), exc_info=True)
                return False, False
        logger.warning("[DAEMON] stage=eod_reconciliation status=SKIP reason=calendar_unknown")
        return True, False

    def _reconcile_crosscheck_sessions(
        self, ref_day: dt.date, day: TradingDayView, anchors: SessionAnchors
    ) -> dict[StreamChildKey, str]:
        """Return per-crosscheck-child outcome in {"ok","gap","error","skipped"}; never affects primary EOD status."""
        cfg = self._cfg
        paths = self._paths
        outcomes: dict[StreamChildKey, str] = {}
        for spec in self._session_specs():
            if spec.role is not StreamRole.CROSSCHECK:
                continue
            if day.status is not TradingDayStatus.BUSINESS or self._state.orchestrated_for != ref_day:
                outcomes[spec.key] = "skipped"
                continue
            assert spec.manifest_path is not None
            assert spec.journal_vendor is not None
            try:
                reconciled = check_session_reconciliation(
                    manifest_path=spec.manifest_path(paths, ref_day),
                    date=ref_day,
                    journal_root=paths.journal_root,
                    streams=cfg.streams,
                    vendor=spec.journal_vendor(cfg),
                    venue=MarketVenue.KRX.value,
                    session=MarketSession.REGULAR.value,
                    regular_open=anchors.regular_open,
                    regular_close=anchors.regular_close,
                    failure_level=logging.WARNING,
                )
            except OSError as exc:
                outcomes[spec.key] = "error"
                _log_crosscheck_reconciliation_failure(spec.key.value, exc)
            else:
                outcomes[spec.key] = "ok" if reconciled else "gap"
        return outcomes

    def _stop_aftermarket_children(self, *, clear_circuit_alerts: bool) -> None:
        """Stop all aftermarket shard supervisors and clear their per-key results.

        Circuit-open alert dedup survives a holiday stop and is cleared only at EOD, so a same-day
        restart does not re-alert keys already reported.
        """
        for sup in self._children.aftermarket.values():
            sup.stop(timeout_s=CHILD_STOP_TIMEOUT_S)
        self._children.aftermarket.clear()
        self._state.aftermarket_results.clear()
        if clear_circuit_alerts:
            self._state.aftermarket_circuit_alerted.clear()

    def _stop_regular_kis_children(self, *, clear_circuit_alerts: bool) -> None:
        """Stop all regular KIS shard supervisors and drop their handles.

        Circuit-open alert dedup survives a holiday stop and is cleared only on the next session
        day, so a same-day restart does not re-alert shards already reported.
        """
        for sup in self._children.regular_kis.values():
            sup.stop(timeout_s=CHILD_STOP_TIMEOUT_S)
        self._children.regular_kis.clear()
        if clear_circuit_alerts:
            self._state.regular_kis_circuit_alerted.clear()


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

    run_id = configure_logging(
        "daemon",
        log_dir=runtime.paths.logs_dir if ObservabilitySettings().persistent_logs else None,
        alert_ledger_path=runtime.paths.alert_ledger_file,
    )
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
            record = replace(record, crash_error=error_type, crash_alert_day=crash_day, crash_alerts_sent=sent)
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
