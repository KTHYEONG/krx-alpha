"""Collector 24/7 daemon 오케스트레이션 (service 직접 호출, CLI 역방향 의존 제거)."""

from __future__ import annotations

import datetime as dt
import logging
import pathlib
import signal
import sys
import threading
from dataclasses import dataclass, replace
from typing import Any, cast
from zoneinfo import ZoneInfo

import polars as pl
import requests

from src.core.calendar import SessionState, calc_sleep_seconds, get_target_state
from src.core.config import (
    AftermarketSettings,
    CollectorSettings,
    DataPaths,
    ExecutionSettings,
    KisCredentials,
    KisTokenSettings,
    KrxCredentials,
    ObservabilitySettings,
    SnapshotSettings,
    TossCredentials,
    TossProgramTradesSettings,
    load_credentials,
)
from src.core.errors import KrxAlphaError, MissingCredentialsError
from src.core.observability import EVENT, configure_logging, send_digest
from src.execution.contracts import KisApiError
from src.execution.kis_client import KisRestClient, RateLimiter, kis_token_cache_path
from src.marketdata.krx_bars import KrxBarsError
from src.marketdata.service import BarsRefreshResult as BarsRefreshResult
from src.marketdata.service import KisFallbackError as KisFallbackError
from src.marketdata.service import backfill_universe_program_trades, refresh_bars, refresh_bars_via_kis_fallback
from src.marketdata.toss_calendar import (
    TossCalendarError,
    TradingDay,
    fetch_trading_day,
    load_trading_day_cache,
    save_trading_day_cache,
    trading_day_from_cache,
)
from src.marketdata.toss_program_trades import TossProgramTradesError
from src.orchestration.eod import (
    aftermarket_eod_ready,
    check_backup_freshness,
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
from src.storage.snapshot_store import SnapshotStore
from src.universe.aftermarket import AftermarketUniverseError, refresh_aftermarket_candidates
from src.universe.ipc import CandidateFileError, read_candidate_snapshot, read_candidates
from src.universe.policy import ineligible_security_symbols
from src.universe.service import UniversePlanResult as UniversePlanResult
from src.universe.service import plan_universe

logger = logging.getLogger(__name__)
_KST = ZoneInfo("Asia/Seoul")
HEARTBEAT_SUMMARY_S: float = 600.0
INGEST_STALE_S: float = 300.0
INGEST_CHECK_S: float = 60.0
INGEST_WATCH_START: dt.time = dt.time(9, 5)
INGEST_WATCH_END: dt.time = dt.time(15, 25)
# 컴포즈 stop_grace_period(30s) 안에 자식 정상종료 + 로그 flush 를 끝내기 위한 공유 데드라인.
SHUTDOWN_CHILD_DEADLINE_S: float = 20.0
HOLIDAY_SLEEP_CAP_S: float = 3600.0


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


def _build_kis_client(paths: DataPaths) -> KisRestClient:
    """execution 모듈과 동일한 토큰 캐시를 공유하는 KIS 클라이언트를 생성한다."""
    creds = load_credentials(KisCredentials)
    execution = ExecutionSettings(data_root=paths.root)
    token_settings = KisTokenSettings()
    return KisRestClient(
        creds=creds,
        session=requests,
        token_cache_path=kis_token_cache_path(token_settings.token_cache_dir, creds.kis_app_key),
        limiter=RateLimiter(execution.rest_rate_per_s),
        now=lambda: dt.datetime.now(_KST),
        timeout_s=execution.request_timeout_s,
        allow_token_issue=token_settings.allow_issue,
    )


def _run_program_trades_auto_backfill(paths: DataPaths, today: dt.date) -> None:
    """오늘 유니버스 중 프로그램매매 이력이 부족한 종목만 백필한다 (실패해도 스트리밍 준비를 막지 않는다)."""
    try:
        settings = TossProgramTradesSettings()
        if not settings.auto_backfill_enabled:
            return
        try:
            creds = load_credentials(TossCredentials)
        except MissingCredentialsError as exc:
            logger.warning("[DAEMON] stage=program_trades_auto_backfill status=SKIP reason=%s", str(exc))
            return
        data = read_candidates(paths.candidates)
        rows = cast("list[dict[str, object]] | None", data.get("candidates") if data else None)
        if not rows:
            return
        symbols = tuple(str(row["symbol"]) for row in rows)
        result = backfill_universe_program_trades(
            store_path=paths.program_trades_store,
            symbols=symbols,
            lookback_days=settings.auto_backfill_lookback_days,
            reference_date=today,
            app_key=creds.toss_app_key,
            app_secret=creds.toss_app_secret,
            rate_per_s=settings.rate_per_s,
        )
        if result.symbols_ok + result.symbols_failed > 0:
            logger.info(
                "[DAEMON] stage=program_trades_auto_backfill status=OK symbols_ok=%d symbols_failed=%d appended_rows=%d",
                result.symbols_ok,
                result.symbols_failed,
                result.appended_rows,
            )
    except TossProgramTradesError as exc:
        logger.error("[DAEMON] stage=program_trades_auto_backfill status=FAIL reason=%s", str(exc))
    except Exception as exc:  # noqa: BLE001 - 자동 백필 실패가 스트리밍 준비를 막지 않도록 격리
        logger.error("[DAEMON] stage=program_trades_auto_backfill status=FAIL reason=%s", str(exc))


def run_session_orchestration(
    *, today: dt.date, settings: CollectorSettings, trading_day: TradingDay | None = None
) -> bool:
    """bars 갱신 + 유니버스 선정을 타입드 인자로 직접 호출하고 후보 준비 여부를 반환한다."""
    paths = settings.paths
    # bars 갱신 실패는 기존 store 로 진행 가능하므로 도메인 예외만 흡수한다 (광역 포획 금지).
    try:
        auth_key = load_credentials(KrxCredentials).krx_openapi_key
        refresh_bars(
            store_path=paths.bars_store,
            market_map_path=paths.market_map,
            ref_date=today,
            window_days=settings.bars_window_days,
            auth_key=auth_key,
        )
    except (MissingCredentialsError, KrxBarsError) as exc:
        logger.error("[DAEMON] stage=orchestration status=FAIL step=bars_refresh reason=%s", str(exc))
    if not paths.bars_store.exists():
        logger.critical("[DAEMON] stage=orchestration status=FAIL reason=no_bars_store")
        return False
    decision_date: dt.date = pl.scan_parquet(paths.bars_store).select(pl.col("date").max()).collect().item()
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
                store_path=paths.bars_store,
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
        status_source: KisRestClient | None = _build_kis_client(paths)
    except MissingCredentialsError as exc:
        logger.warning("[DAEMON] stage=orchestration status=DEGRADED step=security_status reason=%s", str(exc))
        status_source = None
    plan_universe(
        bars_path=paths.bars_store,
        decision_date=decision_date,
        out_path=paths.universe_out(decision_date),
        slot_budget=settings.universe_slot_budget,
        candidates_path=paths.candidates,
        session_date=today,
        status_source=status_source,
        snapshot_store=SnapshotStore(paths=paths, session_date=today),
    )
    ready = _candidates_ready(paths.candidates)
    if ready:
        _run_program_trades_auto_backfill(paths, today)
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


def _run_eod_housekeeping(cfg: CollectorSettings, paths: DataPaths, ref_day: dt.date) -> _EodHousekeeping:
    """Run the date-agnostic EOD storage sequence shared by business days and holidays.

    Order matters: L0 is only deleted after the offload has verified its L1 copy
    remotely, so maintenance runs once unverified (normalize only), then again
    with the verified set, followed by the remote L0 purge.
    """
    deleted = 0
    maintenance_ok = True
    try:
        deleted = run_eod_maintenance(
            paths.journal_root,
            retain_days=cfg.journal_retain_days,
            today=ref_day,
            archive_root=paths.archive_root,
            quarantine_root=paths.quarantine_root,
            work_root=paths.work_root,
            verified_remote_l1=None,
        )
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
            post_deleted = run_eod_maintenance(
                paths.journal_root,
                retain_days=cfg.journal_retain_days,
                today=ref_day,
                archive_root=paths.archive_root,
                quarantine_root=paths.quarantine_root,
                work_root=paths.work_root,
                verified_remote_l1=verified,
            )
            deleted = int(deleted) + int(post_deleted)
            try:
                run_eod_remote_l0_purge(paths.journal_root, verified)
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


def _check_backup(paths: DataPaths, ref_day: dt.date) -> tuple[list[str], bool]:
    try:
        missing = check_backup_freshness(manifest_dir=paths.manifest_dir, today=ref_day)
    except RemoteArchiveError as e:
        logger.critical(
            "[DAEMON] stage=backup_freshness status=FAIL reason=%s error=%s",
            classify_remote_failure(str(e)),
            str(e),
        )
        return [], False
    if missing:
        logger.critical("[DAEMON] stage=backup_freshness status=STALE missing=%d oldest=%s", len(missing), missing[0])
        return missing, False
    return [], True


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


def _resolve_trading_day_with_cache(today: dt.date, cache_path: pathlib.Path) -> TradingDay | None:
    day = resolve_trading_day(today)
    if day is not None:
        save_trading_day_cache(cache_path, day)
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


def run_collector_daemon(
    *,
    settings: CollectorSettings | None = None,
    sleep_fn: Any = None,
    max_cycles: int | None = None,
    now_fn: Any = None,
    shutdown: threading.Event | None = None,
) -> None:
    """Collector daemon main loop.

    Shutdown: when ``shutdown`` is set the loop stops starting work,
    stops all supervised children via ``stop_supervisors`` within ``SHUTDOWN_CHILD_DEADLINE_S`` and
    returns. A cycle already inside EOD finishes that call first; the deploy session gate keeps
    deploys out of the EOD window.
    """
    import time

    sleeper = sleep_fn if sleep_fn is not None else time.sleep
    cfg = settings if settings is not None else CollectorSettings()
    sched = replace(cfg.schedule, after_market_enabled=cfg.after_market_enabled)
    paths = cfg.paths
    cycle = 0

    run_id = configure_logging("daemon", log_dir=paths.logs_dir if ObservabilitySettings().persistent_logs else None)
    logger.info("[DAEMON] stage=start status=ONLINE timezone=Asia/Seoul run_id=%s", run_id, extra=EVENT)

    clock = now_fn if now_fn is not None else (lambda: dt.datetime.now(_KST))
    orchestrated_for: dt.date | None = None
    holiday_skip_logged_for: dt.date | None = None
    possible_holiday_warned_for: dt.date | None = None
    eod_attempted_for: dt.date | None = None
    gate = TradingDayGate(resolver=lambda d: _resolve_trading_day_with_cache(d, paths.calendar_cache))
    supervisor: ProcessSupervisor | None = None
    prev_state: SessionState | None = None
    last_summary: dt.datetime | None = None
    streamer_restarts = 0
    last_supervisor_result: str | None = None
    orchestration_day: dt.date | None = None
    next_orchestration_at: dt.datetime | None = None
    orchestration_attempts = 0
    degraded_active = False
    last_ingest_check: dt.datetime | None = None
    ingest_stale = False
    aftermarket_supervisors: dict[str, ProcessSupervisor] = {}
    aftermarket_plan: tuple[AftermarketShard, ...] = ()
    aftermarket_plan_day: dt.date | None = None
    aftermarket_refresh_day: dt.date | None = None
    aftermarket_eligibility_day: dt.date | None = None
    next_aftermarket_refresh_at: dt.datetime | None = None
    snapshot_cfg = SnapshotSettings()
    snapshot_supervisor: ProcessSupervisor | None = None
    last_snapshot_result: str | None = None

    def _shutdown_children() -> None:
        targets: list[ProcessSupervisor] = []
        if supervisor is not None:
            targets.append(supervisor)
        targets.extend(aftermarket_supervisors.values())
        if snapshot_supervisor is not None:
            targets.append(snapshot_supervisor)
        counts = stop_supervisors(targets, deadline_s=SHUTDOWN_CHILD_DEADLINE_S)
        raw = getattr(shutdown, "signal_name", "UNKNOWN") if shutdown is not None else "UNKNOWN"
        sig = str(raw) if raw else "UNKNOWN"
        logger.info(
            "[DAEMON] stage=shutdown status=STOPPED signal=%s graceful=%d killed=%d not_running=%d",
            sig,
            counts["graceful"],
            counts["killed"],
            counts["not_running"],
            extra=EVENT,
        )

    while True:
        if shutdown is not None and shutdown.is_set():
            _shutdown_children()
            return
        cycle += 1
        now = clock()
        state = get_target_state(now, sched)
        day = None
        if state in (
            SessionState.STREAMER_ACTIVE,
            SessionState.FULL_ACTIVE,
            SessionState.AFTER_MARKET_ACTIVE,
            SessionState.POST_MARKET_EOD,
        ):
            day = gate.view(now.astimezone(_KST).date(), now)
            if day.status is TradingDayStatus.HOLIDAY and holiday_skip_logged_for != day.date:
                logger.info(
                    "[DAEMON] stage=session status=SKIP reason=market_holiday date=%s",
                    day.date.isoformat(),
                    extra=EVENT,
                )
                holiday_skip_logged_for = day.date
        logger.debug("[DAEMON] cycle=%d now=%s state=%s", cycle, now.strftime("%Y-%m-%d %H:%M:%S"), state)
        if state != prev_state:
            logger.info(
                "[DAEMON] stage=state_change from=%s to=%s cycle=%d", prev_state, state, cycle, extra=EVENT
            )
            prev_state = state
        streamer_alive = last_supervisor_result in ("started", "running", "restarted")
        if last_summary is None or (now - last_summary).total_seconds() >= HEARTBEAT_SUMMARY_S:
            logger.info(
                "[DAEMON] stage=heartbeat state=%s cycle=%d streamer_alive=%s streamer_restarts=%d",
                state,
                cycle,
                streamer_alive,
                streamer_restarts,
            )
            last_summary = now

        if state == SessionState.WEEKEND_SLEEP:
            sleep_sec = 3600.0  # 주말엔 1시간씩 대기
        elif state == SessionState.PRE_MARKET_SLEEP:
            sleep_sec = min(calc_sleep_seconds(now, sched.streamer_start), 300.0)
        elif state in (SessionState.STREAMER_ACTIVE, SessionState.FULL_ACTIVE, SessionState.AFTER_MARKET_ACTIVE):
            assert day is not None
            today = day.date
            if day.status is TradingDayStatus.HOLIDAY:
                if supervisor is not None:
                    supervisor.stop(timeout_s=15.0)
                    supervisor = None
                    last_supervisor_result = None
                    degraded_active = False
                if snapshot_supervisor is not None:
                    snapshot_supervisor.stop(timeout_s=15.0)
                    snapshot_supervisor = None
                    last_snapshot_result = None
                for _sup in aftermarket_supervisors.values():
                    _sup.stop(timeout_s=15.0)
                aftermarket_supervisors.clear()
                if state is SessionState.STREAMER_ACTIVE:
                    _holiday_target = sched.scanner_start
                elif state is SessionState.FULL_ACTIVE:
                    _holiday_target = sched.market_close
                else:
                    _holiday_target = sched.after_market_close
                sleep_sec = min(calc_sleep_seconds(now, _holiday_target), HOLIDAY_SLEEP_CAP_S)
            else:
                if state is not SessionState.AFTER_MARKET_ACTIVE and today != orchestration_day:
                    # EOD를 거치지 못한 전일 스트리머가 남아 있으면 전일 session-date로 재기동되므로 먼저 정리한다
                    if supervisor is not None:
                        stale_stop = supervisor.stop(timeout_s=15.0)
                        logger.warning("[DAEMON] stage=streamer status=STOP_STALE_DAY stop_result=%s", stale_stop)
                        supervisor = None
                        last_supervisor_result = None
                    if snapshot_supervisor is not None:
                        snapshot_supervisor.stop(timeout_s=15.0)
                        snapshot_supervisor = None
                        last_snapshot_result = None
                    orchestration_day = today
                    next_orchestration_at = None
                    orchestration_attempts = 0
                    degraded_active = False
                    last_ingest_check = None
                    ingest_stale = False
                    next_aftermarket_refresh_at = None
                if (
                    state is not SessionState.AFTER_MARKET_ACTIVE
                    and orchestrated_for != today
                    and (next_orchestration_at is None or now >= next_orchestration_at)
                ):
                    trading_day = day.trading_day
                    orchestration_attempts += 1
                    try:
                        ready = run_session_orchestration(today=today, settings=cfg, trading_day=trading_day)
                    except Exception as e:  # noqa: BLE001 - 오케스트레이션 실패가 데몬 전체를 죽이지 않도록 격리
                        logger.critical(
                            "[DAEMON] stage=session status=FAIL reason=orchestration_error error=%s",
                            str(e),
                            exc_info=True,
                        )
                        ready = False
                    if ready:
                        orchestrated_for = today
                        next_orchestration_at = None
                        if supervisor is not None and degraded_active:
                            stop_result = supervisor.stop(timeout_s=15.0)
                            logger.info(
                                "[DAEMON] stage=streamer status=REPLACE_DEGRADED stop_result=%s",
                                stop_result,
                                extra=EVENT,
                            )
                        supervisor = ProcessSupervisor(
                            cmd=_stream_cmd(today, paths, degraded_reason=None),
                            breaker=RestartCircuitBreaker(),
                        )
                        degraded_active = False
                        last_supervisor_result = None
                        if snapshot_cfg.enabled:
                            if snapshot_supervisor is not None:
                                snapshot_supervisor.stop(timeout_s=15.0)
                            snapshot_supervisor = ProcessSupervisor(
                                cmd=_snapshot_cmd(today, paths),
                                breaker=RestartCircuitBreaker(),
                            )
                            last_snapshot_result = None
                    else:
                        next_orchestration_at = now + dt.timedelta(seconds=cfg.orchestration_retry_s)
                        log_fn = logger.critical if orchestration_attempts == 1 else logger.warning
                        log_fn(
                            "[DAEMON] stage=session status=FAIL reason=candidates_not_ready date=%s attempt=%d next_retry=%s",
                            today.isoformat(),
                            orchestration_attempts,
                            next_orchestration_at.isoformat(),
                        )
                        if supervisor is None:
                            rev = _degraded_candidates_rev(
                                paths.candidates, today, max_age_days=cfg.degraded_candidates_max_age_days
                            )
                            if rev is not None:
                                supervisor = ProcessSupervisor(
                                    cmd=_stream_cmd(today, paths, degraded_reason="orchestration_failed"),
                                    breaker=RestartCircuitBreaker(),
                                )
                                if snapshot_cfg.enabled and snapshot_supervisor is None:
                                    snapshot_supervisor = ProcessSupervisor(
                                        cmd=_snapshot_cmd(today, paths),
                                        breaker=RestartCircuitBreaker(),
                                    )
                                    last_snapshot_result = None
                                degraded_active = True
                                last_supervisor_result = None
                                logger.critical(
                                    "[DAEMON] stage=streamer status=DEGRADED reason=orchestration_failed candidates_rev=%d",
                                    rev,
                                )
                if supervisor is not None:
                    result = supervisor.ensure_running()
                    if result == "started":
                        logger.info("[DAEMON] stage=streamer status=STARTED", extra=EVENT)
                    elif result == "restarted":
                        streamer_restarts += 1
                        logger.warning(
                            "[DAEMON] stage=streamer status=RESTARTED exit_code=%s restarts=%d",
                            supervisor.last_exit_code,
                            streamer_restarts,
                        )
                    elif result == "circuit_open" and last_supervisor_result != "circuit_open":
                        logger.critical("[DAEMON] stage=streamer status=FAIL reason=circuit_open")
                    last_supervisor_result = result
                if snapshot_supervisor is not None and now.astimezone(_KST).time() < snapshot_cfg.run_end:
                    snapshot_result = snapshot_supervisor.ensure_running()
                    if snapshot_result == "restarted":
                        logger.warning(
                            "[DAEMON] stage=snapshots status=RESTARTED exit_code=%s",
                            snapshot_supervisor.last_exit_code,
                        )
                    elif snapshot_result == "circuit_open" and last_snapshot_result != "circuit_open":
                        logger.critical("[DAEMON] stage=snapshots status=FAIL reason=circuit_open")
                    last_snapshot_result = snapshot_result
                if (
                    state == SessionState.FULL_ACTIVE
                    and INGEST_WATCH_START <= now.astimezone(_KST).time() < INGEST_WATCH_END
                    and (last_ingest_check is None or (now - last_ingest_check).total_seconds() >= INGEST_CHECK_S)
                ):
                    last_ingest_check = now
                    age = _journal_age_s(paths.journal_root, cfg.vendor, today, now)
                    stale = age is None or age > INGEST_STALE_S
                    if stale and not ingest_stale and age is None and day.status is TradingDayStatus.UNKNOWN:
                        if possible_holiday_warned_for != today:
                            logger.warning(
                                "[DAEMON] stage=ingest_watchdog status=POSSIBLE_HOLIDAY date=%s",
                                today.isoformat(),
                            )
                            possible_holiday_warned_for = today
                    elif stale and not ingest_stale:
                        logger.critical(
                            "[DAEMON] stage=ingest_watchdog status=STALE date=%s age_s=%s",
                            today.isoformat(),
                            "none" if age is None else str(int(age)),
                        )
                    elif not stale and ingest_stale and age is not None:
                        logger.warning(
                            "[DAEMON] stage=ingest_watchdog status=RECOVERED date=%s age_s=%d",
                            today.isoformat(),
                            int(age),
                        )
                    ingest_stale = stale and not (
                        age is None and day.status is TradingDayStatus.UNKNOWN
                    )
                if state == SessionState.FULL_ACTIVE and cfg.after_market_enabled:
                    after_cfg = AftermarketSettings()
                    if (
                        now.astimezone(_KST).time() >= after_cfg.selection_time
                        and aftermarket_refresh_day != today
                        and (next_aftermarket_refresh_at is None or now >= next_aftermarket_refresh_at)
                    ):
                        excluded_symbols: frozenset[str]
                        if paths.bars_store.exists():
                            try:
                                eligibility_bars = (
                                    pl.scan_parquet(paths.bars_store)
                                    .select(["date", "symbol", "stock_cert_kind", "section"])
                                    .collect()
                                )
                            except (OSError, pl.exceptions.PolarsError):
                                excluded_symbols = frozenset()
                                if aftermarket_eligibility_day != today:
                                    aftermarket_eligibility_day = today
                                    logger.warning(
                                        "[ALGO] stage=aftermarket_eligibility status=DEGRADED reason=bars_unreadable"
                                    )
                            else:
                                excluded_symbols = ineligible_security_symbols(eligibility_bars)
                        else:
                            excluded_symbols = frozenset()
                            if aftermarket_eligibility_day != today:
                                aftermarket_eligibility_day = today
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
                            aftermarket_refresh_day = today
                            next_aftermarket_refresh_at = None
                        except (MissingCredentialsError, KisApiError, AftermarketUniverseError, CandidateFileError) as exc:
                            next_aftermarket_refresh_at = now + dt.timedelta(seconds=60.0)
                            logger.critical(
                                "[DAEMON] stage=aftermarket_reselection status=FAIL reason=%s next_retry=%s",
                                str(exc),
                                next_aftermarket_refresh_at.isoformat(),
                            )
                if state == SessionState.AFTER_MARKET_ACTIVE and cfg.after_market_enabled:
                    if aftermarket_plan_day != today:
                        try:
                            after = AftermarketSettings()
                            snapshot = read_candidate_snapshot(paths.aftermarket_candidates(today), expected_session_date=today, expected_session="aftermarket", max_candidates=after.max_symbols)
                            aftermarket_plan = plan_aftermarket_shards(symbols=tuple(str(row["symbol"]) for row in snapshot.candidates), credentials=load_kis_data_credentials(), pair_capacity_per_connection=int(after.pair_capacity_per_connection or 0), krx_streams=after.krx_streams, nxt_streams=after.nxt_streams)
                        except (CandidateFileError, KrxAlphaError) as exc:
                            logger.critical("[DAEMON] stage=aftermarket_plan status=FAIL reason=%s", str(exc))
                            aftermarket_plan = ()
                        aftermarket_plan_day = today
                    kst_time = now.astimezone(_KST).time()
                    due: list[MarketVenue] = []
                    if kst_time >= dt.time(15, 40):
                        due.append(MarketVenue.NXT)
                    if kst_time >= dt.time(16, 0):
                        due.append(MarketVenue.KRX)
                    for shard in aftermarket_plan:
                        if shard.venue in due:
                            supervisor_key = f"{shard.venue.value}:{shard.shard_index}"
                            if supervisor_key not in aftermarket_supervisors:
                                aftermarket_supervisors[supervisor_key] = ProcessSupervisor(
                                    cmd=_aftermarket_stream_cmd(today, paths, shard=shard),
                                    breaker=RestartCircuitBreaker(),
                                )
                    for sup in aftermarket_supervisors.values():
                        sup.ensure_running()
                sleep_sec = 10.0
        elif state == SessionState.POST_MARKET_EOD:
            if supervisor is not None:
                stop_result = supervisor.stop(timeout_s=15.0)
                logger.info("[DAEMON] stage=streamer_stop result=%s", stop_result, extra=EVENT)
                supervisor = None
                last_supervisor_result = None
                degraded_active = False
            if snapshot_supervisor is not None:
                snapshot_supervisor.stop(timeout_s=15.0)
                snapshot_supervisor = None
                last_snapshot_result = None
            # 15:40 EOD 유지보수 (정규화 및 오래된 저널 prune + L1 오프로드)
            # 같은 거래일에 60초마다 재실행하지 않도록 날짜당 1회만 시도한다
            ref_day = now.astimezone(_KST).date()
            if eod_attempted_for != ref_day:
                eod_attempted_for = ref_day
                for sup in aftermarket_supervisors.values(): sup.stop(timeout_s=15.0)  # noqa: E701 - 20:00 KIS 수집기 종료
                aftermarket_supervisors.clear()
                assert day is not None
                if day.status is TradingDayStatus.HOLIDAY:
                    housekeeping = _run_eod_housekeeping(cfg, paths, ref_day)
                    _check_backup(paths, ref_day)
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
                            for shard in aftermarket_plan
                        ]
                        if cfg.after_market_enabled
                        else []
                    )
                    aftermarket_blocked = cfg.after_market_enabled and not aftermarket_eod_ready(
                        manifests=aftermarket_manifests,
                        date=ref_day,
                        now=now,
                        expected_shards=tuple(aftermarket_plan),
                    )
                    if aftermarket_blocked: logger.critical("[DAEMON] stage=eod_maintenance status=DEGRADED reason=aftermarket_not_ready date=%s", ref_day.isoformat())  # noqa: E701
                    housekeeping = _run_eod_housekeeping(cfg, paths, ref_day)
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
                    backup_missing, backup_ok = _check_backup(paths, ref_day)
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
                            f"run_id={run_id}",
                            f"status={eod_status}",
                            f"deleted_partitions={housekeeping.deleted}",
                            f"uploaded={housekeeping.uploaded}",
                            f"purged={housekeeping.purged}",
                            f"reconciled={reconciled}",
                            f"aftermarket_ready={not aftermarket_blocked}",
                            f"backup_missing={len(backup_missing)}",
                            f"streamer_restarts={streamer_restarts}",
                            f"orchestration_attempts={orchestration_attempts}",
                        ]
                    )
                    send_digest(f"[krx-alpha] EOD {ref_day.isoformat()} {eod_status}", digest_body)
            sleep_sec = 60.0
        else:  # NIGHT_SLEEP
            sleep_sec = min(calc_sleep_seconds(now, sched.streamer_start), 1800.0)

        logger.debug("[DAEMON] sleeping for %.1f seconds...", sleep_sec)
        if shutdown is not None:
            if sleep_fn is not None:
                sleeper(sleep_sec)
            else:
                shutdown.wait(sleep_sec)
            if shutdown.is_set():
                _shutdown_children()
                return
        else:
            sleeper(sleep_sec)

        if max_cycles is not None and cycle >= max_cycles:
            logger.info("[DAEMON] reached max_cycles=%d, exiting gracefully.", max_cycles)
            break


def main() -> None:
    """Install SIGTERM/SIGINT handlers that set a shutdown event, then run the daemon."""
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
