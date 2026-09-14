"""Collector 24/7 daemon 오케스트레이션 (service 직접 호출, CLI 역방향 의존 제거)."""

from __future__ import annotations

import datetime as dt
import logging
import pathlib
import sys
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl
import requests

from src.core.calendar import SessionState, calc_sleep_seconds, get_target_state
from src.core.config import (
    CollectorSettings,
    DataPaths,
    ExecutionSettings,
    KisCredentials,
    KrxCredentials,
    ObservabilitySettings,
    TossCredentials,
    load_credentials,
)
from src.core.errors import KrxAlphaError, MissingCredentialsError
from src.core.observability import EVENT, configure_logging
from src.execution.contracts import KisApiError
from src.execution.kis_client import KisRestClient, RateLimiter
from src.marketdata.krx_bars import KrxBarsError
from src.marketdata.service import BarsRefreshResult as BarsRefreshResult
from src.marketdata.service import KisFallbackError as KisFallbackError
from src.marketdata.service import refresh_bars, refresh_bars_via_kis_fallback
from src.marketdata.toss_calendar import (
    TossCalendarError,
    TradingDay,
    fetch_trading_day,
    load_trading_day_cache,
    save_trading_day_cache,
    trading_day_from_cache,
)
from src.orchestration.eod import check_session_reconciliation, run_eod_maintenance, run_eod_offload
from src.orchestration.supervisor import ProcessSupervisor, RestartCircuitBreaker
from src.universe.ipc import CandidateFileError, read_candidates
from src.universe.service import UniversePlanResult as UniversePlanResult
from src.universe.service import plan_universe

logger = logging.getLogger(__name__)
_KST = ZoneInfo("Asia/Seoul")
HEARTBEAT_SUMMARY_S: float = 600.0


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
    return KisRestClient(
        creds=creds,
        session=requests,
        token_cache_path=paths.kis_token_cache,
        limiter=RateLimiter(execution.rest_rate_per_s),
        now=lambda: dt.datetime.now(_KST),
        timeout_s=execution.request_timeout_s,
    )


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
    plan_universe(
        bars_path=paths.bars_store,
        decision_date=decision_date,
        out_path=paths.universe_out(decision_date),
        slot_budget=settings.universe_slot_budget,
        candidates_path=paths.candidates,
    )
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
) -> None:
    import time

    sleeper = sleep_fn if sleep_fn is not None else time.sleep
    cfg = settings if settings is not None else CollectorSettings()
    sched = cfg.schedule
    paths = cfg.paths
    cycle = 0

    run_id = configure_logging("daemon", log_dir=paths.logs_dir if ObservabilitySettings().persistent_logs else None)
    logger.info("[DAEMON] stage=start status=ONLINE timezone=Asia/Seoul run_id=%s", run_id, extra=EVENT)

    clock = now_fn if now_fn is not None else (lambda: dt.datetime.now(_KST))
    orchestrated_for: dt.date | None = None
    holiday_for: dt.date | None = None
    eod_attempted_for: dt.date | None = None
    supervisor: ProcessSupervisor | None = None
    prev_state: SessionState | None = None
    last_summary: dt.datetime | None = None
    streamer_restarts = 0
    last_supervisor_result: str | None = None
    orchestration_day: dt.date | None = None
    next_orchestration_at: dt.datetime | None = None
    orchestration_attempts = 0
    degraded_active = False

    while True:
        cycle += 1
        now = clock()
        state = get_target_state(now, schedule=sched)
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
        elif state in (SessionState.STREAMER_ACTIVE, SessionState.FULL_ACTIVE):
            today = now.date()
            if today != orchestration_day:
                # EOD를 거치지 못한 전일 스트리머가 남아 있으면 전일 session-date로 재기동되므로 먼저 정리한다
                if supervisor is not None:
                    stale_stop = supervisor.stop(timeout_s=15.0)
                    logger.warning("[DAEMON] stage=streamer status=STOP_STALE_DAY stop_result=%s", stale_stop)
                    supervisor = None
                    last_supervisor_result = None
                orchestration_day = today
                next_orchestration_at = None
                orchestration_attempts = 0
                degraded_active = False
            if (
                orchestrated_for != today
                and holiday_for != today
                and (next_orchestration_at is None or now >= next_orchestration_at)
            ):
                trading_day = _resolve_trading_day_with_cache(today, paths.calendar_cache)
                if trading_day is not None and not trading_day.is_business_day:
                    logger.info(
                        "[DAEMON] stage=session status=SKIP reason=market_holiday date=%s",
                        today.isoformat(),
                        extra=EVENT,
                    )
                    holiday_for = today
                    supervisor = None
                    last_supervisor_result = None
                else:
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
                                degraded_active = True
                                last_supervisor_result = None
                                logger.critical(
                                    "[DAEMON] stage=streamer status=DEGRADED reason=orchestration_failed candidates_rev=%d",
                                    rev,
                                )
            if holiday_for == today:
                sleep_sec = 3600.0
            else:
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
                sleep_sec = 10.0
        elif state == SessionState.POST_MARKET_EOD:
            if supervisor is not None:
                stop_result = supervisor.stop(timeout_s=15.0)
                logger.info("[DAEMON] stage=streamer_stop result=%s", stop_result, extra=EVENT)
                supervisor = None
                last_supervisor_result = None
                degraded_active = False
            # 15:40 EOD 유지보수 (정규화 및 오래된 저널 prune + L1 오프로드)
            # 같은 거래일에 60초마다 재실행하지 않도록 날짜당 1회만 시도한다
            ref_day = now.astimezone(_KST).date()
            if eod_attempted_for != ref_day:
                eod_attempted_for = ref_day
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
                    )
                except (KrxAlphaError, OSError) as e:
                    maintenance_ok = False
                    logger.critical("[DAEMON] stage=eod_maintenance status=FAIL reason=maintenance_error error=%s", str(e))
                offload = {"uploaded": 0, "skipped": 0, "failed": 0, "purged": 0}
                offload_ok = True
                try:
                    offload = run_eod_offload(
                        paths.archive_root, retain_days=cfg.archive_retain_days, reference_date=ref_day
                    )
                except Exception as e:  # noqa: BLE001
                    offload_ok = False
                    logger.error("[DAEMON] stage=eod_maintenance error=%s", str(e), exc_info=True)
                reconcile_ok = True
                try:
                    reconciled = check_session_reconciliation(
                        bars_store=paths.bars_store, manifest_path=paths.manifest_path(ref_day), date=ref_day
                    )
                    if not reconciled:
                        logger.critical(
                            "[DAEMON] stage=eod_maintenance status=FAIL reason=session_data_gap date=%s",
                            ref_day.isoformat(),
                        )
                except (OSError, pl.exceptions.PolarsError) as e:
                    reconcile_ok = False
                    reconciled = False
                    logger.critical(
                        "[DAEMON] stage=eod_reconciliation status=FAIL error=%s", str(e), exc_info=True
                    )
                logger.info(
                    "[DAEMON] stage=eod_maintenance deleted_partitions=%d uploaded=%d purged=%d status=%s",
                    deleted,
                    offload["uploaded"],
                    offload["purged"],
                    "OK" if (maintenance_ok and offload_ok and reconcile_ok) else "DEGRADED",
                    extra=EVENT,
                )
            sleep_sec = 60.0
        else:  # NIGHT_SLEEP
            sleep_sec = min(calc_sleep_seconds(now, sched.streamer_start), 1800.0)

        logger.debug("[DAEMON] sleeping for %.1f seconds...", sleep_sec)
        sleeper(sleep_sec)

        if max_cycles is not None and cycle >= max_cycles:
            logger.info("[DAEMON] reached max_cycles=%d, exiting gracefully.", max_cycles)
            break


def main() -> None:
    run_collector_daemon()


if __name__ == "__main__":  # pragma: no cover
    main()
