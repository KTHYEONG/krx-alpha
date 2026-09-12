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
    TossCredentials,
    load_credentials,
)
from src.core.errors import MissingCredentialsError
from src.execution.contracts import KisApiError
from src.execution.kis_client import KisRestClient, RateLimiter
from src.marketdata.krx_bars import KrxBarsError
from src.marketdata.service import BarsRefreshResult as BarsRefreshResult
from src.marketdata.service import KisFallbackError as KisFallbackError
from src.marketdata.service import refresh_bars, refresh_bars_via_kis_fallback
from src.marketdata.toss_calendar import TossCalendarError, TradingDay, fetch_trading_day
from src.orchestration.eod import check_session_reconciliation, run_eod_maintenance, run_eod_offload
from src.orchestration.supervisor import ProcessSupervisor, RestartCircuitBreaker
from src.universe.ipc import read_candidates
from src.universe.service import UniversePlanResult as UniversePlanResult
from src.universe.service import plan_universe

logger = logging.getLogger(__name__)
_KST = ZoneInfo("Asia/Seoul")


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
    if trading_day is not None and decision_date != trading_day.previous_business_day:
        try:
            fallback = refresh_bars_via_kis_fallback(
                store_path=paths.bars_store,
                market_map_path=paths.market_map,
                target_date=trading_day.previous_business_day,
                kis_client=_build_kis_client(paths),
            )
        except (MissingCredentialsError, KisFallbackError, KisApiError) as exc:
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

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logger.info("[DAEMON] stage=start status=ONLINE timezone=Asia/Seoul")

    clock = now_fn if now_fn is not None else (lambda: dt.datetime.now(_KST))
    orchestrated_for: dt.date | None = None
    holiday_for: dt.date | None = None
    supervisor: ProcessSupervisor | None = None

    while True:
        cycle += 1
        now = clock()
        state = get_target_state(now, schedule=sched)
        logger.info("[DAEMON] cycle=%d now=%s state=%s", cycle, now.strftime("%Y-%m-%d %H:%M:%S"), state)

        if state == SessionState.WEEKEND_SLEEP:
            sleep_sec = 3600.0  # 주말엔 1시간씩 대기
        elif state == SessionState.PRE_MARKET_SLEEP:
            sleep_sec = min(calc_sleep_seconds(now, sched.streamer_start), 300.0)
        elif state in (SessionState.STREAMER_ACTIVE, SessionState.FULL_ACTIVE):
            today = now.date()
            if orchestrated_for != today:
                trading_day = resolve_trading_day(today)
                if trading_day is not None and not trading_day.is_business_day:
                    logger.info(
                        "[DAEMON] stage=session status=SKIP reason=market_holiday date=%s", today.isoformat()
                    )
                    orchestrated_for = today
                    holiday_for = today
                    supervisor = None
                else:
                    try:
                        ready = run_session_orchestration(today=today, settings=cfg, trading_day=trading_day)
                    except Exception as e:  # noqa: BLE001 - 오케스트레이션 실패가 데몬 전체를 죽이지 않도록 격리
                        logger.error("[DAEMON] stage=session status=FAIL reason=orchestration_error error=%s", str(e))
                        ready = False
                    orchestrated_for = today
                    holiday_for = None
                    if ready:
                        manifest_path = paths.manifest_path(today)
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
                            str(manifest_path),
                            "--candidates-path",
                            str(paths.candidates),
                            "--market-map",
                            str(paths.market_map),
                        ]
                        supervisor = ProcessSupervisor(cmd=cmd, breaker=RestartCircuitBreaker())
                    else:
                        supervisor = None
                        logger.critical("[DAEMON] stage=session status=FAIL reason=candidates_not_ready")
            if holiday_for == today:
                sleep_sec = 3600.0
            else:
                if supervisor is not None:
                    result = supervisor.ensure_running()
                    if result == "circuit_open":
                        logger.critical("[DAEMON] stage=streamer status=FAIL reason=circuit_open")
                sleep_sec = 10.0
        elif state == SessionState.POST_MARKET_EOD:
            if supervisor is not None:
                stop_result = supervisor.stop(timeout_s=15.0)
                logger.info("[DAEMON] stage=streamer_stop result=%s", stop_result)
            # 15:40 EOD 유지보수 (정규화 및 오래된 저널 prune + L1 오프로드)
            try:
                ref_day = now.astimezone(_KST).date()
                deleted = run_eod_maintenance(
                    paths.journal_root,
                    retain_days=cfg.journal_retain_days,
                    today=ref_day,
                    archive_root=paths.archive_root,
                    quarantine_root=paths.quarantine_root,
                )
                offload = run_eod_offload(
                    paths.archive_root, retain_days=cfg.archive_retain_days, reference_date=ref_day
                )
                reconciled = check_session_reconciliation(
                    bars_store=paths.bars_store, manifest_path=paths.manifest_path(ref_day), date=ref_day
                )
                if not reconciled:
                    logger.critical(
                        "[DAEMON] stage=eod_maintenance status=FAIL reason=session_data_gap date=%s",
                        ref_day.isoformat(),
                    )
                logger.info(
                    "[DAEMON] stage=eod_maintenance deleted_partitions=%d uploaded=%d purged=%d status=OK",
                    deleted,
                    offload["uploaded"],
                    offload["purged"],
                )
            except Exception as e:  # noqa: BLE001
                logger.error("[DAEMON] stage=eod_maintenance error=%s", str(e))
            sleep_sec = 60.0
        else:  # NIGHT_SLEEP
            sleep_sec = min(calc_sleep_seconds(now, sched.streamer_start), 1800.0)

        logger.info("[DAEMON] sleeping for %.1f seconds...", sleep_sec)
        sleeper(sleep_sec)

        if max_cycles is not None and cycle >= max_cycles:
            logger.info("[DAEMON] reached max_cycles=%d, exiting gracefully.", max_cycles)
            break


def main() -> None:
    run_collector_daemon()


if __name__ == "__main__":  # pragma: no cover
    main()
