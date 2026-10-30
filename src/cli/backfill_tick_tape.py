"""backfill-tick-tape CLI (one-shot Kiwoom tick-tape backfill for LS-uncovered symbol-days)."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import pathlib
import signal
import threading
import uuid
from zoneinfo import ZoneInfo

import requests

from src.core.config import (
    CollectorSettings,
    KiwoomCredentials,
    RepairSettings,
    TickTapeBackfillSettings,
    load_credentials,
)
from src.core.errors import KrxAlphaError, MissingCredentialsError
from src.core.paths import DataPaths
from src.marketdata.tick_tape_backfill import (
    pending_repair_symbol_days,
    plan_tick_tape_targets,
    read_ledger_done,
    run_tick_tape_backfill,
    tape_calendar_sessions,
)

logger = logging.getLogger(__name__)

_KST = ZoneInfo("Asia/Seoul")


def _now_kst() -> dt.datetime:
    return dt.datetime.now(_KST)


def add_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register ``backfill-tick-tape`` (one-shot; never part of the daemon)."""
    parser = subparsers.add_parser("backfill-tick-tape")
    parser.add_argument("--deadline", default=None, help="ISO datetime deadline (KST); defaults to the next window end")
    parser.add_argument("--max-pages", type=int, default=None, help="override the per-run page budget")
    parser.add_argument("--dry-run", action="store_true", help="print the plan summary without any vendor call")
    parser.add_argument("--data-root", type=pathlib.Path, default=None)
    parser.add_argument("--only-symbols", default=None, help="comma-separated symbols; restrict to planned targets")
    parser.set_defaults(handler=run)


def _last_completed_session(today: dt.date, paths: DataPaths) -> dt.date:
    """Return `today` when the calendar cache marks it a business day and its EOD digest exists, else its previous business day; raises ValueError when the current calendar is unavailable."""
    from src.marketdata.toss_calendar import load_trading_day_cache, trading_day_from_cache

    cached = load_trading_day_cache(paths.calendar_cache)
    if cached is not None:
        derived = cached if cached.date == today else trading_day_from_cache(cached, today)
        if derived is not None:
            if derived.is_business_day and paths.eod_digest_path(today).exists():
                return today
            return derived.previous_business_day
    raise ValueError("tick-tape current calendar unavailable")


_CALENDAR_LOOKBACK_DAYS = 10


def _calendar_previous(day: dt.date, paths: DataPaths) -> dt.date | None:
    from src.marketdata.toss_calendar import load_trading_day_cache, trading_day_from_cache

    cached = load_trading_day_cache(paths.calendar_cache)
    derived = trading_day_from_cache(cached, day) if cached is not None else None
    if derived is not None:
        return derived.previous_business_day
    for offset in range(1, _CALENDAR_LOOKBACK_DAYS + 1):
        candidate = day - dt.timedelta(days=offset)
        if (paths.session_calendar_dir / f"dt={candidate.isoformat()}.json").is_file():
            return candidate
    return None


def resolve_backfill_sessions(paths: DataPaths, window_sessions: int, today: dt.date) -> tuple[dt.date, ...]:
    """Return the last ``window_sessions`` business sessions (ascending) through the last completed session."""
    upper = _last_completed_session(today, paths)
    sessions = tape_calendar_sessions(paths, upper)
    if sessions and sessions[-1] != upper and sessions[-1] == _calendar_previous(upper, paths):
        # The latest session's daily bar lands at the next pre-session refresh, so a post-midnight run lags by one session.
        sessions = (*sessions, upper)
    if not sessions or sessions[-1] != upper or len(sessions) < window_sessions + 1:
        raise ValueError("tick-tape historical calendar incomplete")
    return sessions[-window_sessions:]


def _next_window_end(now: dt.datetime, settings: TickTapeBackfillSettings) -> dt.datetime:
    end = settings.window_end_kst
    candidate = dt.datetime.combine(now.astimezone(_KST).date(), end, tzinfo=_KST)
    if candidate <= now.astimezone(_KST):
        candidate += dt.timedelta(days=1)
    return candidate


def _parse_deadline(raw: str | None, settings: TickTapeBackfillSettings, now: dt.datetime) -> dt.datetime:
    if raw is None:
        return _next_window_end(now, settings)
    moment = dt.datetime.fromisoformat(str(raw))
    if moment.tzinfo is None:
        return moment.replace(tzinfo=_KST)
    return moment.astimezone(_KST)


def _parse_only_symbols(raw: str | None) -> frozenset[str] | None:
    if raw is None:
        return None
    names = {part.strip().zfill(6) for part in str(raw).split(",") if part.strip()}
    return frozenset(names)


def run(args: argparse.Namespace) -> int:
    """Plan LS-uncovered symbol-days inside the vendor window and fetch their Kiwoom tapes."""
    settings = TickTapeBackfillSettings()
    if not settings.enabled and not getattr(args, "dry_run", False):
        logger.info("[DATA] stage=tick_tape_backfill status=SKIP reason=disabled")
        return 2
    root = getattr(args, "data_root", None)
    paths = DataPaths(pathlib.Path(root)) if root is not None else CollectorSettings().paths
    try:
        credentials = load_credentials(KiwoomCredentials)
    except MissingCredentialsError as exc:
        logger.error("[DATA] stage=tick_tape_backfill status=FAIL reason=missing_credentials:%s", str(exc))
        return 4
    now = _now_kst()
    try:
        sessions = resolve_backfill_sessions(paths, settings.vendor_window_sessions, now.date())
        ledger_done = read_ledger_done(paths.tick_tape_ledger)
        try:
            repair_cfg = RepairSettings()
            horizon = int(CollectorSettings().journal_retain_days) + int(repair_cfg.hold_max_extra_days) + 1
            repair_days = pending_repair_symbol_days(
                paths, sessions[-horizon:], settings=repair_cfg, tape_done=ledger_done
            )
        except Exception as exc:  # noqa: BLE001 - order degrades, never aborts
            logger.warning("[DATA] stage=tick_tape_plan status=DEGRADED reason=repair_order error=%s", type(exc).__name__)
            repair_days = frozenset()
        targets = plan_tick_tape_targets(
            sessions=sessions, paths=paths, ledger_done=ledger_done, repair_symbol_days=repair_days
        )
    except (OSError, ValueError) as exc:
        logger.error("[DATA] stage=tick_tape_backfill status=FAIL reason=calendar_or_ledger error=%s", type(exc).__name__)
        return 4
    only = _parse_only_symbols(getattr(args, "only_symbols", None))
    if only is not None:
        planned = {item.symbol for item in targets}
        for symbol in sorted(only - planned):
            logger.info("[DATA] stage=tick_tape_backfill status=INFO reason=symbol_not_planned symbol=%s", symbol)
        targets = tuple(item for item in targets if item.symbol in only)
        if not targets:
            logger.info("[DATA] stage=tick_tape_backfill status=OK reason=nothing_to_do")
            return 2
    if getattr(args, "dry_run", False):
        plan = {
            "symbols": len(targets),
            "symbol_days": sum(len(item.days) for item in targets),
            "cost_hint": sum(item.cost_hint for item in targets),
        }
        print(json.dumps(plan))  # noqa: T201 - CLI result goes to stdout
        return 0
    if not targets:
        logger.info("[DATA] stage=tick_tape_backfill status=OK reason=nothing_to_do")
        return 2
    wall_time = now.time()
    start, end = settings.window_start_kst, settings.window_end_kst
    in_window = start <= wall_time < end if start < end else wall_time >= start or wall_time < end
    if not in_window:
        logger.info("[DATA] stage=tick_tape_backfill status=SKIP reason=outside_window")
        return 2
    override = getattr(args, "max_pages", None)
    budget = settings.max_pages_per_run if override is None else int(override)
    if budget < 1:
        logger.error("[DATA] stage=tick_tape_backfill status=FAIL reason=invalid_max_pages")
        return 4
    run_settings = settings.model_copy(update={"max_pages_per_run": budget})
    try:
        deadline = min(_parse_deadline(getattr(args, "deadline", None), settings, now), _next_window_end(now, settings))
    except ValueError:
        logger.error("[DATA] stage=tick_tape_backfill status=FAIL reason=invalid_deadline")
        return 4
    run_id = uuid.uuid4().hex[:12]
    stop_event = threading.Event()

    def _request_stop(signum: int, frame: object) -> None:
        stop_event.set()

    prev_sigterm = signal.getsignal(signal.SIGTERM)
    prev_sigint = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)
    try:
        summary = run_tick_tape_backfill(
            targets=targets,
            paths=paths,
            settings=run_settings,
            credentials=credentials,
            deadline=deadline,
            session=requests,
            now=_now_kst,
            run_id=run_id,
            should_stop=stop_event.is_set,
        )
    except (KrxAlphaError, OSError, ValueError) as exc:
        logger.error("[DATA] stage=tick_tape_backfill status=FAIL reason=setup error=%s", type(exc).__name__)
        return 4
    finally:
        signal.signal(signal.SIGTERM, prev_sigterm)
        signal.signal(signal.SIGINT, prev_sigint)
    if summary.status == "FAIL":
        return 4
    return 0
