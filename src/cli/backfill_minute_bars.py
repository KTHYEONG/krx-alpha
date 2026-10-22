"""backfill-minute-bars CLI (one-shot KIS 1m backfill for the replayed universe)."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import pathlib
import uuid
from zoneinfo import ZoneInfo

import polars as pl
import requests

from src.core.config import (
    CollectorSettings,
    MinuteBackfillSettings,
    TickTapeBackfillSettings,
)
from src.core.errors import KrxAlphaError, MissingCredentialsError
from src.core.paths import DataPaths
from src.core.session_anchors import resolve_session_anchors
from src.marketdata.minute_backfill import (
    MinuteTarget,
    plan_minute_targets,
    read_minute_ledger_done,
    run_minute_backfill,
)
from src.realtime.kis_sharding import load_kis_data_credentials

logger = logging.getLogger(__name__)

_KST = ZoneInfo("Asia/Seoul")
_RETENTION_DAYS = 364


def _now_kst() -> dt.datetime:
    return dt.datetime.now(_KST)


def add_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register ``backfill-minute-bars`` (one-shot; runs as the nightly MINUTE_BARS child)."""
    parser = subparsers.add_parser("backfill-minute-bars")
    parser.add_argument("--start", default=None, help="ISO start date; defaults to history_start")
    parser.add_argument("--end", default=None, help="ISO end date; defaults to last completed session")
    parser.add_argument("--deadline", default=None, help="ISO datetime deadline (KST); defaults to the next nightly window end")
    parser.add_argument("--max-calls", type=int, default=None, help="override the per-run call budget")
    parser.add_argument("--dry-run", action="store_true", help="print the plan summary without any vendor call")
    parser.add_argument("--data-root", type=pathlib.Path, default=None)
    parser.set_defaults(handler=run)


def _last_completed_session(now: dt.datetime, paths: DataPaths) -> dt.date:
    from src.marketdata.toss_calendar import load_trading_day_cache, trading_day_from_cache

    today = now.astimezone(_KST).date()
    cached = load_trading_day_cache(paths.calendar_cache)
    if cached is not None:
        derived = cached if cached.date == today else trading_day_from_cache(cached, today)
        if derived is not None:
            close = resolve_session_anchors(paths.session_calendar_dir, today).regular_close
            if derived.is_business_day and now.astimezone(_KST).time() >= close:
                return today
            return derived.previous_business_day
    raise ValueError("minute-history current calendar unavailable")


def _next_window_end(now: dt.datetime) -> dt.datetime:
    window = TickTapeBackfillSettings()
    end = window.window_end_kst
    candidate = dt.datetime.combine(now.astimezone(_KST).date(), end, tzinfo=_KST)
    if candidate <= now.astimezone(_KST):
        candidate += dt.timedelta(days=1)
    return candidate


def _parse_deadline(raw: str | None, now: dt.datetime) -> dt.datetime:
    if raw is None:
        return _next_window_end(now)
    moment = dt.datetime.fromisoformat(str(raw))
    if moment.tzinfo is None:
        return moment.replace(tzinfo=_KST)
    return moment.astimezone(_KST)


def run(args: argparse.Namespace) -> int:
    """Plan replayed-universe symbol-days and fetch their KIS 1m bars."""
    settings = MinuteBackfillSettings()
    if not settings.enabled and not getattr(args, "dry_run", False):
        logger.info("[DATA] stage=minute_backfill status=SKIP reason=disabled")
        return 2
    root = getattr(args, "data_root", None)
    paths = DataPaths(pathlib.Path(root)) if root is not None else CollectorSettings().paths
    now = _now_kst()
    try:
        upper = _last_completed_session(now, paths)
    except ValueError:
        logger.error("[DATA] stage=minute_backfill status=FAIL reason=calendar")
        return 4
    try:
        start = dt.date.fromisoformat(str(getattr(args, "start", None))) if getattr(args, "start", None) else settings.history_start
        end = dt.date.fromisoformat(str(getattr(args, "end", None))) if getattr(args, "end", None) else upper
    except ValueError:
        logger.error("[DATA] stage=minute_backfill status=FAIL reason=invalid_date")
        return 4
    if end > upper:
        end = upper
    floor = upper - dt.timedelta(days=_RETENTION_DAYS)
    if start < floor:
        logger.info(
            "[DATA] stage=minute_backfill status=clamped start=%s effective_start=%s",
            start.isoformat(), floor.isoformat(),
        )
        start = floor
    try:
        ledger_done = read_minute_ledger_done(paths.minute_history_ledger)
        targets: tuple[MinuteTarget, ...] = plan_minute_targets(paths=paths, start=start, end=end, ledger_done=ledger_done)
    except (OSError, ValueError, pl.exceptions.PolarsError) as exc:
        logger.error("[DATA] stage=minute_backfill status=FAIL reason=plan error=%s", type(exc).__name__)
        return 4
    if getattr(args, "dry_run", False):
        plan = {
            "symbols": len(targets),
            "symbol_days": sum(len(item.days) for item in targets),
        }
        print(json.dumps(plan))  # noqa: T201 - CLI result goes to stdout
        return 0
    if not targets:
        logger.info("[DATA] stage=minute_backfill status=OK reason=nothing_to_do")
        return 2
    try:
        credentials = load_kis_data_credentials()
        if not any(c.slot == str(settings.kis_data_slot) for c in credentials):
            raise MissingCredentialsError(f"no data credential for slot {settings.kis_data_slot}")
    except (KrxAlphaError, MissingCredentialsError) as exc:
        logger.error("[DATA] stage=minute_backfill status=FAIL reason=missing_credentials:%s", str(exc))
        return 4
    override = getattr(args, "max_calls", None)
    budget = settings.max_calls_per_run if override is None else int(override)
    if budget < 1:
        logger.error("[DATA] stage=minute_backfill status=FAIL reason=invalid_max_calls")
        return 4
    run_settings = settings.model_copy(update={"max_calls_per_run": budget})
    try:
        deadline = min(_parse_deadline(getattr(args, "deadline", None), now), _next_window_end(now))
    except ValueError:
        logger.error("[DATA] stage=minute_backfill status=FAIL reason=invalid_deadline")
        return 4
    run_id = uuid.uuid4().hex[:12]
    try:
        summary = run_minute_backfill(
            targets=targets,
            paths=paths,
            settings=run_settings,
            credentials=credentials,
            deadline=deadline,
            session=requests,
            now=_now_kst,
            run_id=run_id,
        )
    except (KrxAlphaError, OSError, ValueError) as exc:
        logger.error("[DATA] stage=minute_backfill status=FAIL reason=setup error=%s", type(exc).__name__)
        return 4
    if summary.status == "FAIL":
        return 4
    return 0
