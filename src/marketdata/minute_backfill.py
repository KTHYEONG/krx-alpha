"""KIS 1-minute bar backfill for the replayed universe, isolated under L1 history."""

from __future__ import annotations

import datetime as dt
import fcntl
import json
import logging
import os
import pathlib
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl

from src.brokers.kis.auth import KisAppAuth
from src.brokers.kis.data import KisMinuteBar
from src.brokers.kis.stack import build_kis_rest_stack
from src.core.config import KisTokenSettings, MinuteBackfillSettings
from src.core.errors import MissingCredentialsError
from src.core.paths import DataPaths
from src.core.session_anchors import resolve_session_anchors
from src.execution.contracts import KisApiError
from src.marketdata.kis_minute_history import (
    MinuteBackfillBoundaryError,
    walk_kis_minute_day,
)
from src.marketdata.partitioned_store import PartitionedStoreError, scan_month_partitions
from src.realtime.kis_sharding import KisDataCredential

logger = logging.getLogger(__name__)

MINUTE_VENDOR: str = "kis"
MINUTE_VENUE: str = "KRX"
_LEDGER_OK_STATUSES: frozenset[str] = frozenset({"complete", "vendor_empty"})
_FLUSH_ROW_LIMIT: int = 2_000_000
_KST = ZoneInfo("Asia/Seoul")


def _now_kst() -> dt.datetime:
    return dt.datetime.now(_KST)

MINUTE_PARTITION_SCHEMA: dict[str, Any] = {
    "date": pl.Date,
    "symbol": pl.String,
    "ts_hms": pl.Int64,
    "open": pl.Int64,
    "high": pl.Int64,
    "low": pl.Int64,
    "close": pl.Int64,
    "volume": pl.Int64,
    "vendor": pl.String,
    "venue": pl.String,
    "universe_source": pl.String,
    "run_id": pl.String,
}


class _RunAuthError(KisApiError):
    """Token issuance failed; stop the run without discarding certified days."""


@dataclass(frozen=True)
class MinuteTarget:
    symbol: str
    days: tuple[dt.date, ...]
    universe_sources: Mapping[dt.date, str]


@dataclass(frozen=True)
class MinuteRunSummary:
    status: str
    reason: str | None
    run_id: str
    symbols: int
    symbol_days: int
    calls: int
    vendor_empty: int
    failed: int
    discarded_offhours: int


def _business_sessions(paths: DataPaths, start: dt.date, end: dt.date) -> tuple[dt.date, ...]:
    try:
        dates = (
            scan_month_partitions(paths.bars_daily_dir, min_date=start, max_date=end)
            .select("date")
            .unique()
            .sort("date")
            .collect(engine="streaming")
            .get_column("date")
            .to_list()
        )
    except PartitionedStoreError:
        return ()
    return tuple(sorted(day for day in dates if isinstance(day, dt.date) and start <= day <= end))


def _prev_business_day(day: dt.date, sessions: Sequence[dt.date]) -> dt.date | None:
    prev: dt.date | None = None
    for session in sessions:
        if session < day and (prev is None or session > prev):
            prev = session
    return prev


def _read_universe_symbols(path: pathlib.Path) -> frozenset[str] | None:
    try:
        frame = pl.read_parquet(path)
    except (OSError, ValueError, pl.exceptions.PolarsError):
        return None
    if "symbol" not in frame.columns:
        return None
    return frozenset(str(code) for code in frame.get_column("symbol").to_list() if str(code))


def plan_minute_targets(
    *,
    paths: DataPaths,
    start: dt.date,
    end: dt.date,
    ledger_done: frozenset[tuple[str, dt.date]],
) -> tuple[MinuteTarget, ...]:
    """Per-symbol target days from the universe that applied to each session (replay before the live log existed, live log after)."""
    if end < start:
        return ()
    current = _now_kst()
    close = resolve_session_anchors(paths.session_calendar_dir, current.date()).regular_close
    upper = current.date() if current.time() >= close else current.date() - dt.timedelta(days=1)
    sessions = _business_sessions(paths, dt.date.min, min(end, upper))
    if not sessions:
        return ()
    by_symbol: dict[str, dict[dt.date, str]] = {}
    for day in sessions:
        if day < start:
            continue
        decision = _prev_business_day(day, sessions)
        if decision is None:
            continue
        live_path = paths.universe_out(decision)
        symbols: frozenset[str] | None = None
        source = ""
        if live_path.exists():
            symbols = _read_universe_symbols(live_path)
            source = "live"
        else:
            replay_path = paths.universe_replay_out(decision)
            if replay_path.exists():
                symbols = _read_universe_symbols(replay_path)
                source = "replay"
        if symbols is None:
            continue
        for symbol in symbols:
            if (symbol, day) in ledger_done:
                continue
            by_symbol.setdefault(symbol, {})[day] = source
    targets = [
        MinuteTarget(symbol=symbol, days=tuple(sorted(days)), universe_sources=dict(days))
        for symbol, days in sorted(by_symbol.items())
    ]
    return tuple(targets)


def read_minute_ledger_done(path: pathlib.Path) -> frozenset[tuple[str, dt.date]]:
    """Return (symbol, date) pairs with status complete or vendor_empty; tolerates a torn final line."""
    try:
        contents = pathlib.Path(path).read_bytes()
    except FileNotFoundError:
        return frozenset()
    done: set[tuple[str, dt.date]] = set()
    lines = contents.splitlines(keepends=True)
    for idx, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            record = json.loads(stripped)
        except (ValueError, UnicodeDecodeError):
            if idx == len(lines) - 1 and not line.endswith(b"\n"):
                continue
            raise ValueError("corrupt minute-history ledger") from None
        if not isinstance(record, Mapping):
            continue
        if str(record.get("status")) not in _LEDGER_OK_STATUSES:
            continue
        try:
            done.add((str(record["symbol"]), dt.date.fromisoformat(str(record["date"]))))
        except (KeyError, TypeError, ValueError):
            continue
    return frozenset(done)


def _write_partition(path: pathlib.Path, rows: Sequence[Mapping[str, Any]]) -> None:
    frame = pl.DataFrame(
        list(rows),
        schema=dict(MINUTE_PARTITION_SCHEMA),
    ).with_columns([pl.col(name).cast(dtype) for name, dtype in MINUTE_PARTITION_SCHEMA.items()])
    target = pathlib.Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.parent / f".{target.name}.tmp"
    frame.write_parquet(tmp, compression="zstd")
    with open(tmp, "rb+") as handle:
        handle.flush()
        os.fsync(handle.fileno())
    _publish_partition(tmp, target)


def _publish_partition(tmp: pathlib.Path, target: pathlib.Path) -> None:
    with tmp.open("rb") as handle:
        os.fsync(handle.fileno())
    os.link(tmp, target)
    tmp.unlink()
    _sync_directory(target.parent)


def _sync_directory(path: pathlib.Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _append_ledger_lines(path: pathlib.Path, lines: Sequence[Mapping[str, Any]]) -> None:
    if not lines:
        return
    target = pathlib.Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with open(target, "a+b") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        handle.seek(0)
        contents = handle.read()
        if contents and not contents.endswith(b"\n"):
            tail = contents.rsplit(b"\n", 1)[-1]
            try:
                json.loads(tail)
            except (ValueError, UnicodeDecodeError):
                handle.truncate(len(contents) - len(tail))
            else:
                handle.write(b"\n")
        for record in lines:
            handle.write((json.dumps(record, ensure_ascii=False) + "\n").encode())
        handle.flush()
        os.fsync(handle.fileno())
        _sync_directory(target.parent)


def run_minute_backfill(
    *,
    targets: Sequence[MinuteTarget],
    paths: DataPaths,
    settings: MinuteBackfillSettings,
    credentials: Sequence[KisDataCredential],
    deadline: dt.datetime,
    session: Any,
    now: Callable[[], dt.datetime],
    run_id: str,
) -> MinuteRunSummary:
    """Fetch one KIS symbol-day at a time under the host KIS pacer; write partitions before ledger lines; never raises per-symbol."""
    if not re.fullmatch(r"[A-Za-z0-9_-]+", run_id):
        raise ValueError("invalid minute-history run_id")
    if deadline.tzinfo is None or now().tzinfo is None:
        raise ValueError("deadline and now must be timezone-aware")
    if not settings.enabled:
        return MinuteRunSummary("OK", "disabled", run_id, 0, 0, 0, 0, 0, 0)
    if not targets or now() >= deadline:
        return MinuteRunSummary("OK", "nothing_to_do", run_id, 0, 0, 0, 0, 0, 0)
    run_marker = paths.minute_history_ledger.parent / "runs" / run_id
    run_marker.mkdir(parents=True, exist_ok=False)
    cred = next((c for c in credentials if c.slot == str(settings.kis_data_slot)), None)
    if cred is None:
        raise MissingCredentialsError(f"no data credential for slot {settings.kis_data_slot}")
    calls_used = 0
    boundary_stopped = False
    token_transport_failed = False
    started = 0
    settled_days = 0
    empty_days = 0
    failed = 0
    discarded_total = 0
    buffers: dict[dt.date, list[dict[str, Any]]] = {}
    buffered_rows = 0
    ledger_queue: list[dict[str, Any]] = []
    staged: dict[dt.date, list[pathlib.Path]] = {}
    staging_root = paths.work_root / "minute_history" / run_id

    def _boundary() -> dt.datetime:
        nonlocal boundary_stopped
        current = now().astimezone(_KST)
        if calls_used >= settings.max_calls_per_run or current >= deadline or dt.time(8, 10) <= current.time() < dt.time(22):
            boundary_stopped = True
            raise MinuteBackfillBoundaryError("EXHAUSTED", "minute-history request boundary")
        return current

    def _timeout() -> float:
        current = _boundary()
        return min(settings.request_timeout_s, (deadline - current).total_seconds())

    def _get(url: str, **kwargs: Any) -> Any:
        nonlocal calls_used
        kwargs["timeout"] = _timeout()
        calls_used += 1
        return session.get(url, **kwargs)

    def _post(url: str, **kwargs: Any) -> Any:
        nonlocal token_transport_failed
        kwargs["timeout"] = _timeout()
        try:
            return session.post(url, **kwargs)
        except Exception as exc:
            token_transport_failed = True
            raise _RunAuthError("TOKEN", "token issuance transport failed") from exc

    token_settings = KisTokenSettings()
    stack = build_kis_rest_stack(
        auth=KisAppAuth(cred.app_key, cred.app_secret),
        cache_dir=token_settings.token_cache_dir,
        session=SimpleNamespace(get=_get, post=_post),
        now=now,
        rate_per_s=settings.rate_per_s,
        max_lead_s=1.0,
        timeout_s=settings.request_timeout_s,
        allow_issue=token_settings.allow_issue,
    )

    def _flush(*, final: bool = False) -> None:
        nonlocal buffers, buffered_rows
        for day in sorted(buffers):
            parts = staged.setdefault(day, [])
            chunk = staging_root / day.isoformat() / f"chunk-{len(parts)}.parquet"
            _write_partition(chunk, buffers[day])
            parts.append(chunk)
        buffers = {}
        buffered_rows = 0
        if final:
            for day, parts in sorted(staged.items()):
                target_path = paths.minute_history_partition(day, run_id)
                target_path.parent.mkdir(parents=True, exist_ok=True)
                tmp = target_path.with_suffix(".tmp")
                pl.scan_parquet(parts).sink_parquet(tmp, compression="zstd")
                _publish_partition(tmp, target_path)
            for parts in staged.values():
                for part in parts:
                    part.unlink()

    status = "OK"
    reason: str | None = None
    for target in targets:
        try:
            _boundary()
        except MinuteBackfillBoundaryError:
            break
        started += 1
        for day in sorted(target.days):
            try:
                _boundary()
            except MinuteBackfillBoundaryError:
                break
            remaining = settings.max_calls_per_run - calls_used
            calls_before = calls_used

            def _fetch(before_hms: int, _symbol: str = target.symbol, _day: dt.date = day) -> tuple[KisMinuteBar, ...]:
                _boundary()
                try:
                    return stack.data.get_historical_minute_bars(_symbol, day=_day, before_hms=before_hms)
                except KisApiError as exc:
                    if boundary_stopped:
                        raise MinuteBackfillBoundaryError("EXHAUSTED", "minute-history request boundary") from exc
                    if token_transport_failed or exc.msg_cd.startswith("TOKEN") or exc.msg_cd in {"EGW00121", "EGW00123", "EGW00133"}:
                        raise _RunAuthError(exc.msg_cd, "minute history authentication failed") from exc
                    raise

            try:
                walked = walk_kis_minute_day(target.symbol, day, fetch=_fetch, max_calls=remaining)
            except _RunAuthError:
                status, reason = "FAIL", "auth"
                failed += 1
                break
            except Exception as exc:
                logger.warning(
                    "[DATA] stage=minute_history symbol=%s status=FAIL reason=%s", target.symbol, type(exc).__name__
                )
                failed += 1
                continue
            discarded_total += walked.discarded_offhours
            if walked.status == "incomplete":
                continue
            if walked.status == "vendor_empty":
                ledger_queue.append(
                    {"date": day.isoformat(), "symbol": target.symbol, "status": "vendor_empty",
                     "rows": 0, "run_id": run_id, "vendor": MINUTE_VENDOR,
                     "universe_source": str(target.universe_sources.get(day, "")),
                     "reason": walked.reason}
                )
                empty_days += 1
                logger.info(
                    "[DATA] stage=minute_history symbol=%s days=%d calls=%d",
                    target.symbol, 0, calls_used - calls_before,
                )
                continue
            rows = [
                {
                    "date": day,
                    "symbol": target.symbol,
                    "ts_hms": bar.ts_hms,
                    "open": bar.open,
                    "high": bar.high,
                    "low": bar.low,
                    "close": bar.close,
                    "volume": bar.volume,
                    "vendor": MINUTE_VENDOR,
                    "venue": MINUTE_VENUE,
                    "universe_source": str(target.universe_sources.get(day, "")),
                    "run_id": run_id,
                }
                for bar in walked.bars
            ]
            buffers.setdefault(day, []).extend(rows)
            buffered_rows += len(rows)
            ledger_queue.append(
                {"date": day.isoformat(), "symbol": target.symbol, "status": "complete",
                 "rows": len(rows), "run_id": run_id, "vendor": MINUTE_VENDOR,
                 "universe_source": str(target.universe_sources.get(day, ""))}
            )
            settled_days += 1
            logger.info(
                "[DATA] stage=minute_history symbol=%s days=%d calls=%d",
                target.symbol, 1, calls_used - calls_before,
            )
            if buffered_rows > _FLUSH_ROW_LIMIT:
                try:
                    _flush()
                except (OSError, pl.exceptions.PolarsError) as exc:
                    logger.warning("[DATA] stage=minute_history status=FAIL reason=%s", type(exc).__name__)
                    failed += 1
                    status, reason = "FAIL", "persistence"
                    break
        if status == "FAIL":
            break
    try:
        if reason != "persistence":
            _flush(final=True)
            _append_ledger_lines(paths.minute_history_ledger, ledger_queue)
    except (OSError, pl.exceptions.PolarsError) as exc:
        logger.warning("[DATA] stage=minute_backfill status=FAIL reason=%s run_id=%s", type(exc).__name__, run_id)
        failed += 1
        if status == "OK":
            status = "FAIL"
            reason = type(exc).__name__
    if status == "FAIL" and reason != "auth":
        settled_days = 0
        empty_days = 0
    logger.info(
        "[DATA] stage=minute_backfill status=%s run_id=%s symbols=%d symbol_days=%d calls=%d vendor_empty=%d failed=%d discarded_offhours=%d",
        status, run_id, started, settled_days, calls_used, empty_days, failed, discarded_total,
    )
    return MinuteRunSummary(
        status=status, reason=reason, run_id=run_id, symbols=started,
        symbol_days=settled_days, calls=calls_used, vendor_empty=empty_days,
        failed=failed, discarded_offhours=discarded_total,
    )
