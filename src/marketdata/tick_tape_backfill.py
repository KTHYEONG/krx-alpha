"""Kiwoom research-tape backfill, isolated from canonical market-data partitions."""

from __future__ import annotations

import datetime as dt
import fcntl
import json
import logging
import os
import pathlib
import re
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import polars as pl

from src.brokers.kis.rate import HostPacedRateLimiter, host_state_path
from src.core.config import KisTokenSettings, KiwoomCredentials, TickTapeBackfillSettings
from src.core.paths import DataPaths
from src.marketdata.kiwoom_ticks import (
    KiwoomAuthRejected,
    KiwoomThrottled,
    KiwoomTickTapeError,
    TapeBoundaryError,
    TapeTick,
    TapeWalk,
    TickPage,
    fetch_tick_page,
    issue_kiwoom_token,
    walk_symbol_tape,
)
from src.marketdata.partitioned_store import PartitionedStoreError, scan_month_partitions
from src.marketdata.toss_calendar import load_trading_day_cache, trading_day_from_cache

logger = logging.getLogger(__name__)

TAPE_VENDOR: str = "kiwoom"
_LEDGER_OK_STATUSES: frozenset[str] = frozenset({"complete", "vendor_empty"})
# Rows staged on disk between durable checkpoints: bounds the work a killed run can lose, not memory.
_FLUSH_ROW_LIMIT: int = 2_000_000

TAPE_PARTITION_SCHEMA: dict[str, Any] = {
    "date": pl.Date,
    "symbol": pl.String,
    "tape_seq": pl.Int64,
    "ts_hms": pl.Int64,
    "price": pl.Int64,
    "volume": pl.Int64,
    "vendor": pl.String,
    "run_id": pl.String,
}


@dataclass(frozen=True)
class TickTapeTarget:
    symbol: str
    days: tuple[dt.date, ...]
    cost_hint: float


@dataclass(frozen=True)
class TickTapeRunSummary:
    status: str
    reason: str | None
    run_id: str
    symbols: int
    symbol_days: int
    pages: int
    edge_partial: int
    failed: int
    oversize: int = 0


def tape_calendar_sessions(paths: DataPaths, through: dt.date) -> tuple[dt.date, ...]:
    """Read historically observed business dates without inventing weekday sessions."""
    try:
        return tuple(
            scan_month_partitions(paths.bars_daily_dir, max_date=through)
            .select("date").unique().sort("date").collect().get_column("date").to_list()
        )
    except PartitionedStoreError:
        return ()


def _prev_business_day(day: dt.date, paths: DataPaths) -> dt.date:
    cached = load_trading_day_cache(paths.calendar_cache)
    derived = trading_day_from_cache(cached, day) if cached is not None else None
    if derived is not None:
        return derived.previous_business_day
    sessions = tape_calendar_sessions(paths, day - dt.timedelta(days=1))
    if not sessions:
        raise ValueError("tick-tape business calendar unavailable")
    return sessions[-1]


def _read_universe_symbols(path: pathlib.Path) -> frozenset[str] | None:
    try:
        frame = pl.read_parquet(path)
    except (OSError, ValueError, pl.exceptions.PolarsError):
        return None
    if "symbol" not in frame.columns:
        return None
    return frozenset(str(code) for code in frame.get_column("symbol").to_list() if str(code))


def _read_manifest_accepted(path: pathlib.Path) -> frozenset[str]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return frozenset()
    acks = raw.get("subscription_acks", []) if isinstance(raw, Mapping) else []
    if not isinstance(acks, list):
        return frozenset()
    return frozenset(
        str(entry.get("symbol"))
        for entry in acks
        if isinstance(entry, Mapping) and entry.get("accepted") is True and str(entry.get("symbol"))
    )


def _read_audit_gap_symbols(path: pathlib.Path) -> frozenset[str]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return frozenset()
    symbols = raw.get("symbols", []) if isinstance(raw, Mapping) else []
    if not isinstance(symbols, list):
        return frozenset()
    gaps: set[str] = set()
    for entry in symbols:
        if not isinstance(entry, Mapping):
            continue
        try:
            missing = int(entry.get("confirmed_missing", 0) or 0)
        except (TypeError, ValueError):
            continue
        if missing > 0 and str(entry.get("symbol")):
            gaps.add(str(entry.get("symbol")))
    return frozenset(gaps)


def _bars_value_sum(
    bars_daily_dir: pathlib.Path, symbol: str, start: dt.date, end: dt.date
) -> float:
    try:
        frame = (
            scan_month_partitions(pathlib.Path(bars_daily_dir), min_date=start, max_date=end)
            .filter((pl.col("symbol") == symbol) & (pl.col("date") >= start) & (pl.col("date") <= end))
            .select("trade_value_100m")
            .collect(engine="streaming")
        )
    except (PartitionedStoreError, OSError, pl.exceptions.PolarsError):
        return 0.0
    except Exception as exc:
        logger.warning("[DATA] stage=tick_tape_plan status=DEGRADED reason=bars_unreadable error=%s", type(exc).__name__)
        return 0.0
    if frame.height == 0 or "trade_value_100m" not in frame.columns:
        return 0.0
    return float(frame.get_column("trade_value_100m").sum() or 0.0)


def plan_tick_tape_targets(
    *,
    sessions: Sequence[dt.date],
    paths: DataPaths,
    ledger_done: frozenset[tuple[str, dt.date]],
) -> tuple[TickTapeTarget, ...]:
    """Return per-symbol targets for symbol-days inside the vendor window that the live LS feed did not fully capture and the ledger has not settled."""
    ordered = sorted(set(sessions))
    if not ordered:
        return ()
    last_session = ordered[-1]
    by_symbol: dict[str, list[dt.date]] = {}
    skip_logged: set[dt.date] = set()
    for day in ordered:
        decision = _prev_business_day(day, paths)
        live = _read_universe_symbols(paths.universe_out(decision))
        # The tiered research universe is the recovery target; days planned before it went live exist only as replay.
        replay = _read_universe_symbols(paths.universe_replay_out(decision))
        universe = None if live is None and replay is None else (live or frozenset()) | (replay or frozenset())
        if universe is None:
            if day not in skip_logged:
                skip_logged.add(day)
                logger.warning("[DATA] stage=tick_tape_plan status=SKIP reason=no_universe day=%s", day.isoformat())
            continue
        if not universe:
            continue
        accepted = _read_manifest_accepted(paths.manifest_path(day))
        gaps = _read_audit_gap_symbols(paths.session_audit_path(day, "ls"))
        for symbol in universe:
            if (symbol, day) in ledger_done:
                continue
            if symbol in accepted and symbol not in gaps:
                continue
            by_symbol.setdefault(symbol, []).append(day)
    targets: list[TickTapeTarget] = []
    for symbol, days in by_symbol.items():
        ascending = tuple(sorted(days))
        hint = _bars_value_sum(paths.bars_daily_dir, symbol, ascending[0], last_session)
        targets.append(TickTapeTarget(symbol=symbol, days=ascending, cost_hint=hint))
    # The vendor drops the oldest session every day, so expiry outranks cost; cost then maximizes days per page.
    targets.sort(key=lambda item: (item.days[0], item.cost_hint, item.symbol))
    return tuple(targets)


def read_ledger_done(path: pathlib.Path) -> frozenset[tuple[str, dt.date]]:
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
            raise ValueError("corrupt tick-tape ledger") from None
        if not isinstance(record, Mapping):
            continue
        if str(record.get("status")) not in _LEDGER_OK_STATUSES:
            continue
        try:
            done.add((str(record["symbol"]), dt.date.fromisoformat(str(record["date"]))))
        except (KeyError, TypeError, ValueError):
            continue
    return frozenset(done)


class _RunAuthError(KiwoomTickTapeError):
    """Second auth rejection inside one run; aborts the whole run."""


class _SymbolThrottleError(KiwoomTickTapeError):
    """Persistent throttle on one symbol; aborts only that symbol."""


def _write_day_chunk(path: pathlib.Path, symbol: str, day: dt.date, ticks: Sequence[TapeTick], label: str) -> None:
    frame = (
        pl.DataFrame(
            {
                "tape_seq": pl.int_range(len(ticks), eager=True),
                "ts_hms": [tick.ts_hms for tick in ticks],
                "price": [tick.price for tick in ticks],
                "volume": [tick.volume for tick in ticks],
            }
        )
        .with_columns(
            pl.lit(day).alias("date"),
            pl.lit(symbol).alias("symbol"),
            pl.lit(TAPE_VENDOR).alias("vendor"),
            pl.lit(label).alias("run_id"),
        )
        .select([pl.col(name).cast(dtype) for name, dtype in TAPE_PARTITION_SCHEMA.items()])
    )
    target = pathlib.Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.parent / f".{target.name}.tmp"
    frame.write_parquet(tmp, compression="zstd")
    os.replace(tmp, target)


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


class _PersistenceError(Exception):
    """Staging or checkpoint I/O failed; aborts the run without settling the in-flight symbol."""


def run_tick_tape_backfill(
    *,
    targets: Sequence[TickTapeTarget],
    paths: DataPaths,
    settings: TickTapeBackfillSettings,
    credentials: KiwoomCredentials,
    deadline: dt.datetime,
    session: Any,
    now: Callable[[], dt.datetime],
    run_id: str,
    should_stop: Callable[[], bool] = lambda: False,
) -> TickTapeRunSummary:
    """Fetch targets under the host-shared pacer, page budget and deadline; persist partitions then ledger lines at durable checkpoints; never raises for per-symbol failures."""
    if not re.fullmatch(r"[A-Za-z0-9_-]+", run_id):
        raise ValueError("invalid tick-tape run_id")
    if deadline.tzinfo is None or now().tzinfo is None:
        raise ValueError("deadline and now must be timezone-aware")
    if not settings.enabled:
        return TickTapeRunSummary("OK", "disabled", run_id, 0, 0, 0, 0, 0)
    run_marker = paths.tick_tape_ledger.parent / "runs" / run_id
    run_marker.mkdir(parents=True, exist_ok=False)
    limiter = HostPacedRateLimiter(
        host_state_path(KisTokenSettings().token_cache_dir, "kiwoom", credentials.kiwoom_app_key, "ka10079"),
        settings.rate_per_s,
    )
    token: str | None = None
    auth_retried = False
    pages_used = 0
    started = 0
    committed_days = 0
    edge_days = 0
    failed = 0
    oversize = 0
    stop_requested = False
    ledger_queue: list[dict[str, Any]] = []
    staged: dict[dt.date, list[pathlib.Path]] = {}
    staged_rows = 0
    checkpoint_no = 0
    staging_root = paths.work_root / "tick_tape" / run_id

    def _label() -> str:
        return run_id if checkpoint_no == 0 else f"{run_id}-{checkpoint_no}"

    def _boundary() -> None:
        nonlocal stop_requested
        if should_stop():
            stop_requested = True
            raise TapeBoundaryError("tick-tape stop requested")
        current = now()
        wall_time = current.astimezone(dt.timezone(dt.timedelta(hours=9))).time()
        start, end = settings.window_start_kst, settings.window_end_kst
        in_window = start <= wall_time < end if start < end else wall_time >= start or wall_time < end
        if pages_used >= settings.max_pages_per_run or current >= deadline or not in_window:
            raise TapeBoundaryError("tick-tape request boundary")

    def _issue() -> str:
        _boundary()
        try:
            return issue_kiwoom_token(
                app_key=credentials.kiwoom_app_key,
                secret_key=credentials.kiwoom_secret_key,
                session=session,
                timeout_s=min(settings.request_timeout_s, (deadline - now()).total_seconds()),
            )
        except KiwoomTickTapeError as exc:
            raise _RunAuthError("kiwoom token issuance failed") from exc

    def _fetch(symbol: str, next_key: str | None) -> TickPage:
        nonlocal token, auth_retried, pages_used
        throttles = 0
        while True:
            _boundary()
            if token is None:
                token = _issue()
            limiter.acquire()
            _boundary()
            pages_used += 1
            if pages_used % settings.progress_every_pages == 0:
                logger.info(
                    "[DATA] stage=tick_tape_progress run_id=%s symbol=%s run_pages=%d staged_rows=%d committed_days=%d",
                    run_id,
                    symbol,
                    pages_used,
                    staged_rows,
                    committed_days,
                )
            try:
                return fetch_tick_page(
                    symbol,
                    access_token=token,
                    app_key=credentials.kiwoom_app_key,
                    next_key=next_key,
                    session=session,
                    timeout_s=min(settings.request_timeout_s, (deadline - now()).total_seconds()),
                )
            except KiwoomThrottled as exc:
                throttles += 1
                if throttles > 3:
                    raise _SymbolThrottleError(f"kiwoom tape throttled for {symbol}") from exc
                time.sleep(1.2)
            except KiwoomAuthRejected as exc:
                if auth_retried:
                    raise _RunAuthError("kiwoom tape auth rejected twice") from exc
                auth_retried = True
                token = _issue()

    def _checkpoint() -> None:
        nonlocal staged, ledger_queue, staged_rows, checkpoint_no, committed_days
        if not staged and not ledger_queue:
            return
        label = _label()
        for day, parts in sorted(staged.items()):
            target_path = paths.tick_tape_partition(day, label)
            target_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = target_path.with_suffix(".tmp")
            pl.scan_parquet(parts).sink_parquet(tmp, compression="zstd")
            _publish_partition(tmp, target_path)
        for parts in staged.values():
            for part in parts:
                part.unlink()
        _append_ledger_lines(paths.tick_tape_ledger, ledger_queue)
        committed_days += sum(1 for record in ledger_queue if record["status"] in _LEDGER_OK_STATUSES)
        staged = {}
        ledger_queue = []
        staged_rows = 0
        checkpoint_no += 1

    status = "OK"
    reason: str | None = None
    for target in targets:
        try:
            _boundary()
        except TapeBoundaryError:
            break
        remaining = settings.max_pages_per_run - pages_used
        started += 1
        label = _label()
        symbol_chunks: list[tuple[dt.date, pathlib.Path, int]] = []

        def _fetch_page(key: str | None, _symbol: str = target.symbol) -> TickPage:
            return _fetch(_symbol, key)

        def _stage_day(
            day: dt.date,
            ticks: tuple[TapeTick, ...],
            _symbol: str = target.symbol,
            _label: str = label,
            _chunks: list[tuple[dt.date, pathlib.Path, int]] = symbol_chunks,
        ) -> None:
            chunk = staging_root / day.isoformat() / f"{_symbol}.parquet"
            try:
                _write_day_chunk(chunk, _symbol, day, ticks, _label)
            except (OSError, pl.exceptions.PolarsError) as exc:
                raise _PersistenceError(type(exc).__name__) from exc
            _chunks.append((day, chunk, len(ticks)))

        def _discard_symbol(_chunks: list[tuple[dt.date, pathlib.Path, int]] = symbol_chunks) -> None:
            for _day, chunk, _rows in _chunks:
                chunk.unlink(missing_ok=True)

        try:
            walk: TapeWalk = walk_symbol_tape(
                target.symbol,
                wanted_days=frozenset(target.days),
                fetch_page=_fetch_page,
                max_pages=remaining,
                on_day_complete=_stage_day,
                max_ticks_per_day=settings.max_ticks_per_day,
            )
        except _RunAuthError:
            _discard_symbol()
            logger.error("[DATA] stage=tick_tape_backfill status=FAIL reason=auth run_id=%s", run_id)
            status = "FAIL"
            reason = "auth"
            failed += 1
            break
        except _PersistenceError as exc:
            _discard_symbol()
            logger.warning("[DATA] stage=tick_tape status=FAIL reason=%s", exc)
            failed += 1
            status, reason = "FAIL", "persistence"
            break
        except Exception as exc:
            _discard_symbol()
            logger.warning(
                "[DATA] stage=tick_tape symbol=%s status=FAIL reason=%s", target.symbol, type(exc).__name__
            )
            failed += 1
            continue
        for day, chunk, rows in symbol_chunks:
            staged.setdefault(day, []).append(chunk)
            staged_rows += rows
            ledger_queue.append(
                {"date": day.isoformat(), "symbol": target.symbol, "status": "complete",
                 "rows": rows, "run_id": label, "vendor": TAPE_VENDOR}
            )
        ledger_queue.extend(
            {"date": day.isoformat(), "symbol": target.symbol, "status": "vendor_empty",
             "rows": 0, "run_id": label, "vendor": TAPE_VENDOR}
            for day in sorted(walk.vendor_empty)
        )
        ledger_queue.extend(
            {"date": day.isoformat(), "symbol": target.symbol, "status": "edge_partial",
             "rows": 0, "run_id": label, "vendor": TAPE_VENDOR}
            for day in sorted(walk.edge_partial & frozenset(target.days))
        )
        edge_days += len(walk.edge_partial)
        logger.info(
            "[DATA] stage=tick_tape symbol=%s days=%d pages=%d termination=%s",
            target.symbol,
            len(symbol_chunks),
            walk.pages,
            walk.termination,
        )
        if walk.termination == "oversize":
            oversize += 1
            logger.warning(
                "[DATA] stage=tick_tape symbol=%s status=OVERSIZE day=%s cap=%d",
                target.symbol,
                walk.oversize_day.isoformat() if walk.oversize_day is not None else "?",
                settings.max_ticks_per_day,
            )
        if staged_rows > _FLUSH_ROW_LIMIT:
            try:
                _checkpoint()
            except (OSError, pl.exceptions.PolarsError) as exc:
                logger.warning("[DATA] stage=tick_tape status=FAIL reason=%s", type(exc).__name__)
                failed += 1
                status, reason = "FAIL", "persistence"
                break
    if reason != "persistence":
        try:
            _checkpoint()
        except (OSError, pl.exceptions.PolarsError) as exc:
            logger.warning("[DATA] stage=tick_tape_backfill status=FAIL reason=%s run_id=%s", type(exc).__name__, run_id)
            failed += 1
            if status == "OK":
                status = "FAIL"
                reason = type(exc).__name__
    if status == "OK" and stop_requested:
        reason = "stop_requested"
    logger.info(
        "[DATA] stage=tick_tape_backfill status=%s run_id=%s symbols=%d symbol_days=%d pages=%d edge_partial=%d failed=%d oversize=%d",
        status,
        run_id,
        started,
        committed_days,
        pages_used,
        edge_days,
        failed,
        oversize,
    )
    return TickTapeRunSummary(
        status=status,
        reason=reason,
        run_id=run_id,
        symbols=started,
        symbol_days=committed_days,
        pages=pages_used,
        edge_partial=edge_days,
        failed=failed,
        oversize=oversize,
    )
