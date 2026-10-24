"""Decoded per-trade table of one repaired LS regular day: live trades plus gap-fill rows."""

from __future__ import annotations

import bisect
import collections
import contextlib
import datetime as dt
import hashlib
import json
import logging
import os
import pathlib
import sqlite3
import tempfile
import time
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any, Literal

import pyarrow as pa
import pyarrow.parquet as pq

from src.core.paths import DataPaths
from src.storage.l0_audit import (
    _BATCH_BYTES,
    _TICK_STREAM,
    _decode_envelope,
    _decode_received_tick,
    _iter_line_batches,
    _tick_body,
)
from src.storage.layout import L0_JOURNAL_GLOB, l0_partition_key, l0_partition_relpath

logger = logging.getLogger(__name__)

DEFAULT_OVERFILL_TOLERANCE_RATIO = 0.02

_OUTPUT_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("chetime", pa.string(), nullable=False),
        pa.field("price", pa.int64(), nullable=False),
        pa.field("volume", pa.int64(), nullable=False),
        pa.field("cum", pa.int64(), nullable=False),
        pa.field("source", pa.string(), nullable=False),
        pa.field("interval_id", pa.int64(), nullable=True),
        pa.field("recv_wall_ns", pa.int64(), nullable=True),
    ]
)


@dataclass(frozen=True)
class RepairedTicksSummary:
    day: dt.date
    vendor: str
    live_rows: int
    filled_rows: int
    residual: int
    audit_trades_observed: int
    audit_sha256: str
    status: Literal["OK", "MISMATCH"]
    gapfill_report_sha256: str | None = None
    count_note: str | None = None
    audit_trades_received: int | None = None
    deduped_late_live: int = 0
    overfill_dropped: int = 0


@dataclass(frozen=True)
class _LiveTick:
    symbol: str
    chetime: str
    price: int
    volume: int
    cum: int
    recv_ns: int


def _iter_live_ticks(journal_root: pathlib.Path, vendor: str, day: dt.date) -> Iterator[_LiveTick]:
    key = l0_partition_key(vendor, "krx", "regular", _TICK_STREAM, day)
    if key is None:
        return
    files = sorted((journal_root / l0_partition_relpath(key)).glob(L0_JOURNAL_GLOB), key=lambda p: p.name)
    for path in files:
        for blob in _iter_line_batches(path, _BATCH_BYTES, [False]):
            for line in blob.splitlines():
                record = _decode_envelope(line)
                if record is None:
                    continue
                tick = _decode_received_tick(_tick_body(vendor, record.raw))
                if tick is None:
                    continue
                yield _LiveTick(
                    tick.symbol,
                    f"{tick.chetime:06d}",
                    tick.price,
                    tick.volume,
                    tick.cum,
                    record.recv_ns,
                )


def _read_audit(paths: DataPaths, day: dt.date, vendor: str) -> tuple[bytes, dict[str, Any]]:
    try:
        raw = paths.session_audit_path(day, vendor).read_bytes()
    except OSError as exc:
        raise ValueError(f"audit unreadable for {day.isoformat()} {vendor}") from exc
    try:
        doc = json.loads(raw)
    except ValueError as exc:
        raise ValueError(f"audit torn for {day.isoformat()} {vendor}") from exc
    if not isinstance(doc, dict) or doc.get("status") not in ("OK", "PARTIAL"):
        raise ValueError(f"audit unavailable for {day.isoformat()} {vendor}")
    totals = doc.get("totals")
    if not isinstance(totals, dict):
        raise ValueError(f"audit totals missing for {day.isoformat()} {vendor}")
    observed = totals.get("trades_observed")
    confirmed = totals.get("confirmed_missing")
    if (
        isinstance(observed, bool)
        or not isinstance(observed, int)
        or observed < 0
        or isinstance(confirmed, bool)
        or not isinstance(confirmed, int)
        or confirmed < 0
    ):
        raise ValueError(f"audit totals unusable for {day.isoformat()} {vendor}")
    if (doc.get("session_date"), doc.get("vendor"), doc.get("venue"), doc.get("session")) != (
        day.isoformat(), vendor, "krx", "regular",
    ):
        raise ValueError(f"audit identity mismatch for {day.isoformat()} {vendor}")
    return raw, doc


def _audit_received_count(audit: dict[str, Any]) -> tuple[int, str]:
    totals = audit["totals"]
    if "trades_received" in totals:
        count = totals["trades_received"]
        if type(count) is not int or count < 0:
            raise ValueError("audit trades_received must be a non-negative integer")
        return count, "trades_received"
    symbols = audit.get("symbols")
    if not isinstance(symbols, list):
        raise ValueError("legacy audit requires per-symbol receipt counts")
    count = 0
    for symbol in symbols:
        if not isinstance(symbol, dict):
            raise ValueError("legacy audit has unusable symbol counts")
        values: list[Any] = [symbol.get(name) for name in ("frames", "duplicates", "decode_fail")]
        if any(type(value) is not int or value < 0 for value in values) or values[2] != 0:
            raise ValueError("legacy audit receipt count is ambiguous; regenerate audit explicitly")
        count += values[0] + values[1]
    sources = audit.get("sources")
    if not isinstance(sources, list):
        raise ValueError("legacy audit requires tick source inventory")
    source_count = 0
    for source in sources:
        if not isinstance(source, dict):
            raise ValueError("legacy audit has unusable source inventory")
        if source.get("stream") != _TICK_STREAM:
            continue
        rows = source.get("rows")
        if type(rows) is not int or rows < 0 or source.get("truncated") is not False:
            raise ValueError("legacy tick source inventory is incomplete")
        source_count += rows
    if source_count != count:
        raise ValueError("legacy receipt count does not match tick source inventory")
    return count, "legacy_frames_plus_duplicates"


def _read_report(
    paths: DataPaths, day: dt.date, vendor: str, audit_raw: bytes, audit_mtime_ns: int, confirmed: int
) -> tuple[dict[str, Any] | None, str | None]:
    _, report_path = paths.gapfill_paths(day, vendor)
    try:
        body = report_path.read_bytes()
    except OSError as exc:
        if confirmed > 0:
            raise ValueError(f"gap-fill report missing for {day.isoformat()} {vendor}") from exc
        return None, None
    try:
        doc = json.loads(body)
    except ValueError as exc:
        raise ValueError(f"gap-fill report torn for {day.isoformat()} {vendor}") from exc
    if not isinstance(doc, dict) or not isinstance(doc.get("totals"), dict):
        raise ValueError(f"gap-fill report unusable for {day.isoformat()} {vendor}")
    totals = doc["totals"]
    residual = totals.get("residual")
    recovered = totals.get("recovered")
    for value in (residual, recovered):
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"gap-fill report totals unusable for {day.isoformat()} {vendor}")
    if not isinstance(doc.get("intervals"), list):
        raise ValueError(f"gap-fill report intervals unusable for {day.isoformat()} {vendor}")
    notes = doc.get("notes", {})
    if not isinstance(notes, dict):
        raise ValueError(f"gap-fill report notes unusable for {day.isoformat()} {vendor}")
    digest = hashlib.sha256(audit_raw).hexdigest()
    if "audit_sha256" in notes:
        if notes["audit_sha256"] != digest:
            raise ValueError(f"gap-fill report stale for {day.isoformat()} {vendor}")
    else:
        try:
            report_mtime_ns = report_path.stat().st_mtime_ns
        except OSError as exc:
            raise ValueError(f"gap-fill report unreadable for {day.isoformat()} {vendor}") from exc
        if report_mtime_ns < audit_mtime_ns:
            raise ValueError(f"gap-fill report stale for {day.isoformat()} {vendor}")
    return doc, hashlib.sha256(body).hexdigest()


@dataclass(frozen=True)
class _FilledRows:
    db: sqlite3.Connection
    height: int

    def iter_rows(self) -> Iterator[dict[str, Any]]:
        cursor = self.db.execute(
            "SELECT * FROM fills ORDER BY symbol, chetime, tape_seq, seq"
        )
        names = [column[0] for column in cursor.description]
        for row in cursor:
            yield dict(zip(names, row, strict=True))


def _read_gapfill_rows(
    paths: DataPaths, day: dt.date, vendor: str, recovered: int, db: sqlite3.Connection, batch_rows: int
) -> _FilledRows:
    names = ("symbol", "chetime", "price", "volume", "tape_seq", "interval_id", "cum_prev", "cum_next", "provenance")
    db.execute(
        "CREATE TABLE fills (seq INTEGER PRIMARY KEY, symbol TEXT, chetime TEXT, price INTEGER, volume INTEGER, "
        "tape_seq INTEGER, interval_id INTEGER, cum_prev INTEGER, cum_next INTEGER, provenance TEXT)"
    )
    rows_path, _ = paths.gapfill_paths(day, vendor)
    if recovered == 0 and not rows_path.exists():
        return _FilledRows(db, 0)
    try:
        with pq.ParquetFile(rows_path) as frame:
            if set(names) - set(frame.schema_arrow.names):
                raise ValueError("gap-fill rows lack columns")
            for batch in frame.iter_batches(batch_size=batch_rows, columns=list(names)):
                db.executemany(
                    "INSERT INTO fills (symbol, chetime, price, volume, tape_seq, interval_id, cum_prev, cum_next, provenance) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (tuple(row[name] for name in names) for row in batch.to_pylist()),
                )
            height = frame.metadata.num_rows
    except (OSError, ValueError, pa.ArrowException, sqlite3.Error) as exc:
        raise ValueError(f"gap-fill rows unreadable for {day.isoformat()} {vendor}") from exc
    return _FilledRows(db, height)


def _mismatch(
    day: dt.date,
    vendor: str,
    live_rows: int,
    filled_rows: int,
    residual: int,
    observed: int,
    audit_sha256: str,
    report_sha: str | None,
    count_note: str | None = None,
) -> RepairedTicksSummary:
    return RepairedTicksSummary(
        day, vendor, live_rows, filled_rows, residual, observed, audit_sha256, "MISMATCH", report_sha, count_note
    )


def build_repaired_ticks(
    *,
    paths: DataPaths,
    journal_root: pathlib.Path,
    day: dt.date,
    vendor: str,
    batch_rows: int = 250_000,
    overfill_tolerance_ratio: float = DEFAULT_OVERFILL_TOLERANCE_RATIO,
) -> RepairedTicksSummary:
    """Write the repaired-ticks parquet and its summary for one repaired or clean day."""
    if vendor != "ls":
        raise ValueError(f"unsupported repair vendor: {vendor!r}")
    paths.work_root.mkdir(parents=True, exist_ok=True)
    with (
        tempfile.TemporaryDirectory(prefix="repair-merge-", dir=paths.work_root) as work,
        contextlib.closing(sqlite3.connect(pathlib.Path(work) / "index.sqlite")) as db,
    ):
        db.execute("PRAGMA cache_size = -2048")
        db.execute("PRAGMA temp_store = FILE")
        db.execute("CREATE TABLE counters (symbol TEXT, cum INTEGER, PRIMARY KEY (symbol, cum)) WITHOUT ROWID")
        db.execute("CREATE TABLE late (symbol TEXT, cum INTEGER, chetime INTEGER, price INTEGER, volume INTEGER)")
        db.execute("CREATE INDEX late_symbol_cum ON late (symbol, cum)")
        return _build_repaired_ticks(
            paths=paths, journal_root=journal_root, day=day, vendor=vendor, batch_rows=batch_rows,
            overfill_tolerance_ratio=overfill_tolerance_ratio, db=db,
        )


def _build_repaired_ticks(
    *, paths: DataPaths, journal_root: pathlib.Path, day: dt.date, vendor: str, batch_rows: int,
    overfill_tolerance_ratio: float, db: sqlite3.Connection,
) -> RepairedTicksSummary:
    if isinstance(batch_rows, bool) or not isinstance(batch_rows, int) or batch_rows <= 0:
        raise ValueError(f"batch_rows must be a positive int: {batch_rows!r}")
    audit_raw, audit = _read_audit(paths, day, vendor)
    try:
        audit_mtime_ns = paths.session_audit_path(day, vendor).stat().st_mtime_ns
    except OSError as exc:
        raise ValueError(f"audit unreadable for {day.isoformat()} {vendor}") from exc
    audit_sha256 = hashlib.sha256(audit_raw).hexdigest()
    observed = int(audit["totals"]["trades_observed"])
    received, count_basis = _audit_received_count(audit)
    confirmed = int(audit["totals"]["confirmed_missing"])
    report, report_sha = _read_report(paths, day, vendor, audit_raw, audit_mtime_ns, confirmed)
    recovered = int(report["totals"]["recovered"]) if report is not None else 0
    residual = int(report["totals"]["residual"]) if report is not None else 0
    filled = _read_gapfill_rows(paths, day, vendor, recovered, db, batch_rows)

    if filled.height != recovered:
        return _mismatch(day, vendor, 0, filled.height, residual, observed, audit_sha256, report_sha)

    target = paths.repaired_ticks_path(day, vendor)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.parent / f".{target.name}.{os.getpid()}.tmp"
    live_rows = 0
    columns: dict[str, list[Any]] = {name: [] for name in _OUTPUT_SCHEMA.names}

    batch = batch_rows

    def flush(writer: pq.ParquetWriter) -> None:
        table = pa.table(
            {name: pa.array(values, type=_OUTPUT_SCHEMA.field(name).type) for name, values in columns.items()},
            schema=_OUTPUT_SCHEMA,
        )
        writer.write_table(table, row_group_size=batch)
        for values in columns.values():
            del values[:]

    def emit(
        symbol: str,
        chetime: str,
        price: int,
        volume: int,
        cum: int,
        source: str,
        interval_id: int | None,
        recv_ns: int | None,
    ) -> None:
        columns["symbol"].append(symbol)
        columns["chetime"].append(chetime)
        columns["price"].append(price)
        columns["volume"].append(volume)
        columns["cum"].append(cum)
        columns["source"].append(source)
        columns["interval_id"].append(interval_id)
        columns["recv_wall_ns"].append(recv_ns)

    gap_ranges: dict[str, list[tuple[int, int]]] = {}
    for symbol, lo, hi in db.execute("SELECT DISTINCT symbol, cum_prev, cum_next FROM fills "
        "WHERE cum_prev IS NOT NULL AND cum_next IS NOT NULL ORDER BY symbol, cum_prev"):
        gap_ranges.setdefault(symbol, []).append((lo, hi))
    gap_starts = {symbol: [lo for lo, _ in spans] for symbol, spans in gap_ranges.items()}

    def inside_gap(symbol: str, cum: int) -> bool:
        spans = gap_ranges.get(symbol)
        if spans is None:
            return False
        index = bisect.bisect_left(gap_starts[symbol], cum) - 1
        return index >= 0 and spans[index][0] < cum < spans[index][1]

    writer: pq.ParquetWriter | None = None
    outcome: RepairedTicksSummary | None = None
    deduped = 0
    overfill_dropped = 0
    try:
        try:
            writer = pq.ParquetWriter(tmp, _OUTPUT_SCHEMA, compression="zstd")
            for tick in _iter_live_ticks(pathlib.Path(journal_root), vendor, day):
                inserted_live = db.execute("INSERT OR IGNORE INTO counters VALUES (?, ?)", (tick.symbol, tick.cum))
                if inserted_live.rowcount and inside_gap(tick.symbol, tick.cum):
                    db.execute(
                        "INSERT INTO late VALUES (?, ?, ?, ?, ?)",
                        (tick.symbol, tick.cum, int(tick.chetime), tick.price, tick.volume),
                    )
                emit(tick.symbol, tick.chetime, tick.price, tick.volume, tick.cum, "live", None, tick.recv_ns)
                live_rows += 1
                if len(columns["symbol"]) >= batch_rows:
                    flush(writer)
            if live_rows != received:
                outcome = _mismatch(day, vendor, live_rows, filled.height, residual, observed, audit_sha256, report_sha)
            else:
                filled_rows = 0
                next_slot: dict[tuple[str, int], int] = {}
                late_pool: dict[tuple[str, int], collections.Counter[tuple[int, int, int]]] = {}
                for row in filled.iter_rows():
                    try:
                        interval_id = int(row["interval_id"])
                        cum_prev = int(row["cum_prev"])
                        cum_next = int(row["cum_next"])
                        symbol = str(row["symbol"])
                        price = int(row["price"])
                        volume = int(row["volume"])
                        provenance = str(row["provenance"])
                        chetime = str(row["chetime"])
                    except (TypeError, ValueError, KeyError) as exc:
                        raise ValueError(f"gap-fill rows unusable for {day.isoformat()} {vendor}") from exc
                    key = (symbol, interval_id)
                    pool = late_pool.get(key)
                    if pool is None:
                        pool = collections.Counter(
                            db.execute(
                                "SELECT chetime, price, volume FROM late WHERE symbol = ? AND cum > ? AND cum < ?",
                                (symbol, cum_prev, cum_next),
                            ).fetchall()
                        )
                        late_pool[key] = pool
                    twin = (int(chetime), price, volume)
                    if pool[twin] > 0:
                        pool[twin] -= 1
                        deduped += 1
                        continue
                    slot = next_slot.get(key, cum_prev + 1)
                    while slot < cum_next:
                        inserted = db.execute("INSERT OR IGNORE INTO counters VALUES (?, ?)", (symbol, slot))
                        if inserted.rowcount:
                            break
                        slot += 1
                    if not cum_prev < slot < cum_next:
                        overfill_dropped += 1
                        continue
                    next_slot[key] = slot + 1
                    emit(symbol, chetime, price, volume, slot, provenance, interval_id, None)
                    filled_rows += 1
                    if len(columns["symbol"]) >= batch_rows:
                        flush(writer)
                if outcome is None and overfill_dropped > overfill_tolerance_ratio * filled.height:
                    outcome = _mismatch(
                        day, vendor, live_rows, filled.height, residual, observed, audit_sha256, report_sha
                    )
                if outcome is None and columns["symbol"]:
                    flush(writer)
        finally:
            if writer is not None:
                writer.close()
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    if outcome is not None:
        tmp.unlink(missing_ok=True)
        return outcome
    count_note: str | None = None
    if confirmed != filled_rows + residual:
        count_note = "gapfill_counts_intervals"
    try:
        os.replace(tmp, target)
    except OSError:
        tmp.unlink(missing_ok=True)
        raise
    summary_path = paths.repair_summary_path(day, vendor)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "day": day.isoformat(),
        "vendor": vendor,
        "live_rows": live_rows,
        "filled_rows": filled_rows,
        "residual": residual,
        "audit_trades_observed": observed,
        "audit_trades_received": received,
        "live_count_basis": count_basis,
        "audit_sha256": audit_sha256,
        "gapfill_report_sha256": report_sha,
        "status": "OK",
        "count_note": count_note,
        "deduped_late_live": deduped,
        "overfill_dropped": overfill_dropped,
        "generated_at": time.time_ns(),
    }
    summary_tmp = summary_path.parent / f".{summary_path.name}.{os.getpid()}.tmp"
    try:
        summary_tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        os.replace(summary_tmp, summary_path)
    except OSError:
        summary_tmp.unlink(missing_ok=True)
        with contextlib.suppress(OSError):
            target.unlink(missing_ok=True)
        raise
    logger.info(
        "[DATA] stage=repair_merge day=%s vendor=%s live=%d filled=%d deduped=%d overfill_dropped=%d residual=%d status=OK",
        day.isoformat(),
        vendor,
        live_rows,
        filled_rows,
        deduped,
        overfill_dropped,
        residual,
    )
    return RepairedTicksSummary(
        day,
        vendor,
        live_rows,
        filled_rows,
        residual,
        observed,
        audit_sha256,
        "OK",
        report_sha,
        count_note,
        received,
        deduped,
        overfill_dropped,
    )


def repaired_ticks_valid(paths: DataPaths, day: dt.date, vendor: str) -> bool:
    """True when the parquet and summary exist, the summary is OK, and its audit hash matches."""
    try:
        if not paths.repaired_ticks_path(day, vendor).is_file():
            return False
        doc = json.loads(paths.repair_summary_path(day, vendor).read_bytes())
        if not isinstance(doc, dict) or doc.get("status") != "OK":
            return False
        if doc.get("day") != day.isoformat() or doc.get("vendor") != vendor:
            return False
        counts: list[Any] = [doc.get(name) for name in ("live_rows", "filled_rows", "audit_trades_received")]
        if any(type(value) is not int or value < 0 for value in counts) or counts[0] != counts[2]:
            return False
        metadata = pq.read_metadata(paths.repaired_ticks_path(day, vendor))
        if metadata.num_rows != counts[0] + counts[1]:
            return False
        audit_raw = paths.session_audit_path(day, vendor).read_bytes()
    except (OSError, ValueError, pa.ArrowException):
        return False
    return doc.get("audit_sha256") == hashlib.sha256(audit_raw).hexdigest()
