"""Canonical per-minute source selection across vendors, written as a derived L1 dataset.

L0 and vendor L1 partitions are only read; the selection is an additive view built nightly.
"""

from __future__ import annotations

import collections
import dataclasses
import datetime as dt
import json
import logging
import os
import pathlib
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Final, Literal
from zoneinfo import ZoneInfo

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from src.core.paths import DataPaths
from src.core.session_anchors import resolve_session_anchors
from src.storage.layout import l0_partition_key, l0_partition_relpath, l1_relpath_for_l0_partition
from src.storage.quality_db import decode_db_quotes, decode_db_ticks
from src.storage.quality_kis import decode_kis_quotes, decode_kis_ticks
from src.storage.quality_ls import decode_ls_quotes, decode_ls_ticks

logger = logging.getLogger(__name__)

_KST = ZoneInfo("Asia/Seoul")
_NS_PER_S: Final[int] = 1_000_000_000
_MINUTE_NS: Final[int] = 60 * _NS_PER_S
_CHUNK_ROWS: Final[int] = 20_000
_MINUTE_FORMAT: Final[str] = "%Y-%m-%dT%H:%M"
_SESSION: Final[str] = "regular"
_STATUS_OK: Final[str] = "OK"
_STATUS_NO_SECONDARY: Final[str] = "NO_SECONDARY"
_KIND_TICK: Final[Literal["tick"]] = "tick"
_KIND_BOOK: Final[Literal["book"]] = "book"

Decoder = Callable[[pl.DataFrame], pl.DataFrame]
_DECODERS: Final[Mapping[str, tuple[Decoder, Decoder]]] = {
    "ls": (decode_ls_ticks, decode_ls_quotes),
    "kis": (decode_kis_ticks, decode_kis_quotes),
    "db": (decode_db_ticks, decode_db_quotes),
}


@dataclass(frozen=True, slots=True)
class SelectionRow:
    symbol: str
    venue: str
    session: str
    kind: Literal["tick", "book"]
    minute_kst: str
    selected_vendor: str
    primary_vendor: str
    reason: str
    frames_primary: int
    frames_selected: int


@dataclass(frozen=True, slots=True)
class SelectionSpec:
    """Selection thresholds; `primary_by_venue` names the vendor preferred per venue (KRX regular: ls, NXT regular: kis)."""

    min_frames: int = 1
    min_advantage_frames: int = 3
    primary_by_venue: tuple[tuple[str, str], ...] = (("krx", "ls"), ("nxt", "kis"))
    # Vendors whose trade stream is conflated (per-trade detail lost); used for a symbol-minute only when no lossless source has frames.
    conflated_trade_vendors: frozenset[str] = frozenset({"db"})

    def primary_for(self, venue: str) -> str | None:
        return dict(self.primary_by_venue).get(venue)


@dataclass(frozen=True, slots=True)
class _Route:
    venue: str
    tick_stream: str
    book_stream: str


_ROUTES: Final[tuple[_Route, ...]] = (
    _Route("krx", "H0STCNT0", "H0STASP0"),
    _Route("nxt", "H0NXCNT0", "H0NXASP0"),
)


@dataclass(frozen=True, slots=True)
class SelectionSummary:
    status: str
    rows: int
    reasons: Mapping[str, int]
    symbols_affected: int
    vendors: Mapping[str, Mapping[str, int]]


def _minute_wall(minute_kst: str) -> float:
    return dt.datetime.strptime(minute_kst, _MINUTE_FORMAT).replace(tzinfo=_KST).timestamp()


def select_minutes(
    *,
    venue: str,
    session: str,
    kind: Literal["tick", "book"],
    primary_vendor: str,
    candidates: Mapping[str, Mapping[tuple[str, str], int]],
    demoted_from: Mapping[str, float],
    active_windows: Mapping[tuple[str, str], Sequence[tuple[float, float]]],
    spec: SelectionSpec,
) -> list[SelectionRow]:
    """Pick one source per (symbol, KST minute) from per-vendor frame counts; deterministic and sorted.

    A vendor that appears in `active_windows` is eligible for a symbol only inside its windows; other
    vendors are always eligible. The primary is kept on ties and on advantages below `min_advantage_frames`.
    On the trade stream a conflated vendor is eligible for a symbol-minute only when no lossless source has frames.
    """
    managed = {vendor for vendor, _ in active_windows}
    conflated = spec.conflated_trade_vendors if kind == _KIND_TICK else frozenset()
    keys: set[tuple[str, str]] = set()
    for counts in candidates.values():
        keys.update(key for key, frames in counts.items() if frames > 0)
    secondaries = sorted(vendor for vendor in candidates if vendor != primary_vendor)
    rows: list[SelectionRow] = []
    for symbol, minute in sorted(keys):
        primary_frames = candidates.get(primary_vendor, {}).get((symbol, minute), 0)
        minute_wall = _minute_wall(minute)
        in_window = [
            vendor
            for vendor in secondaries
            if candidates[vendor].get((symbol, minute), 0) > 0
            and _eligible(vendor, symbol, minute_wall, managed, active_windows)
        ]
        lossless_present = (primary_frames > 0 and primary_vendor not in conflated) or any(
            vendor not in conflated for vendor in in_window
        )
        eligible = [vendor for vendor in in_window if vendor not in conflated or not lossless_present]
        best = max(eligible, key=lambda vendor: candidates[vendor][(symbol, minute)], default=None)
        best_frames = candidates[best][(symbol, minute)] if best is not None else 0
        demoted_at = demoted_from.get(symbol)
        is_demoted = demoted_at is not None and minute_wall >= demoted_at
        if best is not None and is_demoted and best_frames >= spec.min_frames:
            chosen, reason = best, "demoted"
        elif best is not None and best_frames >= primary_frames + spec.min_advantage_frames:
            chosen = best
            reason = "secondary_only" if primary_frames == 0 else "primary_gap"
        else:
            chosen = primary_vendor
            reason = "primary_ok" if primary_frames >= spec.min_frames else "no_better_source"
        rows.append(
            SelectionRow(
                symbol=symbol,
                venue=venue,
                session=session,
                kind=kind,
                minute_kst=minute,
                selected_vendor=chosen,
                primary_vendor=primary_vendor,
                reason=reason,
                frames_primary=primary_frames,
                frames_selected=candidates[chosen].get((symbol, minute), 0),
            )
        )
    return rows


def _eligible(
    vendor: str,
    symbol: str,
    minute_wall: float,
    managed: set[str],
    active_windows: Mapping[tuple[str, str], Sequence[tuple[float, float]]],
) -> bool:
    if vendor not in managed:
        return True
    return any(start <= minute_wall < end for start, end in active_windows.get((vendor, symbol), ()))


def _kst_minute(minute_ns: int) -> str:
    return dt.datetime.fromtimestamp(minute_ns / _NS_PER_S, _KST).strftime(_MINUTE_FORMAT)


def _count_frames(path: pathlib.Path, decode: Decoder) -> dict[tuple[str, str], int]:
    """Frames per (symbol, KST minute) of one L1 partition, decoded in bounded chunks."""
    counts: collections.Counter[tuple[str, int]] = collections.Counter()
    reader = pq.ParquetFile(path)
    for batch in reader.iter_batches(batch_size=_CHUNK_ROWS, columns=["raw", "recv_wall_ns"]):
        frame = pl.DataFrame(pa.Table.from_batches([batch]))
        decoded = decode(frame).filter(pl.col("shcode").is_not_null())
        grouped = decoded.with_columns(
            (pl.col("recv_wall_ns") // _MINUTE_NS).alias("minute")
        ).group_by(["shcode", "minute"]).len()
        for symbol, minute, count in grouped.iter_rows():
            counts[(str(symbol).zfill(6), int(minute))] += int(count)
    return {(symbol, _kst_minute(minute * _MINUTE_NS)): count for (symbol, minute), count in counts.items()}


def _l1_partition(paths: DataPaths, day: dt.date, vendor: str, venue: str, stream: str) -> pathlib.Path | None:
    key = l0_partition_key(vendor, venue, _SESSION, stream, day)
    assert key is not None, "routes name valid venues and the regular session"
    path = paths.archive_root / l1_relpath_for_l0_partition(l0_partition_relpath(key))
    return path if path.is_file() else None


def _read_controller_log(path: pathlib.Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            row = json.loads(line)
        except ValueError:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _controller_windows(
    rows: Sequence[dict[str, Any]], close_wall: float
) -> tuple[dict[str, float], dict[tuple[str, str], list[tuple[float, float]]]]:
    """Earliest demotion instant per symbol and active admission windows per (vendor, symbol) from the controller log."""
    plans: dict[str, list[tuple[float, tuple[str, ...]]]] = collections.defaultdict(list)
    for row in rows:
        if row.get("kind") == "group_plan" and isinstance(row.get("group"), str):
            plans[row["group"]].append((float(row["wall"]), tuple(str(s) for s in row.get("symbols", ()))))
    demoted: dict[str, float] = {}
    windows: dict[tuple[str, str], list[tuple[float, float]]] = collections.defaultdict(list)
    for row in rows:
        kind = row.get("kind")
        if kind == "TRANSITION" and row.get("new") == "DAY_DEMOTED":
            group = str(row.get("group"))
            at = float(row["wall"])
            known = [entry for entry in plans.get(group, ()) if entry[0] <= at] or plans.get(group, [])
            if not known:
                logger.warning("[DATA] stage=feed_selection status=WARN reason=demotion_without_plan group=%s", group)
                continue
            for symbol in max(known, key=lambda entry: entry[0])[1]:
                demoted[symbol] = min(demoted.get(symbol, at), at)
        elif kind == "ADMIT" and row.get("mode") == "active" and isinstance(row.get("feed"), str):
            vendor = row["feed"].split(":", 1)[0]
            symbols = row.get("symbols") or ()
            for symbol in symbols:
                windows[(vendor, str(symbol))].append((float(row["wall"]), close_wall))
    return demoted, {key: sorted(value) for key, value in windows.items()}


def _write_partition(path: pathlib.Path, rows: Iterable[SelectionRow]) -> None:
    table = pa.Table.from_pylist([dataclasses.asdict(row) for row in rows], schema=_SCHEMA)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    pq.write_table(table, tmp, compression="zstd")
    os.replace(tmp, path)


def _write_json(path: pathlib.Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    os.replace(tmp, path)


_SCHEMA: Final[pa.Schema] = pa.schema([
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("venue", pa.string(), nullable=False),
    pa.field("session", pa.string(), nullable=False),
    pa.field("kind", pa.string(), nullable=False),
    pa.field("minute_kst", pa.string(), nullable=False),
    pa.field("selected_vendor", pa.string(), nullable=False),
    pa.field("primary_vendor", pa.string(), nullable=False),
    pa.field("reason", pa.string(), nullable=False),
    pa.field("frames_primary", pa.int64(), nullable=False),
    pa.field("frames_selected", pa.int64(), nullable=False),
])


def selection_partition_path(paths: DataPaths, day: dt.date) -> pathlib.Path:
    return paths.archive_root / "derived" / "feed_selection" / f"dt={day.isoformat()}.parquet"


def selection_summary_path(paths: DataPaths, day: dt.date) -> pathlib.Path:
    return paths.manifest_dir / "feed_quality" / f"{day.isoformat()}.selection.json"


def build_feed_selection(day: dt.date, *, paths: DataPaths, spec: SelectionSpec) -> SelectionSummary:
    """Write `l1/derived/feed_selection/dt=<day>.parquet` and its summary; status-only when no secondary exists."""
    log_path = paths.feed_control_log_path(day)
    log_rows = _read_controller_log(log_path)
    anchors = resolve_session_anchors(paths.session_calendar_dir, day)
    close_wall = dt.datetime.combine(day, anchors.regular_close, tzinfo=_KST).timestamp()
    demoted, windows = _controller_windows(log_rows, close_wall)

    all_rows: list[SelectionRow] = []
    symbols: set[str] = set()
    vendor_stats: dict[str, dict[str, int]] = {}
    vendor_minutes: dict[str, set[str]] = {}
    secondary_present = False
    for route in _ROUTES:
        primary = spec.primary_for(route.venue)
        if primary is None:
            continue
        kinds: tuple[tuple[Literal["tick", "book"], str], ...] = (
            (_KIND_TICK, route.tick_stream),
            (_KIND_BOOK, route.book_stream),
        )
        for kind, stream in kinds:
            candidates: dict[str, dict[tuple[str, str], int]] = {}
            for vendor, (tick_decode, quote_decode) in _DECODERS.items():
                path = _l1_partition(paths, day, vendor, route.venue, stream)
                if path is None:
                    continue
                if vendor != primary:
                    secondary_present = True
                decode = tick_decode if kind == _KIND_TICK else quote_decode
                counts = _count_frames(path, decode)
                candidates[vendor] = counts
                stats = vendor_stats.setdefault(
                    vendor, {"tick_frames": 0, "book_frames": 0, "conflated_selected_minutes": 0}
                )
                stats[f"{kind}_frames"] += sum(counts.values())
            if primary not in candidates:
                continue
            rows = select_minutes(
                venue=route.venue,
                session=_SESSION,
                kind=kind,
                primary_vendor=primary,
                candidates=candidates,
                demoted_from=demoted,
                active_windows=windows,
                spec=spec,
            )
            all_rows.extend(rows)
            for row in rows:
                if row.kind == _KIND_TICK and row.selected_vendor in spec.conflated_trade_vendors:
                    vendor_stats[row.selected_vendor]["conflated_selected_minutes"] += 1
            symbols.update(row.symbol for row in rows if row.reason != "primary_ok")
            for vendor, counts in candidates.items():
                minutes = vendor_minutes.setdefault(vendor, set())
                minutes.update(minute for _, minute in counts)

    if not log_path.exists() and not secondary_present:
        summary = SelectionSummary(_STATUS_NO_SECONDARY, 0, {}, 0, {})
        _write_json(selection_summary_path(paths, day), {"status": _STATUS_NO_SECONDARY, "date": day.isoformat()})
        logger.info("[DATA] stage=feed_selection status=%s date=%s", _STATUS_NO_SECONDARY, day.isoformat())
        return summary

    ordered = sorted(all_rows, key=lambda row: (row.symbol, row.minute_kst, row.venue, row.kind))
    reasons = dict(sorted(collections.Counter(row.reason for row in ordered).items()))
    _write_partition(selection_partition_path(paths, day), ordered)
    summary = SelectionSummary(
        status=_STATUS_OK,
        rows=len(ordered),
        reasons=reasons,
        symbols_affected=len(symbols),
        vendors={
            vendor: {**vendor_stats[vendor], "minutes_with_frames": len(vendor_minutes.get(vendor, ()))}
            for vendor in sorted(vendor_stats)
        },
    )
    _write_json(
        selection_summary_path(paths, day),
        {
            "date": day.isoformat(),
            "status": summary.status,
            "rows": summary.rows,
            "reasons": summary.reasons,
            "symbols_affected": summary.symbols_affected,
            "vendors": summary.vendors,
        },
    )
    logger.info(
        "[DATA] stage=feed_selection status=%s date=%s rows=%d symbols_affected=%d",
        summary.status,
        day.isoformat(),
        summary.rows,
        summary.symbols_affected,
    )
    return summary
