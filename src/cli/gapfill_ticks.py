"""gapfill-ticks CLI (one-shot; derives gap-fill rows for audited L0 counter gaps from external tick tapes)."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import pathlib
from collections.abc import Callable

import polars as pl
import requests

from src.brokers.kis.stack import build_kis_data_slot_stack
from src.core.config import KisTokenSettings, SnapshotSettings
from src.core.paths import DEFAULT_DATA_ROOT, DataPaths
from src.realtime.kis_sharding import load_kis_data_credentials
from src.storage.l0_gapfill import (
    GapfillInterval,
    GapfillSource,
    GapfillStatus,
    derive_gapfill,
    received_trades_for_gaps,
    summarize_gapfill,
    write_gapfill,
)

_KST = dt.timezone(dt.timedelta(hours=9))
TradeFetcher = Callable[[str, dt.time, dt.time], tuple[tuple[dict[str, object], ...], tuple[str, ...]]]


def add_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register ``gapfill-ticks`` (one-shot; never part of the daemon)."""
    parser = subparsers.add_parser("gapfill-ticks")
    parser.add_argument("--day", required=True, type=dt.date.fromisoformat)
    parser.add_argument("--vendor", default="ls", help="journal vendor of the audited session")
    parser.add_argument("--tape", required=True, type=pathlib.Path, help="parquet with symbol, ts_hms, price, volume")
    parser.add_argument("--tape-vendor", default="kiwoom")
    parser.add_argument("--kis-min-missing", type=int, default=0, help="also query KIS REST for tape-less intervals with at least this many missing trades (0 disables)")
    parser.add_argument("--data-root", type=pathlib.Path, default=DEFAULT_DATA_ROOT)
    parser.add_argument(
        "--journal-root",
        type=pathlib.Path,
        default=None,
        help="L0 journal root to anchor the tape order and recover the boundary seconds of each gap (read-only)",
    )
    parser.set_defaults(handler=run)


def _hms(value: str) -> dt.time:
    return dt.datetime.strptime(value, "%H%M%S").time()


def fetch_kis_tape(
    fetch: TradeFetcher, targets: list[GapfillInterval]
) -> tuple[pl.DataFrame, dict[str, list[str]]]:
    """Query `fetch` once per target interval; returns the combined tape and the saturated seconds per symbol."""
    rows: list[dict[str, object]] = []
    stalled: dict[str, list[str]] = {}
    for item in targets:
        trades, saturated = fetch(item.symbol, _hms(item.chetime_prev), _hms(item.chetime_next))
        rows.extend(
            {"symbol": item.symbol, "ts_hms": int(str(t["hour"])), "price": t["price"], "volume": t["volume"]}
            for t in trades
        )
        if saturated:
            stalled.setdefault(item.symbol, []).extend(saturated)
    schema = {"symbol": pl.String, "ts_hms": pl.Int64, "price": pl.Int64, "volume": pl.Int64}
    return pl.DataFrame(rows, schema=schema), stalled


def _kis_fetcher() -> TradeFetcher:
    stack, _ = build_kis_data_slot_stack(
        snapshot=SnapshotSettings(),
        credentials=load_kis_data_credentials(),
        token_settings=KisTokenSettings(),
        session=requests,
        now=lambda: dt.datetime.now(_KST),
    )
    return lambda symbol, start, end: stack.data.get_time_conclusions(symbol, start=start, end=end)


def run(args: argparse.Namespace, *, kis_fetcher: TradeFetcher | None = None) -> int:
    """Write gap-fill rows and report for ``--day`` and print the totals; returns 0 on success, 2 when the audit file is absent."""
    paths = DataPaths(args.data_root)
    audit_path = paths.session_audit_path(args.day, args.vendor)
    if not audit_path.exists():
        print(json.dumps({"error": f"audit absent: {audit_path}"}))  # noqa: T201 - CLI result goes to stdout
        return 2
    audit = json.loads(audit_path.read_text(encoding="utf-8"))
    sources = [
        GapfillSource(args.tape_vendor, f"gapfill:{args.tape_vendor}_tape", pl.read_parquet(args.tape)),
    ]
    journal_root = getattr(args, "journal_root", None)
    received = (
        received_trades_for_gaps(audit, journal_root, vendor=args.vendor, day=args.day) if journal_root is not None else None
    )
    rows, outcomes = derive_gapfill(audit, sources, received)
    notes: dict[str, object] = {"anchored_on_journal": received is not None}
    if args.kis_min_missing > 0:
        targets = [
            item
            for item in outcomes
            if item.status is GapfillStatus.NONE and item.missing_trades >= args.kis_min_missing
        ]
        if targets:
            kis_tape, stalled = fetch_kis_tape(kis_fetcher or _kis_fetcher(), targets)
            sources.append(GapfillSource("kis_rest", "gapfill:kis_rest_time_conclusion", kis_tape))
            rows, outcomes = derive_gapfill(audit, sources, received)
            notes["kis_saturated_seconds"] = stalled
    rows_path, report_path = paths.gapfill_paths(args.day, args.vendor)
    write_gapfill(rows, outcomes, rows_path=rows_path, report_path=report_path, notes=notes)
    print(json.dumps({"rows": rows_path.as_posix(), **summarize_gapfill(outcomes)}))  # noqa: T201 - CLI result goes to stdout
    return 0
