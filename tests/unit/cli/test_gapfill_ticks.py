from __future__ import annotations

import datetime as dt

from src.cli.main import build_parser


def test_gapfill_parser_retains_manual_vendor_options() -> None:
    args = build_parser().parse_args([
        "gapfill-ticks", "--day", "2026-10-23", "--tape", "tape.parquet", "--journal-root", "journals",
    ])
    assert args.day == dt.date(2026, 10, 23)
    assert args.vendor == "ls"
    assert args.kis_min_missing == 0
    assert str(args.journal_root) == "journals"
