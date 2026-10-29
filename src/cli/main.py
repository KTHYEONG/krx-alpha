"""krx-alpha CLI 진입점 (서브커맨드 디스패처)."""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Sequence

from src.core.config import CollectorSettings, ObservabilitySettings
from src.core.observability import configure_logging

logger = logging.getLogger(__name__)


def register_subcommands(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """서브커맨드를 파서에 등록한다."""
    from src.cli import (
        backfill_minute_bars,
        backfill_tick_tape,
        bars_refresh,
        collect_aftermarket,
        collect_init,
        collect_premarket,
        collect_regular_kis,
        collect_snapshots,
        collect_status,
        collect_stream,
        collect_stream_db,
        collect_stream_kiwoom,
        gapfill_ticks,
        order,
        probe_db_stream,
        repair_ticks,
        toss_program_trades_backfill,
        universe_plan,
        universe_replay,
    )

    universe_plan.add_parser(subparsers)
    universe_replay.add_parser(subparsers)
    collect_status.add_parser(subparsers)
    collect_init.add_parser(subparsers)
    collect_stream.add_parser(subparsers)
    collect_stream_db.add_parser(subparsers)
    collect_stream_kiwoom.add_parser(subparsers)
    probe_db_stream.add_parser(subparsers)
    gapfill_ticks.add_parser(subparsers)
    backfill_tick_tape.add_parser(subparsers)
    backfill_minute_bars.add_parser(subparsers)
    repair_ticks.add_parser(subparsers)
    collect_aftermarket.add_parser(subparsers)
    collect_premarket.add_parser(subparsers)
    collect_regular_kis.add_parser(subparsers)
    collect_snapshots.add_parser(subparsers)
    bars_refresh.add_parser(subparsers)
    toss_program_trades_backfill.add_parser(subparsers)
    order.add_parser(subparsers)


def build_parser() -> argparse.ArgumentParser:
    """최상위 인자 파서를 구성한다."""
    parser = argparse.ArgumentParser(prog="krx-alpha")
    parser.add_argument("--log-level", default="INFO")
    subparsers = parser.add_subparsers(dest="command", required=True)
    register_subcommands(subparsers)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """CLI 실행 진입점."""
    parser = build_parser()
    args = parser.parse_args(argv)
    repair_preview = args.command == "repair-ticks" and args.dry_run
    configure_logging(
        f"cli-{args.command}",
        log_dir=CollectorSettings().paths.logs_dir if ObservabilitySettings().persistent_logs and not repair_preview else None,
        level=str(args.log_level).upper(),
        alert_ledger_path=CollectorSettings().paths.alert_ledger_file,
    )
    handler = getattr(args, "handler", None)
    if handler is None:
        parser.error(f"no handler bound for command: {args.command}")
    return int(handler(args))


if __name__ == "__main__":
    sys.exit(main())
