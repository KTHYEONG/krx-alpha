"""krx-alpha CLI 진입점 (서브커맨드 디스패처)."""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Sequence

logger = logging.getLogger(__name__)


def register_subcommands(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """서브커맨드를 파서에 등록한다."""
    from src.cli import collect_init, collect_status, universe_plan

    universe_plan.add_parser(subparsers)
    collect_status.add_parser(subparsers)
    collect_init.add_parser(subparsers)


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
    logging.basicConfig(level=getattr(logging, str(args.log_level).upper(), logging.INFO))
    handler = getattr(args, "handler", None)
    if handler is None:
        parser.error(f"no handler bound for command: {args.command}")
    return int(handler(args))


if __name__ == "__main__":
    sys.exit(main())
