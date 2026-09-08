"""collect-status CLI 서브커맨드."""

from __future__ import annotations

import argparse
import logging
import pathlib

logger = logging.getLogger(__name__)


def add_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """'collect-status' 서브커맨드를 등록한다."""
    parser = subparsers.add_parser("collect-status")
    parser.add_argument("--manifest-path", required=True)
    parser.set_defaults(handler=run)


def run(args: argparse.Namespace) -> int:
    """세션 manifest 요약을 출력한다."""
    from src.collector.manifest import SessionManifest

    try:
        manifest = SessionManifest.load(pathlib.Path(str(args.manifest_path)))
    except FileNotFoundError:
        logger.error("[DATA] stage=collect_status status=FAIL reason=missing_manifest path=%s", str(args.manifest_path))
        return 2
    accepted = manifest.accepted_symbols()
    logger.info(
        "[DATA] stage=collect_status accepted=%d gaps=%d offset_ns=%d",
        len(accepted),
        len(manifest.gaps),
        int(manifest.clock_offset_ns),
    )
    return 0
