"""collect-init CLI 서브커맨드."""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import pathlib

from src.collector.clock import ClockUnsyncedError
from src.collector.session import SessionConfig, bootstrap_session

logger = logging.getLogger(__name__)


def add_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """'collect-init' 서브커맨드를 등록한다."""
    parser = subparsers.add_parser("collect-init")
    parser.add_argument("--session-date", required=True)
    parser.add_argument("--journal-root", required=True)
    parser.add_argument("--manifest-path", required=True)
    parser.add_argument("--candidates-path", required=True)
    parser.add_argument("--ntp-host", default="kr.pool.ntp.org")
    parser.add_argument("--slot-budget", type=int, default=41)
    parser.add_argument("--max-clock-offset-ns", type=int, default=2_000_000_000)
    parser.add_argument("--streams", default="H0STCNT0")
    parser.add_argument("--vendor", default="kis")
    parser.set_defaults(handler=run)


def run(args: argparse.Namespace) -> int:
    """수집 세션을 부트스트랩하고 manifest 를 저장한다."""
    cfg = SessionConfig(
        session_date=dt.date.fromisoformat(str(args.session_date)),
        journal_root=pathlib.Path(str(args.journal_root)),
        manifest_path=pathlib.Path(str(args.manifest_path)),
        candidates_path=pathlib.Path(str(args.candidates_path)),
        ntp_host=str(args.ntp_host),
        slot_budget=int(args.slot_budget),
        max_clock_offset_ns=int(args.max_clock_offset_ns),
        desired_streams=tuple(str(args.streams).split(",")),
        vendor=str(args.vendor),
    )
    try:
        session = bootstrap_session(cfg)
    except ClockUnsyncedError as exc:
        logger.error("[DATA] stage=collect_init status=FAIL reason=%s", str(exc))
        return 3
    pairs = session.replay_pairs()
    logger.info(
        "[DATA] stage=collect_init pairs=%d offset_ns=%d status=OK",
        len(pairs),
        int(session.manifest.clock_offset_ns),
    )
    return 0
