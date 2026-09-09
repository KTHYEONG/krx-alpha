"""collect-init CLI 서브커맨드."""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import pathlib

from src.core.config import CollectorSettings
from src.realtime.clock import ClockUnsyncedError
from src.realtime.session import SessionConfig, bootstrap_session

logger = logging.getLogger(__name__)

_DEFAULTS = CollectorSettings()


def add_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """'collect-init' 서브커맨드를 등록한다."""
    parser = subparsers.add_parser("collect-init")
    parser.add_argument("--session-date", required=True)
    parser.add_argument("--journal-root", required=True)
    parser.add_argument("--manifest-path", required=True)
    parser.add_argument("--candidates-path", required=True)
    parser.add_argument("--ntp-host", default=_DEFAULTS.ntp_host)
    parser.add_argument("--slot-budget", type=int, default=_DEFAULTS.subscription_pair_budget)
    parser.add_argument("--max-clock-offset-ns", type=int, default=_DEFAULTS.max_clock_offset_ns)
    parser.add_argument("--streams", default=",".join(_DEFAULTS.streams))
    parser.add_argument("--vendor", default=_DEFAULTS.vendor)
    parser.add_argument("--archive-root", default=None)
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
        archive_root=(pathlib.Path(str(args.archive_root)) if getattr(args, "archive_root", None) else None),
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
