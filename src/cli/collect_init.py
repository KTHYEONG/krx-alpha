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


def add_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register 'collect-init'. Settings-derived flags default to None so registration reads no env."""
    parser = subparsers.add_parser("collect-init")
    parser.add_argument("--session-date", required=True)
    parser.add_argument("--journal-root", required=True)
    parser.add_argument("--manifest-path", required=True)
    parser.add_argument("--candidates-path", required=True)
    parser.add_argument("--ntp-host", default=None)
    parser.add_argument("--slot-budget", type=int, default=None)
    parser.add_argument("--max-clock-offset-ns", type=int, default=None)
    parser.add_argument("--streams", default=None)
    parser.add_argument("--vendor", default=None)
    parser.add_argument("--archive-root", default=None)
    parser.add_argument("--min-free-disk-gb", type=float, default=None)
    parser.add_argument("--journal-retain-days", type=int, default=None)
    parser.set_defaults(handler=run)


def run(args: argparse.Namespace) -> int:
    """Bootstrap one collection session and persist its manifest, returning exit code."""
    settings = CollectorSettings()
    raw_streams = getattr(args, "streams", None)
    raw_ntp_host = getattr(args, "ntp_host", None)
    raw_slot_budget = getattr(args, "slot_budget", None)
    raw_max_offset = getattr(args, "max_clock_offset_ns", None)
    raw_vendor = getattr(args, "vendor", None)
    raw_min_free = getattr(args, "min_free_disk_gb", None)
    raw_retain = getattr(args, "journal_retain_days", None)
    cfg = SessionConfig(
        session_date=dt.date.fromisoformat(str(args.session_date)),
        journal_root=pathlib.Path(str(args.journal_root)),
        manifest_path=pathlib.Path(str(args.manifest_path)),
        candidates_path=pathlib.Path(str(args.candidates_path)),
        ntp_host=str(raw_ntp_host) if raw_ntp_host is not None else settings.ntp_host,
        slot_budget=int(raw_slot_budget) if raw_slot_budget is not None else settings.subscription_pair_budget,
        max_clock_offset_ns=int(raw_max_offset) if raw_max_offset is not None else settings.max_clock_offset_ns,
        desired_streams=tuple(str(raw_streams).split(",")) if raw_streams is not None else settings.streams,
        vendor=str(raw_vendor) if raw_vendor is not None else settings.vendor,
        archive_root=(pathlib.Path(str(args.archive_root)) if getattr(args, "archive_root", None) else None),
        min_free_disk_gb=float(raw_min_free) if raw_min_free is not None else settings.min_free_disk_gb,
        journal_retain_days=int(raw_retain) if raw_retain is not None else settings.journal_retain_days,
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
