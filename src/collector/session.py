"""수집 세션 composition root (코어 프리미티브 결선 facade)."""

# ruff: noqa: I001 - spec pins import order check_disk_watermark, StorageExhaustedError
from __future__ import annotations

import datetime as dt
import logging
import pathlib
import time
from dataclasses import dataclass, field
from typing import Any, cast

from src.collector.clock import measure_ntp_offset_ns
from src.collector.ipc import read_candidates
from src.collector.journal import L0JournalWriter
from src.collector.manifest import SessionManifest
from src.collector.daemon import CollectorDaemonSchedule, calc_sleep_seconds, get_target_state, run_eod_maintenance
from src.collector.storage_guard import check_disk_watermark, StorageExhaustedError
from src.collector.subscription import SubscriptionDiff, SubscriptionRegistry

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SessionConfig:
    session_date: dt.date
    journal_root: pathlib.Path
    manifest_path: pathlib.Path
    candidates_path: pathlib.Path
    ntp_host: str
    slot_budget: int
    max_clock_offset_ns: int
    desired_streams: tuple[str, ...]
    vendor: str
    schedule: CollectorDaemonSchedule = field(default_factory=CollectorDaemonSchedule)


@dataclass
class CollectorSession:
    manifest: SessionManifest
    registry: SubscriptionRegistry
    journals: dict[tuple[str, str], L0JournalWriter]
    manifest_path: pathlib.Path
    schedule: CollectorDaemonSchedule = field(default_factory=CollectorDaemonSchedule)

    def current_state(self, now_dt: dt.datetime | None = None) -> str:
        current = now_dt or dt.datetime.now(dt.UTC)
        return get_target_state(current)

    def seconds_to_streamer(self, now_dt: dt.datetime | None = None) -> float:
        current = now_dt or dt.datetime.now(dt.UTC)
        return calc_sleep_seconds(current, self.schedule.streamer_start)

    def record_frame(
        self,
        *,
        vendor: str,
        stream: str,
        raw: str,
        recv_mono_ns: int,
        recv_wall_ns: int,
        conn_id: str,
        conn_seq: int,
    ) -> None:
        if not check_disk_watermark(self.manifest_path.parent, min_free_gb=3.0): raise StorageExhaustedError('free disk below watermark')  # noqa: E701
        key = (vendor, stream)
        if key not in self.journals:
            raise KeyError(f"{vendor}/{stream}")
        self.journals[key].append(
            raw=raw,
            recv_mono_ns=recv_mono_ns,
            recv_wall_ns=recv_wall_ns,
            conn_id=conn_id,
            conn_seq=conn_seq,
        )

    def flush_journals(self) -> int:
        return sum(writer.flush() for writer in self.journals.values())

    def note_ack(self, *, vendor: str, tr_id: str, symbol: str, rt_cd: str, accepted: bool) -> None:
        self.manifest.record_ack(vendor=vendor, tr_id=tr_id, symbol=symbol, rt_cd=rt_cd, accepted=accepted)

    def note_gap(self, *, symbol: str, gap_start_ns: int, gap_end_ns: int, reason: str) -> None:
        self.manifest.record_gap(symbol=symbol, gap_start_ns=gap_start_ns, gap_end_ns=gap_end_ns, reason=reason)

    def replay_pairs(self) -> list[tuple[str, str]]:
        return self.registry.replay_pairs()

    def persist(self) -> None:
        self.manifest.save(self.manifest_path)


def bootstrap_session(cfg: SessionConfig, *, ntp_client: object | None = None, now_ns: int | None = None) -> CollectorSession:
    offset_ns = measure_ntp_offset_ns(cfg.ntp_host, client=ntp_client)
    started = now_ns if now_ns is not None else time.time_ns()
    manifest = SessionManifest(session_date=cfg.session_date, clock_offset_ns=offset_ns, started_at_ns=started)
    manifest.assert_clock_within(max_offset_ns=cfg.max_clock_offset_ns)
    stored = read_candidates(cfg.candidates_path)
    rows: list[dict[str, Any]] = cast(list[dict[str, Any]], stored["candidates"]) if stored is not None else []
    desired = {str(row["symbol"]): tuple(cfg.desired_streams) for row in rows}
    registry = SubscriptionRegistry(slot_budget=cfg.slot_budget)
    diff: SubscriptionDiff = registry.plan(desired)
    registry.apply(diff)
    journals = {(cfg.vendor, stream): L0JournalWriter(root=cfg.journal_root, vendor=cfg.vendor, stream=stream) for stream in cfg.desired_streams}
    run_eod_maintenance(cfg.journal_root, today=cfg.session_date)
    session = CollectorSession(manifest=manifest, registry=registry, journals=journals, manifest_path=cfg.manifest_path)
    session.persist()
    logger.info("[DATA] stage=bootstrap pairs=%d offset_ns=%d status=OK", len(session.replay_pairs()), offset_ns)
    return session
