"""수집 세션 composition root (코어 프리미티브 결선 facade)."""

# ruff: noqa: I001 - spec pins import order check_disk_watermark, StorageExhaustedError
from __future__ import annotations

import datetime as dt
import logging
import os
import pathlib
import time
from dataclasses import dataclass, field
from typing import Any, cast

from src.core.calendar import SessionSchedule, SessionState, calc_sleep_seconds, get_target_state
from src.realtime.clock import ClockUnsyncedError, measure_ntp_offset_ns
from src.universe.ipc import read_candidates
from src.storage.journal import L0JournalWriter
from src.realtime.manifest import SessionManifest
from src.storage.retention import check_disk_watermark, StorageExhaustedError, prune_old_journals
from src.realtime.subscription import SubscriptionDiff, SubscriptionRegistry

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
    archive_root: pathlib.Path | None = None
    schedule: SessionSchedule = field(default_factory=SessionSchedule)
    degraded_reason: str | None = None
    ntp_fallback_hosts: tuple[str, ...] = ()


@dataclass
class CollectorSession:
    manifest: SessionManifest
    registry: SubscriptionRegistry
    journals: dict[tuple[str, str], L0JournalWriter]
    manifest_path: pathlib.Path
    schedule: SessionSchedule = field(default_factory=SessionSchedule)

    def current_state(self, now_dt: dt.datetime | None = None) -> SessionState:
        current = now_dt or dt.datetime.now(dt.UTC)
        return get_target_state(current, schedule=self.schedule)

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
    offset_ns: int | None = None
    for host in (cfg.ntp_host, *cfg.ntp_fallback_hosts):
        try:
            offset_ns = measure_ntp_offset_ns(host, client=ntp_client)
            break
        except ClockUnsyncedError:
            logger.warning("[DATA] stage=ntp_probe status=FAIL host=%s", host)
    clock_status = "measured" if offset_ns is not None else "unmeasured"
    if offset_ns is None:
        logger.critical(
            "[DATA] stage=bootstrap status=DEGRADED reason=ntp_unmeasured hosts=%d",
            1 + len(cfg.ntp_fallback_hosts),
        )
    started = now_ns if now_ns is not None else time.time_ns()
    manifest: SessionManifest | None = None
    if cfg.manifest_path.exists():
        try:
            loaded = SessionManifest.load(cfg.manifest_path)
        except (ValueError, KeyError, TypeError, OSError):
            corrupt = cfg.manifest_path.with_name(cfg.manifest_path.name + ".corrupt")
            os.replace(cfg.manifest_path, corrupt)
            logger.warning(
                "[DATA] stage=manifest_load status=CORRUPT action=fresh path=%s", str(cfg.manifest_path)
            )
            loaded = None
        if loaded is not None and loaded.session_date == cfg.session_date:
            manifest = loaded
            manifest.clock_offset_ns = offset_ns or 0
            manifest.clock_status = clock_status
    if manifest is None:
        manifest = SessionManifest(
            session_date=cfg.session_date,
            clock_offset_ns=offset_ns or 0,
            started_at_ns=started,
            clock_status=clock_status,
        )
    stored = read_candidates(cfg.candidates_path)
    rows: list[dict[str, Any]] = cast(list[dict[str, Any]], stored["candidates"]) if stored is not None else []
    desired = {str(row["symbol"]): tuple(cfg.desired_streams) for row in rows}
    registry = SubscriptionRegistry(slot_budget=cfg.slot_budget)
    diff: SubscriptionDiff = registry.plan(desired)
    registry.apply(diff)
    journals = {(cfg.vendor, stream): L0JournalWriter(root=cfg.journal_root, vendor=cfg.vendor, stream=stream) for stream in cfg.desired_streams}
    prune_old_journals(cfg.journal_root, retain_days=3, reference_date=cfg.session_date, archive_root=cfg.archive_root)
    session = CollectorSession(
        manifest=manifest, registry=registry, journals=journals, manifest_path=cfg.manifest_path, schedule=cfg.schedule
    )
    if stored is not None and "rev" in stored:
        manifest.candidates_rev = int(cast(Any, stored["rev"]))
    manifest.degraded_reason = cfg.degraded_reason
    manifest.boots.append(
        {
            "started_at_ns": started,
            "clock_offset_ns": manifest.clock_offset_ns,
            "clock_status": clock_status,
        }
    )
    if clock_status == "measured":
        manifest.assert_clock_within(max_offset_ns=cfg.max_clock_offset_ns)
    session.persist()
    logger.info(
        "[DATA] stage=bootstrap pairs=%d offset_ns=%d state=%s status=OK",
        len(session.replay_pairs()),
        manifest.clock_offset_ns,
        session.current_state(),
    )
    return session
