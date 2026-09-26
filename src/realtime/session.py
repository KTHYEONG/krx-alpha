"""수집 세션 composition root (코어 프리미티브 결선 facade)."""

# ruff: noqa: I001 - spec pins import order check_disk_watermark, StorageExhaustedError
from __future__ import annotations

import datetime as dt
import logging
import os
import pathlib
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, cast
from zoneinfo import ZoneInfo

from src.core.calendar import SessionSchedule, SessionState, calc_sleep_seconds, get_target_state
from src.core.config import DataPaths
from src.core.session_anchors import resolve_session_anchors
from src.realtime.clock import ClockUnsyncedError, measure_ntp_offset_ns
from src.realtime.contracts import MarketSession, MarketVenue
from src.realtime.kis_sharding import AftermarketShard
from src.universe.ipc import read_candidates
from src.storage.journal import L0JournalWriter
from src.realtime.manifest import SessionManifest
from src.storage.retention import check_disk_watermark, StorageExhaustedError, prune_old_journals
from src.realtime.subscription import SubscriptionDiff, SubscriptionRegistry

logger = logging.getLogger(__name__)
_KST = ZoneInfo("Asia/Seoul")


def last_journal_write_ns(journals: Mapping[tuple[str, ...], L0JournalWriter], at_ns: int) -> int | None:
    """Latest on-disk write time among this session's journal partitions for the date of ``at_ns``.

    A restarted collector cannot know when its predecessor stopped receiving
    frames; the newest journal file mtime is the last durable evidence of
    reception, so the window from it to the new boot is the restart gap.
    """
    latest: int | None = None
    for writer in journals.values():
        for path in writer.owned_files(at_ns):
            mtime_ns = path.stat().st_mtime_ns
            if latest is None or mtime_ns > latest:
                latest = mtime_ns
    return latest


@dataclass(frozen=True)
class StreamRoute:
    venue: MarketVenue
    session: MarketSession


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
    route: StreamRoute | None = None
    shard: AftermarketShard | None = None
    min_free_disk_gb: float = 3.0
    journal_retain_days: int = 3


@dataclass
class CollectorSession:
    manifest: SessionManifest
    registry: SubscriptionRegistry
    journals: dict[tuple[str, ...], L0JournalWriter]
    manifest_path: pathlib.Path
    schedule: SessionSchedule = field(default_factory=SessionSchedule)
    route: StreamRoute | None = None
    min_free_disk_gb: float = 3.0

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
        venue: MarketVenue | str | None = None,
        session: MarketSession | str | None = None,
        symbol: str = "",
        exchange_event_time: str = "",
    ) -> None:
        if not check_disk_watermark(self.manifest_path.parent, min_free_gb=self.min_free_disk_gb): raise StorageExhaustedError('free disk below watermark')  # noqa: E701
        route = getattr(self, "route", None)
        # 라우팅 세션은 (venue, session, stream) 파티션 키로 검증한다.
        # 경로 불일치 프레임은 저널 미스 KeyError 로 쓰기 전에 거부된다 (모호한 파티션 기록 방지).
        key: tuple[str, ...] = (
            (str(MarketVenue(venue).value), str(MarketSession(session).value), stream)
            if route is not None and venue is not None and session is not None
            else (vendor, stream)
        )
        if key not in self.journals:
            raise KeyError(f"{vendor}/{stream}")
        self.journals[key].append(
            raw=raw,
            recv_mono_ns=recv_mono_ns,
            recv_wall_ns=recv_wall_ns,
            conn_id=conn_id,
            conn_seq=conn_seq,
            exchange_event_time=exchange_event_time,
            symbol=symbol,
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
    """Start a collection session only after a measured clock passes its offset gate.

    Args:
        cfg: Session paths, streams, NTP hosts, and maximum clock offset.
        ntp_client: Optional injected NTP client for a deterministic boundary.
        now_ns: Optional supplied start timestamp.

    Returns:
        A session with a persisted, measured clock offset.

    Raises:
        ClockUnsyncedError: Every configured host failed measurement, or
            the measured offset exceeded cfg.max_clock_offset_ns.
    """
    if not cfg.min_free_disk_gb > 0:
        raise ValueError("min_free_disk_gb must be positive")
    if cfg.journal_retain_days < 0:
        raise ValueError("journal_retain_days must be nonnegative")
    hosts = (cfg.ntp_host, *cfg.ntp_fallback_hosts)
    offset_ns: int | None = None
    for host in hosts:
        try:
            offset_ns = measure_ntp_offset_ns(host, client=ntp_client)
            break
        except ClockUnsyncedError:
            logger.warning("[DATA] stage=ntp_probe status=FAIL host=%s", host)
    if offset_ns is None:
        logger.critical(
            "[DATA] stage=bootstrap status=FAIL reason=ntp_unmeasured hosts=%d",
            len(hosts),
        )
        raise ClockUnsyncedError(f"ntp unreachable: all {len(hosts)} hosts failed")
    if abs(offset_ns) > cfg.max_clock_offset_ns:
        raise ClockUnsyncedError(f"clock offset {offset_ns}ns exceeds {cfg.max_clock_offset_ns}ns")
    measured_ns: int = offset_ns
    clock_status = "measured"
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
            manifest.clock_offset_ns = measured_ns
            manifest.clock_status = clock_status
    if manifest is None:
        venue = cfg.route.venue.value if cfg.route is not None else MarketVenue.KRX.value
        market_session = cfg.route.session.value if cfg.route is not None else MarketSession.REGULAR.value
        expected_close_ns = 0
        if cfg.route is not None:
            anchors = resolve_session_anchors(
                DataPaths(cfg.journal_root.parent).session_calendar_dir, cfg.session_date
            )
            close_at = dt.datetime.combine(cfg.session_date, anchors.after_market_end, tzinfo=_KST)
            expected_close_ns = int(close_at.timestamp() * 1_000_000_000)
        manifest = SessionManifest(
            session_date=cfg.session_date,
            clock_offset_ns=measured_ns,
            started_at_ns=started,
            clock_status=clock_status,
            venue=venue,
            session=market_session,
            expected_close_ns=expected_close_ns,
        )
    stored = read_candidates(cfg.candidates_path)
    rows: list[dict[str, Any]] = cast(list[dict[str, Any]], stored["candidates"]) if stored is not None else []
    if cfg.shard is not None:
        manifest.planned_pairs = [
            {"symbol": symbol, "tr_id": stream} for symbol in cfg.shard.symbols for stream in cfg.shard.streams
        ]
        manifest.shard_index = cfg.shard.shard_index
        manifest.credential_key_id = cfg.shard.credential_key_id
        desired = {str(symbol): tuple(cfg.shard.streams) for symbol in cfg.shard.symbols}
    else:
        desired = {str(row["symbol"]): tuple(cfg.desired_streams) for row in rows}
    registry = SubscriptionRegistry(slot_budget=cfg.slot_budget)
    diff: SubscriptionDiff = registry.plan(desired)
    registry.apply(diff)
    if cfg.route is not None:
        journals: dict[tuple[str, ...], L0JournalWriter] = {
            (cfg.route.venue.value, cfg.route.session.value, stream): L0JournalWriter(
                root=cfg.journal_root,
                vendor=cfg.vendor,
                venue=cfg.route.venue,
                session=cfg.route.session,
                stream=stream,
                file_tag=f"s{cfg.shard.shard_index}" if cfg.shard is not None else None,
            )
            for stream in cfg.desired_streams
        }
    else:
        journals = {(cfg.vendor, stream): L0JournalWriter(root=cfg.journal_root, vendor=cfg.vendor, stream=stream) for stream in cfg.desired_streams}
    if manifest.boots:
        last_write_ns = last_journal_write_ns(journals, started)
        if last_write_ns is not None and started > last_write_ns:
            manifest.record_gap(symbol="*", gap_start_ns=last_write_ns, gap_end_ns=started, reason="restart")
            logger.warning(
                "[DATA] stage=bootstrap status=RESTART_GAP gap_s=%.1f",
                (started - last_write_ns) / 1_000_000_000,
            )
    prune_old_journals(cfg.journal_root, archive_root=cfg.archive_root, retain_days=cfg.journal_retain_days, reference_date=cfg.session_date)
    session = CollectorSession(
        manifest=manifest, registry=registry, journals=journals, manifest_path=cfg.manifest_path, schedule=cfg.schedule,
        route=cfg.route, min_free_disk_gb=cfg.min_free_disk_gb,
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
    session.persist()
    logger.info(
        "[DATA] stage=bootstrap pairs=%d offset_ns=%d state=%s status=OK",
        len(session.replay_pairs()),
        manifest.clock_offset_ns,
        session.current_state(),
    )
    return session
