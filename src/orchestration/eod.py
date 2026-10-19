"""EOD 유지보수/오프로드 (daemon 에서 추출한 타입드 유스케이스)."""

from __future__ import annotations

import datetime as dt
import functools
import json
import logging
import pathlib
from collections.abc import Callable, Mapping, Sequence
from collections.abc import Set as AbstractSet
from dataclasses import dataclass
from enum import StrEnum
from typing import Any
from zoneinfo import ZoneInfo

from src.core.config import AuditMode, AuditSettings
from src.core.session_anchors import STANDARD_AFTER_MARKET_END, SessionAnchors
from src.realtime.kis_sharding import AftermarketShard
from src.realtime.manifest import SessionManifest
from src.realtime.streamer import aftermarket_open_time
from src.storage.l0_audit import AuditStatus, LatencyBreach, SessionAudit, StreamLatency, latency_breaches
from src.storage.layout import (
    L0_JOURNAL_GLOB,
    L0_REPO_PREFIX,
    L1_REPO_PREFIX,
    l0_partition_for_l1,
    l0_partition_key,
    l0_partition_relpath,
    l1_repo_path,
)
from src.storage.market_phase import MarketPhase
from src.storage.normalize_worker import run_isolated_normalize
from src.storage.remote import GDriveArchiver, PurgeStats, RcloneArchiver, RemoteArchiveError, SyncStats
from src.storage.retention import prune_local_l1, prune_old_journals

logger = logging.getLogger(__name__)

MAX_SESSION_GAP_S: float = 600.0
_KST = ZoneInfo("Asia/Seoul")


@dataclass(frozen=True)
class EodOffloadResult:
    l1: SyncStats
    manifests: SyncStats
    verified_remote_l1: frozenset[str]
    purged: int = 0


def run_eod_maintenance(
    journal_root: pathlib.Path,
    *,
    retain_days: int = 3,
    today: dt.date | None = None,
    archive_root: pathlib.Path | None = None,
    quarantine_root: pathlib.Path | None = None,
    work_root: pathlib.Path | None = None,
    verified_remote_l1: AbstractSet[str] | None = None,
    progress: Callable[[], None] | None = None,
    normalize: bool = True,
    reuse_fresh_l1: bool = False,
) -> int:
    return prune_old_journals(
        journal_root,
        archive_root=archive_root,
        retain_days=retain_days,
        reference_date=today,
        quarantine_root=quarantine_root,
        verified_remote_l1=verified_remote_l1,
        progress=progress,
        normalize=normalize,
        reuse_fresh_l1=reuse_fresh_l1,
        # 데몬 OOM crash loop를 막기 위해 정규화는 자식 프로세스로 격리한다
        normalizer=functools.partial(run_isolated_normalize, work_root=work_root),
    )


def run_eod_offload(
    archive_root: pathlib.Path,
    manifest_root: pathlib.Path | None = None,
    remote: Any | None = None,
    *,
    archiver: Any = None,
    reference_date: dt.date | None = None,
    retain_days: int = 30,
    progress: Callable[[], None] | None = None,
    journal_root: pathlib.Path | None = None,
) -> EodOffloadResult:
    arc = remote if remote is not None else archiver
    if arc is None:
        arc = GDriveArchiver.try_from_env()
    if arc is None:
        logger.critical("[DAEMON] stage=eod_offload status=FAIL reason=rclone_settings_missing")
        empty = SyncStats()
        return EodOffloadResult(l1=empty, manifests=SyncStats(), verified_remote_l1=frozenset(), purged=0)
    if progress is not None and hasattr(arc, "bind_progress"):
        arc.bind_progress(progress)
    l1_stats = arc.sync_l1_tree(archive_root, progress=progress)
    manifest_path = (
        pathlib.Path(manifest_root) if manifest_root is not None else pathlib.Path(archive_root).parent / "manifest"
    )
    archiver = arc
    manifests_stats = archiver.sync_manifest_tree(manifest_path)
    if l1_stats.failed_verification > 0 or manifests_stats.failed_verification > 0:
        raise RemoteArchiveError(
            f"offload verification failed: l1={l1_stats.failed_verification} manifests={manifests_stats.failed_verification}"
        )
    sizes = arc.remote_file_sizes(L1_REPO_PREFIX) if hasattr(arc, "remote_file_sizes") else {}
    local_root = pathlib.Path(archive_root)
    verified: set[str] = set()
    for pq in sorted(local_root.rglob("*.parquet")):
        rel = l1_repo_path(pq.relative_to(local_root))
        if sizes.get(rel) == pq.stat().st_size:
            verified.add(rel)
    confirmed = arc.remote_files(L1_REPO_PREFIX)
    if journal_root is not None:
        # Keep verified bytes until the second maintenance pass has removed their inputs.
        confirmed = {
            path
            for path in confirmed
            if (l0_dir := l0_partition_for_l1(path)) is None
            or not (journal_root / l0_dir.removeprefix(L0_REPO_PREFIX)).exists()
        }
    purged = prune_local_l1(
        archive_root, retain_days=retain_days, reference_date=reference_date, confirmed_remote=confirmed
    )
    return EodOffloadResult(
        l1=l1_stats, manifests=manifests_stats, verified_remote_l1=frozenset(verified), purged=purged
    )


def run_eod_remote_l0_purge(
    journal_root: pathlib.Path,
    verified_remote_l1: AbstractSet[str],
    *,
    archiver: Any = None,
    progress: Callable[[], None] | None = None,
    quarantine_root: pathlib.Path | None = None,
) -> PurgeStats:
    """Purge Drive L0 partitions superseded by remote-verified L1 after local L0 pruning.

    Returns zero stats (and logs CRITICAL reason=rclone_settings_missing) when no
    archiver can be built, mirroring run_eod_offload.
    When ``quarantine_root`` is provided, remote L0 for quarantined partitions
    is retained because its records may not be represented in verified L1.
    """
    arc = archiver if archiver is not None else GDriveArchiver.try_from_env()
    if arc is None:
        logger.critical("[DAEMON] stage=eod_l0_remote_purge status=FAIL reason=rclone_settings_missing")
        return PurgeStats()
    if progress is not None and hasattr(arc, "bind_progress"):
        arc.bind_progress(progress)
    if quarantine_root is not None:
        # A quarantined input may contain records absent from the previously verified L1.
        verified_remote_l1 = {
            path
            for path in verified_remote_l1
            if (l0_dir := l0_partition_for_l1(path)) is None
            or not (quarantine_root / l0_dir.removeprefix(L0_REPO_PREFIX)).exists()
        }
    return arc.purge_superseded_l0(verified_remote_l1, pathlib.Path(journal_root), progress=progress)


class GapBudgetStatus(StrEnum):
    """Ledger verdict of one regular-session gap budget."""

    PASS = "PASS"  # noqa: S105 - verdict label, not a secret
    WARN = "WARN"
    FAIL = "FAIL"


@dataclass(frozen=True)
class GapBudgetVerdict:
    """Count-based gap budget verdict from manifest spans and the session audit."""

    status: GapBudgetStatus
    basis: str
    reasons: tuple[str, ...]
    confirmed_missing: int
    trades_observed: int
    loss_ratio: float
    loss_span_s: float
    silent_span_s: float
    silent_spans: int
    tail_silence_s: float
    legacy_gap_s: float


def _clip_merge_spans(
    gaps: Sequence[Mapping[str, object]], date: dt.date, regular_open: dt.time, regular_close: dt.time
) -> tuple[tuple[tuple[int, int], ...], float]:
    open_ns = int(dt.datetime.combine(date, regular_open, tzinfo=_KST).timestamp()) * 1_000_000_000
    close_ns = int(dt.datetime.combine(date, regular_close, tzinfo=_KST).timestamp()) * 1_000_000_000
    clipped: list[tuple[int, int]] = []
    for g in gaps:
        start = int(g["gap_start_ns"])  # type: ignore[call-overload]
        end = int(g["gap_end_ns"])  # type: ignore[call-overload]
        s = max(start, open_ns)
        e = min(end, close_ns)
        if e > s:
            clipped.append((s, e))
    clipped.sort()
    merged: list[list[int]] = []
    for s, e in clipped:
        if merged and s <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], e)
        else:
            merged.append([s, e])
    total_ns = sum(e - s for s, e in merged)
    return tuple((s, e) for s, e in merged), total_ns / 1e9


def _split_silent_loss_spans(
    merged: tuple[tuple[int, int], ...], audit: SessionAudit
) -> tuple[float, float, int]:
    windows: list[tuple[int, int]] = []
    for symbol in audit.symbols:
        for interval in symbol.intervals:
            if interval.phase != MarketPhase.REGULAR.value:
                continue
            windows.append((min(interval.recv_prev_ns, interval.recv_next_ns), max(interval.recv_prev_ns, interval.recv_next_ns)))
    silent_s = 0.0
    loss_s = 0.0
    silent_count = 0
    for s, e in merged:
        if any(s < hi and lo < e for lo, hi in windows):
            loss_s += (e - s) / 1e9
        else:
            silent_s += (e - s) / 1e9
            silent_count += 1
    return silent_s, loss_s, silent_count


def evaluate_session_gap_budget(
    *,
    gaps: Sequence[Mapping[str, object]],
    date: dt.date,
    regular_open: dt.time,
    regular_close: dt.time,
    audit: SessionAudit | None,
    settings: AuditSettings | None,
    max_gap_s: float = MAX_SESSION_GAP_S,
) -> GapBudgetVerdict:
    """Pure verdict from manifest spans and the session audit under `settings.mode`; legacy time budget is the fallback basis."""
    merged, legacy_gap_s = _clip_merge_spans(gaps, date, regular_open, regular_close)
    matched = audit is not None and audit.session_date == date and audit.status in (AuditStatus.OK, AuditStatus.PARTIAL)
    if matched:
        assert audit is not None
        confirmed = audit.totals.confirmed_missing
        trades = audit.totals.trades_observed
        loss_ratio = (confirmed / trades) if trades else 0.0
        tail_silence_s = audit.tail_silence_s if audit.tail_silence_s is not None else 0.0
        silent_span_s, loss_span_s, silent_spans = _split_silent_loss_spans(merged, audit)
    else:
        confirmed = 0
        trades = 0
        loss_ratio = 0.0
        tail_silence_s = 0.0
        silent_span_s = 0.0
        loss_span_s = legacy_gap_s
        silent_spans = 0
    ledger_active = (
        settings is not None and settings.mode is AuditMode.ENFORCE and matched and trades > 0
    )
    if not ledger_active:
        if legacy_gap_s > max_gap_s:
            return GapBudgetVerdict(
                GapBudgetStatus.FAIL, "legacy", (f"gap_exceeded:{int(legacy_gap_s)}s",),
                confirmed, trades, loss_ratio, loss_span_s, silent_span_s, silent_spans,
                tail_silence_s, legacy_gap_s,
            )
        return GapBudgetVerdict(
            GapBudgetStatus.PASS, "legacy", (), confirmed, trades, loss_ratio,
            loss_span_s, silent_span_s, silent_spans, tail_silence_s, legacy_gap_s,
        )
    assert audit is not None
    assert settings is not None
    reasons: list[str] = []
    failed = False
    if loss_ratio > settings.loss_fail_ratio:
        failed = True
        reasons.append(f"trade_loss_exceeded:{confirmed}/{trades}")
    breaching = [
        symbol for symbol in audit.symbols
        if symbol.status == "OK"
        and symbol.trades_observed >= settings.symbol_min_trades
        and symbol.trades_observed > 0
        and symbol.confirmed_missing / symbol.trades_observed > settings.symbol_loss_fail_ratio
    ]
    if breaching:
        failed = True
        ordered = sorted(breaching, key=lambda s: (s.confirmed_missing / s.trades_observed, s.confirmed_missing), reverse=True)
        worst = ordered[0]
        extra = f"+{len(ordered) - 1}" if len(ordered) > 1 else ""
        reasons.append(f"symbol_loss_exceeded:{worst.symbol}:{worst.confirmed_missing}/{worst.trades_observed}{extra}")
    if tail_silence_s > settings.tail_silence_max_s:
        failed = True
        reasons.append(f"tail_silence_exceeded:{int(tail_silence_s)}s")
    partial = audit.status is AuditStatus.PARTIAL
    if partial and legacy_gap_s > max_gap_s:
        failed = True
        reasons.insert(0, f"gap_exceeded:{int(legacy_gap_s)}s")
    status = GapBudgetStatus.FAIL if failed else GapBudgetStatus.PASS
    if not failed and loss_ratio > settings.loss_warn_ratio:
        status = GapBudgetStatus.WARN
        reasons.append(f"trade_loss_warn:{confirmed}/{trades}")
    return GapBudgetVerdict(
        status, "ledger+legacy" if partial else "ledger", tuple(reasons),
        confirmed, trades, loss_ratio, loss_span_s, silent_span_s, silent_spans,
        tail_silence_s, legacy_gap_s,
    )


def _log_stream_latency(entry: StreamLatency, *, threshold_s: float, date: dt.date) -> None:
    minutes = entry.minutes
    worst = max(minutes, key=lambda m: (m.max_s, m.minute_kst)) if minutes else None
    logger.info(
        "[DATA] stage=ingest_latency status=OK date=%s stream=%s minutes=%d worst_minute=%s max_s=%.1f p50_s=%.1f threshold_s=%.1f",
        date.isoformat(),
        entry.stream,
        len(minutes),
        worst.minute_kst if worst is not None else "-",
        worst.max_s if worst is not None else 0.0,
        worst.p50_s if worst is not None else 0.0,
        threshold_s,
    )


def log_session_audit(audit: SessionAudit | None, *, settings: AuditSettings, anchors: SessionAnchors, date: dt.date) -> None:
    """Emit the audit summary and per-stream latency lines; CRITICAL once per stream whose regular-window minutes exceed the latency threshold."""
    if audit is None or settings.mode is AuditMode.OFF:
        return
    try:
        totals = audit.totals
        status = audit.status.value
        reason = audit.reason or "-"
        tail_silence_s = audit.tail_silence_s if audit.tail_silence_s is not None else 0.0
        trades = totals.trades_observed
        confirmed = totals.confirmed_missing
        count_only = totals.count_only_missing
        loss_ratio = totals.loss_ratio
        with_loss = totals.symbols_with_loss
        no_counter = totals.symbols_without_counter
    except Exception:  # noqa: BLE001
        logger.warning("[DATA] stage=session_audit status=UNAVAILABLE reason=summary_error date=%s", date.isoformat())
        return
    if status == "OK":
        logger.info(
            "[DATA] stage=session_audit status=%s reason=%s date=%s mode=%s trades=%d confirmed_missing=%d count_only_missing=%d loss_ratio=%.6g symbols_with_loss=%d symbols_no_counter=%d tail_silence_s=%.1f",
            status,
            reason,
            date.isoformat(),
            settings.mode.value,
            trades,
            confirmed,
            count_only,
            loss_ratio,
            with_loss,
            no_counter,
            tail_silence_s,
        )
    else:
        logger.warning(
            "[DATA] stage=session_audit status=%s reason=%s date=%s mode=%s trades=%d confirmed_missing=%d count_only_missing=%d loss_ratio=%.6g symbols_with_loss=%d symbols_no_counter=%d tail_silence_s=%.1f",
            status,
            reason,
            date.isoformat(),
            settings.mode.value,
            trades,
            confirmed,
            count_only,
            loss_ratio,
            with_loss,
            no_counter,
            tail_silence_s,
        )
    for stream in dict.fromkeys(settings.latency_streams):
        try:
            entry = next(
                (item for item in audit.latency if item.stream == stream),
                StreamLatency(stream, AuditStatus.UNAVAILABLE, "unavailable", 0, 0, ()),
            )
            _log_stream_latency(entry, threshold_s=settings.latency_alert_threshold_s, date=date)
        except Exception:  # noqa: BLE001
            logger.debug("[DATA] stage=ingest_latency status=SKIP date=%s", date.isoformat())
            continue
    try:
        breaches = latency_breaches(
            audit,
            threshold_s=settings.latency_alert_threshold_s,
            window_start=anchors.regular_open,
            window_end=anchors.regular_close,
        )
    except Exception:  # noqa: BLE001
        return
    grouped: dict[str, list[LatencyBreach]] = {}
    for breach in breaches:
        grouped.setdefault(breach.stream, []).append(breach)
    for stream, items in grouped.items():
        worst = max(items, key=lambda b: (b.max_s, b.minute_kst))
        logger.critical(
            "[DATA] stage=ingest_latency status=FAIL reason=latency_exceeded date=%s stream=%s minute=%s max_s=%.1f p50_s=%.1f threshold_s=%.1f breach_minutes=%d",
            date.isoformat(),
            stream,
            worst.minute_kst,
            worst.max_s,
            worst.p50_s,
            settings.latency_alert_threshold_s,
            len(items),
        )


def classify_remote_failure(message: str) -> str:
    if "invalid_grant" in message or "couldn't fetch token" in message:
        return "auth_expired"
    return "remote_error"


def _aftermarket_streams_ok(loaded: SessionManifest) -> bool:
    venue = str(loaded.venue)
    required = {"H0STCNT0", "H0STASP0"} if venue == "krx" else {"H0NXCNT0", "H0NXASP0"} if venue == "nxt" else set()
    accepted = {str(a.get("tr_id")) for a in loaded.subscription_acks if a.get("accepted") is True}
    return bool(required) and required.issubset(accepted)


def _expected_aftermarket_shards_ready(
    loaded: dict[tuple[str, int], SessionManifest],
    *,
    date: dt.date,
    expected_shards: tuple[AftermarketShard, ...],
) -> bool:
    """모든 expected shard의 날짜·route·key_id·planned pairs·accepted ACK를 검증한다."""
    for shard in expected_shards:
        manifest = loaded.get((shard.venue.value, shard.shard_index))
        if manifest is None:
            return False
        expected_session = "nxt_after" if shard.venue.value == "nxt" else "krx_after"
        if manifest.venue != shard.venue.value or manifest.session != expected_session:
            return False
        if manifest.session_date != date:
            return False
        if manifest.credential_key_id != shard.credential_key_id:
            return False
        want = sorted(f"{symbol}:{stream}" for symbol in shard.symbols for stream in shard.streams)
        got = sorted(f"{pair.get('symbol')}:{pair.get('tr_id')}" for pair in manifest.planned_pairs)
        if got != want:
            return False
        if manifest.writer_closed_at_ns is None:
            return False
        accepted = {
            (str(ack.get("symbol")), str(ack.get("tr_id")))
            for ack in manifest.subscription_acks
            if ack.get("accepted") is True
        }
        if any((symbol, stream) not in accepted for symbol in shard.symbols for stream in shard.streams):
            return False
    return True


def aftermarket_eod_ready(
    *,
    manifests: list[pathlib.Path],
    date: dt.date,
    now: dt.datetime,
    expected_shards: tuple[AftermarketShard, ...] | None = None,
    after_market_end: dt.time = STANDARD_AFTER_MARKET_END,
) -> bool:
    # 그날 애프터마켓 종료(앵커) 전에는 EOD 준비 완료로 보지 않는다.
    if now.astimezone(_KST).time() < after_market_end:
        return False  # noqa: E701
    if not manifests:
        return False  # noqa: E701 - 대상 manifest 없이 성공 표기 금지
    closed: list[bool] = []
    routes: set[tuple[str, str]] = set()
    loaded: dict[tuple[str, int], SessionManifest] = {}
    for path in manifests:
        try:
            manifest = SessionManifest.load(pathlib.Path(path))
        except (ValueError, KeyError, TypeError, OSError) as exc:
            logger.critical("[DAEMON] stage=eod_readiness status=FAIL manifest=%s error=%s", str(path), str(exc))
            closed.append(False)
            continue
        routes.add((str(manifest.venue), str(manifest.session)))
        loaded[(str(manifest.venue), manifest.shard_index if manifest.shard_index is not None else -1)] = manifest
        closed.append(
            manifest.session_date == date
            and manifest.writer_closed_at_ns is not None
            and manifest.expected_close_ns > 0
            and _aftermarket_streams_ok(manifest)
        )
    if expected_shards:
        return (
            _expected_aftermarket_shards_ready(loaded, date=date, expected_shards=expected_shards)
            and len(closed) == len(expected_shards)
            and all(closed)
        )
    required_routes = {("krx", "krx_after"), ("nxt", "nxt_after")}
    return len(closed) == 2 and all(closed) and routes == required_routes


class PrearmVerdict(StrEnum):
    ARMED = "ARMED"
    LATE_START = "LATE_START"
    LATE_ACK = "LATE_ACK"
    UNVERIFIABLE = "UNVERIFIABLE"


@dataclass(frozen=True)
class AftermarketPrearmFinding:
    """Pre-arm verdict of one expected aftermarket shard."""

    venue: str
    shard_index: int
    verdict: PrearmVerdict
    open_at_ns: int
    started_at_ns: int | None
    initial_ack_ns: int | None
    reason: str


def check_aftermarket_prearm(
    *,
    expected: tuple[tuple[AftermarketShard, pathlib.Path], ...],
    date: dt.date,
    anchors: SessionAnchors,
) -> tuple[AftermarketPrearmFinding, ...]:
    """Classify each expected shard's manifest against its venue's shifted open; never raises on bad manifests."""
    findings: list[AftermarketPrearmFinding] = []
    for shard, path in expected:
        open_at_ns = int(
            dt.datetime.combine(date, aftermarket_open_time(shard.venue, anchors), tzinfo=_KST).timestamp()
            * 1_000_000_000
        )
        venue = shard.venue.value
        try:
            if not pathlib.Path(path).exists():
                findings.append(
                    AftermarketPrearmFinding(
                        venue, shard.shard_index, PrearmVerdict.UNVERIFIABLE, open_at_ns, None, None, "manifest_missing"
                    )
                )
                continue
            manifest = SessionManifest.load(pathlib.Path(path))
        except (ValueError, KeyError, TypeError, OSError, AttributeError, OverflowError):
            findings.append(
                AftermarketPrearmFinding(
                    venue, shard.shard_index, PrearmVerdict.UNVERIFIABLE, open_at_ns, None, None, "manifest_unreadable"
                )
            )
            continue
        if manifest.session_date != date:
            findings.append(
                AftermarketPrearmFinding(
                    venue,
                    shard.shard_index,
                    PrearmVerdict.UNVERIFIABLE,
                    open_at_ns,
                    manifest.started_at_ns,
                    None,
                    "date_mismatch",
                )
            )
            continue
        if manifest.venue != venue:
            findings.append(
                AftermarketPrearmFinding(
                    venue,
                    shard.shard_index,
                    PrearmVerdict.UNVERIFIABLE,
                    open_at_ns,
                    manifest.started_at_ns,
                    None,
                    "venue_mismatch",
                )
            )
            continue
        started_at_ns: int = manifest.started_at_ns
        if started_at_ns >= open_at_ns:
            findings.append(
                AftermarketPrearmFinding(
                    venue,
                    shard.shard_index,
                    PrearmVerdict.LATE_START,
                    open_at_ns,
                    started_at_ns,
                    None,
                    "started_after_open",
                )
            )
            continue
        per_pair: list[int] = []
        unverifiable: str | None = None
        try:
            for symbol in shard.symbols:
                for stream in shard.streams:
                    times = [
                        a.get("acked_at_ns")
                        for a in manifest.subscription_acks
                        if a.get("accepted") is True and str(a.get("symbol")) == symbol and str(a.get("tr_id")) == stream
                    ]
                    if not times:
                        unverifiable = "pair_unacked"
                        break
                    if any(t is None for t in times):
                        unverifiable = "ack_time_missing"
                        break
                    valid_times: list[int] = []
                    for t in times:
                        if type(t) is not int:
                            raise TypeError("ack time must be an integer")
                        valid_times.append(t)
                    per_pair.append(min(valid_times))
                if unverifiable is not None:
                    break
        except (ValueError, TypeError, AttributeError, OverflowError):
            unverifiable = "manifest_unreadable"
        if unverifiable is not None:
            findings.append(
                AftermarketPrearmFinding(
                    venue, shard.shard_index, PrearmVerdict.UNVERIFIABLE, open_at_ns, started_at_ns, None, unverifiable
                )
            )
            continue
        initial_ack_ns = max(per_pair) if per_pair else None
        if initial_ack_ns is not None and initial_ack_ns >= open_at_ns:
            findings.append(
                AftermarketPrearmFinding(
                    venue,
                    shard.shard_index,
                    PrearmVerdict.LATE_ACK,
                    open_at_ns,
                    started_at_ns,
                    initial_ack_ns,
                    "acked_after_open",
                )
            )
        else:
            findings.append(
                AftermarketPrearmFinding(
                    venue, shard.shard_index, PrearmVerdict.ARMED, open_at_ns, started_at_ns, initial_ack_ns, "-"
                )
            )
    return tuple(findings)


@dataclass(frozen=True)
class AftermarketCoverageFinding:
    """Zero-frame accepted symbols of one aftermarket shard (None when unverifiable)."""

    venue: str
    shard_index: int
    accepted_symbols: int
    zero_frame_symbols: tuple[str, ...] | None
    reason: str


def check_aftermarket_symbol_coverage(
    *,
    expected: tuple[tuple[AftermarketShard, pathlib.Path], ...],
    date: dt.date,
) -> tuple[AftermarketCoverageFinding, ...]:
    """Report accepted shard symbols with zero journaled frames on every stream; never raises on bad manifests."""
    findings: list[AftermarketCoverageFinding] = []
    for shard, path in expected:
        venue = shard.venue.value
        try:
            if not pathlib.Path(path).exists():
                findings.append(
                    AftermarketCoverageFinding(venue, shard.shard_index, 0, None, "manifest_missing")
                )
                continue
            manifest = SessionManifest.load(pathlib.Path(path))
            accepted = {
                str(ack.get("symbol")) for ack in manifest.subscription_acks if ack.get("accepted") is True
            }
            accepted_symbols = tuple(symbol for symbol in shard.symbols if symbol in accepted)
        except (ValueError, KeyError, TypeError, OSError, AttributeError, OverflowError):
            logger.debug("[DATA] stage=aftermarket_coverage status=UNVERIFIABLE venue=%s shard=%d", venue, shard.shard_index)
            findings.append(
                AftermarketCoverageFinding(venue, shard.shard_index, 0, None, "manifest_unreadable")
            )
            continue
        if manifest.session_date != date:
            logger.debug("[DATA] stage=aftermarket_coverage status=UNVERIFIABLE venue=%s shard=%d", venue, shard.shard_index)
            findings.append(
                AftermarketCoverageFinding(venue, shard.shard_index, 0, None, "date_mismatch")
            )
            continue
        if manifest.frame_counts is None:
            logger.debug("[DATA] stage=aftermarket_coverage status=UNVERIFIABLE venue=%s shard=%d", venue, shard.shard_index)
            findings.append(
                AftermarketCoverageFinding(venue, shard.shard_index, len(accepted_symbols), None, "untracked")
            )
            continue
        zero = tuple(
            symbol
            for symbol in accepted_symbols
            if manifest.symbol_frame_total(symbol) == 0
        )
        logger.debug("[DATA] stage=aftermarket_coverage status=CHECKED venue=%s shard=%d", venue, shard.shard_index)
        findings.append(
            AftermarketCoverageFinding(venue, shard.shard_index, len(accepted_symbols), zero, "-")
        )
    return tuple(findings)


def check_backup_freshness(*, manifest_dir: pathlib.Path, today: dt.date, archiver: Any = None) -> list[str]:
    arc = archiver if archiver is not None else RcloneArchiver.try_from_env()
    if arc is None:
        return []
    local: list[str] = []
    base = pathlib.Path(manifest_dir)
    for p in sorted(base.rglob("*.json")):
        rel = p.relative_to(base).as_posix()
        stem_date = p.name.split(".")[0]
        try:
            d = dt.date.fromisoformat(stem_date)
        except ValueError:
            continue
        if d < today:
            local.append(rel)
    remote = {p.removeprefix("manifests/") for p in arc.remote_files("manifests/")}
    return sorted(n for n in local if n not in remote)


def check_host_backup_freshness(*, status_path: pathlib.Path, now: dt.datetime, max_age: dt.timedelta) -> str | None:
    """Return a staleness reason when the host Drive backup has not succeeded recently.

    The host backup runs outside the container under a lock shared with other
    tenants, and its failure alert depends on another project's unit. The
    daemon therefore reads the status file the backup script writes after
    every attempt and raises its own alarm when success is overdue.

    Args:
        status_path: Status JSON written by ``deploy/host/krx-host-backup.sh``.
        now: Timezone-aware current time.
        max_age: Maximum tolerated age of ``last_ok_at``.

    Returns:
        ``None`` when ``last_ok_at`` is within ``max_age``; otherwise one of
        ``"missing"``, ``"unreadable"``, ``"never_succeeded"`` or
        ``"stale:<age_hours rounded to 1 decimal>h"``.

    Raises:
        ValueError: If ``now`` is naive.
    """
    if now.tzinfo is None or now.tzinfo.utcoffset(now) is None:
        raise ValueError("now must be timezone-aware")
    try:
        raw = pathlib.Path(status_path).read_text(encoding="utf-8")
    except OSError:
        return "missing"
    try:
        body = json.loads(raw)
        last_ok_raw = body.get("last_ok_at")
    except (ValueError, AttributeError):
        return "unreadable"
    if last_ok_raw is None:
        return "never_succeeded"
    if not isinstance(last_ok_raw, str):
        return "unreadable"
    try:
        last_ok = dt.datetime.fromisoformat(last_ok_raw)
    except ValueError:
        return "unreadable"
    if last_ok.tzinfo is None or last_ok.tzinfo.utcoffset(last_ok) is None:
        return "unreadable"
    age = now - last_ok
    if age.total_seconds() < 0 or age <= max_age:
        return None
    return f"stale:{age.total_seconds() / 3600:.1f}h"


def check_session_reconciliation(
    *,
    manifest_path: pathlib.Path,
    date: dt.date,
    journal_root: pathlib.Path,
    streams: tuple[str, ...],
    vendor: str,
    venue: str,
    session: str,
    regular_open: dt.time,
    regular_close: dt.time,
    max_gap_s: float = MAX_SESSION_GAP_S,
    failure_level: int = logging.CRITICAL,
    audit: SessionAudit | None = None,
    audit_settings: AuditSettings | None = None,
) -> bool:
    """Verify that a business-day regular session left a complete collection trail.

    The caller must invoke this only for dates the trading-day gate confirmed
    as business days. The bars store cannot answer that question at EOD
    because the day's bars are published the next morning.

    Checks, all of which must hold:
    - the session manifest exists and is readable;
    - every stream in ``streams`` has at least one ``*.jsonl.zst`` journal
      under ``journal_root/vendor/venue/session/stream/dt=<date>``;
    - the regular-session gap recorded in the manifest does not exceed
      ``max_gap_s``.

    Args:
        manifest_path: Session manifest for ``date``.
        date: Business date being reconciled (KST).
        journal_root: L0 journal root.
        streams: Stream identifiers expected for the regular session.
        vendor: Journal vendor directory (e.g. ``"ls"``).
        venue: Routed venue directory (e.g. ``"krx"``).
        session: Routed session directory (e.g. ``"regular"``).
        regular_open: Regular-session open bound for gap clipping (KST).
        regular_close: Regular-session close bound for gap clipping (KST).
        max_gap_s: Maximum tolerated regular-session gap in seconds.
        failure_level: Log level for the failure line; WARNING, ERROR or CRITICAL.
        audit: Session audit for count-based loss accounting; without it the
            check is purely time-budget based.
        audit_settings: Rollout mode and loss budget thresholds; without it the
            check is purely time-budget based.

    Returns:
        True when every check holds; otherwise False, after one
        ``[DATA] stage=session_reconciliation status=FAIL`` log at ``failure_level``
        listing the failed checks.

    Raises:
        ValueError: ``failure_level`` is not WARNING, ERROR or CRITICAL.
    """
    if failure_level not in (logging.WARNING, logging.ERROR, logging.CRITICAL):
        raise ValueError(f"failure_level must be WARNING, ERROR or CRITICAL: {failure_level}")
    manifest = pathlib.Path(manifest_path)
    root = pathlib.Path(journal_root)
    structural: list[str] = []
    if not manifest.exists():
        structural.append("manifest_missing")
    for stream in streams:
        key = l0_partition_key(vendor, venue, session, stream, date)
        if key is None or not list((root / l0_partition_relpath(key)).glob(L0_JOURNAL_GLOB)):
            structural.append(f"journal_missing:{stream}")
    gaps: list[dict[str, object]] = []
    if manifest.exists():
        try:
            loaded = SessionManifest.load(manifest)
            gaps = list(loaded.gaps)
            for g in gaps:
                int(g["gap_start_ns"])  # type: ignore[call-overload]
                int(g["gap_end_ns"])  # type: ignore[call-overload]
        except (ValueError, KeyError, TypeError):
            gaps = []
            structural.append("manifest_unreadable")
        except AttributeError:
            if audit is None or audit_settings is None or audit_settings.mode is AuditMode.OFF:
                raise
            gaps = []
            structural.append("manifest_unreadable")
    if audit is None or audit_settings is None or audit_settings.mode is AuditMode.OFF:
        legacy = evaluate_session_gap_budget(
            gaps=gaps, date=date, regular_open=regular_open, regular_close=regular_close,
            audit=None, settings=None, max_gap_s=max_gap_s,
        )
        issues = structural + list(legacy.reasons)
        if issues:
            logger.log(
                failure_level,
                "[DATA] stage=session_reconciliation status=FAIL date=%s reasons=%s",
                date.isoformat(),
                ",".join(issues),
            )
            return False
        return True
    mismatch = audit.session_date != date
    usable = not mismatch and audit.status in (AuditStatus.OK, AuditStatus.PARTIAL) and audit.totals.trades_observed > 0
    if not usable:
        logger.warning(
            "[DATA] stage=session_audit status=UNAVAILABLE reason=%s basis=legacy date=%s",
            "date_mismatch" if mismatch else audit.reason,
            date.isoformat(),
        )
        legacy = evaluate_session_gap_budget(
            gaps=gaps, date=date, regular_open=regular_open, regular_close=regular_close,
            audit=None, settings=None, max_gap_s=max_gap_s,
        )
        issues = structural + list(legacy.reasons)
        if issues:
            logger.log(
                failure_level,
                "[DATA] stage=session_reconciliation status=FAIL date=%s reasons=%s",
                date.isoformat(),
                ",".join(issues),
            )
            return False
        return True
    if audit_settings.mode is AuditMode.SHADOW:
        enforce_view = audit_settings.model_copy(update={"mode": AuditMode.ENFORCE})
        would = evaluate_session_gap_budget(
            gaps=gaps, date=date, regular_open=regular_open, regular_close=regular_close,
            audit=audit, settings=enforce_view, max_gap_s=max_gap_s,
        )
        legacy = evaluate_session_gap_budget(
            gaps=gaps, date=date, regular_open=regular_open, regular_close=regular_close,
            audit=None, settings=None, max_gap_s=max_gap_s,
        )
        would_label = would.status.value
        legacy_label = "FAIL" if legacy.status is GapBudgetStatus.FAIL else "PASS"
        shadow_reasons = ",".join(would.reasons) if would.reasons else "-"
        if would.status is GapBudgetStatus.FAIL:
            logger.warning(
                "[DATA] stage=session_audit mode=shadow date=%s would=%s legacy=%s reasons=%s",
                date.isoformat(),
                would_label,
                legacy_label,
                shadow_reasons,
            )
        else:
            logger.info(
                "[DATA] stage=session_audit mode=shadow date=%s would=%s legacy=%s reasons=%s",
                date.isoformat(),
                would_label,
                legacy_label,
                shadow_reasons,
            )
        issues = structural + list(legacy.reasons)
        if issues:
            logger.log(
                failure_level,
                "[DATA] stage=session_reconciliation status=FAIL date=%s reasons=%s",
                date.isoformat(),
                ",".join(issues),
            )
            return False
        return True
    verdict = evaluate_session_gap_budget(
        gaps=gaps, date=date, regular_open=regular_open, regular_close=regular_close,
        audit=audit, settings=audit_settings, max_gap_s=max_gap_s,
    )
    partial = " partial=1" if audit.status is AuditStatus.PARTIAL else ""
    logger.info(
        "[DATA] stage=session_audit date=%s silent_span_s=%.1f silent_spans=%d loss_span_s=%.1f legacy_gap_s=%.1f%s",
        date.isoformat(),
        verdict.silent_span_s,
        verdict.silent_spans,
        verdict.loss_span_s,
        verdict.legacy_gap_s,
        partial,
    )
    issues = structural + list(verdict.reasons)
    if structural or verdict.status is GapBudgetStatus.FAIL:
        logger.log(
            failure_level,
            "[DATA] stage=session_reconciliation status=FAIL date=%s reasons=%s%s",
            date.isoformat(),
            ",".join(issues),
            partial,
        )
        return False
    if verdict.status is GapBudgetStatus.WARN:
        logger.warning(
            "[DATA] stage=session_reconciliation status=WARN date=%s reasons=%s%s",
            date.isoformat(),
            ",".join(verdict.reasons),
            partial,
        )
    return True
