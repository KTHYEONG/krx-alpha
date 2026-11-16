"""Feed failover supervisor: feeds live health into the pure controller and applies its admissions and recycles."""

from __future__ import annotations

import dataclasses
import datetime as dt
import json
import logging
import math
import os
import pathlib
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any
from zoneinfo import ZoneInfo

from src.core.alerts import PageKind, page
from src.core.config import CollectorSettings, FeedControlSettings, RegularKisSettings
from src.core.observability import EVENT
from src.core.paths import DataPaths
from src.core.session_anchors import SessionAnchors
from src.orchestration.feed_control.controller import initial_state, step
from src.orchestration.feed_control.model import (
    Action,
    ActionKind,
    ControllerState,
    FeedHealth,
    FeedStateError,
    GroupSpec,
    SecondaryCapacity,
    StepResult,
    WindowContext,
)
from src.realtime.feed_health import SCHEMA as FEED_HEALTH_SCHEMA
from src.realtime.kis_sharding import AftermarketShard
from src.realtime.ls_sharding import LsShard
from src.universe.intraday import DynamicEntry, DynamicPlan, read_dynamic_admissions, write_dynamic_admissions
from src.universe.ipc import CandidateFileError, read_candidates

logger = logging.getLogger(__name__)

STUCK_AFTER_TICKS = 30
_KST = ZoneInfo("Asia/Seoul")
_NS_PER_S = 1_000_000_000
_LOG_EVERY_S = 60.0
_PERSIST_EVERY_S = 60.0
_MAX_HEALTH_BYTES = 64 * 1024
_HEAD_SYMBOLS = 5


@dataclass(frozen=True, slots=True)
class SecondaryRoute:
    feed_id: str
    vendor: str
    venues: frozenset[str]
    pair_capacity: int


@dataclass(frozen=True, slots=True)
class PrimaryView:
    running: bool
    circuit_open: bool
    auth_rejected: bool


def _secondary_routes(regular_kis: RegularKisSettings) -> dict[str, SecondaryRoute]:
    capacity = int(regular_kis.pair_capacity_per_connection or 0)
    return {"kis:5": SecondaryRoute("kis:5", "kis", frozenset({"krx", "nxt"}), capacity)}


def _venue_locks(plan: DynamicPlan) -> dict[str, str]:
    """Venue each dynamic feed is bound to for the day: the venue of its first admission (entries are append-only)."""
    locks: dict[str, str] = {}
    for entry in plan.entries:
        locks.setdefault(entry.feed, entry.venue)
    return locks


def read_feed_health(path: pathlib.Path, session_date: dt.date) -> FeedHealth | None:
    """Parse one `feed-health.v1` file; None when absent, unreadable, oversized, or for another session."""
    try:
        if path.stat().st_size > _MAX_HEALTH_BYTES:
            return None
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(raw, dict) or raw.get("schema") != FEED_HEALTH_SCHEMA:
        return None
    if raw.get("session_date") != session_date.isoformat():
        return None
    try:
        streams = {str(name): _required_float(value) for name, value in dict(raw["stream_last_frame_wall"]).items()}
        return FeedHealth(
            feed_id=str(raw["feed_id"]),
            vendor=str(raw["vendor"]),
            updated_wall=_optional_float(raw["updated_wall"]),
            running=False,
            connected=bool(raw["connected"]),
            circuit_open=False,
            auth_rejected=False,
            last_frame_wall=_optional_float(raw["last_frame_wall"]),
            stream_last_frame_wall=streams,
            lag_p50_s=_optional_float(raw["lag_p50_s"]),
            lag_p99_s=_optional_float(raw["lag_p99_s"]),
            accepted_pairs=int(raw["accepted_pairs"]),
            planned_pairs=int(raw["planned_pairs"]),
            rejected_pairs=int(raw["rejected_pairs"]),
            pairs_pending_since_wall=_optional_float(raw["pairs_pending_since_wall"]),
        )
    except (KeyError, TypeError, ValueError, AttributeError):
        return None


def _optional_float(value: Any) -> float | None:
    return None if value is None else _required_float(value)


def _required_float(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float) or not math.isfinite(value):
        raise ValueError("health numeric field is malformed")
    return float(value)


class FeedFailoverSupervisor:
    """Runs one controller step per daemon tick; never raises, and writes only when secondary feeds are configured."""

    def __init__(
        self,
        *,
        paths: DataPaths,
        settings: FeedControlSettings,
        collector: CollectorSettings,
        regular_kis: RegularKisSettings,
        stop_secondary: Callable[[str], None],
        wall: Callable[[], float] = time.time,
    ) -> None:
        self._paths = paths
        self._settings = settings
        self._collector = collector
        self._routes = _secondary_routes(regular_kis)
        self._streams = tuple(regular_kis.streams)
        self._stop_secondary = stop_secondary
        self._wall = wall
        self._state: ControllerState | None = None
        self._loaded = False
        self._groups_cache: tuple[object, tuple[GroupSpec, ...]] | None = None
        self._last_persist_wall: float | None = None
        self._saved_day: dt.date | None = None
        self._failures = 0
        self._last_fail_log_wall: float | None = None
        self._stuck_paged_day: dt.date | None = None
        self._plan_day: dt.date | None = None
        self._written_plans: dict[str, tuple[str, ...]] = {}

    def tick(
        self,
        *,
        now: dt.datetime,
        today: dt.date,
        anchors: SessionAnchors,
        ls_plan: Sequence[LsShard],
        kis_plan: Sequence[AftermarketShard],
        nxt_plan: Sequence[AftermarketShard],
        primaries: Mapping[str, PrimaryView],
    ) -> None:
        if not self._settings.watch_groups:
            return
        try:
            self._tick(
                now=now,
                today=today,
                anchors=anchors,
                ls_plan=ls_plan,
                kis_plan=kis_plan,
                nxt_plan=nxt_plan,
                primaries=primaries,
            )
        except Exception as exc:  # noqa: BLE001 - failover is advisory; the primary watchdogs keep full authority
            self._on_tick_failure(exc, today)
        else:
            self._failures = 0

    def _on_tick_failure(self, exc: Exception, today: dt.date) -> None:
        self._failures += 1
        wall_now = self._wall()
        if self._last_fail_log_wall is None or wall_now - self._last_fail_log_wall >= _LOG_EVERY_S:
            self._last_fail_log_wall = wall_now
            logger.warning("[SYS] stage=feed_control status=TICK_FAIL reason=%s", type(exc).__name__)
        if self._failures >= STUCK_AFTER_TICKS and self._stuck_paged_day != today:
            self._stuck_paged_day = today
            logger.critical(
                "[SYS] stage=feed_control status=FAIL reason=stuck failures=%d date=%s",
                self._failures,
                today.isoformat(),
                extra=page(PageKind.FEED_CONTROLLER_STUCK),
            )

    def _tick(
        self,
        *,
        now: dt.datetime,
        today: dt.date,
        anchors: SessionAnchors,
        ls_plan: Sequence[LsShard],
        kis_plan: Sequence[AftermarketShard],
        nxt_plan: Sequence[AftermarketShard],
        primaries: Mapping[str, PrimaryView],
    ) -> None:
        now_wall = now.timestamp()
        plan = read_dynamic_admissions(self._paths.dynamic_admissions(today))
        groups = self._group_specs(ls_plan, kis_plan, nxt_plan, today, _venue_locks(plan))
        state = self._current_state(today, groups, now_wall)
        health = self._health(today, groups, primaries)
        ctx = self._window(today, now_wall, anchors)
        capacity = self._capacity(plan)
        groups, result = self._resolve_venue_conflicts(groups, state, health, capacity, ctx, now_wall)
        active = bool(self._settings.secondary_feeds)
        mode = "active" if active else "observe"
        new_state = result.state
        admits = [action for action in result.actions if action.kind is ActionKind.ADMIT]
        if admits and active:
            venue_of = {spec.group_id: spec.venue for spec in groups}
            new_state = self._write_admissions(today, new_state, admits, now_wall, venue_of)
        self._state = new_state
        if result.transitions or result.actions or new_state.day != self._saved_day or self._persist_due(now_wall):
            self._save_state(new_state)
            self._last_persist_wall = now_wall
        if self._plan_day != today:
            self._plan_day = today
            self._written_plans = {}
        rows = self._group_plan_rows(groups, mode, now_wall) + self._journal_rows(result, mode, new_state.plan_rev, now_wall)
        if rows:
            self._append_journal(today, rows)
        for action in result.actions:
            self._apply_action(action, mode, active)

    def _resolve_venue_conflicts(
        self,
        groups: tuple[GroupSpec, ...],
        state: ControllerState,
        health: Mapping[str, FeedHealth],
        capacity: Mapping[str, SecondaryCapacity],
        ctx: WindowContext,
        now_wall: float,
    ) -> tuple[tuple[GroupSpec, ...], StepResult]:
        """Keep a feed on one venue per tick: the first admitted venue wins and the other venue's admissions are refused."""
        result = step(state, groups, health, capacity, ctx, self._settings)
        venue_of = {spec.group_id: spec.venue for spec in groups}
        winners: dict[str, str] = {}
        for action in result.actions:
            if action.kind is ActionKind.ADMIT and action.feed_id is not None:
                winners.setdefault(action.feed_id, venue_of[action.group_id])
        refused = [
            action
            for action in result.actions
            if action.kind is ActionKind.ADMIT
            and action.feed_id is not None
            and venue_of[action.group_id] != winners[str(action.feed_id)]
        ]
        if not refused:
            return groups, result
        for action in refused:
            logger.warning(
                "[DATA] stage=feed_control status=REFUSED group=%s feed=%s symbols_n=%d reason=capacity_exhausted venue=%s locked_venue=%s",
                action.group_id,
                action.feed_id,
                len(action.symbols),
                venue_of[action.group_id],
                winners[str(action.feed_id)],
                extra=EVENT,
            )
        trimmed = tuple(
            dataclasses.replace(
                spec,
                secondaries=tuple(
                    feed for feed in spec.secondaries if winners.get(feed, spec.venue) == spec.venue
                ),
            )
            for spec in groups
        )
        return trimmed, step(state, trimmed, health, capacity, ctx, self._settings)

    def _current_state(self, today: dt.date, groups: Sequence[GroupSpec], now_wall: float) -> ControllerState:
        if not self._loaded:
            self._loaded = True
            loaded = self._load_state(today)
            if loaded is not None:
                self._state = loaded
        if self._state is None or self._state.day != today:
            self._state = initial_state(today, groups, now_wall=now_wall)
        return self._state

    def _load_state(self, today: dt.date) -> ControllerState | None:
        path = self._paths.feed_controller_state_path()
        try:
            text = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return None
        try:
            state = ControllerState.from_json(json.loads(text))
        except (ValueError, TypeError, KeyError, FeedStateError):
            self._move_aside(path)
            return None
        if state.day != today:
            return None
        return self._reconcile_with_plan(state, today)

    def _move_aside(self, path: pathlib.Path) -> None:
        target = path.with_name(f"{path.name}.corrupt-{int(self._wall())}")
        try:
            path.rename(target)
        except OSError as exc:
            logger.warning("[SYS] stage=feed_control status=WARN reason=state_move_failed error_type=%s", type(exc).__name__)
            return
        logger.info("[SYS] stage=feed_control status=INFO reason=state_corrupt_moved_aside", extra=EVENT)

    def _reconcile_with_plan(self, state: ControllerState, today: dt.date) -> ControllerState:
        plan = read_dynamic_admissions(self._paths.dynamic_admissions(today))
        groups = {}
        for group_id, runtime in state.groups.items():
            admitted = {}
            for feed, symbols in runtime.admitted.items():
                planned = set(plan.symbols_for(feed))
                kept = tuple(symbol for symbol in symbols if symbol in planned)
                if kept:
                    admitted[feed] = kept
            groups[group_id] = dataclasses.replace(runtime, admitted=admitted)
        return dataclasses.replace(state, groups=groups)

    def _group_specs(
        self,
        ls_plan: Sequence[LsShard],
        kis_plan: Sequence[AftermarketShard],
        nxt_plan: Sequence[AftermarketShard],
        today: dt.date,
        locks: Mapping[str, str],
    ) -> tuple[GroupSpec, ...]:
        pool = self._nxt_pool(today) if nxt_plan else None
        key = (tuple(ls_plan), tuple(kis_plan), tuple(nxt_plan), today, tuple(sorted(locks.items())), pool)
        if self._groups_cache is not None and self._groups_cache[0] == key:
            return self._groups_cache[1]
        watch = set(self._settings.watch_groups)
        specs: list[GroupSpec] = []
        for ls_shard in ls_plan:
            group_id = f"ls:{ls_shard.shard_index}"
            if group_id in watch and ls_shard.symbols:
                specs.append(self._spec(group_id, group_id, "krx", ls_shard.symbols, ls_shard.streams, locks))
        for krx_shard in kis_plan:
            group_id = f"kis-krx:{krx_shard.shard_index}"
            if group_id in watch and krx_shard.symbols:
                specs.append(self._spec(group_id, group_id, "krx", krx_shard.symbols, krx_shard.streams, locks))
        for nxt_shard in nxt_plan:
            group_id = f"kis-nxt:{nxt_shard.shard_index}"
            if group_id in watch and nxt_shard.symbols:
                symbols = self._within_nxt_pool(group_id, nxt_shard.symbols, pool or frozenset())
                specs.append(self._spec(group_id, group_id, "nxt", symbols, nxt_shard.streams, locks))
        result = tuple(specs)
        self._groups_cache = (key, result)
        return result

    def _nxt_pool(self, today: dt.date) -> frozenset[str]:
        try:
            data = read_candidates(self._paths.regular_nxt_candidates(today))
        except CandidateFileError:
            return frozenset()
        rows = data.get("candidates") if isinstance(data, dict) else None
        if not isinstance(rows, list):
            return frozenset()
        return frozenset(str(row["symbol"]) for row in rows if isinstance(row, dict) and isinstance(row.get("symbol"), str))

    def _within_nxt_pool(self, group_id: str, symbols: Sequence[str], pool: frozenset[str]) -> tuple[str, ...]:
        kept = tuple(symbol for symbol in symbols if symbol in pool)
        dropped = tuple(symbol for symbol in symbols if symbol not in pool)
        if dropped:
            logger.info(
                "[DATA] stage=feed_control status=INFO group=%s reason=outside_nxt_pool dropped_n=%d dropped=%s",
                group_id,
                len(dropped),
                ",".join(dropped[:_HEAD_SYMBOLS]),
                extra=EVENT,
            )
        return kept

    def _spec(
        self,
        group_id: str,
        primary: str,
        venue: str,
        symbols: Sequence[str],
        streams: Sequence[str],
        locks: Mapping[str, str],
    ) -> GroupSpec:
        configured = self._settings.secondary_feeds or tuple(self._routes)
        secondaries = tuple(
            feed
            for feed in configured
            if feed in self._routes and venue in self._routes[feed].venues and locks.get(feed, venue) == venue
        )
        return GroupSpec(
            group_id=group_id,
            primary_feed=primary,
            venue=venue,
            session="regular",
            symbols=tuple(symbols),
            streams=tuple(streams),
            secondaries=secondaries,
        )

    def _health(
        self,
        today: dt.date,
        groups: Sequence[GroupSpec],
        primaries: Mapping[str, PrimaryView],
    ) -> dict[str, FeedHealth]:
        feeds = {spec.primary_feed for spec in groups} | set(self._routes)
        result: dict[str, FeedHealth] = {}
        for feed in feeds:
            found = read_feed_health(self._paths.feed_health_path(feed), today)
            if found is None:
                continue
            view = primaries.get(feed)
            if view is None:
                result[feed] = found
            else:
                result[feed] = dataclasses.replace(
                    found,
                    running=view.running,
                    circuit_open=view.circuit_open,
                    auth_rejected=view.auth_rejected,
                )
        return result

    def _window(self, today: dt.date, now_wall: float, anchors: SessionAnchors) -> WindowContext:
        open_wall = _wall_at(today, anchors.regular_open)
        close_wall = _wall_at(today, anchors.regular_close)
        auction_wall = _wall_at(today, anchors.closing_auction_start)
        lead_s = self._collector.regular_open_standby_lead_s
        hold_s = self._collector.regular_open_standby_hold_s
        in_standby = open_wall - lead_s <= now_wall <= open_wall + hold_s
        in_auction = auction_wall <= now_wall < close_wall
        return WindowContext(
            day=today,
            now_wall=now_wall,
            in_session=open_wall <= now_wall < close_wall,
            in_high_risk=in_standby or in_auction,
            session_open_wall=open_wall,
            session_close_wall=close_wall,
        )

    def _capacity(self, plan: DynamicPlan) -> dict[str, SecondaryCapacity]:
        result: dict[str, SecondaryCapacity] = {}
        for feed, route in self._routes.items():
            admitted = frozenset(
                (symbol, stream) for symbol in plan.symbols_for(feed) for stream in self._streams
            )
            result[feed] = SecondaryCapacity(
                feed_id=feed,
                free_pairs=max(0, route.pair_capacity - len(admitted)),
                admitted=admitted,
            )
        return result

    def _write_admissions(
        self,
        today: dt.date,
        state: ControllerState,
        admits: Sequence[Action],
        now_wall: float,
        venue_of: Mapping[str, str],
    ) -> ControllerState:
        path = self._paths.dynamic_admissions(today)
        current = read_dynamic_admissions(path)
        decided_ns = int(now_wall * _NS_PER_S)
        new_entries = tuple(
            DynamicEntry(
                symbol=symbol,
                feed=_admit_feed(action),
                decided_at_ns=decided_ns,
                venue=venue_of[action.group_id],
            )
            for action in admits
            for symbol in action.symbols
        )
        rev = max(current.rev + 1, state.plan_rev)
        write_dynamic_admissions(path, DynamicPlan(rev=rev, entries=current.entries + new_entries))
        return dataclasses.replace(state, plan_rev=rev)

    def _persist_due(self, now_wall: float) -> bool:
        return self._last_persist_wall is None or now_wall - self._last_persist_wall >= _PERSIST_EVERY_S

    def _save_state(self, state: ControllerState) -> None:
        path = self._paths.feed_controller_state_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        tmp.write_text(json.dumps(state.to_json(), ensure_ascii=False, sort_keys=True), encoding="utf-8")
        os.replace(tmp, path)
        self._saved_day = state.day

    def _journal_rows(self, result: StepResult, mode: str, plan_rev: int, now_wall: float) -> list[dict[str, Any]]:
        transitions: list[dict[str, Any]] = [
            {
                "wall": now_wall,
                "mode": mode,
                "kind": "TRANSITION",
                "group": transition.group_id,
                "old": transition.old.value,
                "new": transition.new.value,
                "reason": transition.reason,
                "feed": None,
                "symbols_n": 0,
                "symbols_head": [],
                "plan_rev": plan_rev,
            }
            for transition in result.transitions
        ]
        actions = [
            {
                "wall": now_wall,
                "mode": mode,
                "kind": action.kind.value,
                "group": action.group_id,
                "old": None,
                "new": None,
                "reason": action.reason,
                "feed": action.feed_id,
                "symbols_n": len(action.symbols),
                "symbols_head": list(action.symbols[:_HEAD_SYMBOLS]),
                "symbols": list(action.symbols) if action.kind is ActionKind.ADMIT else None,
                "plan_rev": plan_rev,
            }
            for action in result.actions
        ]
        return transitions + actions

    def _group_plan_rows(self, groups: Sequence[GroupSpec], mode: str, now_wall: float) -> list[dict[str, Any]]:
        """One `group_plan` row per watched group whenever its symbol list changes; the list is re-emitted on each new day."""
        rows: list[dict[str, Any]] = []
        for spec in groups:
            if self._written_plans.get(spec.group_id) == spec.symbols:
                continue
            self._written_plans[spec.group_id] = spec.symbols
            rows.append(
                {
                    "wall": now_wall,
                    "mode": mode,
                    "kind": "group_plan",
                    "group": spec.group_id,
                    "old": None,
                    "new": None,
                    "reason": "plan",
                    "feed": None,
                    "symbols_n": len(spec.symbols),
                    "symbols_head": list(spec.symbols[:_HEAD_SYMBOLS]),
                    "symbols": list(spec.symbols),
                    "plan_rev": None,
                }
            )
        return rows

    def _append_journal(self, today: dt.date, rows: Sequence[dict[str, Any]]) -> None:
        path = self._paths.feed_control_log_path(today)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")

    def _apply_action(self, action: Action, mode: str, active: bool) -> None:
        if action.kind is ActionKind.ADMIT:
            status = "ADMITTED" if active else "WOULD_ADMIT"
            logger.info(
                "[DATA] stage=feed_control status=%s group=%s feed=%s symbols_n=%d reason=%s",
                status,
                action.group_id,
                action.feed_id,
                len(action.symbols),
                action.reason,
                extra=EVENT,
            )
        elif action.kind is ActionKind.RECYCLE:
            self._stop_secondary(_admit_feed(action))
            logger.info(
                "[DATA] stage=feed_control status=RECYCLED group=%s feed=%s reason=%s",
                action.group_id,
                action.feed_id,
                action.reason,
                extra=EVENT,
            )
        elif action.kind is ActionKind.PAGE:
            logger.critical(
                "[DATA] stage=feed_control status=FAIL group=%s shard=%s reason=%s",
                action.group_id,
                action.group_id,
                action.reason,
                extra=page(PageKind.FEED_GROUP_DOWN),
            )
        else:
            logger.info(
                "[DATA] stage=feed_control status=INFO group=%s reason=%s mode=%s",
                action.group_id,
                action.reason,
                mode,
                extra=EVENT,
            )


def _admit_feed(action: Action) -> str:
    assert action.feed_id is not None, "admit and recycle actions always name a feed"
    return action.feed_id


def _wall_at(day: dt.date, at: dt.time) -> float:
    return dt.datetime.combine(day, at, tzinfo=_KST).timestamp()
