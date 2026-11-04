"""collect-intraday-discovery CLI subcommand (Kiwoom ka10032 intraday admission poller)."""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import pathlib
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from zoneinfo import ZoneInfo

import polars as pl
import requests

from deploy.shared.host_broker_contract import AsyncHostPacer, IssuedToken, admission_state_path, rate_for
from src.brokers.kis.auth import KisAppAuth
from src.brokers.kis.stack import build_kis_rest_stack
from src.core.config import (
    CollectorSettings,
    ExecutionSettings,
    IntradayAdmissionSettings,
    KisTokenSettings,
    KiwoomCredentials,
    RegularKisSettings,
    load_credentials,
    resolve_collector_runtime,
)
from src.core.errors import KrxAlphaError, MissingCredentialsError
from src.core.observability import EVENT
from src.core.paths import DataPaths
from src.core.session_anchors import resolve_session_anchors
from src.execution.contracts import KisApiError
from src.marketdata.host_token_store import STORE_FAILURE_TYPES, kiwoom_issued, make_kiwoom_store
from src.marketdata.kiwoom_ranking import KiwoomRankingRow, fetch_trade_amount_ranking
from src.marketdata.kiwoom_ticks import KiwoomTickTapeError, issue_kiwoom_token
from src.marketdata.partitioned_store import PartitionedStoreError, scan_month_partitions
from src.marketdata.snapshot_schema import SnapshotDataset
from src.realtime.kis_sharding import KisDataCredential, load_kis_data_credentials
from src.storage.snapshot_store import SnapshotStore, SnapshotStoreError
from src.universe.intraday import (
    Admission,
    AdmissionDecision,
    AdmissionState,
    DynamicEntry,
    DynamicPlan,
    decide_admissions,
    feed_capacities,
    qualifies,
    read_dynamic_admissions,
    state_from_ledger,
    write_dynamic_admissions,
)
from src.universe.membership import MembershipRow, append_membership, effective_tiers, read_membership
from src.universe.policy import ineligible_security_symbols
from src.universe.status_policy import classify_security_status

logger = logging.getLogger(__name__)

_KST = ZoneInfo("Asia/Seoul")
_KIWOOM_VENDOR: str = "kiwoom"
_KIWOOM_SCOPE: str = "ka10032"
_CONSECUTIVE_FAILURE_CRITICAL: int = 5
_CAPACITY_SOURCE: str = "intraday_capacity"
_POST_CLOSE_SOURCE: str = "post_close_screen"
_LIST_KIND: str = "kiwoom_trade_amount"
_SOURCE_TR: str = "ka10032"
_MARKET_DIV: str = "J"
_UNKNOWN_LEVEL: str = "unknown"
_LS_RESERVE_FEED: str = "ls:1"


def add_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register ``collect-intraday-discovery``."""
    parser = subparsers.add_parser("collect-intraday-discovery")
    parser.add_argument("--session-date", required=True)
    parser.set_defaults(handler=run)


@dataclass
class DiscoveryState:
    admission: AdmissionState
    reject_once: set[str] = field(default_factory=set)
    capacity_once: set[str] = field(default_factory=set)
    warning_levels: dict[str, str] = field(default_factory=dict)
    failures: int = 0
    polls: int = 0


class IntradayDiscovery:
    """Schedule-driven admission loop: admission polls up to the cutoff, then one post-close screen poll."""

    def __init__(
        self,
        *,
        cfg: IntradayAdmissionSettings,
        session_date: dt.date,
        open_time: dt.time,
        membership_path: pathlib.Path,
        ipc_path: pathlib.Path,
        fetch: Callable[[], tuple[KiwoomRankingRow, ...]],
        persist_ranking: Callable[[int, tuple[KiwoomRankingRow, ...]], None],
        status_of: Callable[[str], Mapping[str, object]] | None,
        persist_status: Callable[[Mapping[str, object], int], None],
        initial_state: AdmissionState,
        now: Callable[[], dt.datetime],
        wall_ns: Callable[[], int],
        sleep: Callable[[float], None],
    ) -> None:
        self._cfg = cfg
        self._session_date = session_date
        self._open_time = open_time
        self._membership_path = membership_path
        self._ipc_path = ipc_path
        self._fetch = fetch
        self._persist_ranking = persist_ranking
        self._status_of = status_of
        self._persist_status = persist_status
        self._now = now
        self._wall_ns = wall_ns
        self._sleep = sleep
        self._state = DiscoveryState(admission=initial_state)

    def run(self) -> int:
        """Run the scheduled admission polls, then the single post-close screen poll; returns 0."""
        cutoff = self._at(self._cfg.admission_cutoff)
        scheduled = self._at(self._open_time) + dt.timedelta(seconds=self._cfg.start_after_open_s)
        while scheduled <= cutoff:
            self._wait_until(scheduled)
            if self._now() > cutoff:
                break
            self._state.polls += 1
            self._admission_poll()
            scheduled += dt.timedelta(seconds=self._cfg.poll_interval_s)
        self._wait_until(self._at(self._cfg.post_close_screen_at))
        self._post_close_poll()
        return 0

    def _at(self, clock: dt.time) -> dt.datetime:
        return dt.datetime.combine(self._session_date, clock, tzinfo=_KST)

    def _wait_until(self, at: dt.datetime) -> None:
        remaining = (at - self._now()).total_seconds()
        if remaining > 0:
            self._sleep(remaining)

    def _poll(self) -> tuple[KiwoomRankingRow, ...] | None:
        try:
            rows = self._fetch()
        except (KiwoomTickTapeError, OSError) as exc:
            state = self._state
            state.failures += 1
            logger.warning(
                "[DATA] stage=intraday_discovery status=POLL_FAIL poll=%d consecutive=%d error_type=%s",
                state.polls,
                state.failures,
                type(exc).__name__,
                extra=EVENT,
            )
            if state.failures == _CONSECUTIVE_FAILURE_CRITICAL:
                logger.critical(
                    "[DATA] stage=intraday_discovery status=CRITICAL reason=consecutive_poll_failures count=%d",
                    state.failures,
                    extra=EVENT,
                )
            return None
        self._state.failures = 0
        self._persist_ranking(self._wall_ns(), rows)
        return rows

    def _admission_poll(self) -> None:
        rows = self._poll()
        if rows is None:
            return
        state = self._state
        ineligible_new = 0
        status_new = 0
        fresh: list[KiwoomRankingRow] = []
        for row in rows:
            if row.symbol in state.admission.collected or row.symbol in state.reject_once:
                continue
            if not qualifies(row, self._cfg):
                continue
            if row.symbol in state.admission.ineligible:
                state.reject_once.add(row.symbol)
                ineligible_new += 1
                continue
            if self._status_of is not None and row.symbol not in state.warning_levels:
                excluded, level = self._check_status(row.symbol)
                if excluded:
                    state.reject_once.add(row.symbol)
                    state.admission = AdmissionState(
                        collected=state.admission.collected,
                        ineligible=state.admission.ineligible | {row.symbol},
                        capacity_left=state.admission.capacity_left,
                    )
                    status_new += 1
                    continue
                state.warning_levels[row.symbol] = level
            fresh.append(row)
        decision = decide_admissions(fresh, state.admission, self._cfg)
        by_symbol = {row.symbol: row for row in fresh}
        capacity_rows = [
            by_symbol[symbol]
            for symbol, reason in decision.rejected
            if reason == "capacity" and symbol not in state.capacity_once
        ]
        state.capacity_once.update(row.symbol for row in capacity_rows)
        self._publish(decision, capacity_rows)
        if decision.admitted:
            logger.info(
                "[ALGO] stage=intraday_admission poll=%d admitted=%d rejected_capacity=%d rejected_ineligible=%d "
                "rejected_status=%d ls_left=%d kis_left=%d",
                state.polls,
                len(decision.admitted),
                len(capacity_rows),
                ineligible_new,
                status_new,
                state.admission.capacity_left.get(_LS_RESERVE_FEED, 0),
                _kis_left(state.admission.capacity_left, self._cfg.kis_dynamic_slot),
                extra=EVENT,
            )

    def _check_status(self, symbol: str) -> tuple[bool, str]:
        assert self._status_of is not None
        try:
            status = self._status_of(symbol)
        except (KisApiError, OSError) as exc:
            logger.warning(
                "[DATA] stage=intraday_status status=LOOKUP_FAIL error_type=%s", type(exc).__name__, extra=EVENT
            )
            return False, _UNKNOWN_LEVEL
        self._persist_status(status, self._wall_ns())
        verdict = classify_security_status(status)
        return verdict.exclude, verdict.warning_level.value

    def _publish(self, decision: AdmissionDecision, capacity_rows: list[KiwoomRankingRow]) -> None:
        state = self._state
        if not decision.admitted and not capacity_rows:
            return
        decided = self._wall_ns()
        rows = [_admitted_row(admission, decided, state.warning_levels) for admission in decision.admitted]
        rows.extend(_tier_c(row, source=_CAPACITY_SOURCE, decided=decided) for row in capacity_rows)
        try:
            append_membership(self._membership_path, rows)
        except (OSError, ValueError, KrxAlphaError) as exc:
            logger.error(
                "[DATA] stage=intraday_admission status=FAIL reason=membership_write error_type=%s",
                type(exc).__name__,
                extra=EVENT,
            )
            return
        if decision.admitted:
            self._publish_ipc(decision.admitted, decided)

    def _publish_ipc(self, admitted: tuple[Admission, ...], decided: int) -> None:
        state = self._state
        left = dict(state.admission.capacity_left)
        for admission in admitted:
            left[admission.feed] = left.get(admission.feed, 0) - 1
        state.admission = AdmissionState(
            collected=state.admission.collected | {admission.symbol for admission in admitted},
            ineligible=state.admission.ineligible,
            capacity_left=left,
        )
        try:
            current = read_dynamic_admissions(self._ipc_path)
            entries = tuple(
                DynamicEntry(symbol=admission.symbol, feed=admission.feed, decided_at_ns=decided)
                for admission in admitted
            )
            write_dynamic_admissions(self._ipc_path, DynamicPlan(rev=current.rev + 1, entries=current.entries + entries))
        except (OSError, KrxAlphaError) as exc:
            logger.error(
                "[DATA] stage=intraday_admission status=FAIL reason=ipc_write error_type=%s",
                type(exc).__name__,
                extra=EVENT,
            )

    def _post_close_poll(self) -> None:
        rows = self._poll()
        if rows is None:
            return
        ledger = effective_tiers(read_membership(self._membership_path))
        decided = self._wall_ns()
        screen = [
            _tier_c(row, source=_POST_CLOSE_SOURCE, decided=decided)
            for row in rows
            if qualifies(row, self._cfg) and row.symbol not in ledger
        ]
        if screen:
            try:
                append_membership(self._membership_path, screen)
            except (OSError, ValueError, KrxAlphaError) as exc:
                logger.error(
                    "[DATA] stage=intraday_post_close status=FAIL reason=membership_write error_type=%s",
                    type(exc).__name__,
                    extra=EVENT,
                )
        logger.info(
            "[ALGO] stage=intraday_post_close screened=%d tier_c=%d", len(rows), len(screen), extra=EVENT
        )


def _feed_fields(feed: str) -> tuple[int | None, str | None]:
    kind, _, index = feed.partition(":")
    if kind == "ls":
        return int(index), None
    return None, index


def _kis_left(capacity: Mapping[str, int], kis_slot: str | None) -> int:
    return capacity.get(f"kis:{kis_slot}", 0) if kis_slot is not None else 0


def _admitted_row(admission: Admission, decided: int, warning_levels: Mapping[str, str]) -> MembershipRow:
    ls_shard, kis_slot = _feed_fields(admission.feed)
    return MembershipRow(
        symbol=admission.symbol,
        tier="B",
        source="dynamic",
        decided_at_ns=decided,
        direction=admission.direction,
        warning_level=warning_levels.get(admission.symbol),
        ls_shard=ls_shard,
        kis_slot=kis_slot,
        trade_value_krw=admission.trade_value_krw,
        reason=None,
    )


def _tier_c(row: KiwoomRankingRow, *, source: str, decided: int) -> MembershipRow:
    return MembershipRow(
        symbol=row.symbol,
        tier="C",
        source=source,
        decided_at_ns=decided,
        direction="up" if row.change_pct > 0 else "down",
        warning_level=None,
        ls_shard=None,
        kis_slot=None,
        trade_value_krw=row.trade_value_krw,
        reason=None,
    )


def _ranking_rows(observed: int, session_date: dt.date, rows: tuple[KiwoomRankingRow, ...]) -> list[dict[str, object]]:
    return [
        {
            "session_date": session_date,
            "observed_at_ns": observed,
            "source_tr": _SOURCE_TR,
            "market_div_code": _MARKET_DIV,
            "list_kind": _LIST_KIND,
            "rank": row.rank,
            "symbol": row.symbol,
            "change_pct": row.change_pct,
            "trade_value_krw": row.trade_value_krw,
        }
        for row in rows
    ]


def _bars_ineligible(paths: DataPaths, session_date: dt.date) -> frozenset[str]:
    try:
        bars = (
            scan_month_partitions(paths.bars_daily_dir, max_date=session_date - dt.timedelta(days=1))
            .select(["date", "symbol", "stock_cert_kind", "section"])
            .collect()
        )
    except (PartitionedStoreError, OSError, pl.exceptions.PolarsError):
        return frozenset()
    return ineligible_security_symbols(bars)


def _today_status_excluded(store: SnapshotStore) -> frozenset[str]:
    frame = store.frame(SnapshotDataset.SECURITY_STATUS)
    if frame.is_empty():
        return frozenset()
    return frozenset(
        str(row["symbol"]) for row in frame.iter_rows(named=True) if classify_security_status(row).exclude
    )


def _slot_credential(slot: str, credentials: tuple[KisDataCredential, ...]) -> KisDataCredential:
    cred = next((c for c in credentials if c.slot == slot), None)
    if cred is None:
        raise MissingCredentialsError(f"no KIS data credential for dynamic slot {slot}")
    return cred


def run(args: argparse.Namespace) -> int:
    """Poll ka10032 on schedule and admit qualifiers until the post-close screen; returns 0.

    Raises:
        KrxAlphaError: Intraday discovery is disabled.
        MissingCredentialsError: Kiwoom credentials or the dynamic KIS slot credential are absent.
    """
    return build_discovery(args, now=lambda: dt.datetime.now(_KST), sleep=time.sleep).run()


def build_discovery(
    args: argparse.Namespace,
    *,
    now: Callable[[], dt.datetime],
    sleep: Callable[[float], None],
) -> IntradayDiscovery:
    """Wire the real vendor, storage and credential boundaries around one discovery loop.

    Raises:
        KrxAlphaError: Intraday discovery is disabled.
        MissingCredentialsError: Kiwoom credentials or the dynamic KIS slot credential are absent.
    """
    cfg = IntradayAdmissionSettings()
    if not cfg.enabled:
        raise KrxAlphaError("intraday discovery is disabled")
    runtime = resolve_collector_runtime()
    collector: CollectorSettings = runtime.collector
    regular_kis: RegularKisSettings = runtime.regular_kis
    paths = runtime.paths
    session_date = dt.date.fromisoformat(str(args.session_date))
    anchors = resolve_session_anchors(paths.session_calendar_dir, session_date)
    membership_path = paths.membership_path(session_date)
    ipc_path = paths.dynamic_admissions(session_date)
    kiwoom = load_credentials(KiwoomCredentials)
    token_settings = KisTokenSettings()
    timeout_s = float(collector.kis_ack_timeout_s)
    pacer = AsyncHostPacer(
        admission_state_path(token_settings.token_cache_dir, _KIWOOM_VENDOR, kiwoom.kiwoom_app_key, _KIWOOM_SCOPE),
        rate_for(_KIWOOM_VENDOR, _KIWOOM_SCOPE),
    )
    token_store = make_kiwoom_store(
        token_settings.token_cache_dir, kiwoom.kiwoom_app_key, clock=lambda: dt.datetime.now(dt.UTC)
    )

    def _issue_token() -> IssuedToken:
        return kiwoom_issued(
            issue_kiwoom_token(
                app_key=kiwoom.kiwoom_app_key,
                secret_key=kiwoom.kiwoom_secret_key,
                session=requests,
                timeout_s=timeout_s,
            )
        )

    try:
        token = token_store.get_or_issue(_issue_token).access_token
    except STORE_FAILURE_TYPES as exc:
        raise KiwoomTickTapeError("kiwoom token store failed") from exc
    store = SnapshotStore(paths=paths, session_date=session_date)
    kis_slot = cfg.kis_dynamic_slot if regular_kis.enabled else None
    kis_slot_symbols = (regular_kis.pair_capacity_per_connection or 0) // len(regular_kis.streams)
    ls_reserve = collector.ls_dynamic_reserve_symbols if collector.ls_active_sessions >= 2 else 0
    capacity = feed_capacities(ls_reserve_symbols=ls_reserve, kis_slot_symbols=kis_slot_symbols, kis_slot=kis_slot)
    ineligible = _bars_ineligible(paths, session_date)
    if cfg.status_check:
        ineligible = ineligible | _today_status_excluded(store)
    initial = state_from_ledger(read_membership(membership_path), ineligible=ineligible, capacity=capacity)
    status_of: Callable[[str], Mapping[str, object]] | None = None
    if cfg.status_check and cfg.kis_dynamic_slot is not None:
        cred = _slot_credential(cfg.kis_dynamic_slot, load_kis_data_credentials())
        stack = build_kis_rest_stack(
            auth=KisAppAuth(cred.app_key, cred.app_secret),
            cache_dir=token_settings.token_cache_dir,
            session=requests,
            now=now,
            rate_per_s=ExecutionSettings(data_root=paths.root).rest_rate_per_s,
            max_lead_s=collector.kis_rest_max_lead_s,
            timeout_s=timeout_s,
            allow_issue=token_settings.allow_issue,
        )
        status_of = stack.data.get_security_status

    def fetch() -> tuple[KiwoomRankingRow, ...]:
        return fetch_trade_amount_ranking(
            session=requests,
            access_token=token,
            app_key=kiwoom.kiwoom_app_key,
            pacer=pacer,
            max_pages=cfg.max_pages,
            min_trade_value_krw=cfg.min_trade_value_krw,
            timeout_s=timeout_s,
        )

    def persist_ranking(observed: int, rows: tuple[KiwoomRankingRow, ...]) -> None:
        try:
            store.append(SnapshotDataset.RANKING, _ranking_rows(observed, session_date, rows))
        except SnapshotStoreError as exc:
            logger.error("[DATA] stage=intraday_discovery status=PERSIST_FAIL reason=%s", str(exc), extra=EVENT)

    def persist_status(status: Mapping[str, object], observed: int) -> None:
        try:
            store.append(
                SnapshotDataset.SECURITY_STATUS,
                [{**status, "session_date": session_date, "observed_at_ns": observed}],
            )
        except SnapshotStoreError as exc:
            logger.error("[DATA] stage=intraday_status status=PERSIST_FAIL reason=%s", str(exc), extra=EVENT)

    return IntradayDiscovery(
        cfg=cfg,
        session_date=session_date,
        open_time=anchors.regular_open,
        membership_path=membership_path,
        ipc_path=ipc_path,
        fetch=fetch,
        persist_ranking=persist_ranking,
        status_of=status_of,
        persist_status=persist_status,
        initial_state=initial,
        now=now,
        wall_ns=time.time_ns,
        sleep=sleep,
    )


__all__ = ["DiscoveryState", "IntradayDiscovery", "add_parser", "run"]
