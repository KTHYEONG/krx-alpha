"""Intraday dynamic admission policy and its atomic dynamic-admissions IPC file."""

from __future__ import annotations

import json
import os
import pathlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal

from src.core.config import IntradayAdmissionSettings
from src.core.errors import KrxAlphaError
from src.core.symbols import is_krx_short_code
from src.marketdata.kiwoom_ranking import KiwoomRankingRow
from src.universe.membership import MembershipRow, effective_tiers

Direction = Literal["up", "down"]

_COLLECTED_TIERS: frozenset[str] = frozenset({"A", "B"})
_ADMITTED_SOURCES: frozenset[str] = frozenset({"static", "dynamic"})


class DynamicAdmissionsFileError(KrxAlphaError):
    """Dynamic-admissions IPC file unreadable or violating its schema."""


@dataclass(frozen=True)
class AdmissionState:
    collected: frozenset[str]
    ineligible: frozenset[str]
    capacity_left: Mapping[str, int]


@dataclass(frozen=True)
class Admission:
    symbol: str
    feed: str
    change_pct: float
    trade_value_krw: int
    direction: Direction


@dataclass(frozen=True)
class AdmissionDecision:
    admitted: tuple[Admission, ...]
    rejected: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class DynamicEntry:
    symbol: str
    feed: str
    decided_at_ns: int


@dataclass(frozen=True)
class DynamicPlan:
    rev: int
    entries: tuple[DynamicEntry, ...]

    def symbols_for(self, feed: str) -> tuple[str, ...]:
        return tuple(entry.symbol for entry in self.entries if entry.feed == feed)


def qualifies(row: KiwoomRankingRow, cfg: IntradayAdmissionSettings) -> bool:
    """True iff the row's absolute move and KRW trade value both meet the thresholds (inclusive)."""
    return abs(row.change_pct) >= cfg.min_abs_change_pct and row.trade_value_krw >= cfg.min_trade_value_krw


def decide_admissions(
    rows: Sequence[KiwoomRankingRow], state: AdmissionState, cfg: IntradayAdmissionSettings
) -> AdmissionDecision:
    """Pure admission rule for one poll; deterministic for identical inputs."""
    remaining = dict(state.capacity_left)
    rejected: list[tuple[str, str]] = []
    seen: set[str] = set()
    qualifiers: list[KiwoomRankingRow] = []
    for row in rows:
        if row.symbol in seen or row.symbol in state.collected or not qualifies(row, cfg):
            continue
        seen.add(row.symbol)
        if row.symbol in state.ineligible:
            rejected.append((row.symbol, "ineligible"))
            continue
        qualifiers.append(row)
    qualifiers.sort(key=lambda row: (-row.trade_value_krw, row.symbol))
    admitted: list[Admission] = []
    for row in qualifiers:
        feed = next((name for name, left in remaining.items() if left > 0), None)
        if feed is None:
            rejected.append((row.symbol, "capacity"))
            continue
        remaining[feed] -= 1
        admitted.append(
            Admission(
                symbol=row.symbol,
                feed=feed,
                change_pct=row.change_pct,
                trade_value_krw=row.trade_value_krw,
                direction="up" if row.change_pct > 0 else "down",
            )
        )
    return AdmissionDecision(admitted=tuple(admitted), rejected=tuple(rejected))


def feed_capacities(*, ls_reserve_symbols: int, kis_slot_symbols: int, kis_slot: str | None) -> dict[str, int]:
    """Free-symbol capacity per dynamic feed in fill order: LS shard 1 reserve, then the KIS dynamic slot."""
    capacity: dict[str, int] = {"ls:1": ls_reserve_symbols}
    if kis_slot is not None:
        capacity[f"kis:{kis_slot}"] = kis_slot_symbols
    return {feed: max(0, count) for feed, count in capacity.items()}


def state_from_ledger(
    rows: Sequence[MembershipRow],
    *,
    ineligible: frozenset[str],
    capacity: Mapping[str, int],
) -> AdmissionState:
    """Rebuild admission state from ledger rows so a restarted child never re-admits or over-fills a feed."""
    collected = frozenset(
        symbol
        for symbol, row in effective_tiers(rows).items()
        if row.tier in _COLLECTED_TIERS and row.source in _ADMITTED_SOURCES
    )
    left = dict(capacity)
    for row in rows:
        if row.source != "dynamic":
            continue
        feed = _feed_of(row)
        if feed in left:
            left[feed] = max(0, left[feed] - 1)
    return AdmissionState(collected=collected, ineligible=ineligible, capacity_left=left)


def _feed_of(row: MembershipRow) -> str:
    if row.ls_shard is not None:
        return f"ls:{row.ls_shard}"
    if row.kis_slot is not None:
        return f"kis:{row.kis_slot}"
    raise DynamicAdmissionsFileError(f"dynamic membership row has no feed: {row.symbol}")


def read_dynamic_admissions(path: pathlib.Path) -> DynamicPlan:
    """Read the dynamic-admissions file; a missing file is an empty plan at rev 0."""
    try:
        text = pathlib.Path(path).read_text(encoding="utf-8")
    except FileNotFoundError:
        return DynamicPlan(rev=0, entries=())
    try:
        payload: dict[str, Any] = json.loads(text)
        rev = payload["rev"]
        raw_entries = payload["entries"]
        if not isinstance(rev, int) or isinstance(rev, bool) or rev < 0 or not isinstance(raw_entries, list):
            raise ValueError("rev and entries have invalid types")
        entries = tuple(
            DynamicEntry(
                symbol=str(item["symbol"]),
                feed=str(item["feed"]),
                decided_at_ns=int(item["decided_at_ns"]),
            )
            for item in raw_entries
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise DynamicAdmissionsFileError(f"invalid dynamic admissions file {path}: {exc}") from exc
    if any(not is_krx_short_code(entry.symbol) for entry in entries):
        raise DynamicAdmissionsFileError(f"invalid symbol in dynamic admissions file {path}")
    return DynamicPlan(rev=rev, entries=entries)


def write_dynamic_admissions(path: pathlib.Path, plan: DynamicPlan) -> None:
    """Atomically replace the dynamic-admissions file; the revision must strictly exceed the stored one."""
    target = pathlib.Path(path)
    current = read_dynamic_admissions(target).rev
    if plan.rev <= current:
        raise DynamicAdmissionsFileError(f"dynamic admissions rev {plan.rev} must exceed stored rev {current}")
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "rev": plan.rev,
        "entries": [
            {"symbol": entry.symbol, "feed": entry.feed, "decided_at_ns": entry.decided_at_ns}
            for entry in plan.entries
        ],
    }
    tmp = target.parent / f".{target.name}.{os.getpid()}.tmp"
    tmp.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    os.replace(tmp, target)
