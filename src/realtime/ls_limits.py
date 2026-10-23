"""LS session/subscription limit guard (vendor notices 2026-09-23 and 2026-10-16)."""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol, cast

from src.core.config import LS_MAX_SESSIONS, LS_MAX_SUBSCRIPTIONS_PER_SESSION
from src.core.observability import EVENT

logger = logging.getLogger(__name__)

LS_WARN_RATIO: float = 0.8
LS_SESSIONS_PEAK_PLANNED: int = 2

_TIER_RANK: dict[str, int] = {"core": 0, "event": 1, "events": 1, "follow": 2}

# Vendor close/reject texts observed to indicate a session or subscription
# limit. Empty until a rejection is seen in production: unobserved text must
# stay ``unknown`` rather than guessed.
LS_LIMIT_OBSERVED_MARKERS: tuple[str, ...] = ()


class ManifestNote(Protocol):
    note: str | None


@dataclass(frozen=True)
class LsGuardConfig:
    """Ceilings and priority inputs for one LS regular-session stream start."""

    max_subscriptions: int = LS_MAX_SUBSCRIPTIONS_PER_SESSION
    max_sessions: int = LS_MAX_SESSIONS
    tiers: Mapping[str, str] | None = None
    order: tuple[str, ...] | None = None


def ls_limit_status(n_pairs: int, cap: int) -> str:
    """Return ``VIOLATION``/``WARN``/``OK`` for a subscription count against ``cap``."""
    if n_pairs > cap:
        return "VIOLATION"
    if n_pairs > LS_WARN_RATIO * cap:
        return "WARN"
    return "OK"


def prioritize_ls_pairs(
    pairs: Sequence[tuple[str, str]],
    *,
    tier_of: Mapping[str, str] | None = None,
    order: Sequence[str] | None = None,
) -> list[tuple[str, str]]:
    """Order pairs by tier (core, events, follow), then candidate order, stably."""
    if tier_of is None and order is None:
        return list(pairs)
    position = {symbol: index for index, symbol in enumerate(order)} if order is not None else {}
    fallback = len(position) + len(pairs)

    def _key(entry: tuple[int, tuple[str, str]]) -> tuple[int, int, int]:
        index, (symbol, _stream) = entry
        tier = _TIER_RANK.get(str((tier_of or {}).get(symbol, "")).lower(), 1) if tier_of is not None else 0
        return (tier, position.get(symbol, fallback), index)

    return [pair for _, pair in sorted(enumerate(pairs), key=_key)]


def split_ls_pairs(
    pairs: Sequence[tuple[str, str]],
    cap: int,
    *,
    tier_of: Mapping[str, str] | None = None,
    order: Sequence[str] | None = None,
) -> tuple[list[tuple[str, str]], list[str]]:
    """Keep whole symbols in priority order within ``cap``; return kept pairs and dropped symbols."""
    ordered = prioritize_ls_pairs(pairs, tier_of=tier_of, order=order)
    grouped: dict[str, list[tuple[str, str]]] = {}
    for pair in ordered:
        grouped.setdefault(pair[0], []).append(pair)
    kept: list[tuple[str, str]] = []
    dropped: list[str] = []
    used = 0
    for symbol, group in grouped.items():
        if used + len(group) <= cap:
            kept.extend(group)
            used += len(group)
        else:
            dropped.append(symbol)
    return kept, dropped


def classify_ls_close_reason(detail: str | None) -> str:
    """Classify a vendor close/reject text as ``ls_limit`` only on observed wording."""
    text = (detail or "").lower()
    if text and any(marker in text for marker in LS_LIMIT_OBSERVED_MARKERS):
        return "ls_limit"
    return "unknown"


def is_vendor_close_detail(detail: str | None) -> bool:
    """Detect a vendor-initiated close frame inside a disconnect detail."""
    text = detail or ""
    return "WSMsgType.CLOSE" in text or ("code=" in text and "reason=" in text)


def stream_disconnect_reason(pump_reason: str, detail: str | None) -> str:
    """Map a vendor close to its limit classification; leave local failures untouched."""
    if pump_reason == "disconnect" and is_vendor_close_detail(detail):
        return classify_ls_close_reason(detail)
    return pump_reason


def apply_ls_subscription_guard(
    pairs: Sequence[tuple[str, str]],
    *,
    session: object = None,
    guard: LsGuardConfig | None = None,
) -> list[tuple[str, str]]:
    """Emit the ``ls_limits`` verdict and cap an over-limit universe without silent loss."""
    cfg = guard if guard is not None else LsGuardConfig()
    planned = list(pairs)
    status = ls_limit_status(len(planned), cfg.max_subscriptions)
    logger.info(
        "[DATA] stage=ls_limits status=%s pairs=%d cap=%d sessions_peak_planned=%d cap_sessions=%d",
        status,
        len(planned),
        cfg.max_subscriptions,
        LS_SESSIONS_PEAK_PLANNED,
        cfg.max_sessions,
        extra=EVENT,
    )
    if status != "VIOLATION":
        return planned
    kept, dropped = split_ls_pairs(planned, cfg.max_subscriptions, tier_of=cfg.tiers, order=cfg.order)
    logger.critical(
        "[DATA] stage=ls_limits status=VIOLATION pairs=%d cap=%d kept=%d dropped=%d dropped_symbols=%s",
        len(planned),
        cfg.max_subscriptions,
        len(kept),
        len(dropped),
        ",".join(dropped),
    )
    manifest = cast(ManifestNote | None, getattr(session, "manifest", session))
    if manifest is not None:
        note = f"ls_limit cap={cfg.max_subscriptions} dropped={len(dropped)} symbols={','.join(dropped)}"
        previous = getattr(manifest, "note", None)
        manifest.note = f"{previous}; {note}" if previous and note not in previous else (previous or note)
    persist = getattr(session, "persist", None)
    if callable(persist):
        persist()
    return kept


__all__: list[str] = [
    "LS_LIMIT_OBSERVED_MARKERS",
    "LS_SESSIONS_PEAK_PLANNED",
    "LS_WARN_RATIO",
    "LsGuardConfig",
    "apply_ls_subscription_guard",
    "classify_ls_close_reason",
    "is_vendor_close_detail",
    "ls_limit_status",
    "prioritize_ls_pairs",
    "split_ls_pairs",
    "stream_disconnect_reason",
]
