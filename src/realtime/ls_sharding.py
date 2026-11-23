"""LS WebSocket session sharding: the priority-selected static set, split across sessions by expected load."""

from __future__ import annotations

import statistics
from collections.abc import Mapping
from dataclasses import dataclass

from src.core.config import LS_MAX_SESSIONS
from src.realtime.ls_limits import prioritize_ls_pairs


@dataclass(frozen=True)
class LsShard:
    """Whole-symbol block assigned to one LS session; only shard 0 carries the window standby."""

    shard_index: int
    symbols: tuple[str, ...]
    streams: tuple[str, ...]
    dynamic_reserve_symbols: int
    window_standby: bool


def plan_ls_shards(
    symbols: tuple[str, ...],
    *,
    tiers: Mapping[str, str],
    streams: tuple[str, ...],
    active_sessions: int,
    pairs_per_session: int,
    dynamic_reserve_symbols: int,
    max_sessions: int = LS_MAX_SESSIONS,
    loads: Mapping[str, int] | None = None,
) -> tuple[tuple[LsShard, ...], tuple[str, ...]]:
    """Split candidate symbols into LS shards; returns (shards, overflow symbols in priority order).

    The static set is the priority prefix that fits; ``loads`` (prior-day frames per symbol) only decides
    which shard holds each static symbol. Each shard lists its symbols in priority order.

    Raises ValueError for empty streams, a session cap that cannot hold one symbol, or
    active_sessions outside ``1 <= active_sessions <= max_sessions - 1``.
    """
    if not streams:
        raise ValueError("streams must not be empty")
    if pairs_per_session < len(streams):
        raise ValueError(f"pairs_per_session {pairs_per_session} cannot hold one symbol across {len(streams)} streams")
    if not 1 <= active_sessions <= max_sessions - 1:
        raise ValueError(f"active_sessions {active_sessions} must be within [1, {max_sessions - 1}]")
    if dynamic_reserve_symbols < 0:
        raise ValueError(f"dynamic_reserve_symbols {dynamic_reserve_symbols} must be >= 0")

    ordered_symbols = ls_priority_order(symbols, tiers=tiers, streams=streams)
    base_capacity = pairs_per_session // len(streams)
    shard_count = active_sessions if ordered_symbols else 1
    last_index = shard_count - 1
    reserves = [dynamic_reserve_symbols if index == last_index and index >= 1 else 0 for index in range(shard_count)]
    capacities = [max(0, base_capacity - reserve) for reserve in reserves]
    static = ordered_symbols[: sum(capacities)]
    members = _balanced_split(static, capacities, loads)
    rank = {symbol: position for position, symbol in enumerate(static)}
    shards = [
        LsShard(index, tuple(sorted(members[index], key=rank.__getitem__)), streams, reserves[index], index == 0)
        for index in range(shard_count)
    ]
    return tuple(shards), ordered_symbols[len(static) :]


def ls_priority_order(symbols: tuple[str, ...], *, tiers: Mapping[str, str], streams: tuple[str, ...]) -> tuple[str, ...]:
    """Unique symbols in LS subscription priority (tier, then candidate order); the static set is its prefix."""
    unique_symbols = tuple(dict.fromkeys(symbols))
    pairs = [(symbol, stream) for symbol in unique_symbols for stream in streams]
    ordered = prioritize_ls_pairs(pairs, tier_of=tiers, order=unique_symbols)
    return tuple(dict.fromkeys(symbol for symbol, _stream in ordered))


def _balanced_split(
    static: tuple[str, ...], capacities: list[int], loads: Mapping[str, int] | None
) -> list[list[str]]:
    """Assign the static set to shards so expected frame load is even; deterministic for equal inputs.

    Contiguous rank blocks put every top name on shard 0 (2.4-3.5x shard 1's frames, 11-18..11-20), where
    the 09:00 loss concentrated. With prior-day loads: heaviest first to the least-loaded shard with room
    (unknown symbols weigh the median known load, rank breaks ties). Without loads: snake order by rank.
    """
    members: list[list[str]] = [[] for _ in capacities]
    # Open only as many sessions as the static set needs (as before), so a small set never starts an extra session.
    used, room = 0, 0
    while room < len(static) and used < len(capacities):
        room += capacities[used]
        used += 1
    capacities = [cap if index < used else 0 for index, cap in enumerate(capacities)]
    sums = [0] * len(capacities)
    known = [loads[symbol] for symbol in static if loads is not None and symbol in loads]
    if loads is None or not known:
        count = max(1, used)
        for position, symbol in enumerate(static):
            lap, offset = divmod(position, count)
            preferred = offset if lap % 2 == 0 else count - 1 - offset
            open_shards = [i for i in range(len(capacities)) if len(members[i]) < capacities[i]]
            target = preferred if preferred in open_shards else min(open_shards, key=lambda i: (len(members[i]), i))
            members[target].append(symbol)
        return members
    median = statistics.median_low(known)
    weight = {symbol: loads.get(symbol, median) for symbol in static}
    for position in sorted(range(len(static)), key=lambda i: (-weight[static[i]], i)):
        symbol = static[position]
        target = min(
            (i for i in range(len(capacities)) if len(members[i]) < capacities[i]),
            key=lambda i: (sums[i], len(members[i]), i),
        )
        members[target].append(symbol)
        sums[target] += weight[symbol]
    return members


__all__: list[str] = ["LsShard", "ls_priority_order", "plan_ls_shards"]
