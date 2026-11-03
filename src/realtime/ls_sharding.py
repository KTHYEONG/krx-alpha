"""LS WebSocket session sharding: priority-ordered whole-symbol blocks per session."""

from __future__ import annotations

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
) -> tuple[tuple[LsShard, ...], tuple[str, ...]]:
    """Split candidate symbols into LS shards; returns (shards, overflow symbols in priority order).

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

    unique_symbols = tuple(dict.fromkeys(symbols))
    pairs = [(symbol, stream) for symbol in unique_symbols for stream in streams]
    ordered = prioritize_ls_pairs(pairs, tier_of=tiers, order=unique_symbols)
    ordered_symbols = tuple(dict.fromkeys(symbol for symbol, _stream in ordered))

    base_capacity = pairs_per_session // len(streams)
    shard_count = active_sessions if ordered_symbols else 1
    last_index = shard_count - 1
    shards: list[LsShard] = []
    cursor = 0
    for index in range(shard_count):
        reserve = dynamic_reserve_symbols if index == last_index and index >= 1 else 0
        block = ordered_symbols[cursor : cursor + max(0, base_capacity - reserve)]
        cursor += len(block)
        shards.append(LsShard(index, block, streams, reserve, index == 0))
    return tuple(shards), ordered_symbols[cursor:]


__all__: list[str] = ["LsShard", "plan_ls_shards"]
