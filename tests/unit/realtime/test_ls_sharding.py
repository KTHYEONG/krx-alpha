"""Invariant guard tests for LS session sharding."""

from __future__ import annotations

import pytest

from src.realtime.ls_limits import split_ls_pairs
from src.realtime.ls_sharding import plan_ls_shards

_STREAMS = ("H0STCNT0", "H0STASP0")


def _symbols(count: int) -> tuple[str, ...]:
    return tuple(f"{i:06d}" for i in range(count))


def test_single_session_equals_legacy_split() -> None:
    symbols = _symbols(150)
    tiers = {symbols[140]: "core", symbols[5]: "event", symbols[7]: "follow"}
    pairs = [(symbol, stream) for symbol in symbols for stream in _STREAMS]

    kept, dropped = split_ls_pairs(pairs, 200, tier_of=tiers, order=symbols)
    shards, overflow = plan_ls_shards(
        symbols,
        tiers=tiers,
        streams=_STREAMS,
        active_sessions=1,
        pairs_per_session=200,
        dynamic_reserve_symbols=0,
    )

    assert len(shards) == 1
    assert shards[0].shard_index == 0
    assert shards[0].window_standby is True
    assert shards[0].dynamic_reserve_symbols == 0
    assert shards[0].symbols == tuple(dict.fromkeys(symbol for symbol, _ in kept))
    assert overflow == tuple(dropped)


def test_second_shard_takes_next_priority_block_minus_reserve() -> None:
    symbols = _symbols(150)

    shards, overflow = plan_ls_shards(
        symbols,
        tiers={},
        streams=_STREAMS,
        active_sessions=2,
        pairs_per_session=200,
        dynamic_reserve_symbols=40,
    )

    assert [shard.shard_index for shard in shards] == [0, 1]
    assert shards[0].symbols == symbols[:100]
    assert shards[1].symbols == symbols[100:150]
    assert shards[0].window_standby is True
    assert shards[1].window_standby is False
    assert shards[0].dynamic_reserve_symbols == 0
    assert shards[1].dynamic_reserve_symbols == 40
    assert shards[1].streams == _STREAMS
    assert overflow == ()


def test_overflow_preserves_priority() -> None:
    symbols = _symbols(200)

    shards, overflow = plan_ls_shards(
        symbols,
        tiers={},
        streams=_STREAMS,
        active_sessions=2,
        pairs_per_session=200,
        dynamic_reserve_symbols=40,
    )
    assert shards[1].symbols == symbols[100:160]
    assert overflow == symbols[160:]

    demoted = symbols[0]
    shards, overflow = plan_ls_shards(
        symbols,
        tiers={demoted: "follow"},
        streams=_STREAMS,
        active_sessions=2,
        pairs_per_session=200,
        dynamic_reserve_symbols=40,
    )
    reordered = (*symbols[1:], demoted)
    assert shards[0].symbols == reordered[:100]
    assert shards[1].symbols == reordered[100:160]
    assert overflow == reordered[160:]


def test_session_count_validated() -> None:
    symbols = _symbols(10)
    kwargs = {"tiers": {}, "streams": _STREAMS, "pairs_per_session": 200, "dynamic_reserve_symbols": 0}

    shards, _ = plan_ls_shards(symbols, active_sessions=2, max_sessions=3, **kwargs)
    assert len(shards) == 2

    with pytest.raises(ValueError, match="active_sessions"):
        plan_ls_shards(symbols, active_sessions=3, max_sessions=3, **kwargs)


@pytest.mark.parametrize(
    ("streams", "pairs_per_session", "reserve", "message"),
    [
        ((), 200, 0, "streams must not be empty"),
        (_STREAMS, 1, 0, "cannot hold one symbol"),
        (_STREAMS, 200, -1, "must be >= 0"),
    ],
)
def test_invalid_shard_plan_inputs_are_rejected(
    streams: tuple[str, ...], pairs_per_session: int, reserve: int, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        plan_ls_shards(
            _symbols(10),
            tiers={},
            streams=streams,
            active_sessions=1,
            pairs_per_session=pairs_per_session,
            dynamic_reserve_symbols=reserve,
        )
