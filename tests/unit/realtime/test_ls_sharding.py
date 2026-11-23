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
    # Without prior loads the static set is dealt snake-wise by rank until shard 1 (100 - reserve 40 - market 1) is full.
    assert shards[1].symbols == tuple(s for i, s in enumerate(symbols[:118]) if i % 4 in (1, 2))
    assert shards[0].symbols == tuple(s for s in symbols if s not in shards[1].symbols)
    assert (len(shards[0].symbols), len(shards[1].symbols)) == (91, 59)
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
    assert sorted(shards[0].symbols + shards[1].symbols) == list(symbols[:159])
    assert overflow == symbols[159:]

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
    assert set(shards[0].symbols + shards[1].symbols) == set(reordered[:159])
    assert all(list(s.symbols) == sorted(s.symbols, key=reordered.index) for s in shards)
    assert overflow == reordered[159:]


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


def test_prior_loads_balance_shards_and_keep_priority_order_inside_each() -> None:
    symbols = _symbols(200)
    # Activity falls with rank: contiguous blocks would load shard 0 with ~3x shard 1 (11-18..11-20 L0).
    loads = {symbol: 1000 - 4 * index for index, symbol in enumerate(symbols)}
    shards, overflow = plan_ls_shards(
        symbols, tiers={}, streams=_STREAMS, active_sessions=2, pairs_per_session=200,
        dynamic_reserve_symbols=0, loads=loads,
    )
    sums = [sum(loads[s] for s in shard.symbols) for shard in shards]
    assert overflow == symbols[199:]
    assert [len(shard.symbols) for shard in shards] == [100, 99]
    assert max(sums) / min(sums) < 1.01
    assert sorted(shards[0].symbols + shards[1].symbols) == list(symbols[:199])
    assert all(list(shard.symbols) == sorted(shard.symbols) for shard in shards)  # priority (rank) order kept
    # Same inputs give the same plan: daemon, resumed daemon and KIS child must agree.
    again, _ = plan_ls_shards(
        symbols, tiers={}, streams=_STREAMS, active_sessions=2, pairs_per_session=200,
        dynamic_reserve_symbols=0, loads=dict(reversed(list(loads.items()))),
    )
    assert again == shards


def test_unknown_symbols_weigh_the_median_and_small_sets_stay_on_one_session() -> None:
    symbols = _symbols(200)
    loads = {symbols[0]: 10_000, symbols[1]: 10, symbols[2]: 20}
    shards, _ = plan_ls_shards(
        symbols, tiers={}, streams=_STREAMS, active_sessions=2, pairs_per_session=200,
        dynamic_reserve_symbols=0, loads=loads,
    )
    assert symbols[0] in shards[0].symbols
    assert [len(shard.symbols) for shard in shards] == [100, 99]
    small, _ = plan_ls_shards(
        _symbols(80), tiers={}, streams=_STREAMS, active_sessions=2, pairs_per_session=200,
        dynamic_reserve_symbols=0, loads=dict.fromkeys(_symbols(80), 5),
    )
    assert [len(shard.symbols) for shard in small] == [80, 0]


def test_market_state_shard_capacity() -> None:
    symbols = _symbols(500)
    shards, overflow = plan_ls_shards(
        symbols,
        tiers={},
        streams=_STREAMS,
        active_sessions=2,
        pairs_per_session=400,
        dynamic_reserve_symbols=0,
    )
    assert len(shards[0].symbols) == 200
    assert len(shards[1].symbols) == 199
    assert overflow == symbols[399:]


def test_single_shard_no_market_reserve() -> None:
    symbols = _symbols(300)
    shards, _ = plan_ls_shards(
        symbols,
        tiers={},
        streams=_STREAMS,
        active_sessions=1,
        pairs_per_session=400,
        dynamic_reserve_symbols=0,
    )
    assert len(shards[0].symbols) == 200
