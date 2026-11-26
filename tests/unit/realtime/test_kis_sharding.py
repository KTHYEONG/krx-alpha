def test_load_kis_data_credentials_rejects_primary_or_duplicate_key() -> None:
    import pytest
    from src.core.errors import KrxAlphaError
    from src.realtime.kis_sharding import load_kis_data_credentials
    env = {'KIS_DATA_SLOTS':'1,2','KIS_DATA_1_APP_KEY':'same','KIS_DATA_1_APP_SECRET':'s','KIS_DATA_1_HTS_ID':'h','KIS_DATA_2_APP_KEY':'same','KIS_DATA_2_APP_SECRET':'s2','KIS_DATA_2_HTS_ID':'h2','KIS_APP_KEY':'primary'}
    with pytest.raises(KrxAlphaError, match='duplicate'):
        load_kis_data_credentials(env)
    env['KIS_DATA_2_APP_KEY'] = 'primary'
    with pytest.raises(KrxAlphaError, match='primary'):
        load_kis_data_credentials(env)


def test_plan_aftermarket_shards_full_coverage_and_key_shortage(caplog) -> None:
    import logging
    from src.realtime.kis_sharding import KisDataCredential, plan_aftermarket_shards
    keys = tuple(KisDataCredential(str(i), f'key{i}', 'secret', f'hts{i}', f'id{i}') for i in range(4))
    symbols = tuple(f'{i:06d}' for i in range(40))
    plan = plan_aftermarket_shards(krx_symbols=symbols, nxt_symbols=symbols, credentials=keys, pair_capacity_per_connection=41, krx_streams=('H0STCNT0','H0STASP0'), nxt_streams=('H0NXCNT0','H0NXASP0'))
    assert [(x.venue.value,x.shard_index,len(x.symbols),x.credential_slot) for x in plan] == [('nxt',0,20,'0'),('nxt',1,20,'1'),('krx',0,20,'2'),('krx',1,20,'3')]
    with caplog.at_level(logging.WARNING, logger="src.realtime.kis_sharding"):
        short = plan_aftermarket_shards(krx_symbols=symbols, nxt_symbols=symbols, credentials=keys[:3], pair_capacity_per_connection=41, krx_streams=('H0STCNT0','H0STASP0'), nxt_streams=('H0NXCNT0','H0NXASP0'))
    assert [(x.venue.value,x.shard_index,len(x.symbols),x.credential_slot) for x in short] == [('nxt',0,20,'0'),('krx',0,20,'1')]
    assert any("status=TRUNCATED" in rec.message and "kept_nxt=20" in rec.message and "kept_krx=20" in rec.message and "dropped=40" in rec.message for rec in caplog.records)


def test_load_kis_data_credentials_validates_all_branches() -> None:
    import pytest
    from src.core.errors import KrxAlphaError
    from src.realtime.kis_sharding import credential_key_id_for, load_kis_data_credentials
    assert load_kis_data_credentials({}) == ()
    assert load_kis_data_credentials({"KIS_DATA_SLOTS": "  "}) == ()
    creds = load_kis_data_credentials({
        "KIS_DATA_SLOTS": "1,2",
        "KIS_DATA_1_APP_KEY": "k1", "KIS_DATA_1_APP_SECRET": "s1", "KIS_DATA_1_HTS_ID": "h1",
        "KIS_DATA_2_APP_KEY": "k2", "KIS_DATA_2_APP_SECRET": "s2", "KIS_DATA_2_HTS_ID": "h2",
    })
    assert [c.slot for c in creds] == ["1", "2"]
    assert creds[0].key_id == credential_key_id_for("k1")
    assert len(creds[0].key_id) == 12
    with pytest.raises(KrxAlphaError, match="empty"):
        load_kis_data_credentials({"KIS_DATA_SLOTS": "1,,2"})
    with pytest.raises(KrxAlphaError, match="duplicate"):
        load_kis_data_credentials({"KIS_DATA_SLOTS": "1,1"})
    with pytest.raises(KrxAlphaError, match="empty"):
        load_kis_data_credentials({"KIS_DATA_SLOTS": "1", "KIS_DATA_1_APP_KEY": "k1"})
    with pytest.raises(KrxAlphaError, match="KIS_TRADE_APP_KEY"):
        load_kis_data_credentials({
            "KIS_DATA_SLOTS": "1", "KIS_TRADE_APP_KEY": "trade",
            "KIS_DATA_1_APP_KEY": "trade", "KIS_DATA_1_APP_SECRET": "s", "KIS_DATA_1_HTS_ID": "h",
        })


def test_plan_aftermarket_shards_rejects_empty_duplicate_and_tiny_capacity() -> None:
    import pytest
    from src.core.errors import KrxAlphaError, SlotBudgetExceededError
    from src.realtime.kis_sharding import KisDataCredential, plan_aftermarket_shards
    keys = (KisDataCredential("0", "key0", "secret", "hts0", "id0"),)
    with pytest.raises(KrxAlphaError, match="empty"):
        plan_aftermarket_shards(krx_symbols=(), nxt_symbols=(), credentials=keys, pair_capacity_per_connection=41, krx_streams=("H0STCNT0", "H0STASP0"), nxt_streams=("H0NXCNT0", "H0NXASP0"))
    with pytest.raises(KrxAlphaError, match="duplicate"):
        plan_aftermarket_shards(krx_symbols=("005930", "005930"), nxt_symbols=("005930",), credentials=keys, pair_capacity_per_connection=41, krx_streams=("H0STCNT0", "H0STASP0"), nxt_streams=("H0NXCNT0", "H0NXASP0"))
    with pytest.raises(KrxAlphaError, match="duplicate"):
        plan_aftermarket_shards(krx_symbols=("005930",), nxt_symbols=("005930", "005930"), credentials=keys, pair_capacity_per_connection=41, krx_streams=("H0STCNT0", "H0STASP0"), nxt_streams=("H0NXCNT0", "H0NXASP0"))
    with pytest.raises(SlotBudgetExceededError, match="capacity"):
        plan_aftermarket_shards(krx_symbols=("005930",), nxt_symbols=("005930",), credentials=keys, pair_capacity_per_connection=1, krx_streams=("H0STCNT0", "H0STASP0"), nxt_streams=("H0NXCNT0", "H0NXASP0"))


def test_plan_aftermarket_shards_overflow_keeps_top_ranked_symbols(caplog) -> None:
    # Given: 60개 순위 종목, 40개 용량, 2개 키
    import logging

    from src.realtime.kis_sharding import KisDataCredential, plan_aftermarket_shards

    keys = tuple(KisDataCredential(str(i), f'key{i}', 'secret', f'hts{i}', f'id{i}') for i in range(2))
    symbols = tuple(f'{i:06d}' for i in range(60))

    # When
    with caplog.at_level(logging.WARNING, logger="src.realtime.kis_sharding"):
        plan = plan_aftermarket_shards(krx_symbols=symbols, nxt_symbols=symbols, credentials=keys, pair_capacity_per_connection=80, krx_streams=('H0STCNT0','H0STASP0'), nxt_streams=('H0NXCNT0','H0NXASP0'))

    # Then: each venue keeps its top 40, dropping 40 assignments in total.
    covered = [symbol for shard in plan for symbol in shard.symbols]
    assert covered[:40] == list(symbols[:40])
    assert len(plan) == 2
    assert any("status=TRUNCATED" in rec.message and "kept_nxt=40" in rec.message and "kept_krx=40" in rec.message and "dropped=40" in rec.message for rec in caplog.records)


def test_plan_aftermarket_shards_raises_when_nothing_fits() -> None:
    # Given: 샤드 1개도 배정할 수 없는 키 풀
    import pytest

    from src.core.errors import SlotBudgetExceededError
    from src.realtime.kis_sharding import plan_aftermarket_shards

    with pytest.raises(SlotBudgetExceededError, match=r"required=2.*available=0"):
        plan_aftermarket_shards(krx_symbols=("005930",), nxt_symbols=("005930",), credentials=(), pair_capacity_per_connection=41, krx_streams=("H0STCNT0", "H0STASP0"), nxt_streams=("H0NXCNT0", "H0NXASP0"))


def test_premarket_single_shard_uses_configured_slot_only() -> None:
    from src.realtime.contracts import MarketVenue
    from src.realtime.kis_sharding import KisDataCredential, plan_premarket_shard

    keys = tuple(KisDataCredential(str(i), f"key{i}", "secret", f"hts{i}", f"id{i}") for i in range(1, 6))

    shard = plan_premarket_shard(
        symbols=("005930", "000660"), credentials=keys, credential_slot="5",
        pair_capacity_per_connection=41, nxt_streams=("H0NXCNT0", "H0NXASP0"),
    )

    assert shard.venue is MarketVenue.NXT
    assert shard.shard_index == 0
    assert shard.streams == ("H0NXCNT0", "H0NXASP0")
    assert shard.credential_slot == "5"
    assert shard.credential_key_id == "id5"
    assert shard.symbols == ("005930", "000660")


def test_premarket_truncation_keeps_rank_ordered_prefix(caplog) -> None:
    import logging

    from src.realtime.kis_sharding import KisDataCredential, plan_premarket_shard

    keys = (KisDataCredential("5", "key5", "secret", "hts5", "id5"),)
    symbols = tuple(f"{index:06d}" for index in range(30))

    with caplog.at_level(logging.WARNING, logger="src.realtime.kis_sharding"):
        shard = plan_premarket_shard(
            symbols=symbols, credentials=keys, credential_slot="5",
            pair_capacity_per_connection=41, nxt_streams=("H0NXCNT0", "H0NXASP0"),
        )

    assert shard.symbols == symbols[:20]
    assert len(shard.symbols) * 2 <= 41
    assert any("status=TRUNCATED" in rec.message for rec in caplog.records)


def test_premarket_capacity_below_one_symbol_fails() -> None:
    import pytest

    from src.core.errors import SlotBudgetExceededError
    from src.realtime.kis_sharding import KisDataCredential, plan_premarket_shard

    keys = (KisDataCredential("5", "key5", "secret", "hts5", "id5"),)

    with pytest.raises(SlotBudgetExceededError, match="capacity"):
        plan_premarket_shard(
            symbols=("005930",), credentials=keys, credential_slot="5",
            pair_capacity_per_connection=1, nxt_streams=("H0NXCNT0", "H0NXASP0"),
        )


def test_premarket_unknown_slot_fails_closed() -> None:
    import pytest

    from src.core.errors import MissingCredentialsError
    from src.realtime.kis_sharding import KisDataCredential, plan_premarket_shard

    keys = (KisDataCredential("5", "key5", "secret", "hts5", "id5"),)

    with pytest.raises(MissingCredentialsError, match="slot"):
        plan_premarket_shard(
            symbols=("005930",), credentials=keys, credential_slot="9",
            pair_capacity_per_connection=41, nxt_streams=("H0NXCNT0", "H0NXASP0"),
        )


def test_premarket_empty_or_duplicate_symbols_fail() -> None:
    import pytest

    from src.core.errors import KrxAlphaError
    from src.realtime.kis_sharding import KisDataCredential, plan_premarket_shard

    keys = (KisDataCredential("5", "key5", "secret", "hts5", "id5"),)

    with pytest.raises(KrxAlphaError, match="empty"):
        plan_premarket_shard(
            symbols=(), credentials=keys, credential_slot="5",
            pair_capacity_per_connection=41, nxt_streams=("H0NXCNT0", "H0NXASP0"),
        )
    with pytest.raises(KrxAlphaError, match="duplicate"):
        plan_premarket_shard(
            symbols=("005930", "005930"), credentials=keys, credential_slot="5",
            pair_capacity_per_connection=41, nxt_streams=("H0NXCNT0", "H0NXASP0"),
        )


def test_plan_aftermarket_shards_venues_planned_independently() -> None:
    from src.realtime.contracts import MarketVenue
    from src.realtime.kis_sharding import KisDataCredential, plan_aftermarket_shards

    keys = tuple(KisDataCredential(str(i), f"key{i}", "secret", f"hts{i}", f"id{i}") for i in range(2))
    plan = plan_aftermarket_shards(
        krx_symbols=("000003", "000004"), nxt_symbols=("000001", "000002"),
        credentials=keys, pair_capacity_per_connection=4,
        krx_streams=("H0STCNT0", "H0STASP0"), nxt_streams=("H0NXCNT0", "H0NXASP0"),
    )

    assert [(shard.venue, shard.shard_index, shard.symbols) for shard in plan] == [
        (MarketVenue.NXT, 0, ("000001", "000002")),
        (MarketVenue.KRX, 0, ("000003", "000004")),
    ]
    assert [shard.credential_slot for shard in plan] == ["0", "1"]


def test_plan_aftermarket_shards_allows_overlap_across_venues() -> None:
    from src.realtime.contracts import MarketVenue
    from src.realtime.kis_sharding import KisDataCredential, plan_aftermarket_shards

    keys = tuple(KisDataCredential(str(i), f"key{i}", "secret", f"hts{i}", f"id{i}") for i in range(2))
    plan = plan_aftermarket_shards(
        krx_symbols=("000001", "000002"), nxt_symbols=("000001", "000003"),
        credentials=keys, pair_capacity_per_connection=4,
        krx_streams=("H0STCNT0", "H0STASP0"), nxt_streams=("H0NXCNT0", "H0NXASP0"),
    )

    assert [shard.symbols for shard in plan] == [("000001", "000003"), ("000001", "000002")]
    assert all(shard.venue is venue for shard, venue in zip(plan, (MarketVenue.NXT, MarketVenue.KRX), strict=True))


def test_plan_aftermarket_shards_empty_nxt_plans_krx_only() -> None:
    from src.realtime.contracts import MarketVenue
    from src.realtime.kis_sharding import KisDataCredential, plan_aftermarket_shards

    keys = tuple(KisDataCredential(str(i), f"key{i}", "secret", f"hts{i}", f"id{i}") for i in range(2))
    plan = plan_aftermarket_shards(
        krx_symbols=("000001",), nxt_symbols=(),
        credentials=keys, pair_capacity_per_connection=4,
        krx_streams=("H0STCNT0", "H0STASP0"), nxt_streams=("H0NXCNT0", "H0NXASP0"),
    )

    assert [(shard.venue, shard.symbols, shard.credential_slot) for shard in plan] == [
        (MarketVenue.KRX, ("000001",), "0")
    ]


def test_plan_aftermarket_shards_truncation_caps_both_lists_by_rank(caplog) -> None:
    import logging

    from src.realtime.kis_sharding import KisDataCredential, plan_aftermarket_shards

    keys = tuple(KisDataCredential(str(i), f"key{i}", "secret", f"hts{i}", f"id{i}") for i in range(3))
    nxt = tuple(f"1{i:05d}" for i in range(40))
    krx = tuple(f"2{i:05d}" for i in range(40))

    with caplog.at_level(logging.WARNING, logger="src.realtime.kis_sharding"):
        plan = plan_aftermarket_shards(
            krx_symbols=krx, nxt_symbols=nxt,
            credentials=keys, pair_capacity_per_connection=40,
            krx_streams=("H0STCNT0", "H0STASP0"), nxt_streams=("H0NXCNT0", "H0NXASP0"),
        )

    assert [shard.symbols for shard in plan] == [nxt[:20], krx[:20]]
    assert any(
        "status=TRUNCATED" in rec.message
        and "kept_nxt=20" in rec.message
        and "kept_krx=20" in rec.message
        and "dropped=40" in rec.message
        for rec in caplog.records
    )


def test_plan_aftermarket_shards_identical_lists_deal_ranks_round_robin() -> None:
    from src.realtime.contracts import MarketVenue
    from src.realtime.kis_sharding import KisDataCredential, plan_aftermarket_shards

    keys = tuple(KisDataCredential(str(i), f"key{i}", "secret", f"hts{i}", f"id{i}") for i in range(4))
    symbols = tuple(f"{i:06d}" for i in range(40))
    plan = plan_aftermarket_shards(
        krx_symbols=symbols, nxt_symbols=symbols,
        credentials=keys, pair_capacity_per_connection=41,
        krx_streams=("H0STCNT0", "H0STASP0"), nxt_streams=("H0NXCNT0", "H0NXASP0"),
    )

    assert [(shard.venue, shard.shard_index, shard.symbols, shard.streams, shard.credential_slot, shard.credential_key_id) for shard in plan] == [
        (MarketVenue.NXT, 0, symbols[0::2], ("H0NXCNT0", "H0NXASP0"), "0", "id0"),
        (MarketVenue.NXT, 1, symbols[1::2], ("H0NXCNT0", "H0NXASP0"), "1", "id1"),
        (MarketVenue.KRX, 0, symbols[0::2], ("H0STCNT0", "H0STASP0"), "2", "id2"),
        (MarketVenue.KRX, 1, symbols[1::2], ("H0STCNT0", "H0STASP0"), "3", "id3"),
    ]


def test_plan_aftermarket_shards_round_robin_spreads_top_ranks() -> None:
    from src.realtime.contracts import MarketVenue
    from src.realtime.kis_sharding import KisDataCredential, plan_aftermarket_shards

    keys = tuple(KisDataCredential(str(i), f"key{i}", "s", "h", f"id{i}") for i in range(6))
    krx = tuple(f"{i:06d}" for i in range(52))
    plan = plan_aftermarket_shards(
        krx_symbols=krx, nxt_symbols=(), credentials=keys, pair_capacity_per_connection=41,
        krx_streams=("H0STCNT0", "H0STASP0"), nxt_streams=("H0NXCNT0", "H0NXASP0"),
    )
    shards = [shard for shard in plan if shard.venue is MarketVenue.KRX]
    assert [len(shard.symbols) for shard in shards] == [18, 17, 17]
    assert [shard.symbols[0] for shard in shards] == [krx[0], krx[1], krx[2]]


def test_plan_aftermarket_shards_round_robin_keeps_rank_order_within_shard() -> None:
    from src.realtime.kis_sharding import KisDataCredential, plan_aftermarket_shards

    keys = tuple(KisDataCredential(str(i), f"key{i}", "s", "h", f"id{i}") for i in range(6))
    nxt = tuple(f"1{i:05d}" for i in range(33))
    krx = tuple(f"2{i:05d}" for i in range(52))
    plan = plan_aftermarket_shards(
        krx_symbols=krx, nxt_symbols=nxt, credentials=keys, pair_capacity_per_connection=41,
        krx_streams=("H0STCNT0", "H0STASP0"), nxt_streams=("H0NXCNT0", "H0NXASP0"),
    )
    for venue_list in (nxt, krx):
        for shard in (s for s in plan if s.symbols and s.symbols[0] in venue_list):
            ranks = [venue_list.index(symbol) for symbol in shard.symbols]
            assert ranks == sorted(ranks)
            assert len(set(ranks)) == len(ranks)


def test_plan_aftermarket_shards_single_shard_keeps_full_order() -> None:
    from src.realtime.kis_sharding import KisDataCredential, plan_aftermarket_shards

    keys = tuple(KisDataCredential(str(i), f"key{i}", "s", "h", f"id{i}") for i in range(2))
    krx = tuple(f"{i:06d}" for i in range(20))
    plan = plan_aftermarket_shards(
        krx_symbols=krx, nxt_symbols=(), credentials=keys, pair_capacity_per_connection=41,
        krx_streams=("H0STCNT0", "H0STASP0"), nxt_streams=("H0NXCNT0", "H0NXASP0"),
    )
    assert len(plan) == 1
    assert plan[0].symbols == krx


def test_plan_aftermarket_shards_round_robin_union_disjoint_and_capped() -> None:
    from src.realtime.kis_sharding import KisDataCredential, plan_aftermarket_shards

    keys = tuple(KisDataCredential(str(i), f"key{i}", "s", "h", f"id{i}") for i in range(12))
    for length in range(1, 61):
        krx = tuple(f"{i:06d}" for i in range(length))
        plan = plan_aftermarket_shards(
            krx_symbols=krx, nxt_symbols=(), credentials=keys, pair_capacity_per_connection=41,
            krx_streams=("H0STCNT0", "H0STASP0"), nxt_streams=("H0NXCNT0", "H0NXASP0"),
        )
        sizes = [len(shard.symbols) for shard in plan]
        assert all(size <= 20 for size in sizes)
        assert max(sizes) - min(sizes) <= 1
        dealt = [symbol for shard in plan for symbol in shard.symbols]
        assert len(dealt) == len(set(dealt))
        assert set(dealt) == set(krx)


def test_aftermarket_rank_cap_is_maximal_for_unequal_and_empty_venue_lists() -> None:
    import itertools

    import pytest

    from src.core.errors import SlotBudgetExceededError
    from src.realtime.kis_sharding import KisDataCredential, plan_aftermarket_shards

    for n_nxt, n_krx, available in itertools.product((0, 1, 3, 5), (0, 1, 3, 5), range(5)):
        if n_nxt == n_krx == 0:
            continue
        nxt = tuple(f"1{i:05d}" for i in range(n_nxt))
        krx = tuple(f"2{i:05d}" for i in range(n_krx))
        credentials = tuple(KisDataCredential(str(i), f"key{i}", "s", "h", f"id{i}") for i in range(available))
        feasible = [
            k for k in range(1, max(n_nxt, n_krx) + 1)
            if (min(k, n_nxt) + 1) // 2 + (min(k, n_krx) + 1) // 2 <= available
        ]
        kwargs = {
            "nxt_symbols": nxt, "krx_symbols": krx, "credentials": credentials,
            "pair_capacity_per_connection": 4,
            "nxt_streams": ("H0NXCNT0", "H0NXASP0"), "krx_streams": ("H0STCNT0", "H0STASP0"),
        }
        if not feasible:
            with pytest.raises(SlotBudgetExceededError):
                plan_aftermarket_shards(**kwargs)
            continue
        plan = plan_aftermarket_shards(**kwargs)
        cap = max(feasible)
        assert len(plan) <= available
        for venue, symbols in (("nxt", nxt), ("krx", krx)):
            shards = [shard for shard in plan if shard.venue.value == venue]
            dealt = [symbol for shard in shards for symbol in shard.symbols]
            assert len(dealt) == len(set(dealt))
            assert set(dealt) == set(symbols[:cap])
            assert [shard.shard_index for shard in shards] == list(range(len(shards)))
        assert [shard.credential_slot for shard in plan] == [str(i) for i in range(len(plan))]


def test_regular_plan_slices_in_rank_order() -> None:
    from src.realtime.contracts import MarketVenue
    from src.realtime.kis_sharding import KisDataCredential, plan_regular_kis_shards

    keys = tuple(KisDataCredential(str(i), f"key{i}", "s", "h", f"id{i}") for i in (1, 2, 3, 4))
    symbols = tuple(f"{i:06d}" for i in range(45))
    plan = plan_regular_kis_shards(
        symbols=symbols, credentials=keys, credential_slots=("1", "2", "3", "4"),
        pair_capacity_per_connection=41, streams=("H0STCNT0", "H0STASP0"),
    )
    assert [(s.shard_index, len(s.symbols), s.credential_slot) for s in plan] == [(0, 20, "1"), (1, 20, "2"), (2, 5, "3")]
    assert all(s.venue is MarketVenue.KRX for s in plan)
    assert tuple(sym for s in plan for sym in s.symbols) == symbols
    again = plan_regular_kis_shards(
        symbols=symbols, credentials=keys, credential_slots=("1", "2", "3", "4"),
        pair_capacity_per_connection=41, streams=("H0STCNT0", "H0STASP0"),
    )
    assert [s.symbols for s in again] == [s.symbols for s in plan]


def test_regular_plan_truncates_instead_of_aborting(caplog) -> None:
    import logging

    from src.realtime.kis_sharding import KisDataCredential, plan_regular_kis_shards

    keys = tuple(KisDataCredential(str(i), f"key{i}", "s", "h", f"id{i}") for i in (1, 2, 3, 4))
    symbols = tuple(f"{i:06d}" for i in range(100))
    with caplog.at_level(logging.WARNING, logger="src.realtime.kis_sharding"):
        plan = plan_regular_kis_shards(
            symbols=symbols, credentials=keys, credential_slots=("1", "2", "3", "4"),
            pair_capacity_per_connection=41, streams=("H0STCNT0", "H0STASP0"),
        )
    kept = sum(len(s.symbols) for s in plan)
    assert kept == 80
    assert any("stage=regular_kis_plan" in r.message and "status=TRUNCATED" in r.message and "kept=80" in r.message and "dropped=20" in r.message for r in caplog.records)


def test_regular_plan_missing_slot_fails_closed() -> None:
    import pytest

    from src.core.errors import MissingCredentialsError
    from src.realtime.kis_sharding import KisDataCredential, plan_regular_kis_shards

    keys = (KisDataCredential("1", "k", "s", "h", "id1"),)
    with pytest.raises(MissingCredentialsError):
        plan_regular_kis_shards(
            symbols=("005930",), credentials=keys, credential_slots=("4",),
            pair_capacity_per_connection=41, streams=("H0STCNT0", "H0STASP0"),
        )


def test_regular_plan_covers_ls_overflow_before_core_overlap(caplog) -> None:
    from src.realtime.kis_sharding import KisDataCredential, plan_regular_kis_shards

    keys = tuple(KisDataCredential(str(i), f"key{i}", "s", "h", f"id{i}") for i in (1, 2, 3, 4))
    symbols = tuple(f"{i:06d}" for i in range(110))
    ls_covered = frozenset(symbols[:100])
    with caplog.at_level("INFO"):
        plan = plan_regular_kis_shards(
            symbols=symbols, credentials=keys, credential_slots=("1", "2", "3", "4"),
            pair_capacity_per_connection=41, streams=("H0STCNT0", "H0STASP0"),
            ls_covered_symbols=ls_covered,
        )
    selected = tuple(symbol for shard in plan for symbol in shard.symbols)
    assert selected == symbols[100:] + symbols[:70]
    assert len(selected) == len(set(selected)) == 80
    assert set(selected) | ls_covered == set(symbols)
    assert {shard.credential_slot for shard in plan} == {"1", "2", "3", "4"}
    assert "kis_exclusive=10 uncovered=0" in caplog.text


def test_regular_plan_reports_uncovered_tail_beyond_combined_capacity(caplog) -> None:
    from src.realtime.kis_sharding import KisDataCredential, plan_regular_kis_shards

    keys = tuple(KisDataCredential(str(i), f"key{i}", "s", "h", f"id{i}") for i in (1, 2, 3, 4))
    symbols = tuple(f"{i:06d}" for i in range(185))
    with caplog.at_level("CRITICAL"):
        plan = plan_regular_kis_shards(
            symbols=symbols, credentials=keys, credential_slots=("1", "2", "3", "4"),
            pair_capacity_per_connection=41, streams=("H0STCNT0", "H0STASP0"),
            ls_covered_symbols=frozenset(symbols[:100]),
        )
    assert tuple(s for shard in plan for s in shard.symbols) == symbols[100:180]
    assert "stage=regular_feed_coverage status=VIOLATION" in caplog.text
    assert "uncovered=5 uncovered_symbols=" + ",".join(symbols[180:]) in caplog.text
    from src.core.alerts import PageKind, page_kind

    coverage = [r for r in caplog.records if "stage=regular_feed_coverage" in r.getMessage()]
    assert [page_kind(r) for r in coverage] == [PageKind.FEED_COVERAGE_HOLE]
    assert coverage[0].krx_event is True


def test_regular_plan_retains_original_order_when_ls_covers_every_symbol() -> None:
    from src.realtime.kis_sharding import KisDataCredential, plan_regular_kis_shards

    keys = (KisDataCredential("1", "key", "s", "h", "id"),)
    symbols = ("005930", "000660", "000020")
    plan = plan_regular_kis_shards(
        symbols=symbols, credentials=keys, credential_slots=("1",),
        pair_capacity_per_connection=41, streams=("H0STCNT0", "H0STASP0"),
        ls_covered_symbols=frozenset(symbols),
    )
    assert plan[0].symbols == symbols


def test_regular_plan_rejects_duplicate_or_empty_symbols() -> None:
    import pytest

    from src.core.errors import KrxAlphaError, SlotBudgetExceededError
    from src.realtime.kis_sharding import KisDataCredential, plan_regular_kis_shards

    keys = (KisDataCredential("1", "k", "s", "h", "id1"),)
    with pytest.raises(KrxAlphaError):
        plan_regular_kis_shards(
            symbols=(), credentials=keys, credential_slots=("1",),
            pair_capacity_per_connection=41, streams=("H0STCNT0", "H0STASP0"),
        )
    with pytest.raises(KrxAlphaError):
        plan_regular_kis_shards(
            symbols=("005930", "005930"), credentials=keys, credential_slots=("1",),
            pair_capacity_per_connection=41, streams=("H0STCNT0", "H0STASP0"),
        )
    with pytest.raises(SlotBudgetExceededError):
        plan_regular_kis_shards(
            symbols=("005930",), credentials=keys, credential_slots=("1",),
            pair_capacity_per_connection=1, streams=("H0STCNT0", "H0STASP0"),
        )


def test_nxt_regular_plan_carries_venue_without_ls_coverage(caplog) -> None:
    import logging

    from src.realtime.contracts import MarketVenue
    from src.realtime.kis_sharding import KisDataCredential, plan_regular_kis_shards

    keys = tuple(KisDataCredential(str(i), f"key{i}", "s", "h", f"id{i}") for i in (2, 3))
    symbols = tuple(f"{i:06d}" for i in range(40))
    with caplog.at_level(logging.INFO, logger="src.realtime.kis_sharding"):
        plan = plan_regular_kis_shards(
            symbols=symbols, credentials=keys, credential_slots=("2", "3"),
            pair_capacity_per_connection=41, streams=("H0NXCNT0", "H0NXASP0"),
            venue=MarketVenue.NXT,
        )
    assert all(shard.venue is MarketVenue.NXT for shard in plan)
    assert [len(shard.symbols) for shard in plan] == [20, 20]
    assert sorted(sym for shard in plan for sym in shard.symbols) == list(symbols)
    assert not [r for r in caplog.records if "regular_feed_coverage" in r.getMessage()]


def test_nxt_regular_plan_rejects_ls_coverage() -> None:
    import pytest

    from src.core.errors import KrxAlphaError
    from src.realtime.contracts import MarketVenue
    from src.realtime.kis_sharding import KisDataCredential, plan_regular_kis_shards

    keys = (KisDataCredential("2", "key2", "s", "h", "id2"),)
    with pytest.raises(KrxAlphaError, match="KRX-only"):
        plan_regular_kis_shards(
            symbols=("005930",), credentials=keys, credential_slots=("2",),
            pair_capacity_per_connection=41, streams=("H0NXCNT0", "H0NXASP0"),
            ls_covered_symbols=frozenset(), venue=MarketVenue.NXT,
        )


def test_krx_regular_plan_default_venue_is_krx() -> None:
    from src.realtime.contracts import MarketVenue
    from src.realtime.kis_sharding import KisDataCredential, plan_regular_kis_shards

    keys = (KisDataCredential("1", "key1", "s", "h", "id1"),)
    plan = plan_regular_kis_shards(
        symbols=("005930", "000660"), credentials=keys, credential_slots=("1",),
        pair_capacity_per_connection=41, streams=("H0STCNT0", "H0STASP0"),
    )
    assert [(shard.venue, shard.credential_slot) for shard in plan] == [(MarketVenue.KRX, "1")]


def _ls_plan(symbols, blocks, *, covered=None, slots=("1", "2"), venue=None):
    from src.realtime.contracts import MarketVenue
    from src.realtime.kis_sharding import KisDataCredential, plan_regular_kis_shards

    keys = tuple(KisDataCredential(slot, f"key{slot}", "s", "h", f"id{slot}") for slot in slots)
    covered = frozenset(s for block in blocks for s in block) if covered is None else covered
    return plan_regular_kis_shards(
        symbols=symbols, credentials=keys, credential_slots=slots,
        pair_capacity_per_connection=41, streams=("H0STCNT0", "H0STASP0"),
        ls_covered_symbols=covered, ls_shard_symbols=blocks, venue=venue or MarketVenue.KRX,
    )


def _two_blocks(n0=100, n1=100):
    block0 = tuple(f"a{i:05d}" for i in range(n0))
    block1 = tuple(f"b{i:05d}" for i in range(n1))
    return block0, block1


def test_kis_plan_single_ls_block_equals_priority_order() -> None:
    block = tuple(f"{i:06d}" for i in range(60))
    plan = _ls_plan(block, (block,))
    assert tuple(s for shard in plan for s in shard.symbols) == block[:40]


def test_kis_plan_two_shards_aligns_slots_to_shards() -> None:
    block0, block1 = _two_blocks()
    plan = _ls_plan(block0 + block1, (block0, block1))
    assert [shard.symbols for shard in plan] == [block0[:20], block1[:20]]


def test_kis_plan_uncovered_first_then_quota(caplog) -> None:
    import logging

    block0, block1 = _two_blocks(50, 50)
    uncovered = tuple(f"u{i:05d}" for i in range(6))
    with caplog.at_level(logging.INFO, logger="src.realtime.kis_sharding"):
        plan = _ls_plan(uncovered + block0 + block1, (block0, block1))
    flat = tuple(s for shard in plan for s in shard.symbols)
    assert flat == uncovered + block0[:17] + block1[:17]
    assert plan[0].symbols[:6] == uncovered
    assert "dup_by_ls_shard=17,17" in caplog.text


def test_kis_plan_short_block_borrows_from_next() -> None:
    block0, block1 = _two_blocks(5, 100)
    plan = _ls_plan(block0 + block1, (block0, block1))
    flat = tuple(s for shard in plan for s in shard.symbols)
    assert flat == block0 + block1[:35]
    assert len(set(flat)) == 40


def test_kis_plan_rejects_bad_blocks() -> None:
    import pytest

    from src.core.errors import KrxAlphaError
    from src.realtime.contracts import MarketVenue

    block0, block1 = _two_blocks(30, 30)
    symbols = block0 + block1
    with pytest.raises(KrxAlphaError):
        _ls_plan(symbols, (block0, block0[:5] + block1))
    with pytest.raises(KrxAlphaError):
        _ls_plan(symbols, (block0, block1[:-1]), covered=frozenset(symbols))
    with pytest.raises(KrxAlphaError):
        _ls_plan(symbols, (block0, block1), venue=MarketVenue.NXT)


def test_kis_plan_blocks_require_covered_symbols() -> None:
    import pytest

    from src.core.errors import KrxAlphaError
    from src.realtime.kis_sharding import KisDataCredential, plan_regular_kis_shards

    block0, block1 = _two_blocks(5, 5)
    keys = (KisDataCredential("1", "k", "s", "h", "id1"),)
    with pytest.raises(KrxAlphaError):
        plan_regular_kis_shards(
            symbols=block0 + block1, credentials=keys, credential_slots=("1",),
            pair_capacity_per_connection=41, streams=("H0STCNT0", "H0STASP0"),
            ls_shard_symbols=(block0, block1),
        )


def test_regular_nxt_plan_deals_ranks_round_robin_so_top_names_never_share_one_connection() -> None:
    from src.realtime.contracts import MarketVenue
    from src.realtime.kis_sharding import KisDataCredential, plan_regular_kis_shards

    keys = tuple(KisDataCredential(str(i), f"key{i}", "secret", f"hts{i}", f"id{i}") for i in (1, 2))
    ranked = tuple(f"{i:06d}" for i in range(40))
    plan = plan_regular_kis_shards(
        symbols=ranked, credentials=keys, credential_slots=("1", "2"),
        pair_capacity_per_connection=41, streams=("H0NXCNT0", "H0NXASP0"), venue=MarketVenue.NXT,
    )
    assert [(s.shard_index, s.credential_slot) for s in plan] == [(0, "1"), (1, "2")]
    assert [s.symbols for s in plan] == [ranked[0::2], ranked[1::2]]
    assert sorted(plan[0].symbols + plan[1].symbols) == list(ranked)  # same coverage, only the assignment moved
    # Fewer symbols than one connection holds keep using a single connection.
    short = plan_regular_kis_shards(
        symbols=ranked[:15], credentials=keys, credential_slots=("1", "2"),
        pair_capacity_per_connection=41, streams=("H0NXCNT0", "H0NXASP0"), venue=MarketVenue.NXT,
    )
    assert [(s.shard_index, s.symbols) for s in short] == [(0, ranked[:15])]
    # Odd counts split as evenly as capacity allows and never exceed one connection's pair limit.
    odd = plan_regular_kis_shards(
        symbols=ranked[:21], credentials=keys, credential_slots=("1", "2"),
        pair_capacity_per_connection=41, streams=("H0NXCNT0", "H0NXASP0"), venue=MarketVenue.NXT,
    )
    assert [len(s.symbols) for s in odd] == [11, 10]


def test_regular_krx_plan_keeps_contiguous_rank_blocks() -> None:
    from src.realtime.kis_sharding import KisDataCredential, plan_regular_kis_shards

    keys = tuple(KisDataCredential(str(i), f"key{i}", "secret", f"hts{i}", f"id{i}") for i in (3, 4))
    ranked = tuple(f"{i:06d}" for i in range(40))
    plan = plan_regular_kis_shards(
        symbols=ranked, credentials=keys, credential_slots=("3", "4"),
        pair_capacity_per_connection=41, streams=("H0STCNT0", "H0STASP0"), log_coverage=False,
    )
    assert [s.symbols for s in plan] == [ranked[:20], ranked[20:]]


def test_aftermarket_plan_uses_six_credentials_with_no_symbol_dropped() -> None:
    from src.realtime.contracts import MarketVenue
    from src.realtime.kis_sharding import KisDataCredential, plan_aftermarket_shards

    keys = tuple(KisDataCredential(str(i), f"key{i}", "secret", f"hts{i}", f"id{i}") for i in range(1, 7))
    krx = tuple(f"{i:06d}" for i in range(56))
    nxt = tuple(f"{i:06d}" for i in range(100000, 100053))
    plan = plan_aftermarket_shards(
        krx_symbols=krx, nxt_symbols=nxt, credentials=keys, pair_capacity_per_connection=41,
        krx_streams=("H0STCNT0", "H0STASP0"), nxt_streams=("H0NXCNT0", "H0NXASP0"),
    )
    assert len(plan) == 6
    assert [s.venue for s in plan] == [MarketVenue.NXT] * 3 + [MarketVenue.KRX] * 3
    assert sorted(s.credential_slot for s in plan) == ["1", "2", "3", "4", "5", "6"]
    assert sum(len(s.symbols) for s in plan if s.venue is MarketVenue.NXT) == len(nxt)
    assert sum(len(s.symbols) for s in plan if s.venue is MarketVenue.KRX) == len(krx)


def test_nxt_regular_plan_three_slots_deals_round_robin_one_per_slot() -> None:
    from src.realtime.contracts import MarketVenue
    from src.realtime.kis_sharding import KisDataCredential, plan_regular_kis_shards

    keys = tuple(KisDataCredential(slot, f"key{slot}", "secret", f"hts{slot}", f"id{slot}") for slot in ("1", "2", "5"))
    ranked = tuple(f"{i:06d}" for i in range(60))
    plan = plan_regular_kis_shards(
        symbols=ranked, credentials=keys, credential_slots=("1", "2", "5"),
        pair_capacity_per_connection=41, streams=("H0NXCNT0", "H0NXASP0"), venue=MarketVenue.NXT,
    )
    assert [(s.shard_index, s.credential_slot) for s in plan] == [(0, "1"), (1, "2"), (2, "5")]
    assert [s.symbols for s in plan] == [ranked[0::3], ranked[1::3], ranked[2::3]]
    assert [len(s.symbols) for s in plan] == [20, 20, 20]
