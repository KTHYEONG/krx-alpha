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


def test_plan_aftermarket_shards_identical_lists_reproduce_legacy_plan() -> None:
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
        (MarketVenue.NXT, 0, symbols[:20], ("H0NXCNT0", "H0NXASP0"), "0", "id0"),
        (MarketVenue.NXT, 1, symbols[20:], ("H0NXCNT0", "H0NXASP0"), "1", "id1"),
        (MarketVenue.KRX, 0, symbols[:20], ("H0STCNT0", "H0STASP0"), "2", "id2"),
        (MarketVenue.KRX, 1, symbols[20:], ("H0STCNT0", "H0STASP0"), "3", "id3"),
    ]


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
            assert tuple(symbol for shard in shards for symbol in shard.symbols) == symbols[:cap]
            assert [shard.shard_index for shard in shards] == list(range(len(shards)))
        assert [shard.credential_slot for shard in plan] == [str(i) for i in range(len(plan))]
