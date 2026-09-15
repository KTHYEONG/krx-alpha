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


def test_plan_aftermarket_shards_full_coverage_and_key_shortage() -> None:
    import pytest
    from src.core.errors import SlotBudgetExceededError
    from src.realtime.kis_sharding import KisDataCredential, plan_aftermarket_shards
    keys = tuple(KisDataCredential(str(i), f'key{i}', 'secret', f'hts{i}', f'id{i}') for i in range(4))
    symbols = tuple(f'{i:06d}' for i in range(40))
    plan = plan_aftermarket_shards(symbols=symbols, credentials=keys, pair_capacity_per_connection=41, krx_streams=('H0STCNT0','H0STASP0'), nxt_streams=('H0NXCNT0','H0NXASP0'))
    assert [(x.venue.value,x.shard_index,len(x.symbols),x.credential_slot) for x in plan] == [('nxt',0,20,'0'),('nxt',1,20,'1'),('krx',0,20,'2'),('krx',1,20,'3')]
    with pytest.raises(SlotBudgetExceededError, match=r'required=4.*available=3'):
        plan_aftermarket_shards(symbols=symbols, credentials=keys[:3], pair_capacity_per_connection=41, krx_streams=('H0STCNT0','H0STASP0'), nxt_streams=('H0NXCNT0','H0NXASP0'))


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
        plan_aftermarket_shards(symbols=(), credentials=keys, pair_capacity_per_connection=41, krx_streams=("H0STCNT0", "H0STASP0"), nxt_streams=("H0NXCNT0", "H0NXASP0"))
    with pytest.raises(KrxAlphaError, match="duplicate"):
        plan_aftermarket_shards(symbols=("005930", "005930"), credentials=keys, pair_capacity_per_connection=41, krx_streams=("H0STCNT0", "H0STASP0"), nxt_streams=("H0NXCNT0", "H0NXASP0"))
    with pytest.raises(SlotBudgetExceededError, match="capacity"):
        plan_aftermarket_shards(symbols=("005930",), credentials=keys, pair_capacity_per_connection=1, krx_streams=("H0STCNT0", "H0STASP0"), nxt_streams=("H0NXCNT0", "H0NXASP0"))
