def test_data_paths_derive_from_single_root() -> None:
    # Given: data_root 만 지정한 설정
    import datetime as dt
    import pathlib

    from src.core.config import CollectorSettings

    settings = CollectorSettings(data_root=pathlib.Path("var/krx"))

    # When
    paths = settings.paths

    # Then: 모든 산출 경로가 단일 루트에서 파생된다
    assert paths.bars_store == pathlib.Path("var/krx/bars/daily.parquet")
    assert paths.market_map == pathlib.Path("var/krx/market_map.json")
    assert paths.candidates == pathlib.Path("var/krx/candidates.json")
    assert paths.journal_root == pathlib.Path("var/krx/l0")
    assert paths.archive_root == pathlib.Path("var/krx/l1")
    assert paths.universe_out(dt.date(2026, 9, 10)) == pathlib.Path("var/krx/universe/2026-09-10.parquet")
    assert paths.manifest_path(dt.date(2026, 9, 10)) == pathlib.Path("var/krx/manifest/2026-09-10.json")


import pytest


def test_load_credentials_is_fail_closed_on_missing_env(monkeypatch) -> None:
    # Given: 자격증명 env 부재
    from src.core.config import KrxCredentials, load_credentials
    from src.core.errors import MissingCredentialsError

    monkeypatch.delenv("KRX_OPENAPI_KEY", raising=False)

    # When / Then: 빈 문자열 기본값으로 새지 않고 타입드 예외로 실패한다
    with pytest.raises(MissingCredentialsError, match="krx_openapi_key"):
        load_credentials(KrxCredentials)

    monkeypatch.setenv("KRX_OPENAPI_KEY", "dummy-key")
    assert load_credentials(KrxCredentials).krx_openapi_key == "dummy-key"


def test_symbol_and_pair_budgets_are_distinct_named_settings() -> None:
    # Given: 기본 설정 (종목 예산과 구독쌍 예산은 서로 다른 물리량)
    from src.core.config import CollectorSettings

    settings = CollectorSettings()

    # When / Then: 벤더 용량(쌍)에서 종목 예산이 유도되고 두 값이 혼동되지 않는다
    assert settings.streams == ("H0STCNT0", "H0STASP0")
    assert settings.ls_capacity_pairs == 200
    assert settings.subscription_pair_budget == 200
    assert settings.subscription_symbol_budget == 100
    assert settings.universe_slot_budget == 90


def test_subscription_pair_budget_admits_full_universe_across_streams() -> None:
    # Given: 종목 예산 상한까지 선정된 유니버스와 다중 스트림 설정
    from src.core.config import CollectorSettings
    from src.core.errors import SlotBudgetExceededError
    from src.realtime.subscription import SubscriptionRegistry

    settings = CollectorSettings()
    symbols = [f"{i:06d}" for i in range(settings.universe_slot_budget)]
    desired = dict.fromkeys(symbols, settings.streams)
    required_pairs = settings.universe_slot_budget * len(settings.streams)

    # When / Then: 쌍 예산은 쌍 단위 수요를 수용하고 심볼 예산과 쌍 예산이 스트림수로 일관된다
    assert settings.subscription_pair_budget >= required_pairs
    assert settings.subscription_pair_budget == settings.subscription_symbol_budget * len(settings.streams)

    # When: SubscriptionRegistry 는 (symbol, tr_id) 쌍을 계수한다
    registry = SubscriptionRegistry(slot_budget=settings.subscription_pair_budget)
    diff = registry.plan(desired)

    # Then: 쌍 예산으로는 통과하고, 종목 예산을 쌍 예산으로 오용하면 fail-closed 로 거부된다
    assert len(diff.to_add) == required_pairs
    with pytest.raises(SlotBudgetExceededError, match="exceeds slot_budget"):
        SubscriptionRegistry(slot_budget=settings.universe_slot_budget).plan(desired)


def test_universe_slot_budget_default_stays_within_technical_capacity_margin() -> None:
    # Given: 기본 설정
    from src.core.config import CollectorSettings

    settings = CollectorSettings()

    # When / Then: 90은 과거 선정분포가 아니라 실측 기술 상한(100) 아래 10종목 마진으로 설정된다
    assert settings.universe_slot_budget == 90
    assert settings.subscription_symbol_budget == 100
    assert settings.subscription_symbol_budget - settings.universe_slot_budget == 10
    assert settings.universe_slot_budget <= settings.subscription_symbol_budget


def test_universe_slot_budget_exceeding_technical_capacity_raises_fail_closed() -> None:
    # Given: LS 기술 용량(100종목)을 초과하는 universe_slot_budget
    import pytest

    from src.core.config import CollectorSettings
    from src.core.errors import SlotBudgetExceededError

    # When / Then: 생성 시점에 즉시 fail-closed 한다 (SubscriptionRegistry 단계까지 미루지 않음)
    with pytest.raises(SlotBudgetExceededError, match="universe_slot_budget"):
        CollectorSettings(universe_slot_budget=101)

    # And: 상한 이하는 정상 생성된다
    ok = CollectorSettings(universe_slot_budget=100)
    assert ok.universe_slot_budget == 100
