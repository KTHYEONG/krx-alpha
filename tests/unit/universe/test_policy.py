"""Universe policy unit tests (contract skeletons)."""

from __future__ import annotations


def test_compute_selection_features_raises_on_missing_columns():
    # Given: 필수 컬럼 daily_change_pct 가 빠진 일봉 프레임
    import datetime as dt

    import polars as pl
    import pytest

    from src.universe.policy import compute_selection_features

    bars = pl.DataFrame({
        "date": [dt.date(2026, 1, 5)],
        "symbol": ["000001"],
        "close": [1000.0],
        "volume": [1000],
        "trade_value_100m": [100.0],
    })

    # When / Then: 결측 컬럼명을 담은 ValueError 로 fail-closed
    with pytest.raises(ValueError, match="daily_change_pct"):
        compute_selection_features(bars)


def test_compute_selection_features_drops_price_limit_artifacts():
    # Given: 가격제한폭 밖 등락률(29948%), 무거래(volume=0), 정상행이 섞인 프레임
    import datetime as dt

    import polars as pl

    from src.universe.policy import compute_selection_features

    bars = pl.DataFrame({
        "date": [dt.date(2026, 1, 5), dt.date(2026, 1, 6), dt.date(2026, 1, 7)],
        "symbol": ["000001", "000002", "000003"],
        "close": [1000.0, 1000.0, 1000.0],
        "volume": [1000, 0, 1000],
        "trade_value_100m": [100.0, 100.0, 100.0],
        "daily_change_pct": [29948.0, 5.0, 5.0],
    })

    # When
    out = compute_selection_features(bars)

    # Then: 아티팩트/무거래 행은 제거되고 정상행만 남는다
    assert out["symbol"].to_list() == ["000003"]


def test_compute_selection_features_sorts_unsorted_input_before_rolling():
    # Given: 날짜가 뒤섞인 입력과 동일 내용의 정렬된 입력
    import datetime as dt

    import polars as pl

    from src.universe.policy import compute_selection_features

    n = 25
    base = dt.date(2026, 1, 5)
    ordered = pl.DataFrame({
        "date": [base + dt.timedelta(days=i) for i in range(n)],
        "symbol": ["000001"] * n,
        "close": [1000.0 + i for i in range(n)],
        "volume": [1000] * n,
        "trade_value_100m": [100.0] * (n - 1) + [900.0],
        "daily_change_pct": [1.0] * n,
    })
    shuffled = ordered.reverse()

    # When
    from_ordered = compute_selection_features(ordered)
    from_shuffled = compute_selection_features(shuffled)

    # Then: 내부 정렬로 롤링 결과가 동일해야 한다
    assert from_ordered["tv_ratio"].to_list() == from_shuffled["tv_ratio"].to_list()
    assert from_shuffled["date"].to_list() == sorted(from_shuffled["date"].to_list())


def test_compute_selection_features_guards_zero_median_division():
    # Given: 거래대금이 전부 0 이라 롤링 중앙값이 0 인 프레임
    import datetime as dt

    import polars as pl

    from src.universe.policy import compute_selection_features

    n = 25
    base = dt.date(2026, 1, 5)
    bars = pl.DataFrame({
        "date": [base + dt.timedelta(days=i) for i in range(n)],
        "symbol": ["000001"] * n,
        "close": [1000.0] * n,
        "volume": [1000] * n,
        "trade_value_100m": [0.0] * n,
        "daily_change_pct": [1.0] * n,
    })

    # When
    out = compute_selection_features(bars)

    # Then: 0 나눗셈 대신 null 을 반환하고 inf 가 생기지 않는다
    ratios = out["tv_ratio"].to_list()
    assert all(r is None for r in ratios)


def test_select_universe_tags_all_matching_reasons():
    # Given: 상한가 + 급등 + 거래대금급증 + 60일 신고가를 동시에 만족하는 마지막 봉
    import datetime as dt

    import polars as pl

    from src.universe.policy import compute_selection_features, select_universe

    n = 60
    base = dt.date(2026, 1, 5)
    decision = base + dt.timedelta(days=n - 1)
    bars = pl.DataFrame({
        "date": [base + dt.timedelta(days=i) for i in range(n)],
        "symbol": ["000001"] * n,
        "close": [1000.0] * (n - 1) + [1300.0],
        "volume": [1000] * n,
        "trade_value_100m": [100.0] * (n - 1) + [1000.0],
        "daily_change_pct": [0.0] * (n - 1) + [29.9],
    })

    # When
    result = select_universe(compute_selection_features(bars), decision)

    # Then: 네 조건 태그가 모두 기록된다
    assert result.height == 1
    assert result["symbol"].to_list() == ["000001"]
    assert sorted(result["selection_reasons"].to_list()[0]) == ["limit_up", "newhigh60", "surge10", "volsurge"]


def test_select_universe_applies_liquidity_floor():
    # Given: 조건은 만족하나 거래대금이 유동성 하한(50억) 미만인 종목
    import datetime as dt

    import polars as pl

    from src.universe.policy import LIQUIDITY_FLOOR_100M, compute_selection_features, select_universe

    n = 60
    base = dt.date(2026, 1, 5)
    decision = base + dt.timedelta(days=n - 1)
    thin = LIQUIDITY_FLOOR_100M - 1.0
    bars = pl.DataFrame({
        "date": [base + dt.timedelta(days=i) for i in range(n)] * 2,
        "symbol": ["000001"] * n + ["000002"] * n,
        "close": ([1000.0] * (n - 1) + [1300.0]) * 2,
        "volume": [1000] * (2 * n),
        "trade_value_100m": ([1.0] * (n - 1) + [thin]) + ([100.0] * (n - 1) + [1000.0]),
        "daily_change_pct": ([0.0] * (n - 1) + [29.9]) * 2,
    })

    # When
    result = select_universe(compute_selection_features(bars), decision)

    # Then: 유동성 미달 종목은 제외된다
    assert result["symbol"].to_list() == ["000002"]


def test_select_universe_raises_on_lookahead_rows():
    # Given: 판정일 이후 봉이 포함된 피처 프레임 (롤링 피처가 미래로 오염된 상태)
    import datetime as dt

    import polars as pl
    import pytest

    from src.universe.policy import compute_selection_features, select_universe

    n = 60
    base = dt.date(2026, 1, 5)
    decision = base + dt.timedelta(days=n - 2)
    bars = pl.DataFrame({
        "date": [base + dt.timedelta(days=i) for i in range(n)],
        "symbol": ["000001"] * n,
        "close": [1000.0] * (n - 1) + [1300.0],
        "volume": [1000] * n,
        "trade_value_100m": [100.0] * (n - 1) + [1000.0],
        "daily_change_pct": [0.0] * (n - 1) + [29.9],
    })

    # When / Then: 조용히 잘라내지 않고 lookahead 로 실패한다
    with pytest.raises(ValueError, match="lookahead"):
        select_universe(compute_selection_features(bars), decision)


def test_select_universe_raises_when_slot_budget_exceeded():
    # Given: 슬롯 예산(2)보다 많은 3종목이 조건을 만족
    import datetime as dt

    import polars as pl
    import pytest

    from src.universe.policy import SlotBudgetExceededError, compute_selection_features, select_universe

    n = 60
    base = dt.date(2026, 1, 5)
    decision = base + dt.timedelta(days=n - 1)
    symbols = ["000001", "000002", "000003"]
    bars = pl.DataFrame({
        "date": [base + dt.timedelta(days=i) for i in range(n)] * len(symbols),
        "symbol": [s for s in symbols for _ in range(n)],
        "close": ([1000.0] * (n - 1) + [1300.0]) * len(symbols),
        "volume": [1000] * (n * len(symbols)),
        "trade_value_100m": ([100.0] * (n - 1) + [1000.0]) * len(symbols),
        "daily_change_pct": ([0.0] * (n - 1) + [29.9]) * len(symbols),
    })
    featured = compute_selection_features(bars)

    # When / Then: 무음 절단 대신 예외로 실패한다
    with pytest.raises(SlotBudgetExceededError):
        select_universe(featured, decision, slot_budget=2)


def test_select_universe_volsurge_false_without_sufficient_history():
    # Given: 롤링 윈도우를 채우지 못한 짧은 이력(5봉)에서 거래대금만 급증
    import datetime as dt

    import polars as pl

    from src.universe.policy import compute_selection_features, select_universe

    n = 5
    base = dt.date(2026, 1, 5)
    decision = base + dt.timedelta(days=n - 1)
    bars = pl.DataFrame({
        "date": [base + dt.timedelta(days=i) for i in range(n)],
        "symbol": ["000001"] * n,
        "close": [1000.0] * (n - 1) + [1060.0],
        "volume": [1000] * n,
        "trade_value_100m": [100.0] * (n - 1) + [5000.0],
        "daily_change_pct": [0.0] * (n - 1) + [6.0],
    })

    # When: 이력 부족으로 tv_ratio / close_max_60 이 null
    result = select_universe(compute_selection_features(bars), decision)

    # Then: null 조건은 False 로 닫혀 volsurge/newhigh60 로 선정되지 않는다
    assert result.height == 0


def test_select_universe_raises_on_missing_feature_columns():
    # Given: 롤링 피처가 부여되지 않은 원본 일봉 프레임
    import datetime as dt

    import polars as pl
    import pytest

    from src.universe.policy import select_universe

    bars = pl.DataFrame({
        "date": [dt.date(2026, 1, 5)],
        "symbol": ["000001"],
        "close": [1000.0],
        "volume": [1000],
        "trade_value_100m": [100.0],
        "daily_change_pct": [5.0],
    })

    # When / Then: 결측 피처 컬럼명을 담은 ValueError 로 fail-closed
    with pytest.raises(ValueError, match="tv_ratio"):
        select_universe(bars, dt.date(2026, 1, 5))
