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
    result = select_universe(compute_selection_features(bars), decision, slot_budget=100)

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
    result = select_universe(compute_selection_features(bars), decision, slot_budget=100)

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
        select_universe(compute_selection_features(bars), decision, slot_budget=100)


def test_select_universe_truncates_when_slot_budget_exceeded():
    # Given: 슬롯 예산(2)보다 많은 3종목이 조건을 만족 (거래대금 차등)
    import datetime as dt

    import polars as pl

    from src.universe.policy import compute_selection_features, select_universe, select_universe_detailed

    n = 60
    base = dt.date(2026, 1, 5)
    decision = base + dt.timedelta(days=n - 1)
    symbols = ["000001", "000002", "000003"]
    tvs = [300.0, 200.0, 100.0]
    bars = pl.DataFrame({
        "date": [base + dt.timedelta(days=i) for i in range(n)] * len(symbols),
        "symbol": [s for s in symbols for _ in range(n)],
        "close": ([1000.0] * (n - 1) + [1300.0]) * len(symbols),
        "volume": [1000] * (n * len(symbols)),
        "trade_value_100m": [v for tv in tvs for v in ([100.0] * (n - 1) + [tv])],
        "daily_change_pct": ([0.0] * (n - 1) + [29.9]) * len(symbols),
    })
    featured = compute_selection_features(bars)

    # When: 예산 초과는 예외가 아니라 거래대금 하위 절단이다
    detailed = select_universe_detailed(featured, decision, slot_budget=2)
    result = select_universe(featured, decision, slot_budget=2)

    # Then: 상위 2종목만 유지되고 최저 거래대금 종목이 탈락한다
    assert result["symbol"].to_list() == ["000001", "000002"]
    assert detailed.dropped["symbol"].to_list() == ["000003"]


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
    result = select_universe(compute_selection_features(bars), decision, slot_budget=100)

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
        select_universe(bars, dt.date(2026, 1, 5), slot_budget=100)


def test_select_universe_accepts_44_candidates_with_explicit_budget() -> None:
    # Given: 2026-09-09 결정일에 상한가 조건을 만족하는 44개 종목 (9/10 08:20 장애 재현)
    import datetime as dt

    import polars as pl

    from src.universe.policy import compute_selection_features, select_universe

    n = 60
    base = dt.date(2026, 1, 5)
    decision = base + dt.timedelta(days=n - 1)
    symbols = [f"{i:06d}" for i in range(44)]
    bars = pl.DataFrame({
        "date": [base + dt.timedelta(days=i) for i in range(n)] * len(symbols),
        "symbol": [s for s in symbols for _ in range(n)],
        "close": ([1000.0] * (n - 1) + [1300.0]) * len(symbols),
        "volume": [1000] * (n * len(symbols)),
        "trade_value_100m": ([100.0] * (n - 1) + [1000.0]) * len(symbols),
        "daily_change_pct": ([0.0] * (n - 1) + [29.9]) * len(symbols),
    })
    featured = compute_selection_features(bars)

    # When: 명시 예산 100으로 선정한다
    result = select_universe(featured, decision, slot_budget=100)

    # Then: 44종목 전부가 예외 없이 선정된다
    assert result.height == 44
    assert sorted(result["symbol"].to_list()) == sorted(symbols)


def _selection_bars(entries) -> object:
    import datetime as dt

    import polars as pl

    dates = [dt.date(2026, 3, 2), dt.date(2026, 3, 3)]
    rows = []
    for symbol, kind, section in entries:
        for i, day in enumerate(dates):
            last = i == 1
            rows.append({
                "date": day,
                "symbol": symbol,
                "close": 1300.0 if last else 1000.0,
                "volume": 1000,
                "trade_value_100m": 100.0,
                "daily_change_pct": 29.9 if last else 0.0,
                "section": section,
                "stock_cert_kind": kind,
            })
    return pl.DataFrame(rows)


def test_select_universe_excludes_preferred_spac_managed_and_caution_sections():
    import datetime as dt

    from src.universe.policy import compute_selection_features, select_universe

    bars = _selection_bars([
        ("000001", "보통주", ""),
        ("000002", "구형우선주", ""),
        ("000003", "보통주", "SPAC(소속부없음)"),
        ("000004", "보통주", "관리종목(소속부없음)"),
        ("000005", "보통주", "투자주의환기종목(소속부없음)"),
    ])

    result = select_universe(compute_selection_features(bars), dt.date(2026, 3, 3), slot_budget=100)

    assert result["symbol"].to_list() == ["000001"]


def test_select_universe_excludes_before_slot_budget_check():
    import datetime as dt

    import polars as pl

    from src.universe.policy import compute_selection_features, select_universe, select_universe_detailed

    bars = _selection_bars([
        ("000001", "보통주", ""),
        ("000002", "구형우선주", ""),
        ("000003", "신형우선주", ""),
    ])

    result = select_universe(compute_selection_features(bars), dt.date(2026, 3, 3), slot_budget=1)

    assert result["symbol"].to_list() == ["000001"]
    # Two qualifying commons with distinct trade values: truncation keeps the higher-tv one.
    tv_bars = pl.DataFrame([
        {"date": dt.date(2026, 3, 2), "symbol": "000001", "close": 1000.0, "volume": 1000, "trade_value_100m": 100.0, "daily_change_pct": 0.0, "section": "", "stock_cert_kind": "보통주"},
        {"date": dt.date(2026, 3, 3), "symbol": "000001", "close": 1300.0, "volume": 1000, "trade_value_100m": 900.0, "daily_change_pct": 29.9, "section": "", "stock_cert_kind": "보통주"},
        {"date": dt.date(2026, 3, 2), "symbol": "000002", "close": 1000.0, "volume": 1000, "trade_value_100m": 100.0, "daily_change_pct": 0.0, "section": "", "stock_cert_kind": "보통주"},
        {"date": dt.date(2026, 3, 3), "symbol": "000002", "close": 1300.0, "volume": 1000, "trade_value_100m": 100.0, "daily_change_pct": 29.9, "section": "", "stock_cert_kind": "보통주"},
    ])
    detailed = select_universe_detailed(compute_selection_features(tv_bars), dt.date(2026, 3, 3), slot_budget=1)
    assert detailed.selected["symbol"].to_list() == ["000001"]
    assert detailed.dropped["symbol"].to_list() == ["000002"]


def test_select_universe_keeps_rows_with_unknown_class():
    import datetime as dt

    from src.universe.policy import compute_selection_features, select_universe

    bars = _selection_bars([("000001", None, None)])

    result = select_universe(compute_selection_features(bars), dt.date(2026, 3, 3), slot_budget=100)

    assert result["symbol"].to_list() == ["000001"]


def test_compute_selection_features_forward_fills_class_from_past_only():
    import datetime as dt

    import polars as pl

    from src.universe.policy import compute_selection_features

    bars = pl.DataFrame({
        "date": [dt.date(2026, 3, 2), dt.date(2026, 3, 3), dt.date(2026, 3, 4)],
        "symbol": ["000001"] * 3,
        "close": [1000.0, 1000.0, 1000.0],
        "volume": [1000, 1000, 1000],
        "trade_value_100m": [100.0, 100.0, 100.0],
        "daily_change_pct": [0.0, 0.0, 0.0],
        "stock_cert_kind": ["구형우선주", None, "보통주"],
    })

    out = compute_selection_features(bars)

    assert out.filter(pl.col("date") == dt.date(2026, 3, 3))["stock_cert_kind"].to_list() == ["구형우선주"]


def _ca_bars(closes, event_base, event_change=6.0) -> object:
    import datetime as dt

    import polars as pl

    base = dt.date(2026, 1, 5)
    dates = [base + dt.timedelta(days=i) for i in range(len(closes))]
    bases = [closes[0], *closes[:-1]]
    bases[-1] = event_base
    return pl.DataFrame({
        "date": dates,
        "symbol": ["000001"] * len(closes),
        "close": [float(v) for v in closes],
        "volume": [1000] * len(closes),
        "trade_value_100m": [100.0] * len(closes),
        "daily_change_pct": [0.0] * (len(closes) - 1) + [event_change],
        "base_price": [float(v) for v in bases],
    })


def test_newhigh60_adjusts_for_bonus_issue_base_price():
    import datetime as dt

    import polars as pl

    from src.universe.policy import compute_selection_features, select_universe

    closes = [9000.0] * 60 + [9000.0, 9300.0, 9000.0, 3150.0]
    decision = dt.date(2026, 1, 5) + dt.timedelta(days=len(closes) - 1)
    featured = compute_selection_features(_ca_bars(closes, 3000.0))

    event = featured.filter(pl.col("date") == decision)

    assert event["close_max_60"].to_list() == [3150.0]
    result = select_universe(featured, decision, slot_budget=100)
    assert "newhigh60" in result["selection_reasons"].to_list()[0]


def test_newhigh60_rejects_false_high_after_reverse_split():
    import datetime as dt

    import polars as pl

    from src.universe.policy import compute_selection_features, select_universe

    closes = [800.0] * 60 + [800.0, 1000.0, 783.0, 1661.0]
    decision = dt.date(2026, 1, 5) + dt.timedelta(days=len(closes) - 1)
    featured = compute_selection_features(_ca_bars(closes, 1566.0, event_change=12.0))

    event = featured.filter(pl.col("date") == decision)

    assert event["close_max_60"].to_list() == [2000.0]
    result = select_universe(featured, decision, slot_budget=100)
    reasons = result["selection_reasons"].to_list()[0]
    assert "surge10" in reasons
    assert "newhigh60" not in reasons


def test_close_max_60_is_exact_without_corporate_actions():
    import datetime as dt

    import polars as pl

    from src.universe.policy import compute_selection_features

    n = 70
    base = dt.date(2026, 1, 5)
    closes = [1000.0 + i for i in range(n)]
    bars = pl.DataFrame({
        "date": [base + dt.timedelta(days=i) for i in range(n)],
        "symbol": ["000001"] * n,
        "close": closes,
        "volume": [1000] * n,
        "trade_value_100m": [100.0] * n,
        "daily_change_pct": [0.0] * n,
        "base_price": [closes[0], *closes[:-1]],
    })

    out = compute_selection_features(bars)

    pure = pl.Series(closes).rolling_max(window_size=60).to_list()
    got = out["close_max_60"].to_list()
    assert all((a == b) or (a is None and b is None) for a, b in zip(got, pure, strict=True))
    assert got[-1] == closes[-1]


def test_close_max_60_is_causal_under_future_perturbation():
    import datetime as dt

    import polars as pl

    from src.universe.policy import compute_selection_features

    n = 65
    base = dt.date(2026, 1, 5)
    closes = [1000.0 + (i % 7) * 10.0 for i in range(n)]
    decision = base + dt.timedelta(days=n - 1)

    def _frame(extra) -> object:
        ext = closes + extra
        dates = [base + dt.timedelta(days=i) for i in range(len(ext))]
        bases = [ext[0], *ext[:-1]]
        return pl.DataFrame({
            "date": dates,
            "symbol": ["000001"] * len(ext),
            "close": [float(v) for v in ext],
            "volume": [1000] * len(ext),
            "trade_value_100m": [100.0] * len(ext),
            "daily_change_pct": [0.0] * len(ext),
            "base_price": [float(v) for v in bases],
        })

    before = compute_selection_features(_frame([])).filter(pl.col("date") <= decision)
    after = compute_selection_features(_frame([5000.0, 10.0, 9000.0])).filter(pl.col("date") <= decision)

    assert before["close_max_60"].to_list() == after["close_max_60"].to_list()
    assert before["tv_ratio"].to_list() == after["tv_ratio"].to_list()


def _class_bars(rows: list[tuple]) -> object:
    import polars as pl

    return pl.DataFrame(
        {
            "date": [row[0] for row in rows],
            "symbol": [row[1] for row in rows],
            "stock_cert_kind": [row[2] for row in rows],
            "section": [row[3] for row in rows],
        },
        schema={"date": pl.Date, "symbol": pl.String, "stock_cert_kind": pl.String, "section": pl.String},
        strict=False,
    )


def test_ineligible_security_symbols_flags_preferred_share() -> None:
    import datetime as dt

    from src.universe.policy import ineligible_security_symbols

    bars = _class_bars([(dt.date(2026, 9, 22), "005935", "구형우선주", None)])

    assert ineligible_security_symbols(bars) == frozenset({"005935"})


def test_ineligible_security_symbols_flags_excluded_section() -> None:
    import datetime as dt

    from src.universe.policy import ineligible_security_symbols

    bars = _class_bars([(dt.date(2026, 9, 22), "000001", "보통주", "SPAC(소속부없음)")])

    assert ineligible_security_symbols(bars) == frozenset({"000001"})


def test_ineligible_security_symbols_latest_classification_wins() -> None:
    import datetime as dt

    from src.universe.policy import ineligible_security_symbols

    bars = _class_bars([
        (dt.date(2026, 9, 21), "005935", "구형우선주", None),
        (dt.date(2026, 9, 22), "005935", "보통주", None),
        (dt.date(2026, 9, 22), "000001", "보통주", "SPAC(소속부없음)"),
        (dt.date(2026, 9, 21), "000001", "보통주", None),
    ])

    assert ineligible_security_symbols(bars) == frozenset({"000001"})


def test_ineligible_security_symbols_unknown_classification_stays_eligible() -> None:
    import datetime as dt

    from src.universe.policy import ineligible_security_symbols

    bars = _class_bars([(dt.date(2026, 9, 22), "0007J0", None, None)])

    assert ineligible_security_symbols(bars) == frozenset()


def test_ineligible_security_symbols_empty_without_classification_columns() -> None:
    import datetime as dt

    import polars as pl

    from src.universe.policy import ineligible_security_symbols

    bars = pl.DataFrame({"date": [dt.date(2026, 9, 22)], "symbol": ["005930"]})

    assert ineligible_security_symbols(bars) == frozenset()


def _implied_bars(closes, event_pct=None, with_base_col=True, base_values=None) -> object:
    import datetime as dt

    import polars as pl

    base = dt.date(2026, 1, 5)
    dates = [base + dt.timedelta(days=i) for i in range(len(closes))]
    pcts: list[float] = [0.0] * len(closes)
    for i in range(1, len(closes) - (1 if event_pct is not None else 0)):
        pcts[i] = round((closes[i] / closes[i - 1] - 1.0) * 100.0, 2)
    if event_pct is not None:
        pcts[-1] = float(event_pct)
    data = {
        "date": dates,
        "symbol": ["000001"] * len(closes),
        "close": [float(v) for v in closes],
        "volume": [1000] * len(closes),
        "trade_value_100m": [100.0] * len(closes),
        "daily_change_pct": pcts,
    }
    if with_base_col:
        if base_values is None:
            base_values = [None] * len(closes)
        data["base_price"] = base_values
    return pl.DataFrame(data)


def test_close_max_60_suppresses_false_high_on_reverse_split_without_base_price():

    from src.universe.policy import compute_selection_features

    hist = [1000.0] * 30 + [1100.0] + [1000.0] * 29
    closes = [*hist, 5100.0]
    bars = _implied_bars(closes, event_pct=2.0)

    out = compute_selection_features(bars)
    got = out["close_max_60"].to_list()[-1]

    assert got == 5500.0


def test_close_max_60_keeps_genuine_high_on_forward_split_without_base_price():
    import polars as pl

    from src.universe.policy import compute_selection_features, select_universe

    closes = [10000.0] * 60 + [2120.0]
    bars = _implied_bars(closes, event_pct=6.0)
    featured = compute_selection_features(bars)

    event = featured.filter(pl.col("date") == featured["date"].max())

    assert event["close_max_60"].to_list() == [2120.0]
    result = select_universe(featured, featured["date"].max(), slot_budget=100)
    assert "newhigh60" in result["selection_reasons"].to_list()[0]


def test_close_max_60_ignores_rounding_noise_without_base_price():
    import polars as pl

    from src.universe.policy import compute_selection_features

    closes = [1000.0 + i * 3.0 + (i % 5) * 1.7 for i in range(80)]
    bars = _implied_bars(closes)
    out = compute_selection_features(bars)

    pure = pl.Series(closes).rolling_max(window_size=60).to_list()
    got = out["close_max_60"].to_list()
    assert all((a == b) or (a is None and b is None) for a, b in zip(got, pure, strict=True))


def test_close_max_60_matches_legacy_base_price_path():
    import polars as pl

    from src.universe.policy import NEWHIGH_LOOKBACK, compute_selection_features

    closes = [9000.0] * 60 + [9000.0, 9300.0, 9000.0, 3150.0]
    bases = [closes[0], *closes[:-1]]
    bases[-1] = 3000.0
    bars = _implied_bars(closes, event_pct=6.0, base_values=[float(v) for v in bases])

    got = compute_selection_features(bars)["close_max_60"].to_list()

    def _legacy(clean: pl.DataFrame) -> list:
        prev_close = pl.col("close").shift(1).over("symbol")
        tmp = clean.with_columns([
            pl.when(
                pl.col("base_price").is_not_null()
                & (pl.col("base_price") > 0)
                & prev_close.is_not_null()
                & (prev_close > 0)
            )
            .then(pl.col("base_price") / prev_close)
            .otherwise(1.0)
            .alias("_event_ratio"),
        ])
        tmp = tmp.with_columns(pl.col("_event_ratio").cum_prod().over("symbol").alias("_cum_event"))
        tmp = tmp.with_columns((pl.col("close") / pl.col("_cum_event")).alias("_norm_close"))
        tmp = tmp.with_columns(
            pl.col("_norm_close").rolling_max(window_size=NEWHIGH_LOOKBACK - 1).over("symbol").alias("_past_norm_max")
        )
        tmp = tmp.with_columns(pl.col("_past_norm_max").shift(1).over("symbol").alias("_past_norm_max"))
        tmp = tmp.with_columns(
            pl.when(pl.col("_past_norm_max").is_not_null())
            .then(pl.max_horizontal([pl.col("_cum_event") * pl.col("_past_norm_max"), pl.col("close")]))
            .otherwise(None)
            .alias("close_max_60"),
        )
        return tmp["close_max_60"].to_list()

    expected = _legacy(bars.sort(["symbol", "date"]))
    assert got == expected
    assert got[-1] == 3150.0


def test_close_max_60_adjusts_mixed_implied_and_base_price_history():
    from src.universe.policy import compute_selection_features

    hist = [1000.0] * 30 + [1100.0] + [1000.0] * 29
    closes = hist + [5100.0] + [5100.0] * 19 + [10404.0]
    first = len(hist)
    last = len(closes) - 1
    pcts: list[float] = [0.0] * len(closes)
    for i in range(1, len(closes)):
        if i in (first, last):
            continue
        pcts[i] = round((closes[i] / closes[i - 1] - 1.0) * 100.0, 2)
    pcts[first] = 2.0
    pcts[last] = 2.0
    bases: list = [None] * len(closes)
    bases[last] = 10200.0
    bars = _implied_bars(closes, base_values=bases)
    # _implied_bars가 계산한 pct를 혼합 시나리오 pct로 교체한다.
    import polars as pl

    bars = bars.with_columns(pl.Series("daily_change_pct", pcts))

    out = compute_selection_features(bars)
    got = out["close_max_60"].to_list()

    assert got[first] == 5500.0
    assert got[-1] == 11000.0


def test_close_max_60_adjusts_without_base_price_column():
    from src.universe.policy import compute_selection_features

    hist = [1000.0] * 30 + [1100.0] + [1000.0] * 29
    closes = [*hist, 10200.0]
    bars = _implied_bars(closes, event_pct=2.0, with_base_col=False)

    out = compute_selection_features(bars)

    assert "base_price" not in bars.columns
    assert out["close_max_60"].to_list()[-1] == 11000.0


def test_close_max_60_is_causal_with_implied_events_under_future_perturbation():
    import polars as pl

    from src.universe.policy import compute_selection_features

    hist = [1000.0] * 30 + [1100.0] + [1000.0] * 29
    closes = [*hist, 5100.0]
    merge_idx = len(hist)

    def _frame(extra) -> object:
        ext = closes + extra
        frame = _implied_bars(ext, with_base_col=True)
        pcts = frame["daily_change_pct"].to_list()
        pcts[merge_idx] = 2.0
        return frame.with_columns(pl.Series("daily_change_pct", pcts))

    import datetime as dt

    decision = dt.date(2026, 1, 5) + dt.timedelta(days=len(closes) - 1)
    before = compute_selection_features(_frame([])).filter(pl.col("date") <= decision)
    after = compute_selection_features(_frame([5200.0, 5300.0, 5400.0])).filter(pl.col("date") <= decision)

    assert before["close_max_60"].to_list() == after["close_max_60"].to_list()


def _featured_limit_up(entries, *, n: int = 60):
    """Synthetic featured bars: 60 days, limit-up (or given change) on decision day.

    entries: list of (symbol, trade_value_100m, change_pct) for the decision day.
    """
    import datetime as dt

    import polars as pl

    from src.universe.policy import compute_selection_features

    base = dt.date(2026, 1, 5)
    decision = base + dt.timedelta(days=n - 1)
    by_symbol = {symbol: (tv, change) for symbol, tv, change in entries}
    symbols = [symbol for symbol, _, _ in entries]
    bars = pl.DataFrame({
        "date": [base + dt.timedelta(days=i) for i in range(n)] * len(symbols),
        "symbol": [s for s in symbols for _ in range(n)],
        "close": ([1000.0] * (n - 1) + [1300.0]) * len(symbols),
        "volume": [1000] * (n * len(symbols)),
        "trade_value_100m": [v for s in symbols for v in ([100.0] * (n - 1) + [by_symbol[s][0]])],
        "daily_change_pct": [v for s in symbols for v in ([0.0] * (n - 1) + [by_symbol[s][1]])],
    })
    return compute_selection_features(bars), decision


def test_select_universe_exact_at_budget_has_no_truncation(caplog) -> None:
    import logging

    from src.universe.policy import select_universe_detailed

    featured, decision = _featured_limit_up([
        ("000001", 300.0, 29.9), ("000002", 200.0, 29.9), ("000003", 100.0, 29.9),
    ])

    with caplog.at_level(logging.WARNING, logger="src.universe.policy"):
        result = select_universe_detailed(featured, decision, slot_budget=3)

    assert result.selected["symbol"].to_list() == ["000001", "000002", "000003"]
    assert result.dropped.height == 0
    assert not any("TRUNCATED" in rec.message for rec in caplog.records)


def test_select_universe_budget_plus_one_drops_lowest_trade_value() -> None:
    from src.universe.policy import select_universe_detailed

    featured, decision = _featured_limit_up([
        ("000001", 300.0, 29.9), ("000002", 500.0, 29.9),
        ("000003", 100.0, 29.9), ("000004", 400.0, 29.9),
    ])

    result = select_universe_detailed(featured, decision, slot_budget=3)

    assert result.selected["symbol"].to_list() == ["000001", "000002", "000004"]
    assert result.dropped["symbol"].to_list() == ["000003"]


def test_select_universe_ties_resolved_by_symbol_ascending() -> None:
    from src.universe.policy import select_universe_detailed

    featured, decision = _featured_limit_up([
        ("000003", 200.0, 29.9), ("000001", 200.0, 29.9), ("000002", 200.0, 29.9),
    ])

    result = select_universe_detailed(featured, decision, slot_budget=2)

    assert result.selected["symbol"].to_list() == ["000001", "000002"]
    assert result.dropped["symbol"].to_list() == ["000003"]


def test_select_universe_deterministic_under_row_shuffle() -> None:
    import polars as pl
    from polars.testing import assert_frame_equal

    from src.universe.policy import compute_selection_features, select_universe_detailed

    import datetime as dt

    n = 60
    base = dt.date(2026, 1, 5)
    symbols = [f"{i:06d}" for i in range(8)]
    tvs = [100.0 + i * 37.0 for i in range(8)]
    bars = pl.DataFrame({
        "date": [base + dt.timedelta(days=i) for i in range(n)] * len(symbols),
        "symbol": [s for s in symbols for _ in range(n)],
        "close": ([1000.0] * (n - 1) + [1300.0]) * len(symbols),
        "volume": [1000] * (n * len(symbols)),
        "trade_value_100m": [v for tv in tvs for v in ([100.0] * (n - 1) + [tv])],
        "daily_change_pct": ([0.0] * (n - 1) + [29.9]) * len(symbols),
    })
    decision = base + dt.timedelta(days=n - 1)
    reference = select_universe_detailed(compute_selection_features(bars), decision, slot_budget=5)
    for seed in (1, 7, 42):
        shuffled = bars.sample(fraction=1.0, shuffle=True, seed=seed)
        got = select_universe_detailed(compute_selection_features(shuffled), decision, slot_budget=5)
        assert_frame_equal(got.selected, reference.selected)
        assert_frame_equal(got.dropped, reference.dropped)


def test_select_universe_dropped_is_exact_complement() -> None:
    from src.universe.policy import select_universe_detailed

    entries = [(f"{i:06d}", 100.0 + i * 11.0, 29.9) for i in range(10)]
    featured, decision = _featured_limit_up(entries)

    full = select_universe_detailed(featured, decision, slot_budget=100)
    assert full.dropped.height == 0
    qualifying = set(full.selected["symbol"].to_list())

    result = select_universe_detailed(featured, decision, slot_budget=6)

    kept = result.selected["symbol"].to_list()
    dropped = result.dropped["symbol"].to_list()
    assert set(kept) & set(dropped) == set()
    assert set(kept) | set(dropped) == qualifying
    tv_by_symbol = dict(zip(result.selected["symbol"].to_list(), result.selected["trade_value_100m"].to_list(), strict=True))
    tv_by_symbol.update(zip(dropped, result.dropped["trade_value_100m"].to_list(), strict=True))
    assert all(tv_by_symbol[s] <= min(tv_by_symbol[s] for s in kept) for s in dropped)
    priority = sorted(qualifying, key=lambda s: (-tv_by_symbol[s], s))
    assert dropped == [s for s in priority if s in set(dropped)]


def test_select_universe_budget_below_one_raises_value_error() -> None:
    import pytest

    from src.universe.policy import select_universe_detailed

    featured, decision = _featured_limit_up([("000001", 100.0, 1.0)])

    for budget in (0, -1):
        with pytest.raises(ValueError, match="slot_budget"):
            select_universe_detailed(featured, decision, slot_budget=budget)


def test_select_universe_empty_selection_is_valid() -> None:
    from src.universe.policy import select_universe_detailed

    featured, decision = _featured_limit_up([("000001", 100.0, 1.0), ("000002", 100.0, 0.5)])

    result = select_universe_detailed(featured, decision, slot_budget=100)

    expected_columns = ["decision_date", "symbol", "selection_reasons", "daily_change_pct", "trade_value_100m", "tv_ratio"]
    assert result.selected.height == 0
    assert result.dropped.height == 0
    assert result.selected.columns == expected_columns
    assert result.dropped.columns == expected_columns


def test_select_universe_replay_108_to_100(caplog) -> None:
    import logging

    from src.universe.policy import DROPPED_LOG_SAMPLE, select_universe_detailed

    entries = [
        (f"{i:06d}", 60.0 + (i * 53.0) % 900.0, 29.9 if i % 2 == 0 else 12.0)
        for i in range(108)
    ]
    featured, decision = _featured_limit_up(entries)

    with caplog.at_level(logging.WARNING, logger="src.universe.policy"):
        result = select_universe_detailed(featured, decision, slot_budget=100)

    assert result.selected.height == 100
    assert result.dropped.height == 8
    untruncated = select_universe_detailed(featured, decision, slot_budget=200)
    ref = {row["symbol"]: row for row in untruncated.selected.to_dicts()}
    for row in result.selected.to_dicts():
        expected = ref[row["symbol"]]
        assert row["selection_reasons"] == expected["selection_reasons"]
        assert row["daily_change_pct"] == expected["daily_change_pct"]
        assert row["trade_value_100m"] == expected["trade_value_100m"]
        assert row["tv_ratio"] == expected["tv_ratio"]
    warnings = [rec for rec in caplog.records if "TRUNCATED" in rec.message]
    assert len(warnings) == 1
    message = warnings[0].message
    assert "kept=100" in message
    assert "dropped=8" in message
    head = message.split("dropped_head=")[1]
    head_symbols = head.split(",")
    assert len(head_symbols) >= 1
    assert len(head_symbols) <= DROPPED_LOG_SAMPLE


def test_select_universe_non_finite_trade_value_ranks_last() -> None:
    from src.universe.policy import select_universe_detailed

    featured, decision = _featured_limit_up([
        ("000001", 500.0, 29.9), ("000002", 300.0, 29.9),
        ("000003", 200.0, 29.9), ("000004", float("nan"), 29.9),
    ])

    result = select_universe_detailed(featured, decision, slot_budget=3)

    assert result.selected["symbol"].to_list() == ["000001", "000002", "000003"]
    assert result.dropped["symbol"].to_list() == ["000004"]


def test_select_universe_compatibility_wrapper() -> None:
    from polars.testing import assert_frame_equal

    from src.universe.policy import select_universe, select_universe_detailed

    featured, decision = _featured_limit_up([
        ("000002", 200.0, 29.9), ("000001", 400.0, 29.9), ("000003", 100.0, 29.9),
    ])

    assert_frame_equal(
        select_universe(featured, decision, slot_budget=2),
        select_universe_detailed(featured, decision, slot_budget=2).selected,
    )
