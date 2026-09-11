
def test_tick_of_matches_krx_2023_table_boundaries() -> None:
    # Given: 2023-01-25 KOSPI/KOSDAQ 통합 호가단위 경계값
    import pytest

    from src.execution.ticks import is_tick_aligned, tick_of

    cases = {
        1: 1, 1_999: 1, 2_000: 5, 4_995: 5, 5_000: 10, 19_990: 10, 20_000: 50, 49_950: 50,
        50_000: 100, 199_900: 100, 200_000: 500, 499_500: 500, 500_000: 1_000, 2_500_000: 1_000,
    }

    # When / Then: 구간 하한 이상 ~ 다음 하한 미만 단위
    for price, tick in cases.items():
        assert tick_of(price) == tick, price

    # Then: 반호가 체결가(35,325 / 3,387)는 주문가로 쓸 수 없다
    assert is_tick_aligned(259_500) is True
    assert is_tick_aligned(35_325) is False
    assert is_tick_aligned(3_387) is False
    assert is_tick_aligned(0) is False
    with pytest.raises(ValueError, match="price"):
        tick_of(0)
