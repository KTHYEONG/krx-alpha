"""KRX 2023-01-25 통합 호가단위 (KOSPI/KOSDAQ 공통, 하한 이상)."""

from __future__ import annotations

TICK_TABLE: tuple[tuple[int, int], ...] = (
    (500_000, 1_000),
    (200_000, 500),
    (50_000, 100),
    (20_000, 50),
    (5_000, 10),
    (2_000, 5),
    (0, 1),
)


def tick_of(price: int) -> int:
    """가격이 속한 구간의 호가단위를 반환한다."""
    if price < 1:
        raise ValueError(f"price must be >= 1: {price}")
    for floor, tick in TICK_TABLE:
        if price >= floor:
            return tick
    raise ValueError(f"price must be >= 1: {price}")


def is_tick_aligned(price: int) -> bool:
    """주문가로 허용 가능한 격자 가격인지 판정한다."""
    if price < 1:
        return False
    return price % tick_of(price) == 0
