"""NXT premarket pool selection invariant guards (part2)."""

from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo

import pytest

from src.brokers.kis.data import KisRankingRow
from src.execution.contracts import KisApiError
from src.universe.ipc import read_candidate_snapshot, write_candidate_snapshot
from src.universe.premarket import PremarketUniverseError, premarket_pool_ready, refresh_premarket_pool

KST = ZoneInfo("Asia/Seoul")
GENERATED = dt.datetime(2026, 10, 1, 20, 5, tzinfo=KST)
TARGET = dt.date(2026, 10, 2)
EFFECTIVE = dt.datetime(2026, 10, 2, 8, 0, tzinfo=KST)


def _nx_rows(symbols: tuple[str, ...], change: float = 1.0) -> tuple[KisRankingRow, ...]:
    return tuple(
        KisRankingRow(symbol=symbol, rank=index + 1, change_pct=change, trade_value_krw=100)
        for index, symbol in enumerate(symbols)
    )


class _NxClient:
    def __init__(
        self,
        trade_amount: tuple[KisRankingRow, ...],
        fluctuation: tuple[KisRankingRow, ...],
        exc: Exception | None = None,
    ) -> None:
        self._ta = trade_amount
        self._fl = fluctuation
        self._exc = exc
        self.divs: list[str] = []

    def get_trade_amount_ranking(self, *, market_div: str = "J") -> tuple[KisRankingRow, ...]:
        self.divs.append(market_div)
        if self._exc is not None:
            raise self._exc
        return self._ta

    def get_fluctuation_ranking(self, *, market_div: str = "J") -> tuple[KisRankingRow, ...]:
        self.divs.append(market_div)
        if self._exc is not None:
            raise self._exc
        return self._fl


def _symbols(start: int, count: int) -> tuple[str, ...]:
    return tuple(f"{start + index:06d}" for index in range(count))


def test_pool_is_nxt_ranked_and_capacity_bounded(tmp_path) -> None:
    ta = _nx_rows(_symbols(1, 30))
    fl = _nx_rows(_symbols(21, 30))
    client = _NxClient(ta, fl)
    out = tmp_path / "2026-10-02.json"

    snapshot = refresh_premarket_pool(
        target_date=TARGET, generated_at=GENERATED, effective_from=EFFECTIVE,
        client=client, out_path=out, capacity=20,
    )

    assert client.divs == ["NX", "NX"]
    assert snapshot.session == "premarket"
    assert snapshot.rev == 20261002
    assert snapshot.policy_version == "premarket_v1"
    assert [row["rank"] for row in snapshot.candidates] == list(range(1, 21))
    assert [row["symbol"] for row in snapshot.candidates] == [f"{i:06d}" for i in range(1, 21)]
    assert out.is_file()


def test_limit_up_symbols_are_prioritized(tmp_path) -> None:
    ta = _nx_rows(_symbols(1, 5))
    fl = (*_nx_rows(_symbols(1, 4)), KisRankingRow(symbol="999999", rank=5, change_pct=29.5, trade_value_krw=100))
    client = _NxClient(ta, fl)
    out = tmp_path / "pool.json"

    snapshot = refresh_premarket_pool(
        target_date=TARGET, generated_at=GENERATED, effective_from=EFFECTIVE,
        client=client, out_path=out, capacity=5,
    )

    assert snapshot.candidates[0]["symbol"] == "999999"
    assert snapshot.candidates[0]["rank"] == 1


def test_pool_is_point_in_time(tmp_path) -> None:
    client = _NxClient(_nx_rows(_symbols(1, 2)), _nx_rows(_symbols(1, 2)))
    out = tmp_path / "pool.json"

    with pytest.raises(PremarketUniverseError, match="before the target date"):
        refresh_premarket_pool(
            target_date=TARGET,
            generated_at=dt.datetime(2026, 10, 2, 7, 0, tzinfo=KST),
            effective_from=EFFECTIVE, client=client, out_path=out, capacity=20,
        )

    assert not out.exists()


def test_effective_time_must_be_on_target_date(tmp_path) -> None:
    client = _NxClient(_nx_rows(_symbols(1, 2)), _nx_rows(_symbols(1, 2)))
    out = tmp_path / "pool.json"

    with pytest.raises(PremarketUniverseError, match="effective_from"):
        refresh_premarket_pool(
            target_date=TARGET, generated_at=GENERATED,
            effective_from=dt.datetime(2026, 10, 1, 20, 5, tzinfo=KST),
            client=client, out_path=out, capacity=20,
        )

    assert not out.exists()


def test_excluded_symbols_never_enter_pool(tmp_path) -> None:
    symbols = _symbols(1, 3)
    client = _NxClient(_nx_rows(symbols), _nx_rows(symbols))
    out = tmp_path / "pool.json"

    snapshot = refresh_premarket_pool(
        target_date=TARGET, generated_at=GENERATED, effective_from=EFFECTIVE,
        client=client, out_path=out, capacity=20, excluded_symbols=frozenset({"000001"}),
    )

    assert "000001" not in [row["symbol"] for row in snapshot.candidates]


def test_empty_or_invalid_ranking_fails_closed(tmp_path) -> None:
    symbols = _symbols(1, 2)
    client = _NxClient(_nx_rows(symbols), _nx_rows(symbols))
    out = tmp_path / "pool.json"

    with pytest.raises(PremarketUniverseError, match="no eligible"):
        refresh_premarket_pool(
            target_date=TARGET, generated_at=GENERATED, effective_from=EFFECTIVE,
            client=client, out_path=out, capacity=20,
            excluded_symbols=frozenset({"000001", "000002"}),
        )

    assert not out.exists()


def test_vendor_failure_leaves_no_file(tmp_path) -> None:
    client = _NxClient((), (), exc=KisApiError("TIMEOUT", "timeout"))
    out = tmp_path / "pool.json"

    with pytest.raises(KisApiError, match="TIMEOUT"):
        refresh_premarket_pool(
            target_date=TARGET, generated_at=GENERATED, effective_from=EFFECTIVE,
            client=client, out_path=out, capacity=20,
        )

    assert not out.exists()


def test_readiness_reflects_file_validity(tmp_path) -> None:
    missing = tmp_path / "missing.json"
    corrupt = tmp_path / "corrupt.json"
    corrupt.write_text("{not json", encoding="utf-8")
    valid = tmp_path / "valid.json"
    refresh_premarket_pool(
        target_date=TARGET, generated_at=GENERATED, effective_from=EFFECTIVE,
        client=_NxClient(_nx_rows(_symbols(1, 2)), _nx_rows(_symbols(1, 2))),
        out_path=valid, capacity=20,
    )
    other_session = tmp_path / "other_session.json"
    from src.universe.aftermarket import build_aftermarket_snapshot

    aftermarket = build_aftermarket_snapshot(
        session_date=TARGET, generated_at=GENERATED,
        trade_amount_rows=_nx_rows(_symbols(1, 2)), fluctuation_rows=_nx_rows(_symbols(1, 2)),
        capacity=20,
    )
    write_candidate_snapshot(other_session, aftermarket)

    assert premarket_pool_ready(missing, target_date=TARGET, max_candidates=20) is False
    assert premarket_pool_ready(corrupt, target_date=TARGET, max_candidates=20) is False
    assert premarket_pool_ready(valid, target_date=dt.date(2026, 10, 3), max_candidates=20) is False
    assert premarket_pool_ready(other_session, target_date=TARGET, max_candidates=20) is False
    assert premarket_pool_ready(valid, target_date=TARGET, max_candidates=20) is True


def test_round_trip_with_reader(tmp_path) -> None:
    out = tmp_path / "pool.json"
    snapshot = refresh_premarket_pool(
        target_date=TARGET, generated_at=GENERATED, effective_from=EFFECTIVE,
        client=_NxClient(_nx_rows(_symbols(1, 2)), _nx_rows(_symbols(1, 2))),
        out_path=out, capacity=20,
    )

    reread = read_candidate_snapshot(out, expected_session_date=TARGET, expected_session="premarket", max_candidates=20)

    assert reread == snapshot
    assert reread.source_asof <= reread.generated_at <= reread.effective_from
