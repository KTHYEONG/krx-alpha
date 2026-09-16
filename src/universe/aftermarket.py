"""애프터마켓 유니버스 선정: KIS 랭킹 union 결정적 할당 (세션 격리)."""

from __future__ import annotations

import datetime as dt
import math
import pathlib
from typing import cast

from src.core.errors import KrxAlphaError
from src.execution.kis_client import KisRankingRow, KisRestClient
from src.universe.ipc import CandidateSnapshot, write_candidate_snapshot


class AftermarketUniverseError(KrxAlphaError):
    """애프터마켓 선정 fail-closed 신호."""


def build_aftermarket_snapshot(
    *,
    session_date: dt.date,
    generated_at: dt.datetime,
    trade_amount_rows: tuple[KisRankingRow, ...],
    fluctuation_rows: tuple[KisRankingRow, ...],
    capacity: int,
    policy_version: str = "aftermarket_v1",
) -> CandidateSnapshot:
    kst_offset = dt.timedelta(hours=9)
    tz_ok = generated_at.tzinfo is not None and generated_at.utcoffset() == kst_offset
    ta_symbols = [row.symbol for row in trade_amount_rows]
    fl_symbols = [row.symbol for row in fluctuation_rows]
    def _valid_rows(rows: tuple[KisRankingRow, ...], symbols: list[str]) -> bool:
        return (
            bool(rows)
            and len(set(symbols)) == len(symbols)
            and all(symbol.isdigit() and len(symbol) == 6 for symbol in symbols)
            and all(
                isinstance(row.rank, int)
                and row.rank >= 1
                and isinstance(row.trade_value_krw, int)
                and row.trade_value_krw >= 0
                and isinstance(row.change_pct, (int, float))
                and math.isfinite(float(row.change_pct))
                for row in rows
            )
        )

    ta_ok = _valid_rows(trade_amount_rows, ta_symbols)
    fl_ok = _valid_rows(fluctuation_rows, fl_symbols)
    if not tz_ok or capacity < 1 or not ta_ok or not fl_ok:
        raise AftermarketUniverseError("invalid aftermarket source")
    ta_rank = {row.symbol: row.rank for row in trade_amount_rows}
    fl_rank = {row.symbol: row.rank for row in fluctuation_rows}
    ta_by_symbol = {row.symbol: row for row in trade_amount_rows}
    fl_by_symbol = {row.symbol: row for row in fluctuation_rows}
    union = set(ta_rank) | set(fl_rank)
    ordered: list[dict[str, object]] = []
    for symbol in union:
        ta_row = ta_by_symbol.get(symbol)
        fl_row = fl_by_symbol.get(symbol)
        change_row = cast(KisRankingRow, fl_row if fl_row is not None else ta_row)
        value_row = cast(KisRankingRow, ta_row if ta_row is not None else fl_row)
        change = float(change_row.change_pct)
        limit_up = change >= 29.0
        source_ranks: dict[str, int] = {}
        if symbol in ta_rank:
            source_ranks["trade_amount"] = ta_rank[symbol]
        if symbol in fl_rank:
            source_ranks["fluctuation"] = fl_rank[symbol]
        reasons: list[str] = []
        if limit_up:
            reasons.append("limit_up")
        if symbol in ta_rank:
            reasons.append("trade_amount")
        if symbol in fl_rank:
            reasons.append("fluctuation")
        ordered.append({"symbol": symbol, "limit_up": limit_up, "ta_rank": ta_rank.get(symbol), "fl_rank": fl_rank.get(symbol), "source_ranks": source_ranks, "metrics": {"trade_value_krw": value_row.trade_value_krw, "change_pct": change}, "selection_reasons": reasons})
    ordered.sort(key=lambda row: (0 if row["limit_up"] else 1, row["ta_rank"] if row["ta_rank"] is not None else float("inf"), row["fl_rank"] if row["fl_rank"] is not None else float("inf"), str(row["symbol"])))
    selected_rows = ordered[:capacity]
    candidates: list[dict[str, object]] = []
    for index, row in enumerate(selected_rows, start=1):
        candidates.append({"symbol": row["symbol"], "rank": index, "source_ranks": row["source_ranks"], "metrics": row["metrics"], "selection_reasons": row["selection_reasons"]})
    return CandidateSnapshot(schema_version=1, rev=int(session_date.strftime("%Y%m%d")), session_date=session_date, session="aftermarket", generated_at=generated_at, source_asof=generated_at, effective_from=generated_at, policy_version=policy_version, capacity=capacity, eligible_count=len(union), selected_count=len(selected_rows), candidates=tuple(candidates))


def refresh_aftermarket_candidates(
    *,
    session_date: dt.date,
    generated_at: dt.datetime,
    client: KisRestClient,
    out_path: pathlib.Path,
    capacity: int,
) -> CandidateSnapshot:
    trade_amount_rows = client.get_trade_amount_ranking()
    fluctuation_rows = client.get_fluctuation_ranking()
    snapshot = build_aftermarket_snapshot(session_date=session_date, generated_at=generated_at, trade_amount_rows=trade_amount_rows, fluctuation_rows=fluctuation_rows, capacity=capacity)
    write_candidate_snapshot(out_path, snapshot)
    return snapshot
