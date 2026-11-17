"""Toss unified-stream decoding and cumulative-free DQ behavior."""

from __future__ import annotations

import json
import logging
import pathlib

import polars as pl
import pytest
import zstandard as zstd

from src.storage.normalization import normalize_l0_partition
from src.storage.quality import (
    DQ_SKIPPED_RULES,
    decode_and_flag_quotes,
    decode_and_flag_ticks,
    evaluate_stream_quality,
)
from src.storage.quality_toss import decode_toss_quotes, decode_toss_ticks
from src.core.config import DataQualitySettings


def _trade(symbol: str = "000660", price: str = "1108000", volume: str = "1", ts: str = "2026-11-17T09:45:24.821+09:00") -> str:
    return json.dumps(
        {"type": "message", "topic": f"trade:kr:{symbol}", "data": {"price": price, "volume": volume, "timestamp": ts, "currency": "KRW"}},
        separators=(",", ":"),
    )


def _book(symbol: str = "005930", ask0: int = 192800, bid0: int = 192700, levels: int = 11, ts: str = "2026-11-17T09:45:24.822+09:00") -> str:
    asks = [{"price": str(ask0 + 100 * i), "volume": str(100 + i)} for i in range(levels)]
    bids = [{"price": str(bid0 - 100 * i), "volume": str(200 + i)} for i in range(levels)]
    return json.dumps(
        {"type": "message", "topic": f"orderbook:kr:{symbol}", "data": {"timestamp": ts, "currency": "KRW", "asks": asks, "bids": bids}},
        separators=(",", ":"),
    )


def _frame(raws: list[str], tr_id: str, *, vendor: str = "toss") -> pl.DataFrame:
    return pl.DataFrame(
        {
            "raw": raws,
            "tr_id": [tr_id] * len(raws),
            "vendor": [vendor] * len(raws),
            "recv_wall_ns": [100 + i for i in range(len(raws))],
            "conn_id": ["c1"] * len(raws),
        }
    )


def test_decode_toss_ticks_maps_trade_and_nulls_cumulative_columns() -> None:
    out = decode_toss_ticks(_frame([_trade()], "H0STCNT0"))
    row = out.row(0, named=True)
    assert row["shcode"] == "000660"
    assert row["price_raw"] == "1108000"
    assert row["cvolume_raw"] == "1"
    for column in ("volume_raw", "change_raw", "sign", "drate_raw", "mdchecnt_raw", "mschecnt_raw"):
        assert row[column] is None
    assert row["recv_wall_ns"] == 100


def test_decode_toss_ticks_malformed_rows_are_null_not_dropped() -> None:
    out = decode_toss_ticks(_frame(["not json", _trade(), '{"topic":"trade:kr:000660","data":[1]}'], "H0STCNT0"))
    assert out.height == 3
    assert out["shcode"].to_list() == [None, "000660", None]


def test_decode_toss_quotes_takes_ten_levels_and_hotime_from_millisecond_stamp() -> None:
    row = decode_toss_quotes(_frame([_book()], "H0STASP0")).row(0, named=True)
    assert row["shcode"] == "005930"
    assert row["hotime"] == 94524
    assert row["offerho1"] == 192800
    assert row["offerho10"] == 193700
    assert row["bidrem10"] == 209
    assert row["totofferrem"] is None
    assert row["totbidrem"] is None


def test_decode_toss_quotes_short_book_zero_fills_and_bad_stamp_is_decode_failure() -> None:
    out = decode_toss_quotes(_frame([_book(levels=3), _book(ts="garbage")], "H0STASP0"))
    assert out["offerho4"].to_list()[0] == 0
    assert out["hotime"].to_list()[1] is None
    assert out["offerho1"].to_list()[1] is None


def test_fallback_path_matches_primary_for_numeric_fields() -> None:
    odd = json.dumps({"type": "message", "topic": "trade:kr:000660", "data": {"price": 1108000, "volume": 2}})
    out = decode_toss_ticks(_frame([odd], "H0STCNT0"))
    assert out.row(0, named=True)["price_raw"] == "1108000"
    assert out.row(0, named=True)["cvolume_raw"] == "2"


def test_toss_ticks_pass_dq_without_cumulative_fields() -> None:
    frame = _frame([_trade(volume="3"), _trade(volume="5", price="1109000")], "H0STCNT0")
    summary = decode_and_flag_ticks(frame)
    assert summary is not None
    assert summary.rows == 2
    assert summary.decode_fail == 0
    assert (summary.cum_volume_regression, summary.tick_loss, summary.price_band_violation, summary.schema_disagree) == (0, 0, 0, 0)


def test_toss_zero_volume_trade_is_still_flagged() -> None:
    summary = decode_and_flag_ticks(_frame([_trade(volume="0")], "H0STCNT0"))
    assert summary is not None
    assert summary.zero_volume == 1


def test_toss_quotes_skip_total_remain_but_keep_crossed_rule() -> None:
    frame = _frame([_book(), _book(ask0=192800, bid0=192800)], "H0STASP0")
    summary = decode_and_flag_quotes(frame)
    assert summary is not None
    assert summary.decode_fail == 0
    assert summary.total_remain_short == 0
    assert summary.crossed_book == 1


def test_verdict_reports_skipped_rules_for_toss_only() -> None:
    summary = decode_and_flag_ticks(_frame([_trade()], "H0STCNT0"))
    toss = evaluate_stream_quality(summary, None, settings=DataQualitySettings(), vendors=["toss"])
    assert toss.skipped == DQ_SKIPPED_RULES["toss"]
    assert json.loads(toss.to_metadata()["krx_alpha.dq"])["skipped"] == list(DQ_SKIPPED_RULES["toss"])
    other = evaluate_stream_quality(summary, None, settings=DataQualitySettings(), vendors=["kis"])
    assert other.skipped == ()
    assert "skipped" not in json.loads(other.to_metadata()["krx_alpha.dq"])


def _write_partition(part: pathlib.Path, raws: list[str], tr_id: str) -> None:
    part.mkdir(parents=True, exist_ok=True)
    records = [
        {
            "raw": raw, "recv_mono_ns": i, "recv_wall_ns": 1_000 + i, "conn_id": "toss-1", "conn_seq": i + 1,
            "vendor": "toss", "tr_id": tr_id, "venue": "unified", "session": "regular",
        }
        for i, raw in enumerate(raws)
    ]
    payload = "".join(json.dumps(r) + "\n" for r in records).encode("utf-8")
    (part / "09.jsonl.zst").write_bytes(zstd.ZstdCompressor(level=3).compress(payload))


def test_unified_venue_partition_normalizes_with_skipped_rules(tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture) -> None:
    trade_part = tmp_path / "l0" / "toss" / "unified" / "regular" / "H0STCNT0" / "dt=2026-11-17"
    book_part = tmp_path / "l0" / "toss" / "unified" / "regular" / "H0STASP0" / "dt=2026-11-17"
    _write_partition(trade_part, [_trade(volume=str(i + 1)) for i in range(5)], "H0STCNT0")
    _write_partition(book_part, [_book()], "H0STASP0")
    out = tmp_path / "trade.parquet"
    with caplog.at_level(logging.INFO, logger="src.storage.normalization"):
        assert normalize_l0_partition(trade_part, out) == 5
        assert normalize_l0_partition(book_part, tmp_path / "book.parquet") == 1
    metadata = __import__("pyarrow.parquet", fromlist=["read_metadata"]).read_metadata(out).metadata
    verdict = json.loads(metadata[b"krx_alpha.dq"])
    assert verdict["status"] == "PASS"
    assert verdict["skipped"] == list(DQ_SKIPPED_RULES["toss"])
    assert "stage=quality_skipped" in caplog.text
    assert pl.read_parquet(out).get_column("venue").unique().to_list() == ["unified"]


def test_decode_toss_quotes_fallback_path_matches_primary_and_nulls_malformed_rows() -> None:
    numeric = json.dumps(
        {
            "type": "message",
            "topic": "orderbook:kr:005930",
            "data": {
                "timestamp": "2026-11-17T09:45:24.822+09:00",
                "asks": [{"price": 192800, "volume": 5}],
                "bids": [{"price": 192700, "volume": 6}, "junk"],
            },
        }
    )
    out = decode_toss_quotes(_frame([_book(), numeric, "not json", '{"topic":"orderbook:kr:005930","data":[1]}'], "H0STASP0"))
    primary = out.row(0, named=True)
    fallback = out.row(1, named=True)
    assert out.height == 4
    assert (fallback["shcode"], fallback["hotime"]) == ("005930", 94524)
    assert (fallback["offerho1"], fallback["offerrem1"], fallback["bidho1"], fallback["bidrem1"]) == (192800, 5, 192700, 6)
    assert (fallback["bidho2"], fallback["offerho2"]) == (0, 0)
    assert primary["totofferrem"] is None
    for idx in (2, 3):
        assert all(value is None for value in out.row(idx))


@pytest.mark.parametrize("stamp", [None, "garbage", "2026-11-17T09:45:24"])
def test_decode_toss_quotes_fallback_requires_a_zoned_timestamp(stamp: object) -> None:
    odd = json.dumps({"topic": "orderbook:kr:005930", "data": {"timestamp": stamp, "asks": [{"price": 1, "volume": 1}]}})
    row = decode_toss_quotes(_frame([odd, "not json"], "H0STASP0")).row(0, named=True)
    assert row["hotime"] is None
    assert row["offerho1"] is None
