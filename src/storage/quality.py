"""L1 체결 틱 디코드 + 정합성 검증 (배치 단계, 수집 루프 미접촉)."""

from __future__ import annotations

import itertools
import json
from dataclasses import dataclass

import polars as pl

_TICK_STREAM: str = "H0STCNT0"
_PRICE_BAND_RATIO: float = 0.30

_TICK_BODY_FIELDS: tuple[str, ...] = ("shcode", "price", "cvolume", "volume", "change", "sign", "drate")
_QUOTE_STREAM: str = "H0STASP0"
_SCHEMA_DISAGREE_TOLERANCE: float = 0.01
_QUOTE_LEVELS: int = 10
_QUOTE_CHUNK_ROWS: int = 20_000
_AUCTION_WINDOWS: tuple[tuple[int, int], ...] = ((83000, 90000), (152000, 153000))
_QUOTE_BODY_FIELDS: tuple[str, ...] = (
    "shcode",
    "hotime",
    "totofferrem",
    "totbidrem",
    *[f"offerho{k}" for k in range(1, _QUOTE_LEVELS + 1)],
    *[f"bidho{k}" for k in range(1, _QUOTE_LEVELS + 1)],
    *[f"offerrem{k}" for k in range(1, _QUOTE_LEVELS + 1)],
    *[f"bidrem{k}" for k in range(1, _QUOTE_LEVELS + 1)],
)


@dataclass(frozen=True)
class TickQualitySummary:
    rows: int
    decode_fail: int
    zero_volume: int
    price_band_violation: int
    cum_volume_regression: int
    schema_disagree: int
    tick_loss: int
    tick_duplicate: int
    lost_volume: int


@dataclass(frozen=True)
class QuoteQualitySummary:
    rows: int
    decode_fail: int
    ladder_disorder: int
    crossed_book: int
    negative_remain: int
    total_remain_short: int


def _safe_parse_ls_body(raw: str) -> dict[str, object] | None:
    try:
        body = json.loads(raw)["body"]
        return {
            "shcode": body["shcode"],
            "price": body["price"],
            "cvolume": body["cvolume"],
            "volume": body["volume"],
            "change": body["change"],
            "sign": body["sign"],
            "drate": body["drate"],
        }
    except (json.JSONDecodeError, KeyError, TypeError, AttributeError):
        return None


def _safe_parse_ls_quote_body(raw: str) -> dict[str, object] | None:
    try:
        body = json.loads(raw)["body"]
        return {field: body[field] for field in _QUOTE_BODY_FIELDS}
    except (json.JSONDecodeError, KeyError, TypeError, AttributeError):
        return None


def decode_and_flag_ticks(df: pl.DataFrame) -> TickQualitySummary | None:
    ticks = df.filter(pl.col("tr_id") == _TICK_STREAM)
    if ticks.height == 0:
        return None
    body_dtype = pl.Struct(dict.fromkeys(_TICK_BODY_FIELDS, pl.String))
    raw_dtype = pl.Struct({"header": pl.Struct({"tr_cd": pl.String, "tr_key": pl.String}), "body": body_dtype})
    try:
        decoded = ticks.with_columns(pl.col("raw").str.json_decode(raw_dtype).alias("decoded"))
        raw_fields = decoded.with_columns(
            shcode=pl.col("decoded").struct.field("body").struct.field("shcode"),
            price_raw=pl.col("decoded").struct.field("body").struct.field("price"),
            cvolume_raw=pl.col("decoded").struct.field("body").struct.field("cvolume"),
            volume_raw=pl.col("decoded").struct.field("body").struct.field("volume"),
            change_raw=pl.col("decoded").struct.field("body").struct.field("change"),
            sign=pl.col("decoded").struct.field("body").struct.field("sign"),
            drate_raw=pl.col("decoded").struct.field("body").struct.field("drate"),
        ).select("shcode", "price_raw", "cvolume_raw", "volume_raw", "change_raw", "sign", "drate_raw", "recv_wall_ns")
    except pl.exceptions.ComputeError:
        # 벤더 JSON 파싱 자체가 실패한 배치: 행 단위 폴백은 문자열 그대로 남기고
        # 숫자 캐스팅은 아래 strict=False cast 한 곳에서만 수행한다 (ValueError 이중 발생 지점 제거).
        fallback: list[dict[str, object]] = []
        for raw, wall in zip(ticks["raw"].to_list(), ticks["recv_wall_ns"].to_list(), strict=True):
            row = _safe_parse_ls_body(raw)
            if row is None:
                row = {"shcode": None, "price": None, "cvolume": None, "volume": None, "change": None, "sign": None, "drate": None}
            fallback.append({
                "shcode": row["shcode"],
                "price_raw": row["price"],
                "cvolume_raw": row["cvolume"],
                "volume_raw": row["volume"],
                "change_raw": row["change"],
                "sign": row["sign"],
                "drate_raw": row["drate"],
                "recv_wall_ns": wall,
            })
        raw_fields = pl.DataFrame(
            fallback,
            schema={
                "shcode": pl.String, "price_raw": pl.String, "cvolume_raw": pl.String,
                "volume_raw": pl.String, "change_raw": pl.String, "sign": pl.String,
                "drate_raw": pl.String, "recv_wall_ns": pl.Int64,
            },
            strict=False,
        )
    frame = raw_fields.with_columns(
        price=pl.col("price_raw").cast(pl.Float64, strict=False),
        cvolume=pl.col("cvolume_raw").cast(pl.Int64, strict=False),
        volume=pl.col("volume_raw").cast(pl.Int64, strict=False),
        change=pl.col("change_raw").cast(pl.Float64, strict=False),
        drate=pl.col("drate_raw").cast(pl.Float64, strict=False),
    )
    flagged = frame.with_columns(
        dq_decode_fail=(
            pl.col("shcode").is_null()
            | pl.col("price").is_null()
            | pl.col("price").is_nan()
            | pl.col("cvolume").is_null()
            | pl.col("volume").is_null()
        ),
        ref_price=pl.when(pl.col("sign").is_in(["1", "2"])).then(pl.col("price") - pl.col("change")).otherwise(pl.col("price") + pl.col("change")),
        drate_factor=1 + pl.col("drate") / 100,
    )
    flagged = flagged.with_columns(
        ref_from_drate=pl.when(pl.col("drate_factor").abs() > 1e-9)
        .then(pl.col("price") / pl.col("drate_factor"))
        .otherwise(None),
    )
    flagged = flagged.with_columns(
        dq_zero_volume=(~pl.col("dq_decode_fail")) & (pl.col("cvolume") <= 0),
        dq_price_band_violation=(~pl.col("dq_decode_fail"))
        & (pl.col("ref_price") > 0)
        & (
            (pl.col("price") > pl.col("ref_price") * (1 + _PRICE_BAND_RATIO))
            | (pl.col("price") < pl.col("ref_price") * (1 - _PRICE_BAND_RATIO))
        ),
        dq_schema_disagree=(~pl.col("dq_decode_fail"))
        & pl.col("ref_from_drate").is_not_null()
        & ((pl.col("ref_price") - pl.col("ref_from_drate")).abs() > pl.col("ref_from_drate").abs() * _SCHEMA_DISAGREE_TOLERANCE),
    )
    flagged = flagged.sort(["shcode", "recv_wall_ns"]).with_columns(
        volume_prev=pl.col("volume").shift(1).over("shcode"),
    )
    flagged = flagged.with_columns(
        vol_delta=pl.col("volume") - pl.col("volume_prev"),
    )
    flagged = flagged.with_columns(
        dq_cum_volume_regression=(~pl.col("dq_decode_fail")) & pl.col("volume_prev").is_not_null() & (pl.col("volume") < pl.col("volume_prev")),
        dq_tick_loss=(~pl.col("dq_decode_fail")) & (pl.col("cvolume") > 0) & pl.col("volume_prev").is_not_null() & (pl.col("vol_delta") > pl.col("cvolume")),
        dq_tick_duplicate=(~pl.col("dq_decode_fail")) & (pl.col("cvolume") > 0) & pl.col("volume_prev").is_not_null() & (pl.col("vol_delta") < pl.col("cvolume")),
        lost_volume=pl.when((~pl.col("dq_decode_fail")) & (pl.col("cvolume") > 0) & pl.col("volume_prev").is_not_null() & (pl.col("vol_delta") > pl.col("cvolume")))
        .then(pl.col("vol_delta") - pl.col("cvolume"))
        .otherwise(0),
    )
    return TickQualitySummary(
        rows=ticks.height,
        decode_fail=int(flagged["dq_decode_fail"].sum()),
        zero_volume=int(flagged["dq_zero_volume"].sum()),
        price_band_violation=int(flagged["dq_price_band_violation"].sum()),
        cum_volume_regression=int(flagged["dq_cum_volume_regression"].sum()),
        schema_disagree=int(flagged["dq_schema_disagree"].sum()),
        tick_loss=int(flagged["dq_tick_loss"].sum()),
        tick_duplicate=int(flagged["dq_tick_duplicate"].sum()),
        lost_volume=int(flagged["lost_volume"].sum()),
    )


def _flag_quote_invariants(frame: pl.DataFrame) -> pl.DataFrame:
    offer_cols = [f"offerho{k}" for k in range(1, _QUOTE_LEVELS + 1)]
    bid_cols = [f"bidho{k}" for k in range(1, _QUOTE_LEVELS + 1)]
    rem_cols = [*[f"offerrem{k}" for k in range(1, _QUOTE_LEVELS + 1)], *[f"bidrem{k}" for k in range(1, _QUOTE_LEVELS + 1)]]
    ladder_bad = pl.lit(False)
    for a, b in itertools.pairwise(offer_cols):
        ladder_bad = ladder_bad | ((pl.col(a) > 0) & (pl.col(b) > 0) & (pl.col(a) >= pl.col(b)))
    for a, b in itertools.pairwise(bid_cols):
        ladder_bad = ladder_bad | ((pl.col(a) > 0) & (pl.col(b) > 0) & (pl.col(a) <= pl.col(b)))
    neg_rem = pl.lit(False)
    for c in rem_cols:
        neg_rem = neg_rem | (pl.col(c) < 0)
    in_auction = pl.lit(False)
    for lo, hi in _AUCTION_WINDOWS:
        in_auction = in_auction | ((pl.col("hotime") >= lo) & (pl.col("hotime") < hi))
    top_offer_sum = sum((pl.col(c) for c in [f"offerrem{k}" for k in range(1, _QUOTE_LEVELS + 1)]), start=pl.lit(0))
    top_bid_sum = sum((pl.col(c) for c in [f"bidrem{k}" for k in range(1, _QUOTE_LEVELS + 1)]), start=pl.lit(0))
    return frame.with_columns(
        dq_decode_fail=(
            pl.col("shcode").is_null()
            | pl.col("hotime").is_null()
            | pl.col("totofferrem").is_null()
            | pl.col("totbidrem").is_null()
        ),
    ).with_columns(
        dq_ladder_disorder=(~pl.col("dq_decode_fail")) & ladder_bad,
        dq_negative_remain=(~pl.col("dq_decode_fail")) & neg_rem,
        dq_crossed_book=(~pl.col("dq_decode_fail")) & (~in_auction) & (pl.col("bidho1") > 0) & (pl.col("offerho1") > 0) & (pl.col("bidho1") >= pl.col("offerho1")),
        dq_total_remain_short=(~pl.col("dq_decode_fail")) & ((pl.col("totofferrem") < top_offer_sum) | (pl.col("totbidrem") < top_bid_sum)),
    )


def decode_and_flag_quotes(df: pl.DataFrame, *, chunk_rows: int = _QUOTE_CHUNK_ROWS) -> QuoteQualitySummary | None:
    quotes = df.filter(pl.col("tr_id") == _QUOTE_STREAM)
    if quotes.height == 0:
        return None
    body_dtype = pl.Struct(dict.fromkeys(_QUOTE_BODY_FIELDS, pl.String))
    raw_dtype = pl.Struct({"header": pl.Struct({"tr_cd": pl.String, "tr_key": pl.String}), "body": body_dtype})
    num_fields = [f for f in _QUOTE_BODY_FIELDS if f != "shcode"]
    decode_fail = 0
    ladder_disorder = 0
    crossed_book = 0
    negative_remain = 0
    total_remain_short = 0
    for offset in range(0, quotes.height, chunk_rows):
        chunk = quotes.slice(offset, chunk_rows)
        try:
            decoded = chunk.with_columns(pl.col("raw").str.json_decode(raw_dtype).alias("decoded"))
            selects = [pl.col("decoded").struct.field("body").struct.field("shcode").alias("shcode")]
            selects.extend(
                pl.col("decoded").struct.field("body").struct.field(c).cast(pl.Int32, strict=False).alias(c) for c in num_fields
            )
            frame = decoded.select(selects)
        except pl.exceptions.ComputeError:
            fallback: list[dict[str, object]] = []
            for raw in chunk["raw"].to_list():
                row = _safe_parse_ls_quote_body(raw)
                if row is None:
                    fallback.append(dict.fromkeys(_QUOTE_BODY_FIELDS, None))
                else:
                    fallback.append({field: row[field] for field in _QUOTE_BODY_FIELDS})
            raw_frame = pl.DataFrame(
                fallback,
                schema={**{"shcode": pl.String}, **dict.fromkeys(num_fields, pl.String)},
                strict=False,
            )
            casts = [pl.col("shcode")]
            casts.extend(pl.col(c).cast(pl.Int32, strict=False).alias(c) for c in num_fields)
            frame = raw_frame.select(casts)
        flagged = _flag_quote_invariants(frame)
        decode_fail += int(flagged["dq_decode_fail"].sum())
        ladder_disorder += int(flagged["dq_ladder_disorder"].sum())
        crossed_book += int(flagged["dq_crossed_book"].sum())
        negative_remain += int(flagged["dq_negative_remain"].sum())
        total_remain_short += int(flagged["dq_total_remain_short"].sum())
    return QuoteQualitySummary(
        rows=quotes.height,
        decode_fail=decode_fail,
        ladder_disorder=ladder_disorder,
        crossed_book=crossed_book,
        negative_remain=negative_remain,
        total_remain_short=total_remain_short,
    )
