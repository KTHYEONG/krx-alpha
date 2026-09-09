"""L1 체결 틱 디코드 + 정합성 검증 (배치 단계, 수집 루프 미접촉)."""

from __future__ import annotations

import json
from dataclasses import dataclass

import polars as pl

_TICK_STREAM: str = "H0STCNT0"
_PRICE_BAND_RATIO: float = 0.30


@dataclass(frozen=True)
class TickQualitySummary:
    rows: int
    decode_fail: int
    zero_volume: int
    price_band_violation: int
    cum_volume_regression: int


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
        }
    except (json.JSONDecodeError, KeyError, TypeError, AttributeError):
        return None


def decode_and_flag_ticks(df: pl.DataFrame) -> TickQualitySummary | None:
    ticks = df.filter(pl.col("tr_id") == _TICK_STREAM)
    if ticks.height == 0:
        return None
    body_dtype = pl.Struct(dict.fromkeys(("shcode", "price", "cvolume", "volume", "change", "sign"), pl.String))
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
        ).select("shcode", "price_raw", "cvolume_raw", "volume_raw", "change_raw", "sign", "recv_wall_ns")
    except pl.exceptions.ComputeError:
        # 벤더 JSON 파싱 자체가 실패한 배치: 행 단위 폴백은 문자열 그대로 남기고
        # 숫자 캐스팅은 아래 strict=False cast 한 곳에서만 수행한다 (ValueError 이중 발생 지점 제거).
        fallback: list[dict[str, object]] = []
        for raw, wall in zip(ticks["raw"].to_list(), ticks["recv_wall_ns"].to_list(), strict=True):
            row = _safe_parse_ls_body(raw)
            if row is None:
                row = {"shcode": None, "price": None, "cvolume": None, "volume": None, "change": None, "sign": None}
            fallback.append({
                "shcode": row["shcode"],
                "price_raw": row["price"],
                "cvolume_raw": row["cvolume"],
                "volume_raw": row["volume"],
                "change_raw": row["change"],
                "sign": row["sign"],
                "recv_wall_ns": wall,
            })
        raw_fields = pl.DataFrame(
            fallback,
            schema={
                "shcode": pl.String, "price_raw": pl.String, "cvolume_raw": pl.String,
                "volume_raw": pl.String, "change_raw": pl.String, "sign": pl.String, "recv_wall_ns": pl.Int64,
            },
            strict=False,
        )
    frame = raw_fields.with_columns(
        price=pl.col("price_raw").cast(pl.Float64, strict=False),
        cvolume=pl.col("cvolume_raw").cast(pl.Int64, strict=False),
        volume=pl.col("volume_raw").cast(pl.Int64, strict=False),
        change=pl.col("change_raw").cast(pl.Float64, strict=False),
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
    )
    flagged = flagged.with_columns(
        dq_zero_volume=(~pl.col("dq_decode_fail")) & (pl.col("cvolume") <= 0),
        dq_price_band_violation=(~pl.col("dq_decode_fail"))
        & (pl.col("ref_price") > 0)
        & (
            (pl.col("price") > pl.col("ref_price") * (1 + _PRICE_BAND_RATIO))
            | (pl.col("price") < pl.col("ref_price") * (1 - _PRICE_BAND_RATIO))
        ),
    )
    flagged = flagged.sort(["shcode", "recv_wall_ns"]).with_columns(
        volume_prev=pl.col("volume").shift(1).over("shcode"),
    )
    flagged = flagged.with_columns(
        dq_cum_volume_regression=(~pl.col("dq_decode_fail")) & pl.col("volume_prev").is_not_null() & (pl.col("volume") < pl.col("volume_prev")),
    )
    return TickQualitySummary(
        rows=ticks.height,
        decode_fail=int(flagged["dq_decode_fail"].sum()),
        zero_volume=int(flagged["dq_zero_volume"].sum()),
        price_band_violation=int(flagged["dq_price_band_violation"].sum()),
        cum_volume_regression=int(flagged["dq_cum_volume_regression"].sum()),
    )
