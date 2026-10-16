"""DB vendor frame decoding into the shared quality field schema."""

from __future__ import annotations

import json

import polars as pl

_DB_TICK_BODY_FIELDS: tuple[str, ...] = (
    "ShrnIscd",
    "StckPrpr",
    "CntgVol",
    "AcmlVol",
    "PrdyVrss",
    "PrdyVrsssign",
    "PrdyCtrt",
    "SelnCntgcsnu",
    "ShnuCntgcsnu",
)
_DB_QUOTE_BODY_FIELDS: tuple[str, ...] = (
    "ShrnIscd",
    "BsopHour",
    "TotalAskprsqn",
    "TotalBidprsqn",
    *[f"Askp{k}" for k in range(1, 11)],
    *[f"Bidp{k}" for k in range(1, 11)],
    *[f"AskpRsqn{k}" for k in range(1, 11)],
    *[f"BidpRsqn{k}" for k in range(1, 11)],
)
_QUOTE_BODY_FIELDS: tuple[str, ...] = (
    "shcode",
    "hotime",
    "totofferrem",
    "totbidrem",
    *[f"offerho{k}" for k in range(1, 11)],
    *[f"bidho{k}" for k in range(1, 11)],
    *[f"offerrem{k}" for k in range(1, 11)],
    *[f"bidrem{k}" for k in range(1, 11)],
)


def _safe_parse_db_tick_body(raw: str) -> dict[str, object] | None:
    try:
        body = json.loads(raw)["body"]
        if not isinstance(body, dict):
            return None
        return {field: body.get(field) for field in _DB_TICK_BODY_FIELDS}
    except (json.JSONDecodeError, KeyError, TypeError, AttributeError):
        return None


def _safe_parse_db_quote_body(raw: str) -> dict[str, object] | None:
    try:
        body = json.loads(raw)["body"]
        if not isinstance(body, dict):
            return None
        if any(body.get(field) is None for field in _DB_QUOTE_BODY_FIELDS):
            return None
        return {field: body[field] for field in _DB_QUOTE_BODY_FIELDS}
    except (json.JSONDecodeError, KeyError, TypeError, AttributeError):
        return None


def _strip_single_minus(expr: pl.Expr) -> pl.Expr:
    return pl.when(expr.str.starts_with("-")).then(expr.str.slice(1)).otherwise(expr)


def decode_db_ticks(chunk: pl.DataFrame) -> pl.DataFrame:
    """Decode DB S00 frames into the shared tick field schema."""
    body_dtype = pl.Struct(dict.fromkeys(_DB_TICK_BODY_FIELDS, pl.String))
    raw_dtype = pl.Struct({"header": pl.Struct({"tr_cd": pl.String, "tr_key": pl.String}), "body": body_dtype})
    try:
        decoded = chunk.with_columns(pl.col("raw").str.json_decode(raw_dtype).alias("decoded"))
        body = pl.col("decoded").struct.field("body")
        return decoded.with_columns(
            shcode=body.struct.field("ShrnIscd"),
            price_raw=body.struct.field("StckPrpr"),
            cvolume_raw=body.struct.field("CntgVol"),
            volume_raw=body.struct.field("AcmlVol"),
            change_raw=_strip_single_minus(body.struct.field("PrdyVrss")),
            sign=body.struct.field("PrdyVrsssign"),
            drate_raw=body.struct.field("PrdyCtrt"),
            mdchecnt_raw=body.struct.field("SelnCntgcsnu"),
            mschecnt_raw=body.struct.field("ShnuCntgcsnu"),
        ).select(
            "shcode", "price_raw", "cvolume_raw", "volume_raw", "change_raw", "sign", "drate_raw",
            "mdchecnt_raw", "mschecnt_raw", "recv_wall_ns",
        )
    except pl.exceptions.ComputeError:
        fallback: list[dict[str, object]] = []
        for raw, wall in zip(chunk["raw"].to_list(), chunk["recv_wall_ns"].to_list(), strict=True):
            row = _safe_parse_db_tick_body(raw)
            if row is None:
                row = dict.fromkeys(_DB_TICK_BODY_FIELDS, None)
            change_raw = None if row["PrdyVrss"] is None else str(row["PrdyVrss"]).removeprefix("-")
            fallback.append({
                "shcode": row["ShrnIscd"],
                "price_raw": row["StckPrpr"],
                "cvolume_raw": row["CntgVol"],
                "volume_raw": row["AcmlVol"],
                "change_raw": change_raw,
                "sign": row["PrdyVrsssign"],
                "drate_raw": row["PrdyCtrt"],
                "mdchecnt_raw": row["SelnCntgcsnu"],
                "mschecnt_raw": row["ShnuCntgcsnu"],
                "recv_wall_ns": wall,
            })
        return pl.DataFrame(
            fallback,
            schema={
                "shcode": pl.String, "price_raw": pl.String, "cvolume_raw": pl.String,
                "volume_raw": pl.String, "change_raw": pl.String, "sign": pl.String,
                "drate_raw": pl.String, "mdchecnt_raw": pl.String, "mschecnt_raw": pl.String,
                "recv_wall_ns": pl.Int64,
            },
            strict=False,
        )


def decode_db_quotes(chunk: pl.DataFrame) -> pl.DataFrame:
    """Decode DB S01 frames into the shared quote field schema."""
    body_dtype = pl.Struct(dict.fromkeys(_DB_QUOTE_BODY_FIELDS, pl.String))
    raw_dtype = pl.Struct({"header": pl.Struct({"tr_cd": pl.String, "tr_key": pl.String}), "body": body_dtype})
    num_fields = [f for f in _QUOTE_BODY_FIELDS if f != "shcode"]
    try:
        decoded = chunk.with_columns(pl.col("raw").str.json_decode(raw_dtype).alias("decoded"))
        body = pl.col("decoded").struct.field("body")
        complete = pl.lit(True)
        for source in _DB_QUOTE_BODY_FIELDS:
            complete = complete & body.struct.field(source).is_not_null()
        complete = complete.fill_null(False)
        selects: list[pl.Expr] = [pl.when(complete).then(body.struct.field("ShrnIscd")).otherwise(None).alias("shcode")]
        mapping: list[tuple[str, str]] = [
            ("hotime", "BsopHour"),
            ("totofferrem", "TotalAskprsqn"),
            ("totbidrem", "TotalBidprsqn"),
            *[(f"offerho{k}", f"Askp{k}") for k in range(1, 11)],
            *[(f"bidho{k}", f"Bidp{k}") for k in range(1, 11)],
            *[(f"offerrem{k}", f"AskpRsqn{k}") for k in range(1, 11)],
            *[(f"bidrem{k}", f"BidpRsqn{k}") for k in range(1, 11)],
        ]
        selects.extend(pl.when(complete).then(body.struct.field(src).cast(pl.Int32, strict=False)).otherwise(None).alias(dst) for dst, src in mapping)
        return decoded.select(selects)
    except pl.exceptions.ComputeError:
        fallback_rows: list[dict[str, object]] = []
        for raw in chunk["raw"].to_list():
            row = _safe_parse_db_quote_body(raw)
            if row is None:
                fallback_rows.append(dict.fromkeys(_QUOTE_BODY_FIELDS, None))
                continue
            mapped: dict[str, object] = {
                "shcode": row["ShrnIscd"],
                "hotime": row["BsopHour"],
                "totofferrem": row["TotalAskprsqn"],
                "totbidrem": row["TotalBidprsqn"],
            }
            for k in range(1, 11):
                mapped[f"offerho{k}"] = row[f"Askp{k}"]
                mapped[f"bidho{k}"] = row[f"Bidp{k}"]
            for k in range(1, 11):
                mapped[f"offerrem{k}"] = row[f"AskpRsqn{k}"]
                mapped[f"bidrem{k}"] = row[f"BidpRsqn{k}"]
            fallback_rows.append(mapped)
        raw_frame = pl.DataFrame(
            fallback_rows,
            schema={**{"shcode": pl.String}, **dict.fromkeys(num_fields, pl.String)},
            strict=False,
        )
        casts = [pl.col("shcode")]
        casts.extend(pl.col(c).cast(pl.Int32, strict=False).alias(c) for c in num_fields)
        return raw_frame.select(casts)
