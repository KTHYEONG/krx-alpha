"""LS vendor frame decoding into the shared quality field schema."""

from __future__ import annotations

import json

import polars as pl

_TICK_BODY_FIELDS: tuple[str, ...] = ("shcode", "price", "cvolume", "volume", "change", "sign", "drate", "mdchecnt", "mschecnt")
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


def _safe_parse_ls_body(raw: str) -> dict[str, object] | None:
    try:
        body = json.loads(raw)["body"]
        return {field: body.get(field) for field in _TICK_BODY_FIELDS}
    except (json.JSONDecodeError, KeyError, TypeError, AttributeError):
        return None


def _safe_parse_ls_quote_body(raw: str) -> dict[str, object] | None:
    try:
        body = json.loads(raw)["body"]
        return {field: body[field] for field in _QUOTE_BODY_FIELDS}
    except (json.JSONDecodeError, KeyError, TypeError, AttributeError):
        return None


def decode_ls_ticks(chunk: pl.DataFrame) -> pl.DataFrame:
    """Decode one bounded LS frame into the shared quality field schema.

    Malformed vendor records retain the current decode-failure representation so
    the common verdict can count them without silently dropping rows.
    """
    body_dtype = pl.Struct(dict.fromkeys(_TICK_BODY_FIELDS, pl.String))
    raw_dtype = pl.Struct({"header": pl.Struct({"tr_cd": pl.String, "tr_key": pl.String}), "body": body_dtype})
    try:
        decoded = chunk.with_columns(pl.col("raw").str.json_decode(raw_dtype).alias("decoded"))
        raw_fields = decoded.with_columns(
            shcode=pl.col("decoded").struct.field("body").struct.field("shcode"),
            price_raw=pl.col("decoded").struct.field("body").struct.field("price"),
            cvolume_raw=pl.col("decoded").struct.field("body").struct.field("cvolume"),
            volume_raw=pl.col("decoded").struct.field("body").struct.field("volume"),
            change_raw=pl.col("decoded").struct.field("body").struct.field("change"),
            sign=pl.col("decoded").struct.field("body").struct.field("sign"),
            drate_raw=pl.col("decoded").struct.field("body").struct.field("drate"),
            mdchecnt_raw=pl.col("decoded").struct.field("body").struct.field("mdchecnt"),
            mschecnt_raw=pl.col("decoded").struct.field("body").struct.field("mschecnt"),
        ).select(
            "shcode", "price_raw", "cvolume_raw", "volume_raw", "change_raw", "sign", "drate_raw",
            "mdchecnt_raw", "mschecnt_raw", "recv_wall_ns",
        )
    except pl.exceptions.ComputeError:
        fallback: list[dict[str, object]] = []
        for raw, wall in zip(chunk["raw"].to_list(), chunk["recv_wall_ns"].to_list(), strict=True):
            row = _safe_parse_ls_body(raw)
            if row is None:
                row = {
                    "shcode": None, "price": None, "cvolume": None, "volume": None, "change": None,
                    "sign": None, "drate": None, "mdchecnt": None, "mschecnt": None,
                }
            fallback.append({
                "shcode": row["shcode"],
                "price_raw": row["price"],
                "cvolume_raw": row["cvolume"],
                "volume_raw": row["volume"],
                "change_raw": row["change"],
                "sign": row["sign"],
                "drate_raw": row["drate"],
                "mdchecnt_raw": row["mdchecnt"],
                "mschecnt_raw": row["mschecnt"],
                "recv_wall_ns": wall,
            })
        raw_fields = pl.DataFrame(
            fallback,
            schema={
                "shcode": pl.String, "price_raw": pl.String, "cvolume_raw": pl.String,
                "volume_raw": pl.String, "change_raw": pl.String, "sign": pl.String,
                "drate_raw": pl.String, "mdchecnt_raw": pl.String, "mschecnt_raw": pl.String,
                "recv_wall_ns": pl.Int64,
            },
            strict=False,
        )
    return raw_fields


def decode_ls_quotes(chunk: pl.DataFrame) -> pl.DataFrame:
    """Decode one bounded LS frame into the shared quality field schema.

    Malformed vendor records retain the current decode-failure representation so
    the common verdict can count them without silently dropping rows.
    """
    body_dtype = pl.Struct(dict.fromkeys(_QUOTE_BODY_FIELDS, pl.String))
    raw_dtype = pl.Struct({"header": pl.Struct({"tr_cd": pl.String, "tr_key": pl.String}), "body": body_dtype})
    num_fields = [f for f in _QUOTE_BODY_FIELDS if f != "shcode"]
    try:
        decoded = chunk.with_columns(pl.col("raw").str.json_decode(raw_dtype).alias("decoded"))
        selects = [pl.col("decoded").struct.field("body").struct.field("shcode").alias("shcode")]
        selects.extend(
            pl.col("decoded").struct.field("body").struct.field(c).cast(pl.Int32, strict=False).alias(c) for c in num_fields
        )
        return decoded.select(selects)
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
        return raw_frame.select(casts)
