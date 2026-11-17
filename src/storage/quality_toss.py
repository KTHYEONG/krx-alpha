"""Toss unified-stream frame decoding into the shared quality field schema.

Toss trade frames carry no cumulative or reference fields and its book has no total-remain field, so those
columns are NULL here; the DQ rules that need them are skipped per vendor (see ``quality.DQ_SKIPPED_RULES``).
"""

from __future__ import annotations

import datetime as dt
import json

import polars as pl

_TRADE_RE = r"^trade:kr:([0-9A-Z]{6})$"
_BOOK_RE = r"^orderbook:kr:([0-9A-Z]{6})$"
_KST = "Asia/Seoul"
_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S%.f%z"
_LEVELS = 10
_TICK_COLUMNS: tuple[str, ...] = (
    "shcode", "price_raw", "cvolume_raw", "volume_raw", "change_raw", "sign", "drate_raw",
    "mdchecnt_raw", "mschecnt_raw",
)
_QUOTE_COLUMNS: tuple[str, ...] = (
    "shcode",
    "hotime",
    "totofferrem",
    "totbidrem",
    *[f"offerho{k}" for k in range(1, _LEVELS + 1)],
    *[f"bidho{k}" for k in range(1, _LEVELS + 1)],
    *[f"offerrem{k}" for k in range(1, _LEVELS + 1)],
    *[f"bidrem{k}" for k in range(1, _LEVELS + 1)],
)
_LEVEL_DTYPE = pl.List(pl.Struct({"price": pl.String, "volume": pl.String}))
_TICK_RAW_DTYPE = pl.Struct(
    {"topic": pl.String, "data": pl.Struct({"price": pl.String, "volume": pl.String})}
)
_QUOTE_RAW_DTYPE = pl.Struct(
    {
        "topic": pl.String,
        "data": pl.Struct({"timestamp": pl.String, "asks": _LEVEL_DTYPE, "bids": _LEVEL_DTYPE}),
    }
)
_TICK_SCHEMA: dict[str, pl.DataType | type[pl.DataType]] = {
    **dict.fromkeys(_TICK_COLUMNS, pl.String),
    "recv_wall_ns": pl.Int64,
}


def _tick_frame(rows: list[dict[str, object]]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=_TICK_SCHEMA, strict=False)


def _hhmmss(stamp: object) -> int | None:
    if not isinstance(stamp, str):
        return None
    try:
        moment = dt.datetime.fromisoformat(stamp)
    except ValueError:
        return None
    if moment.tzinfo is None:
        return None
    local = moment.astimezone(dt.timezone(dt.timedelta(hours=9)))
    return local.hour * 10000 + local.minute * 100 + local.second


def _text(value: object) -> str | None:
    return None if value is None or isinstance(value, (dict, list)) else str(value)


def decode_toss_ticks(chunk: pl.DataFrame) -> pl.DataFrame:
    """Decode Toss ``trade:kr`` frames; per-print volume maps to ``cvolume_raw`` and every cumulative column is NULL."""
    try:
        decoded = chunk.with_columns(pl.col("raw").str.json_decode(_TICK_RAW_DTYPE).alias("decoded"))
        data = pl.col("decoded").struct.field("data")
        shcode = pl.col("decoded").struct.field("topic").str.extract(_TRADE_RE, 1)
        out = decoded.select(
            shcode=shcode,
            price_raw=data.struct.field("price"),
            cvolume_raw=data.struct.field("volume"),
            recv_wall_ns=pl.col("recv_wall_ns"),
        )
        return out.with_columns(
            pl.lit(None, dtype=pl.String).alias(name) for name in _TICK_COLUMNS if name not in out.columns
        ).select(*_TICK_COLUMNS, "recv_wall_ns")
    except pl.exceptions.ComputeError:
        rows: list[dict[str, object]] = []
        for raw, wall in zip(chunk["raw"].to_list(), chunk["recv_wall_ns"].to_list(), strict=True):
            row: dict[str, object] = dict.fromkeys(_TICK_COLUMNS)
            row["recv_wall_ns"] = wall
            try:
                obj = json.loads(raw)
                topic, data = obj["topic"], obj["data"]
                if isinstance(topic, str) and isinstance(data, dict):
                    parts = topic.split(":")
                    if len(parts) == 3 and parts[:2] == ["trade", "kr"]:
                        row["shcode"] = parts[2]
                    row["price_raw"] = _text(data.get("price"))
                    row["cvolume_raw"] = _text(data.get("volume"))
            except (ValueError, KeyError, TypeError, AttributeError):
                pass
            rows.append(row)
        return _tick_frame(rows)


def decode_toss_quotes(chunk: pl.DataFrame) -> pl.DataFrame:
    """Decode Toss ``orderbook:kr`` frames (top 10 levels); the total-remain columns are NULL."""
    try:
        decoded = chunk.with_columns(pl.col("raw").str.json_decode(_QUOTE_RAW_DTYPE).alias("decoded"))
        data = pl.col("decoded").struct.field("data")
        stamp = data.struct.field("timestamp").str.to_datetime(
            _TIMESTAMP_FORMAT, time_zone=_KST, strict=False
        )
        selects: list[pl.Expr] = [
            pl.col("decoded").struct.field("topic").str.extract(_BOOK_RE, 1).alias("shcode"),
            (
                stamp.dt.hour().cast(pl.Int32) * 10000
                + stamp.dt.minute().cast(pl.Int32) * 100
                + stamp.dt.second().cast(pl.Int32)
            ).alias("hotime"),
            pl.lit(None, dtype=pl.Int32).alias("totofferrem"),
            pl.lit(None, dtype=pl.Int32).alias("totbidrem"),
        ]
        for side, prefix in (("asks", "offer"), ("bids", "bid")):
            for field, label in (("price", "ho"), ("volume", "rem")):
                for k in range(1, _LEVELS + 1):
                    level = data.struct.field(side).list.get(k - 1, null_on_oob=True)
                    value = level.struct.field(field).cast(pl.Int32, strict=False)
                    selects.append(value.fill_null(0).alias(f"{prefix}{label}{k}"))
        out = decoded.select(selects)
        # Levels are zero-filled like the vendor's own empty rungs, but an unparsable frame must stay a decode failure.
        parsed = pl.col("shcode").is_not_null() & pl.col("hotime").is_not_null()
        return out.with_columns(
            pl.when(parsed).then(pl.col(name)).otherwise(None).alias(name) for name in _QUOTE_COLUMNS[4:]
        ).select(*_QUOTE_COLUMNS)
    except pl.exceptions.ComputeError:
        fallback: list[dict[str, object]] = []
        for raw in chunk["raw"].to_list():
            row: dict[str, object] = dict.fromkeys(_QUOTE_COLUMNS)
            try:
                obj = json.loads(raw)
                topic, data = obj["topic"], obj["data"]
                parts = topic.split(":") if isinstance(topic, str) else []
                hotime = _hhmmss(data.get("timestamp"))
                if len(parts) == 3 and parts[:2] == ["orderbook", "kr"] and hotime is not None:
                    row["shcode"] = parts[2]
                    row["hotime"] = hotime
                    for side, prefix in (("asks", "offer"), ("bids", "bid")):
                        levels = data.get(side)
                        levels = levels if isinstance(levels, list) else []
                        for k in range(1, _LEVELS + 1):
                            item = levels[k - 1] if k <= len(levels) and isinstance(levels[k - 1], dict) else {}
                            row[f"{prefix}ho{k}"] = _text(item.get("price")) or "0"
                            row[f"{prefix}rem{k}"] = _text(item.get("volume")) or "0"
            except (ValueError, KeyError, TypeError, AttributeError):
                row = dict.fromkeys(_QUOTE_COLUMNS)
            fallback.append(row)
        raw_frame = pl.DataFrame(
            fallback,
            schema={"shcode": pl.String, **dict.fromkeys(_QUOTE_COLUMNS[1:], pl.String)},
            strict=False,
        )
        return raw_frame.select(
            pl.col("shcode"), *(pl.col(name).cast(pl.Int32, strict=False) for name in _QUOTE_COLUMNS[1:])
        )
