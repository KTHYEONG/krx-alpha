"""KIS vendor frame decoding into the shared quality field schema."""

from __future__ import annotations

import polars as pl

_QUOTE_LEVELS: int = 10
_KIS_TICK_MIN_FIELDS: int = 17
_KIS_TICK_SYMBOL: int = 0
_KIS_TICK_PRICE: int = 2
_KIS_TICK_SIGN: int = 3
_KIS_TICK_CHANGE: int = 4
_KIS_TICK_DRATE: int = 5
_KIS_TICK_CVOLUME: int = 12
_KIS_TICK_VOLUME: int = 13
_KIS_TICK_MDCHECNT: int = 15
_KIS_TICK_MSCHECNT: int = 16
_KIS_QUOTE_MIN_FIELDS: int = 45
_KIS_QUOTE_SYMBOL: int = 0
_KIS_QUOTE_HOTIME: int = 1
_KIS_QUOTE_ASK_BASE: int = 3
_KIS_QUOTE_BID_BASE: int = 13
_KIS_QUOTE_ASKREM_BASE: int = 23
_KIS_QUOTE_BIDREM_BASE: int = 33
_KIS_QUOTE_TOT_ASK: int = 43
_KIS_QUOTE_TOT_BID: int = 44
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


def decode_kis_ticks(chunk: pl.DataFrame) -> pl.DataFrame:
    """Decode one bounded KIS frame into the shared quality field schema.

    Malformed caret fields retain the current decode-failure representation so the
    common verdict can distinguish bad vendor data from valid zero values.
    """
    rows: list[dict[str, object]] = []
    for raw, wall in zip(chunk["raw"].to_list(), chunk["recv_wall_ns"].to_list(), strict=True):
        parts = raw.split("^") if isinstance(raw, str) else []
        if len(parts) < _KIS_TICK_MIN_FIELDS:
            rows.append({
                "shcode": None, "price_raw": None, "cvolume_raw": None, "volume_raw": None,
                "change_raw": None, "sign": None, "drate_raw": None,
                "mdchecnt_raw": None, "mschecnt_raw": None, "recv_wall_ns": wall,
            })
            continue
        change_raw = parts[_KIS_TICK_CHANGE]
        if isinstance(change_raw, str) and change_raw.startswith("-"):
            change_raw = change_raw[1:]
        rows.append({
            "shcode": parts[_KIS_TICK_SYMBOL],
            "price_raw": parts[_KIS_TICK_PRICE],
            "cvolume_raw": parts[_KIS_TICK_CVOLUME],
            "volume_raw": parts[_KIS_TICK_VOLUME],
            "change_raw": change_raw,
            "sign": parts[_KIS_TICK_SIGN],
            "drate_raw": parts[_KIS_TICK_DRATE],
            "mdchecnt_raw": parts[_KIS_TICK_MDCHECNT],
            "mschecnt_raw": parts[_KIS_TICK_MSCHECNT],
            "recv_wall_ns": wall,
        })
    return pl.DataFrame(
        rows,
        schema={
            "shcode": pl.String, "price_raw": pl.String, "cvolume_raw": pl.String,
            "volume_raw": pl.String, "change_raw": pl.String, "sign": pl.String,
            "drate_raw": pl.String, "mdchecnt_raw": pl.String, "mschecnt_raw": pl.String,
            "recv_wall_ns": pl.Int64,
        },
        strict=False,
    )


def decode_kis_quotes(chunk: pl.DataFrame) -> pl.DataFrame:
    """Decode one bounded KIS frame into the shared quality field schema.

    Malformed caret fields retain the current decode-failure representation so the
    common verdict can distinguish bad vendor data from valid zero values.
    """
    data: dict[str, list[object]] = {"shcode": [], "hotime": [], "totofferrem": [], "totbidrem": []}
    for k in range(1, _QUOTE_LEVELS + 1):
        data[f"offerho{k}"] = []
        data[f"bidho{k}"] = []
    for k in range(1, _QUOTE_LEVELS + 1):
        data[f"offerrem{k}"] = []
        data[f"bidrem{k}"] = []
    for raw in chunk["raw"].to_list():
        parts = raw.split("^") if isinstance(raw, str) else []
        if len(parts) < _KIS_QUOTE_MIN_FIELDS:
            for key in data:
                data[key].append(None)
            continue
        data["shcode"].append(parts[_KIS_QUOTE_SYMBOL])
        data["hotime"].append(parts[_KIS_QUOTE_HOTIME])
        for k in range(1, _QUOTE_LEVELS + 1):
            data[f"offerho{k}"].append(parts[_KIS_QUOTE_ASK_BASE + k - 1])
            data[f"bidho{k}"].append(parts[_KIS_QUOTE_BID_BASE + k - 1])
        for k in range(1, _QUOTE_LEVELS + 1):
            data[f"offerrem{k}"].append(parts[_KIS_QUOTE_ASKREM_BASE + k - 1])
            data[f"bidrem{k}"].append(parts[_KIS_QUOTE_BIDREM_BASE + k - 1])
        data["totofferrem"].append(parts[_KIS_QUOTE_TOT_ASK])
        data["totbidrem"].append(parts[_KIS_QUOTE_TOT_BID])
    num_quote_fields = [f for f in _QUOTE_BODY_FIELDS if f != "shcode"]
    raw_frame = pl.DataFrame(
        data,
        schema={**{"shcode": pl.String}, **dict.fromkeys(num_quote_fields, pl.String)},
        strict=False,
    )
    casts = [pl.col("shcode")]
    casts.extend(pl.col(c).cast(pl.Int32, strict=False).alias(c) for c in num_quote_fields)
    return raw_frame.select(casts)
