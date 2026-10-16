import json

import polars as pl

from src.storage.quality_db import decode_db_quotes, decode_db_ticks
from src.storage.quality_ls import decode_ls_quotes, decode_ls_ticks


def _db_tick_body(**overrides: str) -> dict[str, str]:
    body = {
        "ShrnIscd": "005930",
        "StckPrpr": "70000",
        "CntgVol": "10",
        "AcmlVol": "100",
        "PrdyVrss": "500",
        "PrdyVrsssign": "2",
        "PrdyCtrt": "0.72",
        "SelnCntgcsnu": "7",
        "ShnuCntgcsnu": "3",
    }
    body.update(overrides)
    return body


def _db_tick_raw(wall: int = 100, **overrides: str) -> dict[str, object]:
    return {
        "raw": json.dumps({"header": {"tr_cd": "S00", "tr_key": "005930"}, "body": _db_tick_body(**overrides)}),
        "recv_wall_ns": wall,
    }


def _db_quote_body() -> dict[str, str]:
    body: dict[str, str] = {
        "ShrnIscd": "005930",
        "BsopHour": "101500",
        "TotalAskprsqn": "1000",
        "TotalBidprsqn": "1000",
    }
    for k in range(1, 11):
        body[f"Askp{k}"] = str(70100 + (k - 1) * 100)
        body[f"Bidp{k}"] = str(69900 - (k - 1) * 100)
        body[f"AskpRsqn{k}"] = "100"
        body[f"BidpRsqn{k}"] = "100"
    return body


def test_decode_db_ticks_maps_fields_to_shared_schema() -> None:
    chunk = pl.DataFrame([_db_tick_raw()])
    out = decode_db_ticks(chunk)

    body = _db_tick_body()
    assert out.columns == ["shcode", "price_raw", "cvolume_raw", "volume_raw", "change_raw", "sign", "drate_raw", "mdchecnt_raw", "mschecnt_raw", "recv_wall_ns"]
    assert out.row(0) == (
        body["ShrnIscd"], body["StckPrpr"], body["CntgVol"], body["AcmlVol"], body["PrdyVrss"],
        body["PrdyVrsssign"], body["PrdyCtrt"], body["SelnCntgcsnu"], body["ShnuCntgcsnu"], 100,
    )

    ls_chunk = pl.DataFrame([{
        "raw": json.dumps({"header": {"tr_cd": "S3_", "tr_key": "005930"}, "body": {
            "shcode": "005930", "price": "1", "cvolume": "1", "volume": "1", "change": "0",
            "sign": "3", "drate": "0", "mdchecnt": "1", "mschecnt": "1"}}),
        "recv_wall_ns": 100,
    }])
    assert list(out.schema.items()) == list(decode_ls_ticks(ls_chunk).schema.items())


def test_decode_db_ticks_strips_single_leading_minus_from_change() -> None:
    out = decode_db_ticks(pl.DataFrame([_db_tick_raw(PrdyVrss="-2500", PrdyVrsssign="5")]))

    assert out["change_raw"].to_list() == ["2500"]
    assert out["sign"].to_list() == ["5"]


def test_decode_db_ticks_malformed_rows_are_null_not_dropped() -> None:
    chunk = pl.DataFrame([_db_tick_raw(wall=100), {"raw": "not-json", "recv_wall_ns": 200}])
    out = decode_db_ticks(chunk)

    assert out.height == 2
    assert all(v is None for v in out.row(1)[:-1])


def test_decode_db_ticks_non_object_body_is_null() -> None:
    chunk = pl.DataFrame([
        _db_tick_raw(wall=100, PrdyVrss="-2500"),
        {"raw": json.dumps({"header": {"tr_cd": "S00", "tr_key": "005930"}, "body": "str"}), "recv_wall_ns": 200},
    ])
    out = decode_db_ticks(chunk)

    assert out.height == 2
    assert out["change_raw"].to_list()[0] == "2500"
    assert all(v is None for v in out.row(1)[:-1])


def test_decode_db_quotes_maps_ten_levels() -> None:
    raw = json.dumps({"header": {"tr_cd": "S01", "tr_key": "005930"}, "body": _db_quote_body()})
    out = decode_db_quotes(pl.DataFrame({"raw": [raw], "recv_wall_ns": [100]}))

    assert out["offerho1"].to_list() == [70100]
    assert out["offerho10"].to_list() == [71000]
    assert out["bidho1"].to_list() == [69900]
    assert out["bidrem10"].to_list() == [100]
    assert out.schema["offerho1"] == pl.Int32

    ls_body = {"shcode": "005930", "hotime": "101500", "totofferrem": "1000", "totbidrem": "1000"}
    for k in range(1, 11):
        ls_body[f"offerho{k}"] = "1"
        ls_body[f"bidho{k}"] = "1"
        ls_body[f"offerrem{k}"] = "1"
        ls_body[f"bidrem{k}"] = "1"
    ls_chunk = pl.DataFrame({"raw": [json.dumps({"header": {"tr_cd": "H1_", "tr_key": "005930"}, "body": ls_body})], "recv_wall_ns": [100]})
    assert list(out.schema.items()) == list(decode_ls_quotes(ls_chunk).schema.items())


def test_decode_db_quotes_missing_level_is_decode_failure() -> None:
    body = _db_quote_body()
    del body["Askp7"]
    raw = json.dumps({"header": {"tr_cd": "S01", "tr_key": "005930"}, "body": body})
    out = decode_db_quotes(pl.DataFrame({"raw": [raw], "recv_wall_ns": [100]}))

    assert out.height == 1
    assert all(v is None for v in out.row(0))


def test_decode_db_quotes_malformed_rows_are_null_not_dropped() -> None:
    valid = json.dumps({"header": {"tr_cd": "S01", "tr_key": "005930"}, "body": _db_quote_body()})
    missing = _db_quote_body()
    del missing["Askp7"]
    missing_raw = json.dumps({"header": {"tr_cd": "S01", "tr_key": "005930"}, "body": missing})
    non_object = json.dumps({"header": {"tr_cd": "S01", "tr_key": "005930"}, "body": "str"})
    chunk = pl.DataFrame({"raw": [valid, missing_raw, non_object, "not-json"], "recv_wall_ns": [100, 200, 300, 400]})
    out = decode_db_quotes(chunk)

    assert out.height == 4
    assert out["offerho1"].to_list()[0] == 70100
    for idx in (1, 2, 3):
        assert all(v is None for v in out.row(idx))


def test_decode_db_quotes_explicit_null_level_is_all_null_on_both_paths() -> None:
    nulled = _db_quote_body()
    nulled["Askp7"] = None  # type: ignore[assignment]
    nulled_raw = json.dumps({"header": {"tr_cd": "S01", "tr_key": "005930"}, "body": nulled})
    valid = json.dumps({"header": {"tr_cd": "S01", "tr_key": "005930"}, "body": _db_quote_body()})

    fast = decode_db_quotes(pl.DataFrame({"raw": [valid, nulled_raw], "recv_wall_ns": [1, 2]}))
    fallback = decode_db_quotes(pl.DataFrame({"raw": [valid, nulled_raw, "not-json"], "recv_wall_ns": [1, 2, 3]}))

    assert all(v is None for v in fast.row(1))
    assert all(v is None for v in fallback.row(1))
    assert fast.row(0) == fallback.row(0)


def test_decode_db_ticks_numeric_change_is_stripped_on_both_paths() -> None:
    body = {**_db_tick_body(), "PrdyVrss": -500, "StckPrpr": 70000}
    raw = json.dumps({"header": {"tr_cd": "S00", "tr_key": "005930"}, "body": body})

    fast = decode_db_ticks(pl.DataFrame({"raw": [raw], "recv_wall_ns": [1]}))
    fallback = decode_db_ticks(pl.DataFrame({"raw": [raw, "not-json"], "recv_wall_ns": [1, 2]}))

    assert fast["change_raw"].to_list() == ["500"]
    assert fallback.row(0) == fast.row(0)
