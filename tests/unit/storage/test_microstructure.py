import json

import polars as pl
import pytest

from src.storage.microstructure import (
    KIS_QUOTE_TOKENS,
    KIS_TRADE_TOKENS,
    MICROSTRUCTURE_COLUMNS,
    annotate_microstructure,
)


def _kis_tokens(count: int, **fields: str) -> str:
    tokens = [""] * count
    for index, value in fields.items():
        tokens[int(index.removeprefix("i"))] = value
    return "^".join(tokens)


def _kis_trade(*, ccld: str = "", mkop: str = "", halted: str = "N", vi_price: str = "") -> str:
    return _kis_tokens(
        KIS_TRADE_TOKENS, i21=ccld, i34=mkop, i35=halted, i45=vi_price
    )


def _kis_quote(*, hour_cls: str = "0", cnpr: str = "", cnqn: str = "") -> str:
    return _kis_tokens(KIS_QUOTE_TOKENS, i2=hour_cls, i47=cnpr, i48=cnqn)


def _ls_body(stream: str, **body: str) -> str:
    return json.dumps({"header": {"tr_cd": stream}, "body": body})


def _rows(entries: list[tuple[str, str, str]]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "vendor": [vendor for vendor, _, _ in entries],
            "stream": [stream for _, stream, _ in entries],
            "raw": [raw for _, _, raw in entries],
        }
    )


def test_kis_trade_buy_sell_initiator() -> None:
    frame = _rows(
        [
            ("kis", "H0STCNT0", _kis_trade(ccld="1")),
            ("kis", "H0STCNT0", _kis_trade(ccld="5")),
            ("kis", "H0STCNT0", _kis_trade(ccld="3")),
        ]
    )

    out = annotate_microstructure(frame)

    assert out["initiator"].to_list() == ["buy", "sell", None]


def test_kis_trade_halt_vi_base() -> None:
    frame = _rows(
        [
            ("kis", "H0STCNT0", _kis_trade(halted="Y", mkop="20", vi_price="67900")),
            ("kis", "H0STCNT0", _kis_trade(vi_price="0")),
        ]
    )

    out = annotate_microstructure(frame)

    assert out["trading_halted"].to_list() == [True, False]
    assert out["mkop_code"].to_list() == ["20", None]
    assert out["vi_base_price"].to_list() == [67900, None]


def test_kis_quote_vi_and_expected() -> None:
    frame = _rows(
        [
            ("kis", "H0STASP0", _kis_quote(hour_cls="C", cnpr="68800", cnqn="21475")),
            ("kis", "H0STASP0", _kis_quote(hour_cls="0", cnpr="0")),
        ]
    )

    out = annotate_microstructure(frame)

    assert out["vi_active"].to_list() == [True, False]
    assert out["single_price"].to_list() == [True, False]
    assert out["expected_price"].to_list() == [68800, None]
    assert out["expected_qty"].to_list() == [21475, None]


@pytest.mark.parametrize("hour_cls", ["A", "B", "D"])
def test_kis_quote_auction_codes(hour_cls: str) -> None:
    frame = _rows([("kis", "H0STASP0", _kis_quote(hour_cls=hour_cls))])

    out = annotate_microstructure(frame)

    assert out["single_price"].to_list() == [True]
    assert out["vi_active"].to_list() == [False]


def test_kis_wrong_token_count_fails_closed() -> None:
    short_trade = "^".join(["x"] * (KIS_TRADE_TOKENS - 2))
    long_quote = "^".join(["0"] * (KIS_QUOTE_TOKENS + 1))
    frame = _rows(
        [
            ("kis", "H0STCNT0", _kis_trade(ccld="1")),
            ("kis", "H0STCNT0", short_trade),
            ("kis", "H0STASP0", _kis_quote(hour_cls="C", cnpr="68800")),
            ("kis", "H0STASP0", long_quote),
        ]
    )

    out = annotate_microstructure(frame)

    assert out["initiator"].to_list() == ["buy", None, None, None]
    assert out["vi_active"].to_list() == [None, None, True, None]
    assert out["expected_price"].to_list() == [None, None, 68800, None]


def test_ls_trade_cgubun() -> None:
    frame = _rows(
        [
            ("ls", "H0STCNT0", _ls_body("H0STCNT0", cgubun="+")),
            ("ls", "H0STCNT0", _ls_body("H0STCNT0", cgubun="-")),
            ("ls", "H0STCNT0", _ls_body("H0STCNT0", cgubun="")),
        ]
    )

    out = annotate_microstructure(frame)

    assert out["initiator"].to_list() == ["buy", "sell", None]
    assert out["vi_active"].to_list() == [None, None, None]


def test_ls_quote_donsigubun() -> None:
    frame = _rows(
        [
            ("ls", "H0STASP0", _ls_body("H0STASP0", donsigubun="3")),
            ("ls", "H0STASP0", _ls_body("H0STASP0", donsigubun="1")),
        ]
    )

    out = annotate_microstructure(frame)

    assert out["single_price"].to_list() == [True, False]
    assert out["vi_active"].to_list() == [None, None]


def test_unsupported_vendor_stream_all_null() -> None:
    frame = _rows(
        [
            ("toss", "H0STCNT0", _kis_trade(ccld="1", halted="Y", vi_price="67900")),
            ("kis", "H0NXCNT0", _kis_trade(ccld="1", halted="Y", vi_price="67900")),
            ("db", "H0STASP0", _kis_quote(hour_cls="C", cnpr="68800", cnqn="21475")),
        ]
    )

    out = annotate_microstructure(frame)

    for column in MICROSTRUCTURE_COLUMNS:
        assert out[column].null_count() == out.height, column


def test_preserves_rows_and_columns() -> None:
    frame = pl.DataFrame(
        {
            "symbol": ["005930", "000660", "035420"],
            "raw": [_kis_trade(ccld="1"), _kis_quote(hour_cls="C"), "not-json"],
            "vendor": ["kis", "kis", "ls"],
            "stream": ["H0STCNT0", "H0STASP0", "H0STCNT0"],
        }
    )

    out = annotate_microstructure(frame)

    assert out.height == frame.height
    assert out.columns == frame.columns + list(MICROSTRUCTURE_COLUMNS)
    assert out.select(frame.columns).equals(frame)


def test_lazy_and_eager_agree() -> None:
    frame = _rows(
        [
            ("kis", "H0STCNT0", _kis_trade(ccld="1", halted="Y", vi_price="67900")),
            ("kis", "H0STASP0", _kis_quote(hour_cls="C", cnpr="68800", cnqn="21475")),
            ("ls", "H0STCNT0", _ls_body("H0STCNT0", cgubun="-")),
            ("ls", "H0STASP0", _ls_body("H0STASP0", donsigubun="3")),
        ]
    )

    eager = annotate_microstructure(frame)
    lazy = annotate_microstructure(frame.lazy()).collect()

    assert eager.equals(lazy)


def test_missing_column_raises() -> None:
    frame = pl.DataFrame({"vendor": ["kis"], "raw": [_kis_trade(ccld="1")]})

    with pytest.raises(ValueError, match="stream"):
        annotate_microstructure(frame)


def test_existing_output_column_raises() -> None:
    frame = _rows([("kis", "H0STCNT0", _kis_trade(ccld="1"))]).with_columns(pl.lit("x").alias("initiator"))

    with pytest.raises(ValueError, match="initiator"):
        annotate_microstructure(frame)
