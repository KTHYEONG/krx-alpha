"""Invariant tests for KIS historical minute bars (FHKST03010230, KRX-only)."""

from __future__ import annotations

import datetime as dt

import pytest

from src.brokers.kis.data import (
    TR_HISTORICAL_MINUTE_CHART,
    KisDataClient,
    _PATH_HISTORICAL_MINUTE_CHART,
)
from src.execution.contracts import KisApiError

_DAY = dt.date(2026, 10, 20)


class _Transport:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows
        self.paths: list[str] = []
        self.trs: list[str] = []
        self.params: list[dict] = []

    def get(self, path: str, tr_id: str, params: dict[str, str], tr_cont: str = ""):
        self.paths.append(path)
        self.trs.append(tr_id)
        self.params.append(dict(params))
        return {"rt_cd": "0", "output2": self._rows}, ""


def _row(date: str, hour: str, **overrides) -> dict:
    base = {
        "stck_bsop_date": date,
        "stck_cntg_hour": hour,
        "stck_oprc": "100",
        "stck_hgpr": "101",
        "stck_lwpr": "99",
        "stck_prpr": "100",
        "cntg_vol": "10",
    }
    base.update(overrides)
    return base


def test_krx_only_request_params() -> None:
    transport = _Transport([_row("20261020", "153000")])
    client = KisDataClient(transport=transport)  # type: ignore[arg-type]
    bars = client.get_historical_minute_bars("005930", day=_DAY, before_hms=153000)
    assert transport.trs == [TR_HISTORICAL_MINUTE_CHART]
    assert transport.paths == [_PATH_HISTORICAL_MINUTE_CHART]
    params = transport.params[0]
    assert params["FID_COND_MRKT_DIV_CODE"] == "J"
    assert params["FID_FAKE_TICK_INCU_YN"] == ""
    assert params["FID_INPUT_ISCD"] == "005930"
    assert params["FID_INPUT_DATE_1"] == "20261020"
    assert params["FID_INPUT_HOUR_1"] == "153000"
    assert params["FID_PW_DATA_INCU_YN"] == "Y"
    assert len(bars) == 1
    assert bars[0].ts_hms == 153000


def test_cross_date_padding_dropped() -> None:
    transport = _Transport([
        _row("20261020", "090100"),
        _row("20261020", "090000"),
        _row("20261019", "153000"),
        _row("20261019", "152900"),
    ])
    client = KisDataClient(transport=transport)  # type: ignore[arg-type]
    bars = client.get_historical_minute_bars("005930", day=_DAY, before_hms=153000)
    assert [bar.ts_hms for bar in bars] == [90100, 90000]
    assert all(bar.day == _DAY for bar in bars)


def test_malformed_numeric_raises() -> None:
    transport = _Transport([_row("20261020", "153000", cntg_vol="-3")])
    client = KisDataClient(transport=transport)  # type: ignore[arg-type]
    with pytest.raises(KisApiError):
        client.get_historical_minute_bars("005930", day=_DAY, before_hms=153000)


@pytest.mark.parametrize("value", ["abc", "NaN", "Infinity", "1.5", "", None, True])
def test_non_integer_numeric_rejected(value) -> None:
    transport = _Transport([_row("20261020", "153000", cntg_vol=value)])
    with pytest.raises(KisApiError):
        KisDataClient(transport=transport).get_historical_minute_bars("005930", day=_DAY, before_hms=153000)  # type: ignore[arg-type]


@pytest.mark.parametrize("hour", ["bad", "256000", "126099"])
def test_invalid_wall_time_rejected(hour) -> None:
    transport = _Transport([_row("20261020", hour)])
    with pytest.raises(KisApiError):
        KisDataClient(transport=transport).get_historical_minute_bars("005930", day=_DAY, before_hms=153000)  # type: ignore[arg-type]


def test_missing_output2_is_not_vendor_empty() -> None:
    class Transport:
        def get(self, *args):
            return {"rt_cd": "0"}, ""

    with pytest.raises(KisApiError, match="output2"):
        KisDataClient(transport=Transport()).get_historical_minute_bars("005930", day=_DAY, before_hms=153000)  # type: ignore[arg-type]


def test_invalid_row_is_schema_error() -> None:
    transport = _Transport([None])
    with pytest.raises(KisApiError, match="row"):
        KisDataClient(transport=transport).get_historical_minute_bars("005930", day=_DAY, before_hms=153000)  # type: ignore[arg-type]


def test_bars_returned_newest_first() -> None:
    transport = _Transport([_row("20261020", "090000"), _row("20261020", "090100")])
    bars = KisDataClient(transport=transport).get_historical_minute_bars("005930", day=_DAY, before_hms=153000)  # type: ignore[arg-type]
    assert [bar.ts_hms for bar in bars] == [90100, 90000]
