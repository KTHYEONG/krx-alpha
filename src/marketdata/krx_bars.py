"""KRX Open API 기반 일별매매정보 수집 (bars store + market-map)."""

from __future__ import annotations

import datetime as dt
import logging
import pathlib
from typing import Any, cast

import polars as pl
import requests
from tenacity import RetryCallState, retry, retry_if_exception_type, stop_after_attempt

from src.marketdata.bar_store import (
    MIN_ROWCOUNT_RATIO,
    ImplausibleRowCountError,
    append_daily_bars,
    write_market_map,
)
from src.marketdata.bar_validation import (
    MAX_CHANGE_PCT_DISAGREEMENT_PP,
    ImplausibleBarValuesError,
    KrxBarsError,
    validate_daily_bars,
)
from src.marketdata.schema import BAR_SCHEMA

KOSPI_URL = "https://data-dbg.krx.co.kr/svc/apis/sto/stk_bydd_trd"
KOSDAQ_URL = "https://data-dbg.krx.co.kr/svc/apis/sto/ksq_bydd_trd"
KOSPI_BASE_INFO_URL = "https://data-dbg.krx.co.kr/svc/apis/sto/stk_isu_base_info"
KOSDAQ_BASE_INFO_URL = "https://data-dbg.krx.co.kr/svc/apis/sto/ksq_isu_base_info"

logger = logging.getLogger(__name__)


__all__ = [
    "MAX_CHANGE_PCT_DISAGREEMENT_PP",
    "MIN_ROWCOUNT_RATIO",
    "ImplausibleBarValuesError",
    "ImplausibleRowCountError",
    "IncompleteMarketError",
    "KrxBarsError",
    "KrxNoTradingDataError",
    "KrxTransportError",
    "append_daily_bars",
    "validate_daily_bars",
    "write_market_map",
]


RETRY_WAIT_BASE_S: float = 1.0
RETRY_WAIT_MAX_S: float = 4.0


class KrxTransportError(KrxBarsError):
    """KRX 전송 계층 실패 (재시도 대상, 휴장 아님)."""


class KrxNoTradingDataError(KrxBarsError):
    """양 시장 모두 빈 응답 (비거래일)."""


class IncompleteMarketError(KrxBarsError):
    """KOSPI/KOSDAQ 중 일부 시장만 응답한 부분 수집 fail-closed 신호."""


def retry_wait_seconds(retry_state: RetryCallState) -> float:
    return float(min(RETRY_WAIT_BASE_S * 2 ** (retry_state.attempt_number - 1), RETRY_WAIT_MAX_S))


@retry(
    stop=stop_after_attempt(3),
    wait=retry_wait_seconds,
    retry=retry_if_exception_type(requests.RequestException),
    reraise=True,
)
def _post_krx(session: Any, url: str, auth_key: str, date: dt.date) -> dict[str, Any]:
    resp = session.post(
        url,
        headers={"AUTH_KEY": auth_key, "Content-Type": "application/json", "Accept": "application/json"},
        json={"basDd": date.strftime("%Y%m%d")},
        timeout=15,
    )
    resp.raise_for_status()
    return cast("dict[str, Any]", resp.json())


def fetch_daily_bars(date: dt.date, *, auth_key: str, session: Any | None = None) -> pl.DataFrame:
    """Fetch KOSPI/KOSDAQ daily bars for one date with security classification.

    Trading data is authoritative and fail-closed; the listing base info that
    supplies share class and security group is auxiliary, so its failure only
    leaves the class columns null.

    Raises:
        KrxTransportError: Trading data transport failure after retries.
        KrxNoTradingDataError: Both markets returned no trading rows.
        IncompleteMarketError: Exactly one market returned no trading rows.
    """
    sess = session or requests
    try:
        kospi = _post_krx(sess, KOSPI_URL, auth_key, date)
        kosdaq = _post_krx(sess, KOSDAQ_URL, auth_key, date)
    except requests.RequestException as exc:
        raise KrxTransportError(f"krx request failed for {date}: {exc}") from exc
    kospi_rows = kospi.get("OutBlock_1", [])
    kosdaq_rows = kosdaq.get("OutBlock_1", [])
    if not kospi_rows and not kosdaq_rows:
        raise KrxNoTradingDataError(f"no trading data for {date}")
    if not kospi_rows or not kosdaq_rows:
        empty = "KOSPI" if not kospi_rows else "KOSDAQ"
        raise IncompleteMarketError(f"partial market data for {date}: empty {empty}")
    kospi_classes = _fetch_base_classes(sess, KOSPI_BASE_INFO_URL, auth_key, date, "KOSPI")
    kosdaq_classes = _fetch_base_classes(sess, KOSDAQ_BASE_INFO_URL, auth_key, date, "KOSDAQ")
    rows: list[dict[str, object]] = [
        _map_trading_row(date, row, market, classes)
        for payload, market, classes in (
            (kospi, "KOSPI", kospi_classes),
            (kosdaq, "KOSDAQ", kosdaq_classes),
        )
        for row in payload.get("OutBlock_1", [])
    ]
    return pl.DataFrame(rows, schema=BAR_SCHEMA)


def _opt_text(row: dict[str, Any], key: str) -> str | None:
    if key not in row:
        return None
    text = str(row[key]).strip()
    return text or None


def _opt_float(row: dict[str, Any], key: str) -> float | None:
    text = _opt_text(row, key)
    return float(text) if text is not None else None


def _opt_int(row: dict[str, Any], key: str) -> int | None:
    text = _opt_text(row, key)
    return int(text) if text is not None else None


def _map_trading_row(
    date: dt.date, row: dict[str, Any], market: str, classes: dict[str, dict[str, str | None]]
) -> dict[str, object]:
    volume = int(row["ACC_TRDVOL"])
    close = float(row["TDD_CLSPRC"])
    diff_text = _opt_text(row, "CMPPREVDD_PRC")
    class_info = classes.get(str(row["ISU_CD"]), {})
    return {
        "date": date,
        "symbol": str(row["ISU_CD"]),
        "close": close,
        "volume": volume,
        "trade_value_100m": float(row["ACC_TRDVAL"]) / 1e8,
        "daily_change_pct": float(row["FLUC_RT"]),
        "market": market,
        "open": None if volume == 0 else _opt_float(row, "TDD_OPNPRC"),
        "high": None if volume == 0 else _opt_float(row, "TDD_HGPRC"),
        "low": None if volume == 0 else _opt_float(row, "TDD_LWPRC"),
        "base_price": None if diff_text is None else close - float(diff_text),
        "market_cap_krw": _opt_int(row, "MKTCAP"),
        "listed_shares": _opt_int(row, "LIST_SHRS"),
        "section": _opt_text(row, "SECT_TP_NM"),
        "stock_cert_kind": class_info.get("stock_cert_kind"),
        "security_group": class_info.get("security_group"),
    }


def _fetch_base_classes(
    sess: Any, url: str, auth_key: str, date: dt.date, market: str
) -> dict[str, dict[str, str | None]]:
    try:
        body = _post_krx(sess, url, auth_key, date)
    except requests.RequestException as exc:
        logger.warning("[DATA] stage=krx_base_info status=DEGRADED market=%s reason=%s", market, str(exc))
        return {}
    rows = body.get("OutBlock_1", [])
    if not rows or all("ISU_SRT_CD" not in row for row in rows):
        reason = "empty OutBlock_1" if not rows else "missing ISU_SRT_CD"
        logger.warning("[DATA] stage=krx_base_info status=DEGRADED market=%s reason=%s", market, reason)
        return {}
    return {
        str(row["ISU_SRT_CD"]): {
            "stock_cert_kind": _opt_text(row, "KIND_STKCERT_TP_NM"),
            "security_group": _opt_text(row, "SECUGRP_NM"),
        }
        for row in rows
        if "ISU_SRT_CD" in row
    }


def latest_trading_day(
    ref_date: dt.date, *, auth_key: str, max_lookback: int = 10, session: Any | None = None
) -> tuple[dt.date, pl.DataFrame]:
    cursor = ref_date - dt.timedelta(days=1)
    for _ in range(max_lookback):
        try:
            bars = fetch_daily_bars(cursor, auth_key=auth_key, session=session)
        except KrxNoTradingDataError:
            cursor -= dt.timedelta(days=1)
            continue
        return cursor, bars
    raise KrxBarsError(f"no trading day found within {max_lookback} days before {ref_date}")


def derive_market_map(bars: pl.DataFrame) -> dict[str, str]:
    return dict(zip(bars["symbol"].to_list(), bars["market"].to_list(), strict=True))


def backfill_bars(
    store_path: pathlib.Path, *, auth_key: str, end_date: dt.date, window_days: int = 90, session: Any | None = None
) -> dict[str, int]:
    calendar_cap = window_days * 3
    cursor = end_date
    scanned = 0
    trading_days_found = 0
    appended_total = 0
    while trading_days_found < window_days and scanned < calendar_cap:
        try:
            bars = fetch_daily_bars(cursor, auth_key=auth_key, session=session)
        except KrxNoTradingDataError:
            cursor -= dt.timedelta(days=1)
            scanned += 1
            continue
        appended_total += append_daily_bars(store_path, bars)
        trading_days_found += 1
        cursor -= dt.timedelta(days=1)
        scanned += 1
    if trading_days_found < window_days:
        raise KrxBarsError(f"only found {trading_days_found}/{window_days} trading days within {calendar_cap} calendar days")
    return {"trading_days": trading_days_found, "appended_rows": appended_total}
