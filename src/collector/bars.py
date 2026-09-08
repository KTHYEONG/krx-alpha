"""KRX Open API 기반 일별매매정보 수집 (bars store + market-map)."""

from __future__ import annotations

import datetime as dt
import json
import os
import pathlib
from typing import Any, cast

import polars as pl
import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from src.universe.policy import REQUIRED_BAR_COLUMNS

KOSPI_URL = "https://data-dbg.krx.co.kr/svc/apis/sto/stk_bydd_trd"
KOSDAQ_URL = "https://data-dbg.krx.co.kr/svc/apis/sto/ksq_bydd_trd"


class KrxBarsError(RuntimeError):
    """KRX 요청 실패/빈 응답(비영업일)/거래일 미발견 fail-closed 신호."""


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=0.01, max=0.1),
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
    sess = session or requests
    try:
        kospi = _post_krx(sess, KOSPI_URL, auth_key, date)
        kosdaq = _post_krx(sess, KOSDAQ_URL, auth_key, date)
    except requests.RequestException as exc:
        raise KrxBarsError(f"krx request failed for {date}: {exc}") from exc
    rows: list[dict[str, object]] = []
    for payload, market in ((kospi, "KOSPI"), (kosdaq, "KOSDAQ")):
        rows.extend(
            {
                "date": date,
                "symbol": str(row["ISU_CD"]),
                "close": float(row["TDD_CLSPRC"]),
                "volume": int(row["ACC_TRDVOL"]),
                "trade_value_100m": float(row["ACC_TRDVAL"]) / 1e8,
                "daily_change_pct": float(row["FLUC_RT"]),
                "market": market,
            }
            for row in payload.get("OutBlock_1", [])
        )
    if not rows:
        raise KrxBarsError(f"no trading data for {date}")
    return pl.DataFrame(
        rows,
        schema={
            "date": pl.Date,
            "symbol": pl.String,
            "close": pl.Float64,
            "volume": pl.Int64,
            "trade_value_100m": pl.Float64,
            "daily_change_pct": pl.Float64,
            "market": pl.String,
        },
    )


def latest_trading_day(
    ref_date: dt.date, *, auth_key: str, max_lookback: int = 10, session: Any | None = None
) -> tuple[dt.date, pl.DataFrame]:
    cursor = ref_date - dt.timedelta(days=1)
    for _ in range(max_lookback):
        try:
            bars = fetch_daily_bars(cursor, auth_key=auth_key, session=session)
        except KrxBarsError:
            cursor -= dt.timedelta(days=1)
            continue
        return cursor, bars
    raise KrxBarsError(f"no trading day found within {max_lookback} days before {ref_date}")


def derive_market_map(bars: pl.DataFrame) -> dict[str, str]:
    return dict(zip(bars["symbol"].to_list(), bars["market"].to_list(), strict=True))


def append_daily_bars(store_path: pathlib.Path, bars: pl.DataFrame) -> int:
    store = pathlib.Path(store_path)
    incoming = bars.select(REQUIRED_BAR_COLUMNS).with_columns(
        [
            pl.col("date").cast(pl.Date),
            pl.col("symbol").cast(pl.String),
            pl.col("close").cast(pl.Float64),
            pl.col("volume").cast(pl.Int64),
            pl.col("trade_value_100m").cast(pl.Float64),
            pl.col("daily_change_pct").cast(pl.Float64),
        ]
    )
    if store.exists():
        existing = pl.read_parquet(store)
        existing_dates = existing["date"].to_list()
        new_rows = incoming.filter(~pl.col("date").is_in(existing_dates))
        if new_rows.height == 0:
            return 0
        combined = pl.concat([existing, new_rows]).sort(["symbol", "date"])
    else:
        new_rows = incoming
        combined = incoming.sort(["symbol", "date"])
    store.parent.mkdir(parents=True, exist_ok=True)
    tmp = store.parent / f".{store.name}.tmp"
    combined.write_parquet(tmp, compression="zstd")
    os.replace(tmp, store)
    return new_rows.height


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
        except KrxBarsError:
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


def write_market_map(path: pathlib.Path, market_map: dict[str, str]) -> None:
    target = pathlib.Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.parent / f".{target.name}.tmp"
    tmp.write_text(json.dumps(market_map, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, target)
