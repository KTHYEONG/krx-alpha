"""KIS market-data endpoints: quotation and snapshot reads over safe GET."""

from __future__ import annotations

import datetime as dt
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, cast
from zoneinfo import ZoneInfo

from src.brokers.kis.auth import TokenSource
from src.brokers.kis.http import _MAX_PAGES, KisGetTransport
from src.core.symbols import is_krx_short_code
from src.execution.contracts import KisApiError, Quote

logger = logging.getLogger(__name__)

TR_PRICE: str = "FHKST01010100"
TR_ASKING: str = "FHKST01010200"
TR_DAILY_CHART: str = "FHKST03010100"
# 실측 정정(2026-09-17): FHPST01720000/"/ranking/trade-amount"는 공식 KIS 스펙상
# "거래대금순위"가 아니라 "호가잔량순위"이며 해당 경로는 404를 반환한다(공식
# koreainvestment/open-trading-api 재확인). 거래대금순위는 거래량순위 TR에
# FID_BLNG_CLS_CODE="3"(거래금액순)을 지정해 조회한다.
TR_TRADE_AMOUNT: str = "FHPST01710000"
TR_FLUCTUATION: str = "FHPST01700000"
TR_INVESTOR_ESTIMATE: str = "HHPTJ04160200"
TR_PROGRAM_TRADE: str = "FHPPG04650101"
TR_INDEX_PRICE: str = "FHPUP02100000"
TR_INDEX_MINUTE_CHART: str = "FHKUP03500200"
TR_MINUTE_CHART: str = "FHKST03010200"
TR_NEWS_TITLE: str = "FHKST01011800"

_PATH_PRICE = "/uapi/domestic-stock/v1/quotations/inquire-price"
_PATH_ASKING = "/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn"
_PATH_DAILY_CHART: str = "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
_PATH_TRADE_AMOUNT = "/uapi/domestic-stock/v1/quotations/volume-rank"
_PATH_FLUCTUATION = "/uapi/domestic-stock/v1/ranking/fluctuation"
_PATH_INVESTOR_ESTIMATE = "/uapi/domestic-stock/v1/quotations/investor-trend-estimate"
_PATH_PROGRAM_TRADE = "/uapi/domestic-stock/v1/quotations/program-trade-by-stock"
_PATH_INDEX_PRICE = "/uapi/domestic-stock/v1/quotations/inquire-index-price"
_PATH_INDEX_MINUTE_CHART = "/uapi/domestic-stock/v1/quotations/inquire-time-indexchartprice"
_PATH_MINUTE_CHART = "/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
_PATH_NEWS_TITLE = "/uapi/domestic-stock/v1/quotations/news-title"

_KST: dt.tzinfo = ZoneInfo("Asia/Seoul")


@dataclass(frozen=True)
class KisRankingRow:
    """KIS 랭킹 행 (거래대금·등락률 공용)."""

    symbol: str
    rank: int
    change_pct: float
    trade_value_krw: int


def _parse_ranking_rows(rows: list[dict[str, Any]]) -> tuple[KisRankingRow, ...]:
    try:
        parsed: list[KisRankingRow] = []
        seen: set[str] = set()
        for row in rows:
            symbol = str(row.get("stck_shrn_iscd", ""))
            # 형태가 깨진 코드는 건너뛴다. 종목 구분(보통주 여부)은 바 분류
            # 메타데이터로 하류에서 걸러내므로 여기서 판단하지 않는다.
            if not is_krx_short_code(symbol) or symbol in seen:
                continue
            change_raw = str(row.get("prdy_ctrt", ""))
            try:
                change = Decimal(change_raw)
            except (ValueError, TypeError, ArithmeticError):
                continue
            if not change.is_finite():
                continue
            # 등락률 랭킹(FHPST01700000)은 acml_tr_pbmn 필드를 아예 반환하지 않는다
            # (실측 확인) -- 선택 순위는 랭크 위치로만 결정되고 이 값은 메타데이터
            # 표시용이라, 미제공을 0으로 취급해도 선정 로직에 영향이 없다.
            try:
                trade_value = int(Decimal(str(row.get("acml_tr_pbmn") or "0")))
            except (ValueError, TypeError, ArithmeticError):
                continue
            if trade_value < 0:
                continue
            seen.add(symbol)
            parsed.append(
                KisRankingRow(
                    symbol=symbol,
                    rank=len(parsed) + 1,
                    change_pct=float(change),
                    trade_value_krw=trade_value,
                )
            )
        if not parsed:
            raise ValueError("invalid ranking schema")
        return tuple(parsed)
    except (ValueError, KeyError, TypeError, AttributeError, ArithmeticError) as exc:
        raise KisApiError("SCHEMA", "invalid ranking schema") from exc


def _to_int(raw: Any) -> int:
    text = str(raw or "0")
    return int(Decimal(text))


def _snapshot_raw(output: Mapping[str, Any], key: str) -> Any:
    if key not in output:
        raise KisApiError("SCHEMA", f"missing snapshot field: {key}")
    return output[key]


def _snapshot_code(output: Mapping[str, Any], key: str) -> str:
    return str(_snapshot_raw(output, key)).strip()


def _snapshot_int(output: Mapping[str, Any], key: str) -> int:
    text = str(_snapshot_raw(output, key)).strip()
    try:
        value = Decimal(text)
    except (ArithmeticError, ValueError) as exc:
        raise KisApiError("SCHEMA", f"invalid int field: {key}") from exc
    if not value.is_finite() or value != value.to_integral_value():
        raise KisApiError("SCHEMA", f"invalid int field: {key}")
    return int(value)


def _snapshot_float(output: Mapping[str, Any], key: str) -> float:
    text = str(_snapshot_raw(output, key)).strip()
    try:
        value = Decimal(text)
    except (ArithmeticError, ValueError) as exc:
        raise KisApiError("SCHEMA", f"invalid float field: {key}") from exc
    if not value.is_finite():
        raise KisApiError("SCHEMA", f"invalid float field: {key}")
    return float(value)


def _snapshot_yn(output: Mapping[str, Any], key: str) -> bool:
    text = str(_snapshot_raw(output, key)).strip()
    if text == "Y":
        return True
    if text == "N":
        return False
    raise KisApiError("SCHEMA", f"invalid Y/N field: {key}")


def _snapshot_ladder_level(output: Mapping[str, Any], key: str) -> int:
    if str(_snapshot_raw(output, key)).strip() == "":
        return 0
    return _snapshot_int(output, key)


def _snapshot_news_ns(date_text: str, time_text: str) -> int:
    if len(date_text) != 8 or len(time_text) != 6 or not date_text.isdigit() or not time_text.isdigit():
        raise KisApiError("SCHEMA", "malformed news publication time")
    try:
        moment = dt.datetime(
            int(date_text[0:4]),
            int(date_text[4:6]),
            int(date_text[6:8]),
            int(time_text[0:2]),
            int(time_text[2:4]),
            int(time_text[4:6]),
            tzinfo=_KST,
        )
    except ValueError as exc:
        raise KisApiError("SCHEMA", "malformed news publication time") from exc
    delta = moment.astimezone(dt.UTC) - dt.datetime(1970, 1, 1, tzinfo=dt.UTC)
    return (delta.days * 86_400 + delta.seconds) * 1_000_000_000


class KisDataClient:
    """Read and validate KIS market data for collection and execution quotes.

    Each method retains its existing KIS transaction contract and rejects malformed
    vendor fields before they enter selection, snapshots, or execution pricing.
    """

    def __init__(self, *, transport: KisGetTransport) -> None:
        self._transport = transport

    def ensure_token(self) -> TokenSource:
        """Ensure a valid token exists before collection starts."""
        return self._transport.ensure_token()

    def access_token(self, *, force: bool = False) -> str:
        """Return a usable token, refreshing only when expiring."""
        return self._transport.access_token(force=force)

    def get_quote(self, symbol: str) -> Quote:
        """현재가+10단계 호가를 조회한다."""
        params = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": symbol}
        price_body, _ = self._transport.get(_PATH_PRICE, TR_PRICE, params)
        asking_body, _ = self._transport.get(_PATH_ASKING, TR_ASKING, params)
        output = price_body.get("output") or {}
        book = asking_body.get("output1") or {}
        asks: list[tuple[int, int]] = []
        bids: list[tuple[int, int]] = []
        for i in range(1, 11):
            ask_price = _to_int(book.get(f"askp{i}", "0"))
            if ask_price > 0:
                asks.append((ask_price, _to_int(book.get(f"askp_rsqn{i}", "0"))))
            bid_price = _to_int(book.get(f"bidp{i}", "0"))
            if bid_price > 0:
                bids.append((bid_price, _to_int(book.get(f"bidp_rsqn{i}", "0"))))
        quote = Quote(
            symbol=symbol,
            last=_to_int(output.get("stck_prpr", "0")),
            upper_limit=_to_int(output.get("stck_mxpr", "0")),
            lower_limit=_to_int(output.get("stck_llam", "0")),
            tick=_to_int(output.get("aspr_unit", "0")),
            halted=str(output.get("temp_stop_yn", "N")) == "Y",
            asks=tuple(asks),
            bids=tuple(bids),
        )
        logger.debug(
            "[EXEC] stage=quote symbol=%s last=%d halted=%s", symbol, quote.last, str(quote.halted)
        )
        return quote

    def get_daily_bar(self, symbol: str, day: dt.date) -> dict[str, str] | None:
        """일자별 itemchartprice(output2 선두행)를 조회한다 (무효 심볼의 전필드 0 응답은 None)."""
        stamp = day.strftime("%Y%m%d")
        params = {
            "fid_cond_mrkt_div_code": "J",
            "fid_input_iscd": symbol,
            "fid_input_date_1": stamp,
            "fid_input_date_2": stamp,
            "fid_period_div_code": "D",
            "fid_org_adj_prc": "1",
        }
        body, _ = self._transport.get(_PATH_DAILY_CHART, TR_DAILY_CHART, params)
        rows = body.get("output2") or []
        if not rows:
            return None
        row = rows[0]
        if str(row.get("stck_clpr", "0")) == "0":
            return None
        return cast("dict[str, str]", row)

    def get_trade_amount_ranking(self) -> tuple[KisRankingRow, ...]:
        """거래대금 랭킹(TR FHPST01710000, 거래량순위 화면의 거래금액순 정렬)을 조회한다."""
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_COND_SCR_DIV_CODE": "20171",
            "FID_INPUT_ISCD": "0000",
            "FID_DIV_CLS_CODE": "0",
            "FID_BLNG_CLS_CODE": "3",  # 3: 거래금액순 (실측 확인)
            "FID_TRGT_CLS_CODE": "0000000000",
            # 10자리: 위험/경고/주의 관리종목 정리매매 불성실공시 우선주 거래정지 ETF ETN
            # 신용주문불가 SPAC 순. 우선주(5번째 자리) 및 ETF/ETN(7,8번째 자리)을 제외하여
            # 6자리 숫자가 아닌 종목코드(예: 우선주 "0161M0", 레버리지 ETF "0193T0")가
            # 랭킹에 유입되는 것을 방지한다(실측 확인: 2026-09-21 네오사피엔스 0161M0 회귀).
            "FID_TRGT_EXLS_CLS_CODE": "0000101100",
            "FID_INPUT_PRICE_1": "",
            "FID_INPUT_PRICE_2": "",
            "FID_VOL_CNT": "",
            "FID_INPUT_DATE_1": "",
        }
        body, _ = self._transport.get(_PATH_TRADE_AMOUNT, TR_TRADE_AMOUNT, params)
        # 이 화면의 종목코드 필드는 mksc_shrn_iscd 이며 등락률 화면(stck_shrn_iscd)과
        # 다르다(실측 확인). 공용 파서가 stck_shrn_iscd 만 읽으므로 여기서 정규화한다.
        rows = [
            {**row, "stck_shrn_iscd": row.get("stck_shrn_iscd") or row.get("mksc_shrn_iscd", "")}
            for row in (body.get("output") or [])
        ]
        return _parse_ranking_rows(rows)

    def get_fluctuation_ranking(self) -> tuple[KisRankingRow, ...]:
        """등락률 랭킹(TR FHPST01700000)을 조회한다."""
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_COND_SCR_DIV_CODE": "20170",
            "FID_INPUT_ISCD": "0000",
            "FID_RANK_SORT_CLS_CODE": "0",
            "FID_INPUT_CNT_1": "200",
            "FID_PRC_CLS_CODE": "0",
            "FID_INPUT_PRICE_1": "",
            "FID_INPUT_PRICE_2": "",
            "FID_VOL_CNT": "",
            "FID_TRGT_CLS_CODE": "0",
            "FID_TRGT_EXLS_CLS_CODE": "0",
            "FID_DIV_CLS_CODE": "0",
            "FID_RSFL_RATE1": "0",
            "FID_RSFL_RATE2": "30",
        }
        body, _ = self._transport.get(_PATH_FLUCTUATION, TR_FLUCTUATION, params)
        return _parse_ranking_rows(list(body.get("output") or []))

    def get_security_status(self, symbol: str) -> dict[str, object]:
        """Fetch exchange designation flags and price limits for one stock.

        The flags are a point-in-time observation; KIS offers no history, so the
        caller must persist every observation it relies on.

        Raises:
            KisApiError: On transport/API failure or when a field violates its type.
        """
        params = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": symbol}
        body, _ = self._transport.get(_PATH_PRICE, TR_PRICE, params)
        output = body.get("output") or {}
        row = {
            "source_tr": TR_PRICE,
            "market_div_code": "J",
            "symbol": symbol,
            "status_code": _snapshot_code(output, "iscd_stat_cls_code"),
            "managed": _snapshot_yn(output, "mang_issu_cls_code"),
            "market_warning_code": _snapshot_code(output, "mrkt_warn_cls_code"),
            "short_overheated": _snapshot_yn(output, "short_over_yn"),
            "investment_caution": _snapshot_yn(output, "invt_caful_yn"),
            "liquidation_trading": _snapshot_yn(output, "sltr_yn"),
            "trading_halted": _snapshot_yn(output, "temp_stop_yn"),
            "vi_code": _snapshot_code(output, "vi_cls_code"),
            "ovtm_vi_cls_code": _snapshot_code(output, "ovtm_vi_cls_code"),
            "credit_available": _snapshot_yn(output, "crdt_able_yn"),
            "last_price": _snapshot_int(output, "stck_prpr"),
            "base_price": _snapshot_int(output, "stck_sdpr"),
            "upper_limit": _snapshot_int(output, "stck_mxpr"),
            "lower_limit": _snapshot_int(output, "stck_llam"),
        }
        logger.debug("[DATA] stage=kis_snapshot tr=%s symbol=%s rows=1", TR_PRICE, symbol)
        return row

    def get_auction_book(self, symbol: str) -> dict[str, object]:
        """Fetch the 10-level book with the single-price auction expectation.

        Ladder lists always keep ten positional levels (empty levels as 0) so a
        level index means the same depth across observations.

        Raises:
            KisApiError: On transport/API failure or schema violation.
        """
        params = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": symbol}
        body, _ = self._transport.get(_PATH_ASKING, TR_ASKING, params)
        book = body.get("output1") or {}
        expect = body.get("output2") or {}
        row = {
            "source_tr": TR_ASKING,
            "market_div_code": "J",
            "symbol": symbol,
            "book_time": _snapshot_code(book, "aspr_acpt_hour"),
            "auction_code": _snapshot_code(expect, "antc_mkop_cls_code"),
            "expected_price": _snapshot_int(expect, "antc_cnpr"),
            "expected_volume": _snapshot_int(expect, "antc_vol"),
            "expected_change_pct": _snapshot_float(expect, "antc_cntg_prdy_ctrt"),
            "vi_code": _snapshot_code(expect, "vi_cls_code"),
            "last_price": _snapshot_int(expect, "stck_prpr"),
            "base_price": _snapshot_int(expect, "stck_sdpr"),
            "ask_prices": [_snapshot_ladder_level(book, f"askp{i}") for i in range(1, 11)],
            "ask_sizes": [_snapshot_ladder_level(book, f"askp_rsqn{i}") for i in range(1, 11)],
            "bid_prices": [_snapshot_ladder_level(book, f"bidp{i}") for i in range(1, 11)],
            "bid_sizes": [_snapshot_ladder_level(book, f"bidp_rsqn{i}") for i in range(1, 11)],
            "total_ask_size": _snapshot_int(book, "total_askp_rsqn"),
            "total_bid_size": _snapshot_int(book, "total_bidp_rsqn"),
        }
        logger.debug("[DATA] stage=kis_snapshot tr=%s symbol=%s rows=1", TR_ASKING, symbol)
        return row

    def get_investor_estimate(self, symbol: str) -> tuple[dict[str, object], ...]:
        """Fetch provisional foreign/institution cumulative net quantities by bucket.

        Buckets follow the exchange provisional-flow publication slots (1..5); the
        response carries no publication timestamp, so observation time is the only
        point-in-time anchor.

        Raises:
            KisApiError: On transport/API failure, bucket outside 1..5, duplicate
                bucket, or schema violation.
        """
        body, _ = self._transport.get(
            _PATH_INVESTOR_ESTIMATE, TR_INVESTOR_ESTIMATE, {"MKSC_SHRN_ISCD": symbol}
        )
        raw_rows = body.get("output2") or []
        ranked: list[tuple[int, dict[str, object]]] = []
        seen: set[int] = set()
        for raw in raw_rows:
            bucket = _snapshot_int(raw, "bsop_hour_gb")
            if bucket < 1 or bucket > 5:
                raise KisApiError("SCHEMA", f"bucket out of range: {bucket}")
            if bucket in seen:
                raise KisApiError("SCHEMA", f"duplicate bucket: {bucket}")
            seen.add(bucket)
            ranked.append((
                bucket,
                {
                    "source_tr": TR_INVESTOR_ESTIMATE,
                    "market_div_code": "",
                    "symbol": symbol,
                    "bucket": bucket,
                    "foreign_net_qty": _snapshot_int(raw, "frgn_fake_ntby_qty"),
                    "institution_net_qty": _snapshot_int(raw, "orgn_fake_ntby_qty"),
                    "total_net_qty": _snapshot_int(raw, "sum_fake_ntby_qty"),
                },
            ))
        ranked.sort(key=lambda item: item[0])
        logger.debug(
            "[DATA] stage=kis_snapshot tr=%s symbol=%s rows=%d", TR_INVESTOR_ESTIMATE, symbol, len(ranked)
        )
        return tuple(row for _, row in ranked)

    def get_program_trade_latest(self, symbol: str) -> dict[str, object] | None:
        """Fetch the latest cumulative program-trading totals for one stock.

        Returns:
            The row with the greatest ``trade_time``, or ``None`` if no program
            trade has been reported yet.

        Raises:
            KisApiError: On transport/API failure, schema violation, or when the
                selected row breaks ``net = buy - sell`` for quantity or value.
        """
        params = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": symbol}
        body, _ = self._transport.get(_PATH_PROGRAM_TRADE, TR_PROGRAM_TRADE, params)
        raw_rows = body.get("output") or []
        if not raw_rows:
            logger.debug("[DATA] stage=kis_snapshot tr=%s symbol=%s rows=0", TR_PROGRAM_TRADE, symbol)
            return None
        timed = [(_snapshot_code(raw, "bsop_hour"), raw) for raw in raw_rows]
        trade_time, winner = max(timed, key=lambda item: item[0])
        sell_qty = _snapshot_int(winner, "whol_smtn_seln_vol")
        buy_qty = _snapshot_int(winner, "whol_smtn_shnu_vol")
        net_qty = _snapshot_int(winner, "whol_smtn_ntby_qty")
        sell_value = _snapshot_int(winner, "whol_smtn_seln_tr_pbmn")
        buy_value = _snapshot_int(winner, "whol_smtn_shnu_tr_pbmn")
        net_value = _snapshot_int(winner, "whol_smtn_ntby_tr_pbmn")
        if net_qty != buy_qty - sell_qty or net_value != buy_value - sell_value:
            raise KisApiError("SCHEMA", "program trade net identity broken")
        row = {
            "source_tr": TR_PROGRAM_TRADE,
            "market_div_code": "J",
            "symbol": symbol,
            "trade_time": trade_time,
            "cum_volume": _snapshot_int(winner, "acml_vol"),
            "sell_qty": sell_qty,
            "buy_qty": buy_qty,
            "net_qty": net_qty,
            "sell_value_krw": sell_value,
            "buy_value_krw": buy_value,
            "net_value_krw": net_value,
        }
        logger.debug("[DATA] stage=kis_snapshot tr=%s symbol=%s rows=1", TR_PROGRAM_TRADE, symbol)
        return row

    def get_index_snapshot(self, index_code: str) -> dict[str, object]:
        """Fetch the current value, breadth and cumulative turnover of a market index.

        Raises:
            KisApiError: On transport/API failure or schema violation.
        """
        params = {"FID_COND_MRKT_DIV_CODE": "U", "FID_INPUT_ISCD": index_code}
        body, _ = self._transport.get(_PATH_INDEX_PRICE, TR_INDEX_PRICE, params)
        output = body.get("output") or {}
        row = {
            "source_tr": TR_INDEX_PRICE,
            "market_div_code": "U",
            "index_code": index_code,
            "index_value": _snapshot_float(output, "bstp_nmix_prpr"),
            "change_pct": _snapshot_float(output, "bstp_nmix_prdy_ctrt"),
            "cum_value_mil_krw": _snapshot_int(output, "acml_tr_pbmn"),
            "advancers": _snapshot_int(output, "ascn_issu_cnt"),
            "decliners": _snapshot_int(output, "down_issu_cnt"),
        }
        logger.debug("[DATA] stage=kis_snapshot tr=%s symbol=%s rows=1", TR_INDEX_PRICE, index_code)
        return row

    def get_index_minute_bars(
        self, index_code: str, *, session_date: dt.date
    ) -> tuple[dict[str, object], ...]:
        """Fetch the vendor's most recent one-minute index bars (fixed 60-second interval).

        The endpoint has no time cursor and no continuation mechanism, so one call
        only ever returns its most recent page; full-day coverage depends on the
        caller polling at an interval no longer than that page's span.

        Args:
            session_date: Trading date bars must belong to; other-day rows are
                dropped rather than misfiled into this session.

        Raises:
            KisApiError: On transport/API failure, schema violation, or a bar
                whose low/high does not bound its open and close.
        """
        stamp = session_date.strftime("%Y%m%d")
        params = {
            "FID_COND_MRKT_DIV_CODE": "U",
            "FID_ETC_CLS_CODE": "0",
            "FID_INPUT_ISCD": index_code,
            "FID_INPUT_HOUR_1": "60",
            "FID_PW_DATA_INCU_YN": "Y",
        }
        body, _ = self._transport.get(_PATH_INDEX_MINUTE_CHART, TR_INDEX_MINUTE_CHART, params)
        rows = body.get("output2") or []
        bars: dict[str, dict[str, object]] = {}
        for raw in rows:
            hour = _snapshot_code(raw, "stck_cntg_hour")
            if hour in ("999999", "888888"):
                continue
            if str(raw.get("stck_bsop_date", "")) != stamp:
                continue
            if hour in bars:
                continue
            open_p = _snapshot_float(raw, "bstp_nmix_oprc")
            high = _snapshot_float(raw, "bstp_nmix_hgpr")
            low = _snapshot_float(raw, "bstp_nmix_lwpr")
            close_p = _snapshot_float(raw, "bstp_nmix_prpr")
            if low > min(open_p, close_p) or max(open_p, close_p) > high:
                raise KisApiError("SCHEMA", f"bar range violated: {hour}")
            bars[hour] = {
                "source_tr": TR_INDEX_MINUTE_CHART,
                "market_div_code": "U",
                "index_code": index_code,
                "bar_time": hour,
                "open": open_p,
                "high": high,
                "low": low,
                "close": close_p,
                "volume": _snapshot_int(raw, "cntg_vol"),
                "cum_value_mil_krw": _snapshot_int(raw, "acml_tr_pbmn"),
            }
        ordered = tuple(bars[key] for key in sorted(bars))
        logger.debug(
            "[DATA] stage=kis_snapshot tr=%s symbol=%s rows=%d", TR_INDEX_MINUTE_CHART, index_code, len(ordered)
        )
        return ordered

    def get_stock_minute_bars(
        self,
        symbol: str,
        *,
        session_date: dt.date,
        session_open: dt.time,
        session_close: dt.time,
    ) -> tuple[dict[str, object], ...]:
        """Fetch same-day KRX one-minute bars inside the regular session window.

        The endpoint pages backward in 30-bar windows keyed by an inclusive time
        cursor, so the method walks the cursor from ``session_close`` toward
        ``session_open`` and stops on exhaustion.

        Returns:
            Bars sorted by ``bar_time`` ascending, unique per ``bar_time``.

        Raises:
            KisApiError: On transport/API failure, schema violation, a bar whose
                low/high does not bound its open and close, or when the page cap
                is exceeded.
        """
        stamp = session_date.strftime("%Y%m%d")
        open_s = session_open.strftime("%H%M%S")
        close_s = session_close.strftime("%H%M%S")
        bars: dict[str, dict[str, object]] = {}
        cursor = close_s
        for _ in range(_MAX_PAGES):
            params = {
                "FID_ETC_CLS_CODE": "",
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": symbol,
                "FID_INPUT_HOUR_1": cursor,
                "FID_PW_DATA_INCU_YN": "N",
            }
            body, _ = self._transport.get(_PATH_MINUTE_CHART, TR_MINUTE_CHART, params)
            rows = body.get("output2") or []
            if not rows:
                break
            day_rows = [raw for raw in rows if str(raw.get("stck_bsop_date", "")) == stamp]
            fresh = False
            min_hour: str | None = None
            for raw in day_rows:
                hour = _snapshot_code(raw, "stck_cntg_hour")
                if len(hour) != 6 or not hour.isdigit():
                    raise KisApiError("SCHEMA", f"malformed bar time: {hour}")
                if min_hour is None or hour < min_hour:
                    min_hour = hour
                if hour < open_s or hour > close_s or hour in bars:
                    continue
                open_p = _snapshot_int(raw, "stck_oprc")
                high = _snapshot_int(raw, "stck_hgpr")
                low = _snapshot_int(raw, "stck_lwpr")
                close_p = _snapshot_int(raw, "stck_prpr")
                if low > min(open_p, close_p) or max(open_p, close_p) > high:
                    raise KisApiError("SCHEMA", f"bar range violated: {hour}")
                bars[hour] = {
                    "source_tr": TR_MINUTE_CHART,
                    "market_div_code": "J",
                    "symbol": symbol,
                    "bar_time": hour,
                    "open": open_p,
                    "high": high,
                    "low": low,
                    "close": close_p,
                    "volume": _snapshot_int(raw, "cntg_vol"),
                }
                fresh = True
            if not fresh:
                break
            assert min_hour is not None
            if min_hour <= open_s:
                break
            cursor = (dt.datetime.strptime(min_hour, "%H%M%S") - dt.timedelta(seconds=60)).strftime("%H%M%S")
        else:
            raise KisApiError("PAGINATION", f"exceeded {_MAX_PAGES} pages")
        ordered = tuple(bars[key] for key in sorted(bars))
        logger.debug(
            "[DATA] stage=kis_snapshot tr=%s symbol=%s rows=%d", TR_MINUTE_CHART, symbol, len(ordered)
        )
        return ordered

    def get_news_titles(
        self, *, before: tuple[str, str] | None = None
    ) -> tuple[dict[str, object], ...]:
        """Fetch one page of market news and exchange disclosure headlines.

        Args:
            before: Optional inclusive ``(YYYYMMDD, HHMMSS)`` cursor; ``None``
                requests the newest page.

        Returns:
            Rows in vendor order (newest first) with the KST publication time
            converted to UTC epoch nanoseconds.

        Raises:
            KisApiError: On transport/API failure, empty news id, malformed
                publication date/time, or schema violation.
        """
        params = {
            "FID_NEWS_OFER_ENTP_CODE": "",
            "FID_COND_MRKT_CLS_CODE": "",
            "FID_INPUT_ISCD": "",
            "FID_TITL_CNTT": "",
            "FID_INPUT_DATE_1": "",
            "FID_INPUT_HOUR_1": "",
            "FID_RANK_SORT_CLS_CODE": "",
            "FID_INPUT_SRNO": "",
        }
        if before is not None:
            params["FID_INPUT_DATE_1"] = before[0]
            params["FID_INPUT_HOUR_1"] = before[1]
        body, _ = self._transport.get(_PATH_NEWS_TITLE, TR_NEWS_TITLE, params)
        raw_rows = body.get("output")
        if not raw_rows:
            return ()
        parsed: list[dict[str, object]] = []
        for raw in raw_rows:
            news_id = _snapshot_code(raw, "cntt_usiq_srno")
            if not news_id:
                raise KisApiError("SCHEMA", "empty news id")
            symbols = [
                text for i in range(1, 11) if (text := str(raw.get(f"iscd{i}", "")).strip())
            ]
            parsed.append({
                "source_tr": TR_NEWS_TITLE,
                "market_div_code": "",
                "news_id": news_id,
                "published_at_ns": _snapshot_news_ns(
                    _snapshot_code(raw, "data_dt"), _snapshot_code(raw, "data_tm")
                ),
                "provider": _snapshot_code(raw, "dorg"),
                "provider_code": _snapshot_code(raw, "news_ofer_entp_code"),
                "category_code": _snapshot_code(raw, "news_lrdv_code"),
                "title": _snapshot_code(raw, "hts_pbnt_titl_cntt"),
                "symbols": symbols,
            })
        logger.debug("[DATA] stage=kis_snapshot tr=%s symbol=%s rows=%d", TR_NEWS_TITLE, "", len(parsed))
        return tuple(parsed)
