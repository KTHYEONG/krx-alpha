"""L1 체결 틱 디코드 + 정합성 검증 (배치 단계, 수집 루프 미접촉)."""

from __future__ import annotations

import itertools
import json
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum

import polars as pl

from src.core.config import DataQualitySettings
from src.storage.quality_kis import decode_kis_quotes, decode_kis_ticks
from src.storage.quality_ls import decode_ls_quotes, decode_ls_ticks

_decode_tick_chunk = decode_ls_ticks
_decode_ls_quote_chunk = decode_ls_quotes
_decode_kis_tick_chunk = decode_kis_ticks
_decode_kis_quote_frame = decode_kis_quotes

_TICK_STREAM: str = "H0STCNT0"
_TICK_STREAMS: tuple[str, ...] = ("H0STCNT0", "H0NXCNT0")
_PRICE_BAND_RATIO: float = 0.30

_TICK_BODY_FIELDS: tuple[str, ...] = ("shcode", "price", "cvolume", "volume", "change", "sign", "drate", "mdchecnt", "mschecnt")
_QUOTE_STREAM: str = "H0STASP0"
_QUOTE_STREAMS: tuple[str, ...] = ("H0STASP0", "H0NXASP0")
_SCHEMA_DISAGREE_TOLERANCE: float = 0.01
_QUOTE_LEVELS: int = 10
_QUOTE_CHUNK_ROWS: int = 20_000
_TICK_CHUNK_ROWS: int = 20_000
_BUCKET_HASH_SEED: int = 0x9E3779B1
_AUCTION_WINDOWS: tuple[tuple[int, int], ...] = ((83000, 90000), (152000, 153000))
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


@dataclass(frozen=True)
class TickQualitySummary:
    rows: int
    decode_fail: int
    zero_volume: int
    price_band_violation: int
    cum_volume_regression: int
    schema_disagree: int
    tick_loss: int
    lost_volume: int


@dataclass(frozen=True)
class QuoteQualitySummary:
    rows: int
    decode_fail: int
    ladder_disorder: int
    crossed_book: int
    negative_remain: int
    total_remain_short: int


class DqStatus(StrEnum):
    PASS = "PASS"  # noqa: S105
    WARN = "WARN"  # noqa: S105
    FAIL = "FAIL"  # noqa: S105


DQ_METADATA_KEY: str = "krx_alpha.dq"


@dataclass(frozen=True)
class DqVerdict:
    status: DqStatus
    tick: TickQualitySummary | None
    quote: QuoteQualitySummary | None
    reasons: tuple[str, ...]

    def to_metadata(self) -> dict[str, str]:
        """Serialize into parquet footer key-value metadata under key ``krx_alpha.dq``."""
        payload = {
            "quote": None if self.quote is None else {
                "crossed_book": self.quote.crossed_book,
                "decode_fail": self.quote.decode_fail,
                "ladder_disorder": self.quote.ladder_disorder,
                "negative_remain": self.quote.negative_remain,
                "rows": self.quote.rows,
                "total_remain_short": self.quote.total_remain_short,
            },
            "reasons": list(self.reasons),
            "status": self.status.value,
            "tick": None if self.tick is None else {
                "cum_volume_regression": self.tick.cum_volume_regression,
                "decode_fail": self.tick.decode_fail,
                "lost_volume": self.tick.lost_volume,
                "price_band_violation": self.tick.price_band_violation,
                "rows": self.tick.rows,
                "schema_disagree": self.tick.schema_disagree,
                "tick_loss": self.tick.tick_loss,
                "zero_volume": self.tick.zero_volume,
            },
        }
        return {DQ_METADATA_KEY: json.dumps(payload, sort_keys=True)}


def evaluate_stream_quality(
    tick: TickQualitySummary | None,
    quote: QuoteQualitySummary | None,
    *,
    settings: DataQualitySettings,
) -> DqVerdict:
    """Classify one L1 partition's DQ counters into a PASS/WARN/FAIL verdict.

    PASS means every counter is zero. WARN means some counter is non-zero but
    every ratio stays within its configured ceiling (known vendor noise). FAIL
    means at least one ratio exceeds its ceiling, i.e. the feed or the decoder
    is structurally broken for this partition.

    Args:
        tick: Tick summary, or None when the partition holds no tick rows.
        quote: Quote summary, or None when the partition holds no quote rows.
        settings: Ratio ceilings.

    Returns:
        Verdict with human-readable reasons naming each exceeded counter and ratio.

    Raises:
        ValueError: If both summaries are None.
    """
    if tick is None and quote is None:
        raise ValueError("both tick and quote summaries are None")
    reasons: list[str] = []
    any_nonzero = False

    def _check(count: int, rows: int, ceiling: float, name: str) -> None:
        nonlocal any_nonzero
        if rows <= 0:
            return
        if count > 0:
            any_nonzero = True
            ratio = count / rows
            if ratio > ceiling:
                reasons.append(f"{name}={count}/{rows} ratio={ratio:.6f} exceeds {ceiling:.6f}")

    if tick is not None:
        _check(tick.decode_fail, tick.rows, settings.max_decode_fail_ratio, "tick.decode_fail")
        _check(tick.zero_volume, tick.rows, settings.max_invariant_violation_ratio, "tick.zero_volume")
        _check(tick.price_band_violation, tick.rows, settings.max_invariant_violation_ratio, "tick.price_band_violation")
        _check(tick.cum_volume_regression, tick.rows, settings.max_invariant_violation_ratio, "tick.cum_volume_regression")
        _check(tick.schema_disagree, tick.rows, settings.max_invariant_violation_ratio, "tick.schema_disagree")
        _check(tick.tick_loss, tick.rows, settings.max_invariant_violation_ratio, "tick.tick_loss")
    if quote is not None:
        _check(quote.decode_fail, quote.rows, settings.max_decode_fail_ratio, "quote.decode_fail")
        _check(quote.ladder_disorder, quote.rows, settings.max_invariant_violation_ratio, "quote.ladder_disorder")
        _check(quote.crossed_book, quote.rows, settings.max_invariant_violation_ratio, "quote.crossed_book")
        _check(quote.negative_remain, quote.rows, settings.max_invariant_violation_ratio, "quote.negative_remain")
        _check(quote.total_remain_short, quote.rows, settings.max_total_remain_short_ratio, "quote.total_remain_short")
    status = DqStatus.FAIL if reasons else (DqStatus.WARN if any_nonzero else DqStatus.PASS)
    return DqVerdict(status=status, tick=tick, quote=quote, reasons=tuple(reasons))


def _split_kis_ls(chunk: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    if "vendor" not in chunk.columns:
        return chunk.slice(0, 0), chunk
    kis = chunk.filter(pl.col("vendor") == "kis")
    ls = chunk.filter((pl.col("vendor") != "kis") | pl.col("vendor").is_null())
    return kis, ls


def _decode_tick_chunk_mixed(chunk: pl.DataFrame) -> pl.DataFrame:
    kis, ls = _split_kis_ls(chunk)
    parts: list[pl.DataFrame] = []
    if kis.height > 0:
        parts.append(_decode_kis_tick_chunk(kis))
    if ls.height > 0:
        parts.append(_decode_tick_chunk(ls))
    return pl.concat(parts)


def _summarize_tick_fields(raw_fields: pl.DataFrame, *, rows: int) -> TickQualitySummary:
    frame = raw_fields.with_columns(
        price=pl.col("price_raw").cast(pl.Float64, strict=False),
        cvolume=pl.col("cvolume_raw").cast(pl.Int64, strict=False),
        volume=pl.col("volume_raw").cast(pl.Int64, strict=False),
        change=pl.col("change_raw").cast(pl.Float64, strict=False),
        drate=pl.col("drate_raw").cast(pl.Float64, strict=False),
        mdchecnt=pl.col("mdchecnt_raw").cast(pl.Int64, strict=False),
        mschecnt=pl.col("mschecnt_raw").cast(pl.Int64, strict=False),
    )
    flagged = frame.with_columns(
        dq_decode_fail=(
            pl.col("shcode").is_null()
            | pl.col("price").is_null()
            | pl.col("price").is_nan()
            | pl.col("cvolume").is_null()
            | pl.col("volume").is_null()
        ),
        ref_price=pl.when(pl.col("sign").is_in(["1", "2"])).then(pl.col("price") - pl.col("change")).otherwise(pl.col("price") + pl.col("change")),
        drate_factor=1 + pl.col("drate") / 100,
    )
    flagged = flagged.with_columns(
        ref_from_drate=pl.when(pl.col("drate_factor").abs() > 1e-9)
        .then(pl.col("price") / pl.col("drate_factor"))
        .otherwise(None),
    )
    flagged = flagged.with_columns(
        dq_zero_volume=(~pl.col("dq_decode_fail")) & (pl.col("cvolume") <= 0),
        dq_price_band_violation=(~pl.col("dq_decode_fail"))
        & (pl.col("ref_price") > 0)
        & (
            (pl.col("price") > pl.col("ref_price") * (1 + _PRICE_BAND_RATIO))
            | (pl.col("price") < pl.col("ref_price") * (1 - _PRICE_BAND_RATIO))
        ),
        dq_schema_disagree=(~pl.col("dq_decode_fail"))
        & pl.col("ref_from_drate").is_not_null()
        & ((pl.col("ref_price") - pl.col("ref_from_drate")).abs() > pl.col("ref_from_drate").abs() * _SCHEMA_DISAGREE_TOLERANCE),
    )
    flagged = flagged.sort(["shcode", "recv_wall_ns"]).with_columns(
        volume_prev=pl.col("volume").shift(1).over("shcode"),
        total_checnt=pl.col("mdchecnt") + pl.col("mschecnt"),
    )
    flagged = flagged.with_columns(
        checnt_prev=pl.col("total_checnt").shift(1).over("shcode"),
    )
    flagged = flagged.with_columns(
        vol_delta=pl.col("volume") - pl.col("volume_prev"),
        checnt_delta=pl.col("total_checnt") - pl.col("checnt_prev"),
    )
    # 실측(scratch/data1_check, ADR_20260909_stream_integrity_v2 정정): 단순 volume-cvolume 비교는
    # 벤더가 짧은 시간 내 여러 체결을 한 메시지로 배치 전송할 때(checnt_delta>1) 대량 오탐(7.1%)을 낸다.
    # mdchecnt+mschecnt(누적 체결건수) 델타로 "실제로 새 체결이 1건 이하였는지"를 게이트해야
    # 진짜 이상만 남는다(재검증 후 0.0006%).
    loss_applicable = (
        (~pl.col("dq_decode_fail"))
        & (pl.col("cvolume") > 0)
        & pl.col("volume_prev").is_not_null()
        & pl.col("checnt_prev").is_not_null()
        & (pl.col("checnt_delta") <= 1)
    )
    flagged = flagged.with_columns(
        dq_cum_volume_regression=(~pl.col("dq_decode_fail")) & pl.col("volume_prev").is_not_null() & (pl.col("volume") < pl.col("volume_prev")),
        dq_tick_loss=loss_applicable & (pl.col("vol_delta") > pl.col("cvolume")),
        lost_volume=pl.when(loss_applicable & (pl.col("vol_delta") > pl.col("cvolume")))
        .then(pl.col("vol_delta") - pl.col("cvolume"))
        .otherwise(0),
    )
    return TickQualitySummary(
        rows=rows,
        decode_fail=int(flagged["dq_decode_fail"].sum()),
        zero_volume=int(flagged["dq_zero_volume"].sum()),
        price_band_violation=int(flagged["dq_price_band_violation"].sum()),
        cum_volume_regression=int(flagged["dq_cum_volume_regression"].sum()),
        schema_disagree=int(flagged["dq_schema_disagree"].sum()),
        tick_loss=int(flagged["dq_tick_loss"].sum()),
        lost_volume=int(flagged["lost_volume"].sum()),
    )


def decode_tick_raw_fields(df: pl.DataFrame, *, chunk_rows: int = _TICK_CHUNK_ROWS) -> pl.DataFrame | None:
    """Decode H0STCNT0 tick bodies into a field frame, one bounded chunk at a time.

    Args:
        df: L1 rows containing raw/tr_id/recv_wall_ns columns.
        chunk_rows: Maximum rows per decode chunk; must be positive.

    Returns:
        Concatenated tick field frame, or None when no tick rows exist.

    Raises:
        ValueError: If chunk_rows is not positive.

    Note:
        Callers needing a one-shot in-memory summary instead of the raw
        fields should use decode_and_flag_ticks.
    """
    ticks = df.filter(pl.col("tr_id").is_in(_TICK_STREAMS))
    if ticks.height == 0:
        return None
    if chunk_rows <= 0:
        raise ValueError(f"chunk_rows must be > 0, got {chunk_rows}")
    # 청크 단위 디코딩으로 피크 메모리를 chunk_rows 행으로 묶는다
    return pl.concat([_decode_tick_chunk_mixed(ticks.slice(offset, chunk_rows)) for offset in range(0, ticks.height, chunk_rows)])


def decode_and_flag_ticks(df: pl.DataFrame, *, chunk_rows: int = _TICK_CHUNK_ROWS) -> TickQualitySummary | None:
    fields = decode_tick_raw_fields(df, chunk_rows=chunk_rows)
    if fields is None:
        return None
    return _summarize_tick_fields(fields, rows=fields.height)


def summarize_tick_fields_bucketed(fields: pl.LazyFrame, *, rows: int, buckets: int) -> TickQualitySummary:
    """Summarize spill-backed tick fields in shcode hash buckets with bounded memory.

    Args:
        fields: Lazy tick field frame (e.g. scan of per-chunk spill parquet).
        rows: Total tick row count reported on the summary.
        buckets: Number of hash buckets; must be positive.

    Returns:
        TickQualitySummary whose counters equal the unbucketed computation.

    Raises:
        ValueError: If buckets is not positive.
    """
    if buckets <= 0:
        raise ValueError(f"buckets must be > 0, got {buckets}")
    # 같은 shcode는 항상 같은 버킷에 모이므로 종목별 누적 상태가 경계를 넘지 않는다
    decode_fail = 0
    zero_volume = 0
    price_band_violation = 0
    cum_volume_regression = 0
    schema_disagree = 0
    tick_loss = 0
    lost_volume = 0
    for b in range(buckets):
        frame = fields.filter((pl.col("shcode").hash(_BUCKET_HASH_SEED) % buckets) == b).collect()
        if frame.height == 0:
            continue
        s = _summarize_tick_fields(frame, rows=frame.height)
        decode_fail += s.decode_fail
        zero_volume += s.zero_volume
        price_band_violation += s.price_band_violation
        cum_volume_regression += s.cum_volume_regression
        schema_disagree += s.schema_disagree
        tick_loss += s.tick_loss
        lost_volume += s.lost_volume
    return TickQualitySummary(
        rows=rows,
        decode_fail=decode_fail,
        zero_volume=zero_volume,
        price_band_violation=price_band_violation,
        cum_volume_regression=cum_volume_regression,
        schema_disagree=schema_disagree,
        tick_loss=tick_loss,
        lost_volume=lost_volume,
    )


def sum_quote_summaries(summaries: Sequence[QuoteQualitySummary]) -> QuoteQualitySummary | None:
    items = list(summaries)
    if not items:
        return None
    return QuoteQualitySummary(
        rows=sum(s.rows for s in items),
        decode_fail=sum(s.decode_fail for s in items),
        ladder_disorder=sum(s.ladder_disorder for s in items),
        crossed_book=sum(s.crossed_book for s in items),
        negative_remain=sum(s.negative_remain for s in items),
        total_remain_short=sum(s.total_remain_short for s in items),
    )


def _flag_quote_invariants(frame: pl.DataFrame) -> pl.DataFrame:
    offer_cols = [f"offerho{k}" for k in range(1, _QUOTE_LEVELS + 1)]
    bid_cols = [f"bidho{k}" for k in range(1, _QUOTE_LEVELS + 1)]
    rem_cols = [*[f"offerrem{k}" for k in range(1, _QUOTE_LEVELS + 1)], *[f"bidrem{k}" for k in range(1, _QUOTE_LEVELS + 1)]]
    ladder_bad = pl.lit(False)
    for a, b in itertools.pairwise(offer_cols):
        ladder_bad = ladder_bad | ((pl.col(a) > 0) & (pl.col(b) > 0) & (pl.col(a) >= pl.col(b)))
    for a, b in itertools.pairwise(bid_cols):
        ladder_bad = ladder_bad | ((pl.col(a) > 0) & (pl.col(b) > 0) & (pl.col(a) <= pl.col(b)))
    neg_rem = pl.lit(False)
    for c in rem_cols:
        neg_rem = neg_rem | (pl.col(c) < 0)
    in_auction = pl.lit(False)
    for lo, hi in _AUCTION_WINDOWS:
        in_auction = in_auction | ((pl.col("hotime") >= lo) & (pl.col("hotime") < hi))
    top_offer_sum = sum((pl.col(c) for c in [f"offerrem{k}" for k in range(1, _QUOTE_LEVELS + 1)]), start=pl.lit(0))
    top_bid_sum = sum((pl.col(c) for c in [f"bidrem{k}" for k in range(1, _QUOTE_LEVELS + 1)]), start=pl.lit(0))
    return frame.with_columns(
        dq_decode_fail=(
            pl.col("shcode").is_null()
            | pl.col("hotime").is_null()
            | pl.col("totofferrem").is_null()
            | pl.col("totbidrem").is_null()
        ),
    ).with_columns(
        dq_ladder_disorder=(~pl.col("dq_decode_fail")) & ladder_bad,
        dq_negative_remain=(~pl.col("dq_decode_fail")) & neg_rem,
        dq_crossed_book=(~pl.col("dq_decode_fail")) & (~in_auction) & (pl.col("bidho1") > 0) & (pl.col("offerho1") > 0) & (pl.col("bidho1") >= pl.col("offerho1")),
        dq_total_remain_short=(~pl.col("dq_decode_fail")) & ((pl.col("totofferrem") < top_offer_sum) | (pl.col("totbidrem") < top_bid_sum)),
    )


def decode_and_flag_quotes(df: pl.DataFrame, *, chunk_rows: int = _QUOTE_CHUNK_ROWS) -> QuoteQualitySummary | None:
    quotes = df.filter(pl.col("tr_id").is_in(_QUOTE_STREAMS))
    if quotes.height == 0:
        return None
    decode_fail = 0
    ladder_disorder = 0
    crossed_book = 0
    negative_remain = 0
    total_remain_short = 0
    for offset in range(0, quotes.height, chunk_rows):
        chunk = quotes.slice(offset, chunk_rows)
        kis, ls = _split_kis_ls(chunk)
        frames: list[pl.DataFrame] = []
        if kis.height > 0:
            frames.append(_decode_kis_quote_frame(kis))
        if ls.height > 0:
            frames.append(_decode_ls_quote_chunk(ls))
        frame = pl.concat(frames) if len(frames) > 1 else frames[0]
        flagged = _flag_quote_invariants(frame)
        decode_fail += int(flagged["dq_decode_fail"].sum())
        ladder_disorder += int(flagged["dq_ladder_disorder"].sum())
        crossed_book += int(flagged["dq_crossed_book"].sum())
        negative_remain += int(flagged["dq_negative_remain"].sum())
        total_remain_short += int(flagged["dq_total_remain_short"].sum())
    return QuoteQualitySummary(
        rows=quotes.height,
        decode_fail=decode_fail,
        ladder_disorder=ladder_disorder,
        crossed_book=crossed_book,
        negative_remain=negative_remain,
        total_remain_short=total_remain_short,
    )
