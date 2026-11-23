"""Invariant guards for the live trade continuity watch (EOD tick-loss parity)."""

from __future__ import annotations

import logging

import polars as pl
import pytest

from src.core.config import DataQualitySettings
from src.realtime.contracts import L0Frame
from src.realtime.trade_continuity import TradeContinuityWatch
from src.storage.quality import _summarize_tick_fields
from src.storage.quality_kis import (
    KIS_TICK_CVOLUME,
    KIS_TICK_MDCHECNT,
    KIS_TICK_MIN_FIELDS,
    KIS_TICK_MSCHECNT,
    KIS_TICK_VOLUME,
)
from src.storage.quality_ls import decode_ls_ticks

_BASE_NS = 1_794_878_824_000_000_000
_SEC_NS = 1_000_000_000


def _ls_raw(symbol: str, volume: int, cvolume: int, mdchecnt: int, mschecnt: int = 0) -> str:
    return (
        '{"header":{"tr_cd":"S3_","tr_key":"' + symbol + '"},"body":{"shcode":"' + symbol + '","price":"1000",'
        '"cvolume":"' + str(cvolume) + '","volume":"' + str(volume) + '","change":"0","sign":"2","drate":"0",'
        '"mdchecnt":"' + str(mdchecnt) + '","mschecnt":"' + str(mschecnt) + '"}}'
    )


def _kis_raw(symbol: str, volume: int, cvolume: int, mdchecnt: int, mschecnt: int = 0) -> str:
    parts = [""] * KIS_TICK_MIN_FIELDS
    parts[0] = symbol
    parts[1] = "090000"
    parts[2] = "1000"
    parts[KIS_TICK_CVOLUME] = str(cvolume)
    parts[KIS_TICK_VOLUME] = str(volume)
    parts[KIS_TICK_MDCHECNT] = str(mdchecnt)
    parts[KIS_TICK_MSCHECNT] = str(mschecnt)
    return "^".join(parts)


def _frame(
    vendor: str,
    raw: str,
    *,
    at_s: float = 0.0,
    conn: str = "c1",
    symbol: str = "005930",
    stream: str = "H0STCNT0",
) -> L0Frame:
    return L0Frame(vendor, stream, symbol, raw, 1, _BASE_NS + int(at_s * _SEC_NS), 1, conn)


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def _watch(vendor: str = "ls", clock: _Clock | None = None) -> tuple[TradeContinuityWatch, _Clock]:
    clock = clock or _Clock()
    return TradeContinuityWatch(vendor=vendor, report_interval_s=60.0, monotonic=clock), clock


def test_continuous_trades_never_report(caplog) -> None:
    watch, clock = _watch()
    caplog.set_level(logging.WARNING)
    volume = 100
    for step in range(5):
        watch.observe(_frame("ls", _ls_raw("005930", volume, 10, step + 1), at_s=step))
        volume += 10
    clock.now = 600.0
    watch.observe(_frame("ls", _ls_raw("005930", volume, 10, 6), at_s=6))
    assert watch.loss_events == 0
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


def test_illiquid_silence_never_reports() -> None:
    watch, _ = _watch()
    watch.observe(_frame("ls", _ls_raw("005930", 100, 10, 1), at_s=0))
    watch.observe(_frame("ls", _ls_raw("005930", 110, 10, 2), at_s=1800))
    assert watch.loss_events == 0


def test_missed_trade_is_proven_at_next_trade(caplog) -> None:
    watch, clock = _watch()
    caplog.set_level(logging.WARNING)
    watch.observe(_frame("ls", _ls_raw("005930", 100, 10, 1), at_s=0))
    watch.observe(_frame("ls", _ls_raw("005930", 130, 10, 2), at_s=1))
    assert watch.loss_events == 1
    assert watch.lost_volume == 20
    assert not caplog.records
    clock.now = 60.0
    watch.observe(_frame("ls", _ls_raw("005930", 140, 10, 3), at_s=2))
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "005930:20" in warnings[0]
    assert "stage=trade_continuity status=LOSS" in warnings[0]


def test_batched_message_is_not_loss() -> None:
    watch, _ = _watch()
    watch.observe(_frame("ls", _ls_raw("005930", 100, 10, 1), at_s=0))
    watch.observe(_frame("ls", _ls_raw("005930", 130, 10, 4), at_s=1))
    assert watch.loss_events == 0


def test_regression_resets_baseline() -> None:
    watch, _ = _watch()
    watch.observe(_frame("ls", _ls_raw("005930", 500, 10, 1), at_s=0))
    watch.observe(_frame("ls", _ls_raw("005930", 100, 10, 2), at_s=1))
    assert watch.regressions == 1
    watch.observe(_frame("ls", _ls_raw("005930", 110, 10, 3), at_s=2))
    assert watch.loss_events == 0


def test_connections_are_separate_series() -> None:
    watch, _ = _watch()
    watch.observe(_frame("ls", _ls_raw("005930", 100, 10, 1), at_s=0, conn="primary"))
    watch.observe(_frame("ls", _ls_raw("005930", 100, 10, 1), at_s=0, conn="standby"))
    watch.observe(_frame("ls", _ls_raw("005930", 110, 10, 2), at_s=1, conn="primary"))
    watch.observe(_frame("ls", _ls_raw("005930", 110, 10, 2), at_s=1, conn="standby"))
    assert watch.loss_events == 0


def test_kis_caret_frames_parse_with_quality_indices() -> None:
    watch, _ = _watch(vendor="kis")
    watch.observe(_frame("kis", _kis_raw("005930", 100, 10, 1), at_s=0, stream="H0NXCNT0"))
    watch.observe(_frame("kis", _kis_raw("005930", 130, 10, 2), at_s=1, stream="H0NXCNT0"))
    assert watch.loss_events == 1
    assert watch.lost_volume == 20


def test_non_trade_and_malformed_frames_are_ignored() -> None:
    watch, _ = _watch()
    watch.observe(_frame("ls", _ls_raw("005930", 100, 10, 1), at_s=0))
    watch.observe(_frame("ls", "{}", at_s=1, stream="H0STASP0"))
    watch.observe(_frame("ls", "not json", at_s=2))
    watch.observe(_frame("ls", '{"body":{"volume":"x"}}', at_s=3))
    watch.observe(_frame("ls", "[1,2]", at_s=4))
    kis = TradeContinuityWatch(vendor="kis", monotonic=_Clock())
    kis.observe(_frame("kis", "short^record", at_s=0))
    kis.observe(_frame("kis", "^".join(["x"] * KIS_TICK_MIN_FIELDS), at_s=1))
    assert watch.loss_events == 0
    assert watch.regressions == 0
    assert kis.loss_events == 0


def test_eod_parity_on_frames() -> None:
    rows = [
        (0, _ls_raw("005930", 100, 10, 1)),
        (1, _ls_raw("005930", 130, 10, 2)),
        (2, _ls_raw("005930", 135, 5, 3)),
        (3, _ls_raw("005930", 160, 10, 6)),
        (4, _ls_raw("005930", 170, 10, 7)),
    ]
    watch, _ = _watch()
    for at_s, raw in rows:
        watch.observe(_frame("ls", raw, at_s=at_s))
    chunk = pl.DataFrame({"raw": [raw for _, raw in rows], "recv_wall_ns": [_BASE_NS + at_s * _SEC_NS for at_s, _ in rows]})
    summary = _summarize_tick_fields(
        decode_ls_ticks(chunk),
        rows=len(rows),
        settings=DataQualitySettings(),
        listing_symbols=frozenset(),
    )
    assert watch.loss_events == summary.tick_loss
    assert watch.lost_volume == summary.lost_volume


def test_unsupported_vendor_rejected() -> None:
    with pytest.raises(ValueError, match="unsupported"):
        TradeContinuityWatch(vendor="db")


def test_observe_never_raises_on_broken_clock() -> None:
    def _broken() -> float:
        raise RuntimeError("clock")

    watch = TradeContinuityWatch(vendor="ls", monotonic=lambda: 0.0)
    watch._monotonic = _broken  # noqa: SLF001 - guard: observe swallows failures from the clock path
    watch.observe(_frame("ls", _ls_raw("005930", 100, 10, 1), at_s=0))
    watch.observe(_frame("ls", _ls_raw("005930", 130, 10, 2), at_s=1))


def test_non_positive_report_interval_rejected() -> None:
    with pytest.raises(ValueError, match="positive"):
        TradeContinuityWatch(vendor="ls", report_interval_s=0.0)
