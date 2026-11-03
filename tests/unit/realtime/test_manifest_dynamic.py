from __future__ import annotations

import datetime as dt

from src.realtime.manifest import SessionManifest


def _manifest() -> SessionManifest:
    return SessionManifest(
        session_date=dt.date(2026, 11, 3),
        clock_offset_ns=0,
        started_at_ns=1,
        clock_status="measured",
        venue="KRX",
        session="REGULAR",
        expected_close_ns=0,
    )


def test_static_ack_row_has_no_dynamic_marker() -> None:
    manifest = _manifest()

    manifest.record_ack(vendor="ls", tr_id="H0STCNT0", symbol="005930", rt_cd="00000", accepted=True)

    assert manifest.subscription_acks == [
        {"vendor": "ls", "tr_id": "H0STCNT0", "symbol": "005930", "rt_cd": "00000", "accepted": True, "acked_at_ns": None}
    ]


def test_dynamic_ack_row_is_marked_dynamic() -> None:
    manifest = _manifest()

    manifest.record_ack(
        vendor="ls", tr_id="H0STCNT0", symbol="000660", rt_cd="00000", accepted=True, acked_at_ns=7, dynamic=True
    )

    assert manifest.subscription_acks[-1]["dynamic"] is True
    assert manifest.subscription_acks[-1]["acked_at_ns"] == 7
