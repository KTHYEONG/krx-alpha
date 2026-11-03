from __future__ import annotations

import logging

import pytest

from src.universe.status_policy import WarningLevel, classify_security_status


def _status(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "status_code": "00",
        "managed": False,
        "market_warning_code": "00",
        "short_overheated": False,
        "liquidation_trading": False,
        "trading_halted": False,
        "credit_available": True,
    }
    row.update(overrides)
    return row


def test_short_overheated_is_excluded() -> None:
    verdict = classify_security_status(_status(short_overheated=True, status_code="59"))

    assert verdict.exclude is True
    assert "short_overheated" in verdict.flags


def test_investment_warning_is_kept_and_tagged() -> None:
    verdict = classify_security_status(_status(market_warning_code="02"))

    assert verdict.exclude is False
    assert verdict.warning_level is WarningLevel.WARNING


def test_unknown_warning_code_is_kept_as_unknown(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger="src.universe.status_policy"):
        verdict = classify_security_status(_status(market_warning_code="07"))

    assert verdict.exclude is False
    assert verdict.warning_level is WarningLevel.UNKNOWN
    warnings = [rec for rec in caplog.records if rec.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "status=UNMAPPED" in warnings[0].message


def test_margin_and_credit_are_labels_only() -> None:
    verdict = classify_security_status(_status(status_code="57", credit_available=False))

    assert verdict.exclude is False
    assert verdict.flags == ("credit_unavailable", "margin_100")


@pytest.mark.parametrize(
    "flag_key",
    ["managed", "trading_halted", "liquidation_trading"],
)
def test_managed_halted_and_liquidation_are_excluded(flag_key: str) -> None:
    verdict = classify_security_status(_status(**{flag_key: True}))

    assert verdict.exclude is True


def test_missing_warning_code_maps_to_unknown() -> None:
    verdict = classify_security_status({"managed": False})

    assert verdict.exclude is False
    assert verdict.warning_level is WarningLevel.UNKNOWN
