"""Toss response classification contract."""

from __future__ import annotations

import pytest
import requests

from src.marketdata.toss_auth import is_auth_rejection, is_client_error, is_invalid_token_envelope


def _http_error(status: int | None) -> requests.HTTPError:
    err = requests.HTTPError(f"{status}")
    err.response = type("R", (), {"status_code": status})()
    return err


@pytest.mark.parametrize(
    ("exc", "client", "auth"),
    [
        (_http_error(401), True, True),
        (_http_error(404), True, False),
        (_http_error(500), False, False),
        (_http_error(None), False, False),
        (requests.ConnectionError("reset"), False, False),
    ],
)
def test_http_error_classification(exc, client, auth) -> None:
    assert is_client_error(exc) is client
    assert is_auth_rejection(exc) is auth


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        ({"error": {"code": "invalid-token"}}, True),
        ({"code": "invalid-token"}, True),
        ({"error": {"code": "other"}}, False),
        (["invalid-token"], False),
        (None, False),
    ],
)
def test_invalid_token_envelope(body, expected) -> None:
    assert is_invalid_token_envelope(body) is expected
