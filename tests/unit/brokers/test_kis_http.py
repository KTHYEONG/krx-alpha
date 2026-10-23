"""Transport retry invariants for KisGetTransport (connection/timeout only)."""

import logging

import pytest
import requests

from src.brokers.kis.auth import KisAppAuth
from src.brokers.kis.http import MAX_TRANSPORT_RETRIES, KisGetTransport
from src.execution.contracts import KisApiError
from tests.unit.execution.fakes import FakeResponse, FakeSession


class _Tokens:
    def access_token(self, *, force: bool = False, rejected_token: str | None = None) -> str:
        return "tok"


class _Limiter:
    def __init__(self) -> None:
        self.acquired = 0

    def acquire(self) -> None:
        self.acquired += 1


def _transport(session, sleeps: list) -> tuple[KisGetTransport, _Limiter]:
    limiter = _Limiter()
    transport = KisGetTransport(
        auth=KisAppAuth(app_key="k", app_secret="s"),
        tokens=_Tokens(),  # type: ignore[arg-type]
        session=session,
        limiter=limiter,  # type: ignore[arg-type]
        timeout_s=5.0,
        base_url="https://test",
        retry_backoff_s=0.0,
        sleep=sleeps.append,
    )
    return transport, limiter


def _ok() -> FakeResponse:
    return FakeResponse({"rt_cd": "0", "msg_cd": "MCA00000", "msg1": "ok"}, headers={"tr_cont": "D"})


@pytest.mark.parametrize("error", [requests.ConnectionError, requests.Timeout])
def test_transport_retry_succeeds(caplog, error) -> None:
    session = FakeSession([error("sensitive detail"), _ok()])
    transport, limiter = _transport(session, sleeps := [])
    with caplog.at_level(logging.WARNING, logger="src.brokers.kis.http"):
        body, cont = transport.get("/p", "TR", {})
    assert body["rt_cd"] == "0"
    assert cont == "D"
    assert limiter.acquired == 2
    assert len(sleeps) == 1
    assert sum("stage=kis_transport_retry" in r.message for r in caplog.records) == 1
    assert caplog.records[-1].message == f"[DATA] stage=kis_transport_retry tr_id=TR attempt=1 error={error.__name__}"


def test_transport_retry_bounded() -> None:
    session = FakeSession([requests.ConnectionError("d")] * (MAX_TRANSPORT_RETRIES + 2))
    transport, limiter = _transport(session, [])
    with pytest.raises(KisApiError) as excinfo:
        transport.get("/p", "TR", {})
    assert excinfo.value.msg_cd == "TRANSPORT"
    assert excinfo.value.message == "ConnectionError"
    assert len([c for c in session.calls if c["method"] == "GET"]) == MAX_TRANSPORT_RETRIES + 1
    assert limiter.acquired == MAX_TRANSPORT_RETRIES + 1


def test_non_transport_errors_not_retried() -> None:
    session = FakeSession([FakeResponse(None, status=502, raw_text=True)])
    transport, _ = _transport(session, [])
    with pytest.raises(KisApiError):
        transport.get("/p", "TR", {})
    assert len(session.calls) == 1

    err = FakeResponse({"rt_cd": "1", "msg_cd": "APBK1234", "msg1": "bad"})
    session = FakeSession([err])
    transport, _ = _transport(session, [])
    with pytest.raises(KisApiError) as excinfo:
        transport.get("/p", "TR", {})
    assert excinfo.value.msg_cd == "APBK1234"
    assert len(session.calls) == 1


def test_rate_limit_retry_independent_of_transport_retry() -> None:
    rate = FakeResponse({"rt_cd": "1", "msg_cd": "EGW00201", "msg1": "limited"})
    session = FakeSession([rate, requests.ConnectionError("down"), rate, requests.ConnectionError("down"), _ok()])
    transport, limiter = _transport(session, [])
    body = transport.get("/p", "TR", {})[0]
    assert body["rt_cd"] == "0"
    assert limiter.acquired == 5


def test_token_connection_error_is_not_retried() -> None:
    from unittest.mock import Mock

    session = FakeSession([])
    transport, limiter = _transport(session, sleeps := [])
    tokens = Mock()
    tokens.access_token.side_effect = requests.ConnectionError("token POST failed")
    transport._tokens = tokens
    with pytest.raises(KisApiError, match="TRANSPORT: ConnectionError"):
        transport.get("/p", "TR", {})
    assert tokens.access_token.call_count == 1
    assert limiter.acquired == 1
    assert session.calls == []
    assert sleeps == []
