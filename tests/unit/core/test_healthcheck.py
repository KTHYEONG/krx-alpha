def _fake_session(*, status_code=200, exc=None):
    calls = []

    class _Response:
        def __init__(self, code):
            self.status_code = code

    class _Session:
        def post(self, url, *, params=None, data=None, timeout=None):
            calls.append({"url": url, "params": params, "data": data, "timeout": timeout})
            if exc is not None:
                raise exc
            return _Response(status_code)

    return _Session(), calls


def test_signals_hit_endpoints_with_shared_run_id() -> None:
    from src.core.healthcheck import HealthcheckPinger

    session, calls = _fake_session()
    pinger = HealthcheckPinger(
        "https://hc.example.com/ping/token-1", timeout_s=5.0, run_id="run-7", session=session
    )

    assert pinger.start() is True
    assert pinger.success() is True
    assert pinger.fail("boom") is True

    assert calls[0]["url"] == "https://hc.example.com/ping/token-1/start"
    assert calls[1]["url"] == "https://hc.example.com/ping/token-1"
    assert calls[2]["url"] == "https://hc.example.com/ping/token-1/fail"
    assert calls[0]["data"] is None
    assert calls[1]["data"] is None
    assert calls[2]["data"] == b"boom"
    rids = {c["params"]["rid"] for c in calls}
    assert len(rids) == 1
    assert calls[0]["timeout"] == 5.0

    other, other_calls = _fake_session()
    other_pinger = HealthcheckPinger(
        "https://hc.example.com/ping/token-1", timeout_s=5.0, run_id="run-8", session=other
    )
    other_pinger.success()

    assert other_calls[0]["params"]["rid"] != calls[0]["params"]["rid"]


def test_fail_sends_bounded_reason_body() -> None:
    from src.core.healthcheck import HealthcheckPinger

    session, calls = _fake_session()
    pinger = HealthcheckPinger(
        "https://hc.example.com/ping/token-1", timeout_s=5.0, run_id="run-7", session=session
    )

    assert pinger.fail("x" * 5000) is True

    assert len(calls[0]["data"].decode("utf-8")) <= 1000


def test_network_error_never_raises_and_warns_once(caplog) -> None:
    import logging

    from src.core.healthcheck import HealthcheckPinger

    session, _ = _fake_session(exc=ConnectionError("down"))
    pinger = HealthcheckPinger(
        "https://hc.example.com/ping/token-1", timeout_s=5.0, run_id="run-7", session=session
    )

    with caplog.at_level(logging.WARNING):
        assert pinger.success() is False
        assert pinger.success() is False

    failures = [r for r in caplog.records if "stage=healthcheck status=FAIL" in r.getMessage()]
    assert len(failures) == 1
    assert "reason=ConnectionError" in failures[0].getMessage()


def test_recovery_logged_once(caplog) -> None:
    import logging

    from src.core.healthcheck import HealthcheckPinger

    calls: list[str] = []

    class _Flaky:
        def post(self, url, *, params=None, data=None, timeout=None):
            calls.append(url)
            if len(calls) == 1:
                raise ConnectionError("down")

            class _Response:
                status_code = 200

            return _Response()

    pinger = HealthcheckPinger(
        "https://hc.example.com/ping/token-1", timeout_s=5.0, run_id="run-7", session=_Flaky()
    )

    with caplog.at_level(logging.INFO):
        assert pinger.success() is False
        assert pinger.success() is True

    recovered = [r for r in caplog.records if "status=RECOVERED" in r.getMessage()]
    assert len(recovered) == 1
    assert recovered[0].levelno == logging.INFO


def test_url_never_logged(caplog) -> None:
    import logging

    from src.core.healthcheck import HealthcheckPinger

    url = "https://hc.example.com/ping/secret-token-xyz"
    session, _ = _fake_session(status_code=500)
    pinger = HealthcheckPinger(url, timeout_s=5.0, run_id="run-7", session=session)

    with caplog.at_level(logging.WARNING):
        assert pinger.success() is False

    assert "secret-token-xyz" not in caplog.text
    assert "reason=500" in caplog.text


def test_noop_pinger_never_touches_network() -> None:
    from src.core.healthcheck import NoopPinger

    pinger = NoopPinger()

    assert pinger.start() is False
    assert pinger.success() is False
    assert pinger.fail("anything") is False
