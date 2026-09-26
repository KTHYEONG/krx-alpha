"""healthchecks.io lifecycle pings (external dead-man's switch)."""

from __future__ import annotations

import logging
import uuid
from enum import StrEnum
from typing import Any

import requests

logger = logging.getLogger(__name__)

_MAX_REASON_CHARS: int = 1000


class HealthcheckSignal(StrEnum):
    START = "start"
    SUCCESS = "success"
    FAIL = "fail"


class HealthcheckPinger:
    """Send healthchecks.io lifecycle pings without ever affecting the caller.

    The external service alerts when pings stop (hang, crash loop, host down)
    or when an explicit ``fail`` arrives, which covers the failure modes the
    in-process email alerter cannot report about itself.
    """

    def __init__(self, url: str, *, timeout_s: float, run_id: str, session: Any | None = None) -> None:
        self._base = url.rstrip("/")
        self._timeout_s = timeout_s
        self._rid = str(uuid.uuid5(uuid.NAMESPACE_URL, run_id))
        self._session = session if session is not None else requests
        self._last_ok: bool | None = None

    def start(self) -> bool:
        return self._ping(HealthcheckSignal.START)

    def success(self) -> bool:
        return self._ping(HealthcheckSignal.SUCCESS)

    def fail(self, reason: str) -> bool:
        return self._ping(HealthcheckSignal.FAIL, reason)

    def _ping(self, signal: HealthcheckSignal, reason: str | None = None) -> bool:
        suffix = {HealthcheckSignal.START: "/start", HealthcheckSignal.SUCCESS: "", HealthcheckSignal.FAIL: "/fail"}[
            signal
        ]
        body = reason[:_MAX_REASON_CHARS].encode("utf-8") if reason is not None else None
        try:
            response = self._session.post(
                f"{self._base}{suffix}",
                params={"rid": self._rid},
                data=body,
                timeout=self._timeout_s,
            )
            ok = 200 <= int(response.status_code) < 300
            detail = "" if ok else str(response.status_code)
        except Exception as exc:
            ok = False
            detail = type(exc).__name__
        if ok:
            if self._last_ok is False:
                logger.info("[SYS] stage=healthcheck status=RECOVERED signal=%s", signal.value)
        elif self._last_ok is not False:
            logger.warning(
                "[SYS] stage=healthcheck status=FAIL signal=%s reason=%s", signal.value, detail
            )
        self._last_ok = ok
        return ok


class NoopPinger:
    """No-network stand-in with the ``HealthcheckPinger`` call interface."""

    def start(self) -> bool:
        return False

    def success(self) -> bool:
        return False

    def fail(self, reason: str) -> bool:
        return False
