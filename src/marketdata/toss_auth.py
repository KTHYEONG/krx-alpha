"""Toss OpenAPI response classification shared by every Toss endpoint client."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import requests

# 연결 리셋/타임아웃은 벤더 측 일시 장애로 재시도하면 대개 회복된다(실측: 2026-09-17
# 전체 백필 중 ConnectionResetError 4건 전량 재실행으로 복구). 4xx 는 같은 요청을 반복해도
# 결과가 같고(실측: 상장폐지 종목 404), 401 은 토큰 회전으로만 회복되므로 재시도하지 않는다.
TOSS_TRANSIENT_EXCEPTIONS: tuple[type[Exception], ...] = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ChunkedEncodingError,
)


def http_status_code(exc: BaseException) -> int | None:
    status = getattr(getattr(exc, "response", None), "status_code", None)
    return status if isinstance(status, int) else None


def is_client_error(exc: BaseException) -> bool:
    """Return True for an HTTP 4xx response, which a plain resend cannot fix."""
    status = http_status_code(exc)
    return isinstance(exc, requests.HTTPError) and status is not None and 400 <= status < 500


def is_auth_rejection(exc: BaseException) -> bool:
    return isinstance(exc, requests.HTTPError) and http_status_code(exc) == 401


def is_invalid_token_envelope(body: Any) -> bool:
    if not isinstance(body, Mapping):
        return False
    error = body.get("error")
    if isinstance(error, Mapping):
        return str(error.get("code", "")) == "invalid-token"
    return str(body.get("code", "")) == "invalid-token"
