"""KIS app authentication and per-key token cache provider."""

from __future__ import annotations

import datetime as dt
import fcntl
import hashlib
import json
import logging
import os
import pathlib
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from enum import StrEnum
from typing import Any
from zoneinfo import ZoneInfo

from src.brokers.kis.rate import RateLimiter
from src.execution.contracts import KisApiError

logger = logging.getLogger(__name__)

_PATH_TOKEN = "/oauth2/tokenP"  # noqa: S105 - public endpoint, not a secret
_TOKEN_REFRESH_MARGIN: dt.timedelta = dt.timedelta(minutes=10)
# KIS는 앱키당 1분 1회 초과 발급을 거부한다(EGW00133). 백오프를 해당 창보다
# 약간 길게 잡아 거부된 키가 매 REST 호출마다 발급 시도로 번지지 않게 한다.
TOKEN_ISSUE_RETRY_BACKOFF: dt.timedelta = dt.timedelta(seconds=65)
_KST: dt.tzinfo = ZoneInfo("Asia/Seoul")


class TokenSource(StrEnum):
    CACHE = "cache"
    ISSUED = "issued"


@dataclass(frozen=True)
class KisAppAuth:
    """KIS app key pair used for token issuance and request signing."""

    app_key: str
    app_secret: str


def kis_app_key_fingerprint(app_key: str) -> str:
    """앱키 지문 (토큰 캐시 파일명·WS lease와 공유하는 규칙)."""
    return hashlib.sha256(app_key.encode("utf-8")).hexdigest()[:12]


def kis_token_cache_path(cache_dir: pathlib.Path, app_key: str) -> pathlib.Path:
    """앱키별 canonical 토큰 캐시 경로 (token_<sha256(app_key)[:12]>.json)."""
    return pathlib.Path(cache_dir) / f"token_{kis_app_key_fingerprint(app_key)}.json"


_TOKEN_KEY_LOCKS: dict[str, threading.Lock] = {}
_TOKEN_KEY_LOCKS_GUARD = threading.Lock()


def _lock_for_token_cache(path: pathlib.Path) -> threading.Lock:
    with _TOKEN_KEY_LOCKS_GUARD:
        return _TOKEN_KEY_LOCKS.setdefault(str(path), threading.Lock())


@contextmanager
def _token_file_lock(path: pathlib.Path) -> Iterator[None]:
    """프로세스 간 토큰 발급을 직렬화하는 advisory lock."""
    lock_path = pathlib.Path(f"{path}.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = open(lock_path, "a+b")  # noqa: SIM115 - lock handle은 context 수명 동안 유지
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


class KisTokenProvider:
    """Per-key KIS token cache with issuance guards (thread- and process-safe)."""

    def __init__(
        self,
        *,
        auth: KisAppAuth,
        session: Any,
        cache_path: pathlib.Path,
        limiter: RateLimiter,
        now: Callable[[], dt.datetime],
        timeout_s: float,
        base_url: str,
        allow_issue: bool,
    ) -> None:
        self._auth = auth
        self._session = session
        self._token_cache_path = cache_path
        self._limiter = limiter
        self._now = now
        self._timeout_s = timeout_s
        self._base_url = base_url
        self._allow_token_issue = allow_issue
        self._token: str | None = None
        self._token_expires_at: dt.datetime | None = None
        self._token_issue_failed_at: dt.datetime | None = None

    def _read_valid_token(self, now: dt.datetime) -> tuple[str, dt.datetime] | None:
        """유효한 캐시 토큰을 반환하고 무효·만료 임박분은 None으로 fail-closed 처리한다."""
        try:
            cached = json.loads(self._token_cache_path.read_text(encoding="utf-8"))
            expires_at = dt.datetime.fromisoformat(
                str(cached.get("expired_at") or cached.get("expires_at"))
            )
            if cached.get("app_key") != self._auth.app_key:
                return None
            token = cached.get("access_token")
            if not isinstance(token, str) or not token:
                return None
            if expires_at - now <= _TOKEN_REFRESH_MARGIN:
                return None
            return (token, expires_at)
        except (OSError, ValueError, KeyError, TypeError, AttributeError):
            return None

    def _cached_issue_day(self) -> dt.date | None:
        """캐시에 기록된 발급일(KST)을 반환한다 (legacy 캐시는 None)."""
        try:
            cached = json.loads(self._token_cache_path.read_text(encoding="utf-8"))
            return dt.datetime.fromisoformat(str(cached.get("issued_at", ""))).date()
        except (OSError, ValueError, KeyError, TypeError, AttributeError):
            return None

    def _backoff_active(self, now: dt.datetime) -> bool:
        failed_at = self._token_issue_failed_at
        return failed_at is not None and now < failed_at + TOKEN_ISSUE_RETRY_BACKOFF

    def _inherit_cache_dir_owner(self, *targets: pathlib.Path) -> None:
        if os.geteuid() != 0:
            return
        owner = self._token_cache_path.parent.stat()
        for target in targets:
            os.chown(str(target), owner.st_uid, owner.st_gid)

    def ensure_token(self) -> TokenSource:
        """Ensure a valid token exists under the per-key cache and issuance guards.

        Returns:
            CACHE for reusable in-memory or file tokens; ISSUED after a new grant.

        Raises:
            KisApiError: For missing cache, active backoff, daily issuance guard,
                transport failure, or vendor rejection.
        """
        now = self._now()
        if (
            self._token is not None
            and self._token_expires_at is not None
            and self._token_expires_at - now > _TOKEN_REFRESH_MARGIN
        ):
            return TokenSource.CACHE
        hit = self._read_valid_token(now)
        if hit is not None:
            self._token, self._token_expires_at = hit
            return TokenSource.CACHE
        if not self._allow_token_issue:
            raise KisApiError("TOKEN_CACHE", "token cache missing/expired and issuance disabled")
        if self._backoff_active(now):
            raise KisApiError("TOKEN_BACKOFF", "token issuance failed recently; backing off")
        _, source = self._issue_token_and_report(now, reuse_valid=True)
        return source

    def access_token(self, *, force: bool = False) -> str:
        """Return a usable token while retaining lock, expiry, and refresh semantics."""
        now = self._now()
        if (
            not force
            and self._token is not None
            and self._token_expires_at is not None
            and self._token_expires_at - now > _TOKEN_REFRESH_MARGIN
        ):
            return self._token
        if not force:
            hit = self._read_valid_token(now)
            if hit is not None:
                self._token, self._token_expires_at = hit
                return self._token
        if not self._allow_token_issue:
            raise KisApiError("TOKEN_CACHE", "token cache missing/expired and issuance disabled")
        if self._backoff_active(now):
            hit = self._read_valid_token(now)
            if hit is not None:
                self._token, self._token_expires_at = hit
                return self._token
            raise KisApiError("TOKEN_BACKOFF", "token issuance failed recently; backing off")
        return self._issue_token(now, reuse_valid=not force)

    def _issue_token(self, now: dt.datetime, *, reuse_valid: bool) -> str:
        """per-key lock으로 캐시를 재검사한 뒤 atomic 0600 write로 발급한다 (당일 재발급은 force도 거부)."""
        token, _ = self._issue_token_and_report(now, reuse_valid=reuse_valid)
        return token

    def _issue_token_and_report(self, now: dt.datetime, *, reuse_valid: bool) -> tuple[str, TokenSource]:
        with _token_file_lock(self._token_cache_path), _lock_for_token_cache(self._token_cache_path):
            cached = self._read_valid_token(now) if reuse_valid else None
            if cached is not None:
                self._token, self._token_expires_at = cached
                return self._token, TokenSource.CACHE
            if self._cached_issue_day() == now.astimezone(_KST).date():
                raise KisApiError("TOKEN_DAILY_LIMIT", "token already issued today (KST)")
            return self._issue_token_unlocked(now), TokenSource.ISSUED

    def _fail_issue(self, now: dt.datetime, reason: str) -> None:
        self._token_issue_failed_at = now
        logger.warning(
            "[EXEC] stage=token_issue status=FAIL key_id=%s reason=%s",
            kis_app_key_fingerprint(self._auth.app_key),
            reason,
        )

    def _issue_token_unlocked(self, now: dt.datetime) -> str:
        self._limiter.acquire()
        try:
            resp = self._session.post(
                self._base_url + _PATH_TOKEN,
                json={
                    "grant_type": "client_credentials",
                    "appkey": self._auth.app_key,
                    "appsecret": self._auth.app_secret,
                },
                timeout=self._timeout_s,
            )
        except Exception as exc:  # noqa: BLE001 - 전송 실패도 백오프 대상이다
            self._fail_issue(now, type(exc).__name__)
            raise KisApiError("TRANSPORT", type(exc).__name__) from exc
        try:
            body = resp.json()
        except ValueError as exc:
            self._fail_issue(now, "TOKEN")
            raise KisApiError("TOKEN", "non_json_token_response") from exc
        if "access_token" not in body:
            code = str(body.get("error_code", "TOKEN"))
            self._fail_issue(now, code)
            raise KisApiError(code, str(body.get("error_description", "")))
        expires_at = dt.datetime.strptime(
            str(body["access_token_token_expired"]), "%Y-%m-%d %H:%M:%S"
        ).replace(tzinfo=_KST)
        self._token = str(body["access_token"])
        self._token_expires_at = expires_at
        payload = json.dumps(
            {
                "access_token": self._token,
                "expired_at": expires_at.isoformat(),
                "app_key": self._auth.app_key,
                "issued_at": now.isoformat(),
            }
        )
        self._token_cache_path.parent.mkdir(parents=True, exist_ok=True)
        lock_path = pathlib.Path(f"{self._token_cache_path}.lock")
        tmp_path = self._token_cache_path.parent / (self._token_cache_path.name + ".tmp")
        fd = os.open(str(tmp_path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(payload)
        os.chmod(str(tmp_path), 0o600)
        self._inherit_cache_dir_owner(tmp_path, lock_path)
        os.replace(str(tmp_path), str(self._token_cache_path))
        self._token_issue_failed_at = None
        logger.info(
            "[EXEC] stage=token_issued key_id=%s expires_at=%s",
            kis_app_key_fingerprint(self._auth.app_key),
            expires_at.isoformat(),
        )
        return self._token
