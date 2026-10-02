"""KIS REST stack factory invariant guards (P4)."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import pathlib
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")
T0 = dt.datetime(2026, 9, 11, 9, 0, 0, tzinfo=KST)


def _auth(key: str = "app-key"):
    from src.brokers.kis.auth import KisAppAuth

    return KisAppAuth(app_key=key, app_secret="app-secret")


class _FakeResponse:
    def __init__(self, body, *, headers=None) -> None:
        self._body = body
        self.headers = headers or {}
        self.status_code = 200

    def json(self):
        return self._body


class _FakeSession:
    def __init__(self, script: list) -> None:
        self.script = list(script)
        self.calls: list[dict] = []

    def get(self, url: str, **kwargs):
        self.calls.append({"method": "GET", "url": url, **kwargs})
        item = self.script.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item

    def post(self, url: str, **kwargs):
        self.calls.append({"method": "POST", "url": url, **kwargs})
        item = self.script.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item


def _ok_body():
    return _FakeResponse(
        {"rt_cd": "0", "msg_cd": "MCA00000", "msg1": "ok", "output1": []},
        headers={"tr_cont": "D"},
    )


def test_protocol_paths_unchanged(tmp_path) -> None:
    from src.brokers.kis.stack import build_kis_rest_stack

    cache = tmp_path / "cache"
    cache.mkdir(parents=True)
    (cache / ".host-admission").touch()
    stack = build_kis_rest_stack(
        auth=_auth("k"),
        cache_dir=cache,
        session=_FakeSession([]),
        now=lambda: T0,
        rate_per_s=18.0,
        max_lead_s=1.0,
        timeout_s=5.0,
        allow_issue=False,
    )
    digest = hashlib.sha256(b"k").hexdigest()[:12]
    assert stack.tokens._token_cache_path == cache / f"token_{digest}.json"
    assert stack.limiter._state_path == cache / f"tps_{digest}.state"


def test_single_pacer_and_provider(tmp_path) -> None:
    from src.brokers.kis.stack import build_kis_rest_stack
    from src.core.config import KisCredentials

    cache = tmp_path / "cache"
    cache.mkdir(parents=True)
    (cache / ".host-admission").touch()
    session = _FakeSession([])
    stack = build_kis_rest_stack(
        auth=_auth("app-key"),
        cache_dir=cache,
        session=session,
        now=lambda: T0,
        rate_per_s=18.0,
        max_lead_s=1.0,
        timeout_s=5.0,
        allow_issue=False,
    )
    assert stack.transport._limiter is stack.limiter
    assert stack.transport._tokens is stack.tokens
    assert stack.data._transport is stack.transport
    creds = KisCredentials(
        kis_app_key="app-key",
        kis_app_secret="app-secret",
        kis_account_no="12345678",
        kis_account_product_code="01",
    )
    trading = stack.trading_client(creds)
    assert trading._transport is stack.transport
    assert trading._limiter is stack.limiter
    assert trading._session is session
    assert trading._timeout_s == 5.0
    assert trading._base_url == stack.base_url


def test_issuance_shares_pacer(tmp_path) -> None:
    from src.brokers.kis.stack import build_kis_rest_stack

    cache = tmp_path / "cache"
    cache.mkdir(parents=True)
    (cache / ".host-admission").touch()
    t0 = T0.timestamp()
    sleeps: list[float] = []
    session = _FakeSession(
        [
            _FakeResponse(
                {
                    "access_token": "tok-new",
                    "access_token_token_expired": "2026-09-12 09:00:00",
                }
            ),
            _ok_body(),
        ]
    )
    stack = build_kis_rest_stack(
        auth=_auth("app-key"),
        cache_dir=cache,
        session=session,
        now=lambda: T0,
        rate_per_s=10.0,
        max_lead_s=None,
        timeout_s=5.0,
        allow_issue=True,
        clock=lambda: t0,
        sleep=sleeps.append,
    )
    stack.data._transport.get("/uapi/test", "TR", {})
    import pytest

    state = float((stack.limiter._state_path).read_text(encoding="utf-8").strip())
    assert state == pytest.approx(t0 + 2 / 10.0)
    assert sleeps == pytest.approx([1 / 10.0])


def test_no_env_read(tmp_path, monkeypatch) -> None:
    from src.brokers.kis.stack import build_kis_rest_stack

    elsewhere = tmp_path / "elsewhere"
    monkeypatch.setenv("KRX_ALPHA_KIS_TOKEN_CACHE_DIR", str(elsewhere))
    cache = tmp_path / "cache"
    cache.mkdir(parents=True)
    (cache / ".host-admission").touch()
    token_path = cache / f"token_{hashlib.sha256(b'app-key').hexdigest()[:12]}.json"
    token_path.write_text(
        json.dumps(
            {
                "access_token": "tok-1",
                "expired_at": (T0 + dt.timedelta(days=1)).isoformat(),
                "app_key": "app-key",
                "issued_at": (T0 - dt.timedelta(days=1)).isoformat(),
            }
        ),
        encoding="utf-8",
    )
    stack = build_kis_rest_stack(
        auth=_auth("app-key"),
        cache_dir=cache,
        session=_FakeSession([_ok_body()]),
        now=lambda: T0,
        rate_per_s=1000.0,
        max_lead_s=None,
        timeout_s=5.0,
        allow_issue=False,
    )
    stack.data._transport.get("/uapi/test", "TR", {})
    assert not elsewhere.exists()
    assert list(pathlib.Path(cache).glob("*")) != []


def test_invalid_rate_fails_closed(tmp_path) -> None:
    import pytest

    from src.brokers.kis.stack import build_kis_rest_stack

    cache = tmp_path / "cache"
    cache.mkdir(parents=True)
    with pytest.raises(ValueError, match="positive"):
        build_kis_rest_stack(
            auth=_auth("app-key"),
            cache_dir=cache,
            session=_FakeSession([]),
            now=lambda: T0,
            rate_per_s=0,
            max_lead_s=None,
            timeout_s=5.0,
            allow_issue=False,
        )
    assert list(cache.iterdir()) == []


def test_build_rejects_empty_app_key(tmp_path) -> None:
    # Given: an empty app key
    import pytest

    from src.brokers.kis.stack import build_kis_rest_stack

    # When / Then: no token or pacing file path is derived from an empty key
    cache_dir = tmp_path / "stack-cache"
    cache_dir.mkdir()
    with pytest.raises(ValueError, match="app_key"):
        build_kis_rest_stack(
            auth=_auth(""),
            cache_dir=cache_dir,
            session=_FakeSession([]),
            now=lambda: T0,
            rate_per_s=10.0,
            max_lead_s=None,
            timeout_s=5.0,
            allow_issue=False,
        )
    assert list(cache_dir.iterdir()) == []


def test_trading_client_rejects_foreign_app_key(tmp_path) -> None:
    # Given: a stack built for app-key
    import pytest

    from src.brokers.kis.stack import build_kis_rest_stack
    from src.core.config import KisCredentials

    stack = build_kis_rest_stack(
        auth=_auth(),
        cache_dir=tmp_path,
        session=_FakeSession([]),
        now=lambda: T0,
        rate_per_s=10.0,
        max_lead_s=None,
        timeout_s=5.0,
        allow_issue=False,
    )
    creds = KisCredentials(
        kis_app_key="other-key",
        kis_app_secret="other-secret",
        kis_account_no="12345678",
        kis_account_product_code="01",
    )

    # When / Then: an account client is never bound to another key's token/pacer
    with pytest.raises(ValueError, match="app key"):
        stack.trading_client(creds)


def _slot_credential(slot: str, key: str):
    from src.realtime.kis_sharding import KisDataCredential

    return KisDataCredential(slot=slot, app_key=key, app_secret=f"{key}-secret", hts_id="hts", key_id=f"kid-{slot}")


def test_data_slot_picks_configured_slot(tmp_path) -> None:
    # Given: 슬롯 1/2 가짜 자격증명과 스냅샷 슬롯 2 지정
    from src.brokers.kis.auth import kis_token_cache_path
    from src.brokers.kis.stack import build_kis_data_slot_stack
    from src.core.config import KisTokenSettings, SnapshotSettings

    cache = tmp_path / "cache"
    cache.mkdir(parents=True)
    (cache / ".host-admission").touch()
    token_settings = KisTokenSettings(token_cache_dir=cache, allow_issue=False)
    creds = (_slot_credential("1", "key-one"), _slot_credential("2", "key-two"))

    # When: 데이터 슬롯 스택을 조립한다
    stack, key_id = build_kis_data_slot_stack(
        snapshot=SnapshotSettings(kis_data_slot="2"),
        credentials=creds,
        token_settings=token_settings,
        session=_FakeSession([]),
        now=lambda: T0,
    )

    # Then: 슬롯 2 키로 구성되고 토큰 경로는 명시 설정 디렉터리를 따른다
    assert stack.app_key == "key-two"
    assert key_id == "kid-2"
    assert stack.tokens._token_cache_path == kis_token_cache_path(cache, "key-two")
    assert stack.tokens._allow_token_issue is token_settings.allow_issue


def test_data_slot_missing_fails_closed(tmp_path) -> None:
    # Given: 설정 슬롯에 해당하는 자격증명이 없음
    import pytest

    from src.brokers.kis.stack import build_kis_data_slot_stack
    from src.core.config import KisTokenSettings, SnapshotSettings
    from src.core.errors import MissingCredentialsError

    cache = tmp_path / "cache"
    cache.mkdir(parents=True)
    token_settings = KisTokenSettings(token_cache_dir=cache, allow_issue=False)

    # When / Then: 슬롯을 포함한 MissingCredentialsError, 파일 생성 없음
    with pytest.raises(MissingCredentialsError, match="9"):
        build_kis_data_slot_stack(
            snapshot=SnapshotSettings(kis_data_slot="9"),
            credentials=(_slot_credential("1", "key-one"),),
            token_settings=token_settings,
            session=_FakeSession([]),
            now=lambda: T0,
        )
    assert list(cache.iterdir()) == []


def test_data_slot_no_env_read(tmp_path, monkeypatch) -> None:
    # Given: 환경 토큰 캐시 디렉터리가 다른 곳을 가리킴
    from src.brokers.kis.stack import build_kis_data_slot_stack
    from src.core.config import KisTokenSettings, SnapshotSettings

    elsewhere = tmp_path / "elsewhere"
    monkeypatch.setenv("KRX_ALPHA_KIS_TOKEN_CACHE_DIR", str(elsewhere))
    cache = tmp_path / "cache"
    cache.mkdir(parents=True)
    (cache / ".host-admission").touch()

    # When: 명시 설정으로 데이터 슬롯 스택을 조립한다
    stack, _ = build_kis_data_slot_stack(
        snapshot=SnapshotSettings(kis_data_slot="1"),
        credentials=(_slot_credential("1", "key-one"),),
        token_settings=KisTokenSettings(token_cache_dir=cache, allow_issue=False),
        session=_FakeSession([]),
        now=lambda: T0,
    )

    # Then: 경로는 인자에서만 유도된다
    assert stack.tokens._token_cache_path.parent == cache
    assert not elsewhere.exists()
