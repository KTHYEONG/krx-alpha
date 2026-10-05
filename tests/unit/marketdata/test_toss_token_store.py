"""Shared Toss/LS token store invariants (KCA schema-interoperable)."""

from __future__ import annotations

import json
import pathlib

from src.marketdata.toss_token_store import IssuedToken


def _store(tmp_path: pathlib.Path, **kwargs: object) -> object:
    from src.marketdata.toss_token_store import TossTokenStore

    params: dict[str, object] = {"lock_timeout_s": 5.0, "expiry_margin_s": 600.0}
    params.update(kwargs)
    return TossTokenStore(tmp_path / "token_toss_abc.json", **params)  # type: ignore[arg-type]


def _kca_payload(token: str, generation: int, issued_at: str | None = None) -> dict[str, object]:
    import datetime as dt

    return {
        "schema_version": 1,
        "access_token": token,
        "issued_at": issued_at if issued_at is not None else dt.datetime.now(dt.UTC).isoformat(),
        "expires_at": None,
        "generation": generation,
    }


def test_calendar_and_program_sync_share_one_token(tmp_path) -> None:
    # Given: 빈 저장소와 호출 횟수를 세는 가짜 발급기
    store = _store(tmp_path)
    calls: list[str] = []

    def _issue() -> IssuedToken:
        calls.append("oauth")
        return IssuedToken("shared-tok", None)

    # When: 캘린더와 프로그램 싱크가 각각 토큰을 요청한다
    first = store.get_or_issue(_issue)  # type: ignore[attr-defined]
    second = store.get_or_issue(_issue)  # type: ignore[attr-defined]

    # Then: oauth 엔드포인트는 1회만 호출되고 같은 토큰을 공유한다
    assert (first, second) == ("shared-tok", "shared-tok")
    assert calls == ["oauth"]


def test_peer_rotation_adopted_without_issuance(tmp_path) -> None:
    # Given: KCA 형식으로 기록된 B(거부된 A와 다름)를 쥔 저장소
    path = tmp_path / "token_toss_abc.json"
    path.write_text(json.dumps(_kca_payload("B", 2)), encoding="utf-8")
    store = _store(tmp_path)

    def _boom() -> IssuedToken:
        raise AssertionError("must not issue when a peer already rotated")

    # When
    token = store.replace_rejected("A", _boom)  # type: ignore[attr-defined]

    # Then: 발급 없이 B를 재시용한다
    assert token == "B"


def test_store_file_interoperates_with_kca_schema(tmp_path) -> None:
    # Given: KCA가 쓴 JSON 픽스처
    from src.marketdata.toss_token_store import ls_token_path, toss_token_path

    path = tmp_path / "token_toss_abc.json"
    path.write_text(json.dumps(_kca_payload("K", 7)), encoding="utf-8")
    store = _store(tmp_path)

    # When / Then: 파싱되고 KRX 쓰기도 같은 스키마로 읽힌다
    record = store.read()  # type: ignore[attr-defined]
    assert (record.access_token, record.generation) == ("K", 7)
    assert record.issued_at.tzinfo is not None
    assert record.expires_at is None
    assert store.replace_rejected("K", lambda: IssuedToken("K2", None)) == "K2"  # type: ignore[attr-defined]
    raw = json.loads(path.read_text(encoding="utf-8"))
    assert set(raw) == {"schema_version", "access_token", "issued_at", "expires_at", "generation"}
    assert raw["generation"] == 8
    assert "app_key" not in raw
    assert "secret" not in raw.values()
    assert toss_token_path(tmp_path, "k").name.startswith("token_toss_")
    assert ls_token_path(tmp_path, "k").name.startswith("token_ls_")


def test_expired_token_is_reissued_with_generation_plus_one(tmp_path) -> None:
    # Given: 과거에 만료된 토큰과 가짜 시계
    import datetime as dt

    from src.marketdata.toss_token_store import IssuedToken, TossTokenStore

    now = [dt.datetime(2026, 9, 30, tzinfo=dt.UTC)]
    path = tmp_path / "token_toss_abc.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "access_token": "old",
                "issued_at": "2026-09-01T00:00:00+00:00",
                "expires_at": "2026-09-02T00:00:00+00:00",
                "generation": 3,
            }
        ),
        encoding="utf-8",
    )
    store = TossTokenStore(path, lock_timeout_s=5.0, expiry_margin_s=0.0, clock=lambda: now[0], sleep=lambda s: None)
    calls: list[str] = []

    # When
    token = store.get_or_issue(lambda: calls.append("x") or IssuedToken("new", 3600.0))

    # Then: 1회 발급되고 만료시각과 세대가 전진한다
    assert token == "new"
    assert calls == ["x"]
    record = store.read()
    assert record is not None
    assert record.generation == 4
    assert record.expires_at == now[0] + dt.timedelta(seconds=3600.0)


def test_failed_issuance_preserves_file_bytes(tmp_path) -> None:
    # Given: 저장된 토큰 없이 실패하는 발급기
    import pytest


    path = tmp_path / "token_toss_abc.json"
    path.write_text(json.dumps(_kca_payload("K", 1)), encoding="utf-8")
    before = path.read_bytes()
    store = _store(tmp_path)

    def _fail() -> IssuedToken:
        raise RuntimeError("vendor down")

    # When / Then: 예외가 전파되고 파일 바이트가 그대로다
    with pytest.raises(RuntimeError, match="vendor down"):
        store.replace_rejected("K", _fail)  # type: ignore[attr-defined]
    assert path.read_bytes() == before


def test_lock_timeout_without_ever_blocking(tmp_path) -> None:
    # Given: 외부에서 잡힌 잠금과 짧은 타임아웃
    import fcntl
    import os

    import pytest

    from src.marketdata.toss_token_store import TokenStoreLockTimeout

    store = _store(tmp_path, lock_timeout_s=0.05)
    lock_path = tmp_path / "token_toss_abc.json.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(lock_path), os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(fd, fcntl.LOCK_EX)
    try:
        # When / Then: 제한 안에 얻지 못하면 타임아웃한다
        with pytest.raises(TokenStoreLockTimeout):
            store.get_or_issue(lambda: (_ for _ in ()).throw(AssertionError("no issue under contention")))  # type: ignore[attr-defined]
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def test_corrupt_file_treated_as_absent(tmp_path, caplog) -> None:
    # Given: 깨진 JSON 파일
    import logging


    path = tmp_path / "token_toss_abc.json"
    path.write_text("{not json", encoding="utf-8")
    store = _store(tmp_path)

    # When
    assert store.read() is None  # type: ignore[attr-defined]
    with caplog.at_level(logging.WARNING, logger="src.marketdata.toss_token_store"):
        token = store.get_or_issue(lambda: IssuedToken("fresh", None))  # type: ignore[attr-defined]

    # Then: 발급 후 덮어쓰고 경고가 기록된다
    assert token == "fresh"
    assert any("SCHEMA_INVALID" in rec.message for rec in caplog.records)


def test_unreadable_file_returns_none(tmp_path) -> None:
    # Given: 디렉터리인 저장소 경로
    store = _store(tmp_path)
    (tmp_path / "token_toss_abc.json").mkdir()

    # When / Then: 예외 없이 None이다
    assert store.read() is None  # type: ignore[attr-defined]


def test_schema_hygiene_rejects_malformed_records(tmp_path) -> None:
    # Given: 스키마 위반 페이로드들
    store = _store(tmp_path)
    path = tmp_path / "token_toss_abc.json"
    bad_payloads: list[object] = [
        ["not", "a", "dict"],
        {"schema_version": 1},
        {**_kca_payload("K", 1), "extra": True},
        {**_kca_payload("K", 1), "schema_version": 2},
        {**_kca_payload("", 1)},
        {**_kca_payload("K", 0)},
        {**_kca_payload("K", True)},
        {**_kca_payload("K", 1), "issued_at": 123},
        {**_kca_payload("K", 1), "issued_at": "bad"},
        {**_kca_payload("K", 1), "issued_at": "2026-09-14T00:00:00"},
        {**_kca_payload("K", 1), "expires_at": "2026-09-14T00:00:00"},
        {**_kca_payload("K", 1), "expires_at": 123},
        {**_kca_payload("K", 1), "access_token": 123},
    ]
    for payload in bad_payloads:
        path.write_text(json.dumps(payload), encoding="utf-8")
        assert store.read() is None, payload  # type: ignore[attr-defined]


def test_double_checked_lock_adopts_concurrent_issuance(tmp_path, monkeypatch) -> None:
    # Given: 첫 읽기는 비어 있고 잠금 후에는 동료가 채운 저장소

    store = _store(tmp_path)
    states = iter([None, _kca_payload("peer", 5)])
    real_read = store.read  # type: ignore[attr-defined]

    def _flaky() -> object:
        payload = next(states)
        if payload is None:
            return None
        (tmp_path / "token_toss_abc.json").write_text(json.dumps(payload), encoding="utf-8")
        return real_read()

    monkeypatch.setattr(store, "read", _flaky)

    def _boom() -> IssuedToken:
        raise AssertionError("double-checked hit must not issue")

    # When / Then: 잠금 후 재검사로 동료 토큰을 채택한다
    assert store.get_or_issue(_boom) == "peer"  # type: ignore[attr-defined]


def test_async_variants_share_the_same_file(tmp_path) -> None:
    # Given: 비동기 발급기를 쓰는 저장소
    import asyncio


    store = _store(tmp_path)

    async def _issue() -> IssuedToken:
        await asyncio.sleep(0)
        return IssuedToken("async-tok", None)

    async def _boom() -> IssuedToken:
        raise AssertionError("must not issue")

    async def _main() -> tuple[str, str]:
        first = await store.aget_or_issue(_issue)  # type: ignore[attr-defined]
        second = await store.areplace_rejected("other", _boom)  # type: ignore[attr-defined]
        return first, second

    # When / Then
    assert asyncio.run(_main()) == ("async-tok", "async-tok")


def test_async_double_checked_lock_adopts_concurrent_issuance(tmp_path, monkeypatch) -> None:
    # Given: 첫 읽기는 비어 있고 잠금 후에는 동료가 채운 저장소
    import asyncio

    store = _store(tmp_path)
    states = iter([None, _kca_payload("peer", 5)])
    real_read = store.read  # type: ignore[attr-defined]

    def _flaky() -> object:
        payload = next(states)
        if payload is None:
            return None
        (tmp_path / "token_toss_abc.json").write_text(json.dumps(payload), encoding="utf-8")
        return real_read()

    monkeypatch.setattr(store, "read", _flaky)

    async def _boom() -> IssuedToken:
        raise AssertionError("double-checked hit must not issue")

    # When / Then
    assert asyncio.run(store.aget_or_issue(_boom)) == "peer"  # type: ignore[attr-defined]


def test_async_lock_timeout(tmp_path) -> None:
    # Given: 외부에서 잡힌 잠금
    import asyncio
    import fcntl
    import os

    import pytest

    from src.marketdata.toss_token_store import IssuedToken, TokenStoreLockTimeout

    store = _store(tmp_path, lock_timeout_s=0.05)
    lock_path = tmp_path / "token_toss_abc.json.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(lock_path), os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(fd, fcntl.LOCK_EX)
    try:
        with pytest.raises(TokenStoreLockTimeout):
            asyncio.run(store.aget_or_issue(lambda: asyncio.sleep(0, result=IssuedToken("x", None))))  # type: ignore[attr-defined]
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def test_constructor_and_path_validation() -> None:
    import pytest

    from src.marketdata.toss_token_store import TossTokenStore, token_store_path

    with pytest.raises(ValueError, match="positive"):
        TossTokenStore(pathlib.Path("x.json"), lock_timeout_s=0.0)
    with pytest.raises(ValueError, match="non-negative"):
        TossTokenStore(pathlib.Path("x.json"), expiry_margin_s=-1.0)
    with pytest.raises(ValueError, match="non-empty"):
        token_store_path(pathlib.Path("c"), "", "cred")
    with pytest.raises(ValueError, match="non-empty"):
        token_store_path(pathlib.Path("c"), "toss", "")


def test_publish_failure_rolls_back_temp_file(tmp_path, monkeypatch) -> None:
    # Given: os.replace가 실패하는 저장소
    import os as _os

    import pytest


    store = _store(tmp_path)
    monkeypatch.setattr(_os, "replace", lambda *a: (_ for _ in ()).throw(OSError("disk gone")))

    # When / Then: 예외가 전파되고 임시 파일이 남지 않는다
    with pytest.raises(OSError, match="disk gone"):
        store.get_or_issue(lambda: IssuedToken("x", None))  # type: ignore[attr-defined]
    leftovers = [p for p in tmp_path.iterdir() if p.suffix == ".tmp"]
    assert leftovers == []


def test_unknown_expiry_token_older_than_max_age_is_reissued(tmp_path) -> None:
    # Given: expires_at 없이 기록된 13시간 전 토큰과 12시간 상한
    import datetime as dt

    now = dt.datetime(2026, 10, 6, 8, 20, tzinfo=dt.UTC)
    path = tmp_path / "token_toss_abc.json"
    path.write_text(json.dumps(_kca_payload("OLD", 1, issued_at=(now - dt.timedelta(hours=13)).isoformat())), encoding="utf-8")
    store = _store(tmp_path, clock=lambda: now, unknown_expiry_max_age_s=12 * 3600.0)

    # When: 토큰을 요청한다
    token = store.get_or_issue(lambda: IssuedToken("NEW", None))  # type: ignore[attr-defined]

    # Then: 만료 불명 토큰은 상한을 넘으면 재발급되고 generation 이 증가한다
    assert token == "NEW"
    assert json.loads(path.read_text(encoding="utf-8"))["generation"] == 2


def test_unknown_expiry_token_within_max_age_is_reused(tmp_path) -> None:
    # Given: expires_at 없이 기록된 1시간 전 토큰
    import datetime as dt

    now = dt.datetime(2026, 10, 6, 8, 20, tzinfo=dt.UTC)
    path = tmp_path / "token_toss_abc.json"
    path.write_text(json.dumps(_kca_payload("FRESH", 1, issued_at=(now - dt.timedelta(hours=1)).isoformat())), encoding="utf-8")
    store = _store(tmp_path, clock=lambda: now, unknown_expiry_max_age_s=12 * 3600.0)

    # When / Then: 발급기를 호출하지 않고 재사용한다
    assert store.get_or_issue(lambda: IssuedToken("NEW", None)) == "FRESH"  # type: ignore[attr-defined]


def test_non_positive_unknown_expiry_max_age_is_rejected(tmp_path) -> None:
    import pytest

    with pytest.raises(ValueError, match="unknown_expiry_max_age_s"):
        _store(tmp_path, unknown_expiry_max_age_s=0.0)
