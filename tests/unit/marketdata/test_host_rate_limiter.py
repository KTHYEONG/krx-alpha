"""Host-paced KIS rate limiter protocol invariants (KCA byte-compatible)."""

from __future__ import annotations

import pathlib


def _marker(cache: pathlib.Path) -> None:
    (cache / ".host-admission").touch()


def test_protocol_byte_compatibility_with_kca_fixture(tmp_path) -> None:
    # Given: KCA 형식으로 기록된 상태 파일과 가짜 벽시계
    from src.brokers.kis.rate import HostPacedRateLimiter

    _marker(tmp_path)
    state = tmp_path / "tps_abc123.state"
    state.write_text("1790000000.25", encoding="utf-8")
    now = [1790000001.0]
    limiter = HostPacedRateLimiter(state, 18.0, clock=lambda: now[0], sleep=lambda s: None)

    # When
    limiter.acquire()

    # Then: max(now, next_free) + 1/18 이 float() 파싱 가능한 텍스트로 기록된다
    assert float(state.read_text(encoding="utf-8")) == 1790000001.0 + 1.0 / 18.0


def test_interleaved_consumers_keep_interval_spacing(tmp_path) -> None:
    # Given: 하나의 파일을 공유하는 두 리미터와 고정된 가짜 시계
    from src.brokers.kis.rate import HostPacedRateLimiter

    _marker(tmp_path)
    state = tmp_path / "tps_shared.state"
    first = HostPacedRateLimiter(state, 18.0, clock=lambda: 1790000000.0, sleep=lambda s: None)
    second = HostPacedRateLimiter(state, 18.0, clock=lambda: 1790000000.0, sleep=lambda s: None)

    # When: 번갈아 슬롯을 예약한다
    seen: list[float] = []
    for limiter in (first, second, first, second):
        limiter.acquire()
        seen.append(float(state.read_text(encoding="utf-8")))

    # Then: 예약 슬롯이 정확히 한 인터벌씩 엄격히 증가한다
    assert seen == [1790000000.0 + (i + 1) / 18.0 for i in range(4)]


def test_bulk_lead_bound_writes_nothing_before_lead_window(tmp_path) -> None:
    # Given: 3초 뒤에 열리는 슬롯과 0.25초 리드 경계
    from src.brokers.kis.rate import HostPacedRateLimiter

    _marker(tmp_path)
    state = tmp_path / "tps_bulk.state"
    now = [1790000000.0]
    state.write_text("1790000003.0", encoding="utf-8")
    snapshots: list[str] = []

    def _sleep(seconds: float) -> None:
        snapshots.append(state.read_text(encoding="utf-8"))
        now[0] += seconds

    limiter = HostPacedRateLimiter(state, 18.0, max_lead_s=0.25, clock=lambda: now[0], sleep=_sleep)

    # When
    limiter.acquire()

    # Then: 리드 창에 들어서기 전에는 쓰기가 없고 정확히 한 인터벌만 전진한다
    assert snapshots[0] == "1790000003.0"
    assert now[0] == 1790000003.0
    assert float(state.read_text(encoding="utf-8")) == 1790000003.0 + 1.0 / 18.0


def test_critical_books_without_bound(tmp_path) -> None:
    # Given: 5초 뒤에 열리는 슬롯과 무제한 크리티컬 리미터
    from src.brokers.kis.rate import HostPacedRateLimiter

    _marker(tmp_path)
    state = tmp_path / "tps_crit.state"
    state.write_text("1790000005.0", encoding="utf-8")
    now = [1790000000.0]
    slept: list[float] = []

    def _sleep(seconds: float) -> None:
        slept.append(seconds)
        now[0] += seconds

    limiter = HostPacedRateLimiter(state, 18.0, max_lead_s=None, clock=lambda: now[0], sleep=_sleep)

    # When
    limiter.acquire()

    # Then: 즉시 예약하고 슬롯까지 대기한다
    assert slept == [5.0]
    assert float(state.read_text(encoding="utf-8")) == 1790000005.0 + 1.0 / 18.0


def test_container_without_marker_fails_closed(tmp_path, monkeypatch) -> None:
    # Given: 컨테이너 판정과 마커 없는 디렉터리
    import pytest

    import src.brokers.kis.rate as rate_mod
    from src.brokers.kis.rate import HostPacedRateLimiter
    from src.core.errors import KrxAlphaError

    monkeypatch.setattr(rate_mod, "_in_container", lambda: True)

    # When / Then: 생성자가 KrxAlphaError 하위 예외로 거부한다
    with pytest.raises(KrxAlphaError):
        HostPacedRateLimiter(tmp_path / "tps_x.state", 18.0)

    # When: 마커가 있으면 통과한다
    _marker(tmp_path)
    HostPacedRateLimiter(tmp_path / "tps_x.state", 18.0)

    # When: 컨테이너가 아니면 마커 없이도 통과한다
    monkeypatch.setattr(rate_mod, "_in_container", lambda: False)
    HostPacedRateLimiter(tmp_path / "other" / "tps_y.state", 18.0)


def test_state_paths_are_deterministic_and_validated(tmp_path) -> None:
    # Given / When / Then: 경로 규칙과 검증
    import hashlib

    import pytest

    from src.brokers.kis.rate import HostPacedRateLimiter, host_state_path, kis_state_path

    digest = hashlib.sha256(b"app-key").hexdigest()[:12]
    assert kis_state_path(tmp_path, "app-key") == tmp_path / f"tps_{digest}.state"
    assert (
        host_state_path(tmp_path, "toss", "app-key", "STOCK_TRADING_TREND")
        == tmp_path / f"admission_toss_{digest}_STOCK_TRADING_TREND.state"
    )
    assert host_state_path(tmp_path, "toss", "app-key") == tmp_path / f"admission_toss_{digest}.state"
    with pytest.raises(ValueError, match="invalid vendor"):
        host_state_path(tmp_path, "../x", "app-key")
    with pytest.raises(ValueError, match="invalid scope"):
        host_state_path(tmp_path, "toss", "app-key", "../x")
    with pytest.raises(ValueError, match="non-empty"):
        host_state_path(tmp_path, "toss", "")
    with pytest.raises(ValueError, match="non-empty"):
        kis_state_path(tmp_path, "")
    _marker(tmp_path)
    with pytest.raises(ValueError, match="positive"):
        HostPacedRateLimiter(tmp_path / "s.state", 0.0)
    with pytest.raises(ValueError, match="positive"):
        HostPacedRateLimiter(tmp_path / "s.state", 18.0, max_lead_s=0.0)


def test_unparsable_state_resets_with_warning(tmp_path, caplog) -> None:
    # Given: 손상된 상태 파일
    import logging

    from src.brokers.kis.rate import HostPacedRateLimiter

    _marker(tmp_path)
    state = tmp_path / "tps_bad.state"
    state.write_text("garbage", encoding="utf-8")
    limiter = HostPacedRateLimiter(state, 18.0, clock=lambda: 1790000000.0, sleep=lambda s: None)

    # When
    with caplog.at_level(logging.WARNING, logger="src.brokers.kis.rate"):
        limiter.acquire()

    # Then: 예약이 성공하고 STATE_RESET 경고가 1회 기록된다
    assert float(state.read_text(encoding="utf-8")) == 1790000000.0 + 1.0 / 18.0
    assert sum("STATE_RESET" in rec.message for rec in caplog.records) == 1
