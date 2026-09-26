"""테스트 전역 설정: 임시 경로를 프로젝트 내부(tmp/)로 고정한다."""

from __future__ import annotations

import os
import shutil
import socket
import tempfile
from collections.abc import Generator
from pathlib import Path

# 다중 프로젝트 및 로컬 동시성 환경 리소스 안전 가드:
# Polars, NumPy, OpenBLAS, MKL, Numba 등이 8코어 머신에서 스레드를 과도하게 점유하지 못하도록 상한선 강제
for _key, _val in (
    ("POLARS_MAX_THREADS", "2"),
    ("OMP_NUM_THREADS", "1"),
    ("OPENBLAS_NUM_THREADS", "1"),
    ("MKL_NUM_THREADS", "1"),
    ("NUMBA_NUM_THREADS", "1"),
    ("RAY_ACCEL_NUM_WORKERS", "1"),
    ("PYTHONDONTWRITEBYTECODE", "1"),
):
    os.environ.setdefault(_key, _val)

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROJECT_TMP = PROJECT_ROOT / "tmp" / "pytest"


@pytest.fixture(scope="session", autouse=True)
def _pin_tmp_root() -> Generator[None, None, None]:
    """외부 /tmp 사용을 차단하고 저장소 내부 tmp/ 로 강제하며, 세션 종료 시 임시 파일을 정리한다."""
    PROJECT_TMP.mkdir(parents=True, exist_ok=True)
    for var in ("TMPDIR", "TEMP", "TMP"):
        os.environ[var] = str(PROJECT_TMP)
    tempfile.tempdir = str(PROJECT_TMP)
    yield
    shutil.rmtree(PROJECT_TMP, ignore_errors=True)


@pytest.fixture(scope="session")
def tmp_path_factory_root() -> Path:
    return PROJECT_TMP


@pytest.fixture(autouse=True)
def _hermetic_rclone_and_alert_env(monkeypatch) -> None:
    monkeypatch.setattr("src.storage.remote.shutil.which", lambda name: None)
    for var in ("ALERT_GMAIL_USER", "ALERT_GMAIL_APP_PASSWORD", "ALERT_GMAIL_TO"):
        monkeypatch.delenv(var, raising=False)


_CREDENTIAL_ENV_PREFIXES: tuple[str, ...] = (
    "KIS_",
    "LS_",
    "TOSS_",
    "KRX_OPENAPI",
    "OPENDART",
    "KIWOM_",
    "BINANCE_",
    # healthchecks.io ping URL authorizes pings, so it is credential-like; scrub it
    # to keep liveness disabled by default in unit tests.
    "KRX_ALPHA_LIVENESS_",
)
_LOOPBACK_HOSTS: frozenset[str] = frozenset({"127.0.0.1", "::1", "localhost"})


@pytest.fixture(autouse=True)
def _hermetic_credentials_and_network(monkeypatch) -> None:
    # 개발 셸에 export된 실키로 단위 테스트가 실제 벤더 API(토큰 발급 등)를 호출하지 않도록
    # 자격증명 env를 지우고 루프백 외 소켓 연결을 차단한다.
    for var in list(os.environ):
        if var.startswith(_CREDENTIAL_ENV_PREFIXES):
            monkeypatch.delenv(var, raising=False)

    original_connect = socket.socket.connect

    def _guarded_connect(self: socket.socket, address: object) -> None:
        if self.family == socket.AF_UNIX:
            original_connect(self, address)
            return
        host = address[0] if isinstance(address, tuple) and address else address
        if host in _LOOPBACK_HOSTS:
            original_connect(self, address)
            return
        raise OSError(f"external network blocked in unit tests: {address!r}")

    monkeypatch.setattr(socket.socket, "connect", _guarded_connect)


@pytest.fixture(autouse=True)
def _shutdown_managed_logging():
    yield
    from src.core.observability import shutdown_logging

    shutdown_logging()
