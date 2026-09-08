"""테스트 전역 설정: 임시 경로를 프로젝트 내부(tmp/)로 고정한다."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROJECT_TMP = PROJECT_ROOT / "tmp" / "pytest"


@pytest.fixture(scope="session", autouse=True)
def _pin_tmp_root() -> None:
    """외부 /tmp 사용을 차단하고 저장소 내부 tmp/ 로 강제한다."""
    PROJECT_TMP.mkdir(parents=True, exist_ok=True)
    for var in ("TMPDIR", "TEMP", "TMP"):
        os.environ[var] = str(PROJECT_TMP)
    tempfile.tempdir = str(PROJECT_TMP)


@pytest.fixture(scope="session")
def tmp_path_factory_root() -> Path:
    return PROJECT_TMP
