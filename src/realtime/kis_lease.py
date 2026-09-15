"""KIS WebSocket 키별 독점 lease (host-visible 비차단 파일 lock)."""

from __future__ import annotations

import fcntl
import pathlib
from typing import Any

from src.realtime.contracts import VendorAuthRejected


class KisWebSocketLease:
    """data/work/kis_ws_leases/<key_id>.lock에 대한 비차단 독점 lock이다."""

    def __init__(self, *, root: pathlib.Path | str, credential_key_id: str = "") -> None:
        base = pathlib.Path(root)
        self._path = base / f"{credential_key_id}.lock"
        self._handle: Any = None

    async def acquire(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        handle = open(self._path, "a+b")  # noqa: ASYNC230,SIM115 - lock handle은 release까지 생존, flock은 비차단
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            handle.close()
            raise VendorAuthRejected(f"key_lease_busy:{self._path.name}") from None
        self._handle = handle

    async def release(self) -> None:
        handle, self._handle = self._handle, None
        if handle is not None:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()
