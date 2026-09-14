from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _fast_krx_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    import src.marketdata.krx_bars as bars_mod

    monkeypatch.setattr(bars_mod, "RETRY_WAIT_BASE_S", 0.0)
