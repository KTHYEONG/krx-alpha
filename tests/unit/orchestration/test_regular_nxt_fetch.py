"""Boundary guards for the live NXT ranking fetch used by the evening pool job."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

import src.orchestration.regular_nxt as nxt_fetch
from src.marketdata.kiwoom_ranking import KiwoomRankingRow
from src.marketdata.kiwoom_ticks import KiwoomTickTapeError


class _Store:
    def __init__(self, *, fail: bool) -> None:
        self._fail = fail

    def get_or_issue(self, issue):
        if self._fail:
            raise OSError("token cache unavailable")
        issued = issue()
        return SimpleNamespace(access_token=issued)


@pytest.fixture
def wired(monkeypatch, tmp_path):
    calls: dict[str, object] = {}
    monkeypatch.setattr(
        nxt_fetch,
        "load_credentials",
        lambda _cls: SimpleNamespace(kiwoom_app_key="app", kiwoom_secret_key="secret"),
    )
    monkeypatch.setattr(nxt_fetch, "KisTokenSettings", lambda: SimpleNamespace(token_cache_dir=tmp_path))
    monkeypatch.setattr(nxt_fetch, "AsyncHostPacer", lambda *a, **k: "pacer")
    monkeypatch.setattr(nxt_fetch, "kiwoom_issued", lambda token: f"issued:{token}")
    monkeypatch.setattr(nxt_fetch, "issue_kiwoom_token", lambda **kwargs: "tok")

    def _fetch(**kwargs):
        calls["fetch"] = kwargs
        return (KiwoomRankingRow(rank=1, symbol="000660", last_price=1, change_pct=0.0, trade_value_krw=5),)

    monkeypatch.setattr(nxt_fetch, "fetch_trade_amount_ranking", _fetch)
    return calls


def test_live_fetch_requests_nxt_selector_with_shared_token(monkeypatch, wired) -> None:
    # Given: a token store that issues on first use
    monkeypatch.setattr(nxt_fetch, "make_kiwoom_store", lambda *a, **k: _Store(fail=False))

    # When
    rows = nxt_fetch.fetch_nxt_trade_amount_ranking_live()

    # Then: NXT selector, no value floor, host-shared token from the store
    assert [row.symbol for row in rows] == ["000660"]
    kwargs = wired["fetch"]
    assert kwargs["stex_tp"] == "2"
    assert kwargs["min_trade_value_krw"] == 1
    assert kwargs["max_pages"] == 3
    assert kwargs["access_token"] == "issued:tok"


def test_token_store_failure_is_wrapped(monkeypatch, wired) -> None:
    # Given: a token store that cannot read or write its cache
    monkeypatch.setattr(nxt_fetch, "make_kiwoom_store", lambda *a, **k: _Store(fail=True))

    # Then: the failure surfaces as the Kiwoom tape error and no ranking request is made
    with pytest.raises(KiwoomTickTapeError, match="kiwoom token store failed"):
        nxt_fetch.fetch_nxt_trade_amount_ranking_live()
    assert "fetch" not in wired
