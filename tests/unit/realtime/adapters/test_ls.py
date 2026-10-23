"""Capacity-default guard for the LS realtime adapter."""

from __future__ import annotations

import inspect

from src.core.config import LS_MAX_SUBSCRIPTIONS_PER_SESSION
from src.realtime.adapters.ls import LsRealtimeAdapter


def test_ls_default_capacity_tracks_vendor_subscription_cap() -> None:
    params = inspect.signature(LsRealtimeAdapter.__init__).parameters
    assert params["capacity_pairs"].default == LS_MAX_SUBSCRIPTIONS_PER_SESSION == 200
