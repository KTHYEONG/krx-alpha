from __future__ import annotations

import asyncio
from typing import Any, ClassVar

from src.realtime.streaming_child import run_streaming_child


class _Adapter:
    capacity_pairs = 200


class _Watch:
    def pairs(self) -> list[tuple[str, str]]:
        return [("005930", "H0STCNT0")]

    async def run(self, adapter_of, stop) -> None:
        await stop.wait()


class _Streamer:
    captured: ClassVar[dict[str, Any]] = {}

    def __init__(self, **kwargs) -> None:
        type(self).captured = kwargs

    async def run_forever(self, stop, max_cycles=None) -> None:
        return None


def test_live_watch_is_passed_to_streamer_with_pairs_provider() -> None:
    watch = _Watch()

    asyncio.run(
        run_streaming_child(
            session=object(),
            adapter_factory=lambda http: _Adapter(),
            replay_pairs=[("005930", "H0STCNT0")],
            silence_limit=lambda: None,
            max_cycles=1,
            streamer_factory=_Streamer,
            live_watch=watch,  # type: ignore[arg-type]
        )
    )

    assert _Streamer.captured["live_watch"] is watch
    assert _Streamer.captured["pairs_provider"] == watch.pairs
