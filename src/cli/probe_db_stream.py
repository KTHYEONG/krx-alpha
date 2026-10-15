"""probe-db-stream CLI (one-shot DB rehearsal; never part of the daemon)."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import pathlib
import time
from typing import Any

import aiohttp

from src.core.config import CollectorSettings, DbCredentials, KisTokenSettings, load_credentials
from src.marketdata.toss_token_store import TossTokenStore, db_token_path
from src.realtime.adapters.db import DB_TR_CD, DbRealtimeAdapter
from src.realtime.contracts import VendorAuthRejected, VendorDisconnected

_DEFAULT_STREAMS = ("H0STCNT0", "H0STASP0")


def add_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register ``probe-db-stream`` (one-shot rehearsal; never part of the daemon)."""
    parser = subparsers.add_parser("probe-db-stream")
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--streams", default=",".join(_DEFAULT_STREAMS))
    parser.add_argument("--seconds", type=float, default=120.0)
    parser.add_argument("--out", required=True)
    parser.set_defaults(handler=run)


def run(args: argparse.Namespace) -> int:
    """Connect once, subscribe the given symbols, write every raw text message for ``--seconds`` to ``--out`` JSONL, print a summary; returns 0 iff at least one data frame per accepted pair arrived."""
    return asyncio.run(_run_probe(args))


def _parse_symbols(raw: str) -> list[str]:
    symbols = [s.strip() for s in str(raw).split(",") if s.strip()]
    if not symbols or len(symbols) > 25:
        raise ValueError("symbols must hold 1..25 entries")
    return symbols


def _parse_streams(raw: str) -> tuple[str, ...]:
    streams = tuple(s.strip() for s in str(raw).split(",") if s.strip()) or _DEFAULT_STREAMS
    if set(streams) - set(DB_TR_CD):
        raise ValueError(f"unsupported streams: {streams}")
    return streams


def _out_inside_l0(out: pathlib.Path, journal_root: pathlib.Path) -> bool:
    try:
        out.resolve().relative_to(journal_root.resolve())
        return True
    except ValueError:
        return False


async def _run_probe(args: argparse.Namespace) -> int:
    settings = CollectorSettings()
    try:
        symbols = _parse_symbols(args.symbols)
        streams = _parse_streams(getattr(args, "streams", ",".join(_DEFAULT_STREAMS)))
        seconds = float(getattr(args, "seconds", 120.0))
        if not 0 < seconds <= 900:
            raise ValueError("seconds must be in (0, 900]")
        out_path = pathlib.Path(str(args.out))
        if _out_inside_l0(out_path, settings.paths.journal_root):
            raise ValueError(f"out must be outside {settings.paths.journal_root}")
    except ValueError as exc:
        print(json.dumps({"error": str(exc)}))  # noqa: T201 - gate result goes to stdout
        return 2
    creds = load_credentials(DbCredentials)
    token_store = TossTokenStore(
        db_token_path(KisTokenSettings().token_cache_dir, creds.db_app_key),
        rotation_cooldown_s=settings.db_token_rotation_cooldown_s,
    )
    http = aiohttp.ClientSession()
    tap: list[dict[str, Any]] = []
    ping_count = 0
    adapter = DbRealtimeAdapter(
        app_key=creds.db_app_key,
        app_secret=creds.db_app_secret,
        http=http,
        token_store=token_store,
        streams=streams,
        capacity_pairs=settings.db_capacity_pairs,
        ack_timeout_s=settings.db_ack_timeout_s,
        subscribe_spacing_s=settings.db_subscribe_spacing_s,
    )
    try:
        try:
            await adapter.connect()
        except (VendorDisconnected, VendorAuthRejected) as exc:
            print(json.dumps({"error": str(exc)}))  # noqa: T201 - gate result goes to stdout
            return 1
        token = adapter._token or ""
        ws = adapter._ws

        def _record(text: str) -> None:
            redacted = text.replace(token, "<token>") if token else text
            tap.append({"recv_wall_ns": time.time_ns(), "text": redacted})

        orig_receive_str = ws.receive_str
        orig_receive = ws.receive

        async def _tapped_receive_str() -> str:
            text = await orig_receive_str()
            _record(str(text))
            return str(text)

        async def _tapped_receive() -> Any:
            nonlocal ping_count
            msg = await orig_receive()
            mtype = getattr(msg, "type", None)
            data = getattr(msg, "data", None)
            if mtype is aiohttp.WSMsgType.PING:
                ping_count += 1
            elif isinstance(data, str) and data:
                _record(data)
            return msg

        ws.receive_str = _tapped_receive_str
        ws.receive = _tapped_receive
        try:
            acks = await adapter.subscribe([(s, t) for s in symbols for t in streams])
        except (VendorDisconnected, VendorAuthRejected, ValueError) as exc:
            _flush(out_path, tap)
            print(json.dumps({"error": str(exc), "ping_count": ping_count}))  # noqa: T201 - gate result goes to stdout
            return 1
        accepted = [a for a in acks if a.accepted]
        deadline = time.monotonic() + seconds
        frames: list[Any] = []
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                frame = await asyncio.wait_for(adapter.recv(), timeout=remaining)
            except TimeoutError:
                break
            except (VendorDisconnected, VendorAuthRejected):
                break
            frames.append(frame)
        _flush(out_path, tap)
        counts: dict[str, int] = {}
        top_keys: set[str] = set()
        header_keys: set[str] = set()
        first_body_keys: list[str] = []
        for frame in frames:
            counts[f"{frame.symbol}|{frame.stream}"] = counts.get(f"{frame.symbol}|{frame.stream}", 0) + 1
            try:
                o = json.loads(frame.raw)
            except ValueError:
                continue
            if isinstance(o, dict):
                top_keys.update(str(k) for k in o)
                header = o.get("header")
                if isinstance(header, dict):
                    header_keys.update(str(k) for k in header)
                if not first_body_keys:
                    body = o.get("body")
                    if isinstance(body, dict):
                        first_body_keys = [str(k) for k in body]
        summary = {
            "accepted": [f"{a.symbol}|{a.stream}" for a in accepted],
            "frame_counts": counts,
            "unparsable_frames": adapter.unparsable_frames,
            "top_level_keys": sorted(top_keys),
            "header_keys": sorted(header_keys),
            "first_body_keys": first_body_keys,
            "ping_count": ping_count,
        }
        print(json.dumps(summary))  # noqa: T201 - gate result goes to stdout
        if not accepted:
            return 1
        return 0 if all(counts.get(f"{a.symbol}|{a.stream}", 0) >= 1 for a in accepted) else 1
    finally:
        with contextlib.suppress(aiohttp.ClientError, OSError, ValueError, KeyError):
            await adapter.aclose()
        await http.close()


def _flush(out_path: pathlib.Path, tap: list[dict[str, Any]]) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as handle:
        for entry in tap:
            handle.write(json.dumps(entry) + "\n")
