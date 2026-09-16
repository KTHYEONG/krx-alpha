"""Trusted-workstation CLI to provision the shared KIS data-key pool fragment.

Invoked only from a trusted workstation as
``uv run python -m src.cli.provision_kis_keypool --host or-vps``.
It never runs in GitHub Actions and never logs credential values.
"""

from __future__ import annotations

import argparse
import logging
from collections.abc import Sequence
from pathlib import Path

from src.core.kis_keypool_provisioning import build_shared_fragment, install_shared_fragment

logger = logging.getLogger(__name__)

DEFAULT_HOST = "or-vps"
SELECTOR_KEYS = ("KIS_DATA_SLOTS", "KIS_HOST_DATA_SLOTS")


def main(argv: Sequence[str] | None = None) -> int:
    """Validate the workstation source and install the shared fragment."""
    parser = argparse.ArgumentParser(description="Provision the shared KIS data-key pool fragment.")
    parser.add_argument("--source", type=Path, default=Path.home() / ".quant.env")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    fragment = build_shared_fragment(args.source)
    data_key_total = sum(
        1 for line in fragment.splitlines() if line.partition("=")[0] not in SELECTOR_KEYS
    )
    if not args.dry_run:
        install_shared_fragment(args.host, fragment)
    logger.info("validated %d accepted data keys", data_key_total)
    return 0


if __name__ == "__main__":  # pragma: no cover - interpreter entry point only
    raise SystemExit(main())
