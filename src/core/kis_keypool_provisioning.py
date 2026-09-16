"""Trusted-workstation provisioning of the shared KIS data-key pool fragment.

The fragment at ``/home/ubuntu/quant-secrets/kis-data.env`` is the single
runtime source for KIS data credentials. It is installed once from a trusted
workstation (see ``src.cli.provision_kis_keypool``); CI only validates it and
never writes or prints credential values.
"""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path

from src.core.errors import KrxAlphaError

logger = logging.getLogger(__name__)

REMOTE_ENV_PATH = "/home/ubuntu/quant-secrets/kis-data.env"
CANONICAL_SLOTS_LINE = "KIS_DATA_SLOTS=1,2,3,4,5"
CANONICAL_HOST_SLOTS_LINE = "KIS_HOST_DATA_SLOTS=1,2,3,4"
DATA_SLOTS = (1, 2, 3, 4, 5)
DATA_FIELDS = ("APP_KEY", "APP_SECRET", "HTS_ID")
ACCEPTED_KEYS: tuple[str, ...] = tuple(
    f"KIS_DATA_{slot}_{field}" for slot in DATA_SLOTS for field in DATA_FIELDS
)
_ACCEPTED_KEY_SET = frozenset(ACCEPTED_KEYS)
REMOTE_INSTALL_SCRIPT = """set -euo pipefail
dest="/home/ubuntu/quant-secrets/kis-data.env"
mkdir -p "$(dirname "$dest")"
chmod 0700 "$(dirname "$dest")"
tmp="$(mktemp "$dest.XXXXXX")"
cat > "$tmp"
chmod 600 "$tmp"
chown ubuntu:ubuntu "$tmp"
mv -f "$tmp" "$dest"
chmod 600 "$dest"
chown ubuntu:ubuntu "$dest"
"""


def build_shared_fragment(source_path: Path) -> str:
    """Build the canonical 17-line shared fragment from a workstation source file.

    Only ``KIS_DATA_<1..5>_{APP_KEY,APP_SECRET,HTS_ID}`` assignments are
    accepted, with or without an ``export `` prefix. Comments, blank lines,
    selectors, account fields, trade keys, primary keys, and unrelated
    settings are ignored. Selectors copied from a local-host allocation are
    never honored; the canonical VPS selectors are always emitted.
    """
    accepted: dict[str, str] = {}
    for raw_line in Path(source_path).read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if line.startswith("export "):
            line = line[len("export "):].strip()
        name, sep, value = line.partition("=")
        if not sep or name.strip() not in _ACCEPTED_KEY_SET:
            continue
        key = name.strip()
        if key in accepted:
            raise KrxAlphaError(f"duplicate accepted data key: {key}")
        candidate = value.strip()
        if len(candidate) >= 2 and candidate[0] == candidate[-1] and candidate[0] in ("'", '"'):
            candidate = candidate[1:-1]
        if candidate:
            accepted[key] = candidate
    missing = [key for key in ACCEPTED_KEYS if key not in accepted]
    if missing:
        raise KrxAlphaError(f"missing required data keys: {', '.join(missing)}")
    lines = [CANONICAL_SLOTS_LINE, CANONICAL_HOST_SLOTS_LINE]
    lines.extend(f"{key}={accepted[key]}" for key in ACCEPTED_KEYS)
    return "\n".join(lines) + "\n"


def install_shared_fragment(host: str, fragment: str) -> None:
    """Install the fragment on the VPS over SSH, sending it only via stdin."""
    logger.info("installing shared KIS data fragment on host: %s", host)
    subprocess.run(  # noqa: S603 - fixed argv without shell; credentials travel only via stdin
        ["ssh", host, "bash", "-c", REMOTE_INSTALL_SCRIPT],  # noqa: S607 - ssh resolved via PATH on trusted workstation
        input=fragment,
        text=True,
        check=True,
    )
