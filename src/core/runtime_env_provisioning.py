"""Trusted-workstation provisioning of the project runtime env fragment.

The fragment at ``/home/ubuntu/quant-secrets/krx-alpha.env`` is the single
runtime source for project credentials. It is installed once from a trusted
workstation (see ``src.cli.provision_kis_keypool``); CI only validates it and
never writes or prints credential values.
"""

from __future__ import annotations

import logging
import shlex
import subprocess
from dataclasses import dataclass
from pathlib import Path

from src.core.errors import KrxAlphaError
from src.core.kis_keypool_provisioning import parse_workstation_assignments

logger = logging.getLogger(__name__)

REMOTE_RUNTIME_ENV_PATH = "/home/ubuntu/quant-secrets/krx-alpha.env"


@dataclass(frozen=True)
class RuntimeEnvKey:
    target: str
    sources: tuple[str, ...]


RUNTIME_ENV_SPEC: tuple[RuntimeEnvKey, ...] = (
    RuntimeEnvKey(target="KRX_OPENAPI_KEY", sources=("KRX_OPENAPI_KEY",)),
    RuntimeEnvKey(target="TOSS_APP_KEY", sources=("TOSS_APP_KEY",)),
    RuntimeEnvKey(target="TOSS_APP_SECRET", sources=("TOSS_APP_SECRET",)),
    RuntimeEnvKey(target="LS_APP_KEY", sources=("LS_APP_KEY",)),
    RuntimeEnvKey(target="LS_APP_SECRET", sources=("LS_APP_SECRET",)),
    RuntimeEnvKey(target="KIS_APP_KEY", sources=("KIS_APP_KEY",)),
    RuntimeEnvKey(target="KIS_APP_SECRET", sources=("KIS_APP_SECRET",)),
    RuntimeEnvKey(target="KIS_ACCOUNT_NO", sources=("KIS_ACCOUNT_NO",)),
    RuntimeEnvKey(target="KIS_ACCOUNT_PRODUCT_CODE", sources=("KIS_ACCOUNT_PRODUCT_CODE",)),
    RuntimeEnvKey(target="ALERT_GMAIL_USER", sources=("ALERT_GMAIL_USER", "LIVE_ALERT_GMAIL_USER")),
    RuntimeEnvKey(
        target="ALERT_GMAIL_APP_PASSWORD",
        sources=("ALERT_GMAIL_APP_PASSWORD", "LIVE_ALERT_GMAIL_APP_PASSWORD"),
    ),
    RuntimeEnvKey(target="ALERT_GMAIL_TO", sources=("ALERT_GMAIL_TO",)),
)

REMOTE_RUNTIME_INSTALL_SCRIPT = """set -euo pipefail
dest="/home/ubuntu/quant-secrets/krx-alpha.env"
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


def build_runtime_fragment(source_path: Path) -> str:
    """Build the canonical 12-line runtime fragment from a workstation source file."""
    accepted_keys = frozenset(source for key in RUNTIME_ENV_SPEC for source in key.sources)
    parsed = parse_workstation_assignments(source_path, accepted_keys)
    lines: list[str] = []
    for key in RUNTIME_ENV_SPEC:
        value = ""
        for source in key.sources:
            candidate = parsed.get(source, "")
            if candidate:
                value = candidate
                break
        if not value:
            raise KrxAlphaError(f"missing required runtime key: {key.target}")
        lines.append(f"{key.target}={value}")
    return "\n".join(lines) + "\n"


def install_runtime_fragment(host: str, fragment: str) -> None:
    """Install the runtime fragment on the VPS over SSH, sending it only via stdin."""
    logger.info("installing runtime env fragment on host: %s", host)
    # ssh는 argv[2:]를 공백으로 이어붙여 원격 로그인 셸에 통째로 전달한다. shlex.quote로
    # 단일 문자열 인자를 만들어 원격 셸이 정확히 "bash -c <SCRIPT>"로만 해석하게 한다
    # (분리 인자로 넘기면 개행 포함 SCRIPT가 재분리되어 셸 환경변수가 유출됨, 실측 확인).
    subprocess.run(  # noqa: S603 - fixed argv without shell; credentials travel only via stdin
        ["ssh", host, f"bash -c {shlex.quote(REMOTE_RUNTIME_INSTALL_SCRIPT)}"],  # noqa: S607
        input=fragment,
        text=True,
        check=True,
    )
