"""Trusted-workstation provisioning of the shared KIS data-key pool fragment.

The fragment at ``/home/ubuntu/quant-secrets/kis-data.env`` is the single
runtime source for KIS data credentials. It is installed once from a trusted
workstation (see ``src.cli.provision_kis_keypool``); CI only validates it and
never writes or prints credential values.
"""

from __future__ import annotations

import logging
import re
import shlex
import subprocess
from pathlib import Path

from src.core.errors import KrxAlphaError

# ~/.quant.env 는 소싱되는 bash 스크립트라 "KIS_APP_KEY=$KIS_TRADE_APP_KEY" 같은
# 셸 변수 참조가 정상 문법이다(실측: 2026-09-16 프로덕션 장애 — 이 리터럴 텍스트를
# 그대로 배포해 KisCredentials 가 빈 문자열로 주입됨). 값 전체가 단일 참조일 때만
# 같은 파일 내 다른 할당을 조회해 해석한다.
_VAR_REF_RE = re.compile(r"^\$\{?([A-Za-z_][A-Za-z0-9_]*)\}?$")

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


def parse_workstation_assignments(source_path: Path, accepted_keys: frozenset[str]) -> dict[str, str]:
    """Collect accepted workstation assignments with shell-syntax normalization.

    A value that is exactly a bare ``$VAR``/``${VAR}`` reference is resolved
    against other assignments in the same file (mirroring bash ``source``
    semantics); an unresolvable reference resolves to empty and is therefore
    treated as absent, so callers fail closed on the true value being missing
    rather than silently shipping the literal reference text.
    """
    raw: dict[str, str] = {}
    accepted_seen: set[str] = set()
    for raw_line in Path(source_path).read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].strip()
        name, sep, value = line.partition("=")
        if not sep:
            continue
        key = name.strip()
        if not key:
            continue
        candidate = value.strip()
        if len(candidate) >= 2 and candidate[0] == candidate[-1] and candidate[0] in ("'", '"'):
            candidate = candidate[1:-1]
        if key in accepted_keys:
            if key in accepted_seen:
                raise KrxAlphaError(f"duplicate accepted data key: {key}")
            accepted_seen.add(key)
        raw[key] = candidate  # 비허용 키의 재할당은 정상 bash 문법이라 마지막 값이 우선한다

    def _resolve(key: str, chain: frozenset[str]) -> str:
        value = raw.get(key, "")
        match = _VAR_REF_RE.match(value)
        if not match:
            return value
        ref = match.group(1)
        if ref in chain:
            raise KrxAlphaError(f"circular variable reference resolving {key}")
        return _resolve(ref, chain | {ref})

    accepted: dict[str, str] = {}
    for key in accepted_seen:
        resolved = _resolve(key, frozenset({key}))
        if resolved:
            accepted[key] = resolved
    return accepted


def build_shared_fragment(source_path: Path) -> str:
    """Build the canonical 17-line shared fragment from a workstation source file.

    Only ``KIS_DATA_<1..5>_{APP_KEY,APP_SECRET,HTS_ID}`` assignments are
    accepted, with or without an ``export `` prefix. Comments, blank lines,
    selectors, account fields, trade keys, primary keys, and unrelated
    settings are ignored. Selectors copied from a local-host allocation are
    never honored; the canonical VPS selectors are always emitted.
    """
    accepted = parse_workstation_assignments(source_path, _ACCEPTED_KEY_SET)
    missing = [key for key in ACCEPTED_KEYS if key not in accepted]
    if missing:
        raise KrxAlphaError(f"missing required data keys: {', '.join(missing)}")
    lines = [CANONICAL_SLOTS_LINE, CANONICAL_HOST_SLOTS_LINE]
    lines.extend(f"{key}={accepted[key]}" for key in ACCEPTED_KEYS)
    return "\n".join(lines) + "\n"


def install_shared_fragment(host: str, fragment: str) -> None:
    """Install the fragment on the VPS over SSH, sending it only via stdin."""
    logger.info("installing shared KIS data fragment on host: %s", host)
    # ssh는 argv[2:]를 공백으로 이어붙여 원격 로그인 셸에 통째로 전달한다. 4개 별도
    # 인자("bash","-c",SCRIPT)로 넘기면 개행 포함 SCRIPT가 재분리되어 -c는 첫 단어만
    # 받고 나머지는 원격 로그인 셸에서 개별 실행돼(우연히 성공은 하지만) 인자 없는
    # "bash -c set"이 끼어들어 셸 환경변수 전체가 표준출력에 유출된다(실측 확인).
    # shlex.quote로 단일 문자열 인자를 만들면 원격 셸이 정확히 "bash -c <SCRIPT>"로만
    # 해석한다.
    subprocess.run(  # noqa: S603 - fixed argv without shell; credentials travel only via stdin
        ["ssh", host, f"bash -c {shlex.quote(REMOTE_INSTALL_SCRIPT)}"],  # noqa: S607
        input=fragment,
        text=True,
        check=True,
    )
