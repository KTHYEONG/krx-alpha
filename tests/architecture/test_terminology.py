"""Repository-wide guard against ambiguous market-session vocabulary."""

from __future__ import annotations

import pathlib
import re

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]

_CANONICAL_TOKENS = ("장전 시간외종가", "장후 시간외종가")
_LEGACY_TOKEN = "overtime_vi_code"
_LEGACY_ALLOWLIST_PATHS = frozenset(
    {"src/marketdata/snapshot_contracts.py", "src/marketdata/snapshot_schema.py"}
)

_KO_PATTERNS = ("시간외", "장후")
_EN_PATTERNS = (
    re.compile(r"after[-_ ]?hours?", re.IGNORECASE),
    re.compile(r"overtime", re.IGNORECASE),
    re.compile(r"extended[-_ ]hours", re.IGNORECASE),
)


def find_forbidden_terms(text: str, *, path: str) -> list[str]:
    """Return forbidden session-vocabulary hits in ``text`` for repository file ``path``.

    Canonical tokens (``장전 시간외종가``, ``장후 시간외종가``) and per-path
    allowlisted legacy identifiers are removed before matching, so only
    ambiguous usages remain.
    """
    cleaned = text
    for token in _CANONICAL_TOKENS:
        cleaned = cleaned.replace(token, "")
    if path in _LEGACY_ALLOWLIST_PATHS:
        cleaned = cleaned.replace(_LEGACY_TOKEN, "")
    hits: list[str] = [pattern for pattern in _KO_PATTERNS if pattern in cleaned]
    for pattern in _EN_PATTERNS:
        match = pattern.search(cleaned)
        if match is not None:
            hits.append(match.group(0))
    return hits


def _scanned_files() -> list[pathlib.Path]:
    patterns = [
        "src/**/*.py",
        "docs/architecture/**/*.md",
        "docs/decisions/**/*.json",
        "docs/decisions/**/*.md",
        "README.md",
        "AGENTS.md",
        ".agents/rules/*.md",
        ".claude/CLAUDE.md",
        ".claude/skills/**/*.md",
    ]
    files: list[pathlib.Path] = []
    for pattern in patterns:
        files.extend(p for p in _REPO_ROOT.glob(pattern) if p.is_file())
    return sorted(set(files))


def test_repository_has_no_ambiguous_session_vocabulary() -> None:
    failures: list[str] = []
    for file in _scanned_files():
        rel = file.relative_to(_REPO_ROOT).as_posix()
        try:
            text = file.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        lines = text.splitlines()
        failures.extend(
            f"{rel}:{lineno}: {hit}"
            for lineno, line in enumerate(lines, start=1)
            for hit in find_forbidden_terms(line, path=rel)
        )
    assert not failures, "forbidden session vocabulary:\n" + "\n".join(failures)


def test_canonical_closing_price_tokens_allowed() -> None:
    assert find_forbidden_terms("장전 시간외종가와 장후 시간외종가", path="docs/architecture/data-flow.md") == []


def test_abolished_session_name_flagged() -> None:
    assert len(find_forbidden_terms("시간외 단일가", path="docs/architecture/data-flow.md")) == 1


def test_standalone_post_market_word_flagged() -> None:
    assert len(find_forbidden_terms("장후 KIS 틱", path="src/realtime/streamer.py")) == 1


def test_english_after_hours_variants_flagged() -> None:
    for text in ("after-hours", "AfterHours", "overtime"):
        assert find_forbidden_terms(text, path="src/realtime/streamer.py"), text


def test_legacy_column_allowed_only_in_contract_module() -> None:
    assert find_forbidden_terms("overtime_vi_code", path="src/marketdata/snapshot_contracts.py") == []
    assert find_forbidden_terms("overtime_vi_code", path="src/marketdata/snapshot_schema.py") == []
    assert len(find_forbidden_terms("overtime_vi_code", path="src/execution/kis_client.py")) == 1
