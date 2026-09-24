"""KRX short-code shape validation shared by collection entry points."""

from __future__ import annotations

import re

KRX_SHORT_CODE_PATTERN: str = r"^[0-9A-Z]{6}$"

_SHORT_CODE_RE = re.compile(KRX_SHORT_CODE_PATTERN)


def is_krx_short_code(code: object) -> bool:
    """Return True when ``code`` is a well-formed 6-character KRX short code.

    KRX issues alphanumeric short codes (digits and upper-case letters) to new
    listings, so a digits-only check silently drops recently listed common
    stocks that dominate short-term momentum. Shape says nothing about the
    security class; common-stock eligibility must come from classification
    metadata, never from this check.
    """
    return isinstance(code, str) and _SHORT_CODE_RE.fullmatch(code) is not None
