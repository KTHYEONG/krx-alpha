"""Legacy daemon test path: aggregates the split daemon suites.

Cases live in the focused test_daemon_<theme>.py modules; this module keeps
the historical import path resolving for source-to-test mapping.
"""

from __future__ import annotations

from tests.unit.orchestration import (
    test_daemon_aftermarket as _aftermarket,
    test_daemon_eod as _eod,
    test_daemon_orchestration as _orchestration,
    test_daemon_shutdown as _shutdown,
    test_daemon_supervision as _supervision,
)

__all__ = ["_aftermarket", "_eod", "_orchestration", "_shutdown", "_supervision"]
