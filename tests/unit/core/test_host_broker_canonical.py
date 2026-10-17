"""Pin the distributed host broker artifact and its public ABI."""

from __future__ import annotations

import ast
import hashlib
import inspect
import sys
from pathlib import Path

from deploy.shared import host_broker_contract as contract

CANONICAL_SHA256 = "0bfab4ccae0f0921a32493492683014e1e080e708db3d92473731a5c4eb0b0c1"
CANONICAL_PATH = Path(__file__).resolve().parents[3] / "deploy" / "shared" / "host_broker_contract.py"


def test_canonical_hash_and_marker() -> None:
    content = CANONICAL_PATH.read_bytes()
    assert hashlib.sha256(content).hexdigest() == CANONICAL_SHA256
    assert content.splitlines()[0] == b"# canonical: krx-alpha/deploy/shared/host_broker_contract.py contract=v1"


def test_canonical_has_only_absolute_stdlib_imports() -> None:
    for node in ast.walk(ast.parse(CANONICAL_PATH.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            assert all(alias.name.split(".")[0] in sys.stdlib_module_names for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            assert node.level == 0
            assert node.module is not None
            assert node.module.split(".")[0] in sys.stdlib_module_names


def test_token_store_abi_is_path_bound_with_constructor_policy() -> None:
    for store in (contract.TokenStore, contract.AsyncTokenStore):
        parameters = inspect.signature(store).parameters
        assert list(parameters) == [
            "path",
            "vendor",
            "lock_timeout_seconds",
            "expiry_margin_seconds",
            "clock",
            "issue_throttle_policy",
            "unknown_expiry_max_age_seconds",
            "sleep",
        ]
        assert parameters["issue_throttle_policy"].default is inspect.Parameter.empty
        assert list(inspect.signature(store.get_or_issue).parameters) == ["self", "issue"]
        assert list(inspect.signature(store.replace_rejected).parameters) == ["self", "rejected_token", "issue"]


def test_directory_and_path_abi(tmp_path: Path) -> None:
    assert list(inspect.signature(contract.resolve_host_broker_dir).parameters) == [
        "configured",
        "require_shared",
        "in_container",
    ]
    assert contract.token_record_path(tmp_path, "dbsec", "K").name == "token_dbsec_86be9a55762d.json"
    assert contract.admission_state_path(tmp_path, "toss", "K", "STOCK_TRADING_TREND").name.endswith(
        "_STOCK_TRADING_TREND.state"
    )
