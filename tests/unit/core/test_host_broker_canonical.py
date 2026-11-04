"""Pin the distributed host broker artifact and its public ABI."""

from __future__ import annotations

import ast
import asyncio
import datetime as dt
import hashlib
import inspect
import json
import sys
import traceback
from pathlib import Path
from typing import Any

import pytest

from deploy.shared import host_broker_contract as contract

CANONICAL_SHA256 = "f1d63e22493355082d1afadb87fbe1a8eed3c8f6aedd8123cd8f5598691637a7"
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
    assert "TokenStoreReadError" in contract.__all__
    assert issubclass(contract.TokenStoreReadError, OSError)
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


def _store(path: Path, *, asynchronous: bool, vendor: str = "ls") -> Any:
    store_type = contract.AsyncTokenStore if asynchronous else contract.TokenStore
    return store_type(
        path,
        vendor=vendor,
        lock_timeout_seconds=1,
        expiry_margin_seconds=600,
        clock=lambda: dt.datetime(2026, 11, 4, tzinfo=dt.UTC),
        issue_throttle_policy="raise",
    )


def _operate(store: Any, *, asynchronous: bool, operation: str, calls: list[str]) -> Any:
    def issue() -> contract.IssuedToken:
        calls.append("issued")
        return contract.IssuedToken("synthetic-token", None)

    async def aissue() -> contract.IssuedToken:
        return issue()

    method = getattr(store, operation)
    args = ("rejected",) if operation == "replace_rejected" else ()
    if asynchronous:
        return asyncio.run(method(*args, aissue))
    return method(*args, issue)


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("operation", ["get_or_issue", "replace_rejected"])
@pytest.mark.parametrize(
    "contents",
    [
        b"",
        b"{sensitive-token",
        b"\xffsensitive-token",
        b"{}",
        b"[]",
        pytest.param(b"[" * 10_000 + b'"sensitive-token"' + b"]" * 10_000, id="excessive-depth"),
    ],
)
def test_invalid_record_never_issues(tmp_path: Path, asynchronous: bool, operation: str, contents: bytes) -> None:
    path = tmp_path / "token.json"
    path.write_bytes(contents)
    calls: list[str] = []
    with pytest.raises(contract.TokenStoreReadError) as error:
        _operate(_store(path, asynchronous=asynchronous), asynchronous=asynchronous, operation=operation, calls=calls)
    assert calls == []
    assert path.read_bytes() == contents
    assert "sensitive-token" not in "".join(traceback.format_exception(error.value))


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("operation", ["get_or_issue", "replace_rejected"])
def test_schema_invalid_record_never_issues(tmp_path: Path, asynchronous: bool, operation: str) -> None:
    path = tmp_path / "token.json"
    contents = json.dumps(
        {
            "schema_version": 1,
            "access_token": "sensitive-token",
            "issued_at": "2026-11-04T00:00:00+00:00",
            "expires_at": None,
            "generation": False,
        }
    )
    path.write_text(contents, encoding="utf-8")
    calls: list[str] = []
    with pytest.raises(contract.TokenStoreReadError, match="schema is invalid"):
        _operate(_store(path, asynchronous=asynchronous), asynchronous=asynchronous, operation=operation, calls=calls)
    assert calls == []
    assert path.read_text(encoding="utf-8") == contents


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("operation", ["get_or_issue", "replace_rejected"])
@pytest.mark.parametrize("failure", ["directory", "permission"])
def test_unreadable_record_never_issues(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, asynchronous: bool, operation: str, failure: str
) -> None:
    path = tmp_path / "token.json"
    if failure == "directory":
        path.mkdir()
    else:
        original = Path.read_text

        def read_text(self: Path, *args: Any, **kwargs: Any) -> str:
            if self == path:
                raise PermissionError("sensitive-token")
            return original(self, *args, **kwargs)

        monkeypatch.setattr(Path, "read_text", read_text)
    calls: list[str] = []
    with pytest.raises(contract.TokenStoreReadError, match="record is unreadable") as error:
        _operate(_store(path, asynchronous=asynchronous), asynchronous=asynchronous, operation=operation, calls=calls)
    assert calls == []
    assert "sensitive-token" not in "".join(traceback.format_exception(error.value))


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("operation", ["get_or_issue", "replace_rejected"])
@pytest.mark.parametrize("contents", [b"", b"sensitive-token", b"nan", b"inf", b"-1", b"\xff", b"0" * 65])
def test_malformed_db_guard_never_issues(tmp_path: Path, asynchronous: bool, operation: str, contents: bytes) -> None:
    path = contract.token_record_path(tmp_path, "dbsec", "synthetic-key")
    guard = contract.issue_guard_path(tmp_path, "dbsec", "synthetic-key")
    guard.write_bytes(contents)
    calls: list[str] = []
    with pytest.raises(contract.TokenStoreReadError, match="guard is malformed") as error:
        _operate(
            _store(path, asynchronous=asynchronous, vendor="dbsec"),
            asynchronous=asynchronous,
            operation=operation,
            calls=calls,
        )
    assert calls == []
    assert not path.exists()
    assert guard.read_bytes() == contents
    assert "sensitive-token" not in "".join(traceback.format_exception(error.value))


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("operation", ["get_or_issue", "replace_rejected"])
@pytest.mark.parametrize("failure", ["directory", "permission"])
def test_unreadable_db_guard_never_issues(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, asynchronous: bool, operation: str, failure: str
) -> None:
    path = contract.token_record_path(tmp_path, "dbsec", "synthetic-key")
    guard = contract.issue_guard_path(tmp_path, "dbsec", "synthetic-key")
    if failure == "directory":
        guard.mkdir()
    else:
        original = contract.os.open

        def open_file(name: Any, *args: Any, **kwargs: Any) -> int:
            if Path(name) == guard:
                raise PermissionError("sensitive-token")
            return original(name, *args, **kwargs)

        monkeypatch.setattr(contract.os, "open", open_file)
    calls: list[str] = []
    with pytest.raises(contract.TokenStoreReadError, match="guard is unreadable") as error:
        _operate(
            _store(path, asynchronous=asynchronous, vendor="dbsec"),
            asynchronous=asynchronous,
            operation=operation,
            calls=calls,
        )
    assert calls == []
    assert not path.exists()
    assert "sensitive-token" not in "".join(traceback.format_exception(error.value))


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("operation", ["get_or_issue", "replace_rejected"])
@pytest.mark.parametrize("guard_exists", [False, True])
def test_absent_record_and_absent_or_valid_old_guard_allow_issuance(
    tmp_path: Path, asynchronous: bool, operation: str, guard_exists: bool
) -> None:
    path = contract.token_record_path(tmp_path, "dbsec", "synthetic-key")
    guard = contract.issue_guard_path(tmp_path, "dbsec", "synthetic-key")
    if guard_exists:
        guard.write_text("0.0", encoding="ascii")
    calls: list[str] = []
    store = _store(path, asynchronous=asynchronous, vendor="dbsec")
    record = _operate(store, asynchronous=asynchronous, operation=operation, calls=calls)
    assert calls == ["issued"]
    assert record.access_token == "synthetic-token"
    assert store.read() == record
    assert float(guard.read_text(encoding="ascii")) == dt.datetime(2026, 11, 4, tzinfo=dt.UTC).timestamp()
