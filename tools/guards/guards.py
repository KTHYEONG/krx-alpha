#!/usr/bin/env python3
"""Fail-closed PreToolUse guard for Claude Code Bash calls (stdlib only, run with python3 -I)."""

from __future__ import annotations

import json
import os
import posixpath
import re
import shlex
import sys
import tomllib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, time, timezone
from pathlib import Path
from typing import Any, TextIO
from zoneinfo import ZoneInfo

_MAX_COMMAND_CHARS = 100_000
_MAX_REASON_CHARS = 300
_MAX_TARGET_ECHO_CHARS = 120
_MAX_NESTING_DEPTH = 3
_SPEC_PATH = Path(__file__).resolve().with_name("guards.toml")

_OPERATOR_CHARS = "();&|<>"
_MASK_BASE = 0xF0000
_MASKED = {char: chr(_MASK_BASE + index) for index, char in enumerate(_OPERATOR_CHARS + "\n")}
_UNMASKED = {masked: char for char, masked in _MASKED.items()}

_WRAPPER_WORDS = frozenset(
    {
        "sudo",
        "env",
        "command",
        "nohup",
        "time",
        "nice",
        "exec",
        "eval",
        "builtin",
        "if",
        "then",
        "else",
        "elif",
        "do",
        "while",
        "until",
        "!",
        "{",
        "}",
    }
)
_ASSIGNMENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*=")
_ARMED_VALUES = frozenset({"true", "1", "yes"})
_READ_ONLY_COMPOSE = frozenset({"ps", "logs", "config"})
_RM_LITERAL_TARGETS = frozenset({"/", ".", "..", ".git"})
_RM_EXPANSION_MARKERS = ("~", "$", "`")
_SHELLS = frozenset({"bash", "sh", "zsh", "dash", "ksh"})
_SHELL_COMMAND_FLAG = re.compile(r"-[A-Za-z]*c[A-Za-z]*$")
_SSH_VALUE_OPTIONS = frozenset("-b -c -D -E -e -F -I -i -J -L -l -m -O -o -p -Q -R -S -W -w".split())
_FIND_EXEC_FLAGS = frozenset({"-exec", "-execdir"})
_TEXT_COMMANDS = frozenset(
    {"echo", "printf", "cat", "grep", "egrep", "fgrep", "rg", "head", "tail", "sed", "awk", "less", "more", "wc", "git", "diff", "tee"}
)
_HEREDOC_START = re.compile(r"<<-?\s*(['\"]?)([A-Za-z_][A-Za-z0-9_]*)\1")
_SUBSTITUTION_MARKERS = ("`", "$(", "<(", ">(")

_LIVE_REASON = "Blocked: live trading is human-only. Do not set KRX_ALPHA_EXEC_LIVE_ARMED or KRX_ALPHA_EXEC_MODE=live."
_FORCE_PUSH_REASON = "Blocked: force push or '+' refspec is forbidden. Push without force or ask the user."


@dataclass(frozen=True)
class GuardSpec:
    timezone: str
    freeze_start: time
    freeze_end: time
    freeze_command_patterns: tuple[str, ...]
    order_cli_markers: tuple[str, ...]
    order_subcommands: tuple[str, ...]
    exec_mode_env: str
    allowed_exec_modes: tuple[str, ...]
    live_arm_env: str
    forbidden_force_push_flags: tuple[str, ...]
    coarse_risk_pattern: str


@dataclass(frozen=True)
class GuardDecision:
    allow: bool
    rule: str
    reason: str


@dataclass(frozen=True)
class _SimpleCommand:
    tokens: tuple[str, ...]
    assignments: tuple[str, ...]
    argv: tuple[str, ...]
    wrappers: tuple[str, ...]


_ALLOW = GuardDecision(allow=True, rule="", reason="")


def load_guard_spec(path: Path) -> GuardSpec:
    """Parse the policy TOML; raises ValueError on a missing, ill-typed, or invalid key."""
    with path.open("rb") as handle:
        raw: dict[str, Any] = tomllib.load(handle)
    spec = GuardSpec(
        timezone=_text(raw, "timezone"),
        freeze_start=_clock(raw, "freeze_start"),
        freeze_end=_clock(raw, "freeze_end"),
        freeze_command_patterns=_texts(raw, "freeze_command_patterns", regex=True),
        order_cli_markers=_texts(raw, "order_cli_markers"),
        order_subcommands=_texts(raw, "order_subcommands"),
        exec_mode_env=_text(raw, "exec_mode_env"),
        allowed_exec_modes=_texts(raw, "allowed_exec_modes"),
        live_arm_env=_text(raw, "live_arm_env"),
        forbidden_force_push_flags=_texts(raw, "forbidden_force_push_flags"),
        coarse_risk_pattern=_text(raw, "coarse_risk_pattern", regex=True),
    )
    try:
        ZoneInfo(spec.timezone)
    except (KeyError, ValueError) as exc:
        raise ValueError(f"guard spec timezone is not a valid IANA name: {spec.timezone}") from exc
    if not spec.freeze_start < spec.freeze_end:
        raise ValueError("guard spec requires freeze_start < freeze_end")
    return spec


def evaluate_bash(command: str, now: datetime, project_dir: Path, spec: GuardSpec) -> GuardDecision:
    """Return the first deny decision for a Bash command; raises ValueError on naive now or relative project_dir."""
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    if not project_dir.is_absolute():
        raise ValueError("project_dir must be absolute")
    return _evaluate(command, now, project_dir, spec, 0)


def _evaluate(command: str, now: datetime, project_dir: Path, spec: GuardSpec, depth: int) -> GuardDecision:
    if len(command) > _MAX_COMMAND_CHARS:
        raise ValueError("command exceeds the guard size limit")
    if not command.strip():
        return _ALLOW
    parse_text, unquoted_bodies = _split_heredocs(command)
    try:
        commands = _simple_commands(parse_text)
    except ValueError:
        return _unparsed_decision(parse_text, spec)

    payloads = _nested_payloads(commands)
    if payloads and depth >= _MAX_NESTING_DEPTH:
        return _deny("internal_error", "Blocked: shell, ssh, or eval nesting is too deep to verify. Run the inner command directly.")
    for payload in payloads:
        inner = _evaluate(payload, now, project_dir, spec, depth + 1)
        if not inner.allow:
            return inner

    root = posixpath.normpath(project_dir.as_posix())
    reason = _live_env_reason(commands, spec)
    if reason is not None:
        return _deny("live_env", reason)
    reason = _rm_reason(commands, root) or _bulk_delete_reason(commands, root)
    if reason is not None:
        return _deny("rm", reason)
    reason = _force_push_reason(commands, spec)
    if reason is not None:
        return _deny("force_push", reason)
    reason = _order_reason(commands, spec)
    if reason is not None:
        return _deny("order", reason)
    reason = _freeze_reason(commands, spec, now)
    if reason is not None:
        return _deny("freeze", reason)
    reason = _substitution_reason(parse_text + "\n" + unquoted_bodies, commands, spec)
    if reason is not None:
        return _deny("internal_error", reason)
    return _ALLOW


def main_pretool(stdin: TextIO, stderr: TextIO, now: datetime, project_dir: Path, spec: GuardSpec) -> int:
    """Hook protocol: exit 0 allows, exit 2 blocks with the reason on stderr; never writes stdout."""
    try:
        payload: object = json.loads(stdin.read())
    except (OSError, ValueError):
        stderr.write("guard: invalid hook payload (fail-closed)\n")
        return 2
    if not isinstance(payload, dict):
        stderr.write("guard: hook payload is not a JSON object (fail-closed)\n")
        return 2
    if payload.get("tool_name") != "Bash":
        return 0
    tool_input = payload.get("tool_input")
    command = tool_input.get("command") if isinstance(tool_input, dict) else None
    if not isinstance(command, str):
        stderr.write("guard: missing Bash tool_input.command (fail-closed)\n")
        return 2
    try:
        decision = evaluate_bash(command, now, project_dir, spec)
    except Exception as exc:
        if re.search(spec.coarse_risk_pattern, command) is None:
            return 0
        stderr.write(f"guard internal error (fail-closed): {type(exc).__name__}\n")
        return 2
    if decision.allow:
        return 0
    stderr.write(f"{decision.reason}\n")
    return 2


def _run_pretool() -> int:
    spec = load_guard_spec(_SPEC_PATH)
    project_dir = Path(os.environ.get("CLAUDE_PROJECT_DIR") or str(Path.cwd())).absolute()
    return main_pretool(sys.stdin, sys.stderr, datetime.now(timezone.utc), project_dir, spec)


_HANDLERS: Mapping[str, Callable[[], int]] = {"pretool": _run_pretool}


def main(argv: Sequence[str]) -> int:
    handler = _HANDLERS.get(argv[0]) if argv else None
    if handler is None:
        sys.stderr.write("usage: guards.py pretool\n")
        return 2
    try:
        return handler()
    except Exception as exc:
        sys.stderr.write(f"guard crashed (fail-closed): {type(exc).__name__}\n")
        return 2


def _text(raw: Mapping[str, Any], key: str, regex: bool = False) -> str:
    value = _field(raw, key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"guard spec key '{key}' must be a non-empty string")
    if regex:
        _check_regex(value, key)
    return value


def _texts(raw: Mapping[str, Any], key: str, regex: bool = False) -> tuple[str, ...]:
    value = _field(raw, key)
    if not isinstance(value, list) or not value or not all(isinstance(item, str) and item for item in value):
        raise ValueError(f"guard spec key '{key}' must be a non-empty list of non-empty strings")
    if regex:
        for item in value:
            _check_regex(item, key)
    return tuple(value)


def _clock(raw: Mapping[str, Any], key: str) -> time:
    return time.fromisoformat(_text(raw, key))


def _field(raw: Mapping[str, Any], key: str) -> Any:
    if key not in raw:
        raise ValueError(f"guard spec missing key '{key}'")
    return raw[key]


def _check_regex(pattern: str, key: str) -> None:
    try:
        re.compile(pattern)
    except re.error as exc:
        raise ValueError(f"guard spec key '{key}' has an invalid regex: {exc}") from exc


def _deny(rule: str, reason: str) -> GuardDecision:
    return GuardDecision(allow=False, rule=rule, reason=reason[:_MAX_REASON_CHARS])


def _mask_quoted_text(command: str) -> str:
    # Operator characters inside quotes or escaped must not reach shlex as punctuation tokens,
    # otherwise a quoted "|" would split a recursive rm and hide its remaining targets.
    out: list[str] = []
    quote = ""
    index = 0
    while index < len(command):
        char = command[index]
        if quote == "'":
            if char == "'":
                quote = ""
            out.append(char if char == "'" else _MASKED.get(char, char))
        elif char == "\\":
            following = command[index + 1 : index + 2]
            if following:
                out.append("\\" + _MASKED.get(following, following))
                index += 1
            else:
                out.append(char)
        elif quote == '"':
            if char == '"':
                quote = ""
            out.append(char if char == '"' else _MASKED.get(char, char))
        elif char in "'\"":
            quote = char
            out.append(char)
        elif char == "\n":
            out.append(";")
        else:
            out.append(char)
        index += 1
    return "".join(out)


def _tokenize(command: str) -> list[str]:
    lexer = shlex.shlex(_mask_quoted_text(command), posix=True, punctuation_chars=True)
    lexer.whitespace_split = True
    lexer.commenters = ""
    return list(lexer)


def _is_operator(token: str) -> bool:
    return bool(token) and all(char in _OPERATOR_CHARS for char in token)


def _is_separator(token: str) -> bool:
    if any(char in token for char in ";|()") or "&&" in token:
        return True
    return "&" in token and "<" not in token and ">" not in token


def _unmask(token: str) -> str:
    return "".join(_UNMASKED.get(char, char) for char in token)


def _simple_commands(command: str) -> tuple[_SimpleCommand, ...]:
    tokens = _tokenize(command)
    commands: list[_SimpleCommand] = []
    words: list[str] = []
    redirects: list[str] = []
    index = 0
    while index < len(tokens):
        token = tokens[index]
        index += 1
        if not _is_operator(token):
            words.append(_unmask(token))
        elif _is_separator(token):
            _append_simple(commands, words, redirects)
            words, redirects = [], []
        elif index < len(tokens) and not _is_operator(tokens[index]):
            redirects.append(_unmask(tokens[index]))
            index += 1
    _append_simple(commands, words, redirects)
    return tuple(commands)


def _append_simple(commands: list[_SimpleCommand], words: list[str], redirects: list[str]) -> None:
    if not words and not redirects:
        return
    assignments, argv, wrappers = _split_prefix(words)
    commands.append(
        _SimpleCommand(tokens=tuple(words + redirects), assignments=assignments, argv=argv, wrappers=wrappers)
    )


def _split_prefix(words: Sequence[str]) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    assignments: list[str] = []
    wrappers: list[str] = []
    index = 0
    while index < len(words):
        word = words[index]
        if _ASSIGNMENT.match(word):
            assignments.append(word)
            index += 1
        elif word in _WRAPPER_WORDS:
            wrappers.append(word)
            index += 1
            while index < len(words) and words[index].startswith("-") and words[index] != "-":
                index += 1
        else:
            break
    return tuple(assignments), tuple(words[index:]), tuple(wrappers)


def _command_name(argv: Sequence[str]) -> str:
    return posixpath.basename(argv[0]) if argv else ""


def _split_heredocs(command: str) -> tuple[str, str]:
    """Return (command without heredoc bodies, text of unquoted heredoc bodies that still expand substitutions)."""
    kept: list[str] = []
    unquoted: list[str] = []
    pending: list[tuple[str, bool]] = []
    for line in command.split("\n"):
        if pending:
            tag, quoted = pending[0]
            if line.strip() == tag:
                pending.pop(0)
            elif not quoted:
                unquoted.append(line)
            continue
        kept.append(line)
        pending.extend((match.group(2), bool(match.group(1))) for match in _HEREDOC_START.finditer(line))
    return "\n".join(kept), "\n".join(unquoted)


def _nested_payloads(commands: Sequence[_SimpleCommand]) -> list[str]:
    payloads: list[str] = []
    for command in commands:
        argv = command.argv
        name = _command_name(argv)
        if "eval" in command.wrappers and argv:
            payloads.append(" ".join(argv))
        elif name in _SHELLS:
            for index, arg in enumerate(argv[1:], start=1):
                if _SHELL_COMMAND_FLAG.fullmatch(arg) and index + 1 < len(argv):
                    payloads.append(argv[index + 1])
                    break
        elif name == "ssh":
            remote = _ssh_remote_command(argv[1:])
            if remote:
                payloads.append(remote)
    return payloads


def _ssh_remote_command(args: Sequence[str]) -> str:
    index = 0
    while index < len(args):
        arg = args[index]
        if arg in _SSH_VALUE_OPTIONS:
            index += 2
        elif arg.startswith("-"):
            index += 1
        else:
            return " ".join(args[index + 1 :])
    return ""


def _bulk_delete_reason(commands: Sequence[_SimpleCommand], root: str) -> str | None:
    for command in commands:
        argv = command.argv
        name = _command_name(argv)
        if name == "xargs":
            rest = argv[1:]
            for index, arg in enumerate(rest):
                if posixpath.basename(arg) == "rm" and _has_recursive_flag(rest[index + 1 :]):
                    return _rm_denied("xargs rm -r has targets that cannot be previewed")
        elif name == "find" and _find_deletes(argv):
            for start in _find_start_paths(argv[1:]) or ["."]:
                if any(marker in start for marker in _RM_EXPANSION_MARKERS):
                    problem: str | None = "uses home, variable, or substitution expansion"
                else:
                    problem = _target_problem(start, root)
                if problem is not None:
                    return _rm_denied(f"find start path {start[:_MAX_TARGET_ECHO_CHARS]!r} {problem}")
    return None


def _find_deletes(argv: Sequence[str]) -> bool:
    if "-delete" in argv:
        return True
    return any(
        arg in _FIND_EXEC_FLAGS and index + 1 < len(argv) and posixpath.basename(argv[index + 1]) == "rm"
        for index, arg in enumerate(argv)
    )


def _find_start_paths(args: Sequence[str]) -> list[str]:
    starts: list[str] = []
    for arg in args:
        if arg.startswith("-") or arg in ("(", "!"):
            break
        starts.append(arg)
    return starts


def _live_env_reason(commands: Sequence[_SimpleCommand], spec: GuardSpec) -> str | None:
    arm = spec.live_arm_env.lower()
    mode = spec.exec_mode_env.lower()
    for command in commands:
        for token in command.tokens:
            lowered = token.lower()
            if lowered.startswith(f"{arm}=") and lowered.partition("=")[2] in _ARMED_VALUES:
                return _LIVE_REASON
            if lowered == f"{mode}=live":
                return _LIVE_REASON
    return None


def _rm_reason(commands: Sequence[_SimpleCommand], root: str) -> str | None:
    for command in commands:
        if _command_name(command.argv) != "rm" or not _has_recursive_flag(command.argv[1:]):
            continue
        text = " ".join(command.tokens)
        if any(marker in text for marker in _RM_EXPANSION_MARKERS):
            return _rm_denied("home, variable, or substitution expansion in command")
        for target in _rm_targets(command.argv[1:]):
            problem = _target_problem(target, root)
            if problem is not None:
                return _rm_denied(f"target {target[:_MAX_TARGET_ECHO_CHARS]!r} {problem}")
    return None


def _rm_denied(detail: str) -> str:
    return (
        f"Blocked: recursive rm rejected ({detail}). Preview exact targets and confirm with the user; "
        "allowed targets are project-relative paths under the project dir."
    )


def _has_recursive_flag(args: Sequence[str]) -> bool:
    for arg in args:
        if arg == "--":
            break
        if arg == "--recursive":
            return True
        if arg.startswith("-") and not arg.startswith("--") and arg != "-" and ("r" in arg[1:] or "R" in arg[1:]):
            return True
    return False


def _rm_targets(args: Sequence[str]) -> list[str]:
    targets: list[str] = []
    options_done = False
    for arg in args:
        if not options_done and arg == "--":
            options_done = True
        elif not options_done and arg.startswith("-") and arg != "-":
            continue
        else:
            targets.append(arg)
    return targets


def _target_problem(target: str, root: str) -> str | None:
    if target in _RM_LITERAL_TARGETS:
        return "is a dangerous literal path"
    if "/" not in target and any(char in target for char in "*?["):
        return "is a bare glob"
    normalized = posixpath.normpath(posixpath.join(root, target))
    if normalized == root:
        return "resolves to the project root"
    prefix = root if root.endswith("/") else root + "/"
    if not normalized.startswith(prefix):
        return "resolves outside the project dir"
    relative = normalized[len(prefix) :]
    if relative == ".git" or relative.startswith(".git/"):
        return "targets git metadata"
    return None


def _force_push_reason(commands: Sequence[_SimpleCommand], spec: GuardSpec) -> str | None:
    for command in commands:
        if _command_name(command.argv) != "git":
            continue
        push_args = _git_push_args(command.argv[1:])
        if push_args is None:
            continue
        for arg in push_args:
            if arg in spec.forbidden_force_push_flags or arg.startswith("--force"):
                return _FORCE_PUSH_REASON
            if arg.startswith("+"):
                return _FORCE_PUSH_REASON
            if arg.startswith("-") and not arg.startswith("--") and "f" in arg[1:]:
                return _FORCE_PUSH_REASON
    return None


def _git_push_args(args: Sequence[str]) -> Sequence[str] | None:
    index = 0
    while index < len(args):
        arg = args[index]
        if arg in ("-C", "-c"):
            index += 2
        elif arg.startswith("-"):
            index += 1
        else:
            return args[index + 1 :] if arg == "push" else None
    return None


def _order_reason(commands: Sequence[_SimpleCommand], spec: GuardSpec) -> str | None:
    for command in commands:
        argv = command.argv
        marker_index = next(
            (index for index, token in enumerate(argv) if any(token.endswith(m) for m in spec.order_cli_markers)),
            None,
        )
        if marker_index is None or not any(token in spec.order_subcommands for token in argv[marker_index + 1 :]):
            continue
        modes = [
            assignment.partition("=")[2]
            for assignment in command.assignments
            if assignment.startswith(f"{spec.exec_mode_env}=")
        ]
        if modes and all(mode in spec.allowed_exec_modes for mode in modes):
            continue
        return (
            f"Blocked: order CLI needs an explicit mode on the same command "
            f"({spec.exec_mode_env}=paper or mock prefix). Live orders are human-only."
        )
    return None


def _freeze_reason(commands: Sequence[_SimpleCommand], spec: GuardSpec, now: datetime) -> str | None:
    frozen = [
        command
        for command in commands
        if _command_name(command.argv) not in _TEXT_COMMANDS
        and not _is_read_only_compose(command.argv)
        and any(re.search(pattern, " ".join(command.tokens)) for pattern in spec.freeze_command_patterns)
    ]
    if not frozen:
        return None
    local = now.astimezone(ZoneInfo(spec.timezone)).time()
    if not spec.freeze_start <= local < spec.freeze_end:
        return None
    return (
        f"Blocked: docker/compose/service-restart commands are frozen {spec.freeze_start:%H:%M}~"
        f"{spec.freeze_end:%H:%M} {spec.timezone} (now {local:%H:%M}). "
        "Defer to the 22:00 KST krx-deferred-recreate timer or run after the freeze ends."
    )


def _is_read_only_compose(argv: Sequence[str]) -> bool:
    return len(argv) >= 3 and _command_name(argv) == "docker" and argv[1] == "compose" and argv[2] in _READ_ONLY_COMPOSE


def _substitution_reason(command: str, commands: Sequence[_SimpleCommand], spec: GuardSpec) -> str | None:
    if not any(marker in command for marker in _SUBSTITUTION_MARKERS):
        return None
    joined = " ".join(token for simple in commands for token in simple.tokens)
    coarse = re.compile(spec.coarse_risk_pattern)
    if coarse.search(command) is None and coarse.search(joined) is None:
        return None
    return (
        "Blocked: command or process substitution around a risky command could not be verified. "
        "Run the inner command as a separate, reviewed call."
    )


def _unparsed_decision(command: str, spec: GuardSpec) -> GuardDecision:
    live_pattern = rf"{re.escape(spec.live_arm_env)}=(?:true|1|yes)|{re.escape(spec.exec_mode_env)}=live"
    if re.search(live_pattern, command, re.IGNORECASE):
        return _deny("live_env", _LIVE_REASON)
    if re.search(r"(?<![\w.-])rm(?!\w)", command) and re.search(r"(?<!\S)-[A-Za-z]*[rR]|--recursive", command):
        return _deny("rm", _rm_denied("unparseable command with a recursive flag"))
    flags = "|".join(re.escape(flag) for flag in spec.forbidden_force_push_flags)
    force_pattern = rf"(?<!\S)(?:{flags})(?!\S)|(?<!\S)--force|(?<!\S)\+|(?<!\S)-[A-Za-z]*f"
    if re.search(r"\bgit\b.*\bpush\b", command, re.DOTALL) and re.search(force_pattern, command, re.DOTALL):
        return _deny("force_push", _FORCE_PUSH_REASON)
    return _ALLOW


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
