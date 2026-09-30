"""Resolved dependency graph for architecture guards and affected-test checks."""

from __future__ import annotations

import ast
import pathlib


def repository_source_files(root: pathlib.Path) -> list[str]:
    """Discover active source files without importing application modules.

    Args:
        root: Repository root containing the active src directory.

    Returns:
        Sorted repository-relative POSIX Python paths, including package initializers.

    Raises:
        FileNotFoundError: If the active source directory is absent.
        OSError: If source discovery cannot complete.
    """
    src_dir = root / "src"
    if not src_dir.is_dir():
        raise FileNotFoundError(f"{src_dir.as_posix()}: active source directory is absent")
    try:
        paths = {
            p.relative_to(root).as_posix()
            for p in src_dir.rglob("*.py")
            if "__pycache__" not in p.parts
        }
    except OSError as exc:
        raise OSError(f"{src_dir.as_posix()}: source discovery cannot complete: {exc}") from exc
    return sorted(paths)


def _module_map(source_files: list[str]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for path in source_files:
        if not path.endswith(".py"):
            continue
        dotted = path[: -len(".py")].replace("/", ".")
        if dotted.endswith(".__init__"):
            dotted = dotted[: -len(".__init__")]
        mapping[dotted] = path
    return mapping


def _package_of(path: str) -> str:
    if path.endswith("/__init__.py") or path == "__init__.py":
        dotted = path[: -len(".py")].replace("/", ".")
        if dotted.endswith(".__init__"):
            dotted = dotted[: -len(".__init__")]
        return dotted
    dotted = path[: -len(".py")].replace("/", ".") if path.endswith(".py") else path.replace("/", ".")
    if "." in dotted:
        return dotted.rsplit(".", 1)[0]
    return ""


def _is_src_dotted(name: str) -> bool:
    return name == "src" or name.startswith("src.")


def _namespace_prefix(base: str, mapping: dict[str, str]) -> bool:
    prefix = base + "."
    return any(key == base or key.startswith(prefix) for key in mapping)


def internal_dependencies(path: str, source_files: list[str], *, root: pathlib.Path) -> set[str]:
    """Resolve static source dependencies for architecture and affected-test checks.

    Args:
        path: Repository-relative Python file to inspect, including a test file.
        source_files: Active source paths used to resolve modules and packages.
        root: Repository root used to read the file.

    Returns:
        Resolved active source paths referenced by Python import statements.

    Raises:
        SyntaxError: If the inspected Python file is invalid.
        ValueError: If an explicit src import cannot resolve in the active source tree.
        OSError: If the inspected file cannot be read.
    """
    try:
        text = (root / path).read_text(encoding="utf-8")
    except OSError as exc:
        raise OSError(f"{path}: cannot read inspected file: {exc}") from exc
    try:
        tree = ast.parse(text, filename=path)
    except SyntaxError as exc:
        raise SyntaxError(f"{path}: invalid Python at line {exc.lineno}: {exc.msg}") from exc

    mapping = _module_map(source_files)
    package = _package_of(path)
    package_parts = package.split(".") if package else []
    is_src_file = path.startswith("src/")
    dependencies: set[str] = set()

    def _add_parent_initializers(dotted: str) -> None:
        parts = dotted.split(".")
        for i in range(1, len(parts)):
            parent = ".".join(parts[:i])
            parent_path = mapping.get(parent)
            if parent_path is not None and parent_path.endswith("__init__.py") and parent_path != path:
                dependencies.add(parent_path)

    def add_from_base(base: str, aliases: list[ast.alias]) -> None:
        if mapping.get(base) is None and not _namespace_prefix(base, mapping):
            raise ValueError(f"{path}: unresolved src import '{base}'")
        if base in mapping:
            dependencies.add(mapping[base])
        _add_parent_initializers(base)
        for alias in aliases:
            if alias.name == "*":
                continue
            candidate = f"{base}.{alias.name}"
            candidate_path = mapping.get(candidate)
            if candidate_path is not None:
                dependencies.add(candidate_path)

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                name = alias.name
                if not _is_src_dotted(name):
                    continue
                if name not in mapping:
                    raise ValueError(f"{path}: unresolved src import '{name}'")
                dependencies.add(mapping[name])
                _add_parent_initializers(name)
        elif isinstance(node, ast.ImportFrom):
            level = node.level
            module = node.module
            if level == 0:
                if module is None:
                    continue
                if not _is_src_dotted(module):
                    continue
                add_from_base(module, list(node.names))
            else:
                if len(package_parts) < level:
                    if is_src_file:
                        raise ValueError(f"{path}: relative import escapes src package")
                    continue
                base_parts = package_parts[: len(package_parts) - (level - 1)]
                if module is not None:
                    base_parts = [*base_parts, *module.split(".")]
                base = ".".join(base_parts)
                if not base or not _is_src_dotted(base):
                    continue
                add_from_base(base, list(node.names))
    return dependencies


def dependency_cycles(graph: dict[str, set[str]]) -> list[tuple[str, ...]]:
    """Identify mutually dependent source components before module extraction.

    Args:
        graph: Source paths mapped to their resolved source dependencies.

    Returns:
        Sorted tuples of sorted paths for cyclic strongly connected components,
        including explicit self dependencies.

    Raises:
        ValueError: If a dependency is absent from the graph's node set.
    """
    for node, deps in graph.items():
        for dep in deps:
            if dep not in graph:
                raise ValueError(f"unknown dependency '{dep}' referenced by '{node}'")
    nodes = sorted(graph)
    index_of: dict[str, int] = {}
    lowlink: dict[str, int] = {}
    on_stack: set[str] = set()
    stack: list[str] = []
    components: list[list[str]] = []
    counter = 0

    for root in nodes:
        if root in index_of:
            continue
        work: list[tuple[str, int]] = [(root, 0)]
        while work:
            node, child_pos = work[-1]
            if child_pos == 0:
                index_of[node] = counter
                lowlink[node] = counter
                counter += 1
                stack.append(node)
                on_stack.add(node)
            neighbors = sorted(graph[node])
            if child_pos < len(neighbors):
                work[-1] = (node, child_pos + 1)
                child = neighbors[child_pos]
                if child not in index_of:
                    work.append((child, 0))
                elif child in on_stack:
                    lowlink[node] = min(lowlink[node], index_of[child])
            else:
                work.pop()
                if work:
                    parent = work[-1][0]
                    lowlink[parent] = min(lowlink[parent], lowlink[node])
                if lowlink[node] == index_of[node]:
                    component: list[str] = []
                    while True:
                        member = stack.pop()
                        on_stack.discard(member)
                        component.append(member)
                        if member == node:
                            break
                    components.append(component)
    cyclic: list[tuple[str, ...]] = []
    for component in components:
        if len(component) > 1:
            cyclic.append(tuple(sorted(component)))
        elif component[0] in graph[component[0]]:
            cyclic.append((component[0],))
    return sorted(cyclic)
