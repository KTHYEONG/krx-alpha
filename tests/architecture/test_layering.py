def test_no_upward_layer_dependency_in_src() -> None:
    # Given: 레이어 랭크 계약 (신규 모듈은 반드시 등재되어야 한다)
    import pathlib

    from tests.architecture.layers import LAYER_RANK
    from tools.agent_skills.dependency_graph import internal_dependencies, repository_source_files

    source_files = repository_source_files(pathlib.Path("."))
    modules = [m for m in source_files if not m.endswith("__init__.py")]

    # Then: 미등재 모듈 0건
    assert [m for m in modules if m not in LAYER_RANK] == []

    # When: 함수 내부 지연 임포트를 포함한 모든 내부 의존 간선을 수집한다
    violations: list[str] = []
    initializer_violations: list[str] = []
    for module in source_files:
        dependencies = internal_dependencies(module, source_files, root=pathlib.Path("."))
        if module.endswith("__init__.py"):
            initializer_violations.extend(
                f"{module} initializer must remain dependency-neutral -> {dep}"
                for dep in sorted(dependencies)
                if dep in LAYER_RANK
            )
            continue
        rank = LAYER_RANK[module]
        for dep in sorted(dependencies):
            if dep not in LAYER_RANK or dep == module:
                continue
            dep_rank = LAYER_RANK[dep]
            same_package = module.split("/")[1] == dep.split("/")[1]
            if dep_rank > rank or (dep_rank == rank and not same_package):
                violations.append(f"{module}(L{rank}) -> {dep}(L{dep_rank})")

    # Then: 상위/동일랭크 교차패키지 의존 0건
    assert initializer_violations == []
    assert violations == []


def test_no_dependency_cycles_in_src() -> None:
    import pathlib

    from tools.agent_skills.dependency_graph import (
        dependency_cycles,
        internal_dependencies,
        repository_source_files,
    )

    source_files = repository_source_files(pathlib.Path("."))
    graph = {module: internal_dependencies(module, source_files, root=pathlib.Path(".")) for module in source_files}

    assert dependency_cycles(graph) == [], f"cyclic components: {dependency_cycles(graph)}"


def test_no_hardcoded_filesystem_paths_outside_config() -> None:
    # Given: 설정 모듈을 제외한 전 src 모듈
    import ast
    import pathlib

    allowed = {"src/core/paths.py"}
    offenders: list[str] = []

    # When: Path("...") 리터럴 인자를 AST 로 수집한다
    for path in sorted(pathlib.Path("src").rglob("*.py")):
        if "__pycache__" in path.parts or path.as_posix() in allowed:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", "")
            if name != "Path":
                continue
            offenders.extend(
                f"{path.as_posix()}:{node.lineno} Path({arg.value!r})"
                for arg in node.args
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str) and arg.value
            )

    # Then: 경로 리터럴은 core/config 단일 소스에만 존재한다
    assert offenders == []


def test_environment_access_confined_to_core_config() -> None:
    # Given: 설정 모듈을 제외한 전 src 모듈
    import pathlib
    import re

    allowed = {"src/core/config.py"}
    pattern = re.compile(r"os\.environ|os\.getenv")
    offenders: list[str] = []

    # When
    for path in sorted(pathlib.Path("src").rglob("*.py")):
        if "__pycache__" in path.parts or path.as_posix() in allowed:
            continue
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if pattern.search(line):
                offenders.append(f"{path.as_posix()}:{lineno}")

    # Then: 자격증명/환경 접근은 단일 진입점만 갖는다
    assert offenders == []


def test_normalize_worker_is_registered_and_env_access_goes_through_config() -> None:
    import pathlib

    from tests.architecture.layers import LAYER_RANK

    assert LAYER_RANK["src/storage/normalize_worker.py"] == 3
    assert LAYER_RANK["src/core/observability.py"] == 0
    for module in ("src/storage/normalize_worker.py", "src/core/observability.py"):
        text = pathlib.Path(module).read_text(encoding="utf-8")
        assert "os.environ" not in text
        assert "os.getenv" not in text
