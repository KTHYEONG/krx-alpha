def test_no_upward_layer_dependency_in_src() -> None:
    # Given: 레이어 랭크 계약 (신규 모듈은 반드시 등재되어야 한다)
    import ast
    import pathlib

    from tests.architecture.layers import LAYER_RANK

    src_root = pathlib.Path("src")
    modules = sorted(
        p.as_posix()
        for p in src_root.rglob("*.py")
        if p.name != "__init__.py" and "__pycache__" not in p.parts
    )

    # Then: 미등재 모듈 0건
    assert [m for m in modules if m not in LAYER_RANK] == []

    # When: 함수 내부 지연 임포트를 포함한 모든 내부 의존 간선을 수집한다
    violations: list[str] = []
    for module in modules:
        rank = LAYER_RANK[module]
        tree = ast.parse(pathlib.Path(module).read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            targets: list[str] = []
            if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("src"):
                targets = [f"{node.module}.{alias.name}" for alias in node.names] + [node.module]
            elif isinstance(node, ast.Import):
                targets = [alias.name for alias in node.names if alias.name.startswith("src")]
            for dotted in targets:
                dep = dotted.replace(".", "/") + ".py"
                if dep not in LAYER_RANK or dep == module:
                    continue
                dep_rank = LAYER_RANK[dep]
                same_package = module.split("/")[1] == dep.split("/")[1]
                if dep_rank > rank or (dep_rank == rank and not same_package):
                    violations.append(f"{module}(L{rank}) -> {dep}(L{dep_rank})")

    # Then: 상위/동일랭크 교차패키지 의존 0건
    assert violations == []


def test_no_hardcoded_filesystem_paths_outside_config() -> None:
    # Given: 설정 모듈을 제외한 전 src 모듈
    import ast
    import pathlib

    allowed = {"src/core/config.py"}
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
