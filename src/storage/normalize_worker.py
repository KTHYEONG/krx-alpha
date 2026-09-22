"""L0→L1 정규화 자식 프로세스 진입점 (EOD 격리 실행)."""

from __future__ import annotations

import argparse
import json
import pathlib
import subprocess
import sys
from collections.abc import Callable, Sequence

from src.core.config import CollectorSettings, DataQualitySettings, ObservabilitySettings, child_process_env
from src.core.observability import configure_logging
from src.storage.retention import L1NormalizationError, L1WorkerCrashError, normalize_l0_partition

EXIT_DATA_FAULT: int = 3
CHILD_MALLOC_CONF: str = "dirty_decay_ms:0,muzzy_decay_ms:0"


def run_isolated_normalize(
    part_dir: pathlib.Path,
    out_path: pathlib.Path,
    *,
    work_root: pathlib.Path | None = None,
    runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> int:
    """Run L0->L1 normalization in a child process isolated from the daemon.

    Args:
        part_dir: L0 partition directory to normalize.
        out_path: Destination L1 parquet path.
        work_root: Spill directory root forwarded to the child.
        runner: Subprocess entry point (defaults to subprocess.run).

    Returns:
        Number of L1 rows reported by the child.

    Raises:
        L1NormalizationError: When the child exits with EXIT_DATA_FAULT.
        L1WorkerCrashError: When the child crashes or reports no verified row count.
    """
    cmd = [sys.executable, "-m", "src.storage.normalize_worker", "--part", str(part_dir), "--out", str(out_path)]
    if work_root is not None:
        cmd += ["--work-root", str(work_root)]
    # jemalloc dirty-page를 즉시 반환해야 500MB cgroup에서 살아남는다
    env = child_process_env({"_RJEM_MALLOC_CONF": CHILD_MALLOC_CONF})
    run = runner if runner is not None else subprocess.run
    # 자식 [DATA] 로그는 컨테이너 로그로 흘려보내야 하므로 stderr를 계승한다
    result = run(cmd, stdout=subprocess.PIPE, text=True, check=False, env=env)
    payload: dict[str, object] = {}
    lines = (result.stdout or "").splitlines()
    lines = [line for line in lines if line.strip()]
    if lines:
        try:
            parsed: object = json.loads(lines[-1])
        except ValueError:
            parsed = None
        if isinstance(parsed, dict):
            payload = parsed
    rows = payload.get("rows")
    if result.returncode == 0 and isinstance(rows, int):
        return rows
    if result.returncode == EXIT_DATA_FAULT:
        raise L1NormalizationError(str(payload.get("error", f"normalize failed: {part_dir}")))
    raise L1WorkerCrashError(f"normalize worker crashed: part={part_dir} returncode={result.returncode}")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--part", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--work-root", default=None)
    args = parser.parse_args(argv)
    configure_logging(
        "normalize-worker",
        log_dir=CollectorSettings().paths.logs_dir if ObservabilitySettings().persistent_logs else None,
    )
    try:
        rows = normalize_l0_partition(
            pathlib.Path(args.part),
            pathlib.Path(args.out),
            work_root=pathlib.Path(args.work_root) if args.work_root is not None else None,
            dq_settings=DataQualitySettings(),
        )
    except L1NormalizationError as exc:
        print(json.dumps({"error": str(exc)}), flush=True)  # noqa: T201 - child stdout protocol, read by parent
        return EXIT_DATA_FAULT
    print(json.dumps({"rows": rows}), flush=True)  # noqa: T201 - child stdout protocol, read by parent
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
