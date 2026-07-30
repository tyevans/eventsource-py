"""CLI tests: memory-only quick run end to end, then report over its output."""

from pathlib import Path

from bench.__main__ import main
from bench.core.results import RunResult


def test_run_quick_memory_only(tmp_path: Path) -> None:
    code = main(
        [
            "run",
            "--quick",
            "--backend",
            "memory",
            "--scenario",
            "store.append_batch",
            "--scenario",
            "snapshot.save",
            "--out",
            str(tmp_path),
        ]
    )
    assert code == 0
    files = list(tmp_path.glob("bench-*.json"))
    assert len(files) == 1
    run = RunResult.from_json(files[0].read_text())
    names = {c.scenario for c in run.cells}
    assert names == {"store.append_batch", "snapshot.save"}
    assert all(c.backend == "memory" for c in run.cells)
    assert all(c.status == "ok" for c in run.cells)


def test_report_command(tmp_path: Path) -> None:
    main(
        [
            "run",
            "--quick",
            "--backend",
            "memory",
            "--scenario",
            "store.append_batch",
            "--out",
            str(tmp_path),
        ]
    )
    result_file = next(tmp_path.glob("bench-*.json"))
    out_file = tmp_path / "report.md"
    code = main(["report", str(result_file), "--out", str(out_file)])
    assert code == 0
    text = out_file.read_text()
    assert "# Benchmark Report" in text
    assert "store.append_batch" in text


def test_unknown_filter_values_error() -> None:
    assert main(["run", "--backend", "nope"]) == 2
    assert main(["run", "--scenario", "nope.nope"]) == 2
