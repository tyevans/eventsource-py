"""Architectural guard enforcing line ceilings across src/eventsource/, tests/, and examples/.

Locks in module factoring gains (ADR-0002) by verifying that every Python
source file under ``src/eventsource/`` contains strictly fewer than
``MAX_SRC_LINES`` (255) lines, and every Python file under ``tests/`` and
``examples/`` contains strictly fewer than ``MAX_TEST_EXAMPLE_LINES`` (282) lines.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import eventsource

MAX_SRC_LINES = 255
MAX_TEST_EXAMPLE_LINES = 282

REPO_ROOT = Path(__file__).resolve().parents[3]
SRC_EVENTSOURCE_DIR = Path(eventsource.__file__).resolve().parent
TESTS_DIR = REPO_ROOT / "tests"
EXAMPLES_DIR = REPO_ROOT / "examples"


def find_ceiling_violations(
    root: Path = SRC_EVENTSOURCE_DIR,
    max_lines: int = MAX_SRC_LINES,
) -> list[tuple[Path, int]]:
    """Return ``(path, line_count)`` pairs for ``.py`` files with ``line_count >= max_lines``."""
    violations: list[tuple[Path, int]] = []
    for path in sorted(root.rglob("*.py")):
        line_count = len(path.read_text(encoding="utf-8").splitlines())
        if line_count >= max_lines:
            violations.append((path, line_count))
    return violations


def format_violation_report(
    violations: list[tuple[Path, int]],
    base_dir: Path = REPO_ROOT,
    max_lines: int = MAX_SRC_LINES,
) -> str:
    """Format a human-readable diagnostic listing offending paths and line counts."""
    lines = [
        f"{len(violations)} file(s) under src/eventsource/ reached or exceeded the "
        f"<{max_lines}-line architectural ceiling:"
    ]
    for path, count in violations:
        try:
            display_path = path.relative_to(base_dir)
        except ValueError:
            display_path = path
        lines.append(f"  - {display_path}: {count} lines (limit < {max_lines})")
    return "\n".join(lines)


def test_source_files_strictly_below_255_lines() -> None:
    """Every ``.py`` file under ``src/eventsource/`` must have strictly ``< 255`` lines."""
    py_files = list(SRC_EVENTSOURCE_DIR.rglob("*.py"))
    assert py_files, f"Expected Python files under {SRC_EVENTSOURCE_DIR}"

    violations = find_ceiling_violations(SRC_EVENTSOURCE_DIR, max_lines=MAX_SRC_LINES)
    assert not violations, format_violation_report(
        violations, base_dir=REPO_ROOT, max_lines=MAX_SRC_LINES
    )


def test_test_and_example_files_strictly_below_282_lines() -> None:
    """Every ``.py`` file under ``tests/`` and ``examples/`` must have strictly ``< 282`` lines."""
    violations: list[tuple[Path, int]] = []
    for directory in (TESTS_DIR, EXAMPLES_DIR):
        py_files = list(directory.rglob("*.py"))
        assert py_files, f"Expected Python files under {directory}"
        violations.extend(find_ceiling_violations(directory, max_lines=MAX_TEST_EXAMPLE_LINES))

    assert not violations, format_violation_report(
        violations, base_dir=REPO_ROOT, max_lines=MAX_TEST_EXAMPLE_LINES
    )


@pytest.mark.parametrize(
    ("max_lines", "line_count", "expected_violation"),
    [
        (MAX_SRC_LINES, 254, False),
        (MAX_SRC_LINES, 255, True),
        (MAX_SRC_LINES, 256, True),
        (MAX_TEST_EXAMPLE_LINES, 281, False),
        (MAX_TEST_EXAMPLE_LINES, 282, True),
        (MAX_TEST_EXAMPLE_LINES, 283, True),
    ],
)
def test_find_ceiling_violations_boundary(
    tmp_path: Path,
    max_lines: int,
    line_count: int,
    expected_violation: bool,
) -> None:
    """Guard the guard: ``< max_lines`` passes while ``>= max_lines`` is reported."""
    pkg = tmp_path / "src" / "eventsource"
    pkg.mkdir(parents=True)
    sample = pkg / "sample_module.py"
    sample.write_text("\n".join(f"# line {i}" for i in range(line_count)) + "\n", encoding="utf-8")

    violations = find_ceiling_violations(pkg, max_lines=max_lines)
    if expected_violation:
        assert violations == [(sample, line_count)]
        report = format_violation_report(violations, base_dir=tmp_path, max_lines=max_lines)
        assert "src/eventsource/sample_module.py" in report
        assert f"{line_count} lines (limit < {max_lines})" in report
    else:
        assert violations == []
