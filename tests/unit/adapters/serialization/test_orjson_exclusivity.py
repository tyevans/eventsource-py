"""Architectural guard: enforce orjson exclusivity across src/eventsource/."""

import ast
from pathlib import Path


def _collect_stdlib_json_imports(root: Path) -> list[str]:
    """Return all occurrences of stdlib ``json`` imports under *root*."""
    violations: list[str] = []
    for py_file in sorted(root.rglob("*.py")):
        tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == "json" or alias.name.startswith("json."):
                        violations.append(f"{py_file}:{node.lineno} import {alias.name}")
            elif (
                isinstance(node, ast.ImportFrom)
                and node.level == 0
                and node.module
                and (node.module == "json" or node.module.startswith("json."))
            ):
                violations.append(f"{py_file}:{node.lineno} from {node.module} import ...")
    return violations


def test_no_stdlib_json_imports_in_eventsource_package() -> None:
    """Zero modules in src/eventsource/ may import the stdlib json module."""
    package_root = Path(__file__).resolve().parents[4] / "src" / "eventsource"
    assert package_root.is_dir(), f"Expected package directory at {package_root}"

    violations = _collect_stdlib_json_imports(package_root)
    assert violations == [], (
        "Stdlib 'json' imports are forbidden in src/eventsource/ (use orjson instead):\n"
        + "\n".join(violations)
    )
