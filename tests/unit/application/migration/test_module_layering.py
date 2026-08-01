"""The migration error modules form a one-way dependency DAG (ADR 0044)."""

import ast
from pathlib import Path

import pytest

MIGRATION_DIR = (
    Path(__file__).resolve().parents[4] / "src" / "eventsource" / "application" / "migration"
)

# Deliberate closed-world contract: the DAG is a fixed four-layer design
# (ADR 0044). A new module in this family must be added here consciously,
# not picked up implicitly.
# module -> the migration-package modules it is allowed to import from
ALLOWED: dict[str, set[str]] = {
    "error_classification": set(),
    "exceptions": {"error_classification"},
    "circuit_breaker": {"error_classification", "exceptions"},
    "error_handling": {"error_classification", "exceptions", "circuit_breaker"},
}


def _migration_imports(module_name: str) -> set[str]:
    """Names of sibling migration modules imported by module_name."""
    source = (MIGRATION_DIR / f"{module_name}.py").read_text()
    tree = ast.parse(source)
    found: set[str] = set()
    prefix = "eventsource.application.migration."
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            if node.module.startswith(prefix):
                found.add(node.module[len(prefix) :].split(".")[0])
            elif node.level and node.module in ALLOWED:
                found.add(node.module)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith(prefix):
                    found.add(alias.name[len(prefix) :].split(".")[0])
    return found


@pytest.mark.parametrize("module_name", sorted(ALLOWED))
def test_module_imports_stay_within_its_layer(module_name: str) -> None:
    violations = _migration_imports(module_name) - ALLOWED[module_name]
    assert not violations, f"{module_name}.py imports disallowed siblings: {sorted(violations)}"
