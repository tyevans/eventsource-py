"""`eventsource.ports.readmodels` is importable without sqlalchemy."""

import ast
from pathlib import Path

import eventsource.ports.readmodels as _readmodels_port


def test_public_names_import_from_the_port() -> None:
    from eventsource.ports.readmodels import (
        Filter,
        Query,
        ReadModel,
        ReadModelError,
        ReadModelNotFoundError,
        ReadModelRepository,
        ReadModelVersionConflictError,
    )

    assert getattr(ReadModelRepository, "_is_runtime_protocol", False)
    assert issubclass(ReadModelVersionConflictError, ReadModelError)
    assert issubclass(ReadModelNotFoundError, ReadModelError)
    assert hasattr(ReadModel, "table_name")
    assert hasattr(Query, "with_filter")
    assert hasattr(Filter, "eq")


def test_no_submodule_imports_sqlalchemy_or_adapters() -> None:
    """Statically assert `ports/readmodels/*.py` never imports outward.

    Walks the port package's source with `ast` (resolving relative imports
    against `eventsource.ports.readmodels`) and asserts none of its modules
    names a driver (sqlalchemy/asyncpg/aiosqlite/redis) or an outward package
    (`eventsource.adapters`, `eventsource.readmodels`, `eventsource.application`,
    `eventsource.observability`, `eventsource.testing`) in an import statement.
    """
    forbidden_prefixes = ("sqlalchemy", "asyncpg", "aiosqlite", "redis")
    forbidden_packages = (
        "eventsource.adapters",
        "eventsource.readmodels",
        "eventsource.application",
        "eventsource.observability",
        "eventsource.testing",
    )

    port_dir = Path(_readmodels_port.__file__).parent
    package_parts = ["eventsource", "ports", "readmodels"]
    violations: list[str] = []

    for path in sorted(port_dir.glob("*.py")):
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            module_names: list[str] = []
            if isinstance(node, ast.Import):
                module_names.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                if node.level > 0:
                    base_depth = len(package_parts) - (node.level - 1)
                    if base_depth <= 0:
                        violations.append(
                            f"{path.name}: relative import escapes top-level package (level={node.level})"
                        )
                        continue
                    prefix = ".".join(package_parts[:base_depth])
                    resolved = f"{prefix}.{node.module}" if node.module else prefix
                    module_names.append(resolved)
                elif node.module:
                    module_names.append(node.module)

            for name in module_names:
                if name.startswith(forbidden_prefixes) or name.startswith(forbidden_packages):
                    violations.append(f"{path.name}: imports {name!r}")

    assert not violations, violations
