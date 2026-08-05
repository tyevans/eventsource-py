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
        ReadModelRepositoryProtocol,
        ReadModelVersionConflictError,
    )

    assert ReadModelRepositoryProtocol is ReadModelRepository
    assert issubclass(ReadModelVersionConflictError, ReadModelError)
    assert issubclass(ReadModelNotFoundError, ReadModelError)
    assert hasattr(ReadModel, "table_name")
    assert hasattr(Query, "with_filter")
    assert hasattr(Filter, "eq")


def test_no_submodule_imports_sqlalchemy_or_adapters() -> None:
    """Statically assert `ports/readmodels/*.py` never imports outward.

    A runtime `sys.modules` check is unimplementable here: any `import
    eventsource.ports.readmodels...` first runs `eventsource/__init__.py`,
    which eagerly imports `eventsource.adapters.postgresql` (and other
    sqlalchemy-backed adapters) regardless of readmodels -- a pre-existing
    condition tracked as a lazy-`__init__` backlog item, unrelated to this
    port. So this test walks the port package's source with `ast` instead of
    importing it, and asserts none of its modules names a driver
    (sqlalchemy/asyncpg/aiosqlite/redis) or an outward package
    (`eventsource.adapters`, `eventsource.readmodels`) in an import
    statement. The import-linter Tier-0 contract ("eventsource.ports" as a
    whole package, in `pyproject.toml`) is the primary static guarantee;
    this test is its colocated, pytest-visible mirror.
    """
    forbidden_prefixes = ("sqlalchemy", "asyncpg", "aiosqlite", "redis")
    forbidden_packages = ("eventsource.adapters", "eventsource.readmodels")

    port_dir = Path(_readmodels_port.__file__).parent
    violations: list[str] = []

    for path in sorted(port_dir.glob("*.py")):
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            module_names: list[str] = []
            if isinstance(node, ast.Import):
                module_names.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                module_names.append(node.module)

            for name in module_names:
                if name.startswith(forbidden_prefixes) or name.startswith(forbidden_packages):
                    violations.append(f"{path.name}: imports {name!r}")

    assert not violations, violations
