"""Deprecated import path for read-model persistence.

Every name below still resolves, each with a `DeprecationWarning` naming its
new home. This package is removed in 0.8.0.

- `ReadModel`, `Query`, `Filter`, `ReadModelRepository`, `ReadModelError`,
  `OptimisticLockError`, `ReadModelNotFoundError`
  -> `eventsource.ports.readmodels`
- `InMemoryReadModelRepository` -> `eventsource.adapters.memory.readmodels`
- `PostgreSQLReadModelRepository` -> `eventsource.adapters.postgresql.readmodels`
- `SQLiteReadModelRepository` -> `eventsource.adapters.sqlite.readmodels`
- `ReadModelProjection` -> `eventsource.adapters.sql.readmodel_projection`
- `generate_schema`, `generate_indexes`, `generate_full_schema`,
  `POSTGRESQL_TYPE_MAP`, `SQLITE_TYPE_MAP`
  -> `eventsource.adapters.sql.readmodel_schema`

Resolution is lazy: importing this module pulls in no sqlalchemy and no
aiosqlite until a name that needs them is actually read.

Example:
    >>> from uuid import uuid4
    >>> from decimal import Decimal
    >>> from eventsource.ports.readmodels import ReadModel, Query, Filter
    >>> from eventsource.adapters.memory.readmodels import (
    ...     InMemoryReadModelRepository,
    ... )
    >>>
    >>> class OrderSummary(ReadModel):
    ...     order_number: str
    ...     status: str
    ...     total_amount: Decimal
    ...
    >>> repo = InMemoryReadModelRepository(OrderSummary)
    >>> shipped = await repo.find(Query(filters=[Filter.eq("status", "shipped")]))
"""

import importlib
import warnings

_MOVED = {
    "ReadModel": "eventsource.ports.readmodels.model",
    "ReadModelRepository": "eventsource.ports.readmodels.repository",
    "Query": "eventsource.ports.readmodels.query",
    "Filter": "eventsource.ports.readmodels.query",
    "ReadModelError": "eventsource.ports.readmodels.exceptions",
    "OptimisticLockError": "eventsource.ports.readmodels.exceptions",
    "ReadModelNotFoundError": "eventsource.ports.readmodels.exceptions",
    "InMemoryReadModelRepository": "eventsource.adapters.memory.readmodels",
    "PostgreSQLReadModelRepository": "eventsource.adapters.postgresql.readmodels",
    "SQLiteReadModelRepository": "eventsource.adapters.sqlite.readmodels",
    "ReadModelProjection": "eventsource.adapters.sql.readmodel_projection",
    "generate_schema": "eventsource.adapters.sql.readmodel_schema",
    "generate_indexes": "eventsource.adapters.sql.readmodel_schema",
    "generate_full_schema": "eventsource.adapters.sql.readmodel_schema",
    "POSTGRESQL_TYPE_MAP": "eventsource.adapters.sql.readmodel_schema",
    "SQLITE_TYPE_MAP": "eventsource.adapters.sql.readmodel_schema",
}

__all__ = [
    # Base class
    "ReadModel",
    # Protocol
    "ReadModelRepository",
    # Projection integration
    "ReadModelProjection",
    # Query building
    "Query",
    "Filter",
    # Exceptions
    "ReadModelError",
    "OptimisticLockError",
    "ReadModelNotFoundError",
    # In-memory implementation
    "InMemoryReadModelRepository",
    # PostgreSQL implementation
    "PostgreSQLReadModelRepository",
    # SQLite implementation
    "SQLiteReadModelRepository",
    # Schema generation
    "generate_schema",
    "generate_indexes",
    "generate_full_schema",
    "POSTGRESQL_TYPE_MAP",
    "SQLITE_TYPE_MAP",
]


def __getattr__(name: str) -> object:
    try:
        module_name = _MOVED[name]
    except KeyError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from None
    warnings.warn(
        f"eventsource.readmodels.{name} is deprecated; "
        f"import it from {module_name} instead. "
        f"eventsource.readmodels is removed in 0.8.0.",
        DeprecationWarning,
        stacklevel=2,
    )
    return getattr(importlib.import_module(module_name), name)


def __dir__() -> list[str]:
    return sorted(__all__)
