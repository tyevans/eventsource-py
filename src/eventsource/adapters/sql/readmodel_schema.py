"""Schema generation utilities for read models.

Generates CREATE TABLE SQL statements from ReadModel class definitions.
Supports PostgreSQL and SQLite dialects with appropriate type mappings.

This lives under `adapters/sql/` rather than `ports/` because it hardcodes
`POSTGRESQL_TYPE_MAP` / `SQLITE_TYPE_MAP` and emits dialect-specific
`CREATE TABLE` text -- dialect knowledge belongs to an adapter, not a port.

Example:
    >>> from eventsource.ports.readmodels import ReadModel
    >>> from eventsource.adapters.sql.readmodel_schema import generate_schema
    >>> from decimal import Decimal
    >>>
    >>> class OrderSummary(ReadModel):
    ...     order_number: str
    ...     status: str
    ...     total_amount: Decimal
    ...     item_count: int = 0
    ...
    >>> sql = generate_schema(OrderSummary, dialect="postgresql")
"""

from __future__ import annotations

from eventsource.adapters.sql.readmodel_schema_column import (
    _generate_column,
)
from eventsource.adapters.sql.readmodel_schema_generate import (
    generate_additive_migration,
    generate_full_schema,
    generate_indexes,
    generate_schema,
)
from eventsource.adapters.sql.readmodel_schema_type import (
    POSTGRESQL_TYPE_MAP,
    SQLITE_TYPE_MAP,
    _extract_type,
    _format_default,
    _get_custom_sql_type,
    _is_optional,
)

__all__ = [
    "POSTGRESQL_TYPE_MAP",
    "SQLITE_TYPE_MAP",
    "_extract_type",
    "_format_default",
    "_generate_column",
    "_get_custom_sql_type",
    "_is_optional",
    "generate_additive_migration",
    "generate_full_schema",
    "generate_indexes",
    "generate_schema",
]
