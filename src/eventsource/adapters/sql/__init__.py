"""Dialect-parameterized SQL adapters (PostgreSQL + SQLite).

Sits beside `adapters/_sql/`, which holds the private dialect and
connection helpers these modules build on.
"""

from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository
from eventsource.adapters.sql.dlq import SQLDLQRepository
from eventsource.adapters.sql.projection import DatabaseProjection
from eventsource.adapters.sql.readmodel_projection import ReadModelProjection
from eventsource.adapters.sql.readmodel_schema import (
    POSTGRESQL_TYPE_MAP,
    SQLITE_TYPE_MAP,
    generate_full_schema,
    generate_indexes,
    generate_schema,
)

__all__ = [
    "POSTGRESQL_TYPE_MAP",
    "SQLITE_TYPE_MAP",
    "DatabaseProjection",
    "ReadModelProjection",
    "SQLCheckpointRepository",
    "SQLDLQRepository",
    "generate_full_schema",
    "generate_indexes",
    "generate_schema",
]
