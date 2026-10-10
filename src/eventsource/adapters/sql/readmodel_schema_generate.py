"""Schema statement generation utilities for read models."""

from __future__ import annotations

from collections.abc import Collection
from typing import Any, Literal

from eventsource.adapters.sql.readmodel_schema_column import _generate_column
from eventsource.adapters.sql.readmodel_schema_type import (
    POSTGRESQL_TYPE_MAP,
    SQLITE_TYPE_MAP,
)
from eventsource.ports.readmodels.exceptions import ReadModelSchemaMismatchError
from eventsource.ports.readmodels.model import ReadModel


def generate_schema(
    model_class: type[ReadModel],
    dialect: Literal["postgresql", "sqlite"] = "postgresql",
    if_not_exists: bool = True,
) -> str:
    """Generate CREATE TABLE SQL for a ReadModel class.

    Analyzes the Pydantic model fields and generates appropriate SQL
    column definitions for the specified database dialect.

    Args:
        model_class: The ReadModel subclass to generate schema for
        dialect: Database dialect ('postgresql' or 'sqlite')
        if_not_exists: Include IF NOT EXISTS clause (default True)

    Returns:
        CREATE TABLE SQL statement
    """
    type_map = POSTGRESQL_TYPE_MAP if dialect == "postgresql" else SQLITE_TYPE_MAP
    table_name = model_class.table_name()

    # Build column definitions
    columns = []
    for field_name, field_info in model_class.model_fields.items():
        column_sql = _generate_column(field_name, field_info, type_map, dialect)
        columns.append(column_sql)

    # Build CREATE TABLE statement
    exists_clause = "IF NOT EXISTS " if if_not_exists else ""
    columns_sql = ",\n    ".join(columns)

    return f"""CREATE TABLE {exists_clause}{table_name} (
    {columns_sql}
);"""


def generate_indexes(
    model_class: type[ReadModel],
    dialect: Literal["postgresql", "sqlite"] = "postgresql",
) -> list[str]:
    """Generate CREATE INDEX statements for a ReadModel class.

    Generates standard indexes (soft delete, common query patterns) plus
    any custom indexes defined in the model's __indexes__ attribute.

    Args:
        model_class: The ReadModel subclass to generate indexes for
        dialect: Database dialect ('postgresql' or 'sqlite')

    Returns:
        List of CREATE INDEX SQL statements
    """
    table_name = model_class.table_name()
    indexes = []

    # Standard index on deleted_at for soft delete queries
    if dialect == "postgresql":
        indexes.append(
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_deleted "
            f"ON {table_name}(deleted_at) WHERE deleted_at IS NOT NULL;"
        )
    else:
        indexes.append(
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_deleted ON {table_name}(deleted_at);"
        )

    # Custom indexes from model metadata
    custom_indexes: list[dict[str, Any]] = getattr(model_class, "__indexes__", [])
    for idx_spec in custom_indexes:
        fields = idx_spec.get("fields", [])
        if not fields:
            continue

        idx_name = idx_spec.get("name", f"idx_{table_name}_{'_'.join(fields)}")
        fields_sql = ", ".join(fields)
        where_clause = idx_spec.get("where", "")

        if where_clause and dialect == "postgresql":
            indexes.append(
                f"CREATE INDEX IF NOT EXISTS {idx_name} "
                f"ON {table_name}({fields_sql}) WHERE {where_clause};"
            )
        else:
            indexes.append(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table_name}({fields_sql});")

    return indexes


def generate_full_schema(
    model_class: type[ReadModel],
    dialect: Literal["postgresql", "sqlite"] = "postgresql",
) -> str:
    """Generate complete schema including table and indexes.

    Combines CREATE TABLE and CREATE INDEX statements into a single
    SQL script that can be executed to set up the complete schema
    for a read model.

    Args:
        model_class: The ReadModel subclass to generate schema for
        dialect: Database dialect ('postgresql' or 'sqlite')

    Returns:
        Complete schema SQL with table and indexes
    """
    table_sql = generate_schema(model_class, dialect)
    index_sqls = generate_indexes(model_class, dialect)

    parts = [table_sql] + index_sqls
    return "\n\n".join(parts)


def generate_additive_migration(
    model_class: type[ReadModel],
    existing_columns: Collection[str],
    dialect: Literal["postgresql", "sqlite"] = "postgresql",
) -> list[str]:
    """Emit ALTER TABLE ADD COLUMN statements for fields the table lacks.

    Only additive changes are emitted -- dropped or renamed columns are
    ignored because existing tables may contain historical data that
    cannot be safely dropped without an explicit migration plan.

    Args:
        model_class: The ReadModel subclass to generate migration for
        existing_columns: Columns already present on the database table
        dialect: Database dialect ('postgresql' or 'sqlite')

    Returns:
        List of SQL ALTER TABLE statements (empty if table already matches)

    Raises:
        ReadModelSchemaMismatchError: If the existing table has no primary
            key (and so is not this model's table), or if a missing column
            is required and has no default.
    """
    type_map = POSTGRESQL_TYPE_MAP if dialect == "postgresql" else SQLITE_TYPE_MAP
    table_name = model_class.table_name()
    present = {column.lower() for column in existing_columns}

    if "id" not in present:
        raise ReadModelSchemaMismatchError(
            table_name,
            "id",
            "the table has no primary key column, so it is not this model's "
            "table -- create it with generate_full_schema() rather than "
            "reconciling it",
        )

    statements = []
    for field_name, field_info in model_class.model_fields.items():
        if field_name.lower() in present:
            continue

        column_sql = _generate_column(field_name, field_info, type_map, dialect)

        # Both halves come from _generate_column rather than being re-derived
        # here, so nullability and defaults cannot disagree with what a
        # CREATE TABLE for the same model would have produced.
        if "NOT NULL" in column_sql and "DEFAULT" not in column_sql:
            raise ReadModelSchemaMismatchError(
                table_name,
                field_name,
                f"{field_name} is required and has no default, so it cannot "
                f"be added to a table that may already have rows -- give the "
                f"field a default or make it optional",
            )

        statements.append(f"ALTER TABLE {table_name} ADD COLUMN {column_sql};")

    return statements


__all__ = [
    "generate_additive_migration",
    "generate_full_schema",
    "generate_indexes",
    "generate_schema",
]
