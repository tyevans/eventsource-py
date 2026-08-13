"""Bringing an existing read-model table in line with its model.

`generate_schema` emits `CREATE TABLE IF NOT EXISTS`. Against a database that
already has the table, that statement does nothing at all -- so a field added
to a `ReadModel` never becomes a column, and nothing says so. Tests do not
catch it because tests build their tables from nothing, where the CREATE is
always complete; the gap only opens between a deployed database and a model
that moved on.

This module is opt-in and calls nothing on its own. Nothing in the library
invokes `reconcile_read_model_schema` -- a consumer calls it, at whatever
point in startup it chooses, or does not and uses Alembic instead. The
statements it runs are exactly those `generate_additive_migration` produces,
which is a pure function you can inspect first.
"""

from typing import Literal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from eventsource.adapters._sql.connection import sql_connection
from eventsource.adapters._sql.dialect import Dialect, dialect_of
from eventsource.adapters.sql.readmodel_schema import (
    generate_additive_migration,
    generate_full_schema,
)
from eventsource.ports.readmodels.model import ReadModel

# Column-name introspection per dialect. Both return one row per column with
# the name first; neither is parameterizable by table name in a portable way,
# so the table name is quoted rather than bound.
_COLUMN_QUERIES: dict[Dialect, str] = {
    Dialect.POSTGRESQL: (
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = :table_name AND table_schema = current_schema()"
    ),
    Dialect.SQLITE: "SELECT name FROM pragma_table_info(:table_name)",
}


async def read_table_columns(conn: AsyncConnection, table_name: str) -> set[str]:
    """
    Read the column names a table currently has, lower-cased.

    Returns an empty set when the table does not exist -- both dialects
    report an absent table as zero rows rather than an error.

    Args:
        conn: An active connection
        table_name: Table to introspect

    Returns:
        Lower-cased column names, empty if the table does not exist
    """
    query = _COLUMN_QUERIES[dialect_of(conn)]
    result = await conn.execute(text(query), {"table_name": table_name})
    return {str(row[0]).lower() for row in result}


async def reconcile_read_model_schema(
    conn: AsyncConnection | AsyncEngine,
    model_class: type[ReadModel],
) -> list[str]:
    """
    Add the columns a read model declares and its table lacks.

    Creates the table (with its indexes) when it does not exist; otherwise
    adds missing columns and leaves everything else alone. Additive only:
    nothing is dropped, retyped, or renamed, and a column the model no longer
    declares is left in place. Idempotent -- a second call against a
    reconciled table returns an empty list.

    Call it explicitly, at a point of your choosing. Nothing in the library
    calls it for you, and it is not a substitute for Alembic or whatever else
    owns your schema: it handles the one change that is safe to make
    unattended, so that adding a field to a read model is not silently a
    no-op against a database that already exists.

    The dialect comes from the connection. Nothing here takes a dialect
    argument, because a connection already carries that fact and a second
    place to declare it is a second place for it to be wrong.

    Args:
        conn: An active connection (the caller owns the transaction) or an
            engine (a transaction is opened and committed for you)
        model_class: The ReadModel subclass the table should match

    Returns:
        The statements that were executed, in order. Empty when the table
        already matched the model.

    Raises:
        ReadModelSchemaMismatchError: If a missing column would be NOT NULL
            with no default. Raised before anything is executed, so a refusal
            leaves the table untouched.

    Example:
        >>> # At startup, once the engine exists:
        >>> applied = await reconcile_read_model_schema(engine, OrderSummary)
        >>> for statement in applied:
        ...     logger.info("read model schema: %s", statement)
    """
    async with sql_connection(conn, write=True) as connection:
        dialect: Literal["postgresql", "sqlite"] = (
            "postgresql" if dialect_of(connection) is Dialect.POSTGRESQL else "sqlite"
        )
        table_name = model_class.table_name()
        existing = await read_table_columns(connection, table_name)

        if not existing:
            statements = [
                statement.strip() + ";"
                for statement in generate_full_schema(model_class, dialect).split(";")
                if statement.strip()
            ]
        else:
            # Raises before any execute, so a refusal is not a partial apply.
            statements = generate_additive_migration(model_class, existing, dialect)

        for statement in statements:
            await connection.execute(text(statement))

    return statements


__all__ = ["read_table_columns", "reconcile_read_model_schema"]
