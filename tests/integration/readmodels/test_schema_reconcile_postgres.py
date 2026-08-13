"""`reconcile_read_model_schema` against PostgreSQL.

The unit suite covers the same behavior on SQLite. This exists because the
two dialects are the one thing that is not shared: PostgreSQL introspects
through `information_schema.columns` and SQLite through `pragma_table_info`,
and PostgreSQL folds unquoted identifiers to lower case where SQLite
preserves them. A single-dialect test cannot see either difference.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from sqlalchemy import text

from eventsource.adapters.sql.readmodel_reconcile import (
    read_table_columns,
    reconcile_read_model_schema,
)
from eventsource.adapters.sql.readmodel_schema import generate_schema
from eventsource.ports.readmodels import ReadModel
from eventsource.ports.readmodels.exceptions import ReadModelSchemaMismatchError

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = [pytest.mark.integration, pytest.mark.postgres]

TABLE = "reconcile_pg_summaries"


class SummaryV1(ReadModel):
    __table_name__ = TABLE

    order_number: str


class SummaryV2(ReadModel):
    __table_name__ = TABLE

    order_number: str
    status: str = "pending"
    note: str | None = None


class SummaryRequired(ReadModel):
    __table_name__ = TABLE

    order_number: str
    audited_by: str


@pytest.fixture
async def seeded_engine(postgres_engine: AsyncEngine) -> AsyncEngine:
    """The V1 table, with a row in it, as a deployed database would have."""
    async with postgres_engine.begin() as conn:
        await conn.execute(text(f"DROP TABLE IF EXISTS {TABLE}"))
        await conn.execute(
            text(generate_schema(SummaryV1, dialect="postgresql", if_not_exists=False))
        )
        await conn.execute(
            text(
                f"INSERT INTO {TABLE} (id, created_at, updated_at, version, order_number) "
                f"VALUES (:id, NOW(), NOW(), 1, 'ORD-001')"
            ),
            {"id": uuid4()},
        )
    return postgres_engine


async def test_reconcile_adds_missing_columns_and_keeps_rows(seeded_engine: AsyncEngine) -> None:
    applied = await reconcile_read_model_schema(seeded_engine, SummaryV2)

    async with seeded_engine.connect() as conn:
        columns = await read_table_columns(conn, TABLE)
        row = (await conn.execute(text(f"SELECT order_number, status, note FROM {TABLE}"))).one()

    assert len(applied) == 2
    assert {"status", "note"} <= columns
    assert row[0] == "ORD-001"
    assert row[1] == "pending"
    assert row[2] is None


async def test_reconcile_is_idempotent(seeded_engine: AsyncEngine) -> None:
    await reconcile_read_model_schema(seeded_engine, SummaryV2)

    assert await reconcile_read_model_schema(seeded_engine, SummaryV2) == []


async def test_reconcile_refuses_a_required_column_with_no_default(
    seeded_engine: AsyncEngine,
) -> None:
    with pytest.raises(ReadModelSchemaMismatchError) as exc_info:
        await reconcile_read_model_schema(seeded_engine, SummaryRequired)

    assert exc_info.value.column == "audited_by"

    async with seeded_engine.connect() as conn:
        assert "audited_by" not in await read_table_columns(conn, TABLE)


async def test_reconcile_creates_an_absent_table(postgres_engine: AsyncEngine) -> None:
    async with postgres_engine.begin() as conn:
        await conn.execute(text(f"DROP TABLE IF EXISTS {TABLE}"))

    applied = await reconcile_read_model_schema(postgres_engine, SummaryV2)

    async with postgres_engine.connect() as conn:
        columns = await read_table_columns(conn, TABLE)

    assert any("CREATE TABLE" in statement for statement in applied)
    assert {"id", "order_number", "status", "note"} <= columns


async def test_column_introspection_finds_only_this_schema(seeded_engine: AsyncEngine) -> None:
    """`information_schema.columns` spans every schema in the database, so the
    query is scoped to `current_schema()`; without that, a same-named table in
    another schema would contribute phantom columns and suppress real adds."""
    async with seeded_engine.begin() as conn:
        await conn.execute(text("CREATE SCHEMA IF NOT EXISTS reconcile_other"))
        await conn.execute(
            text(f"CREATE TABLE IF NOT EXISTS reconcile_other.{TABLE} (status TEXT)")
        )

    applied = await reconcile_read_model_schema(seeded_engine, SummaryV2)

    async with seeded_engine.begin() as conn:
        columns = await read_table_columns(conn, TABLE)
        await conn.execute(text("DROP SCHEMA reconcile_other CASCADE"))

    assert len(applied) == 2
    assert {"status", "note"} <= columns
