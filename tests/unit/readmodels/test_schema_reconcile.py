"""`reconcile_read_model_schema` against a live SQLite database.

The scenario the pure generator cannot cover on its own: a table that exists
and has rows, a model that has since gained a field, and the requirement that
the rows survive. `CREATE TABLE IF NOT EXISTS` silently does nothing here,
which is the whole reason this function exists.
"""

from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from eventsource.adapters.sql.readmodel_reconcile import (
    read_table_columns,
    reconcile_read_model_schema,
)
from eventsource.adapters.sql.readmodel_schema import generate_full_schema
from eventsource.ports.readmodels.exceptions import ReadModelSchemaMismatchError
from eventsource.ports.readmodels.model import ReadModel

pytestmark = pytest.mark.sqlite


class OrderSummaryV1(ReadModel):
    """The model as it was when the table was created."""

    __table_name__ = "reconcile_order_summaries"

    order_number: str


class OrderSummaryV2(ReadModel):
    """Same table, two fields later."""

    __table_name__ = "reconcile_order_summaries"

    order_number: str
    status: str = "pending"
    note: str | None = None


class OrderSummaryV3(ReadModel):
    """A field that cannot be added to a table with rows in it."""

    __table_name__ = "reconcile_order_summaries"

    order_number: str
    audited_by: str


@pytest.fixture
async def engine() -> AsyncEngine:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        for statement in generate_full_schema(OrderSummaryV1, dialect="sqlite").split(";"):
            if statement.strip():
                await conn.execute(text(statement))
        await conn.execute(
            text(
                "INSERT INTO reconcile_order_summaries "
                "(id, created_at, updated_at, version, order_number) "
                "VALUES (:id, :now, :now, 1, 'ORD-001')"
            ),
            {"id": str(uuid4()), "now": "2026-08-12T00:00:00+00:00"},
        )
    return engine


async def test_create_table_if_not_exists_does_not_add_the_columns(engine: AsyncEngine) -> None:
    """The defect, stated as a test: re-running the CREATE is not a reconcile."""
    async with engine.begin() as conn:
        for statement in generate_full_schema(OrderSummaryV2, dialect="sqlite").split(";"):
            if statement.strip():
                await conn.execute(text(statement))
        columns = await read_table_columns(conn, OrderSummaryV2.table_name())

    assert "status" not in columns
    assert "note" not in columns


async def test_reconcile_adds_the_missing_columns(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        applied = await reconcile_read_model_schema(conn, OrderSummaryV2)
        columns = await read_table_columns(conn, OrderSummaryV2.table_name())

    assert len(applied) == 2
    assert {"status", "note"} <= columns


async def test_reconcile_preserves_existing_rows(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await reconcile_read_model_schema(conn, OrderSummaryV2)
        rows = (
            await conn.execute(
                text("SELECT order_number, status, note FROM reconcile_order_summaries")
            )
        ).all()

    assert len(rows) == 1
    assert rows[0][0] == "ORD-001"
    assert rows[0][1] == "pending"  # the new column's default reached the existing row
    assert rows[0][2] is None


async def test_reconcile_is_idempotent(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await reconcile_read_model_schema(conn, OrderSummaryV2)
        second_run = await reconcile_read_model_schema(conn, OrderSummaryV2)

    assert second_run == []


async def test_reconcile_accepts_an_engine_and_commits(engine: AsyncEngine) -> None:
    """An engine gets its own transaction; the DDL has to survive it."""
    await reconcile_read_model_schema(engine, OrderSummaryV2)

    async with engine.connect() as conn:
        columns = await read_table_columns(conn, OrderSummaryV2.table_name())

    assert {"status", "note"} <= columns


async def test_reconcile_refuses_a_required_column_with_no_default(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        with pytest.raises(ReadModelSchemaMismatchError) as exc_info:
            await reconcile_read_model_schema(conn, OrderSummaryV3)

        assert exc_info.value.column == "audited_by"
        # Nothing was applied: the check runs before any statement executes.
        columns = await read_table_columns(conn, OrderSummaryV3.table_name())
        assert "audited_by" not in columns


async def test_reconcile_creates_the_table_when_it_does_not_exist(engine: AsyncEngine) -> None:
    class FreshModel(ReadModel):
        __table_name__ = "reconcile_fresh"

        label: str

    async with engine.begin() as conn:
        applied = await reconcile_read_model_schema(conn, FreshModel)
        columns = await read_table_columns(conn, "reconcile_fresh")

    assert any("CREATE TABLE" in statement for statement in applied)
    assert {"id", "label"} <= columns


async def test_dialect_is_read_from_the_connection(engine: AsyncEngine) -> None:
    """No dialect argument: passing one that disagrees with the connection
    would be a second declaration of a fact the connection already carries."""
    async with engine.begin() as conn:
        await reconcile_read_model_schema(conn, OrderSummaryV2)
        types = (
            await conn.execute(
                text(
                    "SELECT type FROM pragma_table_info('reconcile_order_summaries') WHERE name = 'status'"
                )
            )
        ).scalar_one()

    assert types == "TEXT"  # the SQLite map, not VARCHAR(255) from PostgreSQL's
