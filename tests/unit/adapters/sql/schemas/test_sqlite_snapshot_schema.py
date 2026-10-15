"""Unit tests for SQLite snapshot schema."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from tests.conftest import AIOSQLITE_AVAILABLE, skip_if_no_aiosqlite

if AIOSQLITE_AVAILABLE:
    import os
    import tempfile

    import aiosqlite

    from eventsource.adapters.sql.schemas import get_schema, list_schemas


class TestSQLiteSnapshotSchema:
    """Tests for SQLite snapshot schema template."""

    def test_get_snapshots_schema(self):
        schema = get_schema("snapshots", "sqlite")
        assert "CREATE TABLE" in schema
        assert "snapshots" in schema
        assert "aggregate_id" in schema
        assert "TEXT" in schema

    def test_snapshots_schema_has_unique_constraint(self):
        schema = get_schema("snapshots", "sqlite")
        assert "UNIQUE" in schema
        assert "aggregate_id" in schema
        assert "aggregate_type" in schema

    def test_snapshots_schema_has_required_columns(self):
        schema = get_schema("snapshots", "sqlite")
        required_columns = [
            "aggregate_id",
            "aggregate_type",
            "version",
            "schema_version",
            "state",
            "created_at",
        ]
        for column in required_columns:
            assert column in schema, f"Missing column: {column}"

    def test_snapshots_schema_has_indexes(self):
        schema = get_schema("snapshots", "sqlite")
        expected_indexes = [
            "idx_snapshots_aggregate_lookup",
            "idx_snapshots_aggregate_type",
            "idx_snapshots_schema_version",
            "idx_snapshots_created_at",
        ]
        for index in expected_indexes:
            assert index in schema, f"Missing index: {index}"

    def test_snapshots_schema_is_idempotent(self):
        schema = get_schema("snapshots", "sqlite")
        assert "CREATE TABLE IF NOT EXISTS" in schema
        assert "CREATE INDEX IF NOT EXISTS" in schema

    def test_snapshots_in_list_schemas(self):
        assert "snapshots" in list_schemas("sqlite")

    def test_all_schema_includes_snapshots(self):
        schema = get_schema("all", "sqlite")
        assert "snapshots" in schema
        assert "CREATE TABLE IF NOT EXISTS snapshots" in schema


@pytest.mark.sqlite
@skip_if_no_aiosqlite
class TestSQLiteSnapshotSchemaIntegration:
    """Integration tests for SQLite snapshot schema."""

    @pytest.mark.asyncio
    async def test_create_snapshots_table(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            async with aiosqlite.connect(os.path.join(tmpdir, "test.db")) as conn:
                await conn.executescript(get_schema("snapshots", "sqlite"))
                cursor = await conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='snapshots'"
                )
                assert await cursor.fetchone() is not None

    @pytest.mark.asyncio
    async def test_snapshots_table_idempotent(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            async with aiosqlite.connect(os.path.join(tmpdir, "test.db")) as conn:
                schema = get_schema("snapshots", "sqlite")
                await conn.executescript(schema)
                await conn.executescript(schema)
                cursor = await conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='snapshots'"
                )
                assert await cursor.fetchone() is not None

    @pytest.mark.asyncio
    async def test_snapshots_table_has_correct_columns(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            async with aiosqlite.connect(os.path.join(tmpdir, "test.db")) as conn:
                await conn.executescript(get_schema("snapshots", "sqlite"))
                cursor = await conn.execute("PRAGMA table_info(snapshots)")
                column_names = [col[1] for col in await cursor.fetchall()]
                expected_columns = [
                    "id",
                    "aggregate_id",
                    "aggregate_type",
                    "version",
                    "schema_version",
                    "state",
                    "created_at",
                ]
                for expected in expected_columns:
                    assert expected in column_names, f"Missing column: {expected}"

    @pytest.mark.asyncio
    async def test_snapshots_indexes_created(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            async with aiosqlite.connect(os.path.join(tmpdir, "test.db")) as conn:
                await conn.executescript(get_schema("snapshots", "sqlite"))
                cursor = await conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='snapshots'"
                )
                index_names = [idx[0] for idx in await cursor.fetchall() if idx[0]]
                expected_indexes = [
                    "idx_snapshots_aggregate_lookup",
                    "idx_snapshots_aggregate_type",
                    "idx_snapshots_schema_version",
                    "idx_snapshots_created_at",
                ]
                for expected in expected_indexes:
                    assert expected in index_names, f"Missing index: {expected}"

    @pytest.mark.asyncio
    async def test_snapshots_unique_constraint(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            async with aiosqlite.connect(os.path.join(tmpdir, "test.db")) as conn:
                await conn.executescript(get_schema("snapshots", "sqlite"))
                aggregate_id = str(uuid4())
                now = datetime.now(UTC).isoformat()
                sql = (
                    "INSERT INTO snapshots (aggregate_id, aggregate_type, version, "
                    "schema_version, state, created_at) VALUES (?, ?, ?, ?, ?, ?)"
                )
                await conn.execute(sql, (aggregate_id, "Order", 1, 1, "{}", now))
                await conn.commit()

                with pytest.raises(aiosqlite.IntegrityError):
                    await conn.execute(sql, (aggregate_id, "Order", 2, 1, "{}", now))

    @pytest.mark.asyncio
    async def test_all_schema_creates_snapshots_table(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            async with aiosqlite.connect(os.path.join(tmpdir, "test.db")) as conn:
                await conn.executescript(get_schema("all", "sqlite"))
                cursor = await conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='snapshots'"
                )
                assert await cursor.fetchone() is not None
