"""Unit tests for SQLite snapshot schema."""

import pytest

from tests.conftest import AIOSQLITE_AVAILABLE, skip_if_no_aiosqlite

if AIOSQLITE_AVAILABLE:
    import os
    import tempfile

    import aiosqlite

    from eventsource.migrations import get_schema, list_schemas


class TestSQLiteSnapshotSchema:
    """Tests for SQLite snapshot schema template."""

    def test_get_snapshots_schema(self):
        """Test getting SQLite snapshots schema."""
        schema = get_schema("snapshots", "sqlite")

        assert "CREATE TABLE" in schema
        assert "snapshots" in schema
        assert "aggregate_id" in schema
        assert "TEXT" in schema  # SQLite uses TEXT, not UUID

    def test_snapshots_schema_has_unique_constraint(self):
        """Test schema has unique constraint."""
        schema = get_schema("snapshots", "sqlite")

        assert "UNIQUE" in schema
        assert "aggregate_id" in schema
        assert "aggregate_type" in schema

    def test_snapshots_schema_has_required_columns(self):
        """Test schema has all required columns."""
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
        """Test schema has performance indexes."""
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
        """Test schema uses IF NOT EXISTS for idempotency."""
        schema = get_schema("snapshots", "sqlite")

        assert "CREATE TABLE IF NOT EXISTS" in schema
        assert "CREATE INDEX IF NOT EXISTS" in schema

    def test_snapshots_in_list_schemas(self):
        """Test that snapshots schema is listed for SQLite backend."""
        schemas = list_schemas("sqlite")

        assert "snapshots" in schemas

    def test_all_schema_includes_snapshots(self):
        """Test combined schema includes snapshots table."""
        schema = get_schema("all", "sqlite")

        assert "snapshots" in schema
        assert "CREATE TABLE IF NOT EXISTS snapshots" in schema


@pytest.mark.sqlite
@skip_if_no_aiosqlite
class TestSQLiteSnapshotSchemaIntegration:
    """Integration tests for SQLite snapshot schema."""

    @pytest.mark.asyncio
    async def test_create_snapshots_table(self):
        """Test creating snapshots table."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")

            async with aiosqlite.connect(db_path) as conn:
                schema = get_schema("snapshots", "sqlite")
                await conn.executescript(schema)

                # Verify table exists
                cursor = await conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='snapshots'"
                )
                result = await cursor.fetchone()
                assert result is not None

    @pytest.mark.asyncio
    async def test_snapshots_table_idempotent(self):
        """Test schema can be applied multiple times."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")

            async with aiosqlite.connect(db_path) as conn:
                schema = get_schema("snapshots", "sqlite")
                # Apply twice
                await conn.executescript(schema)
                await conn.executescript(schema)
                # Should not raise

                # Verify table still exists
                cursor = await conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='snapshots'"
                )
                result = await cursor.fetchone()
                assert result is not None

    @pytest.mark.asyncio
    async def test_snapshots_table_has_correct_columns(self):
        """Test table has correct column structure."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")

            async with aiosqlite.connect(db_path) as conn:
                schema = get_schema("snapshots", "sqlite")
                await conn.executescript(schema)

                # Get table info
                cursor = await conn.execute("PRAGMA table_info(snapshots)")
                columns = await cursor.fetchall()
                column_names = [col[1] for col in columns]

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
        """Test that indexes are created."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")

            async with aiosqlite.connect(db_path) as conn:
                schema = get_schema("snapshots", "sqlite")
                await conn.executescript(schema)

                # Get index list
                cursor = await conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='snapshots'"
                )
                indexes = await cursor.fetchall()
                index_names = [idx[0] for idx in indexes if idx[0]]

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
        """Test unique constraint on aggregate_id + aggregate_type."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            from datetime import UTC, datetime
            from uuid import uuid4

            async with aiosqlite.connect(db_path) as conn:
                schema = get_schema("snapshots", "sqlite")
                await conn.executescript(schema)

                aggregate_id = str(uuid4())
                now = datetime.now(UTC).isoformat()

                # Insert first snapshot
                await conn.execute(
                    """
                    INSERT INTO snapshots (aggregate_id, aggregate_type, version, schema_version, state, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (aggregate_id, "Order", 1, 1, "{}", now),
                )
                await conn.commit()

                # Try to insert duplicate (should fail due to unique constraint)
                with pytest.raises(aiosqlite.IntegrityError):
                    await conn.execute(
                        """
                        INSERT INTO snapshots (aggregate_id, aggregate_type, version, schema_version, state, created_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (aggregate_id, "Order", 2, 1, "{}", now),
                    )

    @pytest.mark.asyncio
    async def test_all_schema_creates_snapshots_table(self):
        """Test that combined 'all' schema creates snapshots table."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")

            async with aiosqlite.connect(db_path) as conn:
                schema = get_schema("all", "sqlite")
                await conn.executescript(schema)

                # Verify snapshots table exists
                cursor = await conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='snapshots'"
                )
                result = await cursor.fetchone()
                assert result is not None
