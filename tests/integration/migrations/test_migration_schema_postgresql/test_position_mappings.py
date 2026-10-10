"""Tests for migration_position_mappings table operations on PostgreSQL."""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import uuid4

import pytest
from sqlalchemy import text

from ...conftest import skip_if_no_postgres_infra

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = [
    pytest.mark.integration,
    pytest.mark.postgres,
    skip_if_no_postgres_infra,
]


class TestMigrationPositionMappingsTable:
    """Tests for migration_position_mappings table operations."""

    async def test_insert_position_mapping(
        self,
        migration_schema_engine: AsyncEngine,
    ) -> None:
        """Test inserting a position mapping."""
        migration_id = uuid4()
        tenant_id = uuid4()
        event_id = uuid4()

        async with migration_schema_engine.begin() as conn:
            # Create migration first
            await conn.execute(
                text("""
                INSERT INTO tenant_migrations (
                    id, tenant_id, source_store_id, target_store_id, phase
                ) VALUES (
                    :id, :tenant_id, 'source', 'target', 'bulk_copy'
                )
            """),
                {"id": migration_id, "tenant_id": tenant_id},
            )

            # Insert position mapping
            await conn.execute(
                text("""
                INSERT INTO migration_position_mappings (
                    migration_id, source_position, target_position, event_id
                ) VALUES (
                    :migration_id, :source_position, :target_position, :event_id
                )
            """),
                {
                    "migration_id": migration_id,
                    "source_position": 100,
                    "target_position": 50,
                    "event_id": event_id,
                },
            )

            result = await conn.execute(
                text("""
                SELECT source_position, target_position, event_id
                FROM migration_position_mappings
                WHERE migration_id = :migration_id
            """),
                {"migration_id": migration_id},
            )
            row = result.fetchone()

        assert row is not None
        assert row[0] == 100
        assert row[1] == 50
        assert row[2] == event_id

    async def test_position_mapping_unique_constraint(
        self,
        migration_schema_engine: AsyncEngine,
    ) -> None:
        """Test unique constraint on migration_id + source_position."""
        migration_id = uuid4()
        tenant_id = uuid4()

        async with migration_schema_engine.begin() as conn:
            # Create migration
            await conn.execute(
                text("""
                INSERT INTO tenant_migrations (
                    id, tenant_id, source_store_id, target_store_id, phase
                ) VALUES (
                    :id, :tenant_id, 'source', 'target', 'bulk_copy'
                )
            """),
                {"id": migration_id, "tenant_id": tenant_id},
            )

            # Insert first mapping
            await conn.execute(
                text("""
                INSERT INTO migration_position_mappings (
                    migration_id, source_position, target_position, event_id
                ) VALUES (
                    :migration_id, 100, 50, :event_id
                )
            """),
                {"migration_id": migration_id, "event_id": uuid4()},
            )

            # Try to insert duplicate source_position
            with pytest.raises(Exception) as exc_info:
                await conn.execute(
                    text("""
                    INSERT INTO migration_position_mappings (
                        migration_id, source_position, target_position, event_id
                    ) VALUES (
                        :migration_id, 100, 60, :event_id
                    )
                """),
                    {"migration_id": migration_id, "event_id": uuid4()},
                )

            assert (
                "unique" in str(exc_info.value).lower()
                or "duplicate" in str(exc_info.value).lower()
            )

    async def test_position_mappings_cascade_delete(
        self,
        migration_schema_engine: AsyncEngine,
    ) -> None:
        """Test that position mappings are deleted when migration is deleted."""
        migration_id = uuid4()
        tenant_id = uuid4()

        async with migration_schema_engine.begin() as conn:
            # Create migration
            await conn.execute(
                text("""
                INSERT INTO tenant_migrations (
                    id, tenant_id, source_store_id, target_store_id, phase
                ) VALUES (
                    :id, :tenant_id, 'source', 'target', 'completed'
                )
            """),
                {"id": migration_id, "tenant_id": tenant_id},
            )

            # Insert mappings
            for i in range(5):
                await conn.execute(
                    text("""
                    INSERT INTO migration_position_mappings (
                        migration_id, source_position, target_position, event_id
                    ) VALUES (
                        :migration_id, :source_pos, :target_pos, :event_id
                    )
                """),
                    {
                        "migration_id": migration_id,
                        "source_pos": i * 10,
                        "target_pos": i * 5,
                        "event_id": uuid4(),
                    },
                )

            # Delete migration
            await conn.execute(
                text("DELETE FROM tenant_migrations WHERE id = :id"),
                {"id": migration_id},
            )

            # Check that mappings were cascade deleted
            result = await conn.execute(
                text(
                    "SELECT COUNT(*) FROM migration_position_mappings WHERE migration_id = :migration_id"
                ),
                {"migration_id": migration_id},
            )
            count = result.scalar()

        assert count == 0
