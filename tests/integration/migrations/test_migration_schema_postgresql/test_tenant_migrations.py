"""Tests for tenant_migrations table operations on PostgreSQL."""

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


_INSERT_SQL = text(
    "INSERT INTO tenant_migrations (id, tenant_id, source_store_id, target_store_id, phase) "
    "VALUES (:id, :tenant_id, :source_store_id, :target_store_id, :phase)"
)


class TestTenantMigrationsTable:
    """Tests for tenant_migrations table operations."""

    async def test_insert_migration(self, migration_schema_engine: AsyncEngine) -> None:
        migration_id, tenant_id = uuid4(), uuid4()

        async with migration_schema_engine.begin() as conn:
            await conn.execute(
                _INSERT_SQL,
                {
                    "id": migration_id,
                    "tenant_id": tenant_id,
                    "source_store_id": "shared-store",
                    "target_store_id": "dedicated-store",
                    "phase": "pending",
                },
            )
            result = await conn.execute(
                text("SELECT id, tenant_id, phase FROM tenant_migrations WHERE id = :id"),
                {"id": migration_id},
            )
            row = result.fetchone()

        assert row is not None
        assert (row[0], row[1], row[2]) == (migration_id, tenant_id, "pending")

    async def test_phase_check_constraint(self, migration_schema_engine: AsyncEngine) -> None:
        migration_id, tenant_id = uuid4(), uuid4()

        async with migration_schema_engine.begin() as conn:
            with pytest.raises(Exception) as exc_info:
                await conn.execute(
                    _INSERT_SQL,
                    {
                        "id": migration_id,
                        "tenant_id": tenant_id,
                        "source_store_id": "source",
                        "target_store_id": "target",
                        "phase": "invalid_phase",
                    },
                )
            msg = str(exc_info.value).lower()
            assert "check" in msg or "violates" in msg

    async def test_active_migration_unique_constraint(
        self, migration_schema_engine: AsyncEngine
    ) -> None:
        tenant_id, migration1_id, migration2_id = uuid4(), uuid4(), uuid4()

        async with migration_schema_engine.begin() as conn:
            await conn.execute(
                _INSERT_SQL,
                {
                    "id": migration1_id,
                    "tenant_id": tenant_id,
                    "source_store_id": "source",
                    "target_store_id": "target",
                    "phase": "bulk_copy",
                },
            )
            with pytest.raises(Exception) as exc_info:
                await conn.execute(
                    _INSERT_SQL,
                    {
                        "id": migration2_id,
                        "tenant_id": tenant_id,
                        "source_store_id": "source2",
                        "target_store_id": "target2",
                        "phase": "pending",
                    },
                )
            msg = str(exc_info.value).lower()
            assert "unique" in msg or "duplicate" in msg

    async def test_completed_migration_allows_new_active(
        self, migration_schema_engine: AsyncEngine
    ) -> None:
        tenant_id, migration1_id, migration2_id = uuid4(), uuid4(), uuid4()

        async with migration_schema_engine.begin() as conn:
            await conn.execute(
                _INSERT_SQL,
                {
                    "id": migration1_id,
                    "tenant_id": tenant_id,
                    "source_store_id": "source",
                    "target_store_id": "target",
                    "phase": "completed",
                },
            )
            await conn.execute(
                _INSERT_SQL,
                {
                    "id": migration2_id,
                    "tenant_id": tenant_id,
                    "source_store_id": "source2",
                    "target_store_id": "target2",
                    "phase": "pending",
                },
            )
            result = await conn.execute(
                text("SELECT COUNT(*) FROM tenant_migrations WHERE tenant_id = :tenant_id"),
                {"tenant_id": tenant_id},
            )
            count = result.scalar()

        assert count == 2

    async def test_updated_at_trigger(self, migration_schema_engine: AsyncEngine) -> None:
        migration_id, tenant_id = uuid4(), uuid4()

        async with migration_schema_engine.begin() as conn:
            await conn.execute(
                _INSERT_SQL,
                {
                    "id": migration_id,
                    "tenant_id": tenant_id,
                    "source_store_id": "source",
                    "target_store_id": "target",
                    "phase": "pending",
                },
            )
            result = await conn.execute(
                text("SELECT updated_at FROM tenant_migrations WHERE id = :id"),
                {"id": migration_id},
            )
            initial_updated_at = result.scalar()

        async with migration_schema_engine.begin() as conn:
            await conn.execute(
                text("UPDATE tenant_migrations SET phase = 'bulk_copy' WHERE id = :id"),
                {"id": migration_id},
            )
            result = await conn.execute(
                text("SELECT updated_at FROM tenant_migrations WHERE id = :id"),
                {"id": migration_id},
            )
            new_updated_at = result.scalar()

        assert initial_updated_at is not None and new_updated_at is not None
        assert new_updated_at >= initial_updated_at
