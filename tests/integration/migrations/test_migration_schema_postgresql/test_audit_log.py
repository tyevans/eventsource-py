"""Tests for migration_audit_log table operations on PostgreSQL."""

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

_INSERT_MIGRATION_SQL = text(
    "INSERT INTO tenant_migrations (id, tenant_id, source_store_id, target_store_id, phase) "
    "VALUES (:id, :tenant_id, 'source', 'target', :phase)"
)
_INSERT_AUDIT_SQL = text(
    "INSERT INTO migration_audit_log (migration_id, event_type) VALUES (:migration_id, :event_type)"
)


class TestMigrationAuditLogTable:
    """Tests for migration_audit_log table operations."""

    async def test_insert_audit_log_entry(self, migration_schema_engine: AsyncEngine) -> None:
        migration_id, tenant_id = uuid4(), uuid4()

        async with migration_schema_engine.begin() as conn:
            await conn.execute(
                _INSERT_MIGRATION_SQL,
                {"id": migration_id, "tenant_id": tenant_id, "phase": "pending"},
            )
            await conn.execute(
                text("""
                INSERT INTO migration_audit_log (
                    migration_id, event_type, old_phase, new_phase, details, operator
                ) VALUES (
                    :migration_id, :event_type, :old_phase, :new_phase, :details, :operator
                )
                """),
                {
                    "migration_id": migration_id,
                    "event_type": "migration_started",
                    "old_phase": None,
                    "new_phase": "pending",
                    "details": '{"initiated_by": "system"}',
                    "operator": "test_user",
                },
            )
            result = await conn.execute(
                text(
                    "SELECT event_type, new_phase, operator FROM migration_audit_log "
                    "WHERE migration_id = :migration_id"
                ),
                {"migration_id": migration_id},
            )
            row = result.fetchone()

        assert row is not None
        assert row[0] == "migration_started"
        assert row[1] == "pending"
        assert row[2] == "test_user"

    async def test_audit_log_event_type_check_constraint(
        self, migration_schema_engine: AsyncEngine
    ) -> None:
        migration_id, tenant_id = uuid4(), uuid4()

        async with migration_schema_engine.begin() as conn:
            await conn.execute(
                _INSERT_MIGRATION_SQL,
                {"id": migration_id, "tenant_id": tenant_id, "phase": "pending"},
            )
            with pytest.raises(Exception):  # noqa: B017 - DB-specific exception
                await conn.execute(
                    _INSERT_AUDIT_SQL,
                    {"migration_id": migration_id, "event_type": "invalid_event_type"},
                )

    async def test_audit_log_cascade_delete(self, migration_schema_engine: AsyncEngine) -> None:
        migration_id, tenant_id = uuid4(), uuid4()

        async with migration_schema_engine.begin() as conn:
            await conn.execute(
                _INSERT_MIGRATION_SQL,
                {"id": migration_id, "tenant_id": tenant_id, "phase": "completed"},
            )
            for event_type in ["migration_started", "phase_changed", "migration_completed"]:
                await conn.execute(
                    _INSERT_AUDIT_SQL,
                    {"migration_id": migration_id, "event_type": event_type},
                )

            await conn.execute(
                text("DELETE FROM tenant_migrations WHERE id = :id"),
                {"id": migration_id},
            )
            result = await conn.execute(
                text("SELECT COUNT(*) FROM migration_audit_log WHERE migration_id = :migration_id"),
                {"migration_id": migration_id},
            )
            count = result.scalar()

        assert count == 0

    async def test_audit_log_ordering(self, migration_schema_engine: AsyncEngine) -> None:
        migration_id, tenant_id = uuid4(), uuid4()

        async with migration_schema_engine.begin() as conn:
            await conn.execute(
                _INSERT_MIGRATION_SQL,
                {"id": migration_id, "tenant_id": tenant_id, "phase": "completed"},
            )
            for event_type in ["migration_started", "phase_changed", "migration_completed"]:
                await conn.execute(
                    _INSERT_AUDIT_SQL,
                    {"migration_id": migration_id, "event_type": event_type},
                )

            result = await conn.execute(
                text(
                    "SELECT event_type FROM migration_audit_log "
                    "WHERE migration_id = :migration_id ORDER BY occurred_at ASC"
                ),
                {"migration_id": migration_id},
            )
            rows = result.fetchall()

        assert len(rows) == 3
        assert rows[0][0] == "migration_started"
        assert rows[2][0] == "migration_completed"
