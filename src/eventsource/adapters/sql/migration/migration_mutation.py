"""Lifecycle mutation operations mixin for PostgreSQL migration repository."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID

from sqlalchemy import text

from eventsource.adapters.serialization import json_dumps
from eventsource.adapters.sql.migration.migration_helpers import VALID_TRANSITIONS, _token
from eventsource.application.migration.exceptions import (
    InvalidPhaseTransitionError,
    MigrationAlreadyExistsError,
    MigrationNotFoundError,
)
from eventsource.observability.attributes import ATTR_DB_SYSTEM, ATTR_TENANT_ID
from eventsource.ports.migration.models import Migration, MigrationPhase

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

    from eventsource.observability import Tracer


def _get_sql_connection() -> Any:
    import sys

    mod = sys.modules.get("eventsource.adapters.sql.migration.migration")
    if mod is not None and hasattr(mod, "sql_connection"):
        return mod.sql_connection
    from eventsource.adapters._sql.connection import sql_connection

    return sql_connection


class PostgreSQLMigrationMutationMixin:
    """Lifecycle creation and phase transition operations for migration repository."""

    _conn: AsyncConnection | AsyncEngine
    _tracer: Tracer

    if TYPE_CHECKING:

        async def get(self, migration_id: UUID) -> Migration | None: ...
        async def get_by_tenant(self, tenant_id: UUID) -> Migration | None: ...
        def _get_phase_timestamp_updates(self, phase: MigrationPhase) -> str: ...
        def _get_phase_timestamp_params(
            self, phase: MigrationPhase, now: datetime
        ) -> dict[str, Any]: ...

    async def create(self, migration: Migration) -> UUID:
        """Create a new migration record in PENDING phase, ensuring no active tenant migration."""
        attrs = {
            "migration.id": str(migration.id),
            ATTR_TENANT_ID: str(migration.tenant_id),
            ATTR_DB_SYSTEM: "postgresql",
        }
        with self._tracer.span("eventsource.migration_repo.create", attrs):
            existing = await self.get_by_tenant(migration.tenant_id)
            if existing is not None:
                raise MigrationAlreadyExistsError(migration.tenant_id, existing.id)

            now = datetime.now(UTC)
            query = text("""
                INSERT INTO tenant_migrations (
                    id, tenant_id, source_store_id, target_store_id,
                    phase, events_total, events_copied,
                    last_source_position_token, last_target_position_token,
                    config, error_count, created_at, updated_at, created_by
                ) VALUES (
                    :id, :tenant_id, :source_store_id, :target_store_id,
                    :phase, :events_total, :events_copied,
                    :last_source_position_token, :last_target_position_token,
                    :config, :error_count, :created_at, :updated_at, :created_by
                )
            """)
            params = {
                "id": migration.id,
                "tenant_id": migration.tenant_id,
                "source_store_id": migration.source_store_id,
                "target_store_id": migration.target_store_id,
                "phase": migration.phase.value,
                "events_total": migration.events_total,
                "events_copied": migration.events_copied,
                "last_source_position_token": _token(migration.last_source_position),
                "last_target_position_token": _token(migration.last_target_position),
                "config": json_dumps(migration.config.to_dict()),
                "error_count": migration.error_count,
                "created_at": now,
                "updated_at": now,
                "created_by": migration.created_by,
            }
            async with _get_sql_connection()(self._conn, write=True) as conn:
                await conn.execute(query, params)
            return migration.id

    async def update_phase(self, migration_id: UUID, new_phase: MigrationPhase) -> None:
        """Update migration phase with state machine validation and phase timestamps."""
        attrs = {
            "migration.id": str(migration_id),
            "migration.new_phase": new_phase.value,
            ATTR_DB_SYSTEM: "postgresql",
        }
        with self._tracer.span("eventsource.migration_repo.update_phase", attrs):
            migration = await self.get(migration_id)
            if migration is None:
                raise MigrationNotFoundError(migration_id)

            valid_transitions = VALID_TRANSITIONS.get(migration.phase, set())
            if new_phase not in valid_transitions:
                raise InvalidPhaseTransitionError(migration_id, migration.phase, new_phase)

            now = datetime.now(UTC)
            timestamp_updates = self._get_phase_timestamp_updates(new_phase)
            query = text(f"""
                UPDATE tenant_migrations
                SET phase = :phase,
                    {timestamp_updates}
                    updated_at = :updated_at
                WHERE id = :id
            """)  # nosec B608 - all data values are parameterized
            params = {
                "id": migration_id,
                "phase": new_phase.value,
                "updated_at": now,
                **self._get_phase_timestamp_params(new_phase, now),
            }
            async with _get_sql_connection()(self._conn, write=True) as conn:
                await conn.execute(query, params)


__all__ = ["PostgreSQLMigrationMutationMixin"]
