"""Lifecycle mutation operations mixin for PostgreSQL migration repository.

Governed by:
- ADR-0002 (<500 lines per module)
- ADR-0007 (Hexagonal Architecture)
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID

from sqlalchemy import text

from eventsource.adapters.sql.migration.migration_helpers import (
    VALID_TRANSITIONS,
    _token,
)
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
            self,
            phase: MigrationPhase,
            now: datetime,
        ) -> dict[str, Any]: ...

    async def create(self, migration: Migration) -> UUID:
        """Create a new migration record.

        Creates a new migration in PENDING phase. Validates that no
        active migration exists for the tenant to prevent duplicate
        migrations.

        Args:
            migration: Migration instance to persist

        Returns:
            The migration ID

        Raises:
            MigrationAlreadyExistsError: If active migration exists for tenant
        """
        with self._tracer.span(
            "eventsource.migration_repo.create",
            {
                "migration.id": str(migration.id),
                ATTR_TENANT_ID: str(migration.tenant_id),
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            # Check for existing active migration
            existing = await self.get_by_tenant(migration.tenant_id)
            if existing is not None:
                raise MigrationAlreadyExistsError(
                    migration.tenant_id,
                    existing.id,
                )

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
                "config": json.dumps(migration.config.to_dict()),
                "error_count": migration.error_count,
                "created_at": now,
                "updated_at": now,
                "created_by": migration.created_by,
            }

            async with _get_sql_connection()(self._conn, write=True) as conn:
                await conn.execute(query, params)

            return migration.id

    async def update_phase(
        self,
        migration_id: UUID,
        new_phase: MigrationPhase,
    ) -> None:
        """Update migration phase with validation.

        Validates the phase transition against the state machine before
        applying the update. Also updates phase-specific timestamps
        (e.g., started_at, bulk_copy_started_at, etc.).

        Args:
            migration_id: UUID of the migration
            new_phase: New phase to transition to

        Raises:
            MigrationNotFoundError: If migration not found
            InvalidPhaseTransitionError: If transition is invalid
        """
        with self._tracer.span(
            "eventsource.migration_repo.update_phase",
            {
                "migration.id": str(migration_id),
                "migration.new_phase": new_phase.value,
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            # Get current phase
            migration = await self.get(migration_id)
            if migration is None:
                raise MigrationNotFoundError(migration_id)

            # Validate transition
            valid_transitions = VALID_TRANSITIONS.get(migration.phase, set())
            if new_phase not in valid_transitions:
                raise InvalidPhaseTransitionError(
                    migration_id,
                    migration.phase,
                    new_phase,
                )

            # Build update query with phase-specific timestamps
            now = datetime.now(UTC)
            timestamp_updates = self._get_phase_timestamp_updates(new_phase)

            # timestamp_updates is hardcoded SQL from internal enum mapping
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
