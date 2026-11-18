"""Pause, resume, and failure operations for MigrationCoordinator."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from uuid import UUID

from eventsource.application.migration.coordinator_abort import (  # noqa: F401
    ABORT_LOCK_TIMEOUT_SECONDS,  # re-exported: this module held the constant before the split
    CoordinatorAbortMixin,
)
from eventsource.application.migration.exceptions import MigrationError, MigrationNotFoundError
from eventsource.application.migration.metrics import release_migration_metrics
from eventsource.observability.attributes import ATTR_MIGRATION_ID
from eventsource.ports.migration.models import (
    AuditEventType,
    Migration,
    MigrationAuditEntry,
    MigrationPhase,
)

logger = logging.getLogger(__name__)


class CoordinatorControlMixin(CoordinatorAbortMixin):
    """Mixin providing pause, resume, and failure handling."""

    async def pause_migration(self, migration_id: UUID) -> None:
        """Pause an in-progress migration after the current batch completes."""
        with self._tracer.span("eventsource.coordinator.pause_migration", {ATTR_MIGRATION_ID: str(migration_id)}):  # fmt: skip
            if (migration := await self._migration_repo.get(migration_id)) is None:
                raise MigrationNotFoundError(migration_id)
            if migration.phase.is_terminal:
                raise MigrationError("Cannot pause a completed migration", migration_id=migration_id)  # fmt: skip
            if migration.is_paused:
                logger.debug("Migration %s is already paused", migration_id)
                return
            if copier := self._active_copiers.get(migration_id):
                copier.pause()
            await self._migration_repo.set_paused(migration_id, paused=True, reason="Operator requested")  # fmt: skip
            logger.info("Paused migration %s", migration_id)

    async def resume_migration(self, migration_id: UUID) -> None:
        """Resume a paused migration and its tracked in-process copier if present."""
        with self._tracer.span("eventsource.coordinator.resume_migration", {ATTR_MIGRATION_ID: str(migration_id)}):  # fmt: skip
            if (migration := await self._migration_repo.get(migration_id)) is None:
                raise MigrationNotFoundError(migration_id)
            if not migration.is_paused:
                raise MigrationError("Migration is not paused", migration_id=migration_id)
            if copier := self._active_copiers.get(migration_id):
                copier.resume()
            await self._migration_repo.set_paused(migration_id, paused=False)
            logger.info("Resumed migration %s", migration_id)

    async def _fail_migration(
        self, migration: Migration, error: str, details: dict[str, object] | None = None
    ) -> None:
        """Mark migration as failed and clear the migration state.

        Routing keeps whatever store_id it already has. The caller must not
        call this after a routing switch unless it accepts that the tenant
        stays on the store it was switched to.
        """
        await self._migration_repo.update_phase(migration.id, MigrationPhase.FAILED)
        await self._migration_repo.record_error(migration.id, error)
        await self._routing_repo.clear_migration_state(migration.tenant_id)
        self._router.clear_dual_write_interceptor(migration.tenant_id)
        self._cleanup_migration_resources(migration.id)
        release_migration_metrics(str(migration.id))
        logger.error("Migration %s failed: %s", migration.id, error)
        audit_details: dict[str, object] = {"error": error, **(details or {})}
        entry = MigrationAuditEntry(None, migration.id, AuditEventType.MIGRATION_FAILED, migration.phase, MigrationPhase.FAILED, audit_details, None, datetime.now(UTC))  # fmt: skip
        await self._record_audit(entry)
        self._cleanup_status_queues(migration.id)


__all__ = ["CoordinatorControlMixin"]
