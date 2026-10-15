"""Pause, resume, abort, and failure operations for MigrationCoordinator."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID

from eventsource.application.migration.exceptions import MigrationError, MigrationNotFoundError
from eventsource.application.migration.metrics import release_migration_metrics
from eventsource.observability import Tracer
from eventsource.observability.attributes import ATTR_MIGRATION_ID
from eventsource.ports.migration.models import (
    AuditEventType,
    Migration,
    MigrationAuditEntry,
    MigrationPhase,
    MigrationResult,
)

if TYPE_CHECKING:
    from eventsource.application.migration.bulk_copier import BulkCopier
    from eventsource.application.migration.router import TenantStoreRouter
    from eventsource.ports.migration.repositories import (
        MigrationRepository,
        TenantRoutingRepository,
    )

logger = logging.getLogger(__name__)


class CoordinatorControlMixin:
    """Mixin providing pause, resume, abort, and failure handling."""

    _tracer: Tracer
    _migration_repo: MigrationRepository
    _routing_repo: TenantRoutingRepository
    _router: TenantStoreRouter
    _active_copiers: dict[UUID, BulkCopier]
    _active_tasks: dict[UUID, asyncio.Task[None]]
    _lag_trackers: dict[UUID, Any]
    _target_stores: dict[UUID, Any]
    _interceptors: dict[UUID, Any]
    _consistency_reports: dict[UUID, Any]
    _subscription_summaries: dict[UUID, Any]

    def _calculate_duration(self, migration: Migration | None) -> float:
        raise NotImplementedError  # fmt: skip

    async def _record_audit(self, entry: MigrationAuditEntry) -> None:
        raise NotImplementedError  # fmt: skip

    def _cleanup_status_queues(self, migration_id: UUID) -> None:
        raise NotImplementedError  # fmt: skip

    def _cleanup_migration_resources(self, migration_id: UUID) -> None:
        """Clean up resources associated with a migration."""
        for d in (self._lag_trackers, self._target_stores, self._interceptors, self._consistency_reports, self._subscription_summaries):  # fmt: skip
            d.pop(migration_id, None)

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

    async def abort_migration(
        self, migration_id: UUID, reason: str | None = None
    ) -> MigrationResult:
        """Abort and rollback a migration, restoring normal tenant routing."""
        with self._tracer.span("eventsource.coordinator.abort_migration", {ATTR_MIGRATION_ID: str(migration_id)}):  # fmt: skip
            if (migration := await self._migration_repo.get(migration_id)) is None:
                raise MigrationNotFoundError(migration_id)
            if migration.phase.is_terminal:
                raise MigrationError("Cannot abort a completed migration", migration_id=migration_id)  # fmt: skip
            if copier := self._active_copiers.pop(migration_id, None):
                copier.cancel()
            if (task := self._active_tasks.pop(migration_id, None)) and not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

            await self._routing_repo.clear_migration_state(migration.tenant_id)
            self._router.clear_dual_write_interceptor(migration.tenant_id)
            self._cleanup_migration_resources(migration_id)
            release_migration_metrics(str(migration_id))
            await self._migration_repo.update_phase(migration_id, MigrationPhase.ABORTED)
            if reason:
                await self._migration_repo.record_error(migration_id, f"Aborted: {reason}")
            logger.info("Aborted migration %s: %s", migration_id, reason)
            entry = MigrationAuditEntry(None, migration_id, AuditEventType.MIGRATION_ABORTED, migration.phase, MigrationPhase.ABORTED, {"reason": reason} if reason else None, None, datetime.now(UTC))  # fmt: skip
            await self._record_audit(entry)
            self._cleanup_status_queues(migration_id)
            updated = await self._migration_repo.get(migration_id)
            return MigrationResult(
                migration_id=migration_id,
                success=False,
                duration_seconds=self._calculate_duration(updated),
                events_migrated=updated.events_copied if updated else 0,
                final_phase=MigrationPhase.ABORTED,
                error_message=reason,
            )

    async def _fail_migration(self, migration: Migration, error: str) -> None:
        """Mark migration as failed and restore tenant routing to normal state."""
        await self._migration_repo.update_phase(migration.id, MigrationPhase.FAILED)
        await self._migration_repo.record_error(migration.id, error)
        await self._routing_repo.clear_migration_state(migration.tenant_id)
        self._router.clear_dual_write_interceptor(migration.tenant_id)
        self._cleanup_migration_resources(migration.id)
        release_migration_metrics(str(migration.id))
        logger.error("Migration %s failed: %s", migration.id, error)
        entry = MigrationAuditEntry(None, migration.id, AuditEventType.MIGRATION_FAILED, migration.phase, MigrationPhase.FAILED, {"error": error}, None, datetime.now(UTC))  # fmt: skip
        await self._record_audit(entry)
        self._cleanup_status_queues(migration.id)


__all__ = ["CoordinatorControlMixin"]
