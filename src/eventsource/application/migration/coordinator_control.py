"""Pause, resume, abort, and failure operations for MigrationCoordinator."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID

from eventsource.application.migration.exceptions import (
    MigrationError,
    MigrationNotFoundError,
)
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
        raise NotImplementedError

    async def _record_audit(self, entry: MigrationAuditEntry) -> None:
        raise NotImplementedError

    def _cleanup_status_queues(self, migration_id: UUID) -> None:
        raise NotImplementedError

    def _cleanup_migration_resources(self, migration_id: UUID) -> None:
        """Clean up resources associated with a migration."""
        self._lag_trackers.pop(migration_id, None)
        self._target_stores.pop(migration_id, None)
        self._interceptors.pop(migration_id, None)
        self._consistency_reports.pop(migration_id, None)
        self._subscription_summaries.pop(migration_id, None)

    async def pause_migration(self, migration_id: UUID) -> None:
        """
        Pause an in-progress migration.

        The migration will stop after the current batch completes.
        Progress is preserved and can be resumed with resume_migration().

        Args:
            migration_id: UUID of the migration

        Raises:
            MigrationNotFoundError: If migration not found
            MigrationError: If migration cannot be paused (terminal state)
        """
        with self._tracer.span(
            "eventsource.coordinator.pause_migration",
            {ATTR_MIGRATION_ID: str(migration_id)},
        ):
            migration = await self._migration_repo.get(migration_id)
            if migration is None:
                raise MigrationNotFoundError(migration_id)

            if migration.phase.is_terminal:
                raise MigrationError(
                    "Cannot pause a completed migration",
                    migration_id=migration_id,
                )

            if migration.is_paused:
                logger.debug("Migration %s is already paused", migration_id)
                return

            # Pause the active copier
            copier = self._active_copiers.get(migration_id)
            if copier:
                copier.pause()

            await self._migration_repo.set_paused(
                migration_id,
                paused=True,
                reason="Operator requested",
            )

            logger.info("Paused migration %s", migration_id)

    async def resume_migration(self, migration_id: UUID) -> None:
        """
        Resume a paused migration.

        Un-pauses the routing state and, if an in-process copier for this
        migration is still tracked in this coordinator instance, resumes
        it.

        Args:
            migration_id: UUID of the migration

        Raises:
            MigrationNotFoundError: If migration not found
            MigrationError: If migration is not paused
        """
        with self._tracer.span(
            "eventsource.coordinator.resume_migration",
            {ATTR_MIGRATION_ID: str(migration_id)},
        ):
            migration = await self._migration_repo.get(migration_id)
            if migration is None:
                raise MigrationNotFoundError(migration_id)

            if not migration.is_paused:
                raise MigrationError(
                    "Migration is not paused",
                    migration_id=migration_id,
                )

            # Resume the active copier if present
            copier = self._active_copiers.get(migration_id)
            if copier:
                copier.resume()

            await self._migration_repo.set_paused(migration_id, paused=False)

            logger.info("Resumed migration %s", migration_id)

    async def abort_migration(
        self,
        migration_id: UUID,
        reason: str | None = None,
    ) -> MigrationResult:
        """
        Abort and rollback a migration.

        Cancels any in-progress operations, cleans up state, and
        restores the tenant to normal routing.

        Args:
            migration_id: UUID of the migration
            reason: Optional reason for abort

        Returns:
            MigrationResult with final state

        Raises:
            MigrationNotFoundError: If migration not found
            MigrationError: If migration cannot be aborted (terminal state)
        """
        with self._tracer.span(
            "eventsource.coordinator.abort_migration",
            {ATTR_MIGRATION_ID: str(migration_id)},
        ):
            migration = await self._migration_repo.get(migration_id)
            if migration is None:
                raise MigrationNotFoundError(migration_id)

            if migration.phase.is_terminal:
                raise MigrationError(
                    "Cannot abort a completed migration",
                    migration_id=migration_id,
                )

            # Cancel active copier
            copier = self._active_copiers.pop(migration_id, None)
            if copier:
                copier.cancel()

            # Cancel background task
            task = self._active_tasks.pop(migration_id, None)
            if task and not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

            # Clean up routing state
            await self._routing_repo.clear_migration_state(migration.tenant_id)

            # Clean up router interceptors
            self._router.clear_dual_write_interceptor(migration.tenant_id)

            # Clean up P2 resources (lag trackers, target store references)
            self._cleanup_migration_resources(migration_id)
            release_migration_metrics(str(migration_id))

            # Update migration state
            await self._migration_repo.update_phase(
                migration_id,
                MigrationPhase.ABORTED,
            )

            if reason:
                await self._migration_repo.record_error(migration_id, f"Aborted: {reason}")

            logger.info("Aborted migration %s: %s", migration_id, reason)

            await self._record_audit(
                MigrationAuditEntry(
                    id=None,
                    migration_id=migration_id,
                    event_type=AuditEventType.MIGRATION_ABORTED,
                    old_phase=migration.phase,
                    new_phase=MigrationPhase.ABORTED,
                    details={"reason": reason} if reason else None,
                    operator=None,
                    occurred_at=datetime.now(UTC),
                )
            )

            # Clean up status queues
            self._cleanup_status_queues(migration_id)

            # Get final state
            migration = await self._migration_repo.get(migration_id)

            return MigrationResult(
                migration_id=migration_id,
                success=False,
                duration_seconds=self._calculate_duration(migration),
                events_migrated=migration.events_copied if migration else 0,
                final_phase=MigrationPhase.ABORTED,
                error_message=reason,
            )

    async def _fail_migration(self, migration: Migration, error: str) -> None:
        """
        Mark migration as failed.

        Updates the migration phase to FAILED and restores tenant
        routing to normal state.

        Args:
            migration: Migration instance that failed
            error: Error message to record
        """
        await self._migration_repo.update_phase(
            migration.id,
            MigrationPhase.FAILED,
        )
        await self._migration_repo.record_error(migration.id, error)

        # Clean up routing state
        await self._routing_repo.clear_migration_state(migration.tenant_id)

        # Clean up router interceptors
        self._router.clear_dual_write_interceptor(migration.tenant_id)

        # Clean up P2 resources (lag trackers, target store references)
        self._cleanup_migration_resources(migration.id)
        release_migration_metrics(str(migration.id))

        logger.error(
            "Migration %s failed: %s",
            migration.id,
            error,
        )

        await self._record_audit(
            MigrationAuditEntry(
                id=None,
                migration_id=migration.id,
                event_type=AuditEventType.MIGRATION_FAILED,
                old_phase=migration.phase,
                new_phase=MigrationPhase.FAILED,
                details={"error": error},
                operator=None,
                occurred_at=datetime.now(UTC),
            )
        )

        # Clean up status queues
        self._cleanup_status_queues(migration.id)


__all__ = [
    "CoordinatorControlMixin",
]
