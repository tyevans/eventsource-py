"""Abort operation and shared state for MigrationCoordinator."""

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
from eventsource.ports.exceptions import LockAcquisitionError
from eventsource.ports.locks import migration_lock_key
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
    from eventsource.ports.locks import DistributedLock
    from eventsource.ports.migration.repositories import (
        MigrationRepository,
        TenantRoutingRepository,
    )

logger = logging.getLogger(__name__)

# How long abort waits for the tenant's cutover lock. A cutover holds that lock
# only for its locked section, which is bounded by cutover_timeout_ms.
ABORT_LOCK_TIMEOUT_SECONDS = 5.0


class CoordinatorAbortMixin:
    """Mixin providing abort and the per-migration state it tears down."""

    _tracer: Tracer
    _lock_manager: DistributedLock | None
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

    async def abort_migration(
        self, migration_id: UUID, reason: str | None = None
    ) -> MigrationResult:
        """
        Abort and rollback a migration, restoring normal tenant routing.

        Abort is allowed only from PENDING, BULK_COPY, or DUAL_WRITE. Once a
        cutover has started (phase CUTOVER), abort raises. When a lock manager
        is configured, abort takes the tenant's cutover lock and checks the
        phase again, so it cannot interleave with a cutover that is running.

        Raises:
            MigrationError: If the migration is terminal, is in CUTOVER, or
                the tenant's cutover lock is held for longer than the abort
                wait.
        """
        with self._tracer.span("eventsource.coordinator.abort_migration", {ATTR_MIGRATION_ID: str(migration_id)}):  # fmt: skip
            migration = await self._require_abortable(migration_id)
            if self._lock_manager is None:
                return await self._abort_locked(migration, reason)
            try:
                lock_key = migration_lock_key(migration.tenant_id, "cutover")
                async with self._lock_manager.acquire(lock_key, timeout=ABORT_LOCK_TIMEOUT_SECONDS):  # fmt: skip
                    current = await self._require_abortable(migration_id)
                    return await self._abort_locked(current, reason)
            except LockAcquisitionError as e:
                raise MigrationError("Cannot abort migration: a cutover holds the tenant cutover lock", migration_id=migration_id) from e  # fmt: skip

    async def _require_abortable(self, migration_id: UUID) -> Migration:
        """Return the migration, or raise if abort is not allowed in its current phase."""
        if (migration := await self._migration_repo.get(migration_id)) is None:
            raise MigrationNotFoundError(migration_id)
        if migration.phase.is_terminal:
            raise MigrationError("Cannot abort a completed migration", migration_id=migration_id)  # fmt: skip
        if migration.phase == MigrationPhase.CUTOVER:
            raise MigrationError("Cannot abort a migration during cutover; wait for the cutover to finish or roll back", migration_id=migration_id)  # fmt: skip
        return migration

    async def _abort_locked(self, migration: Migration, reason: str | None) -> MigrationResult:
        """Stop the copy, restore routing, and record ABORTED. Caller holds the cutover guard."""
        migration_id = migration.id
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


__all__ = ["CoordinatorAbortMixin"]
