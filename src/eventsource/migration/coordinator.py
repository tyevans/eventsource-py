"""
MigrationCoordinator - Orchestrates the migration lifecycle.

The MigrationCoordinator is the primary entry point for managing tenant
migrations. It coordinates all migration components (BulkCopier,
DualWriteInterceptor, CutoverManager, SyncLagTracker, ConsistencyVerifier,
SubscriptionMigrator) to perform zero-downtime tenant migrations.

This module implements:
- Basic coordinator (P1-007): lifecycle management, bulk copy orchestration
- Dual-write and cutover (P2-005): dual-write phase, sync lag monitoring, cutover
- Consistency verification and subscription migration (P3-005)

Responsibilities:
    - Migration lifecycle management (start, pause, resume, cancel)
    - Phase transitions and state machine enforcement
    - Progress monitoring and status streaming
    - Error handling and automatic rollback
    - Coordination with TenantStoreRouter
    - Dual-write interceptor setup and management (P2-005)
    - Sync lag monitoring during dual-write phase (P2-005)
    - Cutover triggering and rollback handling (P2-005)
    - Post-cutover consistency verification (P3-005)
    - Subscription checkpoint migration (P3-005)

Usage:
    >>> from eventsource.migration import MigrationCoordinator
    >>>
    >>> coordinator = MigrationCoordinator(
    ...     source_store=shared_store,
    ...     migration_repo=migration_repo,
    ...     routing_repo=routing_repo,
    ...     router=tenant_router,
    ... )
    >>>
    >>> # Start migration
    >>> migration = await coordinator.start_migration(
    ...     tenant_id=tenant_id,
    ...     target_store=dedicated_store,
    ...     target_store_id="dedicated-tenant-abc",
    ... )
    >>>
    >>> # Monitor progress
    >>> status = await coordinator.get_status(migration.id)
    >>> print(f"Phase: {status.phase}, Progress: {status.progress_percent}%")
    >>>
    >>> # Wait for dual-write phase, then trigger cutover
    >>> await coordinator.wait_for_phase(migration.id, MigrationPhase.DUAL_WRITE)
    >>> result = await coordinator.trigger_cutover(migration.id)
    >>> if result.success:
    ...     print(f"Cutover completed in {result.duration_ms}ms")

See Also:
    - Task: P1-007-migration-coordinator-basic.md
    - Task: P2-005-coordinator-dual-write.md
    - Task: P3-005-coordinator-subscriptions.md
    - FRD: docs/tasks/multi-tenant-live-migration/multi-tenant-live-migration.md
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from eventsource.migration.bulk_copier import BulkCopier, BulkCopyProgress
from eventsource.migration.consistency import (
    ConsistencyVerifier,
    VerificationLevel,
    VerificationReport,
)
from eventsource.migration.cutover import CutoverManager
from eventsource.migration.dual_write import DualWriteInterceptor
from eventsource.migration.exceptions import (
    MigrationAlreadyExistsError,
    MigrationError,
    MigrationNotFoundError,
    MigrationStateError,
)
from eventsource.migration.models import (
    CutoverResult,
    Migration,
    MigrationConfig,
    MigrationPhase,
    MigrationResult,
    MigrationStatus,
    SyncLag,
    TenantMigrationState,
)
from eventsource.migration.subscription_migrator import (
    MigrationSummary,
    SubscriptionMigrator,
)
from eventsource.migration.sync_lag_tracker import SyncLagTracker
from eventsource.observability import Tracer, create_tracer
from eventsource.stores.interface import EventStore

if TYPE_CHECKING:
    from eventsource.locks import PostgreSQLLockManager
    from eventsource.migration.position_mapper import PositionMapper
    from eventsource.migration.repositories.migration import MigrationRepository
    from eventsource.migration.repositories.routing import TenantRoutingRepository
    from eventsource.migration.router import TenantStoreRouter
    from eventsource.migration.status_streamer import StatusStreamer
    from eventsource.repositories.checkpoint import CheckpointRepository

logger = logging.getLogger(__name__)


class MigrationCoordinator:
    """
    Orchestrates the complete migration lifecycle.

    Coordinates BulkCopier, DualWriteInterceptor, SyncLagTracker, and
    CutoverManager to perform zero-downtime tenant migrations.

    The coordinator manages migrations through phases:
    1. PENDING -> BULK_COPY: Start copying historical events
    2. BULK_COPY -> DUAL_WRITE: Enable dual-write for new events
    3. DUAL_WRITE -> CUTOVER: Perform atomic switch when sync lag is acceptable
    4. CUTOVER -> COMPLETED: Migration finished successfully

    At any point before COMPLETED, the migration can be aborted.

    Phase 2 (P2-005) Features:
    - Automatic transition from BULK_COPY to DUAL_WRITE after bulk copy completes
    - DualWriteInterceptor setup for transparent dual-write during sync
    - SyncLagTracker integration for monitoring sync lag
    - Manual cutover trigger via trigger_cutover()
    - Automatic cutover rollback on failure (returns to DUAL_WRITE)
    - Enhanced status with sync lag and cutover readiness

    Phase 3 (P3-005) Features:
    - Consistency verification before cutover (optional, configurable)
    - Subscription checkpoint migration after successful cutover
    - Verification level configuration (COUNT, HASH, FULL)
    - Consistency report exposed in status
    - Graceful handling of subscription migration failures (logged, not fatal)
    - Methods: verify_consistency(), migrate_subscriptions()

    Usage:
        >>> coordinator = MigrationCoordinator(
        ...     source_store=shared_store,
        ...     migration_repo=migration_repo,
        ...     routing_repo=routing_repo,
        ...     router=tenant_router,
        ...     lock_manager=lock_manager,  # Required for cutover
        ... )
        >>>
        >>> # Start migration
        >>> migration = await coordinator.start_migration(
        ...     tenant_id=tenant_id,
        ...     target_store=dedicated_store,
        ...     target_store_id="dedicated-tenant-abc",
        ... )
        >>>
        >>> # Check status (includes sync lag in DUAL_WRITE phase)
        >>> status = await coordinator.get_status(migration.id)
        >>> print(f"Phase: {status.phase}, Sync Lag: {status.sync_lag_events}")
        >>>
        >>> # Wait for dual-write phase
        >>> await coordinator.wait_for_phase(migration.id, MigrationPhase.DUAL_WRITE)
        >>>
        >>> # Trigger cutover when ready
        >>> if status.cutover_ready:
        ...     result = await coordinator.trigger_cutover(migration.id)
        ...     print(f"Cutover: {'success' if result.success else 'failed'}")

    Attributes:
        _source_store: Source EventStore (where tenant currently resides).
        _migration_repo: Repository for migration state persistence.
        _routing_repo: Repository for tenant routing configuration.
        _router: TenantStoreRouter for routing management.
        _source_store_id: Identifier for the source store.
        _lock_manager: PostgreSQL advisory lock manager for cutover coordination.
        _active_copiers: Dictionary of active BulkCopier instances by migration ID.
        _active_tasks: Dictionary of active background tasks by migration ID.
        _status_queues: Status observer queues for streaming updates.
        _lag_trackers: Dictionary of SyncLagTracker instances by migration ID.
        _target_stores: Dictionary of target EventStore instances by migration ID.
        _cutover_manager: Shared CutoverManager instance for cutover operations.
        _position_mapper: PositionMapper for subscription checkpoint translation (P3-005).
        _checkpoint_repo: CheckpointRepository for subscription checkpoints (P3-005).
        _consistency_reports: Dict mapping migration_id to VerificationReport (P3-005).
        _subscription_summaries: Dict mapping migration_id to MigrationSummary (P3-005).
    """

    def __init__(
        self,
        source_store: EventStore,
        migration_repo: MigrationRepository,
        routing_repo: TenantRoutingRepository,
        router: TenantStoreRouter,
        *,
        source_store_id: str = "default",
        lock_manager: PostgreSQLLockManager | None = None,
        position_mapper: PositionMapper | None = None,
        checkpoint_repo: CheckpointRepository | None = None,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ):
        """
        Initialize the coordinator.

        Args:
            source_store: Source EventStore (where tenant currently is)
            migration_repo: Repository for migration state
            routing_repo: Repository for tenant routing
            router: TenantStoreRouter for routing management
            source_store_id: Identifier for the source store
            lock_manager: PostgreSQL advisory lock manager for cutover coordination.
                Required for cutover operations in dual-write phase.
            position_mapper: PositionMapper for subscription checkpoint translation.
                Required for subscription migration in P3-005.
            checkpoint_repo: CheckpointRepository for subscription checkpoints.
                Required for subscription migration in P3-005.
            tracer: Optional custom Tracer instance.
            enable_tracing: Whether to enable OpenTelemetry tracing
        """
        # Composition-based tracing (replaces TracingMixin)
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._source_store = source_store
        self._migration_repo = migration_repo
        self._routing_repo = routing_repo
        self._router = router
        self._source_store_id = source_store_id
        self._lock_manager = lock_manager

        # Active copiers by migration_id
        self._active_copiers: dict[UUID, BulkCopier] = {}

        # Active background tasks by migration_id
        self._active_tasks: dict[UUID, asyncio.Task[None]] = {}

        # Status observers
        self._status_queues: dict[UUID, list[asyncio.Queue[UUID]]] = {}

        # Phase 2 (P2-005) additions: Dual-write and cutover support
        # Lag trackers by migration_id
        self._lag_trackers: dict[UUID, SyncLagTracker] = {}

        # Target stores by migration_id (needed for dual-write interceptor creation)
        self._target_stores: dict[UUID, EventStore] = {}

        # CutoverManager instance (created lazily when needed)
        self._cutover_manager: CutoverManager | None = None

        # Phase 3 (P3-005) additions: Consistency verification and subscription migration
        self._position_mapper = position_mapper
        self._checkpoint_repo = checkpoint_repo

        # Consistency verification reports by migration_id
        self._consistency_reports: dict[UUID, VerificationReport] = {}

        # Subscription migration summaries by migration_id
        self._subscription_summaries: dict[UUID, MigrationSummary] = {}

    async def start_migration(
        self,
        tenant_id: UUID,
        target_store: EventStore,
        target_store_id: str,
        config: MigrationConfig | None = None,
        *,
        created_by: str | None = None,
    ) -> Migration:
        """
        Start a new tenant migration.

        Initializes migration state and begins bulk copy phase.
        The bulk copy runs in the background; use get_status() or
        wait_for_phase() to monitor progress.

        Args:
            tenant_id: Tenant UUID to migrate
            target_store: Target EventStore instance
            target_store_id: Identifier for target store
            config: Migration configuration (uses defaults if None)
            created_by: Operator identifier for audit

        Returns:
            Migration instance with initial state

        Raises:
            MigrationAlreadyExistsError: If active migration exists for tenant
        """
        with self._tracer.span(
            "eventsource.coordinator.start_migration",
            {
                "tenant_id": str(tenant_id),
                "target_store_id": target_store_id,
            },
        ):
            config = config or MigrationConfig()

            # Check for existing active migration
            existing = await self._migration_repo.get_by_tenant(tenant_id)
            if existing is not None:
                raise MigrationAlreadyExistsError(tenant_id, existing.id)

            # Register target store with router
            self._router.register_store(target_store_id, target_store)

            # Create migration record
            migration = Migration(
                id=uuid4(),
                tenant_id=tenant_id,
                source_store_id=self._source_store_id,
                target_store_id=target_store_id,
                phase=MigrationPhase.PENDING,
                config=config,
                created_by=created_by,
            )

            await self._migration_repo.create(migration)

            logger.info(
                "Created migration %s for tenant %s from %s to %s",
                migration.id,
                tenant_id,
                self._source_store_id,
                target_store_id,
            )

            # Ensure tenant has routing entry
            await self._routing_repo.get_or_default(tenant_id, self._source_store_id)

            # Update routing state to BULK_COPY
            await self._routing_repo.set_migration_state(
                tenant_id,
                TenantMigrationState.BULK_COPY,
                migration.id,
            )

            # Transition to bulk copy phase
            await self._migration_repo.update_phase(
                migration.id,
                MigrationPhase.BULK_COPY,
            )

            # Refresh migration to get updated timestamps
            migration_id = migration.id
            refreshed_migration = await self._migration_repo.get(migration_id)
            if refreshed_migration is None:
                raise MigrationNotFoundError(migration_id)
            migration = refreshed_migration

            # Store target store reference for later dual-write setup
            self._target_stores[migration.id] = target_store

            # Start bulk copy in background
            task = asyncio.create_task(
                self._run_bulk_copy(migration, target_store),
                name=f"bulk_copy_{migration.id}",
            )
            self._active_tasks[migration.id] = task

            return migration

    async def get_status(self, migration_id: UUID) -> MigrationStatus:
        """
        Get current migration status.

        Provides comprehensive status information including progress,
        rate, and estimated completion time.

        Args:
            migration_id: UUID of the migration

        Returns:
            MigrationStatus with current progress

        Raises:
            MigrationNotFoundError: If migration not found
        """
        with self._tracer.span(
            "eventsource.coordinator.get_status",
            {"migration.id": str(migration_id)},
        ):
            migration = await self._migration_repo.get(migration_id)
            if migration is None:
                raise MigrationNotFoundError(migration_id)

            return self._build_status(migration)

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
            {"migration.id": str(migration_id)},
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

        Continues processing from the last checkpoint. If the background
        task has completed or failed, starts a new one.

        Args:
            migration_id: UUID of the migration

        Raises:
            MigrationNotFoundError: If migration not found
            MigrationError: If migration is not paused
        """
        with self._tracer.span(
            "eventsource.coordinator.resume_migration",
            {"migration.id": str(migration_id)},
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
        restores the tenant to normal routing. The target store
        may still contain copied events that should be cleaned up
        separately if needed.

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
            {"migration.id": str(migration_id)},
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

            # Update migration state
            await self._migration_repo.update_phase(
                migration_id,
                MigrationPhase.ABORTED,
            )

            if reason:
                await self._migration_repo.record_error(migration_id, f"Aborted: {reason}")

            logger.info("Aborted migration %s: %s", migration_id, reason)

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

    async def list_active_migrations(self) -> list[MigrationStatus]:
        """
        List all active (non-terminal) migrations.

        Returns migrations in PENDING, BULK_COPY, DUAL_WRITE, or CUTOVER phases.

        Returns:
            List of MigrationStatus for active migrations
        """
        with self._tracer.span(
            "eventsource.coordinator.list_active_migrations",
            {},
        ):
            migrations = await self._migration_repo.list_active()
            return [self._build_status(m) for m in migrations]

    async def wait_for_phase(
        self,
        migration_id: UUID,
        phase: MigrationPhase,
        *,
        timeout: float | None = None,
        poll_interval: float = 1.0,
    ) -> Migration:
        """
        Wait for migration to reach a specific phase.

        Polls the migration status until the target phase is reached
        or a terminal state is encountered.

        Args:
            migration_id: UUID of the migration
            phase: Phase to wait for
            timeout: Maximum seconds to wait (None = forever)
            poll_interval: Seconds between status checks

        Returns:
            Migration when phase is reached or terminal state

        Raises:
            MigrationNotFoundError: If migration not found
            asyncio.TimeoutError: If timeout exceeded
        """
        start = asyncio.get_event_loop().time()

        while True:
            migration = await self._migration_repo.get(migration_id)
            if migration is None:
                raise MigrationNotFoundError(migration_id)

            # Check if we've reached or passed the target phase
            if migration.phase == phase or migration.phase.is_terminal:
                return migration

            # Check timeout
            if timeout is not None:
                elapsed = asyncio.get_event_loop().time() - start
                if elapsed >= timeout:
                    raise TimeoutError(f"Timeout waiting for phase {phase.value}")

            await asyncio.sleep(poll_interval)

    async def get_migration(self, migration_id: UUID) -> Migration | None:
        """
        Get a migration by ID.

        Args:
            migration_id: UUID of the migration

        Returns:
            Migration instance or None if not found
        """
        return await self._migration_repo.get(migration_id)

    async def get_migration_for_tenant(self, tenant_id: UUID) -> Migration | None:
        """
        Get the active migration for a tenant.

        Args:
            tenant_id: Tenant UUID

        Returns:
            Active Migration instance or None if no active migration
        """
        return await self._migration_repo.get_by_tenant(tenant_id)

    def create_status_streamer(
        self,
        migration_id: UUID,
    ) -> StatusStreamer:
        """
        Create a StatusStreamer for real-time migration status streaming.

        Creates a new StatusStreamer instance that can be used to stream
        MigrationStatus updates as an async iterator. Multiple streamers
        can be created for the same migration to support multiple subscribers.

        Args:
            migration_id: UUID of the migration to stream status for

        Returns:
            StatusStreamer instance for streaming status updates

        Example:
            >>> streamer = coordinator.create_status_streamer(migration_id)
            >>> async for status in streamer.stream_status():
            ...     print(f"Phase: {status.phase}, Progress: {status.progress_percent}%")
            ...     if status.phase.is_terminal:
            ...         break

        See Also:
            - stream_status(): Convenience method that yields statuses directly
            - StatusStreamer: Class for advanced streaming configuration
        """
        # Import here to avoid circular imports
        from eventsource.migration.status_streamer import StatusStreamer

        return StatusStreamer(
            coordinator=self,
            migration_id=migration_id,
            enable_tracing=self._enable_tracing,
        )

    async def stream_status(
        self,
        migration_id: UUID,
        *,
        update_interval: float = 1.0,
        include_initial: bool = True,
    ) -> AsyncIterator[MigrationStatus]:
        """
        Stream migration status updates as an async iterator.

        Convenience method that creates a StatusStreamer and yields status
        updates. For more control over streaming configuration, use
        create_status_streamer() instead.

        The iterator yields MigrationStatus updates when:
        - Phase changes occur (BULK_COPY -> DUAL_WRITE, etc.)
        - Progress updates are recorded (at least 1% change)
        - Errors occur
        - The update interval elapses

        The iterator completes when the migration reaches a terminal state
        (COMPLETED, ABORTED, or FAILED) or when explicitly broken.

        Args:
            migration_id: UUID of the migration to stream
            update_interval: Seconds between forced status checks (default 1.0)
            include_initial: Whether to yield the initial status immediately

        Yields:
            MigrationStatus: Current migration status on each update

        Raises:
            MigrationNotFoundError: If migration not found
            ValueError: If update_interval is <= 0

        Example:
            >>> async for status in coordinator.stream_status(migration_id):
            ...     print(f"Phase: {status.phase.value}")
            ...     print(f"Progress: {status.progress_percent:.1f}%")
            ...     if status.phase == MigrationPhase.COMPLETED:
            ...         print("Migration completed successfully!")
            ...         break

        Example with custom interval:
            >>> async for status in coordinator.stream_status(
            ...     migration_id,
            ...     update_interval=0.5,  # Update every 500ms
            ... ):
            ...     handle_status_update(status)
        """
        with self._tracer.span(
            "eventsource.coordinator.stream_status",
            {
                "migration.id": str(migration_id),
                "update_interval": update_interval,
            },
        ):
            streamer = self.create_status_streamer(migration_id)

            try:
                async for status in streamer.stream_status(
                    update_interval=update_interval,
                    include_initial=include_initial,
                ):
                    yield status
            finally:
                await streamer.close()

    # =========================================================================
    # Private methods
    # =========================================================================

    async def _run_bulk_copy(
        self,
        migration: Migration,
        target_store: EventStore,
    ) -> None:
        """
        Run the bulk copy phase.

        Streams historical events from source to target store.
        Updates migration progress as batches complete.

        After bulk copy completes, transitions to dual-write phase
        where new events are written to both stores.

        Args:
            migration: Migration instance
            target_store: Target EventStore to copy to
        """
        with self._tracer.span(
            "eventsource.coordinator.run_bulk_copy",
            {
                "migration.id": str(migration.id),
                "tenant_id": str(migration.tenant_id),
            },
        ):
            copier = BulkCopier(
                self._source_store,
                target_store,
                self._migration_repo,
                enable_tracing=self._enable_tracing,
            )

            self._active_copiers[migration.id] = copier

            try:
                last_progress: BulkCopyProgress | None = None

                async for progress in copier.run(migration):
                    last_progress = progress
                    await self._notify_status_update(migration.id)

                # Bulk copy complete
                if last_progress and last_progress.is_complete:
                    logger.info(
                        "Bulk copy complete for migration %s: %d events",
                        migration.id,
                        last_progress.events_copied,
                    )

                    # Transition to dual-write phase (P2-005)
                    await self._transition_to_dual_write(migration, target_store)

            except asyncio.CancelledError:
                logger.info("Bulk copy cancelled for migration %s", migration.id)
                raise

            except Exception as e:
                logger.error(
                    "Bulk copy failed for migration %s: %s",
                    migration.id,
                    e,
                )
                await self._fail_migration(migration, str(e))

            finally:
                self._active_copiers.pop(migration.id, None)
                self._active_tasks.pop(migration.id, None)

    # =========================================================================
    # Phase 2 (P2-005): Dual-Write and Cutover Methods
    # =========================================================================

    async def _transition_to_dual_write(
        self,
        migration: Migration,
        target_store: EventStore,
    ) -> None:
        """
        Transition from bulk copy to dual-write phase.

        Sets up DualWriteInterceptor and SyncLagTracker for the migration.
        The interceptor ensures new events are written to both stores.

        Args:
            migration: Migration instance
            target_store: Target EventStore to write to
        """
        with self._tracer.span(
            "eventsource.coordinator.transition_to_dual_write",
            {
                "migration.id": str(migration.id),
                "tenant_id": str(migration.tenant_id),
            },
        ):
            # Create dual-write interceptor
            interceptor = DualWriteInterceptor(
                source_store=self._source_store,
                target_store=target_store,
                tenant_id=migration.tenant_id,
                enable_tracing=self._enable_tracing,
            )

            # Register interceptor with router
            self._router.set_dual_write_interceptor(migration.tenant_id, interceptor)

            # Create sync lag tracker
            lag_tracker = SyncLagTracker(
                source_store=self._source_store,
                target_store=target_store,
                config=migration.config,
                tenant_id=migration.tenant_id,
                enable_tracing=self._enable_tracing,
            )
            self._lag_trackers[migration.id] = lag_tracker

            # Update migration phase to DUAL_WRITE
            await self._migration_repo.update_phase(
                migration.id,
                MigrationPhase.DUAL_WRITE,
            )

            # Update routing state to DUAL_WRITE
            await self._routing_repo.set_migration_state(
                migration.tenant_id,
                TenantMigrationState.DUAL_WRITE,
                migration.id,
            )

            logger.info(
                "Migration %s transitioned to dual-write phase for tenant %s",
                migration.id,
                migration.tenant_id,
            )

            # Notify status update
            await self._notify_status_update(migration.id)

    async def trigger_cutover(
        self,
        migration_id: UUID,
        *,
        timeout_ms: float | None = None,
    ) -> CutoverResult:
        """
        Trigger cutover for a migration in dual-write phase.

        Executes the cutover operation, which:
        1. Pauses writes briefly
        2. Verifies sync lag is within threshold
        3. Switches routing to target store
        4. Resumes writes to new target

        If cutover fails, automatically rolls back to dual-write phase.

        Args:
            migration_id: UUID of the migration
            timeout_ms: Maximum time in milliseconds for the cutover pause.
                If not provided, uses config.cutover_timeout_ms.

        Returns:
            CutoverResult with success/failure status and timing details.

        Raises:
            MigrationNotFoundError: If migration not found.
            MigrationStateError: If migration is not in DUAL_WRITE phase.
            MigrationError: If lock_manager was not provided to coordinator.
        """
        with self._tracer.span(
            "eventsource.coordinator.trigger_cutover",
            {"migration.id": str(migration_id)},
        ):
            migration = await self._migration_repo.get(migration_id)
            if migration is None:
                raise MigrationNotFoundError(migration_id)

            if migration.phase != MigrationPhase.DUAL_WRITE:
                raise MigrationStateError(
                    message=f"Cannot trigger cutover: migration is in {migration.phase.value} phase",
                    migration_id=migration_id,
                    current_phase=migration.phase,
                    expected_phases=[MigrationPhase.DUAL_WRITE],
                    operation="trigger_cutover",
                )

            # Get lag tracker
            lag_tracker = self._lag_trackers.get(migration_id)
            if lag_tracker is None:
                raise MigrationError(
                    "No sync lag tracker found for migration",
                    migration_id=migration_id,
                )

            # Get or create cutover manager
            cutover_manager = self._get_cutover_manager()

            # Update migration phase to CUTOVER
            await self._migration_repo.update_phase(
                migration_id,
                MigrationPhase.CUTOVER,
            )

            logger.info(
                "Starting cutover for migration %s, tenant %s",
                migration_id,
                migration.tenant_id,
            )

            # Execute cutover
            result = await cutover_manager.execute_cutover(
                migration_id=migration_id,
                tenant_id=migration.tenant_id,
                lag_tracker=lag_tracker,
                target_store_id=migration.target_store_id,
                config=migration.config,
                timeout_ms=timeout_ms,
            )

            # Handle result
            if result.success:
                await self._complete_cutover(migration)
            else:
                await self._rollback_cutover(migration, result)

            return result

    async def _complete_cutover(self, migration: Migration) -> None:
        """
        Complete the migration after successful cutover.

        Updates migration phase to COMPLETED, performs optional consistency
        verification and subscription migration, then cleans up resources.

        Phase 3 (P3-005) enhancements:
        - Runs consistency verification if config.verify_consistency is True
        - Migrates subscriptions if config.migrate_subscriptions is True
        - Stores verification report and subscription summary for retrieval

        Args:
            migration: Migration instance
        """
        # Phase 3 (P3-005): Run consistency verification if enabled
        consistency_verified = False
        if migration.config.verify_consistency:
            try:
                target_store = self._target_stores.get(migration.id)
                if target_store is not None:
                    report = await self.verify_consistency(
                        migration.id,
                        level=VerificationLevel.HASH,
                        sample_percentage=100.0,
                    )
                    consistency_verified = report.is_consistent
                    if not report.is_consistent:
                        logger.warning(
                            "Post-cutover consistency verification found %d violations "
                            "for migration %s (non-fatal, migration proceeding)",
                            len(report.violations),
                            migration.id,
                        )
                else:
                    logger.warning(
                        "Cannot verify consistency: target store not found for migration %s",
                        migration.id,
                    )
            except Exception as e:
                logger.error(
                    "Consistency verification failed for migration %s: %s "
                    "(non-fatal, migration proceeding)",
                    migration.id,
                    e,
                )

        # Phase 3 (P3-005): Migrate subscriptions if enabled
        subscriptions_migrated = 0
        if migration.config.migrate_subscriptions:
            try:
                if self._position_mapper is not None and self._checkpoint_repo is not None:
                    summary = await self.migrate_subscriptions(migration.id)
                    subscriptions_migrated = summary.successful_count
                    if summary.failed_count > 0:
                        logger.warning(
                            "Subscription migration had %d failures for migration %s "
                            "(non-fatal, migration proceeding)",
                            summary.failed_count,
                            migration.id,
                        )
                else:
                    logger.debug(
                        "Subscription migration skipped: position_mapper or "
                        "checkpoint_repo not configured for migration %s",
                        migration.id,
                    )
            except Exception as e:
                logger.error(
                    "Subscription migration failed for migration %s: %s "
                    "(non-fatal, migration proceeding)",
                    migration.id,
                    e,
                )

        # Update migration phase to COMPLETED
        await self._migration_repo.update_phase(
            migration.id,
            MigrationPhase.COMPLETED,
        )

        # Routing state already updated to MIGRATED by CutoverManager

        logger.info(
            "Cutover completed for migration %s: tenant %s now routes to %s "
            "(consistency_verified=%s, subscriptions_migrated=%d)",
            migration.id,
            migration.tenant_id,
            migration.target_store_id,
            consistency_verified,
            subscriptions_migrated,
        )

        # Clean up resources (but keep reports and summaries for retrieval)
        # Note: _cleanup_migration_resources now also cleans reports/summaries,
        # so we clean selectively here
        self._lag_trackers.pop(migration.id, None)
        self._target_stores.pop(migration.id, None)
        self._cleanup_status_queues(migration.id)

    async def _rollback_cutover(
        self,
        migration: Migration,
        result: CutoverResult,
    ) -> None:
        """
        Rollback to dual-write phase after cutover failure.

        Updates migration phase back to DUAL_WRITE so cutover can be retried.

        Args:
            migration: Migration instance
            result: CutoverResult with failure details
        """
        # Update migration phase back to DUAL_WRITE
        await self._migration_repo.update_phase(
            migration.id,
            MigrationPhase.DUAL_WRITE,
        )

        # Record the error
        if result.error_message:
            await self._migration_repo.record_error(
                migration.id,
                f"Cutover failed: {result.error_message}",
            )

        logger.warning(
            "Cutover rolled back for migration %s: %s",
            migration.id,
            result.error_message,
        )

        # Notify status update
        await self._notify_status_update(migration.id)

    def _get_cutover_manager(self) -> CutoverManager:
        """
        Get or create the CutoverManager instance.

        Returns:
            CutoverManager instance.

        Raises:
            MigrationError: If lock_manager was not provided.
        """
        if self._cutover_manager is not None:
            return self._cutover_manager

        if self._lock_manager is None:
            raise MigrationError(
                "Cannot perform cutover: lock_manager not provided to coordinator. "
                "Provide a PostgreSQLLockManager when creating the coordinator to "
                "enable cutover operations.",
            )

        self._cutover_manager = CutoverManager(
            lock_manager=self._lock_manager,
            router=self._router,
            routing_repo=self._routing_repo,
            enable_tracing=self._enable_tracing,
        )

        return self._cutover_manager

    def _cleanup_migration_resources(self, migration_id: UUID) -> None:
        """
        Clean up resources associated with a migration.

        Removes lag trackers, target stores, consistency reports, and
        subscription summaries.

        Args:
            migration_id: UUID of the migration to clean up
        """
        # Remove lag tracker
        self._lag_trackers.pop(migration_id, None)

        # Remove target store reference
        self._target_stores.pop(migration_id, None)

        # Phase 3 (P3-005) cleanup: consistency reports and subscription summaries
        self._consistency_reports.pop(migration_id, None)
        self._subscription_summaries.pop(migration_id, None)

    async def get_sync_lag(self, migration_id: UUID) -> SyncLag | None:
        """
        Get current sync lag for a migration.

        Calculates and returns the current synchronization lag between
        source and target stores for a migration in DUAL_WRITE phase.

        Args:
            migration_id: UUID of the migration

        Returns:
            SyncLag with current positions and event lag, or None if
            migration is not in dual-write phase or lag tracker not found.

        Raises:
            MigrationNotFoundError: If migration not found.
        """
        migration = await self._migration_repo.get(migration_id)
        if migration is None:
            raise MigrationNotFoundError(migration_id)

        lag_tracker = self._lag_trackers.get(migration_id)
        if lag_tracker is None:
            return None

        return await lag_tracker.calculate_lag()

    async def is_cutover_ready(self, migration_id: UUID) -> tuple[bool, str | None]:
        """
        Check if a migration is ready for cutover.

        Validates that:
        1. Migration is in DUAL_WRITE phase
        2. Sync lag is within the configured threshold

        Args:
            migration_id: UUID of the migration

        Returns:
            Tuple of (is_ready, error_message).
            If is_ready is True, error_message is None.

        Raises:
            MigrationNotFoundError: If migration not found.
        """
        migration = await self._migration_repo.get(migration_id)
        if migration is None:
            raise MigrationNotFoundError(migration_id)

        if migration.phase != MigrationPhase.DUAL_WRITE:
            return (
                False,
                f"Migration is in {migration.phase.value} phase, expected DUAL_WRITE",
            )

        lag_tracker = self._lag_trackers.get(migration_id)
        if lag_tracker is None:
            return False, "No sync lag tracker found for migration"

        # Calculate current lag
        await lag_tracker.calculate_lag()

        if lag_tracker.is_sync_ready():
            return True, None
        else:
            current_lag = lag_tracker.current_lag
            lag_events = current_lag.events if current_lag else "unknown"
            return (
                False,
                f"Sync lag too high: {lag_events} events "
                f"(max: {migration.config.cutover_max_lag_events})",
            )

    async def _complete_migration(self, migration: Migration) -> None:
        """
        Mark migration as completed and update routing.

        Updates the migration phase to COMPLETED and updates tenant
        routing to point to the target store.

        Args:
            migration: Migration instance to complete
        """
        # Update migration phase to COMPLETED
        await self._migration_repo.update_phase(
            migration.id,
            MigrationPhase.COMPLETED,
        )

        # Update routing state to MIGRATED
        await self._routing_repo.set_migration_state(
            migration.tenant_id,
            TenantMigrationState.MIGRATED,
            migration.id,
        )

        # Update routing to point to target store
        await self._routing_repo.set_routing(
            migration.tenant_id,
            migration.target_store_id,
        )

        logger.info(
            "Completed migration %s: tenant %s now routes to %s",
            migration.id,
            migration.tenant_id,
            migration.target_store_id,
        )

        # Clean up status queues
        self._cleanup_status_queues(migration.id)

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

        logger.error(
            "Migration %s failed: %s",
            migration.id,
            error,
        )

        # Clean up status queues
        self._cleanup_status_queues(migration.id)

    def _build_status(self, migration: Migration) -> MigrationStatus:
        """
        Build MigrationStatus from Migration.

        Calculates derived metrics like rate and estimated completion.
        In dual-write phase, includes sync lag from the lag tracker.

        Args:
            migration: Migration instance

        Returns:
            MigrationStatus with current progress and metrics
        """
        phase_started_at = None
        if migration.phase == MigrationPhase.BULK_COPY:
            phase_started_at = migration.bulk_copy_started_at
        elif migration.phase == MigrationPhase.DUAL_WRITE:
            phase_started_at = migration.dual_write_started_at
        elif migration.phase == MigrationPhase.CUTOVER:
            phase_started_at = migration.cutover_started_at

        # Calculate rate
        rate = 0.0
        if migration.started_at and migration.events_copied > 0:
            elapsed = (datetime.now(UTC) - migration.started_at).total_seconds()
            if elapsed > 0:
                rate = migration.events_copied / elapsed

        # Estimate completion
        estimated_completion = None
        if rate > 0 and migration.events_remaining > 0:
            remaining_seconds = migration.events_remaining / rate
            estimated_completion = datetime.now(UTC) + timedelta(seconds=remaining_seconds)

        # Get sync lag from tracker if available (P2-005)
        sync_lag_events = 0
        sync_lag_ms = 0.0
        lag_tracker = self._lag_trackers.get(migration.id)
        if lag_tracker and lag_tracker.current_lag:
            sync_lag_events = lag_tracker.current_lag.events
            sync_lag_ms = lag_tracker.current_lag.lag_ms

        return MigrationStatus(
            migration_id=migration.id,
            tenant_id=migration.tenant_id,
            phase=migration.phase,
            progress_percent=migration.progress_percent,
            events_total=migration.events_total,
            events_copied=migration.events_copied,
            events_remaining=migration.events_remaining,
            sync_lag_events=sync_lag_events,
            sync_lag_ms=sync_lag_ms,
            error_count=migration.error_count,
            started_at=migration.started_at,
            phase_started_at=phase_started_at,
            estimated_completion=estimated_completion,
            current_rate_events_per_sec=rate,
            is_paused=migration.is_paused,
        )

    def _calculate_duration(self, migration: Migration | None) -> float:
        """
        Calculate migration duration in seconds.

        Args:
            migration: Migration instance or None

        Returns:
            Duration in seconds, or 0.0 if migration is None or not started
        """
        if migration is None or migration.started_at is None:
            return 0.0
        end = migration.completed_at or datetime.now(UTC)
        return (end - migration.started_at).total_seconds()

    async def _notify_status_update(self, migration_id: UUID) -> None:
        """
        Notify observers of status update.

        Sends migration_id to all registered queues for this migration.

        Args:
            migration_id: UUID of the migration that was updated
        """
        queues = self._status_queues.get(migration_id, [])
        for queue in queues:
            with contextlib.suppress(asyncio.QueueFull):
                queue.put_nowait(migration_id)

    def _cleanup_status_queues(self, migration_id: UUID) -> None:
        """
        Clean up status queues for a completed/aborted migration.

        Args:
            migration_id: UUID of the migration to clean up
        """
        self._status_queues.pop(migration_id, None)

    def register_status_queue(self, migration_id: UUID, queue: asyncio.Queue[UUID]) -> None:
        """
        Register a queue for status updates.

        The queue will receive migration_id whenever the migration
        status is updated. Used for implementing status streaming.

        Args:
            migration_id: UUID of the migration to observe
            queue: Queue to receive update notifications
        """
        if migration_id not in self._status_queues:
            self._status_queues[migration_id] = []
        self._status_queues[migration_id].append(queue)

    def unregister_status_queue(self, migration_id: UUID, queue: asyncio.Queue[UUID]) -> None:
        """
        Unregister a queue from status updates.

        Args:
            migration_id: UUID of the migration
            queue: Queue to remove
        """
        queues = self._status_queues.get(migration_id, [])
        if queue in queues:
            queues.remove(queue)

    # =========================================================================
    # Phase 3 (P3-005): Consistency Verification and Subscription Migration
    # =========================================================================

    async def verify_consistency(
        self,
        migration_id: UUID,
        *,
        level: VerificationLevel = VerificationLevel.HASH,
        sample_percentage: float = 100.0,
    ) -> VerificationReport:
        """
        Verify data consistency between source and target stores.

        Performs consistency verification for the migrated tenant data
        using the specified verification level. The report is stored
        and can be retrieved via get_consistency_report().

        This method can be called at any point during migration, but
        is typically called after cutover to verify migration success.
        If config.verify_consistency is True, this is called automatically
        during _complete_cutover().

        Args:
            migration_id: UUID of the migration
            level: Verification thoroughness level:
                - COUNT: Verify event counts only (fastest)
                - HASH: Verify event content via SHA-256 hashes (balanced)
                - FULL: Verify complete event data (slowest, most thorough)
            sample_percentage: Percentage of events to sample (1-100).
                Use 100 for complete verification, lower values for
                faster verification of large datasets.

        Returns:
            VerificationReport with detailed verification results.

        Raises:
            MigrationNotFoundError: If migration not found.

        Example:
            >>> report = await coordinator.verify_consistency(
            ...     migration_id,
            ...     level=VerificationLevel.HASH,
            ...     sample_percentage=100.0,
            ... )
            >>> if report.is_consistent:
            ...     print("Verification passed")
            ... else:
            ...     for violation in report.violations:
            ...         print(f"Violation: {violation}")
        """
        with self._tracer.span(
            "eventsource.coordinator.verify_consistency",
            {
                "migration.id": str(migration_id),
                "level": level.value,
                "sample_percentage": sample_percentage,
            },
        ):
            migration = await self._migration_repo.get(migration_id)
            if migration is None:
                raise MigrationNotFoundError(migration_id)

            # Get the target store for this migration
            target_store = self._target_stores.get(migration_id)
            if target_store is None:
                raise MigrationError(
                    "Target store not found for migration. "
                    "Verify consistency is typically called after migration is started.",
                    migration_id=migration_id,
                )

            # Create verifier
            verifier = ConsistencyVerifier(
                source_store=self._source_store,
                target_store=target_store,
                enable_tracing=self._enable_tracing,
            )

            logger.info(
                "Starting consistency verification for migration %s, tenant %s, level=%s",
                migration_id,
                migration.tenant_id,
                level.value,
            )

            # Perform verification
            report = await verifier.verify_tenant_consistency(
                tenant_id=migration.tenant_id,
                level=level,
                sample_percentage=sample_percentage,
            )

            # Store the report
            self._consistency_reports[migration_id] = report

            if report.is_consistent:
                logger.info(
                    "Consistency verification passed for migration %s: "
                    "%d events, %d streams verified",
                    migration_id,
                    report.source_event_count,
                    report.streams_verified,
                )
            else:
                logger.warning(
                    "Consistency verification FAILED for migration %s: %d violations found",
                    migration_id,
                    len(report.violations),
                )

            return report

    async def migrate_subscriptions(
        self,
        migration_id: UUID,
        subscription_names: list[str] | None = None,
        *,
        dry_run: bool = False,
    ) -> MigrationSummary:
        """
        Migrate subscription checkpoints to target store positions.

        Translates subscription checkpoint positions from source to target
        store using the PositionMapper, then updates the checkpoints.
        Subscription migration failures are logged but do not fail the
        overall migration.

        This method can be called after cutover to migrate subscription
        checkpoints. If config.migrate_subscriptions is True, this is
        called automatically during _complete_cutover().

        Args:
            migration_id: UUID of the migration
            subscription_names: List of subscription names to migrate.
                If None, discovers subscriptions from the checkpoint repository.
            dry_run: If True, preview changes without executing them.

        Returns:
            MigrationSummary with results of all subscription migrations.

        Raises:
            MigrationNotFoundError: If migration not found.
            MigrationError: If position_mapper or checkpoint_repo not provided.

        Example:
            >>> summary = await coordinator.migrate_subscriptions(
            ...     migration_id,
            ...     subscription_names=["OrderProjection", "InventoryProjection"],
            ... )
            >>> if summary.all_successful:
            ...     print(f"Migrated {summary.successful_count} subscriptions")
            ... else:
            ...     print(f"Failed: {summary.failed_count} subscriptions")
        """
        with self._tracer.span(
            "eventsource.coordinator.migrate_subscriptions",
            {
                "migration.id": str(migration_id),
                "dry_run": dry_run,
            },
        ):
            migration = await self._migration_repo.get(migration_id)
            if migration is None:
                raise MigrationNotFoundError(migration_id)

            # Check required dependencies
            if self._position_mapper is None:
                raise MigrationError(
                    "Cannot migrate subscriptions: position_mapper not provided "
                    "to coordinator. Provide a PositionMapper when creating the "
                    "coordinator to enable subscription migration.",
                    migration_id=migration_id,
                )

            if self._checkpoint_repo is None:
                raise MigrationError(
                    "Cannot migrate subscriptions: checkpoint_repo not provided "
                    "to coordinator. Provide a CheckpointRepository when creating "
                    "the coordinator to enable subscription migration.",
                    migration_id=migration_id,
                )

            # Create subscription migrator
            migrator = SubscriptionMigrator(
                position_mapper=self._position_mapper,
                checkpoint_repo=self._checkpoint_repo,
                enable_tracing=self._enable_tracing,
            )

            logger.info(
                "Starting subscription migration for migration %s, tenant %s",
                migration_id,
                migration.tenant_id,
            )

            # Perform migration
            summary = await migrator.migrate_tenant_subscriptions(
                migration_id=migration_id,
                tenant_id=migration.tenant_id,
                subscription_names=subscription_names,
                dry_run=dry_run,
            )

            # Store the summary
            self._subscription_summaries[migration_id] = summary

            if summary.all_successful:
                logger.info(
                    "Subscription migration completed for migration %s: %d subscriptions migrated",
                    migration_id,
                    summary.successful_count,
                )
            else:
                logger.warning(
                    "Subscription migration completed with failures for migration %s: "
                    "%d successful, %d failed, %d skipped",
                    migration_id,
                    summary.successful_count,
                    summary.failed_count,
                    summary.skipped_count,
                )

            return summary

    def get_consistency_report(self, migration_id: UUID) -> VerificationReport | None:
        """
        Get the consistency verification report for a migration.

        Returns the stored VerificationReport from a previous call to
        verify_consistency(). Returns None if no verification has been
        performed.

        Args:
            migration_id: UUID of the migration

        Returns:
            VerificationReport if available, None otherwise.
        """
        return self._consistency_reports.get(migration_id)

    def get_subscription_summary(self, migration_id: UUID) -> MigrationSummary | None:
        """
        Get the subscription migration summary for a migration.

        Returns the stored MigrationSummary from a previous call to
        migrate_subscriptions(). Returns None if no subscription migration
        has been performed.

        Args:
            migration_id: UUID of the migration

        Returns:
            MigrationSummary if available, None otherwise.
        """
        return self._subscription_summaries.get(migration_id)


__all__ = [
    "MigrationCoordinator",
    "VerificationLevel",
]
