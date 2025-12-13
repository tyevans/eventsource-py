"""
CutoverManager - Atomic switch with minimal pause.

The CutoverManager handles the critical cutover phase of migration,
performing an atomic switch from source to target store with sub-100ms
pause time. It coordinates write pausing, final sync, and routing updates.

Responsibilities:
    - Coordinate write pause across all instances (using advisory locks)
    - Drain in-flight writes to target store
    - Verify final consistency between stores
    - Update routing atomically
    - Automatic rollback on timeout or failure

Cutover Sequence:
    1. Acquire distributed lock for tenant
    2. Pause incoming writes (queue them)
    3. Wait for in-flight writes to complete
    4. Verify source and target are in sync
    5. Update routing to target store
    6. Release lock and resume writes
    7. Complete or rollback based on success

Performance Guarantee:
    - Maximum pause time: 100ms (configurable)
    - Automatic rollback if timeout exceeded

Usage:
    >>> from eventsource.migration import CutoverManager
    >>>
    >>> cutover = CutoverManager(
    ...     lock_manager=lock_manager,
    ...     router=router,
    ...     routing_repo=routing_repo,
    ... )
    >>>
    >>> # Perform cutover
    >>> result = await cutover.execute_cutover(
    ...     migration_id=migration.id,
    ...     tenant_id=tenant_id,
    ...     lag_tracker=lag_tracker,
    ...     timeout_ms=100.0,
    ... )
    >>>
    >>> if result.success:
    ...     print(f"Cutover completed in {result.duration_ms}ms")
    ... else:
    ...     print(f"Cutover failed: {result.error_message}")

See Also:
    - Task: P2-003-cutover-manager.md
    - FRD: docs/tasks/multi-tenant-live-migration/multi-tenant-live-migration.md
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.locks import LockAcquisitionError, migration_lock_key
from eventsource.migration.exceptions import (
    CutoverError,
    CutoverLagError,
    CutoverTimeoutError,
)
from eventsource.migration.models import (
    CutoverResult,
    MigrationConfig,
    TenantMigrationState,
)
from eventsource.observability import ATTR_TENANT_ID, Tracer, create_tracer

if TYPE_CHECKING:
    from eventsource.locks import PostgreSQLLockManager
    from eventsource.migration.repositories.routing import TenantRoutingRepository
    from eventsource.migration.router import TenantStoreRouter
    from eventsource.migration.sync_lag_tracker import SyncLagTracker

logger = logging.getLogger(__name__)


# Custom attribute keys for cutover tracing
ATTR_MIGRATION_ID = "eventsource.cutover.migration_id"
ATTR_CUTOVER_TIMEOUT_MS = "eventsource.cutover.timeout_ms"
ATTR_CUTOVER_DURATION_MS = "eventsource.cutover.duration_ms"
ATTR_CUTOVER_SUCCESS = "eventsource.cutover.success"
ATTR_CUTOVER_ROLLED_BACK = "eventsource.cutover.rolled_back"
ATTR_SYNC_LAG_EVENTS = "eventsource.cutover.sync_lag_events"
ATTR_TARGET_STORE_ID = "eventsource.cutover.target_store_id"


class CutoverManager:
    """
    Manages the atomic cutover from source to target store.

    Coordinates the critical moment when traffic switches from source
    to target, ensuring sub-100ms pause and automatic rollback on
    timeout or failure.

    The cutover process:
        1. Acquire advisory lock to ensure exclusive access
        2. Pause writes for the tenant
        3. Verify sync lag is within threshold
        4. Update routing state to CUTOVER_PAUSED
        5. Wait for final sync (if needed)
        6. Switch routing to target store
        7. Verify target store is readable
        8. Update routing state to MIGRATED
        9. Resume writes to new target
        10. Release advisory lock

    If any step fails or timeout is exceeded, the manager automatically
    rolls back to the previous state (DUAL_WRITE) and resumes writes.

    Example:
        >>> cutover = CutoverManager(
        ...     lock_manager=lock_manager,
        ...     router=router,
        ...     routing_repo=routing_repo,
        ... )
        >>>
        >>> result = await cutover.execute_cutover(
        ...     migration_id=migration.id,
        ...     tenant_id=tenant_id,
        ...     lag_tracker=lag_tracker,
        ...     target_store_id="dedicated-tenant-abc",
        ...     timeout_ms=100.0,
        ... )
        >>>
        >>> if result.success:
        ...     print(f"Cutover completed in {result.duration_ms:.2f}ms")

    Attributes:
        _lock_manager: PostgreSQL advisory lock manager for coordination.
        _router: TenantStoreRouter for write pause/resume and routing.
        _routing_repo: Repository for atomic routing state updates.
        _lock_acquisition_timeout: Timeout for acquiring advisory lock.
    """

    def __init__(
        self,
        lock_manager: PostgreSQLLockManager,
        router: TenantStoreRouter,
        routing_repo: TenantRoutingRepository,
        *,
        lock_acquisition_timeout: float = 0.5,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ) -> None:
        """
        Initialize the cutover manager.

        Args:
            lock_manager: PostgreSQL advisory lock manager for distributed coordination.
            router: TenantStoreRouter for managing write pause/resume.
            routing_repo: Repository for updating tenant routing state.
            lock_acquisition_timeout: Timeout in seconds for acquiring the advisory lock.
                Defaults to 0.5 seconds (500ms) as per requirements.
            tracer: Optional custom Tracer instance.
            enable_tracing: Whether to enable OpenTelemetry tracing.
        """
        # Composition-based tracing (replaces TracingMixin)
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._lock_manager = lock_manager
        self._router = router
        self._routing_repo = routing_repo
        self._lock_acquisition_timeout = lock_acquisition_timeout

    async def execute_cutover(
        self,
        migration_id: UUID,
        tenant_id: UUID,
        lag_tracker: SyncLagTracker,
        target_store_id: str,
        *,
        config: MigrationConfig | None = None,
        timeout_ms: float | None = None,
    ) -> CutoverResult:
        """
        Execute the atomic cutover from source to target store.

        This is the main entry point for performing a cutover. It acquires
        an advisory lock, pauses writes, verifies sync lag, switches routing,
        and resumes writes. If any step fails or the timeout is exceeded,
        the operation is automatically rolled back.

        Args:
            migration_id: ID of the migration being performed.
            tenant_id: Tenant whose events are being migrated.
            lag_tracker: SyncLagTracker for verifying final sync lag.
            target_store_id: ID of the target store to switch to.
            config: Optional migration configuration. If not provided,
                defaults are used.
            timeout_ms: Maximum time in milliseconds for the cutover pause.
                If not provided, uses config.cutover_timeout_ms or 500ms.

        Returns:
            CutoverResult indicating success/failure and timing details.

        Raises:
            CutoverError: If cutover fails and cannot be rolled back.

        Example:
            >>> result = await cutover.execute_cutover(
            ...     migration_id=migration.id,
            ...     tenant_id=tenant_id,
            ...     lag_tracker=lag_tracker,
            ...     target_store_id="dedicated-tenant-abc",
            ...     timeout_ms=100.0,
            ... )
        """
        config = config or MigrationConfig()
        effective_timeout_ms = timeout_ms if timeout_ms is not None else config.cutover_timeout_ms

        with self._tracer.span(
            "eventsource.cutover.execute",
            {
                ATTR_MIGRATION_ID: str(migration_id),
                ATTR_TENANT_ID: str(tenant_id),
                ATTR_CUTOVER_TIMEOUT_MS: effective_timeout_ms,
                ATTR_TARGET_STORE_ID: target_store_id,
            },
        ):
            logger.info(
                "Starting cutover for migration %s, tenant %s, target store %s",
                migration_id,
                tenant_id,
                target_store_id,
            )

            # Acquire advisory lock for exclusive cutover access
            lock_key = migration_lock_key(tenant_id, "cutover")

            try:
                async with self._lock_manager.acquire(
                    lock_key,
                    timeout=self._lock_acquisition_timeout,
                ):
                    return await self._execute_cutover_locked(
                        migration_id=migration_id,
                        tenant_id=tenant_id,
                        lag_tracker=lag_tracker,
                        target_store_id=target_store_id,
                        config=config,
                        timeout_ms=effective_timeout_ms,
                    )

            except LockAcquisitionError as e:
                logger.warning(
                    "Failed to acquire cutover lock for tenant %s: %s",
                    tenant_id,
                    e,
                )
                return CutoverResult(
                    success=False,
                    duration_ms=0.0,
                    error_message=f"Failed to acquire cutover lock: {e}",
                    rolled_back=False,
                )

    async def _execute_cutover_locked(
        self,
        migration_id: UUID,
        tenant_id: UUID,
        lag_tracker: SyncLagTracker,
        target_store_id: str,
        config: MigrationConfig,
        timeout_ms: float,
    ) -> CutoverResult:
        """
        Execute cutover while holding the advisory lock.

        This method performs the actual cutover sequence. It is called
        after the advisory lock has been acquired.

        Args:
            migration_id: ID of the migration.
            tenant_id: Tenant being migrated.
            lag_tracker: For verifying sync lag.
            target_store_id: Target store to switch to.
            config: Migration configuration.
            timeout_ms: Maximum cutover pause time.

        Returns:
            CutoverResult with outcome details.
        """
        start_time = time.perf_counter()
        events_synced = 0
        rolled_back = False

        try:
            # Step 1: Pause writes for the tenant
            await self._router.pause_writes(tenant_id)

            logger.debug("Paused writes for tenant %s", tenant_id)

            # Step 2: Pre-cutover validation - verify sync lag
            await lag_tracker.calculate_lag()
            lag = lag_tracker.current_lag

            if lag is None:
                raise CutoverError(
                    "Unable to calculate sync lag",
                    migration_id=migration_id,
                    reason="lag_calculation_failed",
                )

            if not lag.is_within_threshold(config.cutover_max_lag_events):
                raise CutoverLagError(
                    migration_id=migration_id,
                    current_lag=lag.events,
                    max_lag=config.cutover_max_lag_events,
                )

            logger.debug(
                "Sync lag within threshold: %d events (max %d)",
                lag.events,
                config.cutover_max_lag_events,
            )

            # Step 3: Update routing state to CUTOVER_PAUSED
            await self._routing_repo.set_migration_state(
                tenant_id,
                TenantMigrationState.CUTOVER_PAUSED,
                migration_id=migration_id,
            )

            # Step 4: Verify we haven't exceeded timeout
            elapsed_ms = (time.perf_counter() - start_time) * 1000
            if elapsed_ms >= timeout_ms:
                raise CutoverTimeoutError(
                    migration_id=migration_id,
                    elapsed_ms=elapsed_ms,
                    timeout_ms=timeout_ms,
                )

            # Step 5: Wait for any final sync (brief sleep to drain in-flight)
            # This is a minimal wait to ensure any in-flight dual-writes complete
            remaining_ms = max(0, timeout_ms - elapsed_ms - 10)  # Reserve 10ms margin
            if remaining_ms > 0:
                await asyncio.sleep(min(remaining_ms / 1000, 0.010))  # Max 10ms wait

            # Step 6: Final lag check after brief wait
            await lag_tracker.calculate_lag()
            final_lag = lag_tracker.current_lag

            if final_lag:
                events_synced = max(0, lag.events - final_lag.events)

            # Step 7: Check timeout again
            elapsed_ms = (time.perf_counter() - start_time) * 1000
            if elapsed_ms >= timeout_ms:
                raise CutoverTimeoutError(
                    migration_id=migration_id,
                    elapsed_ms=elapsed_ms,
                    timeout_ms=timeout_ms,
                )

            # Step 8: Verify target store is healthy (read test)
            try:
                target_store = self._router.get_store(target_store_id)
                if target_store is not None:
                    await target_store.get_global_position()
                else:
                    logger.warning(
                        "Target store %s not found in router registry",
                        target_store_id,
                    )
            except Exception as e:
                raise CutoverError(
                    f"Target store health check failed: {e}",
                    migration_id=migration_id,
                    reason="target_health_check_failed",
                ) from e

            # Step 9: Switch routing to target (atomic update)
            await self._routing_repo.set_routing(tenant_id, target_store_id)
            await self._routing_repo.set_migration_state(
                tenant_id,
                TenantMigrationState.MIGRATED,
                migration_id=migration_id,
            )

            # Step 10: Clear the dual-write interceptor
            self._router.clear_dual_write_interceptor(tenant_id)

            # Calculate final duration
            final_elapsed_ms = (time.perf_counter() - start_time) * 1000

            logger.info(
                "Cutover completed for tenant %s in %.2fms (synced %d events)",
                tenant_id,
                final_elapsed_ms,
                events_synced,
            )

            return CutoverResult(
                success=True,
                duration_ms=final_elapsed_ms,
                events_synced=events_synced,
            )

        except CutoverTimeoutError as e:
            logger.warning(
                "Cutover timeout for tenant %s: %.2fms exceeded %.2fms",
                tenant_id,
                e.elapsed_ms,
                e.timeout_ms,
            )
            rolled_back = await self._rollback(tenant_id, migration_id)
            return CutoverResult(
                success=False,
                duration_ms=e.elapsed_ms,
                events_synced=events_synced,
                error_message=str(e),
                rolled_back=rolled_back,
            )

        except CutoverLagError as e:
            logger.warning(
                "Cutover failed for tenant %s: lag too high (%d > %d)",
                tenant_id,
                e.current_lag,
                e.max_lag,
            )
            rolled_back = await self._rollback(tenant_id, migration_id)
            elapsed_ms = (time.perf_counter() - start_time) * 1000
            return CutoverResult(
                success=False,
                duration_ms=elapsed_ms,
                events_synced=events_synced,
                error_message=str(e),
                rolled_back=rolled_back,
            )

        except CutoverError as e:
            logger.error(
                "Cutover error for tenant %s: %s",
                tenant_id,
                e,
            )
            rolled_back = await self._rollback(tenant_id, migration_id)
            elapsed_ms = (time.perf_counter() - start_time) * 1000
            return CutoverResult(
                success=False,
                duration_ms=elapsed_ms,
                events_synced=events_synced,
                error_message=str(e),
                rolled_back=rolled_back,
            )

        except Exception as e:
            logger.exception(
                "Unexpected error during cutover for tenant %s",
                tenant_id,
            )
            rolled_back = await self._rollback(tenant_id, migration_id)
            elapsed_ms = (time.perf_counter() - start_time) * 1000
            return CutoverResult(
                success=False,
                duration_ms=elapsed_ms,
                events_synced=events_synced,
                error_message=f"Unexpected error: {e}",
                rolled_back=rolled_back,
            )

        finally:
            # Always resume writes, regardless of success/failure
            await self._router.resume_writes(tenant_id)
            logger.debug("Resumed writes for tenant %s", tenant_id)

    async def _rollback(
        self,
        tenant_id: UUID,
        migration_id: UUID,
    ) -> bool:
        """
        Rollback to dual-write state after cutover failure.

        Restores the routing state to DUAL_WRITE so the migration can
        continue or be retried.

        Args:
            tenant_id: Tenant being migrated.
            migration_id: ID of the migration.

        Returns:
            True if rollback was successful, False otherwise.
        """
        with self._tracer.span(
            "eventsource.cutover.rollback",
            {
                ATTR_TENANT_ID: str(tenant_id),
                ATTR_MIGRATION_ID: str(migration_id),
            },
        ):
            try:
                await self._routing_repo.set_migration_state(
                    tenant_id,
                    TenantMigrationState.DUAL_WRITE,
                    migration_id=migration_id,
                )
                logger.info(
                    "Rolled back tenant %s to DUAL_WRITE state",
                    tenant_id,
                )
                return True

            except Exception as e:
                logger.error(
                    "Failed to rollback tenant %s: %s",
                    tenant_id,
                    e,
                )
                return False

    async def validate_cutover_readiness(
        self,
        tenant_id: UUID,
        lag_tracker: SyncLagTracker,
        config: MigrationConfig | None = None,
    ) -> tuple[bool, str | None]:
        """
        Validate that conditions are met for cutover to proceed.

        This is a pre-check that can be called before execute_cutover()
        to verify readiness without actually starting the cutover.

        Args:
            tenant_id: Tenant to validate.
            lag_tracker: SyncLagTracker with current lag information.
            config: Optional migration configuration.

        Returns:
            Tuple of (is_ready, error_message).
            If is_ready is True, error_message is None.

        Example:
            >>> ready, error = await cutover.validate_cutover_readiness(
            ...     tenant_id=tenant_id,
            ...     lag_tracker=lag_tracker,
            ... )
            >>> if ready:
            ...     result = await cutover.execute_cutover(...)
            ... else:
            ...     print(f"Not ready: {error}")
        """
        config = config or MigrationConfig()

        with self._tracer.span(
            "eventsource.cutover.validate_readiness",
            {
                ATTR_TENANT_ID: str(tenant_id),
            },
        ):
            # Check 1: Verify sync lag
            await lag_tracker.calculate_lag()
            lag = lag_tracker.current_lag

            if lag is None:
                return False, "Unable to calculate sync lag"

            if not lag.is_within_threshold(config.cutover_max_lag_events):
                return (
                    False,
                    f"Sync lag too high: {lag.events} events (max {config.cutover_max_lag_events})",
                )

            # Check 2: Verify routing state is DUAL_WRITE
            routing = await self._routing_repo.get_routing(tenant_id)

            if routing is None:
                return False, "No routing configuration found for tenant"

            if routing.migration_state != TenantMigrationState.DUAL_WRITE:
                return (
                    False,
                    f"Invalid migration state: {routing.migration_state.value} (expected DUAL_WRITE)",
                )

            # Check 3: Verify lock is available (non-blocking check)
            lock_key = migration_lock_key(tenant_id, "cutover")
            lock_info = await self._lock_manager.try_acquire(lock_key)

            if lock_info is None:
                return False, "Cutover lock is already held by another process"

            # Release the lock immediately - we just wanted to check availability
            await self._lock_manager.release(lock_key)

            return True, None


__all__ = [
    "CutoverManager",
]
