"""
CutoverManager - Atomic switch with minimal pause.

Handles the cutover phase of migration, performing an atomic switch
from source to target store with bounded pause time.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from eventsource.application.migration.cutover_execution import CutoverExecutionMixin
from eventsource.application.migration.cutover_readiness import CutoverReadinessMixin
from eventsource.application.migration.cutover_rollback import CutoverRollbackMixin
from eventsource.observability import Tracer, create_tracer

if TYPE_CHECKING:
    from eventsource.application.migration.router import TenantStoreRouter
    from eventsource.ports.locks import DistributedLock
    from eventsource.ports.migration.repositories import TenantRoutingRepository

logger = logging.getLogger(__name__)


# Custom attribute keys for cutover tracing.
ATTR_CUTOVER_MIGRATION_ID = "eventsource.cutover.migration_id"
ATTR_CUTOVER_TIMEOUT_MS = "eventsource.cutover.timeout_ms"
ATTR_CUTOVER_DURATION_MS = "eventsource.cutover.duration_ms"
ATTR_CUTOVER_SUCCESS = "eventsource.cutover.success"
ATTR_CUTOVER_ROLLED_BACK = "eventsource.cutover.rolled_back"
ATTR_SYNC_LAG_EVENTS = "eventsource.cutover.sync_lag_events"
ATTR_TARGET_STORE_ID = "eventsource.cutover.target_store_id"


class CutoverManager(
    CutoverRollbackMixin,
    CutoverReadinessMixin,
    CutoverExecutionMixin,
):
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
        _lock_manager: Distributed lock manager for coordination.
        _router: TenantStoreRouter for write pause/resume and routing.
        _routing_repo: Repository for atomic routing state updates.
        _lock_acquisition_timeout: Timeout for acquiring advisory lock.
    """

    def __init__(
        self,
        lock_manager: DistributedLock,
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
            lock_manager: Distributed lock manager for distributed coordination.
            router: TenantStoreRouter for managing write pause/resume.
            routing_repo: Repository for updating tenant routing state.
            lock_acquisition_timeout: Timeout in seconds for acquiring the advisory lock.
                Defaults to 0.5 seconds (500ms) as per requirements.
            tracer: Optional custom Tracer instance.
            enable_tracing: Whether to enable OpenTelemetry tracing.
        """
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._lock_manager = lock_manager
        self._router = router
        self._routing_repo = routing_repo
        self._lock_acquisition_timeout = lock_acquisition_timeout


__all__ = [
    "ATTR_CUTOVER_DURATION_MS",
    "ATTR_CUTOVER_MIGRATION_ID",
    "ATTR_CUTOVER_ROLLED_BACK",
    "ATTR_CUTOVER_SUCCESS",
    "ATTR_CUTOVER_TIMEOUT_MS",
    "ATTR_SYNC_LAG_EVENTS",
    "ATTR_TARGET_STORE_ID",
    "CutoverManager",
]
