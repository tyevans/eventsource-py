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
    >>> from eventsource.application.migration import MigrationCoordinator
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
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.application.migration.consistency import (
    ConsistencyVerifier,
    VerificationLevel,
    VerificationReport,
)
from eventsource.application.migration.coordinator_control import CoordinatorControlMixin
from eventsource.application.migration.coordinator_copy import CoordinatorCopyMixin
from eventsource.application.migration.coordinator_cutover import CoordinatorCutoverMixin
from eventsource.application.migration.coordinator_lifecycle import CoordinatorLifecycleMixin
from eventsource.application.migration.coordinator_post_cutover import CoordinatorPostCutoverMixin
from eventsource.application.migration.coordinator_status import CoordinatorStatusMixin
from eventsource.application.migration.subscription_migrator import (
    MigrationSummary,
    SubscriptionMigrator,
)
from eventsource.observability import Tracer, create_tracer
from eventsource.ports import FullEventStore

if TYPE_CHECKING:
    from eventsource.application.migration.bulk_copier import BulkCopier
    from eventsource.application.migration.cutover import CutoverManager
    from eventsource.application.migration.dual_write import DualWriteInterceptor
    from eventsource.application.migration.position_mapper import PositionMapper
    from eventsource.application.migration.router import TenantStoreRouter
    from eventsource.application.migration.sync_lag_tracker import SyncLagTracker
    from eventsource.ports.checkpoints import CheckpointRepository
    from eventsource.ports.locks import DistributedLock
    from eventsource.ports.migration.repositories import (
        MigrationAuditLogRepository,
        MigrationRepository,
        TenantRoutingRepository,
    )

logger = logging.getLogger(__name__)


class MigrationCoordinator(
    CoordinatorStatusMixin,
    CoordinatorControlMixin,
    CoordinatorPostCutoverMixin,
    CoordinatorCutoverMixin,
    CoordinatorCopyMixin,
    CoordinatorLifecycleMixin,
):
    """
    Orchestrates the complete migration lifecycle.

    Coordinates BulkCopier, DualWriteInterceptor, SyncLagTracker, and
    CutoverManager to perform zero-downtime tenant migrations.

    The coordinator manages migrations through phases:
    1. PENDING -> BULK_COPY: Install the dual-write interceptor, then copy
       historical events
    2. BULK_COPY -> DUAL_WRITE: Copy complete; only the mirror remains
    3. DUAL_WRITE -> CUTOVER: Perform atomic switch when sync lag is acceptable
    4. CUTOVER -> COMPLETED: Migration finished successfully
    """

    def __init__(
        self,
        source_store: FullEventStore,
        migration_repo: MigrationRepository,
        routing_repo: TenantRoutingRepository,
        router: TenantStoreRouter,
        *,
        source_store_id: str = "default",
        lock_manager: DistributedLock | None = None,
        position_mapper: PositionMapper | None = None,
        checkpoint_repo: CheckpointRepository | None = None,
        audit_log_repo: MigrationAuditLogRepository | None = None,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ):
        """Initialize the coordinator."""
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._source_store = source_store
        self._migration_repo = migration_repo
        self._routing_repo = routing_repo
        self._router = router
        self._source_store_id = source_store_id
        self._lock_manager = lock_manager
        self._audit_log_repo = audit_log_repo

        # Active copiers by migration_id
        self._active_copiers: dict[UUID, BulkCopier] = {}

        # Active background tasks by migration_id
        self._active_tasks: dict[UUID, asyncio.Task[None]] = {}

        # Status observers
        self._status_queues: dict[UUID, list[asyncio.Queue[UUID]]] = {}

        # Phase 2 (P2-005) additions: Dual-write and cutover support
        self._lag_trackers: dict[UUID, SyncLagTracker] = {}
        self._target_stores: dict[UUID, FullEventStore] = {}
        self._interceptors: dict[UUID, DualWriteInterceptor] = {}
        self._cutover_manager: CutoverManager | None = None

        # Phase 3 (P3-005) additions: Consistency verification and subscription migration
        self._position_mapper = position_mapper
        self._checkpoint_repo = checkpoint_repo
        self._consistency_reports: dict[UUID, VerificationReport] = {}
        self._subscription_summaries: dict[UUID, MigrationSummary] = {}


__all__ = [
    "ConsistencyVerifier",
    "MigrationCoordinator",
    "SubscriptionMigrator",
    "VerificationLevel",
]
