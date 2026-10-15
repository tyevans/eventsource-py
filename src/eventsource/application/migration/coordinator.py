"""MigrationCoordinator - Orchestrates the zero-downtime tenant migration lifecycle."""

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
    """Orchestrates the complete migration lifecycle across PENDING, BULK_COPY, DUAL_WRITE, CUTOVER, and COMPLETED."""

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
        self._active_copiers: dict[UUID, BulkCopier] = {}
        self._active_tasks: dict[UUID, asyncio.Task[None]] = {}
        self._status_queues: dict[UUID, list[asyncio.Queue[UUID]]] = {}
        self._lag_trackers: dict[UUID, SyncLagTracker] = {}
        self._target_stores: dict[UUID, FullEventStore] = {}
        self._interceptors: dict[UUID, DualWriteInterceptor] = {}
        self._cutover_manager: CutoverManager | None = None
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
