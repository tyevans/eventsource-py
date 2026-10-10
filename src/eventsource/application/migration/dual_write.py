"""
DualWriteInterceptor - Transparent dual-write during migration sync.

The DualWriteInterceptor intercepts write operations for a migrating
tenant, ensuring new events are written to both source and target stores.
It is installed before the bulk-copy pass starts and stays installed
through the DUAL_WRITE phase, so its mirror coverage overlaps the copy
with no gap: every event is either in the copier's feed snapshot or
mirrored by the interceptor. This maintains data consistency while
allowing the target store to catch up with the source.
"""

from __future__ import annotations

import logging
from uuid import UUID

from eventsource.application.migration.dual_write_store import DualWriteStoreMixin
from eventsource.application.migration.dual_write_tracking import DualWriteTrackingMixin
from eventsource.application.migration.dual_write_types import FailedWrite, FailureStats
from eventsource.application.migration.dual_write_watermarks import DualWriteWatermarksMixin
from eventsource.observability import Tracer, create_tracer
from eventsource.ports import FullEventStore, Position

logger = logging.getLogger(__name__)


class DualWriteInterceptor(
    DualWriteTrackingMixin,
    DualWriteWatermarksMixin,
    DualWriteStoreMixin,
):
    """
    Intercepts writes to duplicate to both stores during migration.

    Structural conformance only -- the interceptor satisfies
    `FullEventStore` by having its eight members, not by inheriting
    from any base class.

    Ensures new events are written to both source and target stores
    during the dual-write phase, maintaining consistency while the
    target catches up.

    Write semantics:
        - Source write must succeed, or the entire operation fails
        - Target write is best-effort; failures are logged but don't fail the operation
        - Failed target writes are tracked for background sync recovery

    The interceptor satisfies the `FullEventStore` port, making it a
    drop-in replacement that the TenantStoreRouter can use transparently.

    Example:
        >>> interceptor = DualWriteInterceptor(
        ...     source_store=shared_store,
        ...     target_store=dedicated_store,
        ...     tenant_id=tenant_uuid,
        ... )
        >>>
        >>> # Set on router during dual-write phase
        >>> router.set_dual_write_interceptor(tenant_id, interceptor)
        >>>
        >>> # Now writes automatically go to both stores
        >>> await router.append(stream, events, ExpectedVersion.exact(0))

    Attributes:
        _source: The authoritative source event store.
        _target: The target event store being migrated to.
        _tenant_id: The tenant this interceptor handles.
        _failed_writes: List of failed target writes for recovery.
        _affected_aggregates: Set of aggregate IDs with failed writes.
        _dual_write_success_count: Events successfully mirrored to the target.
        _first_seen_source_position: Where this interceptor's coverage starts.
        _last_synced_source_position: Watermark of the latest successful mirror.
        _unabsorbed_failure_positions: Mirror failures not yet proven
            re-copied by a completed bulk-copy pass.
        _coverage_complete: A copy pass starting after installation has
            completed, so the install window is provably empty.
    """

    def __init__(
        self,
        source_store: FullEventStore,
        target_store: FullEventStore,
        tenant_id: UUID,
        *,
        migration_id: UUID | None = None,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
        max_failure_history: int = 1000,
    ) -> None:
        """
        Initialize the dual-write interceptor.

        Args:
            source_store: The authoritative source event store.
            target_store: The target event store being migrated to.
            tenant_id: The tenant ID this interceptor is for.
            migration_id: Optional migration ID. When set, a failed mirror
                write reports to that migration's `MigrationMetrics`.
            tracer: Optional custom Tracer instance.
            enable_tracing: Whether to enable OpenTelemetry tracing.
            max_failure_history: Maximum number of failures to track.
        """
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._source = source_store
        self._target = target_store
        self._tenant_id = tenant_id
        self._migration_id = migration_id
        self._max_failure_history = max_failure_history

        # Failure tracking
        self._failed_writes: list[FailedWrite] = []
        self._affected_aggregates: set[UUID] = set()

        # Success count (statistics only)
        self._dual_write_success_count = 0

        # Sync watermarks
        self._first_seen_source_position: Position | None = None
        self._last_synced_source_position: Position | None = None

        # Unabsorbed failure positions
        self._unabsorbed_failure_positions: list[Position] = []
        self._failure_positions_saturated = False

        # Coverage complete attestation
        self._coverage_complete = False

    @property
    def source_store(self) -> FullEventStore:
        """Get the source (authoritative) event store."""
        return self._source

    @property
    def target_store(self) -> FullEventStore:
        """Get the target event store."""
        return self._target

    @property
    def tenant_id(self) -> UUID:
        """Get the tenant ID this interceptor handles."""
        return self._tenant_id


__all__ = [
    "DualWriteInterceptor",
    "FailedWrite",
    "FailureStats",
]
