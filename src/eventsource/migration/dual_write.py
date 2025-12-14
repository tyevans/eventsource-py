"""
DualWriteInterceptor - Transparent dual-write during migration sync.

The DualWriteInterceptor intercepts write operations during the dual-write
phase of migration, ensuring new events are written to both source and
target stores. This maintains data consistency while allowing the target
store to catch up with the source.

Responsibilities:
    - Intercept write operations for migrating tenants
    - Write to source store first (authoritative)
    - Write to target store second (best-effort)
    - Handle target write failures gracefully without failing the operation
    - Track failed target writes for monitoring and background recovery
    - Implement EventStore protocol for transparent integration

Consistency Guarantees:
    - Source write always succeeds or operation fails
    - Target write is best-effort during dual-write
    - Events are never lost (source is authoritative)
    - Failed target writes are tracked for eventual consistency via BulkCopier

Usage:
    >>> from eventsource.migration import DualWriteInterceptor
    >>>
    >>> interceptor = DualWriteInterceptor(
    ...     source_store=source,
    ...     target_store=target,
    ...     tenant_id=tenant_uuid,
    ... )
    >>>
    >>> # Use like any EventStore - writes go to both stores
    >>> result = await interceptor.append_events(
    ...     aggregate_id, "Order", events, expected_version
    ... )
    >>>
    >>> # Check failure statistics
    >>> stats = interceptor.get_failure_stats()
    >>> print(f"Failed writes: {stats['total_failures']}")

See Also:
    - Task: P2-001-dual-write-interceptor.md
    - FRD: docs/tasks/multi-tenant-live-migration/multi-tenant-live-migration.md
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID

from eventsource.events.base import DomainEvent
from eventsource.observability import (
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_EVENT_COUNT,
    ATTR_EXPECTED_VERSION,
    ATTR_TENANT_ID,
    Tracer,
    create_tracer,
)
from eventsource.stores.interface import (
    AppendResult,
    EventStore,
    EventStream,
    ReadOptions,
    StoredEvent,
)

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


@dataclass
class FailedWrite:
    """
    Records a failed write to the target store.

    Used for tracking and monitoring failed dual-writes, enabling
    background recovery via the BulkCopier catch-up mechanism.

    Attributes:
        timestamp: When the failure occurred.
        aggregate_id: The aggregate that was being written to.
        aggregate_type: Type of the aggregate.
        event_ids: IDs of events that failed to write.
        error_message: The error message from the failed write.
        source_position: Global position in source store after successful write.
    """

    timestamp: datetime
    aggregate_id: UUID
    aggregate_type: str
    event_ids: list[UUID]
    error_message: str
    source_position: int


@dataclass
class FailureStats:
    """
    Statistics about dual-write failures.

    Provides aggregate metrics for monitoring dual-write health.

    Attributes:
        total_failures: Total number of failed target writes.
        total_events_failed: Total number of events that failed to write.
        first_failure_at: Timestamp of the first failure.
        last_failure_at: Timestamp of the most recent failure.
        unique_aggregates_affected: Number of unique aggregates affected.
    """

    total_failures: int = 0
    total_events_failed: int = 0
    first_failure_at: datetime | None = None
    last_failure_at: datetime | None = None
    unique_aggregates_affected: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "total_failures": self.total_failures,
            "total_events_failed": self.total_events_failed,
            "first_failure_at": (
                self.first_failure_at.isoformat() if self.first_failure_at else None
            ),
            "last_failure_at": (self.last_failure_at.isoformat() if self.last_failure_at else None),
            "unique_aggregates_affected": self.unique_aggregates_affected,
        }


class DualWriteInterceptor(EventStore):
    """
    Intercepts writes to duplicate to both stores during migration.

    Ensures new events are written to both source and target stores
    during the dual-write phase, maintaining consistency while the
    target catches up.

    Write semantics:
        - Source write must succeed, or the entire operation fails
        - Target write is best-effort; failures are logged but don't fail the operation
        - Failed target writes are tracked for background sync recovery

    The interceptor implements the EventStore protocol, making it a
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
        >>> await router.append_events(agg_id, "Order", events, 0)

    Attributes:
        _source: The authoritative source event store.
        _target: The target event store being migrated to.
        _tenant_id: The tenant this interceptor handles.
        _failed_writes: List of failed target writes for recovery.
        _affected_aggregates: Set of aggregate IDs with failed writes.
    """

    def __init__(
        self,
        source_store: EventStore,
        target_store: EventStore,
        tenant_id: UUID,
        *,
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
            tracer: Optional custom Tracer instance.
            enable_tracing: Whether to enable OpenTelemetry tracing.
            max_failure_history: Maximum number of failures to track (older entries
                are discarded to prevent unbounded memory growth).
        """
        # Composition-based tracing (replaces TracingMixin)
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._source = source_store
        self._target = target_store
        self._tenant_id = tenant_id
        self._max_failure_history = max_failure_history

        # Failure tracking
        self._failed_writes: list[FailedWrite] = []
        self._affected_aggregates: set[UUID] = set()

    # =========================================================================
    # Public Properties
    # =========================================================================

    @property
    def source_store(self) -> EventStore:
        """Get the source (authoritative) event store."""
        return self._source

    @property
    def target_store(self) -> EventStore:
        """Get the target event store."""
        return self._target

    @property
    def tenant_id(self) -> UUID:
        """Get the tenant ID this interceptor handles."""
        return self._tenant_id

    # =========================================================================
    # Failure Tracking
    # =========================================================================

    def get_failed_writes(self) -> list[FailedWrite]:
        """
        Get the list of failed target writes.

        Returns:
            List of FailedWrite records in chronological order.
        """
        return list(self._failed_writes)

    def get_failure_stats(self) -> FailureStats:
        """
        Get aggregate statistics about write failures.

        Returns:
            FailureStats with summary metrics.
        """
        if not self._failed_writes:
            return FailureStats()

        total_events = sum(len(fw.event_ids) for fw in self._failed_writes)

        return FailureStats(
            total_failures=len(self._failed_writes),
            total_events_failed=total_events,
            first_failure_at=self._failed_writes[0].timestamp,
            last_failure_at=self._failed_writes[-1].timestamp,
            unique_aggregates_affected=len(self._affected_aggregates),
        )

    def clear_failure_history(self) -> int:
        """
        Clear the failure history.

        Useful after background sync has caught up and recovered all failures.

        Returns:
            Number of failure records cleared.
        """
        count = len(self._failed_writes)
        self._failed_writes.clear()
        self._affected_aggregates.clear()
        return count

    def _record_sync_failure(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
        events: list[DomainEvent],
        error: Exception,
        source_position: int,
    ) -> None:
        """
        Record a failed target write for monitoring and recovery.

        Args:
            aggregate_id: The aggregate that was being written to.
            aggregate_type: Type of the aggregate.
            events: The events that failed to write.
            error: The exception that caused the failure.
            source_position: Global position in source after successful write.
        """
        failed_write = FailedWrite(
            timestamp=datetime.now(UTC),
            aggregate_id=aggregate_id,
            aggregate_type=aggregate_type,
            event_ids=[e.event_id for e in events],
            error_message=str(error),
            source_position=source_position,
        )

        self._failed_writes.append(failed_write)
        self._affected_aggregates.add(aggregate_id)

        # Trim old failures to prevent unbounded growth
        if len(self._failed_writes) > self._max_failure_history:
            # Remove oldest entries
            removed = self._failed_writes[: -self._max_failure_history]
            self._failed_writes = self._failed_writes[-self._max_failure_history :]

            # Rebuild affected aggregates set
            self._affected_aggregates = {fw.aggregate_id for fw in self._failed_writes}

            logger.debug(f"Trimmed {len(removed)} old failure records for tenant {self._tenant_id}")

    # =========================================================================
    # EventStore Protocol Implementation - Write Operations
    # =========================================================================

    async def append_events(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
        events: list[DomainEvent],
        expected_version: int,
    ) -> AppendResult:
        """
        Append events to both source and target stores.

        Writes to source store first (authoritative), then attempts to write
        to target store. Source failures propagate as operation failures.
        Target failures are logged but don't fail the operation.

        Args:
            aggregate_id: ID of the aggregate.
            aggregate_type: Type of aggregate (e.g., 'Order').
            events: Events to append.
            expected_version: Expected current version (for optimistic locking).

        Returns:
            AppendResult from the source store write.

        Raises:
            OptimisticLockError: If source store version check fails.
            ValueError: If events list is empty.
        """
        if not events:
            raise ValueError("Cannot append empty event list")

        with self._tracer.span(
            "eventsource.dual_write.append_events",
            {
                ATTR_AGGREGATE_ID: str(aggregate_id),
                ATTR_AGGREGATE_TYPE: aggregate_type,
                ATTR_TENANT_ID: str(self._tenant_id),
                ATTR_EVENT_COUNT: len(events),
                ATTR_EXPECTED_VERSION: expected_version,
            },
        ):
            # Step 1: Write to source (authoritative)
            # If this fails, the entire operation fails
            source_result = await self._source.append_events(
                aggregate_id,
                aggregate_type,
                events,
                expected_version,
            )

            # If source write failed (conflict), return immediately
            if not source_result.success:
                return source_result

            # Step 2: Write to target (best-effort)
            # Target failures are logged but don't fail the operation
            try:
                await self._target.append_events(
                    aggregate_id,
                    aggregate_type,
                    events,
                    expected_version,
                )
                logger.debug(
                    f"Dual-write success for tenant {self._tenant_id}, aggregate {aggregate_id}"
                )
            except Exception as e:
                # Log the failure but don't fail the operation
                logger.warning(
                    f"Target write failed for tenant {self._tenant_id}, "
                    f"aggregate {aggregate_id}: {e}"
                )
                self._record_sync_failure(
                    aggregate_id=aggregate_id,
                    aggregate_type=aggregate_type,
                    events=events,
                    error=e,
                    source_position=source_result.global_position,
                )

            # Return the source result (operation succeeded)
            return source_result

    # =========================================================================
    # EventStore Protocol Implementation - Read Operations
    # =========================================================================

    async def get_events(
        self,
        aggregate_id: UUID,
        aggregate_type: str | None = None,
        from_version: int = 0,
        from_timestamp: datetime | None = None,
        to_timestamp: datetime | None = None,
    ) -> EventStream:
        """
        Get events from the source store.

        During dual-write, reads always come from the authoritative source.

        Args:
            aggregate_id: ID of the aggregate.
            aggregate_type: Type of aggregate (optional).
            from_version: Start from this version (default: 0).
            from_timestamp: Only get events after this timestamp.
            to_timestamp: Only get events before this timestamp.

        Returns:
            EventStream containing the aggregate's events.
        """
        return await self._source.get_events(
            aggregate_id,
            aggregate_type,
            from_version,
            from_timestamp,
            to_timestamp,
        )

    async def get_events_by_type(
        self,
        aggregate_type: str,
        tenant_id: UUID | None = None,
        from_timestamp: datetime | None = None,
    ) -> list[DomainEvent]:
        """
        Get all events for a specific aggregate type from source store.

        Args:
            aggregate_type: Type of aggregate (e.g., 'Order').
            tenant_id: Filter by tenant ID (optional).
            from_timestamp: Only get events after this timestamp.

        Returns:
            List of events in chronological order.
        """
        return await self._source.get_events_by_type(
            aggregate_type,
            tenant_id,
            from_timestamp,
        )

    async def event_exists(self, event_id: UUID) -> bool:
        """
        Check if an event exists in the source store.

        Args:
            event_id: ID of the event to check.

        Returns:
            True if event exists in source store.
        """
        return await self._source.event_exists(event_id)

    async def get_stream_version(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
    ) -> int:
        """
        Get the current version of an aggregate from source store.

        Args:
            aggregate_id: ID of the aggregate.
            aggregate_type: Type of aggregate.

        Returns:
            Current version (0 if aggregate doesn't exist).
        """
        return await self._source.get_stream_version(aggregate_id, aggregate_type)

    async def read_stream(
        self,
        stream_id: str,
        options: ReadOptions | None = None,
    ) -> AsyncIterator[StoredEvent]:
        """
        Read events from a stream in the source store.

        Args:
            stream_id: The stream identifier.
            options: Options for reading.

        Yields:
            StoredEvent instances from source store.
        """
        async for event in self._source.read_stream(stream_id, options):
            yield event

    async def read_all(
        self,
        options: ReadOptions | None = None,
    ) -> AsyncIterator[StoredEvent]:
        """
        Read all events from the source store.

        Args:
            options: Options for reading.

        Yields:
            StoredEvent instances from source store.
        """
        async for event in self._source.read_all(options):
            yield event

    async def get_global_position(self) -> int:
        """
        Get the global position from the source store.

        Returns:
            The maximum global position in the source store.
        """
        return await self._source.get_global_position()


__all__ = [
    "DualWriteInterceptor",
    "FailedWrite",
    "FailureStats",
]
