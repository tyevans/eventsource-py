"""
DualWriteInterceptor - Transparent dual-write during migration sync.

The DualWriteInterceptor intercepts write operations for a migrating
tenant, ensuring new events are written to both source and target stores.
It is installed before the bulk-copy pass starts and stays installed
through the DUAL_WRITE phase, so its mirror coverage overlaps the copy
with no gap: every event is either in the copier's feed snapshot or
mirrored by the interceptor. This maintains data consistency while
allowing the target store to catch up with the source.

Responsibilities:
    - Intercept write operations for migrating tenants
    - Write to source store first (authoritative)
    - Write to target store second (best-effort)
    - Handle target write failures gracefully without failing the operation
    - Track failed target writes for monitoring and background recovery
    - Satisfy the `FullEventStore` port for transparent integration

Consistency Guarantees:
    - Source write always succeeds or operation fails
    - Target write is best-effort during dual-write
    - Events are never lost (source is authoritative)
    - Failed target writes are tracked for eventual consistency via BulkCopier

Usage:
    >>> from eventsource.application.migration import DualWriteInterceptor
    >>>
    >>> interceptor = DualWriteInterceptor(
    ...     source_store=source,
    ...     target_store=target,
    ...     tenant_id=tenant_uuid,
    ... )
    >>>
    >>> # Use like any FullEventStore - writes go to both stores
    >>> result = await interceptor.append(stream, events, expected)
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
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.observability import (
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_EVENT_COUNT,
    ATTR_EXPECTED_VERSION,
    ATTR_TENANT_ID,
    Tracer,
    create_tracer,
)
from eventsource.ports import (
    AppendResult,
    CategoryReadOptions,
    EventEnvelope,
    ExpectedVersion,
    FeedReadOptions,
    FullEventStore,
    Position,
    StreamReadOptions,
)

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
        source_position: Position of the FIRST event of the source append
            (`AppendResult.position`), not the position after the write.
            None when the source store has no global feed.
    """

    timestamp: datetime
    aggregate_id: UUID
    aggregate_type: str
    event_ids: list[UUID]
    error_message: str
    source_position: Position | None


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


class DualWriteInterceptor:
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

        # Count of EVENTS (not appends) successfully mirrored to the target.
        # Statistics only -- it does NOT feed the lag calculation, because a
        # count cannot distinguish "five mirrored" from "five mirrored after
        # three were dropped".
        self._dual_write_success_count = 0

        # Sync watermarks. Source positions throughout, so they are always
        # mutually comparable.
        self._first_seen_source_position: Position | None = None
        self._last_synced_source_position: Position | None = None

        # Source positions of mirror failures not yet proven re-copied by
        # a bulk-copy checkpoint. min() of this list is the first-failed
        # watermark; `mark_copy_pass_complete` absorbs entries a completed
        # pass has provably re-delivered. Capped fail-closed: once
        # saturated, the anchor never advances again (see safe_lag_anchor).
        self._unabsorbed_failure_positions: list[Position] = []
        self._failure_positions_saturated = False

        # Set by `mark_copy_pass_complete`: a copy pass that STARTED AFTER
        # this interceptor was installed has completed, so no event was
        # ever mirrored by nobody and the install-window clamp releases.
        self._coverage_complete = False

    # =========================================================================
    # Public Properties
    # =========================================================================

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

    @property
    def dual_write_success_count(self) -> int:
        """Events successfully mirrored to the target since construction.

        Counts EVENTS, not append calls. STATISTICS ONLY -- it must never
        be subtracted from a lag count. A bare success count cannot tell
        "five mirrored" from "five mirrored after three were dropped", so
        subtracting it can report zero lag over a hole. Use
        `safe_lag_anchor` instead, which stops at the first failure.
        """
        return self._dual_write_success_count

    @property
    def first_seen_source_position(self) -> Position | None:
        """Source position of the FIRST append this interceptor handled.

        Set once, on the first append, whether or not the mirror
        succeeded -- it marks where this interceptor's coverage starts,
        not where it worked. Writes that landed before it were mirrored
        by nobody.
        """
        return self._first_seen_source_position

    @property
    def last_synced_source_position(self) -> Position | None:
        """Source position of the most recent successful mirror.

        The first-of-batch position, which is a CONSERVATIVE (never
        optimistic) watermark -- not a monotone one: concurrent mirrors
        can complete out of order, so this can move backward. That is
        safe because every clamp errs toward counting more lag, so a
        watermark that lags reality only ever refuses a cutover that
        would have been allowed, never the reverse. For a multi-event
        append the batch's remaining events sit after this position and
        keep counting as lag until the next successful mirror moves past
        them.
        """
        return self._last_synced_source_position

    @property
    def first_failed_source_position(self) -> Position | None:
        """Earliest source position of an unabsorbed mirror failure.

        A later mirror SUCCESS never clears or advances this -- it does
        not retroactively deliver the event that was dropped. The only
        release is `mark_copy_pass_complete`: a completed bulk-copy pass
        whose checkpoint reaches the failure proves the copier
        re-delivered the event, which absorbs it. This is what stops
        `safe_lag_anchor` from advancing over a hole.
        """
        if not self._unabsorbed_failure_positions:
            return None
        # Ordering over same-store positions, not arithmetic.
        return min(self._unabsorbed_failure_positions)

    def safe_lag_anchor(self, checkpoint: Position | None) -> Position | None:
        """The furthest source position provably present in the target.

        Counting lag from here is safe: every source event at or before
        the returned position is known to be in the target.

        Starts from `checkpoint` (the migration's `last_source_position`,
        i.e. what the bulk copy proved) and advances to the synced
        watermark only when BOTH clamps pass. The anchor never moves
        backward.

        Clamp 1 -- the install window. The copier stops at `checkpoint`
        and this interceptor's coverage starts at
        `first_seen_source_position`; anything in between was mirrored by
        nobody. The anchor may advance only when the checkpoint has
        reached the start of coverage, which is the only way the
        interceptor ALONE can know no such gap exists. The clamp is
        released for good by `mark_copy_pass_complete`, the coordinator's
        attestation that a copy pass beginning after installation
        completed -- installed-before-copy means the window is empty by
        construction. Fail-closed: without that attestation, when the two
        watermarks cannot be shown to meet the anchor stays put even if
        there happened to be no events in the window.

        Clamp 2 -- the failure. If mirroring has failed and no completed
        copy pass has absorbed the failure (see
        `mark_copy_pass_complete`), the anchor advances only when the
        whole synced run precedes that first unabsorbed failure.

        CONSEQUENCE: events stranded behind an unabsorbed failure block
        cutover until a copy pass absorbs them, advancing the checkpoint
        past them. Re-copying is safe -- the copier treats an event
        already in the target as already-copied and continues -- so the
        accepted failure mode here is stuck-until-recopied, never a
        cutover over missing data.

        Args:
            checkpoint: Last source position the bulk copy proved copied.

        Returns:
            The anchor to pass as `SyncLagTracker.calculate_lag(since=...)`.
        """
        candidate = self._last_synced_source_position
        if candidate is None:
            return checkpoint

        # Fail-closed under saturation: failure positions were dropped,
        # so no advancement can ever be shown safe again.
        if self._failure_positions_saturated:
            return checkpoint

        # Clamp 1: the checkpoint must have reached this interceptor's
        # coverage, or events between them were mirrored by nobody.
        # A completed covered copy pass proves the window empty instead.
        if not self._coverage_complete:
            first_seen = self._first_seen_source_position
            if first_seen is None:
                return checkpoint
            if checkpoint is None:
                # Nothing was copied, so everything before coverage is a gap.
                return None
            # Same store (source positions), so these orderings are always
            # defined -- PositionForeignError is impossible here.
            assert checkpoint.store_id == first_seen.store_id
            if checkpoint < first_seen:
                return checkpoint

        # Clamp 2: successes after a failure prove nothing about the hole.
        first_failed = self.first_failed_source_position
        if first_failed is not None:
            assert candidate.store_id == first_failed.store_id
            if candidate >= first_failed:
                return checkpoint

        if checkpoint is not None:
            assert candidate.store_id == checkpoint.store_id
            if candidate <= checkpoint:
                return checkpoint

        return candidate

    def mark_copy_pass_complete(self, checkpoint: Position | None) -> int:
        """Record that a covered bulk-copy pass completed at `checkpoint`.

        MUST only be called by the migration coordinator, and only for a
        copy pass that BEGAN AFTER this interceptor was installed and ran
        to completion -- both are ordering facts the interceptor cannot
        observe on its own. Two proofs follow from them:

        - The install window is empty. The pass's feed snapshot contains
          every event predating this interceptor's coverage, so no event
          was mirrored by nobody: the install-window clamp in
          `safe_lag_anchor` is released permanently.
        - Failures at or before `checkpoint` are absorbed. The copier
          verified every feed event through its checkpoint (appended, or
          confirmed already present), so a mirror that dropped one of
          those events was re-delivered by the copy. Later failures stay:
          the checkpoint proves nothing about them.

        Args:
            checkpoint: The completed pass's final source checkpoint
                (`Migration.last_source_position`); None when the tenant
                had no events to copy, which still proves the window
                empty but absorbs nothing.

        Returns:
            The number of unabsorbed mirror failures remaining. Non-zero
            means `safe_lag_anchor` stays clamped at the checkpoint until
            another completed pass absorbs them.
        """
        self._coverage_complete = True
        if checkpoint is not None:
            self._unabsorbed_failure_positions = [
                p for p in self._unabsorbed_failure_positions if p > checkpoint
            ]
        remaining = len(self._unabsorbed_failure_positions)
        if self._failure_positions_saturated:
            # Dropped positions are unknowable: never report clean.
            return remaining + 1
        return remaining

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
        events: Sequence[DomainEvent],
        error: Exception,
        source_position: Position | None,
    ) -> None:
        """
        Record a failed target write for monitoring and recovery.

        Args:
            aggregate_id: The aggregate that was being written to.
            aggregate_type: Type of the aggregate.
            events: The events that failed to write.
            error: The exception that caused the failure.
            source_position: Position of the first event of the successful
                source append; None for a feedless source store.
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
    # FullEventStore Port Implementation - Write Operations
    # =========================================================================

    async def append(
        self,
        stream: StreamId,
        events: Sequence[DomainEvent],
        expected: ExpectedVersion,
    ) -> AppendResult:
        """
        Append events to both source and target stores.

        Writes to source store first (authoritative), then attempts to write
        to target store. Source failures propagate to the caller. Target
        failures are recorded but don't fail the operation.

        The mirror does NOT forward the caller's `expected`: it appends to
        the target with the exact stream version the source held before
        this append (derived from the source result). The mirror therefore
        lands only when the target stream has fully converged with the
        source, which is what keeps the overlap with a running bulk-copy
        pass safe: a mirror can never leapfrog events the copier has not
        yet delivered, so the target's stream order always matches the
        source's -- even for callers appending with `any`. A mirror
        refused for non-convergence is recorded as a failure and the
        event reaches the target through the copy pass instead.

        Args:
            stream: Identity of the stream to append to.
            events: Events to append.
            expected: Optimistic-concurrency expectation.

        Returns:
            AppendResult from the source store write.

        Raises:
            OptimisticLockError: If the source store's version check fails.
            ValueError: If events list is empty.
        """
        if not events:
            raise ValueError("Cannot append empty event list")

        with self._tracer.span(
            "eventsource.dual_write.append",
            {
                ATTR_AGGREGATE_ID: str(stream.aggregate_id),
                ATTR_AGGREGATE_TYPE: stream.category,
                ATTR_TENANT_ID: str(self._tenant_id),
                ATTR_EVENT_COUNT: len(events),
                ATTR_EXPECTED_VERSION: expected.kind,
            },
        ):
            # Step 1: Write to source (authoritative). A concurrency
            # failure raises out of here, which is the honest propagation.
            source_result = await self._source.append(stream, events, expected)

            # Coverage starts at the first append handled, successful
            # mirror or not.
            if self._first_seen_source_position is None and source_result.position is not None:
                self._first_seen_source_position = source_result.position

            # Step 2: Write to target (best-effort)
            # Target failures are logged but don't fail the operation
            try:
                # Mirror at the exact pre-append source version so the
                # mirror only ever extends a converged target stream --
                # see the method docstring for why this is what makes
                # overlap with a bulk-copy pass safe.
                mirror_expected = ExpectedVersion.exact(source_result.new_version - len(events))
                await self._target.append(stream, events, mirror_expected)
                self._dual_write_success_count += len(events)
                if source_result.position is not None:
                    self._last_synced_source_position = source_result.position
                logger.debug(
                    f"Dual-write success for tenant {self._tenant_id}, stream {stream.render()}"
                )
            except Exception as e:
                # Log the failure but don't fail the operation
                logger.warning(
                    f"Target write failed for tenant {self._tenant_id}, "
                    f"stream {stream.render()}: {e}"
                )
                if source_result.position is not None:
                    if len(self._unabsorbed_failure_positions) >= self._max_failure_history:
                        # Fail-closed: dropping a position would make a
                        # future absorption unsound, so pin the anchor
                        # instead (see safe_lag_anchor).
                        self._failure_positions_saturated = True
                    else:
                        self._unabsorbed_failure_positions.append(source_result.position)
                self._record_sync_failure(
                    aggregate_id=stream.aggregate_id,
                    aggregate_type=stream.category,
                    events=events,
                    error=e,
                    source_position=source_result.position,
                )

            # Return the source result (operation succeeded)
            return source_result

    # =========================================================================
    # FullEventStore Port Implementation - Read Operations
    # =========================================================================

    async def read_category(
        self,
        category: str,
        options: CategoryReadOptions | None = None,
    ) -> AsyncIterator[EventEnvelope]:
        """
        Read a category from the source store.

        Args:
            category: The stream category (e.g. 'Order').
            options: Options for reading.

        Yields:
            EventEnvelope instances from the source store.
        """
        async for envelope in self._source.read_category(category, options):
            yield envelope

    async def event_exists(self, event_id: UUID) -> bool:
        """
        Check if an event exists in the source store.

        Args:
            event_id: ID of the event to check.

        Returns:
            True if event exists in source store.
        """
        return await self._source.event_exists(event_id)

    async def get_stream_version(self, stream: StreamId) -> int:
        """
        Get the current version of a stream from the source store.

        Args:
            stream: Identity of the stream.

        Returns:
            Current version (0 if the stream doesn't exist).
        """
        return await self._source.get_stream_version(stream)

    async def read_stream(
        self,
        stream: StreamId,
        options: StreamReadOptions | None = None,
    ) -> AsyncIterator[EventEnvelope]:
        """
        Read events from a stream in the source store.

        Args:
            stream: Identity of the stream.
            options: Options for reading.

        Yields:
            EventEnvelope instances from the source store.
        """
        async for envelope in self._source.read_stream(stream, options):
            yield envelope

    async def read_all(
        self,
        from_position: Position | None = None,
        options: FeedReadOptions | None = None,
    ) -> AsyncIterator[EventEnvelope]:
        """
        Read the global feed from the source store.

        Args:
            from_position: Read strictly after this source-store position.
            options: Options for reading.

        Yields:
            EventEnvelope instances from the source store.
        """
        async for envelope in self._source.read_all(from_position, options):
            yield envelope

    async def current_position(self) -> Position | None:
        """
        Get the current global-feed position of the SOURCE store.

        The returned position belongs to the source store and is not
        comparable with the target store's positions.

        Returns:
            The source store's latest position, or None if it is empty.
        """
        return await self._source.current_position()


__all__ = [
    "DualWriteInterceptor",
    "FailedWrite",
    "FailureStats",
]
