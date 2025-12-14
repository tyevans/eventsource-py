"""
BulkCopier - Streams historical events from source to target store.

The BulkCopier handles the bulk copy phase of migration, efficiently
streaming historical events from the source store to the target store.
It supports resumable operations and progress tracking.

Responsibilities:
    - Stream events from source to target in batches
    - Track copy progress for resumption
    - Record position mappings for subscription continuity
    - Handle backpressure and rate limiting
    - Report progress for monitoring

Performance Characteristics:
    - Batch size configurable (default 1000 events)
    - Supports pause/resume for operational flexibility
    - Minimal impact on source store (read-only operations)

Usage:
    >>> from eventsource.migration import BulkCopier
    >>>
    >>> copier = BulkCopier(source_store, target_store, migration_repo)
    >>>
    >>> # Copy all events for a migration
    >>> async for progress in copier.run(migration):
    ...     print(f"Copied {progress.events_copied} events")

See Also:
    - Task: P1-006-bulk-copier.md
    - FRD: docs/tasks/multi-tenant-live-migration/multi-tenant-live-migration.md
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.migration.exceptions import BulkCopyError
from eventsource.migration.models import Migration
from eventsource.observability import Tracer, create_tracer
from eventsource.stores.interface import EventStore, ReadOptions, StoredEvent

if TYPE_CHECKING:
    from eventsource.migration.position_mapper import PositionMapper
    from eventsource.migration.repositories.migration import MigrationRepository

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BulkCopyProgress:
    """
    Progress information for bulk copy operation.

    Provides real-time metrics about the bulk copy progress including
    events processed, processing rate, and estimated completion time.

    Attributes:
        migration_id: ID of the migration being processed.
        events_copied: Number of events successfully copied.
        events_total: Total events to copy (0 if not yet counted).
        last_source_position: Last processed position in source store.
        last_target_position: Last written position in target store.
        events_per_second: Current processing rate.
        estimated_remaining_seconds: Estimated time to completion (None if unknown).
        is_complete: Whether the copy operation has finished.
    """

    migration_id: UUID
    events_copied: int
    events_total: int
    last_source_position: int
    last_target_position: int
    events_per_second: float
    estimated_remaining_seconds: float | None
    is_complete: bool

    @property
    def progress_percent(self) -> float:
        """
        Calculate progress as percentage (0-100).

        Returns:
            Progress percentage, or 0.0 if total is unknown.
        """
        if self.events_total == 0:
            return 0.0
        return min(100.0, (self.events_copied / self.events_total) * 100)


@dataclass
class BulkCopyResult:
    """
    Result of a completed bulk copy operation.

    Attributes:
        success: Whether the copy completed successfully.
        events_copied: Total number of events copied.
        last_source_position: Final source position processed.
        last_target_position: Final target position written.
        duration_seconds: Total time taken for the operation.
        error_message: Error message if the operation failed.
    """

    success: bool
    events_copied: int
    last_source_position: int
    last_target_position: int
    duration_seconds: float
    error_message: str | None = None


class RateLimiter:
    """
    Simple token bucket rate limiter for controlling event throughput.

    Limits the rate of events processed per second using a token bucket
    algorithm. Tokens are refilled based on elapsed time.

    Attributes:
        _max_rate: Maximum events allowed per second.
        _tokens: Current available tokens.
        _last_update: Time of last token update.
        _lock: Async lock for thread-safe operation.
    """

    def __init__(self, max_rate: int) -> None:
        """
        Initialize rate limiter.

        Args:
            max_rate: Maximum events per second (must be > 0).
        """
        self._max_rate = max_rate
        self._tokens = float(max_rate)
        self._last_update = time.monotonic()
        self._lock = asyncio.Lock()

    async def wait(self, count: int) -> None:
        """
        Wait for capacity to process `count` events.

        If insufficient tokens are available, sleeps until enough
        tokens have been accumulated.

        Args:
            count: Number of events to process.
        """
        if self._max_rate <= 0:
            return  # No rate limiting

        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_update
            self._last_update = now

            # Add tokens based on time elapsed
            self._tokens = min(
                self._max_rate,
                self._tokens + elapsed * self._max_rate,
            )

            # Wait if we need more tokens
            if count > self._tokens:
                wait_time = (count - self._tokens) / self._max_rate
                await asyncio.sleep(wait_time)
                self._tokens = 0
            else:
                self._tokens -= count


class BulkCopier:
    """
    Streams historical events from source to target store.

    The BulkCopier is responsible for the bulk copy phase of tenant migration.
    It reads events from the source store filtered by tenant ID and writes
    them to the target store in batches. It supports:

    - Batched processing for efficiency
    - Checkpoint persistence for crash recovery
    - Rate limiting to prevent overwhelming stores
    - Progress callbacks for monitoring
    - Pause/resume for operational control

    Example:
        >>> copier = BulkCopier(
        ...     source_store=source,
        ...     target_store=target,
        ...     migration_repo=repo,
        ... )
        >>>
        >>> async for progress in copier.run(migration):
        ...     print(f"Progress: {progress.progress_percent:.1f}%")

    Attributes:
        _source: Source event store to read from.
        _target: Target event store to write to.
        _migration_repo: Repository for progress persistence.
        _position_mapper: Optional mapper for position translation.
        _is_cancelled: Flag indicating cancellation requested.
        _is_paused: Flag indicating operation is paused.
        _pause_event: Event for pause/resume synchronization.
    """

    def __init__(
        self,
        source_store: EventStore,
        target_store: EventStore,
        migration_repo: MigrationRepository,
        *,
        position_mapper: PositionMapper | None = None,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ) -> None:
        """
        Initialize the bulk copier.

        Args:
            source_store: EventStore to read from.
            target_store: EventStore to write to.
            migration_repo: Repository for progress persistence.
            position_mapper: Optional mapper for tracking position translations.
            tracer: Optional custom Tracer instance. If not provided, one is
                   created based on enable_tracing setting.
            enable_tracing: Whether to enable OpenTelemetry tracing.
                          Ignored if tracer is explicitly provided.
        """
        # Composition-based tracing (replaces TracingMixin)
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._source = source_store
        self._target = target_store
        self._migration_repo = migration_repo
        self._position_mapper = position_mapper

        # State
        self._is_cancelled = False
        self._is_paused = False
        self._pause_event = asyncio.Event()
        self._pause_event.set()  # Not paused initially

    async def run(
        self,
        migration: Migration,
        progress_callback: Callable[[BulkCopyProgress], None] | None = None,
    ) -> AsyncIterator[BulkCopyProgress]:
        """
        Run the bulk copy operation.

        Streams events from source to target, yielding progress updates.
        Can be resumed from last checkpoint if interrupted.

        Args:
            migration: Migration instance with configuration.
            progress_callback: Optional callback for progress updates.

        Yields:
            BulkCopyProgress instances with copy status.

        Raises:
            BulkCopyError: If copy fails with unrecoverable error.
        """
        config = migration.config
        tenant_id = migration.tenant_id

        with self._tracer.span(
            "eventsource.bulk_copier.run",
            {
                "migration.id": str(migration.id),
                "tenant_id": str(tenant_id),
                "batch_size": config.batch_size,
            },
        ):
            self._is_cancelled = False
            start_time = time.monotonic()

            # Determine starting position (resume from checkpoint)
            from_position = migration.last_source_position
            events_copied = migration.events_copied
            last_target_position = migration.last_target_position
            last_source_position = from_position

            # Count total events if not already set
            if migration.events_total == 0:
                events_total = await self._count_tenant_events(tenant_id)
                await self._migration_repo.set_events_total(
                    migration.id,
                    events_total,
                )
            else:
                events_total = migration.events_total

            logger.info(
                "Starting bulk copy for tenant %s: %d events from position %d",
                tenant_id,
                events_total,
                from_position,
            )

            # Rate limiting setup
            rate_limiter = RateLimiter(config.max_bulk_copy_rate)

            # Process batches
            batch: list[StoredEvent] = []
            batch_start_position = from_position

            try:
                async for event in self._stream_tenant_events(
                    tenant_id,
                    from_position,
                ):
                    # Check for pause/cancel
                    await self._wait_if_paused()
                    if self._is_cancelled:
                        break

                    batch.append(event)

                    # Process batch when full
                    if len(batch) >= config.batch_size:
                        last_target_position = await self._write_batch(
                            migration.id,
                            tenant_id,
                            batch,
                        )

                        events_copied += len(batch)
                        last_source_position = batch[-1].global_position

                        # Update checkpoint
                        await self._migration_repo.update_progress(
                            migration.id,
                            events_copied,
                            last_source_position,
                            last_target_position,
                        )

                        # Rate limiting
                        await rate_limiter.wait(len(batch))

                        # Report progress
                        elapsed = time.monotonic() - start_time
                        rate = events_copied / elapsed if elapsed > 0 else 0

                        progress = BulkCopyProgress(
                            migration_id=migration.id,
                            events_copied=events_copied,
                            events_total=events_total,
                            last_source_position=last_source_position,
                            last_target_position=last_target_position,
                            events_per_second=rate,
                            estimated_remaining_seconds=(
                                (events_total - events_copied) / rate if rate > 0 else None
                            ),
                            is_complete=False,
                        )

                        if progress_callback:
                            progress_callback(progress)
                        yield progress

                        batch = []
                        batch_start_position = last_source_position

                # Process remaining events
                if batch and not self._is_cancelled:
                    last_target_position = await self._write_batch(
                        migration.id,
                        tenant_id,
                        batch,
                    )

                    events_copied += len(batch)
                    last_source_position = batch[-1].global_position

                    await self._migration_repo.update_progress(
                        migration.id,
                        events_copied,
                        last_source_position,
                        last_target_position,
                    )

                # Final progress
                elapsed = time.monotonic() - start_time
                rate = events_copied / elapsed if elapsed > 0 else 0

                final_progress = BulkCopyProgress(
                    migration_id=migration.id,
                    events_copied=events_copied,
                    events_total=events_total,
                    last_source_position=last_source_position,
                    last_target_position=last_target_position,
                    events_per_second=rate,
                    estimated_remaining_seconds=0.0,
                    is_complete=not self._is_cancelled,
                )

                if progress_callback:
                    progress_callback(final_progress)
                yield final_progress

                logger.info(
                    "Bulk copy completed: %d events in %.1fs",
                    events_copied,
                    elapsed,
                )

            except Exception as e:
                logger.error("Bulk copy failed: %s", e)
                await self._migration_repo.record_error(
                    migration.id,
                    str(e),
                )
                raise BulkCopyError(
                    migration.id,
                    batch_start_position,
                    str(e),
                ) from e

    def cancel(self) -> None:
        """
        Cancel the bulk copy operation.

        The operation will stop after the current batch completes.
        Progress is saved and can be resumed.
        """
        self._is_cancelled = True
        logger.info("Bulk copy cancellation requested")

    def pause(self) -> None:
        """
        Pause the bulk copy operation.

        Processing stops until resume() is called.
        Progress is preserved.
        """
        self._is_paused = True
        self._pause_event.clear()
        logger.info("Bulk copy paused")

    def resume(self) -> None:
        """
        Resume a paused bulk copy operation.
        """
        self._is_paused = False
        self._pause_event.set()
        logger.info("Bulk copy resumed")

    @property
    def is_cancelled(self) -> bool:
        """Check if cancellation has been requested."""
        return self._is_cancelled

    @property
    def is_paused(self) -> bool:
        """Check if the operation is paused."""
        return self._is_paused

    async def _wait_if_paused(self) -> None:
        """Wait if operation is paused."""
        if self._is_paused:
            await self._pause_event.wait()

    async def _count_tenant_events(self, tenant_id: UUID) -> int:
        """
        Count total events for tenant in source store.

        Args:
            tenant_id: Tenant UUID to count events for.

        Returns:
            Total number of events for the tenant.
        """
        with self._tracer.span(
            "eventsource.bulk_copier.count_events",
            {"tenant_id": str(tenant_id)},
        ):
            count = 0
            options = ReadOptions(tenant_id=tenant_id)

            async for _ in self._source.read_all(options):
                count += 1

            logger.debug("Counted %d events for tenant %s", count, tenant_id)
            return count

    async def _stream_tenant_events(
        self,
        tenant_id: UUID,
        from_position: int,
    ) -> AsyncIterator[StoredEvent]:
        """
        Stream events for tenant from source store.

        Args:
            tenant_id: Tenant UUID.
            from_position: Position to start from (exclusive).

        Yields:
            StoredEvent instances in global position order.
        """
        options = ReadOptions(
            from_position=from_position,
            tenant_id=tenant_id,
        )

        async for event in self._source.read_all(options):
            yield event

    async def _write_batch(
        self,
        migration_id: UUID,
        tenant_id: UUID,
        events: list[StoredEvent],
    ) -> int:
        """
        Write a batch of events to target store.

        Groups events by aggregate and writes them in order to
        maintain proper versioning in the target store.

        Args:
            migration_id: ID of the migration.
            tenant_id: Tenant UUID.
            events: List of StoredEvent instances to write.

        Returns:
            The last target position written.
        """
        with self._tracer.span(
            "eventsource.bulk_copier.write_batch",
            {
                "migration.id": str(migration_id),
                "tenant_id": str(tenant_id),
                "batch_size": len(events),
            },
        ):
            # Group events by aggregate for proper versioning
            # Events may come from different aggregates
            aggregates: dict[tuple[UUID, str], list[StoredEvent]] = {}

            for event in events:
                key = (event.aggregate_id, event.aggregate_type)
                if key not in aggregates:
                    aggregates[key] = []
                aggregates[key].append(event)

            last_target_position = 0

            # Write each aggregate's events
            for (aggregate_id, aggregate_type), agg_events in aggregates.items():
                # Get current version in target
                try:
                    current_version = await self._target.get_stream_version(
                        aggregate_id,
                        aggregate_type,
                    )
                except Exception:
                    current_version = 0

                # Convert StoredEvent back to DomainEvent for append
                domain_events = [e.event for e in agg_events]

                result = await self._target.append_events(
                    aggregate_id,
                    aggregate_type,
                    domain_events,
                    current_version,
                )

                last_target_position = max(last_target_position, result.global_position)

                # Record position mappings if mapper available
                if self._position_mapper:
                    for i, stored_event in enumerate(agg_events):
                        # Target position is estimated; actual may vary
                        target_pos = result.global_position - len(agg_events) + i + 1
                        await self._position_mapper.record_mapping(
                            migration_id,
                            stored_event.global_position,
                            target_pos,
                            stored_event.event_id,
                        )

            logger.debug(
                "Wrote batch of %d events for migration %s",
                len(events),
                migration_id,
            )
            return last_target_position
