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
    >>> from eventsource.application.migration import BulkCopier
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
from typing import TYPE_CHECKING

from eventsource.application.migration.bulk_copier_reader import BulkCopierReaderMixin
from eventsource.application.migration.bulk_copier_types import (
    BulkCopyProgress,
    BulkCopyResult,
    RateLimiter,
)
from eventsource.application.migration.bulk_copier_writer import BulkCopierWriterMixin
from eventsource.application.migration.exceptions import BulkCopyError
from eventsource.application.migration.metrics import get_migration_metrics
from eventsource.observability import Tracer, create_tracer
from eventsource.ports import EventEnvelope, FullEventStore
from eventsource.ports.migration.models import Migration

if TYPE_CHECKING:
    from eventsource.application.migration.position_mapper import PositionMapper
    from eventsource.ports.migration.repositories import MigrationRepository

logger = logging.getLogger(__name__)


class BulkCopier(BulkCopierReaderMixin, BulkCopierWriterMixin):
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
        source_store: FullEventStore,
        target_store: FullEventStore,
        migration_repo: MigrationRepository,
        *,
        position_mapper: PositionMapper | None = None,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ) -> None:
        """
        Initialize the bulk copier.

        Args:
            source_store: FullEventStore to read from.
            target_store: FullEventStore to write to.
            migration_repo: Repository for progress persistence.
            position_mapper: Optional mapper for tracking position translations.
                When configured, events are appended one at a time so each
                event's exact target position can be recorded.
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
            metrics = get_migration_metrics(str(migration.id), str(tenant_id))

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
                "Starting bulk copy for tenant %s: %d events from position %s",
                tenant_id,
                events_total,
                from_position.to_str() if from_position else "start",
            )

            # Rate limiting setup
            rate_limiter = RateLimiter(config.max_bulk_copy_rate)

            # Process batches
            batch: list[EventEnvelope] = []
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
                        # An all-duplicate batch appends nothing and
                        # returns None; keep the last real position rather
                        # than nulling progress that was genuinely made.
                        last_target_position = (
                            await self._write_batch(migration.id, tenant_id, batch)
                            or last_target_position
                        )

                        events_copied += len(batch)
                        last_source_position = batch[-1].position

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
                        metrics.record_events_copied(len(batch), rate)

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
                    last_target_position = (
                        await self._write_batch(migration.id, tenant_id, batch)
                        or last_target_position
                    )

                    events_copied += len(batch)
                    last_source_position = batch[-1].position

                    await self._migration_repo.update_progress(
                        migration.id,
                        events_copied,
                        last_source_position,
                        last_target_position,
                    )
                    elapsed = time.monotonic() - start_time
                    rate = events_copied / elapsed if elapsed > 0 else 0
                    metrics.record_events_copied(len(batch), rate)

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


__all__ = [
    "BulkCopier",
    "BulkCopyProgress",
    "BulkCopyResult",
    "RateLimiter",
]
