"""BulkCopier - Streams historical events from source to target store."""

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
    """Streams historical events from source to target store in batches."""

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
        """Initialize the bulk copier with source/target stores and migration repository."""
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._source = source_store
        self._target = target_store
        self._migration_repo = migration_repo
        self._position_mapper = position_mapper
        self._is_cancelled = False
        self._is_paused = False
        self._pause_event = asyncio.Event()
        self._pause_event.set()

    async def run(
        self,
        migration: Migration,
        progress_callback: Callable[[BulkCopyProgress], None] | None = None,
    ) -> AsyncIterator[BulkCopyProgress]:
        """Run the bulk copy operation, yielding BulkCopyProgress updates."""
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

            from_position = migration.last_source_position
            events_copied = migration.events_copied
            last_target_position = migration.last_target_position
            last_source_position = from_position

            if migration.events_total == 0:
                events_total = await self._count_tenant_events(tenant_id)
                await self._migration_repo.set_events_total(migration.id, events_total)
            else:
                events_total = migration.events_total

            logger.info(
                "Starting bulk copy for tenant %s: %d events from position %s",
                tenant_id,
                events_total,
                from_position.to_str() if from_position else "start",
            )

            rate_limiter = RateLimiter(config.max_bulk_copy_rate)
            batch: list[EventEnvelope] = []
            batch_start_position = from_position

            async def _flush_batch(current_batch: list[EventEnvelope]) -> float:
                nonlocal events_copied, last_source_position, last_target_position
                last_target_position = (
                    await self._write_batch(migration.id, tenant_id, current_batch)
                    or last_target_position
                )
                events_copied += len(current_batch)
                last_source_position = current_batch[-1].position
                await self._migration_repo.update_progress(
                    migration.id, events_copied, last_source_position, last_target_position
                )
                elapsed_s = time.monotonic() - start_time
                copy_rate = events_copied / elapsed_s if elapsed_s > 0 else 0
                metrics.record_events_copied(len(current_batch), copy_rate)
                return copy_rate

            try:
                async for event in self._stream_tenant_events(tenant_id, from_position):
                    await self._wait_if_paused()
                    if self._is_cancelled:
                        break

                    batch.append(event)
                    if len(batch) >= config.batch_size:
                        rate = await _flush_batch(batch)
                        await rate_limiter.wait(len(batch))
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

                if batch and not self._is_cancelled:
                    await _flush_batch(batch)

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
                logger.info("Bulk copy completed: %d events in %.1fs", events_copied, elapsed)
            except Exception as e:
                logger.error("Bulk copy failed: %s", e)
                await self._migration_repo.record_error(migration.id, str(e))
                raise BulkCopyError(migration.id, batch_start_position, str(e)) from e

    def cancel(self) -> None:
        """Cancel the bulk copy operation after the current batch completes."""
        self._is_cancelled = True
        logger.info("Bulk copy cancellation requested")

    def pause(self) -> None:
        """Pause the bulk copy operation until resume() is called."""
        self._is_paused = True
        self._pause_event.clear()
        logger.info("Bulk copy paused")

    def resume(self) -> None:
        """Resume a paused bulk copy operation."""
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


__all__ = ["BulkCopier", "BulkCopyProgress", "BulkCopyResult", "RateLimiter"]
