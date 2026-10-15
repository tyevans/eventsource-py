"""BulkCopier - Streams historical events from source to target store."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncIterator, Callable
from typing import TYPE_CHECKING

from eventsource.application.migration.bulk_copier_reader import BulkCopierReaderMixin
from eventsource.application.migration.bulk_copier_types import (  # fmt: skip
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
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._source, self._target = source_store, target_store
        self._migration_repo, self._position_mapper = migration_repo, position_mapper
        self._is_cancelled = self._is_paused = False
        self._pause_event = asyncio.Event()
        self._pause_event.set()

    async def run(
        self,
        migration: Migration,
        progress_callback: Callable[[BulkCopyProgress], None] | None = None,
    ) -> AsyncIterator[BulkCopyProgress]:
        """Run the bulk copy operation, yielding BulkCopyProgress updates."""
        config, tenant_id = migration.config, migration.tenant_id
        span_attrs = {"migration.id": str(migration.id), "tenant_id": str(tenant_id), "batch_size": config.batch_size}  # fmt: skip
        with self._tracer.span("eventsource.bulk_copier.run", span_attrs):
            self._is_cancelled = False
            start_time = time.monotonic()
            metrics = get_migration_metrics(str(migration.id), str(tenant_id))
            from_pos = last_source_pos = migration.last_source_position
            events_copied, last_target_pos = migration.events_copied, migration.last_target_position

            if migration.events_total == 0:
                events_total = await self._count_tenant_events(tenant_id)
                await self._migration_repo.set_events_total(migration.id, events_total)
            else:
                events_total = migration.events_total

            pos_label = from_pos.to_str() if from_pos else "start"
            logger.info("Starting bulk copy for tenant %s: %d events from position %s", tenant_id, events_total, pos_label)  # fmt: skip
            rate_limiter = RateLimiter(config.max_bulk_copy_rate)
            batch: list[EventEnvelope] = []
            batch_start_pos = from_pos

            def _make_progress(rate: float, rem: float | None, done: bool) -> BulkCopyProgress:
                return BulkCopyProgress(migration.id, events_copied, events_total, last_source_pos, last_target_pos, rate, rem, done)  # fmt: skip

            async def _flush_batch(cur: list[EventEnvelope]) -> float:
                nonlocal events_copied, last_source_pos, last_target_pos
                written = await self._write_batch(migration.id, tenant_id, cur)
                last_target_pos = written or last_target_pos
                events_copied += len(cur)
                last_source_pos = cur[-1].position
                await self._migration_repo.update_progress(migration.id, events_copied, last_source_pos, last_target_pos)  # fmt: skip
                elapsed_s = time.monotonic() - start_time
                copy_rate = events_copied / elapsed_s if elapsed_s > 0 else 0
                metrics.record_events_copied(len(cur), copy_rate)
                return copy_rate

            try:
                async for event in self._stream_tenant_events(tenant_id, from_pos):
                    await self._wait_if_paused()
                    if self._is_cancelled:
                        break
                    batch.append(event)
                    if len(batch) >= config.batch_size:
                        rate = await _flush_batch(batch)
                        await rate_limiter.wait(len(batch))
                        rem = (events_total - events_copied) / rate if rate > 0 else None
                        progress = _make_progress(rate, rem, False)
                        if progress_callback:
                            progress_callback(progress)
                        yield progress
                        batch, batch_start_pos = [], last_source_pos

                if batch and not self._is_cancelled:
                    await _flush_batch(batch)
                elapsed = time.monotonic() - start_time
                rate = events_copied / elapsed if elapsed > 0 else 0
                final_progress = _make_progress(rate, 0.0, not self._is_cancelled)
                if progress_callback:
                    progress_callback(final_progress)
                yield final_progress
                logger.info("Bulk copy completed: %d events in %.1fs", events_copied, elapsed)
            except Exception as e:
                logger.error("Bulk copy failed: %s", e)
                await self._migration_repo.record_error(migration.id, str(e))
                raise BulkCopyError(migration.id, batch_start_pos, str(e)) from e

    def cancel(self) -> None:
        self._is_cancelled = True
        logger.info("Bulk copy cancellation requested")

    def pause(self) -> None:
        self._is_paused = True
        self._pause_event.clear()
        logger.info("Bulk copy paused")

    def resume(self) -> None:
        self._is_paused = False
        self._pause_event.set()
        logger.info("Bulk copy resumed")

    @property
    def is_cancelled(self) -> bool:
        return self._is_cancelled

    @property
    def is_paused(self) -> bool:
        return self._is_paused

    async def _wait_if_paused(self) -> None:
        if self._is_paused:
            await self._pause_event.wait()


__all__ = ["BulkCopier", "BulkCopyProgress", "BulkCopyResult", "RateLimiter"]
