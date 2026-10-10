"""Batch publishing strategies for RabbitMQ publisher.

Extracted from ``RabbitMQPublisher`` (publisher.py) to keep module sizes
strictly under the 400-line warning threshold (ADR-0002).
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from eventsource.adapters.rabbitmq.models import BatchPublishError, RabbitMQEventBusStats
from eventsource.domain.event import DomainEvent

if TYPE_CHECKING:
    from eventsource.adapters.rabbitmq.config import RabbitMQEventBusConfig
    from eventsource.adapters.rabbitmq.topology import RabbitMQTopology


class RabbitMQPublisherBatchMixin:
    """Batch publishing strategies (concurrent/sequential) for RabbitMQPublisher."""

    _config: RabbitMQEventBusConfig
    _topology: RabbitMQTopology
    _stats: RabbitMQEventBusStats
    _logger: logging.Logger

    async def _publish_single_no_stats(
        self,
        event: DomainEvent,
    ) -> None:
        """Publish a single event without updating statistics. Implemented by publisher."""
        raise NotImplementedError

    async def _publish_chunk_concurrent(
        self,
        events: list[DomainEvent],
        errors: list[Exception],
    ) -> int:
        """Publish a chunk of events concurrently. Implemented by RabbitMQPublisherManyMixin."""
        raise NotImplementedError

    async def _publish_batch_concurrent(
        self,
        events: list[DomainEvent],
    ) -> dict[str, int]:
        """Publish events concurrently with detailed result tracking.

        Implementation for concurrent publishing used by ``publish_batch()``.

        Args:
            events: Events to publish

        Returns:
            Dictionary with batch statistics
        """
        total_events = len(events)
        chunk_size = self._config.publish_chunk_size
        max_concurrent = self._config.max_concurrent_publishes

        self._logger.debug(
            f"Batch publishing {total_events} events (concurrent)",
            extra={
                "batch_size": total_events,
                "chunk_size": chunk_size,
                "max_concurrent": max_concurrent,
            },
        )

        # Track stats
        self._stats.batch_publishes += 1
        published_count = 0
        errors: list[Exception] = []
        num_chunks = 0

        # Process in chunks
        for chunk_start in range(0, total_events, chunk_size):
            chunk_end = min(chunk_start + chunk_size, total_events)
            chunk = events[chunk_start:chunk_end]
            num_chunks += 1

            chunk_published = await self._publish_chunk_concurrent(chunk, errors)
            published_count += chunk_published

            self._logger.debug(
                f"Published chunk {num_chunks} ({chunk_published}/{len(chunk)} events)",
                extra={
                    "chunk_number": num_chunks,
                    "chunk_published": chunk_published,
                    "chunk_total": len(chunk),
                    "total_published": published_count,
                },
            )

        # Update statistics
        self._stats.events_published += published_count
        self._stats.batch_events_published += published_count
        self._stats.last_publish_at = datetime.now(UTC)
        self._stats.publish_confirms += published_count

        failed_count = total_events - published_count
        if failed_count > 0:
            self._stats.batch_partial_failures += 1

        result = {
            "total": total_events,
            "published": published_count,
            "failed": failed_count,
            "chunks": num_chunks,
        }

        self._logger.info(
            f"Batch publish completed: {published_count}/{total_events} events",
            extra=result,
        )

        # Raise BatchPublishError if any failures
        if errors:
            raise BatchPublishError(
                f"Batch publish had {len(errors)} failures",
                results=result,
                errors=errors,
            )

        return result

    async def _publish_batch_sequential(
        self,
        events: list[DomainEvent],
    ) -> dict[str, int]:
        """Publish events sequentially to preserve order.

        Used when ``preserve_order=True`` in ``publish_batch()``.
        Slower than concurrent publishing but guarantees event ordering.

        Args:
            events: Events to publish in order

        Returns:
            Dictionary with batch statistics
        """
        total_events = len(events)

        self._logger.debug(
            f"Batch publishing {total_events} events (sequential/ordered)",
            extra={"batch_size": total_events},
        )

        # Track stats
        self._stats.batch_publishes += 1
        published_count = 0
        errors: list[Exception] = []

        for event in events:
            try:
                await self._publish_single_no_stats(event)
                published_count += 1
            except Exception as e:
                errors.append(e)
                self._logger.warning(
                    f"Failed to publish event in ordered batch: {e}",
                    extra={
                        "event_id": str(event.event_id),
                        "event_type": event.event_type,
                        "error": str(e),
                    },
                )

        # Update statistics
        self._stats.events_published += published_count
        self._stats.batch_events_published += published_count
        self._stats.last_publish_at = datetime.now(UTC)
        self._stats.publish_confirms += published_count

        failed_count = total_events - published_count
        if failed_count > 0:
            self._stats.batch_partial_failures += 1

        result = {
            "total": total_events,
            "published": published_count,
            "failed": failed_count,
            "chunks": 1,  # Sequential is always one "chunk"
        }

        self._logger.info(
            f"Ordered batch publish completed: {published_count}/{total_events} events",
            extra=result,
        )

        # Raise BatchPublishError if any failures
        if errors:
            raise BatchPublishError(
                f"Ordered batch publish had {len(errors)} failures",
                results=result,
                errors=errors,
            )

        return result

    async def publish_batch(
        self,
        events: list[DomainEvent],
        preserve_order: bool = False,
    ) -> dict[str, int]:
        """Publish multiple events with batch optimization.

        This method provides optimized batch publishing using concurrent
        asyncio.gather() to publish multiple events in parallel. Large
        batches are automatically chunked based on config.publish_chunk_size to
        prevent overwhelming the broker.

        Args:
            events: List of events to publish
            preserve_order: If True, publishes events sequentially to
                          maintain order guarantees. Default is False
                          (concurrent publishing).

        Returns:
            Dictionary with batch publishing statistics: total, published,
            failed, chunks.

        Raises:
            RuntimeError: If exchange not initialized
            BatchPublishError: If any events failed to publish (contains
                partial results)
        """
        exchange = self._topology.exchange
        if not exchange:
            raise RuntimeError("Exchange not initialized")

        if preserve_order:
            # Sequential publishing for order guarantees
            return await self._publish_batch_sequential(events)
        else:
            # Concurrent publishing for performance
            return await self._publish_batch_concurrent(events)


__all__ = ["RabbitMQPublisherBatchMixin"]
