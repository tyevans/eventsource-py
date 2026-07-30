"""RabbitMQ publish-path implementation.

Extracted from ``RabbitMQEventBus`` (bus.py) as part of the bus god-class
decomposition (Task 6). Owns single-event publishing, statistics-free
single-event publishing (used internally by batch strategies), and the
sequential/concurrent batch publish strategies.

The facade (``RabbitMQEventBus``) still owns the public ``publish()`` /
``publish_batch()`` signatures and the auto-connect + "is exchange
initialized" checks; this collaborator only needs a live exchange,
obtained via the topology, and a channel via the connection manager if
ever required.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from eventsource.bus.rabbitmq import serialization
from eventsource.bus.rabbitmq.config import RabbitMQEventBusConfig
from eventsource.bus.rabbitmq.models import BatchPublishError, RabbitMQEventBusStats
from eventsource.events.base import DomainEvent
from eventsource.observability import OTEL_AVAILABLE, SpanKindEnum, Tracer
from eventsource.observability.attributes import (
    ATTR_AGGREGATE_ID,
    ATTR_EVENT_COUNT,
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
    ATTR_MESSAGING_DESTINATION,
    ATTR_MESSAGING_SYSTEM,
)

if TYPE_CHECKING:
    from eventsource.bus.rabbitmq.connection import RabbitMQConnectionManager
    from eventsource.bus.rabbitmq.topology import RabbitMQTopology

# OpenTelemetry propagation imports -- kept separate for distributed tracing
# context, mirroring the guard in bus.py (these are NOT part of Tracer and
# must be imported directly for span status/exception recording).
try:
    from opentelemetry.trace import Status, StatusCode

    PROPAGATION_AVAILABLE = OTEL_AVAILABLE
except ImportError:  # pragma: no cover - guarded by RabbitMQEventBus construction
    Status = None  # type: ignore[assignment, misc]
    StatusCode = None  # type: ignore[assignment, misc]
    PROPAGATION_AVAILABLE = False

# Named explicitly (not via __name__) so the logger name is stable and
# matches the facade's pre-extraction "eventsource.bus.rabbitmq" logger --
# callers that configure logging by name keep working unchanged.
logger = logging.getLogger("eventsource.bus.rabbitmq")


class RabbitMQPublisher:
    """Publishes domain events to the RabbitMQ main exchange.

    Owns the single-event publish path (with optional tracing), the
    stats-free single-event publish used internally by batch strategies,
    and the sequential/concurrent batch publishing strategies.
    """

    def __init__(
        self,
        config: RabbitMQEventBusConfig,
        connection: RabbitMQConnectionManager,
        topology: RabbitMQTopology,
        stats: RabbitMQEventBusStats,
        tracer: Tracer | None,
        enable_tracing: bool,
    ) -> None:
        self._config = config
        self._connection = connection
        self._topology = topology
        self._stats = stats
        self._tracer = tracer
        self._enable_tracing = enable_tracing

        self._logger = logging.getLogger("eventsource.bus.rabbitmq")

    async def publish_one(
        self,
        event: DomainEvent,
        wait_for_confirm: bool = True,
    ) -> None:
        """Publish a single event to the exchange with optional tracing.

        Creates an OpenTelemetry span for the publish operation if tracing
        is enabled. The span includes messaging semantic attributes and
        event metadata for distributed tracing correlation.

        Args:
            event: The event to publish
            wait_for_confirm: Whether to wait for publisher confirm.
                            aio-pika handles confirms automatically with RobustConnection,
                            so publish() returns after broker acknowledges receipt.

        Raises:
            RuntimeError: If exchange not initialized
            Exception: If publishing fails
        """
        exchange = self._topology.exchange
        if not exchange:
            raise RuntimeError("Exchange not initialized")

        routing_key = serialization.get_routing_key(event)
        span = None

        # Use Tracer's start_span with SpanKindEnum.PRODUCER for distributed tracing
        # This is needed for context propagation (inject trace context into message)
        if self._enable_tracing and PROPAGATION_AVAILABLE and self._tracer is not None:
            span = self._tracer.start_span(
                "eventsource.event_bus.publish",
                kind=SpanKindEnum.PRODUCER,
                attributes={
                    ATTR_MESSAGING_SYSTEM: "rabbitmq",
                    ATTR_MESSAGING_DESTINATION: self._config.exchange_name,
                    "messaging.destination_kind": "exchange",
                    "messaging.rabbitmq.routing_key": routing_key,
                    ATTR_EVENT_TYPE: event.event_type,
                    ATTR_EVENT_ID: str(event.event_id),
                    "aggregate.type": event.aggregate_type,
                    ATTR_AGGREGATE_ID: str(event.aggregate_id),
                },
            )

        try:
            # Create AMQP message from event with optional trace context injection
            message = serialization.create_message_with_tracing(event, span)

            # Publish to exchange
            # aio-pika's RobustConnection handles publisher confirms automatically
            # The publish() call returns after the broker acknowledges receipt
            await exchange.publish(
                message,
                routing_key=routing_key,
            )

            # Update statistics
            self._stats.events_published += 1
            self._stats.last_publish_at = datetime.now(UTC)
            if wait_for_confirm:
                self._stats.publish_confirms += 1

            if span:
                span.set_status(Status(StatusCode.OK))

            self._logger.debug(
                f"Published {event.event_type}",
                extra={
                    "event_id": str(event.event_id),
                    "event_type": event.event_type,
                    "aggregate_type": event.aggregate_type,
                    "aggregate_id": str(event.aggregate_id),
                    "routing_key": routing_key,
                    "wait_for_confirm": wait_for_confirm,
                },
            )

        except Exception as e:
            if span:
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.record_exception(e)

            self._logger.error(
                f"Failed to publish {event.event_type}: {e}",
                exc_info=True,
                extra={
                    "event_id": str(event.event_id),
                    "event_type": event.event_type,
                    "aggregate_type": event.aggregate_type,
                    "aggregate_id": str(event.aggregate_id),
                    "routing_key": routing_key,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
            )
            raise

        finally:
            if span:
                span.end()

    async def _publish_single_no_stats(
        self,
        event: DomainEvent,
    ) -> None:
        """Publish a single event without updating statistics.

        This is an internal method used by batch publishing to avoid
        double-counting statistics. The batch method updates stats
        for all events at once after the batch completes.

        Args:
            event: The event to publish

        Raises:
            RuntimeError: If exchange not initialized
            Exception: If publishing fails
        """
        exchange = self._topology.exchange
        if not exchange:
            raise RuntimeError("Exchange not initialized")

        routing_key = serialization.get_routing_key(event)
        message = serialization.create_message(event)

        await exchange.publish(
            message,
            routing_key=routing_key,
        )

    async def publish_many(
        self,
        events: list[DomainEvent],
        wait_for_confirm: bool = True,
    ) -> None:
        """Publish multiple events with batch optimization.

        Internal method used by the facade's ``publish()`` for multiple
        events. Uses asyncio.gather for concurrent publishing with chunking
        to prevent overwhelming the broker.

        Creates a parent span for the batch operation when tracing is enabled,
        providing observability into batch publish performance.

        Args:
            events: Events to publish
            wait_for_confirm: Whether to wait for confirms

        Raises:
            RuntimeError: If exchange not initialized
            Exception: Re-raises first error encountered in batch
        """
        exchange = self._topology.exchange
        if not exchange:
            raise RuntimeError("Exchange not initialized")

        total_events = len(events)
        chunk_size = self._config.batch_size
        max_concurrent = self._config.max_concurrent_publishes

        self._logger.debug(
            f"Publishing batch of {total_events} events",
            extra={
                "batch_size": total_events,
                "chunk_size": chunk_size,
                "max_concurrent": max_concurrent,
            },
        )

        # Create parent span for batch operation if tracing is enabled
        span = None
        if self._enable_tracing and PROPAGATION_AVAILABLE and self._tracer is not None:
            span = self._tracer.start_span(
                "eventsource.event_bus.publish_batch",
                kind=SpanKindEnum.PRODUCER,
                attributes={
                    ATTR_MESSAGING_SYSTEM: "rabbitmq",
                    ATTR_MESSAGING_DESTINATION: self._config.exchange_name,
                    ATTR_EVENT_COUNT: total_events,
                    "messaging.destination_kind": "exchange",
                    "messaging.batch.size": total_events,
                    "messaging.batch.chunk_size": chunk_size,
                    "messaging.batch.max_concurrent": max_concurrent,
                },
            )

        try:
            # Track batch stats
            self._stats.batch_publishes += 1
            published_count = 0
            errors: list[Exception] = []

            # Process in chunks to prevent overwhelming the broker
            for chunk_start in range(0, total_events, chunk_size):
                chunk_end = min(chunk_start + chunk_size, total_events)
                chunk = events[chunk_start:chunk_end]

                # Within each chunk, limit concurrency
                chunk_published = await self._publish_chunk_concurrent(
                    chunk, max_concurrent, errors
                )
                published_count += chunk_published

            # Update statistics
            self._stats.events_published += published_count
            self._stats.batch_events_published += published_count
            self._stats.last_publish_at = datetime.now(UTC)

            if wait_for_confirm:
                self._stats.publish_confirms += published_count

            if errors:
                self._stats.batch_partial_failures += 1
                self._logger.error(
                    f"Batch publish had {len(errors)} failures out of {total_events} events",
                    extra={
                        "failures": len(errors),
                        "published": published_count,
                        "total": total_events,
                    },
                )
                if span:
                    span.set_attribute("messaging.batch.published", published_count)
                    span.set_attribute("messaging.batch.failed", len(errors))
                    span.set_status(Status(StatusCode.ERROR, f"{len(errors)} events failed"))
                # Raise the first error to indicate batch failure
                raise errors[0]

            self._logger.debug(
                f"Successfully published batch of {total_events} events",
                extra={"batch_size": total_events, "published": published_count},
            )

            if span:
                span.set_attribute("messaging.batch.published", published_count)
                span.set_status(Status(StatusCode.OK))

        except Exception as e:
            if span:
                span.record_exception(e)
                if not errors:  # Only set error status if not already set above
                    span.set_status(Status(StatusCode.ERROR, str(e)))
            raise

        finally:
            if span:
                span.end()

    async def _publish_chunk_concurrent(
        self,
        events: list[DomainEvent],
        max_concurrent: int,
        errors: list[Exception],
    ) -> int:
        """Publish a chunk of events concurrently with concurrency limit.

        Uses asyncio.Semaphore to limit the number of concurrent publish
        operations, preventing resource exhaustion.

        Args:
            events: Events in this chunk to publish
            max_concurrent: Maximum concurrent publish operations
            errors: List to append any errors to

        Returns:
            Number of successfully published events in this chunk
        """
        semaphore = asyncio.Semaphore(max_concurrent)

        async def publish_with_semaphore(event: DomainEvent) -> bool:
            """Publish a single event with semaphore control."""
            async with semaphore:
                try:
                    await self._publish_single_no_stats(event)
                    return True
                except Exception as e:
                    errors.append(e)
                    self._logger.warning(
                        f"Failed to publish event in batch: {e}",
                        extra={
                            "event_id": str(event.event_id),
                            "event_type": event.event_type,
                            "error": str(e),
                        },
                    )
                    return False

        # Execute all publishes concurrently (up to semaphore limit)
        results = await asyncio.gather(
            *[publish_with_semaphore(event) for event in events],
            return_exceptions=False,  # Exceptions are caught in publish_with_semaphore
        )

        # Count successful publishes
        return sum(1 for result in results if result)

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
        chunk_size = self._config.batch_size
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

            chunk_published = await self._publish_chunk_concurrent(chunk, max_concurrent, errors)
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
        batches are automatically chunked based on config.batch_size to
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


__all__ = ["RabbitMQPublisher"]
