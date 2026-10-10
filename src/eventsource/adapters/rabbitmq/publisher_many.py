"""Batch publishing helper (publish_many) for RabbitMQ publisher.

Extracted from ``RabbitMQPublisher`` (publisher.py) to keep module sizes
strictly under the 400-line warning threshold (ADR-0002).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from eventsource.domain.event import DomainEvent
from eventsource.observability import OTEL_AVAILABLE, SpanKindEnum, Tracer
from eventsource.observability.attributes import (
    ATTR_EVENT_COUNT,
    ATTR_MESSAGING_DESTINATION,
    ATTR_MESSAGING_SYSTEM,
)

if TYPE_CHECKING:
    from eventsource.adapters.rabbitmq.config import RabbitMQEventBusConfig
    from eventsource.adapters.rabbitmq.models import RabbitMQEventBusStats
    from eventsource.adapters.rabbitmq.topology import RabbitMQTopology

try:
    from opentelemetry.trace import Status, StatusCode

    PROPAGATION_AVAILABLE = OTEL_AVAILABLE
except ImportError:  # pragma: no cover - guarded by RabbitMQEventBus construction
    Status = None  # type: ignore[assignment, misc]
    StatusCode = None  # type: ignore[assignment, misc]
    PROPAGATION_AVAILABLE = False


class RabbitMQPublisherManyMixin:
    """Provides publish_many and concurrent chunk publishing for RabbitMQPublisher."""

    _config: RabbitMQEventBusConfig
    _topology: RabbitMQTopology
    _stats: RabbitMQEventBusStats
    _tracer: Tracer | None
    _enable_tracing: bool
    _logger: logging.Logger
    _publish_semaphore: asyncio.Semaphore

    async def _publish_single_no_stats(
        self,
        event: DomainEvent,
    ) -> None:
        """Publish a single event without updating statistics. Implemented by publisher."""
        raise NotImplementedError

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
        chunk_size = self._config.publish_chunk_size
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

                # Within each chunk, limit concurrency via the shared semaphore
                chunk_published = await self._publish_chunk_concurrent(chunk, errors)
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
        errors: list[Exception],
    ) -> int:
        """Publish a chunk of events concurrently with concurrency limit.

        Uses the publisher's shared ``self._publish_semaphore`` to limit the
        number of concurrent publish operations, preventing resource
        exhaustion. That semaphore is constructed once per publisher
        instance (not per chunk/call), so ``max_concurrent_publishes`` is a
        true ceiling even across two concurrent ``publish_many()`` calls.

        Args:
            events: Events in this chunk to publish
            errors: List to append any errors to

        Returns:
            Number of successfully published events in this chunk
        """

        async def publish_with_semaphore(event: DomainEvent) -> bool:
            """Publish a single event with semaphore control."""
            async with self._publish_semaphore:
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


__all__ = ["RabbitMQPublisherManyMixin"]
