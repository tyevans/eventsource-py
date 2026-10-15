"""Batch publishing helper (publish_many) for RabbitMQ publisher."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

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

Status: Any = None
StatusCode: Any = None
try:
    from opentelemetry.trace import Status as _Status
    from opentelemetry.trace import StatusCode as _StatusCode

    Status, StatusCode = _Status, _StatusCode
    PROPAGATION_AVAILABLE = OTEL_AVAILABLE
except ImportError:  # pragma: no cover - guarded by RabbitMQEventBus construction
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

    async def _publish_single_no_stats(self, event: DomainEvent) -> None:
        raise NotImplementedError

    async def publish_many(self, events: list[DomainEvent], wait_for_confirm: bool = True) -> None:
        """Publish multiple events concurrently in chunks with optional batch span tracing."""
        if not self._topology.exchange:
            raise RuntimeError("Exchange not initialized")

        total_events, chunk_size = len(events), self._config.publish_chunk_size
        max_concurrent = self._config.max_concurrent_publishes
        self._logger.debug(
            f"Publishing batch of {total_events} events",
            extra={
                "batch_size": total_events,
                "chunk_size": chunk_size,
                "max_concurrent": max_concurrent,
            },  # fmt: skip
        )

        span = None
        if self._enable_tracing and PROPAGATION_AVAILABLE and self._tracer is not None:
            attrs = {
                ATTR_MESSAGING_SYSTEM: "rabbitmq",
                ATTR_MESSAGING_DESTINATION: self._config.exchange_name,
                ATTR_EVENT_COUNT: total_events,
                "messaging.destination_kind": "exchange",
                "messaging.batch.size": total_events,
                "messaging.batch.chunk_size": chunk_size,
                "messaging.batch.max_concurrent": max_concurrent,
            }
            span = self._tracer.start_span("eventsource.event_bus.publish_batch", kind=SpanKindEnum.PRODUCER, attributes=attrs)  # fmt: skip

        try:
            self._stats.batch_publishes += 1
            published_count = 0
            errors: list[Exception] = []
            for chunk_start in range(0, total_events, chunk_size):
                chunk = events[chunk_start : min(chunk_start + chunk_size, total_events)]
                published_count += await self._publish_chunk_concurrent(chunk, errors)

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
                    },  # fmt: skip
                )
                if span:
                    span.set_attribute("messaging.batch.published", published_count)
                    span.set_attribute("messaging.batch.failed", len(errors))
                    span.set_status(Status(StatusCode.ERROR, f"{len(errors)} events failed"))
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
                if not errors:
                    span.set_status(Status(StatusCode.ERROR, str(e)))
            raise
        finally:
            if span:
                span.end()

    async def _publish_chunk_concurrent(self, events: list[DomainEvent], errors: list[Exception]) -> int:  # fmt: skip
        async def publish_with_semaphore(event: DomainEvent) -> bool:
            async with self._publish_semaphore:
                try:
                    await self._publish_single_no_stats(event)
                    return True
                except Exception as e:
                    errors.append(e)
                    extra = {"event_id": str(event.event_id), "event_type": event.event_type, "error": str(e)}  # fmt: skip
                    self._logger.warning(f"Failed to publish event in batch: {e}", extra=extra)
                    return False

        results = await asyncio.gather(*[publish_with_semaphore(event) for event in events], return_exceptions=False)  # fmt: skip
        return sum(1 for result in results if result)


__all__ = ["RabbitMQPublisherManyMixin"]
