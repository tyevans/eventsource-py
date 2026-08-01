"""Split-phase Kafka publish (send handoff, then batched ack-await).

Extracted from ``KafkaEventBus`` so the send/ack mechanics -- including the
0.6.0 fixes for per-aggregate ordering and plain-``asyncio.Future`` handling
around ``producer.send()`` -- live in one focused collaborator. The facade's
``publish()`` remains the public orchestrator (guard rails, timing, and
aggregate statistics); it delegates the actual event handoff to
``KafkaPublisher.publish_all()``.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from eventsource.adapters.kafka.serialization import EventSerializer
from eventsource.domain.event import DomainEvent
from eventsource.observability import OTEL_AVAILABLE, SpanKindEnum, Tracer
from eventsource.observability.attributes import (
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
    ATTR_MESSAGING_DESTINATION,
    ATTR_MESSAGING_OPERATION,
    ATTR_MESSAGING_SYSTEM,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from eventsource.adapters.kafka.config import KafkaEventBusConfig
    from eventsource.adapters.kafka.connection import KafkaConnectionManager
    from eventsource.adapters.kafka.metrics import KafkaEventBusMetrics
    from eventsource.adapters.kafka.models import KafkaEventBusStats

# OpenTelemetry propagation imports -- kept optional, matching bus.py.
try:
    from opentelemetry.propagate import inject
    from opentelemetry.trace import Status, StatusCode

    PROPAGATION_AVAILABLE = OTEL_AVAILABLE
except ImportError:
    inject = None  # type: ignore[assignment]
    Status = None  # type: ignore[assignment, misc]
    StatusCode = None  # type: ignore[assignment, misc]
    PROPAGATION_AVAILABLE = False

# Pinned explicitly to keep the public logger name stable across the
# bus/kafka package split.
logger = logging.getLogger("eventsource.bus.kafka")


class KafkaPublisher:
    """Owns the send/ack mechanics for publishing events to Kafka.

    Publishing is split into two phases: every event is handed to
    ``producer.send()`` sequentially (never gathered concurrently, since
    concurrent sends could reorder same-partition-key messages across an
    await boundary), then acknowledgments are awaited -- or, for background
    publishes, tracked via a done-callback on the returned future.
    """

    def __init__(
        self,
        config: KafkaEventBusConfig,
        connection: KafkaConnectionManager,
        serializer: EventSerializer,
        stats: KafkaEventBusStats,
        metrics: KafkaEventBusMetrics | None,
        tracer: Tracer,
        enable_tracing: bool,
    ) -> None:
        """Initialize the publisher.

        Args:
            config: Kafka configuration (topic name, etc).
            connection: Connection manager owning the active producer.
            serializer: Event serializer.
            stats: Shared statistics object (mutated in place on errors).
            metrics: Shared metrics container, or None if metrics disabled.
            tracer: Tracer used to create publish spans.
            enable_tracing: Whether tracing is enabled for this bus instance.
        """
        self._config = config
        self._connection = connection
        self._serializer = serializer
        self._stats = stats
        self._metrics = metrics
        self._tracer = tracer
        self._enable_tracing = enable_tracing

    async def publish_all(
        self,
        events: Sequence[DomainEvent],
        background: bool,
    ) -> None:
        """Hand every event to the producer, then resolve delivery.

        Sends are issued in a plain sequential loop (not gathered) so that
        same-partition-key ordering is preserved. For durable publishes, acks
        are then awaited together via ``asyncio.gather``. For background
        publishes, each future is tracked via a done-callback instead of
        being awaited.

        Args:
            events: The domain events to publish, in order.
            background: If True, track delivery via callback instead of
                awaiting acknowledgment.
        """
        pending = []
        for event in events:
            pending.append(await self._begin_publish_single_event(event, background))

        if background:
            for future, published_event, span in pending:
                # For background publishes, add a callback to track delivery
                # asynchronously. The span closes immediately after handoff,
                # matching non-batched background semantics.
                self._track_background_publish(future, published_event, span)
                if span:
                    span.end()
        else:
            await asyncio.gather(*(self._await_publish_ack(handle) for handle in pending))

    async def _begin_publish_single_event(
        self,
        event: DomainEvent,
        background: bool,
    ) -> tuple[Any, DomainEvent, Any]:
        """Serialize and hand a single event to the producer.

        Creates an OpenTelemetry span for the publish operation if tracing
        is enabled. The span is kept open (started via ``start_span``, not
        the context-manager form) so it can be closed later once the
        acknowledgment has been awaited -- this lets ``publish_all`` batch
        the ack-await across events while still tracing the full operation.

        Args:
            event: The event to publish.
            background: Whether the caller intends to skip waiting for
                acknowledgment (used to decide how the returned future is
                handled by the caller).

        Returns:
            A ``(future, event, span)`` tuple. ``span`` is the tracing span
            (or None) that must be ended by the caller after awaiting the
            future (or immediately, for background publishes once tracked).
        """
        producer = self._connection.require_producer()

        # Serialize event
        key = self._get_partition_key(event)
        value = self._serialize_event(event)
        headers = self._create_headers(event)

        span: Any = None
        if self._enable_tracing:
            span = self._tracer.start_span(
                name=f"eventsource.event_bus.publish {event.event_type}",
                kind=SpanKindEnum.PRODUCER,
                attributes={
                    ATTR_MESSAGING_SYSTEM: "kafka",
                    ATTR_MESSAGING_DESTINATION: self._config.topic_name,
                    "messaging.destination_kind": "topic",
                    ATTR_MESSAGING_OPERATION: "publish",
                    ATTR_EVENT_TYPE: event.event_type,
                    ATTR_EVENT_ID: str(event.event_id),
                    ATTR_AGGREGATE_ID: str(event.aggregate_id),
                    ATTR_AGGREGATE_TYPE: event.aggregate_type,
                },
            )

            # Inject trace context into headers for distributed tracing
            if PROPAGATION_AVAILABLE and inject is not None:
                carrier: dict[str, str] = {}
                inject(carrier)
                for trace_key, trace_value in carrier.items():
                    headers.append((trace_key, trace_value.encode("utf-8")))

        try:
            future = await producer.send(
                topic=self._config.topic_name,
                key=key,
                value=value,
                headers=headers,
            )
        except Exception as e:
            if span:
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.record_exception(e)
                span.end()

            # Increment publish_errors counter
            if self._metrics:
                self._metrics.publish_errors.add(
                    1,
                    attributes={
                        "messaging.system": "kafka",
                        "messaging.destination": self._config.topic_name,
                        "event.type": event.event_type,
                        "error.type": type(e).__name__,
                    },
                )

            logger.error(
                "Failed to publish event",
                extra={"error": str(e)},
                exc_info=True,
            )
            self._stats.last_error_at = datetime.now(UTC)
            raise

        return future, event, span

    async def _await_publish_ack(self, handle: tuple[Any, DomainEvent, Any]) -> None:
        """Await broker acknowledgment for a single previously-sent event.

        Args:
            handle: The ``(future, event, span)`` tuple returned by
                ``_begin_publish_single_event``.
        """
        future, event, span = handle

        try:
            record_metadata = await future

            if span:
                if record_metadata is not None:
                    span.set_attribute("messaging.kafka.partition", record_metadata.partition)
                    span.set_attribute("messaging.kafka.offset", record_metadata.offset)
                span.set_status(Status(StatusCode.OK))

            if record_metadata is not None:
                logger.debug(
                    "Event published",
                    extra={
                        "topic": record_metadata.topic,
                        "partition": record_metadata.partition,
                        "offset": record_metadata.offset,
                    },
                )

            # Increment messages_published counter on confirmed success
            if self._metrics:
                self._metrics.messages_published.add(
                    1,
                    attributes={
                        "messaging.system": "kafka",
                        "messaging.destination": self._config.topic_name,
                        "event.type": event.event_type,
                    },
                )

        except Exception as e:
            if span:
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.record_exception(e)

            # Increment publish_errors counter
            if self._metrics:
                self._metrics.publish_errors.add(
                    1,
                    attributes={
                        "messaging.system": "kafka",
                        "messaging.destination": self._config.topic_name,
                        "event.type": event.event_type,
                        "error.type": type(e).__name__,
                    },
                )

            logger.error(
                "Failed to publish event",
                extra={"error": str(e)},
                exc_info=True,
            )
            self._stats.last_error_at = datetime.now(UTC)
            raise
        finally:
            if span:
                span.end()

    def _track_background_publish(
        self,
        future: Any,
        event: DomainEvent,
        span: Any,
    ) -> None:
        """Track a background publish using a callback.

        Adds a callback to the future to track delivery success/failure
        for background publishes. This ensures that errors are not silently
        lost and metrics are updated correctly.

        Args:
            future: The future from producer.send()
            event: The domain event being published (for metrics).
            span: The current tracing span, or None.
        """
        event_type = event.event_type
        event_id = str(event.event_id)
        topic = self._config.topic_name

        def on_send_success(record_metadata: Any) -> None:
            """Callback for successful background publish."""
            logger.debug(
                "Background event published successfully",
                extra={
                    "topic": record_metadata.topic,
                    "partition": record_metadata.partition,
                    "offset": record_metadata.offset,
                    "event_type": event_type,
                    "event_id": event_id,
                },
            )

            # Increment messages_published counter on confirmed success
            if self._metrics:
                self._metrics.messages_published.add(
                    1,
                    attributes={
                        "messaging.system": "kafka",
                        "messaging.destination": topic,
                        "event.type": event_type,
                    },
                )

        def on_send_error(exc: Exception) -> None:
            """Callback for failed background publish."""
            logger.error(
                "Background event publish failed",
                extra={
                    "error": str(exc),
                    "event_type": event_type,
                    "event_id": event_id,
                },
                exc_info=False,  # Exception is passed, don't need traceback
            )

            self._stats.last_error_at = datetime.now(UTC)

            # Increment publish_errors counter
            if self._metrics:
                self._metrics.publish_errors.add(
                    1,
                    attributes={
                        "messaging.system": "kafka",
                        "messaging.destination": topic,
                        "event.type": event_type,
                        "error.type": type(exc).__name__,
                    },
                )

            if span:
                span.set_status(Status(StatusCode.ERROR, str(exc)))
                span.record_exception(exc)

        # Track the result. producer.send() returns a plain asyncio.Future,
        # which only supports add_done_callback (no separate success/error
        # callback registration), so dispatch to the right handler ourselves.
        def on_done(fut: asyncio.Future[Any]) -> None:
            if fut.cancelled():
                return
            exc = fut.exception()
            if isinstance(exc, Exception):
                on_send_error(exc)
            else:
                on_send_success(fut.result())

        future.add_done_callback(on_done)

    def _get_partition_key(self, event: DomainEvent) -> bytes:
        """Get the partition key for an event.

        Uses aggregate_id to ensure events for the same aggregate
        are sent to the same partition, preserving order.

        Args:
            event: The domain event.

        Returns:
            Partition key as bytes.
        """
        return str(event.aggregate_id).encode("utf-8")

    def _serialize_event(self, event: DomainEvent) -> bytes:
        """Serialize an event to bytes using the configured serializer.

        Args:
            event: The domain event to serialize.

        Returns:
            Serialized event as bytes (format depends on serializer).
        """
        return self._serializer.serialize(event)

    def _create_headers(self, event: DomainEvent) -> list[tuple[str, bytes]]:
        """Create Kafka headers from event metadata.

        Headers enable routing and filtering without deserializing the
        message body.

        Args:
            event: The domain event.

        Returns:
            List of header tuples (name, value).
        """
        headers: list[tuple[str, bytes]] = [
            ("event_id", str(event.event_id).encode("utf-8")),
            ("event_type", event.event_type.encode("utf-8")),
            ("aggregate_id", str(event.aggregate_id).encode("utf-8")),
            ("aggregate_type", event.aggregate_type.encode("utf-8")),
            ("aggregate_version", str(event.aggregate_version).encode("utf-8")),
            ("occurred_at", event.occurred_at.isoformat().encode("utf-8")),
            ("correlation_id", str(event.correlation_id).encode("utf-8")),
        ]

        # Optional headers
        if event.tenant_id:
            headers.append(("tenant_id", str(event.tenant_id).encode("utf-8")))
        if event.causation_id:
            headers.append(("causation_id", str(event.causation_id).encode("utf-8")))
        if event.actor_id:
            headers.append(("actor_id", str(event.actor_id).encode("utf-8")))

        return headers
