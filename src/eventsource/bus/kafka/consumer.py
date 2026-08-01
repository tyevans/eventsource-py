"""Kafka consume loop: poll, dispatch, commit, retry, and DLQ routing.

Extracted from ``KafkaEventBus`` so the consume-side mechanics live in one
focused collaborator. The facade keeps ``start_consuming`` /
``stop_consuming`` / ``start_consuming_in_background`` / ``is_consuming``
with unchanged public signatures and delegates to ``KafkaConsumerLoop``.

Invariants preserved verbatim from the facade (ADR 0011 + the 0.6.0 fixes):

- Handlers are looked up by **event class** (``handlers_for(type(event))``),
  never by the ``event_type`` header string.
- Every handler runs for a delivery even if an earlier one failed; failures
  aggregate into a single :class:`HandlerDispatchError`.
- A failed delivery is never committed on the success path, so Kafka
  redelivers (at-least-once). Retry-republish and DLQ paths commit
  deliberately to avoid infinite reprocessing.
- A single-handler failure is unwrapped before DLQ error recording so
  ``dlq_error_type`` reflects the handler's own exception type.

The loop never touches the event registry: name resolution and handler
lookup arrive as callables from the facade.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from eventsource.bus.kafka.models import DeserializationError
from eventsource.bus.kafka.serialization import EventSerializer
from eventsource.domain.exceptions import HandlerDispatchError
from eventsource.events.base import DomainEvent
from eventsource.observability import OTEL_AVAILABLE, SpanKindEnum, Tracer
from eventsource.observability.attributes import (
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
    ATTR_HANDLER_NAME,
    ATTR_MESSAGING_OPERATION,
    ATTR_MESSAGING_SYSTEM,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from eventsource.bus.kafka.config import KafkaEventBusConfig
    from eventsource.bus.kafka.connection import KafkaConnectionManager
    from eventsource.bus.kafka.metrics import KafkaEventBusMetrics
    from eventsource.bus.kafka.models import KafkaEventBusStats
    from eventsource.bus.retry import RetryPolicy
    from eventsource.handlers.adapter import HandlerAdapter

# OpenTelemetry propagation imports -- kept optional, matching bus.py.
try:
    from opentelemetry.propagate import extract
    from opentelemetry.trace import Status, StatusCode

    PROPAGATION_AVAILABLE = OTEL_AVAILABLE
except ImportError:
    extract = None  # type: ignore[assignment]
    Status = None  # type: ignore[assignment, misc]
    StatusCode = None  # type: ignore[assignment, misc]
    PROPAGATION_AVAILABLE = False

# Pinned explicitly to keep the public logger name stable across the
# bus/kafka package split.
logger = logging.getLogger("eventsource.bus.kafka")


class KafkaConsumerLoop:
    """Owns the Kafka consume loop and its retry/DLQ error paths."""

    def __init__(
        self,
        config: KafkaEventBusConfig,
        connection: KafkaConnectionManager,
        serializer: EventSerializer,
        stats: KafkaEventBusStats,
        metrics: KafkaEventBusMetrics | None,
        retry_policy: RetryPolicy,
        handlers_for: Callable[[type[DomainEvent]], tuple[HandlerAdapter, ...]],
        resolve_event_class: Callable[[str], type[DomainEvent] | None],
        tracer: Tracer,
        enable_tracing: bool,
        shutdown_event: asyncio.Event,
        on_start: Callable[[], None] | None = None,
    ) -> None:
        """Initialize the consume loop.

        Args:
            config: Kafka configuration (topic names, retry limits, DLQ).
            connection: Connection manager owning the consumer/producer.
            serializer: Event serializer used for deserialization.
            stats: Shared statistics object (mutated in place).
            metrics: Shared metrics container, or None if metrics disabled.
            retry_policy: Shared backoff policy for retry delays.
            handlers_for: Class-keyed handler lookup supplied by the facade.
            resolve_event_class: Event-type-name to class resolver supplied
                by the facade (the loop never touches the registry itself).
            tracer: Tracer used to create consume/dispatch spans.
            enable_tracing: Whether tracing is enabled for this bus instance.
            shutdown_event: Shared shutdown signal owned by the facade.
            on_start: Optional hook invoked once the consume loop's guards
                pass, before polling begins. The facade uses it to register
                its consumer-lag observable gauge, preserving the original
                ordering inside ``start_consuming``.
        """
        self._config = config
        self._connection = connection
        self._serializer = serializer
        self._stats = stats
        self._metrics = metrics
        self._retry_policy = retry_policy
        self._handlers_for = handlers_for
        self._resolve_event_class = resolve_event_class
        self._tracer = tracer
        self._enable_tracing = enable_tracing
        self._shutdown_event = shutdown_event
        self._on_start = on_start

        self._consuming = False
        self._consume_task: asyncio.Task[None] | None = None

    # =========================================================================
    # Properties
    # =========================================================================

    @property
    def is_consuming(self) -> bool:
        """Check if actively consuming messages.

        Returns:
            True if consume loop is running.
        """
        return self._consuming

    @property
    def _consumer(self) -> Any:
        """The active aiokafka consumer, or None."""
        return self._connection.consumer

    @property
    def _producer(self) -> Any:
        """The active aiokafka producer, or None."""
        return self._connection.producer

    # =========================================================================
    # Lifecycle
    # =========================================================================

    async def start(self, auto_reconnect: bool = True) -> None:
        """Start consuming events from Kafka.

        This method blocks and continuously polls for messages, dispatching
        them to registered handlers. Use stop() from another coroutine to
        stop.

        Events are processed sequentially within each partition. Offsets are
        committed after successful handler execution (at-least-once delivery).

        Auto-Reconnection:
            When auto_reconnect=True (default), the consumer will automatically
            attempt to reconnect on connection errors using exponential backoff.
            This prevents the consumer from dying on transient network issues.

        Args:
            auto_reconnect: If True, automatically reconnect on errors.

        Raises:
            RuntimeError: If not connected or already consuming.
        """
        if not self._connection.is_connected or not self._consumer:
            raise RuntimeError("Not connected to Kafka. Call connect() first.")

        if self._consuming:
            logger.warning("Already consuming events")
            return

        self._consuming = True
        self._shutdown_event.clear()

        # Register consumer lag gauge (only once)
        if self._on_start is not None:
            self._on_start()

        logger.info(
            "Starting Kafka consumer",
            extra={
                "topic": self._config.topic_name,
                "consumer_group": self._config.consumer_group,
                "auto_reconnect": auto_reconnect,
            },
        )

        reconnect_delay = 1.0  # Start with 1 second delay
        max_reconnect_delay = 60.0  # Cap at 60 seconds

        while self._consuming and not self._shutdown_event.is_set():
            try:
                async for message in self._consumer:
                    if self._shutdown_event.is_set() or not self._consuming:
                        break

                    await self._process_message(message)

                    # Reset reconnect delay on successful message processing
                    reconnect_delay = 1.0

                # async for loop completed - only exit if we should stop
                if not self._consuming or self._shutdown_event.is_set():
                    break
                # Otherwise continue the while loop to keep consuming

            except asyncio.CancelledError:
                logger.info("Consumer cancelled")
                raise
            except Exception as e:
                # Increment connection_errors counter for consumer errors
                if self._metrics:
                    self._metrics.connection_errors.add(
                        1,
                        attributes={
                            "error.type": type(e).__name__,
                        },
                    )

                logger.error(
                    "Consumer error",
                    extra={"error": str(e), "auto_reconnect": auto_reconnect},
                    exc_info=True,
                )

                if not auto_reconnect or not self._consuming:
                    raise

                # Attempt reconnection with exponential backoff
                logger.info(
                    "Attempting to reconnect consumer",
                    extra={"delay_seconds": reconnect_delay},
                )

                await asyncio.sleep(reconnect_delay)

                # Exponential backoff with cap
                reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)

                # Record reconnection attempt
                self._connection.record_reconnection()

                # Try to restart the consumer
                try:
                    await self._connection.reconnect_consumer()
                except Exception as reconnect_error:
                    logger.error(
                        "Failed to reconnect consumer",
                        extra={"error": str(reconnect_error)},
                        exc_info=True,
                    )
                    # Continue the loop to retry with backoff

        self._consuming = False
        logger.info("Consumer stopped")

    async def stop(self) -> None:
        """Stop the consumer loop gracefully.

        Sets the consuming flag to False which will cause the consume loop
        to exit on its next iteration.
        """
        self._consuming = False
        logger.info("Stop consuming requested")

    def start_in_background(self) -> asyncio.Task[None]:
        """Start consuming in a background task.

        Returns:
            The background task running the consumer.

        Raises:
            RuntimeError: If consumer is already running in background.
        """
        if self._consume_task is not None and not self._consume_task.done():
            raise RuntimeError("Consumer already running in background")

        self._consume_task = asyncio.create_task(
            self.start(),
            name=f"kafka-consumer-{self._config.consumer_name}",
        )
        return self._consume_task

    # =========================================================================
    # Message Processing
    # =========================================================================

    async def _process_message(self, message: Any) -> None:
        """Process a single Kafka message with optional tracing.

        Deserializes the event, dispatches to handlers, and commits offset
        on success. Implements retry logic on failure.

        When tracing is enabled, creates OpenTelemetry spans for the consume
        operation and extracts trace context from Kafka message headers for
        distributed tracing correlation.

        Args:
            message: The Kafka ConsumerRecord to process.
        """
        self._stats.events_consumed += 1
        self._stats.last_consume_at = datetime.now(UTC)

        # Extract event type from headers for routing
        event_type_name = self._get_header_value(message.headers, "event_type")

        # Increment messages_consumed counter
        if self._metrics and event_type_name:
            self._metrics.messages_consumed.add(
                1,
                attributes={
                    "messaging.system": "kafka",
                    "messaging.destination": message.topic,
                    "messaging.kafka.partition": message.partition,
                    "event.type": event_type_name,
                },
            )

        if not event_type_name:
            logger.error(
                "Message missing event_type header",
                extra={
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset,
                },
            )
            # Commit to avoid reprocessing malformed message
            if self._consumer:
                await self._consumer.commit()
            return

        logger.debug(
            "Processing message",
            extra={
                "event_type": event_type_name,
                "partition": message.partition,
                "offset": message.offset,
            },
        )

        # Use composition-based tracer for tracing
        if self._enable_tracing and PROPAGATION_AVAILABLE and extract is not None:
            # Extract trace context from headers for distributed tracing
            carrier = self._extract_trace_context(message.headers)
            context = extract(carrier)

            # Create span for consume
            with self._tracer.span_with_kind(
                name=f"eventsource.event_bus.consume {event_type_name}",
                kind=SpanKindEnum.CONSUMER,
                attributes={
                    ATTR_MESSAGING_SYSTEM: "kafka",
                    "messaging.source": message.topic,
                    "messaging.source_kind": "topic",
                    ATTR_MESSAGING_OPERATION: "receive",
                    "messaging.kafka.partition": message.partition,
                    "messaging.kafka.offset": message.offset,
                    "messaging.kafka.consumer_group": self._config.consumer_group,
                    ATTR_EVENT_TYPE: event_type_name,
                },
                context=context,
            ) as span:
                await self._process_message_with_span(message, event_type_name, span)
        else:
            await self._process_message_with_span(message, event_type_name, None)

    def _extract_trace_context(
        self,
        headers: list[tuple[str, bytes]] | None,
    ) -> dict[str, str]:
        """Extract OpenTelemetry trace context from message headers.

        Extracts W3C Trace Context headers (traceparent, tracestate, baggage)
        from Kafka message headers for distributed tracing correlation.

        Args:
            headers: Kafka message headers.

        Returns:
            Dictionary of trace context headers suitable for OpenTelemetry
            propagator extraction.
        """
        carrier: dict[str, str] = {}
        if headers:
            for key, value in headers:
                # OpenTelemetry headers typically start with 'traceparent' or 'tracestate'
                if key in ("traceparent", "tracestate", "baggage"):
                    carrier[key] = value.decode("utf-8")
        return carrier

    async def _process_message_with_span(
        self,
        message: Any,
        event_type_name: str,
        span: Any,
    ) -> None:
        """Process message with optional span updates.

        This method contains the core message processing logic, optionally
        updating the provided tracing span with event metadata and status.

        Args:
            message: The Kafka message.
            event_type_name: The event type name.
            span: The current tracing span, or None.
        """
        # Start timing for consume duration histogram
        start_time = time.perf_counter()

        # Get retry count from headers (for retried messages)
        retry_count = self._get_retry_count(message.headers)

        try:
            # Deserialize event - catch DeserializationError separately
            # as these will never succeed on retry
            try:
                event = self._deserialize_message(message)
            except DeserializationError as e:
                # Deserialization errors are unrecoverable - send directly to DLQ
                logger.error(
                    "Deserialization error, sending directly to DLQ",
                    extra={
                        "event_type": event_type_name,
                        "error": str(e),
                    },
                )
                await self._send_to_dlq(message, e, retry_count, reason="deserialization_error")
                if self._consumer:
                    await self._consumer.commit()

                if span:
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    span.record_exception(e)

                # Record consume duration
                if self._metrics:
                    duration_ms = (time.perf_counter() - start_time) * 1000
                    self._metrics.consume_duration.record(
                        duration_ms,
                        attributes={"messaging.destination": message.topic},
                    )
                return

            if span:
                span.set_attribute(ATTR_EVENT_ID, str(event.event_id))
                span.set_attribute(ATTR_AGGREGATE_ID, str(event.aggregate_id))
                span.set_attribute(ATTR_AGGREGATE_TYPE, event.aggregate_type)

            # Get handlers for this event type, keyed by class (not by the
            # event_type header value, which may differ from the class name)
            handlers = self._handlers_for(type(event))

            if not handlers:
                logger.debug(
                    "No handlers for event type",
                    extra={"event_type": event_type_name},
                )
            else:
                # Dispatch to all handlers
                await self._dispatch_to_handlers(event, handlers)

            # Commit offset on success
            if self._consumer:
                await self._consumer.commit()
            self._stats.events_processed_success += 1

            if span:
                span.set_status(Status(StatusCode.OK))

            # Record consume duration histogram - success path
            if self._metrics:
                duration_ms = (time.perf_counter() - start_time) * 1000
                self._metrics.consume_duration.record(
                    duration_ms,
                    attributes={
                        "messaging.destination": message.topic,
                    },
                )

        except Exception as e:
            # Record consume duration histogram - error path
            if self._metrics:
                duration_ms = (time.perf_counter() - start_time) * 1000
                self._metrics.consume_duration.record(
                    duration_ms,
                    attributes={
                        "messaging.destination": message.topic,
                    },
                )

            if span:
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.record_exception(e)

            # Handle retry or DLQ routing. Unwrap a single-failure
            # HandlerDispatchError so retry/DLQ metadata (dlq_error_type,
            # etc.) still reflects the handler's own exception type rather
            # than the aggregate wrapper -- error isolation changes how
            # dispatch runs handlers, not what gets reported for a single
            # failing handler.
            dlq_error: Exception = e
            if isinstance(e, HandlerDispatchError) and len(e.failures) == 1:
                dlq_error = e.failures[0][1]
            await self._handle_processing_error(message, dlq_error, retry_count)

    def _deserialize_message(self, message: Any) -> DomainEvent:
        """Deserialize a Kafka message to a DomainEvent using the configured serializer.

        Args:
            message: The Kafka ConsumerRecord to deserialize.

        Returns:
            The deserialized DomainEvent.

        Raises:
            DeserializationError: If event type is unknown or deserialization fails.
        """
        event_type_name = self._get_header_value(message.headers, "event_type")

        if not event_type_name:
            raise DeserializationError("Message missing event_type header")

        # Get event class from registry
        event_class = self._resolve_event_class(event_type_name)

        if not event_class:
            raise DeserializationError(f"Unknown event type: {event_type_name}")

        # Deserialize using the configured serializer
        return self._serializer.deserialize(message.value, event_type_name, event_class)

    def _get_header_value(
        self,
        headers: list[tuple[str, bytes]] | None,
        key: str,
    ) -> str | None:
        """Get a header value by key.

        Args:
            headers: List of header tuples.
            key: The header key to find.

        Returns:
            The decoded header value, or None if not found.
        """
        if not headers:
            return None

        for header_key, header_value in headers:
            if header_key == key:
                return header_value.decode("utf-8")

        return None

    def _get_retry_count(self, headers: list[tuple[str, bytes]] | None) -> int:
        """Get the retry count from message headers.

        Args:
            headers: Message headers.

        Returns:
            The retry count, or 0 if not present.
        """
        value = self._get_header_value(headers, "retry_count")
        if value:
            try:
                return int(value)
            except ValueError:
                return 0
        return 0

    async def _dispatch_to_handlers(
        self,
        event: DomainEvent,
        handlers: tuple[HandlerAdapter, ...],
    ) -> None:
        """Dispatch an event to all registered handlers with optional tracing.

        Every handler runs for this delivery even if an earlier one failed
        (error isolation) -- failures are collected and raised together as a
        single HandlerDispatchError afterward, so the caller's retry/DLQ path
        still sees the delivery as failed exactly as a single raise would.

        When tracing is enabled, creates child spans for each handler
        invocation to provide detailed visibility into handler execution.

        Args:
            event: The event to dispatch.
            handlers: Tuple of HandlerAdapter instances to invoke.

        Raises:
            HandlerDispatchError: If one or more handlers raise.
        """
        failures: list[tuple[str, Exception]] = []

        for adapter in handlers:
            handler_name = adapter.name

            # Start timing for handler duration histogram
            handler_start_time = time.perf_counter()

            # Use composition-based tracer for tracing
            if self._enable_tracing:
                with self._tracer.span_with_kind(
                    name=f"eventsource.event_bus.dispatch {handler_name}",
                    kind=SpanKindEnum.INTERNAL,
                    attributes={
                        ATTR_HANDLER_NAME: handler_name,
                        ATTR_EVENT_TYPE: event.event_type,
                        ATTR_EVENT_ID: str(event.event_id),
                    },
                ) as span:
                    try:
                        logger.debug(
                            "Dispatching to handler",
                            extra={
                                "event_type": event.event_type,
                                "event_id": str(event.event_id),
                                "handler": handler_name,
                            },
                        )

                        await adapter.handle(event)

                        # Record handler duration and increment counter on success
                        if self._metrics:
                            handler_duration_ms = (time.perf_counter() - handler_start_time) * 1000
                            self._metrics.handler_duration.record(
                                handler_duration_ms,
                                attributes={
                                    "handler.name": handler_name,
                                    "event.type": event.event_type,
                                },
                            )
                            self._metrics.handler_invocations.add(
                                1,
                                attributes={
                                    "handler.name": handler_name,
                                    "event.type": event.event_type,
                                },
                            )

                        logger.debug(
                            "Handler completed",
                            extra={
                                "event_type": event.event_type,
                                "handler": handler_name,
                            },
                        )

                    except Exception as e:
                        # Record handler duration on error path
                        if self._metrics:
                            handler_duration_ms = (time.perf_counter() - handler_start_time) * 1000
                            self._metrics.handler_duration.record(
                                handler_duration_ms,
                                attributes={
                                    "handler.name": handler_name,
                                    "event.type": event.event_type,
                                },
                            )
                            self._metrics.handler_errors.add(
                                1,
                                attributes={
                                    "handler.name": handler_name,
                                    "event.type": event.event_type,
                                    "error.type": type(e).__name__,
                                },
                            )

                        if span is not None:
                            span.set_status(Status(StatusCode.ERROR, str(e)))
                            span.record_exception(e)
                        self._stats.handler_errors += 1

                        logger.error(
                            "Handler error",
                            extra={
                                "event_type": event.event_type,
                                "event_id": str(event.event_id),
                                "handler": handler_name,
                                "error": str(e),
                            },
                            exc_info=True,
                        )
                        failures.append((handler_name, e))
            else:
                try:
                    logger.debug(
                        "Dispatching to handler",
                        extra={
                            "event_type": event.event_type,
                            "event_id": str(event.event_id),
                            "handler": handler_name,
                        },
                    )

                    await adapter.handle(event)

                    # Record handler duration and increment counter on success
                    if self._metrics:
                        handler_duration_ms = (time.perf_counter() - handler_start_time) * 1000
                        self._metrics.handler_duration.record(
                            handler_duration_ms,
                            attributes={
                                "handler.name": handler_name,
                                "event.type": event.event_type,
                            },
                        )
                        self._metrics.handler_invocations.add(
                            1,
                            attributes={
                                "handler.name": handler_name,
                                "event.type": event.event_type,
                            },
                        )

                    logger.debug(
                        "Handler completed",
                        extra={
                            "event_type": event.event_type,
                            "handler": handler_name,
                        },
                    )

                except Exception as e:
                    # Record handler duration on error path
                    if self._metrics:
                        handler_duration_ms = (time.perf_counter() - handler_start_time) * 1000
                        self._metrics.handler_duration.record(
                            handler_duration_ms,
                            attributes={
                                "handler.name": handler_name,
                                "event.type": event.event_type,
                            },
                        )
                        self._metrics.handler_errors.add(
                            1,
                            attributes={
                                "handler.name": handler_name,
                                "event.type": event.event_type,
                                "error.type": type(e).__name__,
                            },
                        )

                    self._stats.handler_errors += 1

                    logger.error(
                        "Handler error",
                        extra={
                            "event_type": event.event_type,
                            "event_id": str(event.event_id),
                            "handler": handler_name,
                            "error": str(e),
                        },
                        exc_info=True,
                    )
                    failures.append((handler_name, e))

        if failures:
            raise HandlerDispatchError(failures)

    # =========================================================================
    # Error Handling: Retry and DLQ
    # =========================================================================

    async def _handle_processing_error(
        self,
        message: Any,
        error: Exception,
        retry_count: int,
    ) -> None:
        """Handle a message processing error.

        Implements non-blocking retry by republishing the message with an
        incremented retry count. After max_retries, the message is sent to DLQ.

        This approach does not block the consumer loop, allowing other messages
        to be processed while failed messages are retried. The retry delay is
        encoded in the message headers and a scheduled retry topic could be
        used for delayed processing (future enhancement).

        Args:
            message: The failed message.
            error: The exception that occurred.
            retry_count: Current retry attempt number.
        """
        self._stats.events_processed_failed += 1
        self._stats.last_error_at = datetime.now(UTC)

        event_type = self._get_header_value(message.headers, "event_type")
        event_id = self._get_header_value(message.headers, "event_id")

        if retry_count >= self._config.max_retries:
            logger.error(
                "Max retries exceeded, message will be sent to DLQ",
                extra={
                    "event_type": event_type,
                    "event_id": event_id,
                    "retry_count": retry_count,
                    "max_retries": self._config.max_retries,
                    "error": str(error),
                },
            )
            # Send to DLQ
            await self._send_to_dlq(message, error, retry_count)
            # Commit to avoid infinite loop
            if self._consumer:
                await self._consumer.commit()
            return

        # Calculate retry delay for logging (actual delay happens on next consumption)
        delay = self._calculate_retry_delay(retry_count)

        logger.warning(
            "Message processing failed, republishing for retry",
            extra={
                "event_type": event_type,
                "event_id": event_id,
                "retry_count": retry_count + 1,
                "max_retries": self._config.max_retries,
                "retry_delay": delay,
                "error": str(error),
            },
        )

        # Republish the message with incremented retry count (non-blocking retry)
        await self._republish_for_retry(message, retry_count + 1, delay)

        # Commit the original message to avoid reprocessing
        if self._consumer:
            await self._consumer.commit()

    async def _republish_for_retry(
        self,
        message: Any,
        new_retry_count: int,
        delay: float,
    ) -> None:
        """Republish a failed message for retry.

        Copies the original message with an updated retry count header
        and a scheduled retry timestamp. This enables non-blocking retries
        that don't hold up the consumer.

        Args:
            message: The original failed message.
            new_retry_count: The incremented retry count.
            delay: Suggested delay before processing (for logging/future use).
        """
        if not self._producer:
            logger.error("Cannot republish for retry: producer not connected")
            return

        # Build new headers with updated retry count and retry timestamp
        new_headers: list[tuple[str, bytes]] = []
        if message.headers:
            for key, value in message.headers:
                if key != "retry_count" and key != "retry_after":
                    new_headers.append((key, value))

        # Add retry metadata
        new_headers.append(("retry_count", str(new_retry_count).encode("utf-8")))
        retry_after = datetime.now(UTC).timestamp() + delay
        new_headers.append(("retry_after", str(retry_after).encode("utf-8")))

        try:
            await self._producer.send(
                topic=self._config.topic_name,
                key=message.key,
                value=message.value,
                headers=new_headers,
            )

            logger.debug(
                "Message republished for retry",
                extra={
                    "event_type": self._get_header_value(message.headers, "event_type"),
                    "retry_count": new_retry_count,
                    "retry_after": retry_after,
                },
            )
        except Exception as e:
            logger.error(
                "Failed to republish message for retry, sending to DLQ",
                extra={"error": str(e)},
                exc_info=True,
            )
            # If we can't republish, send to DLQ to avoid message loss
            await self._send_to_dlq(message, e, new_retry_count - 1, reason="republish_failed")

    def _calculate_retry_delay(self, retry_count: int) -> float:
        """Calculate delay for retry with exponential backoff and jitter.

        Delegates to the shared RetryPolicy. Note this changes Kafka's jitter
        from one-sided positive to symmetric, so effective backoff is slightly
        shorter and no longer exceeds retry_max_delay.

        Args:
            retry_count: The current retry attempt (0-based).

        Returns:
            Delay in seconds before next retry.
        """
        return self._retry_policy.delay_for(retry_count)

    async def _send_to_dlq(
        self,
        message: Any,
        error: Exception,
        retry_count: int,
        reason: str = "max_retries_exceeded",
    ) -> None:
        """Send a failed message to the dead letter queue.

        Preserves the original message and adds DLQ-specific metadata
        in headers for debugging and replay.

        Args:
            message: The failed Kafka message.
            error: The exception that caused the failure.
            retry_count: Number of retry attempts made.
            reason: Reason for DLQ routing. Options:
                - "max_retries_exceeded": Handler failed after max retries
                - "deserialization_error": Message could not be deserialized
                - "handler_error": Handler raised an unrecoverable error
        """
        if not self._config.enable_dlq:
            logger.warning(
                "DLQ disabled, dropping failed message",
                extra={
                    "event_type": self._get_header_value(message.headers, "event_type"),
                    "error": str(error),
                },
            )
            return

        if not self._producer:
            logger.error(
                "Cannot send to DLQ: producer not connected",
                extra={
                    "event_type": self._get_header_value(message.headers, "event_type"),
                },
            )
            return

        # Create DLQ headers with failure metadata
        dlq_headers = self._create_dlq_headers(message, error, retry_count, reason)

        # Combine original headers with DLQ headers
        original_headers = list(message.headers) if message.headers else []
        all_headers = original_headers + dlq_headers

        try:
            # Send to DLQ topic
            await self._producer.send(
                topic=self._config.dlq_topic_name,
                key=message.key,
                value=message.value,
                headers=all_headers,
            )

            self._stats.messages_sent_to_dlq += 1

            # Increment dlq_messages counter
            if self._metrics:
                self._metrics.dlq_messages.add(
                    1,
                    attributes={
                        "dlq.reason": reason,
                        "error.type": type(error).__name__,
                    },
                )

            logger.info(
                "Message sent to DLQ",
                extra={
                    "dlq_topic": self._config.dlq_topic_name,
                    "event_type": self._get_header_value(message.headers, "event_type"),
                    "event_id": self._get_header_value(message.headers, "event_id"),
                    "reason": reason,
                    "error": str(error)[:200],
                },
            )

        except Exception as e:
            logger.error(
                "Failed to send message to DLQ",
                extra={
                    "error": str(e),
                    "original_error": str(error),
                },
                exc_info=True,
            )
            raise

    def _create_dlq_headers(
        self,
        message: Any,
        error: Exception,
        retry_count: int,
        reason: str,
    ) -> list[tuple[str, bytes]]:
        """Create DLQ-specific headers.

        Args:
            message: The failed message.
            error: The exception that caused the failure.
            retry_count: Number of retry attempts.
            reason: Reason for DLQ routing.

        Returns:
            List of DLQ header tuples.
        """
        error_message = str(error)[:1000]  # Truncate to avoid huge headers

        return [
            ("dlq_reason", reason.encode("utf-8")),
            ("dlq_error_type", type(error).__name__.encode("utf-8")),
            ("dlq_error_message", error_message.encode("utf-8")),
            ("dlq_retry_count", str(retry_count).encode("utf-8")),
            ("dlq_timestamp", datetime.now(UTC).isoformat().encode("utf-8")),
            ("dlq_original_topic", message.topic.encode("utf-8")),
            ("dlq_original_partition", str(message.partition).encode("utf-8")),
            ("dlq_original_offset", str(message.offset).encode("utf-8")),
            ("dlq_consumer_group", self._config.consumer_group.encode("utf-8")),
        ]
