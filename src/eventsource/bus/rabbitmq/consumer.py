"""Consume loop, handler dispatch, and retry/DLQ write path for the RabbitMQ bus.

``RabbitMQConsumer`` owns everything on the delivery path: the queue iterator
loop, per-message processing (deserialize -> dispatch -> ack), the error path
(retry republish / DLQ routing), and graceful stop/drain.

Error isolation (ADR 0011) lives here: :meth:`_dispatch_event` runs *every*
handler for a delivery, collects failures, and raises a single
:class:`HandlerDispatchError`; :meth:`_process_message` treats that as a
processing failure and routes to retry/DLQ.

The consumer never touches the bus's subscription registry -- it is handed
``handlers_for`` and ``resolve_event_class`` callables instead.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable, Coroutine
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from eventsource.bus.rabbitmq import death_headers, serialization
from eventsource.events.base import DomainEvent
from eventsource.exceptions import HandlerDispatchError
from eventsource.observability import OTEL_AVAILABLE, SpanKindEnum, Tracer
from eventsource.observability.attributes import (
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
    ATTR_HANDLER_COUNT,
    ATTR_HANDLER_NAME,
    ATTR_HANDLER_SUCCESS,
    ATTR_MESSAGING_DESTINATION,
    ATTR_MESSAGING_SYSTEM,
)

if TYPE_CHECKING:
    from eventsource.bus.rabbitmq.config import RabbitMQEventBusConfig
    from eventsource.bus.rabbitmq.connection import RabbitMQConnectionManager
    from eventsource.bus.rabbitmq.models import RabbitMQEventBusStats
    from eventsource.bus.rabbitmq.topology import RabbitMQTopology
    from eventsource.bus.retry import RetryPolicy
    from eventsource.handlers.adapter import HandlerAdapter

# Optional aio-pika import - fail gracefully if not installed.
try:
    from aio_pika import DeliveryMode, Message
    from aio_pika.abc import AbstractIncomingMessage

    RABBITMQ_AVAILABLE = True
except ImportError:
    RABBITMQ_AVAILABLE = False
    Message = None  # type: ignore[assignment, misc]
    DeliveryMode = None  # type: ignore[assignment, misc]
    AbstractIncomingMessage = None  # type: ignore[assignment, misc]

# OpenTelemetry propagation import - kept separate for distributed tracing.
try:
    from opentelemetry.propagate import extract
    from opentelemetry.trace import Status, StatusCode

    PROPAGATION_AVAILABLE = OTEL_AVAILABLE
except ImportError:
    extract = None  # type: ignore[assignment]
    Status = None  # type: ignore[assignment, misc]
    StatusCode = None  # type: ignore[assignment, misc]
    PROPAGATION_AVAILABLE = False


# Named explicitly so the logger name stays stable across the extraction.
logger = logging.getLogger("eventsource.bus.rabbitmq")


class RabbitMQConsumer:
    """Owns the RabbitMQ consume loop and the retry/DLQ write path."""

    def __init__(
        self,
        config: RabbitMQEventBusConfig,
        connection: RabbitMQConnectionManager,
        topology: RabbitMQTopology,
        stats: RabbitMQEventBusStats,
        retry_policy: RetryPolicy,
        handlers_for: Callable[[type[DomainEvent]], tuple[HandlerAdapter, ...]],
        resolve_event_class: Callable[[str], type[DomainEvent] | None],
        tracer: Tracer | None,
        enable_tracing: bool,
    ) -> None:
        self._config = config
        self._connection = connection
        self._topology = topology
        self._stats = stats
        self._retry_policy = retry_policy
        self._handlers_for = handlers_for
        self._resolve_event_class = resolve_event_class
        self._tracer = tracer
        self._enable_tracing = enable_tracing

        self._consuming = False
        self._consumer_task: asyncio.Task[None] | None = None

        self._logger = logging.getLogger("eventsource.bus.rabbitmq")

    # =========================================================================
    # State
    # =========================================================================

    @property
    def is_consuming(self) -> bool:
        """Check if currently consuming events."""
        return self._consuming

    @property
    def consumer_task(self) -> asyncio.Task[None] | None:
        """The background consumer task, if one is running."""
        return self._consumer_task

    # =========================================================================
    # Consume loop
    # =========================================================================

    async def start(self) -> None:
        """Start consuming events from the RabbitMQ queue.

        Runs continuously, consuming messages from the queue and dispatching
        them to registered handlers, until :meth:`stop` is called or the
        connection is lost.

        Raises:
            RuntimeError: If the consumer queue is not initialized.
        """
        if not self._topology.consumer_queue:
            raise RuntimeError("Consumer queue not initialized")

        self._consuming = True
        consumer_name = self._config.consumer_name

        self._logger.info(
            f"Starting RabbitMQ consumer: {consumer_name}",
            extra={
                "consumer_name": consumer_name,
                "queue": self._config.queue_name,
                "prefetch_count": self._config.prefetch_count,
            },
        )

        try:
            # Use queue iterator for consuming
            async with self._topology.consumer_queue.iterator() as queue_iter:
                async for message in queue_iter:
                    if not self._consuming:
                        break

                    await self._process_message(message)

        except asyncio.CancelledError:
            self._logger.info(
                "Consumer loop cancelled",
                extra={
                    "consumer_name": consumer_name,
                    "queue": self._config.queue_name,
                },
            )
        except Exception as e:
            self._logger.error(
                f"Error in consumer loop: {e}",
                exc_info=True,
                extra={
                    "consumer_name": consumer_name,
                    "queue": self._config.queue_name,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
            )
            raise
        finally:
            self._consuming = False
            self._logger.info(
                "Consumer loop stopped",
                extra={
                    "consumer_name": consumer_name,
                    "queue": self._config.queue_name,
                    "events_consumed": self._stats.events_consumed,
                    "events_processed_success": self._stats.events_processed_success,
                    "events_processed_failed": self._stats.events_processed_failed,
                },
            )

    async def stop(self) -> None:
        """Stop the consumer loop gracefully.

        Sets the consuming flag to False, which will cause the consumer loop
        to exit after processing the current message.
        """
        self._consuming = False
        self._logger.info(
            "Stop consuming requested",
            extra={
                "consumer_name": self._config.consumer_name,
                "queue": self._config.queue_name,
            },
        )

    def start_in_background(
        self,
        runner: Callable[[], Coroutine[Any, Any, None]] | None = None,
    ) -> asyncio.Task[None]:
        """Start consuming in a background task.

        Args:
            runner: Optional coroutine factory to run instead of :meth:`start`.
                The facade passes its own ``start_consuming`` so the background
                task keeps its auto-connect behavior.

        Returns:
            The background task running the consumer.

        Raises:
            RuntimeError: If consumer is already running in background.
        """
        if self._consumer_task is not None:
            raise RuntimeError("Consumer already running in background")

        self._consumer_task = asyncio.create_task(
            self.start() if runner is None else runner(),
            name=f"rabbitmq-consumer-{self._config.consumer_name}",
        )
        return self._consumer_task

    async def resume_if_was_consuming(self) -> None:
        """Restart consuming iff it was active before the connection dropped.

        Intended as a reconnect hook. Reads the ``_was_consuming`` flag the
        connection manager sets from its close callbacks, clears it, and
        restarts the consume loop in the background.

        Note:
            Not currently registered via ``RabbitMQConnectionManager.on_reconnect``
            -- the facade only wires ``RabbitMQTopology.redeclare``. This
            matches the original (pre-decomposition) behavior, which never
            resumed consuming automatically after a reconnect. Registering
            this method would change that behavior and needs an explicit
            decision, not a silent side effect of refactoring.
        """
        if not self._connection._was_consuming:
            return

        self._connection._was_consuming = False
        self._logger.info(
            "Resuming consumer after reconnection",
            extra={
                "consumer_name": self._config.consumer_name,
                "queue": self._config.queue_name,
            },
        )
        self.start_in_background()

    # =========================================================================
    # Message processing
    # =========================================================================

    def _deserialize_event(self, message: AbstractIncomingMessage) -> DomainEvent | None:
        """Deserialize an AMQP message to a domain event."""
        return serialization.deserialize_event(message, self._resolve_event_class, self._logger)

    async def _process_message(
        self,
        message: AbstractIncomingMessage,
    ) -> None:
        """Process a single message from the queue with retry handling and tracing.

        Deserializes the event and dispatches to registered handlers.
        Tracks retry count via x-retry-count header and implements
        exponential backoff before DLQ routing.

        When tracing is enabled, creates a consumer span that extracts
        trace context from message headers for distributed tracing correlation.

        On success: acknowledges the message.
        On failure:
        - If retries remaining: republish with incremented retry count
        - If max_retries exceeded: send to DLQ with failure metadata

        Also tracks DLQ-related information from x-death headers for
        observability and debugging purposes.

        Args:
            message: The incoming AMQP message
        """
        headers = message.headers or {}
        event_type_name = str(headers.get("event_type", "unknown"))
        # Extract retry count with type-safe conversion
        # Header values can be various types, so we ensure numeric conversion
        retry_count_value = headers.get("x-retry-count")
        if retry_count_value is None:
            retry_count = 0
        elif isinstance(retry_count_value, int):
            retry_count = retry_count_value
        else:
            # Handle string or other numeric types
            retry_count = int(str(retry_count_value))

        # Extract death info for logging and tracking
        death_info = death_headers.get_death_info(message)
        is_redelivered = death_headers.is_from_dlq(message)

        log_extra: dict[str, Any] = {
            "message_id": message.message_id,
            "event_type": event_type_name,
            "routing_key": message.routing_key,
            "retry_count": retry_count,
        }

        # Add death info if message was dead-lettered
        if is_redelivered:
            log_extra.update(
                {
                    "is_dead_lettered": True,
                    "death_count": death_info["death_count"],
                    "first_death_queue": death_info["first_death_queue"],
                    "first_death_reason": death_info["first_death_reason"],
                    "original_routing_key": death_info["original_routing_key"],
                }
            )
            self._logger.info(
                f"Processing dead-lettered message: {event_type_name}",
                extra=log_extra,
            )
        else:
            self._logger.debug(
                f"Processing message (attempt {retry_count + 1}): {event_type_name}",
                extra=log_extra,
            )

        processing_start = datetime.now(UTC)

        # Set up tracing if enabled with context extraction for distributed tracing
        span = None
        ctx = None

        # Use Tracer's start_span with SpanKindEnum.CONSUMER for distributed tracing
        # Extract trace context from message headers to link consumer span to publisher span
        if self._enable_tracing and PROPAGATION_AVAILABLE and self._tracer is not None:
            # Extract trace context from message headers for distributed tracing
            if extract is not None:
                ctx = extract(dict(headers))

            span = self._tracer.start_span(
                "eventsource.event_bus.consume",
                kind=SpanKindEnum.CONSUMER,
                attributes={
                    ATTR_MESSAGING_SYSTEM: "rabbitmq",
                    ATTR_MESSAGING_DESTINATION: self._config.queue_name,
                    "messaging.destination_kind": "queue",
                    "messaging.message_id": message.message_id or "",
                    ATTR_EVENT_TYPE: event_type_name,
                    "messaging.rabbitmq.routing_key": message.routing_key or "",
                },
                context=ctx,
            )

        try:
            # Deserialize event
            event = self._deserialize_event(message)

            if event is None:
                # Unknown event type - acknowledge to prevent blocking
                self._logger.warning(
                    f"Unknown event type: {event_type_name}, acknowledging to skip",
                    extra={
                        "event_type": event_type_name,
                        "message_id": message.message_id,
                    },
                )
                if span:
                    span.set_attribute("event.unknown_type", True)
                    span.set_status(Status(StatusCode.OK, "Unknown event type"))
                await message.ack()
                return

            if span:
                span.set_attribute("event.id", str(event.event_id))

            # Dispatch to handlers with tracing
            await self._dispatch_event(event, message, span)

            # Acknowledge successful processing
            await message.ack()

            self._stats.events_consumed += 1
            self._stats.events_processed_success += 1
            self._stats.last_consume_at = datetime.now(UTC)

            processing_duration = (datetime.now(UTC) - processing_start).total_seconds()

            if span:
                span.set_status(Status(StatusCode.OK))

            self._logger.debug(
                f"Successfully processed {event_type_name}",
                extra={
                    "message_id": message.message_id,
                    "event_id": str(event.event_id),
                    "event_type": event_type_name,
                    "retry_count": retry_count,
                    "duration_ms": processing_duration * 1000,
                    "success": True,
                },
            )

        except Exception as e:
            self._stats.events_processed_failed += 1
            self._stats.last_error_at = datetime.now(UTC)

            processing_duration = (datetime.now(UTC) - processing_start).total_seconds()
            error_extra: dict[str, Any] = {
                "message_id": message.message_id,
                "event_type": event_type_name,
                "retry_count": retry_count,
                "duration_ms": processing_duration * 1000,
                "error": str(e),
                "error_type": type(e).__name__,
            }

            # Include death info in error logging
            if is_redelivered:
                error_extra.update(
                    {
                        "is_dead_lettered": True,
                        "death_count": death_info["death_count"],
                        "first_death_queue": death_info["first_death_queue"],
                    }
                )

            if span:
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.record_exception(e)

            self._logger.error(
                f"Failed to process message: {e}",
                exc_info=True,
                extra=error_extra,
            )

            # Handle retry or DLQ routing. Unwrap a single-failure
            # HandlerDispatchError so retry/DLQ metadata (x-dlq-error-type,
            # etc.) still reflects the handler's own exception type rather
            # than the aggregate wrapper -- error isolation changes how
            # dispatch runs handlers, not what gets reported for a single
            # failing handler.
            dlq_error: Exception = e
            if isinstance(e, HandlerDispatchError) and len(e.failures) == 1:
                dlq_error = e.failures[0][1]
            await self._handle_failed_message(message, dlq_error, retry_count)

        finally:
            if span:
                span.end()

    async def _dispatch_event(
        self,
        event: DomainEvent,
        message: AbstractIncomingMessage,
        parent_span: Any = None,
    ) -> None:
        """Dispatch an event to all matching handlers with optional tracing.

        Invokes handlers for the specific event type and wildcard handlers.
        When tracing is enabled and a parent span is provided, creates child
        spans for each handler execution.

        Args:
            event: The deserialized domain event
            message: Original AMQP message for context
            parent_span: Optional parent span for tracing. If provided and
                        tracing is enabled, child spans are created for each
                        handler execution.

        Raises:
            HandlerDispatchError: If one or more handlers raise. Every handler
                still runs for this delivery (error isolation); failures are
                collected and raised together afterward so the message is
                still rejected for retry/DLQ exactly as a single raise would
                have done.
        """
        event_type = type(event)

        # Get all handlers
        handlers = self._handlers_for(event_type)

        if not handlers:
            self._logger.warning(
                f"No handlers registered for {event.event_type}",
                extra={"event_type": event.event_type},
            )
            return

        # Add handler count to parent span for observability
        if parent_span is not None:
            parent_span.set_attribute(ATTR_HANDLER_COUNT, len(handlers))

        self._logger.debug(
            f"Dispatching {event.event_type} to {len(handlers)} handler(s)",
            extra={
                "event_type": event.event_type,
                "event_id": str(event.event_id),
                "handler_count": len(handlers),
            },
        )

        # Process handlers sequentially for ordering guarantees. Every handler
        # runs for this delivery even if an earlier one failed (error
        # isolation) -- failures are collected and raised together as one
        # HandlerDispatchError afterward.
        failures: list[tuple[str, Exception]] = []
        for adapter in handlers:
            handler_name = adapter.name
            handler_start = datetime.now(UTC)
            handler_span = None

            # Create handler span if tracing is enabled (using composition-based tracer)
            if (
                self._enable_tracing
                and parent_span
                and PROPAGATION_AVAILABLE
                and self._tracer is not None
            ):
                handler_span = self._tracer.start_span(
                    "eventsource.event_bus.handle",
                    kind=SpanKindEnum.INTERNAL,
                    attributes={
                        ATTR_HANDLER_NAME: handler_name,
                        ATTR_EVENT_TYPE: event.event_type,
                        ATTR_EVENT_ID: str(event.event_id),
                    },
                )

            try:
                await adapter.handle(event)

                handler_duration = (datetime.now(UTC) - handler_start).total_seconds()

                if handler_span:
                    handler_span.set_attribute("handler.duration_ms", handler_duration * 1000)
                    handler_span.set_attribute(ATTR_HANDLER_SUCCESS, True)
                    handler_span.set_status(Status(StatusCode.OK))

                self._logger.debug(
                    f"Handler {handler_name} processed {event.event_type}",
                    extra={
                        "handler": handler_name,
                        "event_type": event.event_type,
                        "event_id": str(event.event_id),
                        "duration_ms": handler_duration * 1000,
                    },
                )

            except Exception as e:
                self._stats.handler_errors += 1
                self._stats.last_error_at = datetime.now(UTC)
                handler_duration = (datetime.now(UTC) - handler_start).total_seconds()

                if handler_span:
                    handler_span.set_attribute(ATTR_HANDLER_SUCCESS, False)
                    handler_span.set_status(Status(StatusCode.ERROR, str(e)))
                    handler_span.record_exception(e)

                self._logger.error(
                    f"Handler {handler_name} failed: {e}",
                    exc_info=True,
                    extra={
                        "handler": handler_name,
                        "event_type": event.event_type,
                        "event_id": str(event.event_id),
                        "message_id": message.message_id,
                        "duration_ms": handler_duration * 1000,
                        "error": str(e),
                        "error_type": type(e).__name__,
                    },
                )
                failures.append((handler_name, e))

            finally:
                if handler_span:
                    handler_span.end()

        if failures:
            raise HandlerDispatchError(failures)

    # =========================================================================
    # Retry / DLQ write path
    # =========================================================================

    def _calculate_retry_delay(self, retry_count: int) -> float:
        """Calculate the delay before the next retry.

        Delegates to the shared RetryPolicy so Kafka and RabbitMQ cannot drift
        apart again.

        Args:
            retry_count: Zero-based retry attempt number.

        Returns:
            Delay in seconds, with symmetric jitter applied.
        """
        return self._retry_policy.delay_for(retry_count)

    async def _handle_failed_message(
        self,
        message: AbstractIncomingMessage,
        error: Exception,
        retry_count: int,
    ) -> None:
        """Handle a failed message - retry with backoff or route to DLQ.

        This method implements the retry logic with exponential backoff. When
        a message fails processing:
        1. If retry_count < max_retries: republish with incremented retry count
           after applying exponential backoff delay
        2. If retry_count >= max_retries: send to DLQ with failure metadata

        Args:
            message: The failed message
            error: The exception that caused the failure
            retry_count: Current retry count (from x-retry-count header)
        """
        headers = message.headers or {}
        event_type_name = str(headers.get("event_type", "unknown"))

        if retry_count >= self._config.max_retries:
            # Max retries exceeded - send to DLQ
            await self._send_to_dlq(message, error, retry_count)
            await message.ack()  # Ack to remove from main queue

            self._logger.warning(
                f"Message sent to DLQ after {retry_count} retries: {event_type_name}",
                extra={
                    "message_id": message.message_id,
                    "event_type": event_type_name,
                    "retry_count": retry_count,
                    "error": str(error),
                    "dlq_queue": self._config.dlq_queue_name,
                },
            )
        else:
            # Calculate backoff delay
            delay = self._calculate_retry_delay(retry_count)

            self._logger.info(
                f"Scheduling retry {retry_count + 1}/{self._config.max_retries} "
                f"for {event_type_name} after {delay:.2f}s delay",
                extra={
                    "message_id": message.message_id,
                    "event_type": event_type_name,
                    "retry_count": retry_count,
                    "next_retry": retry_count + 1,
                    "max_retries": self._config.max_retries,
                    "delay_seconds": delay,
                },
            )

            # Apply backoff delay
            if delay > 0:
                await asyncio.sleep(delay)

            # Retry - republish with incremented retry count
            await self._republish_for_retry(message, retry_count + 1)
            await message.ack()  # Ack original, republished copy will be processed

            self._logger.info(
                f"Republished message for retry {retry_count + 1}",
                extra={
                    "message_id": message.message_id,
                    "event_type": event_type_name,
                    "retry_count": retry_count + 1,
                },
            )

    async def _republish_for_retry(
        self,
        original_message: AbstractIncomingMessage,
        new_retry_count: int,
    ) -> None:
        """Republish a message with incremented retry count.

        Creates a new message with updated headers containing the incremented
        retry count and timestamp of the retry attempt. The message body and
        other properties are preserved from the original message.

        Args:
            original_message: The original message to retry
            new_retry_count: The new retry count value

        Raises:
            RuntimeError: If exchange is not initialized
        """
        if not self._topology.exchange:
            raise RuntimeError("Exchange not initialized")

        # Copy headers and update retry count
        headers = dict(original_message.headers or {})
        headers["x-retry-count"] = new_retry_count
        headers["x-last-retry-at"] = datetime.now(UTC).isoformat()

        # Create new message with updated headers
        retry_message = Message(
            body=original_message.body,
            content_type=original_message.content_type,
            content_encoding=original_message.content_encoding,
            delivery_mode=DeliveryMode.PERSISTENT,
            message_id=original_message.message_id,
            headers=headers,
        )

        # Republish to exchange with original routing key
        routing_key = original_message.routing_key or ""
        await self._topology.exchange.publish(retry_message, routing_key=routing_key)

    async def _send_to_dlq(
        self,
        message: AbstractIncomingMessage,
        error: Exception,
        retry_count: int,
    ) -> None:
        """Send a failed message to the dead letter queue.

        Publishes the failed message to the DLQ exchange with additional
        headers containing failure metadata:
        - x-dlq-reason: Error message
        - x-dlq-error-type: Exception class name
        - x-dlq-retry-count: Number of retries before DLQ
        - x-dlq-timestamp: When message was sent to DLQ
        - x-original-routing-key: Original routing key

        Args:
            message: The failed message
            error: The exception that caused the failure
            retry_count: Final retry count before DLQ

        Note:
            If DLQ is not enabled or DLQ exchange is not initialized,
            this method logs a warning and returns without action.
        """
        if not self._config.enable_dlq or not self._topology.dlq_exchange:
            self._logger.warning(
                "DLQ not enabled or not initialized, message will be lost",
                extra={"message_id": message.message_id},
            )
            return

        headers = dict(message.headers or {})
        event_type_name = str(headers.get("event_type", "unknown"))

        # Add failure metadata to headers
        headers["x-dlq-reason"] = str(error)
        headers["x-dlq-error-type"] = type(error).__name__
        headers["x-dlq-retry-count"] = retry_count
        headers["x-dlq-timestamp"] = datetime.now(UTC).isoformat()
        headers["x-original-routing-key"] = message.routing_key or ""

        # Create DLQ message with failure metadata
        dlq_message = Message(
            body=message.body,
            content_type=message.content_type,
            content_encoding=message.content_encoding,
            delivery_mode=DeliveryMode.PERSISTENT,
            message_id=message.message_id,
            headers=headers,
        )

        # Publish to DLQ exchange with queue name as routing key
        await self._topology.dlq_exchange.publish(
            dlq_message,
            routing_key=self._config.queue_name,
        )

        self._stats.messages_sent_to_dlq += 1

        self._logger.warning(
            f"Sent message to DLQ after {retry_count} retries: {event_type_name}",
            extra={
                "message_id": message.message_id,
                "event_type": event_type_name,
                "retry_count": retry_count,
                "error": str(error),
                "error_type": type(error).__name__,
                "dlq_queue": self._config.dlq_queue_name,
            },
        )

    # =========================================================================
    # Graceful shutdown
    # =========================================================================

    async def stop_gracefully(self, timeout: float) -> None:
        """Stop consuming and wait for consumer task to finish.

        This method signals the consumer loop to stop by setting the consuming
        flag to False, then waits for the consumer task to complete. If the
        task doesn't complete within the timeout, it is cancelled.

        Args:
            timeout: Maximum time to wait for the consumer to stop.
                    Half of this time is used for graceful stop, the other
                    half for cancellation if needed.

        Raises:
            asyncio.TimeoutError: If the consumer doesn't stop within timeout
        """
        if not self._consuming:
            self._logger.debug("Not consuming, skipping consumer stop")
            return

        self._logger.debug("Stopping consumer...")

        # Signal consumer to stop
        self._consuming = False

        # Wait for consumer task if running
        if self._consumer_task:
            try:
                # Use half timeout for graceful wait, reserve half for cleanup
                await asyncio.wait_for(
                    asyncio.shield(self._consumer_task),
                    timeout=timeout / 2,
                )
                self._logger.debug("Consumer task completed gracefully")
            except TimeoutError:
                self._logger.warning(
                    "Consumer task did not stop in time, cancelling",
                    extra={"timeout": timeout / 2},
                )
                self._consumer_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._consumer_task
                self._logger.debug("Consumer task cancelled successfully")
            except asyncio.CancelledError:
                self._logger.debug("Consumer task was already cancelled")
            finally:
                self._consumer_task = None

        self._logger.debug("Consumer stopped")

    async def drain_in_flight(self, timeout: float) -> None:
        """Wait for any in-flight message processing to complete.

        This is a simple implementation that waits a portion of the timeout
        to allow any ongoing message handlers to complete. In the current
        implementation, handlers run synchronously within the consumer loop,
        so once the consumer stops, no handlers should be running.

        A more sophisticated implementation could track active handlers
        with a counter or semaphore for more precise draining.

        Args:
            timeout: Maximum time available for draining.
                    Actual drain time is min(timeout / 4, 5.0) seconds.
        """
        # In current implementation, handlers run synchronously in consumer loop
        # So if consumer is stopped, no handlers are running
        # This is a placeholder for future async handler support
        drain_time = min(timeout / 4, 5.0)  # Wait up to 5 seconds

        if drain_time > 0:
            self._logger.debug(
                f"Draining in-flight messages ({drain_time:.2f}s)",
                extra={"drain_time_seconds": drain_time},
            )
            await asyncio.sleep(drain_time)
            self._logger.debug("Drain period completed")
