"""Kafka event bus implementation using aiokafka.

This module provides a distributed event bus implementation using Apache Kafka
for high-throughput event distribution across multiple processes and servers.

Features:
- High-throughput event publishing
- Consumer groups for horizontal scaling
- At-least-once delivery guarantees
- Partition-based ordering by aggregate_id
- Dead letter queue for unrecoverable failures
- Configurable retry policies with non-blocking republish
- Consumer rebalance handling for safe horizontal scaling
- Optional OpenTelemetry tracing
- Optional OpenTelemetry metrics

Delivery Guarantees:
    This implementation provides **at-least-once** delivery semantics. Events may
    be processed multiple times in failure scenarios (consumer crash, rebalance).
    Handlers should be idempotent to handle duplicate deliveries gracefully.

    **Exactly-once semantics are NOT supported.** The event bus does not coordinate
    transactionally with the event store. This means:
    - If an event store write succeeds but Kafka publish fails, consumers miss events
    - If Kafka publish succeeds but event store write fails, consumers see phantom events

    For use cases requiring stronger consistency guarantees, consider:
    1. Transactional Outbox Pattern: Store events in an outbox table atomically with
       domain changes, then relay to Kafka separately
    2. Kafka Transactions: Use idempotent producers with transactional semantics
       (requires Kafka 0.11+)
    3. Saga/Compensation patterns: Design for eventual consistency with compensating
       actions for failures

OpenTelemetry Metrics:
    When ``enable_metrics=True`` (default) and the OpenTelemetry SDK is installed,
    the Kafka event bus emits comprehensive metrics for monitoring throughput,
    latency, and health of your event processing pipeline.

    Counter Metrics:
        - ``kafka.eventbus.messages.published``: Total messages published
        - ``kafka.eventbus.messages.consumed``: Total messages consumed
        - ``kafka.eventbus.handler.invocations``: Handler invocation count
        - ``kafka.eventbus.handler.errors``: Handler error count
        - ``kafka.eventbus.messages.dlq``: Messages sent to dead letter queue
        - ``kafka.eventbus.connection.errors``: Connection error count
        - ``kafka.eventbus.reconnections``: Reconnection attempt count
        - ``kafka.eventbus.rebalances``: Consumer rebalance count
        - ``kafka.eventbus.publish.errors``: Publish error count

    Histogram Metrics:
        - ``kafka.eventbus.publish.duration``: Publish latency in milliseconds
        - ``kafka.eventbus.consume.duration``: Message processing latency in ms
        - ``kafka.eventbus.handler.duration``: Handler execution time in ms
        - ``kafka.eventbus.batch.size``: Publish batch sizes

    Observable Gauge Metrics:
        - ``kafka.eventbus.connections.active``: Connection status (1=connected, 0=disconnected)
        - ``kafka.eventbus.consumer.lag``: Messages behind per partition

    See the Kafka Metrics Guide (docs/guides/kafka-metrics.md) for PromQL
    queries, alerting recommendations, and Grafana dashboard examples.

Example:
    >>> from eventsource.bus.kafka import KafkaEventBus, KafkaEventBusConfig
    >>>
    >>> config = KafkaEventBusConfig(
    ...     bootstrap_servers="localhost:9092",
    ...     topic_prefix="events",
    ...     consumer_group="projections",
    ...     enable_metrics=True,  # Enable OpenTelemetry metrics (default)
    ... )
    >>> bus = KafkaEventBus(config=config, event_registry=my_registry)
    >>> await bus.connect()
    >>> await bus.publish([MyEvent(...)])
    >>> await bus.start_consuming()
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
import warnings
from collections.abc import Iterable
from datetime import UTC, datetime
from types import TracebackType
from typing import TYPE_CHECKING, Any

from eventsource.bus.base import BaseEventBus
from eventsource.bus.kafka.config import KafkaEventBusConfig
from eventsource.bus.kafka.connection import (
    KafkaConnectionManager,
    KafkaRebalanceListener,
)
from eventsource.bus.kafka.metrics import (
    KafkaEventBusMetrics,
    register_connection_gauge,
    register_consumer_lag_gauge,
)
from eventsource.bus.kafka.models import (
    DeserializationError,
    KafkaEventBusStats,
    KafkaNotAvailableError,
)
from eventsource.bus.kafka.serialization import EventSerializer
from eventsource.bus.retry import RetryPolicy
from eventsource.events.base import DomainEvent
from eventsource.exceptions import HandlerDispatchError
from eventsource.handlers.adapter import HandlerAdapter
from eventsource.observability import OTEL_AVAILABLE, SpanKindEnum, Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
    ATTR_HANDLER_NAME,
    ATTR_MESSAGING_DESTINATION,
    ATTR_MESSAGING_OPERATION,
    ATTR_MESSAGING_SYSTEM,
)

if TYPE_CHECKING:
    from eventsource.events.registry import EventRegistry

# Optional aiokafka import - fail gracefully if not installed
try:
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
    from aiokafka.errors import KafkaError

    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    AIOKafkaProducer = None
    AIOKafkaConsumer = None
    TopicPartition = None
    KafkaError = Exception

# OpenTelemetry metrics and propagation imports - kept separate from TracingMixin
# These are NOT in TracingMixin and must be imported directly for metrics and inject/extract
try:
    from opentelemetry import metrics as otel_metrics
    from opentelemetry.metrics import CallbackOptions, Observation
    from opentelemetry.propagate import extract, inject
    from opentelemetry.trace import SpanKind, Status, StatusCode

    PROPAGATION_AVAILABLE = OTEL_AVAILABLE
except ImportError:
    otel_metrics = None  # type: ignore[assignment]
    CallbackOptions = None  # type: ignore[assignment, misc]
    Observation = None  # type: ignore[assignment, misc]
    extract = None  # type: ignore[assignment]
    inject = None  # type: ignore[assignment]
    SpanKind = None  # type: ignore[assignment, misc]
    Status = None  # type: ignore[assignment, misc]
    StatusCode = None  # type: ignore[assignment, misc]
    PROPAGATION_AVAILABLE = False

# Pinned explicitly: __name__ is "eventsource.bus.kafka.bus" after the
# package move, but the public logger name must stay "eventsource.bus.kafka".
logger = logging.getLogger("eventsource.bus.kafka")

# Module-level meter cache for lazy initialization
_meter: Any = None


def _get_meter() -> Any:
    """Get or create the OpenTelemetry meter.

    Returns a meter instance for creating metric instruments. The meter is
    lazily initialized on first use and cached for subsequent calls.

    Returns:
        The meter instance, or None if OpenTelemetry is not available.
    """
    global _meter
    if not OTEL_AVAILABLE:
        return None
    if _meter is None:
        _meter = otel_metrics.get_meter("eventsource.bus.kafka")
    return _meter


class KafkaEventBus(BaseEventBus):
    """Kafka implementation of the EventBus interface.

    Provides a distributed event bus using Apache Kafka for high-throughput
    event distribution. Supports consumer groups, dead letter queues, and
    optional OpenTelemetry tracing.

    The bus creates events on a single topic ({topic_prefix}.stream) and uses
    the aggregate_id as the partition key to ensure ordering within aggregates.

    Thread Safety:
        - Subscription methods are thread-safe
        - Publishing and consuming should only be called from async context

    Example:
        >>> config = KafkaEventBusConfig(
        ...     bootstrap_servers="localhost:9092",
        ...     topic_prefix="myapp.events",
        ...     consumer_group="projections",
        ... )
        >>> async with KafkaEventBus(config=config) as bus:
        ...     await bus.publish([event])
        ...     bus.subscribe(OrderCreated, handler)
        ...     await bus.start_consuming()
    """

    def __init__(
        self,
        config: KafkaEventBusConfig | None = None,
        event_registry: EventRegistry | None = None,
        serializer: EventSerializer | None = None,
        *,
        tracer: Tracer | None = None,
    ) -> None:
        """Initialize the Kafka event bus.

        Args:
            config: Configuration for Kafka connection. Uses defaults if None.
            event_registry: Registry for event type resolution. Uses global
                registry if None.
            serializer: Custom event serializer. Uses JSON serializer if None.
                Implement EventSerializer to add Avro, Protobuf, or schema
                registry support.
            tracer: Optional custom Tracer instance. If not provided, one is
                   created based on config.enable_tracing setting.

        Raises:
            KafkaNotAvailableError: If aiokafka is not installed.
        """
        if not KAFKA_AVAILABLE:
            raise KafkaNotAvailableError()

        super().__init__(event_registry=event_registry)

        self._config = config or KafkaEventBusConfig()
        self._serializer = serializer or EventSerializer()

        # Initialize tracing via composition (replaces TracingMixin)
        self._tracer = tracer or create_tracer(__name__, self._config.enable_tracing)
        self._enable_tracing = self._tracer.enabled

        # Connection state
        self._consuming = False
        self._consume_task: asyncio.Task[None] | None = None

        # Shared retry policy (keeps Kafka and RabbitMQ backoff/jitter in sync)
        self._retry_policy = RetryPolicy(
            base_delay=self._config.retry_base_delay,
            max_delay=self._config.retry_max_delay,
            jitter=self._config.retry_jitter,
            max_retries=self._config.max_retries,
        )

        # Statistics
        self._stats = KafkaEventBusStats()

        # Initialize metrics (lazy initialization like tracing)
        metrics: KafkaEventBusMetrics | None = None
        self._meter: Any = None  # Store meter for gauge registration
        if self._config.enable_metrics:
            self._meter = _get_meter()
            if self._meter:
                metrics = KafkaEventBusMetrics(self._meter)

        # Connection lifecycle (producer/consumer state, reconnection/rebalance
        # metrics recording) is delegated to KafkaConnectionManager.
        self._connection_manager = KafkaConnectionManager(
            config=self._config,
            stats=self._stats,
            metrics=metrics,
        )

        # Track if gauges are registered (gauges can only be registered once)
        self._connection_gauge_registered = False
        self._lag_gauge_registered = False

        # Shutdown coordination
        self._shutdown_event = asyncio.Event()

        logger.debug(
            "KafkaEventBus initialized",
            extra=self._config.get_sanitized_config(),
        )

    # =========================================================================
    # Properties
    # =========================================================================

    @property
    def is_connected(self) -> bool:
        """Check if connected to Kafka.

        Returns:
            True if producer and consumer are connected.
        """
        return self._connection_manager.is_connected

    @property
    def _connected(self) -> bool:
        """Internal alias for ``is_connected``, delegating to the connection manager.

        Kept as a settable property (rather than removed outright) because
        tests reach into ``bus._connected`` directly to simulate connection
        state without going through ``connect()``.
        """
        return self._connection_manager.is_connected

    @_connected.setter
    def _connected(self, value: bool) -> None:
        self._connection_manager._connected = value

    @property
    def _producer(self) -> AIOKafkaProducer | None:
        """Internal alias delegating to the connection manager's producer."""
        return self._connection_manager.producer

    @_producer.setter
    def _producer(self, value: AIOKafkaProducer | None) -> None:
        self._connection_manager._producer = value

    @property
    def _consumer(self) -> AIOKafkaConsumer | None:
        """Internal alias delegating to the connection manager's consumer."""
        return self._connection_manager.consumer

    @_consumer.setter
    def _consumer(self, value: AIOKafkaConsumer | None) -> None:
        self._connection_manager._consumer = value

    @property
    def _rebalance_listener(self) -> KafkaRebalanceListener | None:
        """Internal alias delegating to the connection manager's rebalance listener."""
        return self._connection_manager._rebalance_listener

    @_rebalance_listener.setter
    def _rebalance_listener(self, value: KafkaRebalanceListener | None) -> None:
        self._connection_manager._rebalance_listener = value

    @property
    def _metrics(self) -> KafkaEventBusMetrics | None:
        """Internal alias delegating to the connection manager's metrics."""
        return self._connection_manager.metrics

    @_metrics.setter
    def _metrics(self, value: KafkaEventBusMetrics | None) -> None:
        self._connection_manager.metrics = value

    @property
    def is_consuming(self) -> bool:
        """Check if actively consuming messages.

        Returns:
            True if consume loop is running.
        """
        return self._consuming

    @property
    def config(self) -> KafkaEventBusConfig:
        """Get the configuration.

        Returns:
            The KafkaEventBusConfig instance.
        """
        return self._config

    @property
    def stats(self) -> KafkaEventBusStats:
        """Get current statistics.

        Returns:
            The KafkaEventBusStats instance.
        """
        return self._stats

    # =========================================================================
    # Connection Lifecycle Methods
    # =========================================================================

    async def connect(self) -> None:
        """Connect to Kafka cluster.

        Creates and starts the producer and consumer clients. The consumer
        subscribes to the configured topic.

        Raises:
            KafkaError: If connection to Kafka fails.
        """
        await self._connection_manager.connect()

        if self._connection_manager.is_connected:
            # Wire up observable gauges now that the connection is fully
            # established -- never as a side effect of a failed connect.
            self._wire_metrics()

    async def disconnect(self) -> None:
        """Disconnect from Kafka cluster.

        Stops consuming if active and closes producer/consumer connections.
        Safe to call multiple times.
        """
        if not self._connection_manager.is_connected:
            logger.debug("KafkaEventBus not connected, nothing to disconnect")
            return

        # Stop consuming first
        if self._consuming:
            await self.stop_consuming()

        await self._connection_manager.disconnect()

    # =========================================================================
    # Context Manager Support
    # =========================================================================

    async def __aenter__(self) -> KafkaEventBus:
        """Enter async context manager.

        Connects to Kafka and returns the bus instance.

        Returns:
            The connected KafkaEventBus instance.
        """
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit async context manager.

        Gracefully shuts down the bus with configured timeout.
        """
        await self.shutdown(timeout=self._config.shutdown_timeout)

    # =========================================================================
    # Shutdown
    # =========================================================================

    async def shutdown(self, timeout: float | None = None) -> None:
        """Gracefully shutdown the event bus.

        Stops consuming, waits for in-flight messages, and disconnects.

        Args:
            timeout: Maximum time to wait for shutdown. Uses config default
                if None.
        """
        timeout = timeout or self._config.shutdown_timeout

        logger.info("Shutting down KafkaEventBus", extra={"timeout": timeout})

        # Signal shutdown
        self._shutdown_event.set()

        # Wait for consume task to finish
        if self._consume_task and not self._consume_task.done():
            try:
                await asyncio.wait_for(self._consume_task, timeout=timeout)
            except TimeoutError:
                logger.warning("Shutdown timed out, cancelling consume task")
                self._consume_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._consume_task

        # Drain any tracked background tasks (base class bookkeeping; kept
        # for consistency with the other backends even though Kafka's
        # background publishes are tracked via aiokafka producer futures,
        # not asyncio tasks).
        await self._drain_background(timeout or self._config.shutdown_timeout)

        # Disconnect
        await self.disconnect()

        logger.info("KafkaEventBus shutdown complete")

    async def stop_consuming(self) -> None:
        """Stop the consumer loop gracefully.

        Sets the consuming flag to False which will cause the consume loop
        to exit on its next iteration.
        """
        self._consuming = False
        logger.info("Stop consuming requested")

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def get_stats_dict(self) -> dict[str, Any]:
        """Get statistics as a dictionary.

        Returns:
            JSON-serializable dictionary of statistics.
        """
        return self._stats.get_stats_dict()

    async def get_topic_info(self) -> dict[str, Any]:
        """Get information about the configured topic.

        Returns:
            Dictionary with topic metadata.

        Raises:
            RuntimeError: If not connected.
        """
        if not self._connected or not self._consumer:
            raise RuntimeError("Not connected to Kafka")

        partitions = self._consumer.partitions_for_topic(self._config.topic_name)

        return {
            "topic": self._config.topic_name,
            "partitions": list(partitions) if partitions else [],
            "consumer_group": self._config.consumer_group,
            "connected": self._connected,
            "consuming": self._consuming,
        }

    def record_reconnection(self) -> None:
        """Record a reconnection event for metrics.

        .. deprecated:: 0.7.0
            Only ever intended for internal use; scheduled for removal in
            0.8.0. Use the connection manager directly if you need this.
        """
        warnings.warn(
            "KafkaEventBus.record_reconnection() is deprecated and will be "
            "removed in 0.8.0; it was only ever intended for internal use.",
            DeprecationWarning,
            stacklevel=2,
        )
        self._connection_manager.record_reconnection()

    def record_rebalance(self) -> None:
        """Record a consumer rebalance event for metrics.

        .. deprecated:: 0.7.0
            Only ever intended for internal use; scheduled for removal in
            0.8.0. Use the connection manager directly if you need this.
        """
        warnings.warn(
            "KafkaEventBus.record_rebalance() is deprecated and will be "
            "removed in 0.8.0; it was only ever intended for internal use.",
            DeprecationWarning,
            stacklevel=2,
        )
        self._connection_manager.record_rebalance()

    # =========================================================================
    # Observable Gauge Methods
    # =========================================================================

    def _wire_metrics(self) -> None:
        """Register observable gauges after a successful connect.

        Called once from ``connect()`` after the connection is fully
        established. Kept separate from ``connect()`` itself so gauge
        registration is never a side effect of a failed connection attempt.
        """
        self._register_connection_gauge()
        self._register_consumer_lag_gauge()

    def _register_connection_gauge(self) -> None:
        """Register connection status as an observable gauge.

        Reports 1 when connected, 0 when disconnected. This provides
        visibility into connection uptime and disconnection events.

        The gauge is registered once and the callback is invoked by the
        OpenTelemetry SDK at its configured collection interval.
        """
        if not self._meter or not self._config.enable_metrics or self._metrics is None:
            return

        self._connection_gauge_registered = register_connection_gauge(
            self._meter,
            self._metrics,
            lambda: self._connected,
            self._config.consumer_group,
        )

    def _register_consumer_lag_gauge(self) -> None:
        """Register consumer lag as an observable gauge.

        The gauge reports lag per partition, calculated as the difference
        between the high watermark (latest offset) and the current position.
        This callback is invoked by the OpenTelemetry SDK at its configured
        collection interval (default: 60 seconds).

        Safe to call before the consumer has partition assignments -- the
        supplied callback reports nothing until consuming has started.
        """
        if not self._meter or not self._config.enable_metrics or self._metrics is None:
            return

        self._lag_gauge_registered = register_consumer_lag_gauge(
            self._meter,
            self._metrics,
            self._lag_observations,
        )

    def _lag_observations(self) -> Iterable[Observation]:
        """Compute per-partition consumer lag observations.

        Yields:
            Observation objects with lag values and partition attributes.
            Yields nothing when not consuming, mid-rebalance, or in an
            invalid consumer state.
        """
        # Only report when consuming and have a valid consumer
        if not self._consuming or not self._consumer or not self._connected:
            return

        try:
            assignment = self._consumer.assignment()
            if not assignment:
                return

            for tp in assignment:
                try:
                    # Get high watermark (latest offset in partition)
                    highwater = self._consumer.highwater(tp)
                    # Get current position (next offset to be consumed)
                    position = self._consumer.position(tp)

                    if highwater is not None and position is not None:
                        # Lag is the difference, clamped to >= 0
                        lag = max(0, highwater - position)
                        yield Observation(
                            lag,
                            attributes={
                                "messaging.kafka.partition": tp.partition,
                                "messaging.kafka.consumer_group": self._config.consumer_group,
                                "messaging.destination": tp.topic,
                            },
                        )
                except Exception as e:
                    # Skip partitions with errors (e.g., during rebalance)
                    logger.debug(
                        "Skipping partition %s lag metric due to error: %s",
                        tp,
                        e,
                    )
        except Exception as e:
            # Skip if consumer is in invalid state
            logger.debug("Unable to collect consumer lag metrics: %s", e)

    # =========================================================================
    # Publish Methods
    # =========================================================================

    async def publish(
        self,
        events: list[DomainEvent],
        background: bool = False,
    ) -> None:
        """Publish events to Kafka.

        Events are published to the configured topic with the aggregate_id as
        the partition key. This ensures events for the same aggregate are
        processed in order.

        When tracing is enabled, creates OpenTelemetry spans for each publish
        operation and injects trace context into Kafka message headers for
        distributed tracing correlation.

        Args:
            events: List of domain events to publish.
            background: If True, don't wait for broker acknowledgment.
                Faster but less reliable.

        Raises:
            RuntimeError: If not connected to Kafka.
            KafkaError: If publishing fails.
        """
        if not self._connected or not self._producer:
            raise RuntimeError("Not connected to Kafka. Call connect() first.")

        if not events:
            return

        # Start timing for publish duration histogram
        start_time = time.perf_counter()

        logger.debug(
            "Publishing events to Kafka",
            extra={
                "event_count": len(events),
                "topic": self._config.topic_name,
                "background": background,
            },
        )

        # Hand every event to the producer before awaiting any acknowledgment.
        # Publishing serially cost one full broker round-trip per event. Sends
        # are issued in a plain sequential loop (not gathered) because
        # concurrent sends could reorder same-partition-key messages across
        # an await boundary; only the ack-await is batched together.
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

        # Update statistics
        self._stats.events_published += len(events)
        self._stats.last_publish_at = datetime.now(UTC)

        # Record publish duration and batch size histograms
        if self._metrics:
            duration_ms = (time.perf_counter() - start_time) * 1000
            self._metrics.publish_duration.record(
                duration_ms,
                attributes={
                    "messaging.destination": self._config.topic_name,
                },
            )
            self._metrics.batch_size.record(len(events))

        logger.debug(
            "Events published successfully",
            extra={"event_count": len(events)},
        )

    async def _begin_publish_single_event(
        self,
        event: DomainEvent,
        background: bool,
    ) -> tuple[Any, DomainEvent, Any]:
        """Serialize and hand a single event to the producer.

        Creates an OpenTelemetry span for the publish operation if tracing
        is enabled. The span is kept open (started via ``start_span``, not
        the context-manager form) so it can be closed later once the
        acknowledgment has been awaited -- this lets ``publish`` batch the
        ack-await across events while still tracing the full operation.

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
        if not self._producer:
            raise RuntimeError("Producer not connected")

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
            future = await self._producer.send(
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

    # =========================================================================
    # Handler Management Helpers
    # =========================================================================

    def get_handlers_for_event(self, event_type_name: str) -> list[HandlerAdapter]:
        """Get all handlers for an event type name.

        Deprecated:
            Handlers are now keyed by event class, not by name. This resolves
            the name through the event registry and returns the handlers for
            the resulting class. Prefer ``_handlers_for(event_type)``.

        Args:
            event_type_name: The registered event type name.

        Returns:
            List of HandlerAdapter instances, type-specific first.
        """
        warnings.warn(
            "get_handlers_for_event is deprecated; handlers are keyed by event "
            "class. Use the event class directly.",
            DeprecationWarning,
            stacklevel=2,
        )
        event_class = self._resolve_event_class(event_type_name)
        if event_class is None:
            return []
        return list(self._handlers_for(event_class))

    # =========================================================================
    # Consumer Methods
    # =========================================================================

    async def start_consuming(self, auto_reconnect: bool = True) -> None:
        """Start consuming events from Kafka.

        This method blocks and continuously polls for messages, dispatching
        them to registered handlers. Use stop_consuming() from another
        coroutine to stop.

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
        if not self._connected or not self._consumer:
            raise RuntimeError("Not connected to Kafka. Call connect() first.")

        if self._consuming:
            logger.warning("Already consuming events")
            return

        self._consuming = True
        self._shutdown_event.clear()

        # Register consumer lag gauge (only once)
        self._register_consumer_lag_gauge()

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
                self.record_reconnection()

                # Try to restart the consumer
                try:
                    await self._reconnect_consumer()
                except Exception as reconnect_error:
                    logger.error(
                        "Failed to reconnect consumer",
                        extra={"error": str(reconnect_error)},
                        exc_info=True,
                    )
                    # Continue the loop to retry with backoff

        self._consuming = False
        logger.info("Consumer stopped")

    async def _reconnect_consumer(self) -> None:
        """Attempt to reconnect the consumer after an error.

        Stops the current consumer and creates a new one with the same
        configuration and topic subscriptions.
        """
        await self._connection_manager.reconnect_consumer()

    def start_consuming_in_background(self) -> asyncio.Task[None]:
        """Start consuming in a background task.

        Returns:
            The background task running the consumer.

        Raises:
            RuntimeError: If consumer is already running in background.

        Usage:
            task = bus.start_consuming_in_background()
            # ... do other work ...
            await bus.stop_consuming()
            await task
        """
        if self._consume_task is not None and not self._consume_task.done():
            raise RuntimeError("Consumer already running in background")

        self._consume_task = asyncio.create_task(
            self.start_consuming(),
            name=f"kafka-consumer-{self._config.consumer_name}",
        )
        return self._consume_task

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

    def _get_security_config(self) -> dict[str, Any]:
        """Get security configuration for additional consumers.

        Creates a dictionary of security settings suitable for creating
        additional Kafka consumers (e.g., for DLQ inspection).

        Returns:
            Dictionary of security settings.
        """
        return self._connection_manager.get_security_config()

    # =========================================================================
    # DLQ Inspection and Replay Methods
    # =========================================================================

    async def get_dlq_messages(
        self,
        limit: int = 100,
        timeout_ms: int = 5000,
        use_consumer_group: bool = False,
    ) -> list[dict[str, Any]]:
        """Retrieve messages from the dead letter queue.

        Creates a consumer to read DLQ messages. By default, reads without
        committing offsets (inspection mode). When use_consumer_group=True,
        uses the configured DLQ consumer group for coordinated processing.

        Args:
            limit: Maximum number of messages to retrieve.
            timeout_ms: Timeout for polling in milliseconds.
            use_consumer_group: If True, use dlq_consumer_group for coordinated
                DLQ processing. Messages will be committed after retrieval.

        Returns:
            List of DLQ message dictionaries with headers and payload.
            Each message contains:
            - topic: DLQ topic name
            - partition: Partition number
            - offset: Message offset
            - key: Message key (decoded)
            - timestamp: Message timestamp
            - headers: All headers as string dict
            - payload: Deserialized JSON payload or hex-encoded bytes
            - replay_count: Number of times this message has been replayed

        Raises:
            RuntimeError: If not connected to Kafka.
            ValueError: If use_consumer_group=True but dlq_consumer_group not set.
        """
        if not self._connected:
            raise RuntimeError("Not connected to Kafka")

        group_id = None
        if use_consumer_group:
            if not self._config.dlq_consumer_group:
                raise ValueError(
                    "dlq_consumer_group must be set in config to use consumer group mode"
                )
            group_id = self._config.dlq_consumer_group

        # Create a consumer for DLQ inspection/processing
        dlq_consumer = AIOKafkaConsumer(
            self._config.dlq_topic_name,
            bootstrap_servers=self._config.bootstrap_servers,
            group_id=group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=timeout_ms,
            **self._get_security_config(),
        )

        messages: list[dict[str, Any]] = []

        try:
            await dlq_consumer.start()

            count = 0
            async for message in dlq_consumer:
                if count >= limit:
                    break

                # Parse headers into dict
                headers: dict[str, str] = {}
                if message.headers:
                    for key, value in message.headers:
                        headers[key] = value.decode("utf-8")

                # Get replay count from headers
                replay_count = int(headers.get("dlq_replay_count", "0"))

                # Try to decode value as JSON
                try:
                    import json

                    payload = json.loads(message.value.decode("utf-8"))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    payload = message.value.hex() if message.value else None

                messages.append(
                    {
                        "topic": message.topic,
                        "partition": message.partition,
                        "offset": message.offset,
                        "key": message.key.decode("utf-8") if message.key else None,
                        "timestamp": message.timestamp,
                        "headers": headers,
                        "payload": payload,
                        "replay_count": replay_count,
                    }
                )

                count += 1

            # Commit offsets if using consumer group
            if use_consumer_group and messages:
                await dlq_consumer.commit()

        except TimeoutError:
            # Consumer timed out - this is expected when no more messages
            pass
        finally:
            await dlq_consumer.stop()

        logger.debug(
            "Retrieved DLQ messages",
            extra={"count": len(messages), "limit": limit, "consumer_group": group_id},
        )

        return messages

    async def replay_dlq_message(
        self,
        partition: int,
        offset: int,
        force: bool = False,
    ) -> bool:
        """Replay a specific message from the dead letter queue.

        Reads the message from DLQ and republishes it to the main topic
        for reprocessing. The DLQ message is not deleted (Kafka limitation).

        Replay Loop Protection:
            Each replay increments a dlq_replay_count header. If the count
            exceeds dlq_max_replay_attempts (default 3), the replay is rejected
            to prevent infinite replay loops. Use force=True to override.

        The replayed message:
        - Has all DLQ-specific headers removed except dlq_replay_count
        - Has retry_count reset to 0
        - Has dlq_replay_count incremented
        - Maintains original event headers

        Args:
            partition: The DLQ partition containing the message.
            offset: The offset of the message to replay.
            force: If True, replay even if max replay attempts exceeded.

        Returns:
            True if message was successfully republished.

        Raises:
            RuntimeError: If not connected to Kafka.
            ValueError: If message not found at specified location or max replays exceeded.
        """
        if not self._connected or not self._producer:
            raise RuntimeError("Not connected to Kafka")

        dlq_consumer = AIOKafkaConsumer(
            bootstrap_servers=self._config.bootstrap_servers,
            group_id=None,
            enable_auto_commit=False,
            **self._get_security_config(),
        )

        try:
            await dlq_consumer.start()

            # Assign to specific partition
            tp = TopicPartition(self._config.dlq_topic_name, partition)
            dlq_consumer.assign([tp])

            # Seek to specific offset
            dlq_consumer.seek(tp, offset)

            # Read the message
            message = await asyncio.wait_for(
                dlq_consumer.getone(),
                timeout=5.0,
            )

            if message.offset != offset:
                raise ValueError(f"Message not found at offset {offset}")

            # Check replay count for loop protection
            current_replay_count = 0
            if message.headers:
                for key, value in message.headers:
                    if key == "dlq_replay_count":
                        with contextlib.suppress(ValueError, UnicodeDecodeError):
                            current_replay_count = int(value.decode("utf-8"))
                        break

            # Enforce replay limit unless forced
            if not force and current_replay_count >= self._config.dlq_max_replay_attempts:
                event_type = self._get_header_value(message.headers, "event_type")
                logger.warning(
                    "Replay rejected: max replay attempts exceeded",
                    extra={
                        "dlq_partition": partition,
                        "dlq_offset": offset,
                        "replay_count": current_replay_count,
                        "max_replay_attempts": self._config.dlq_max_replay_attempts,
                        "event_type": event_type,
                    },
                )
                raise ValueError(
                    f"Message at partition {partition}, offset {offset} has been replayed "
                    f"{current_replay_count} times, exceeding max of "
                    f"{self._config.dlq_max_replay_attempts}. Use force=True to override."
                )

            # Build headers for republish
            original_headers: list[tuple[str, bytes]] = []
            if message.headers:
                for key, value in message.headers:
                    # Remove DLQ headers except replay count (we'll update it)
                    if not key.startswith("dlq_") and key != "retry_count":
                        original_headers.append((key, value))

            # Add retry_count header (reset to 0 for fresh attempt)
            original_headers.append(("retry_count", b"0"))

            # Increment and add replay count for loop protection
            new_replay_count = current_replay_count + 1
            original_headers.append(("dlq_replay_count", str(new_replay_count).encode("utf-8")))

            # Republish to main topic
            await self._producer.send(
                topic=self._config.topic_name,
                key=message.key,
                value=message.value,
                headers=original_headers,
            )

            logger.info(
                "DLQ message replayed",
                extra={
                    "dlq_partition": partition,
                    "dlq_offset": offset,
                    "target_topic": self._config.topic_name,
                    "event_type": self._get_header_value(original_headers, "event_type"),
                    "replay_count": new_replay_count,
                },
            )

            return True

        except TimeoutError as err:
            raise ValueError(
                f"Timeout reading message at partition {partition}, offset {offset}"
            ) from err
        finally:
            await dlq_consumer.stop()

    async def get_dlq_message_count(self) -> int:
        """Get the approximate number of messages in the DLQ.

        Uses consumer lag calculation to estimate DLQ size by comparing
        beginning and end offsets for each partition.

        Returns:
            Approximate count of DLQ messages across all partitions.

        Raises:
            RuntimeError: If not connected to Kafka.
        """
        if not self._connected:
            raise RuntimeError("Not connected to Kafka")

        dlq_consumer = AIOKafkaConsumer(
            self._config.dlq_topic_name,
            bootstrap_servers=self._config.bootstrap_servers,
            group_id=None,
            **self._get_security_config(),
        )

        total_count = 0

        try:
            await dlq_consumer.start()

            partitions = dlq_consumer.partitions_for_topic(self._config.dlq_topic_name)
            if not partitions:
                return 0

            for partition_id in partitions:
                tp = TopicPartition(self._config.dlq_topic_name, partition_id)
                dlq_consumer.assign([tp])

                # Get beginning and end offsets
                beginning = await dlq_consumer.beginning_offsets([tp])
                end = await dlq_consumer.end_offsets([tp])

                start_offset = beginning.get(tp, 0)
                end_offset = end.get(tp, 0)
                total_count += max(0, end_offset - start_offset)

        finally:
            await dlq_consumer.stop()

        return total_count


__all__ = [
    "DeserializationError",
    "EventSerializer",
    "KAFKA_AVAILABLE",
    "KafkaEventBus",
    "KafkaEventBusConfig",
    "KafkaEventBusMetrics",
    "KafkaEventBusStats",
    "KafkaNotAvailableError",
    "KafkaRebalanceListener",
]
