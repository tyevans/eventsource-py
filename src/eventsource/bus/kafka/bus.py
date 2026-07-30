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
from eventsource.bus.kafka.consumer import KafkaConsumerLoop
from eventsource.bus.kafka.dlq import KafkaDLQAdmin
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
from eventsource.bus.kafka.publisher import KafkaPublisher
from eventsource.bus.kafka.serialization import EventSerializer
from eventsource.bus.retry import RetryPolicy
from eventsource.events.base import DomainEvent
from eventsource.handlers.adapter import HandlerAdapter
from eventsource.observability import OTEL_AVAILABLE, Tracer, create_tracer

if TYPE_CHECKING:
    from eventsource.events.registry import EventRegistry

# Optional aiokafka import - fail gracefully if not installed
try:
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
    from aiokafka.errors import KafkaError

    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    AIOKafkaProducer = None
    AIOKafkaConsumer = None
    KafkaError = Exception

# OpenTelemetry metrics imports - kept separate from TracingMixin. These are
# NOT in TracingMixin and must be imported directly for the meter and the
# observable-gauge callbacks. Context propagation (inject/extract) now lives
# with the publisher and consumer collaborators that use it.
try:
    from opentelemetry import metrics as otel_metrics
    from opentelemetry.metrics import Observation
except ImportError:
    otel_metrics = None  # type: ignore[assignment]
    Observation = None  # type: ignore[assignment, misc]

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

        # Split-phase send/ack publish mechanics are delegated to
        # KafkaPublisher; the facade's publish() remains the orchestrator
        # (guard rails, timing, and aggregate statistics).
        self._publisher = KafkaPublisher(
            config=self._config,
            connection=self._connection_manager,
            serializer=self._serializer,
            stats=self._stats,
            metrics=metrics,
            tracer=self._tracer,
            enable_tracing=self._enable_tracing,
        )

        # Track if gauges are registered (gauges can only be registered once)
        self._connection_gauge_registered = False
        self._lag_gauge_registered = False

        # Shutdown coordination
        self._shutdown_event = asyncio.Event()

        # The poll/dispatch/commit loop, plus its retry and DLQ error paths,
        # is delegated to KafkaConsumerLoop. Handler lookup and event-class
        # resolution are passed as bound methods so the loop never touches
        # the event registry itself.
        self._consumer_loop = KafkaConsumerLoop(
            config=self._config,
            connection=self._connection_manager,
            serializer=self._serializer,
            stats=self._stats,
            metrics=metrics,
            retry_policy=self._retry_policy,
            handlers_for=self._handlers_for,
            resolve_event_class=self._resolve_event_class,
            tracer=self._tracer,
            enable_tracing=self._enable_tracing,
            shutdown_event=self._shutdown_event,
            on_start=self._register_consumer_lag_gauge,
        )

        # Dead letter queue inspection, replay, and counting are delegated
        # to KafkaDLQAdmin, which owns the throwaway-consumer lifecycle for
        # ad-hoc DLQ reads.
        self._dlq_admin = KafkaDLQAdmin(
            config=self._config,
            connection=self._connection_manager,
            serializer=self._serializer,
            stats=self._stats,
        )

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
        self._publisher._metrics = value
        self._consumer_loop._metrics = value

    @property
    def is_consuming(self) -> bool:
        """Check if actively consuming messages.

        Returns:
            True if consume loop is running.
        """
        return self._consumer_loop.is_consuming

    @property
    def _consuming(self) -> bool:
        """Internal alias for ``is_consuming``, delegating to the consume loop.

        Kept as a settable property because tests reach into
        ``bus._consuming`` directly to simulate an active consume loop.
        """
        return self._consumer_loop.is_consuming

    @_consuming.setter
    def _consuming(self, value: bool) -> None:
        self._consumer_loop._consuming = value

    @property
    def _consume_task(self) -> asyncio.Task[None] | None:
        """Internal alias delegating to the consume loop's background task."""
        return self._consumer_loop._consume_task

    @_consume_task.setter
    def _consume_task(self, value: asyncio.Task[None] | None) -> None:
        self._consumer_loop._consume_task = value

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
        await self._consumer_loop.stop()

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

        # Split-phase send/ack mechanics (sequential send handoff preserving
        # per-aggregate ordering, then batched ack-await or background
        # tracking) are delegated to KafkaPublisher.
        await self._publisher.publish_all(events, background)

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

    def _serialize_event(self, event: DomainEvent) -> bytes:
        """Serialize an event to bytes using the configured serializer.

        Deprecated:
            Thin delegating shim kept because tests reach into
            ``bus._serialize_event`` directly. Prefer ``KafkaPublisher``'s
            own ``_serialize_event`` for new code.

        Args:
            event: The domain event to serialize.

        Returns:
            Serialized event as bytes (format depends on serializer).
        """
        return self._publisher._serialize_event(event)

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
        await self._consumer_loop.start(auto_reconnect=auto_reconnect)

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
        return self._consumer_loop.start_in_background()

    # =========================================================================
    # Consume-side Delegating Shims
    # =========================================================================
    # Thin pass-throughs to KafkaConsumerLoop, kept because existing tests --
    # and this facade's own DLQ inspection helpers -- reach into these private
    # names directly. Prefer the KafkaConsumerLoop methods for new code.

    async def _process_message(self, message: Any) -> None:
        """Delegate a single message to the consume loop."""
        await self._consumer_loop._process_message(message)

    def _deserialize_message(self, message: Any) -> DomainEvent:
        """Delegate message deserialization to the consume loop."""
        return self._consumer_loop._deserialize_message(message)

    def _get_header_value(
        self,
        headers: list[tuple[str, bytes]] | None,
        key: str,
    ) -> str | None:
        """Delegate header lookup to the consume loop."""
        return self._consumer_loop._get_header_value(headers, key)

    def _get_retry_count(self, headers: list[tuple[str, bytes]] | None) -> int:
        """Delegate retry-count extraction to the consume loop."""
        return self._consumer_loop._get_retry_count(headers)

    async def _dispatch_to_handlers(
        self,
        event: DomainEvent,
        handlers: tuple[HandlerAdapter, ...],
    ) -> None:
        """Delegate handler dispatch to the consume loop.

        Raises:
            HandlerDispatchError: If one or more handlers raise.
        """
        await self._consumer_loop._dispatch_to_handlers(event, handlers)

    def _calculate_retry_delay(self, retry_count: int) -> float:
        """Delegate retry-delay calculation to the consume loop."""
        return self._consumer_loop._calculate_retry_delay(retry_count)

    async def _send_to_dlq(
        self,
        message: Any,
        error: Exception,
        retry_count: int,
        reason: str = "max_retries_exceeded",
    ) -> None:
        """Delegate DLQ routing to the consume loop."""
        await self._consumer_loop._send_to_dlq(message, error, retry_count, reason)

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
        return await self._dlq_admin.get_messages(
            limit=limit,
            timeout_ms=timeout_ms,
            use_consumer_group=use_consumer_group,
        )

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
        return await self._dlq_admin.replay_message(
            partition=partition,
            offset=offset,
            force=force,
        )

    async def get_dlq_message_count(self) -> int:
        """Get the approximate number of messages in the DLQ.

        Uses consumer lag calculation to estimate DLQ size by comparing
        beginning and end offsets for each partition.

        Returns:
            Approximate count of DLQ messages across all partitions.

        Raises:
            RuntimeError: If not connected to Kafka.
        """
        return await self._dlq_admin.get_message_count()


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
