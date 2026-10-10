"""Kafka event bus implementation using aiokafka.

This module provides a distributed event bus implementation using Apache Kafka
for high-throughput event distribution across multiple processes and servers.

Governed by:
- ADR-0002 (<500 lines per module)
- ADR-0007 (Hexagonal Architecture)
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from types import TracebackType
from typing import TYPE_CHECKING, Any

from eventsource.adapters._bus.base import DEFAULT_MAX_BACKGROUND_TASKS, BaseEventBus
from eventsource.adapters._bus.retry import RetryPolicy
from eventsource.adapters.kafka.bus_consume import KafkaBusConsumeMixin
from eventsource.adapters.kafka.bus_dlq import KafkaBusDLQMixin
from eventsource.adapters.kafka.bus_metrics import KafkaBusMetricsMixin
from eventsource.adapters.kafka.bus_publish import KafkaBusPublishMixin
from eventsource.adapters.kafka.config import KafkaEventBusConfig
from eventsource.adapters.kafka.connection import (
    KafkaConnectionManager,
    KafkaRebalanceListener,
)
from eventsource.adapters.kafka.consumer import KafkaConsumerLoop
from eventsource.adapters.kafka.dlq import KafkaDLQAdmin
from eventsource.adapters.kafka.metrics import (
    KafkaEventBusMetrics,
    register_connection_gauge,
    register_consumer_lag_gauge,
)
from eventsource.adapters.kafka.models import (
    DeserializationError,
    KafkaEventBusStats,
    KafkaNotAvailableError,
)
from eventsource.adapters.kafka.publisher import KafkaPublisher
from eventsource.adapters.kafka.serialization import EventSerializer
from eventsource.observability import OTEL_AVAILABLE, Tracer, create_tracer
from eventsource.ports.exceptions import EventBusConnectionError

if TYPE_CHECKING:
    from eventsource.domain.event_registry import EventRegistry

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

try:
    from opentelemetry import metrics as otel_metrics
except ImportError:
    otel_metrics = None  # type: ignore[assignment]

logger = logging.getLogger("eventsource.bus.kafka")

_meter: Any = None


def _get_meter() -> Any:
    """Get or create the OpenTelemetry meter.

    Returns a meter instance for creating metric instruments. The meter is
    lazily initialized on first use and cached for subsequent calls.
    """
    global _meter
    if not OTEL_AVAILABLE:
        return None
    if _meter is None and otel_metrics is not None:
        _meter = otel_metrics.get_meter("eventsource.bus.kafka")
    return _meter


class KafkaEventBus(
    KafkaBusPublishMixin,
    KafkaBusConsumeMixin,
    KafkaBusDLQMixin,
    KafkaBusMetricsMixin,
    BaseEventBus,
):
    """Kafka implementation of the EventBus interface.

    Provides a distributed event bus using Apache Kafka for high-throughput
    event distribution. Supports consumer groups, dead letter queues, and
    optional OpenTelemetry tracing.
    """

    def __init__(
        self,
        config: KafkaEventBusConfig | None = None,
        event_registry: EventRegistry | None = None,
        serializer: EventSerializer | None = None,
        *,
        tracer: Tracer | None = None,
        max_background_tasks: int | None = DEFAULT_MAX_BACKGROUND_TASKS,
    ) -> None:
        """Initialize the Kafka event bus.

        Raises:
            KafkaNotAvailableError: If aiokafka is not installed.
        """
        if not KAFKA_AVAILABLE:
            raise KafkaNotAvailableError()

        super().__init__(
            event_registry=event_registry,
            max_background_tasks=max_background_tasks,
        )

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
        self._meter: Any = None
        if self._config.enable_metrics:
            self._meter = _get_meter()
            if self._meter:
                metrics = KafkaEventBusMetrics(self._meter)

        self._metrics_instance = metrics

        # Connection lifecycle
        self._connection_manager = KafkaConnectionManager(
            config=self._config,
            stats=self._stats,
            metrics=metrics,
        )

        # Split-phase send/ack publish mechanics
        self._publisher = KafkaPublisher(
            config=self._config,
            connection=self._connection_manager,
            serializer=self._serializer,
            stats=self._stats,
            metrics=metrics,
            tracer=self._tracer,
            enable_tracing=self._enable_tracing,
        )

        # Track if gauges are registered
        self._connection_gauge_registered = False
        self._lag_gauge_registered = False

        # Shutdown coordination
        self._shutdown_event = asyncio.Event()

        # Consumer loop
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

        # DLQ administration
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
        """Check if connected to Kafka."""
        return self._connection_manager.is_connected

    @property
    def _connected(self) -> bool:
        """Internal alias for ``is_connected``."""
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
        """Check if actively consuming messages."""
        return self._consumer_loop.is_consuming

    @property
    def _consuming(self) -> bool:
        """Internal alias for ``is_consuming``."""
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
        """Get the configuration."""
        return self._config

    @property
    def stats(self) -> KafkaEventBusStats:
        """Get current statistics."""
        return self._stats

    # =========================================================================
    # Connection Lifecycle Methods
    # =========================================================================

    async def connect(self) -> None:
        """Connect to Kafka cluster."""
        await self._connection_manager.connect()

        if self._connection_manager.is_connected:
            self._wire_metrics()

    async def disconnect(self) -> None:
        """Disconnect from Kafka cluster."""
        if not self._connection_manager.is_connected:
            logger.debug("KafkaEventBus not connected, nothing to disconnect")
            return

        if self._consuming:
            await self.stop_consuming()

        await self._connection_manager.disconnect()

    async def __aenter__(self) -> KafkaEventBus:
        """Enter async context manager."""
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit async context manager."""
        await self.shutdown(timeout=self._config.shutdown_timeout)

    async def shutdown(self, timeout: float | None = None) -> None:
        """Gracefully shutdown the event bus."""
        timeout = timeout or self._config.shutdown_timeout

        logger.info("Shutting down KafkaEventBus", extra={"timeout": timeout})

        self._shutdown_event.set()

        if self._consume_task and not self._consume_task.done():
            try:
                await asyncio.wait_for(self._consume_task, timeout=timeout)
            except TimeoutError:
                logger.warning("Shutdown timed out, cancelling consume task")
                self._consume_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._consume_task

        await self._drain_background(timeout or self._config.shutdown_timeout)
        await self.disconnect()

        logger.info("KafkaEventBus shutdown complete")

    def _get_security_config(self) -> dict[str, Any]:
        """Get security configuration for additional consumers."""
        return self._connection_manager.get_security_config()

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def get_stats_dict(self) -> dict[str, Any]:
        """Get statistics as a dictionary."""
        return self._stats.get_stats_dict()

    async def get_topic_info(self) -> dict[str, Any]:
        """Get information about the configured topic."""
        if not self._connected or not self._consumer:
            raise EventBusConnectionError("Not connected to Kafka")

        partitions = self._consumer.partitions_for_topic(self._config.topic_name)

        return {
            "topic": self._config.topic_name,
            "partitions": list(partitions) if partitions else [],
            "consumer_group": self._config.consumer_group,
            "connected": self._connected,
            "consuming": self._consuming,
        }


__all__ = [
    "DeserializationError",
    "EventSerializer",
    "KAFKA_AVAILABLE",
    "KafkaEventBus",
    "KafkaEventBusConfig",
    "KafkaEventBusMetrics",
    "KafkaEventBusStats",
    "KafkaNotAvailableError",
    "OTEL_AVAILABLE",
    "_get_meter",
    "register_connection_gauge",
    "register_consumer_lag_gauge",
]
