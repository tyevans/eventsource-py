"""RabbitMQ event bus implementation using aio-pika.

This module provides a distributed event bus implementation using RabbitMQ
for durable event distribution across multiple processes and servers.

Features:
- Durable event storage (events survive restarts)
- Consumer groups via queue bindings
- At-least-once delivery guarantees
- Topic-based routing with exchange types
- Horizontal scaling support
- Dead letter queue for unrecoverable failures
- Configurable retry policies
- Optional OpenTelemetry tracing

Example:
    >>> from eventsource.adapters.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig
    >>>
    >>> config = RabbitMQEventBusConfig(
    ...     rabbitmq_url="amqp://guest:guest@localhost:5672/",
    ...     exchange_name="events",
    ...     consumer_group="projections",
    ... )
    >>> bus = RabbitMQEventBus(config=config, event_registry=my_registry)
    >>> await bus.connect()
    >>> await bus.publish([MyEvent(...)])
    >>> await bus.start_consuming()
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from eventsource.adapters._bus.base import BaseEventBus
from eventsource.adapters._bus.retry import RetryPolicy
from eventsource.adapters.rabbitmq.bus_connection import RabbitMQEventBusConnectionMixin
from eventsource.adapters.rabbitmq.bus_consume import RabbitMQEventBusConsumeMixin
from eventsource.adapters.rabbitmq.bus_dlq import RabbitMQEventBusDLQMixin
from eventsource.adapters.rabbitmq.bus_health import RabbitMQEventBusHealthMixin
from eventsource.adapters.rabbitmq.bus_publish import RabbitMQEventBusPublishMixin
from eventsource.adapters.rabbitmq.bus_shutdown import RabbitMQEventBusShutdownMixin
from eventsource.adapters.rabbitmq.bus_topology import RabbitMQEventBusTopologyMixin
from eventsource.adapters.rabbitmq.config import RabbitMQEventBusConfig
from eventsource.adapters.rabbitmq.connection import RabbitMQConnectionManager
from eventsource.adapters.rabbitmq.consumer import RabbitMQConsumer
from eventsource.adapters.rabbitmq.dlq import RabbitMQDLQAdmin
from eventsource.adapters.rabbitmq.models import (
    BatchPublishError,
    DLQMessage,
    HealthCheckResult,
    QueueInfo,
    RabbitMQEventBusStats,
    RabbitMQNotAvailableError,
    ShutdownError,
)
from eventsource.adapters.rabbitmq.publisher import RabbitMQPublisher
from eventsource.adapters.rabbitmq.topology import RabbitMQTopology
from eventsource.observability import OTEL_AVAILABLE, Tracer, create_tracer

if TYPE_CHECKING:
    from eventsource.domain.event_registry import EventRegistry

# Optional aio-pika import - fail gracefully if not installed
try:
    import aio_pika
    from aio_pika import DeliveryMode, Message
    from aio_pika.abc import (
        AbstractChannel,
        AbstractConnection,
        AbstractExchange,
        AbstractIncomingMessage,
        AbstractQueue,
        AbstractRobustChannel,
        AbstractRobustConnection,
    )

    RABBITMQ_AVAILABLE = True
except ImportError:
    RABBITMQ_AVAILABLE = False
    aio_pika = None  # type: ignore[assignment]
    Message = None  # type: ignore[assignment, misc]
    DeliveryMode = None  # type: ignore[assignment, misc]
    AbstractChannel = None  # type: ignore[assignment, misc]
    AbstractConnection = None  # type: ignore[assignment, misc]
    AbstractExchange = None  # type: ignore[assignment, misc]
    AbstractIncomingMessage = None  # type: ignore[assignment, misc]
    AbstractQueue = None  # type: ignore[assignment, misc]
    AbstractRobustChannel = None  # type: ignore[assignment, misc]
    AbstractRobustConnection = None  # type: ignore[assignment, misc]


# Named explicitly (not via __name__) so the logger name is stable across the
# rabbitmq.py -> rabbitmq/bus.py package move -- callers that configure
# logging by name ("eventsource.adapters.rabbitmq") keep working unchanged.
logger = logging.getLogger("eventsource.adapters.rabbitmq")


class RabbitMQEventBus(
    RabbitMQEventBusConnectionMixin,
    RabbitMQEventBusTopologyMixin,
    RabbitMQEventBusPublishMixin,
    RabbitMQEventBusConsumeMixin,
    RabbitMQEventBusDLQMixin,
    RabbitMQEventBusHealthMixin,
    RabbitMQEventBusShutdownMixin,
    BaseEventBus,
):
    """Event bus implementation using RabbitMQ.

    This implementation provides distributed event delivery with:
    - At-least-once delivery guarantees via message acknowledgments
    - Horizontal scaling via consumer groups
    - Automatic reconnection via aio-pika's RobustConnection
    - Dead letter queue for failed messages
    - Topic-based routing with configurable exchange types

    Thread Safety:
        - Subscription methods are thread-safe
        - Publishing and consuming should only be called from async context

    Example:
        >>> from eventsource.adapters.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig
        >>> from eventsource.domain.event_registry import EventRegistry
        >>>
        >>> config = RabbitMQEventBusConfig(rabbitmq_url="amqp://localhost:5672")
        >>> registry = EventRegistry()
        >>> bus = RabbitMQEventBus(config=config, event_registry=registry)
        >>>
        >>> async with bus:
        ...     bus.subscribe(OrderCreated, order_handler)
        ...     await bus.publish([OrderCreated(...)])
        ...     await bus.start_consuming()
    """

    def __init__(
        self,
        config: RabbitMQEventBusConfig | None = None,
        event_registry: EventRegistry | None = None,
        *,
        tracer: Tracer | None = None,
    ) -> None:
        """Initialize the RabbitMQ event bus.

        Args:
            config: Configuration for the RabbitMQ event bus.
                   Defaults to RabbitMQEventBusConfig() with default values.
            event_registry: Event registry for deserializing events.
                          If None, uses the default registry.
            tracer: Optional custom Tracer instance. If not provided, one is
                   created based on config.enable_tracing setting.

        Raises:
            RabbitMQNotAvailableError: If aio-pika package is not installed
        """
        if not RABBITMQ_AVAILABLE:
            raise RabbitMQNotAvailableError()

        super().__init__(event_registry=event_registry)

        self._config = config or RabbitMQEventBusConfig()

        # Statistics
        self._stats = RabbitMQEventBusStats()

        # Connection lifecycle (connection/channel/connect-lock/reconnect
        # and close callbacks) is owned by RabbitMQConnectionManager.
        self._connection_manager = RabbitMQConnectionManager(config=self._config, stats=self._stats)
        self._connection_manager._is_consuming = lambda: self._consumer._consuming

        # Exchange/queue declaration and bindings are owned by
        # RabbitMQTopology. Re-declaration after a reconnect is wired
        # through the connection manager's reconnect hook.
        self._topology = RabbitMQTopology(config=self._config, connection=self._connection_manager)
        self._connection_manager.on_reconnect(self._topology.redeclare)

        self._retry_policy = RetryPolicy(
            base_delay=self._config.retry_base_delay,
            max_delay=self._config.retry_max_delay,
            jitter=self._config.retry_jitter,
            max_retries=self._config.max_retries,
        )

        # Shutdown state tracking
        self._shutdown_initiated: bool = False

        # Logger (named explicitly -- see module-level `logger` for rationale)
        self._logger = logging.getLogger("eventsource.adapters.rabbitmq")

        # Initialize tracing via composition (replaces TracingMixin)
        tracer_instance = tracer or create_tracer(__name__, self._config.enable_tracing)

        # Publish-path (single/batch, sequential/concurrent strategies) is
        # owned by RabbitMQPublisher.
        self._publisher = RabbitMQPublisher(
            config=self._config,
            connection=self._connection_manager,
            topology=self._topology,
            stats=self._stats,
            tracer=tracer_instance,
            enable_tracing=tracer_instance.enabled,
        )

        # Consume path (consume loop, dispatch, retry/DLQ write path,
        # graceful stop/drain) is owned by RabbitMQConsumer. It receives
        # handler lookup / event resolution as callables so it never touches
        # the subscription registry directly.
        self._consumer = RabbitMQConsumer(
            config=self._config,
            connection=self._connection_manager,
            topology=self._topology,
            stats=self._stats,
            retry_policy=self._retry_policy,
            handlers_for=self._handlers_for,
            resolve_event_class=self._resolve_event_class,
            tracer=tracer_instance,
            enable_tracing=tracer_instance.enabled,
        )

        # DLQ inspection/replay/purge is owned by RabbitMQDLQAdmin.
        self._dlq_admin = RabbitMQDLQAdmin(
            config=self._config,
            connection=self._connection_manager,
            topology=self._topology,
            stats=self._stats,
        )

    @property
    def config(self) -> RabbitMQEventBusConfig:
        """Get the configuration."""
        return self._config

    @property
    def is_connected(self) -> bool:
        """Check if connected to RabbitMQ.

        Returns True only if the connection is established and not closed.
        """
        return self._connection_manager.is_connected

    @property
    def is_consuming(self) -> bool:
        """Check if currently consuming events."""
        return self._consumer.is_consuming


__all__ = [
    "BatchPublishError",
    "DLQMessage",
    "HealthCheckResult",
    "OTEL_AVAILABLE",
    "QueueInfo",
    "RabbitMQEventBus",
    "RabbitMQEventBusConfig",
    "RabbitMQEventBusStats",
    "RabbitMQNotAvailableError",
    "RABBITMQ_AVAILABLE",
    "ShutdownError",
]
