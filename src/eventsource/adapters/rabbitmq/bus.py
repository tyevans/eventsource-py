"""RabbitMQ event bus implementation using aio-pika."""

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
    """Distributed event bus implementation using RabbitMQ and aio-pika."""

    def __init__(
        self,
        config: RabbitMQEventBusConfig | None = None,
        event_registry: EventRegistry | None = None,
        *,
        tracer: Tracer | None = None,
    ) -> None:
        """Initialize the RabbitMQ event bus."""
        if not RABBITMQ_AVAILABLE:
            raise RabbitMQNotAvailableError()

        super().__init__(event_registry=event_registry)

        self._config = config or RabbitMQEventBusConfig()
        self._stats = RabbitMQEventBusStats()

        self._connection_manager = RabbitMQConnectionManager(config=self._config, stats=self._stats)
        self._connection_manager._is_consuming = lambda: self._consumer._consuming

        self._topology = RabbitMQTopology(config=self._config, connection=self._connection_manager)
        self._connection_manager.on_reconnect(self._topology.redeclare)

        self._retry_policy = RetryPolicy(
            base_delay=self._config.retry_base_delay,
            max_delay=self._config.retry_max_delay,
            jitter=self._config.retry_jitter,
            max_retries=self._config.max_retries,
        )

        self._shutdown_initiated: bool = False
        self._logger = logging.getLogger("eventsource.adapters.rabbitmq")

        tracer_instance = tracer or create_tracer(__name__, self._config.enable_tracing)

        self._publisher = RabbitMQPublisher(
            config=self._config,
            connection=self._connection_manager,
            topology=self._topology,
            stats=self._stats,
            tracer=tracer_instance,
            enable_tracing=tracer_instance.enabled,
        )

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
        """Check if connected to RabbitMQ."""
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
