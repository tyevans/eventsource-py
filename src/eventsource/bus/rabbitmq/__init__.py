"""RabbitMQ event bus backend.

Public import surface is identical to the pre-0.7.0 flat module
``eventsource.bus.rabbitmq``. Internal collaborators live in sibling
modules and are not part of the public API.
"""

from eventsource.bus.rabbitmq.bus import (
    OTEL_AVAILABLE,
    RABBITMQ_AVAILABLE,
    BatchPublishError,
    DLQMessage,
    HealthCheckResult,
    QueueInfo,
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
    RabbitMQEventBusStats,
    RabbitMQNotAvailableError,
    ShutdownError,
)

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
