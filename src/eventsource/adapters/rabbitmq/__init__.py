"""RabbitMQ event bus backend.

Public import surface is identical to the pre-0.7.0 flat module
``eventsource.adapters.rabbitmq``. Internal collaborators live in sibling
modules and are not part of the public API.
"""

from eventsource.adapters.rabbitmq.bus import (
    OTEL_AVAILABLE,
    RABBITMQ_AVAILABLE,
    RabbitMQEventBus,
)
from eventsource.adapters.rabbitmq.config import RabbitMQEventBusConfig
from eventsource.adapters.rabbitmq.models import (
    BatchPublishError,
    DLQMessage,
    HealthCheckResult,
    QueueInfo,
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
