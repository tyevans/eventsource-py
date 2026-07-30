"""Kafka event bus backend.

Public import surface is identical to the pre-0.7.0 flat module
``eventsource.bus.kafka``. Internal collaborators live in sibling
modules and are not part of the public API.
"""

from eventsource.bus.kafka.bus import (
    KAFKA_AVAILABLE,
    DeserializationError,
    EventSerializer,
    KafkaEventBus,
    KafkaEventBusConfig,
    KafkaEventBusMetrics,
    KafkaEventBusStats,
    KafkaNotAvailableError,
    KafkaRebalanceListener,
)

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
