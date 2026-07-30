"""Kafka event bus backend.

Public import surface is identical to the pre-0.7.0 flat module
``eventsource.bus.kafka``. Internal collaborators live in sibling
modules and are not part of the public API.
"""

from eventsource.bus.kafka.bus import (
    KAFKA_AVAILABLE,
    KafkaEventBus,
    KafkaEventBusMetrics,
    KafkaRebalanceListener,
)
from eventsource.bus.kafka.config import KafkaEventBusConfig
from eventsource.bus.kafka.models import (
    DeserializationError,
    KafkaEventBusStats,
    KafkaNotAvailableError,
)
from eventsource.bus.kafka.serialization import EventSerializer

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
