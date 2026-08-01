"""Kafka event bus backend.

Public import surface is identical to the pre-0.7.0 flat module
``eventsource.bus.kafka`` (now ``eventsource.adapters.kafka``). Internal
collaborators live in sibling
modules and are not part of the public API.
"""

from eventsource.adapters.kafka.bus import (
    KAFKA_AVAILABLE,
    KafkaEventBus,
)
from eventsource.adapters.kafka.config import KafkaEventBusConfig
from eventsource.adapters.kafka.connection import KafkaRebalanceListener
from eventsource.adapters.kafka.metrics import KafkaEventBusMetrics
from eventsource.adapters.kafka.models import (
    DeserializationError,
    KafkaEventBusStats,
    KafkaNotAvailableError,
)
from eventsource.adapters.kafka.serialization import EventSerializer

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
