"""Pytest configuration and skip conditions for distributed tracing tests."""

from __future__ import annotations

import pytest

from eventsource import RABBITMQ_AVAILABLE
from eventsource.observability import OTEL_AVAILABLE

from ..conftest import TracingTestEvent

try:
    from eventsource.adapters.kafka import KAFKA_AVAILABLE
except ImportError:
    KAFKA_AVAILABLE = False

pytestmark = [pytest.mark.integration]

skip_if_no_rabbitmq = pytest.mark.skipif(
    not RABBITMQ_AVAILABLE,
    reason="RabbitMQ not available (aio_pika not installed)",
)

skip_if_no_kafka = pytest.mark.skipif(
    not KAFKA_AVAILABLE,
    reason="Kafka not available (aiokafka not installed)",
)

skip_if_no_otel = pytest.mark.skipif(
    not OTEL_AVAILABLE,
    reason="OpenTelemetry not available",
)

__all__ = [
    "TracingTestEvent",
    "skip_if_no_kafka",
    "skip_if_no_otel",
    "skip_if_no_rabbitmq",
]
