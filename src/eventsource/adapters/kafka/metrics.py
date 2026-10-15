"""OpenTelemetry metrics for the Kafka event bus."""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from typing import Any

try:
    from opentelemetry.metrics import CallbackOptions, Observation

    OTEL_METRICS_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised via OTEL_AVAILABLE gating
    CallbackOptions = None  # type: ignore[assignment, misc]
    Observation = None  # type: ignore[assignment, misc]
    OTEL_METRICS_AVAILABLE = False

# Public logger name stays "eventsource.bus.kafka" for backward compatibility.
logger = logging.getLogger("eventsource.bus.kafka")


class KafkaEventBusMetrics:
    """Container for Kafka event bus OpenTelemetry metric instruments."""

    def __init__(self, meter: Any) -> None:
        """Initialize metric instruments from an OpenTelemetry meter instance."""
        self.messages_published = meter.create_counter(
            name="kafka.eventbus.messages.published",
            description="Total messages published to Kafka",
            unit="messages",
        )
        self.messages_consumed = meter.create_counter(
            name="kafka.eventbus.messages.consumed",
            description="Total messages consumed from Kafka",
            unit="messages",
        )
        self.handler_invocations = meter.create_counter(
            name="kafka.eventbus.handler.invocations",
            description="Total handler invocations",
            unit="invocations",
        )
        self.handler_errors = meter.create_counter(
            name="kafka.eventbus.handler.errors",
            description="Total handler errors",
            unit="errors",
        )
        self.dlq_messages = meter.create_counter(
            name="kafka.eventbus.messages.dlq",
            description="Total messages sent to dead letter queue",
            unit="messages",
        )
        self.connection_errors = meter.create_counter(
            name="kafka.eventbus.connection.errors",
            description="Total connection errors",
            unit="errors",
        )
        self.reconnections = meter.create_counter(
            name="kafka.eventbus.reconnections",
            description="Total reconnection attempts",
            unit="reconnections",
        )
        self.rebalances = meter.create_counter(
            name="kafka.eventbus.rebalances",
            description="Total consumer rebalances",
            unit="rebalances",
        )
        self.publish_errors = meter.create_counter(
            name="kafka.eventbus.publish.errors",
            description="Total publish errors",
            unit="errors",
        )
        self.publish_duration = meter.create_histogram(
            name="kafka.eventbus.publish.duration",
            description="Time to publish messages to Kafka",
            unit="ms",
        )
        self.consume_duration = meter.create_histogram(
            name="kafka.eventbus.consume.duration",
            description="Time to process consumed messages",
            unit="ms",
        )
        self.handler_duration = meter.create_histogram(
            name="kafka.eventbus.handler.duration",
            description="Handler execution time",
            unit="ms",
        )
        self.batch_publish_size = meter.create_histogram(
            name="kafka.eventbus.batch.size",
            description="Publish batch size",
            unit="messages",
        )
        self.connection_gauge_registered = False
        self.lag_gauge_registered = False


def register_connection_gauge(
    meter: Any,
    metrics: KafkaEventBusMetrics,
    is_connected: Callable[[], bool],
    consumer_group: str,
) -> bool:
    """Register connection status as an observable gauge (1=connected, 0=disconnected)."""
    if not meter:
        return False
    if metrics.connection_gauge_registered:
        return True

    def connection_callback(options: CallbackOptions) -> Iterable[Observation]:
        yield Observation(
            1 if is_connected() else 0,
            attributes={"messaging.kafka.consumer_group": consumer_group},
        )

    meter.create_observable_gauge(
        name="kafka.eventbus.connections.active",
        callbacks=[connection_callback],
        description="Connection status (1=connected, 0=disconnected)",
        unit="connections",
    )
    metrics.connection_gauge_registered = True
    logger.debug("Connection status gauge registered")
    return True


def register_consumer_lag_gauge(
    meter: Any,
    metrics: KafkaEventBusMetrics,
    lag_supplier: Callable[[], Iterable[Observation]],
) -> bool:
    """Register per-partition consumer lag as an observable gauge."""
    if not meter:
        return False
    if metrics.lag_gauge_registered:
        return True

    def lag_callback(options: CallbackOptions) -> Iterable[Observation]:
        yield from lag_supplier()

    meter.create_observable_gauge(
        name="kafka.eventbus.consumer.lag",
        callbacks=[lag_callback],
        description="Consumer lag per partition (messages behind)",
        unit="messages",
    )
    metrics.lag_gauge_registered = True
    logger.debug("Consumer lag gauge registered")
    return True
