"""OpenTelemetry metrics for the Kafka event bus.

Holds the ``KafkaEventBusMetrics`` instrument container and the two
observable-gauge registration functions (connection status, consumer lag).
Extracted from ``bus.py`` so gauge registration can be wired up by the
facade explicitly (via ``KafkaEventBus._wire_metrics()``) instead of as a
side effect buried inside ``connect()``.
"""

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

# Pinned explicitly: __name__ is "eventsource.bus.kafka.metrics" after the
# package move, but the public logger name must stay "eventsource.bus.kafka".
logger = logging.getLogger("eventsource.bus.kafka")


class KafkaEventBusMetrics:
    """Container for Kafka event bus OpenTelemetry metric instruments.

    This class creates and holds all metric instruments used by the
    KafkaEventBus for observability. Instruments are created once at
    initialization and reused throughout the bus lifecycle.

    The metrics follow OpenTelemetry semantic conventions for messaging systems
    and use consistent attribute names across all instruments.

    Attributes:
        messages_published: Counter for messages published to Kafka.
            Attributes: messaging.system, messaging.destination, event.type
        messages_consumed: Counter for messages consumed from Kafka.
            Attributes: messaging.system, messaging.destination,
            messaging.kafka.partition, event.type
        handler_invocations: Counter for handler invocations.
            Attributes: handler.name, event.type
        handler_errors: Counter for handler errors.
            Attributes: handler.name, event.type, error.type
        dlq_messages: Counter for messages sent to dead letter queue.
            Attributes: dlq.reason, error.type
        connection_errors: Counter for connection errors.
            Attributes: error.type
        reconnections: Counter for reconnection attempts. No attributes.
        rebalances: Counter for consumer rebalances.
            Attributes: messaging.kafka.consumer_group
        publish_errors: Counter for publish errors.
            Attributes: messaging.system, messaging.destination,
            event.type, error.type
        publish_duration: Histogram for publish latency in milliseconds.
            Attributes: messaging.destination
        consume_duration: Histogram for consume/process latency in ms.
            Attributes: messaging.destination
        handler_duration: Histogram for handler execution time in ms.
            Attributes: handler.name, event.type
        batch_publish_size: Histogram for publish batch sizes (message
            count per ``publish()`` call). No attributes. Named distinctly
            from ``KafkaEventBusConfig.producer_max_batch_bytes`` (a
            producer-buffering byte threshold, unrelated) even though they
            live on the same adapter -- the OTel instrument name itself,
            ``kafka.eventbus.batch.size``, is unchanged (stable public
            telemetry schema, ADR/architecture.md); only this Python
            attribute name changed.

    Note:
        Observable gauges (connections.active, consumer.lag) are registered
        via ``register_connection_gauge()`` / ``register_consumer_lag_gauge()``
        in this module. Their once-only registration state is tracked on
        ``connection_gauge_registered`` / ``lag_gauge_registered`` below so
        that registration stays idempotent regardless of caller.

    Example:
        >>> meter = _get_meter()
        >>> if meter:
        ...     metrics = KafkaEventBusMetrics(meter)
        ...     metrics.messages_published.add(1, {"event.type": "OrderCreated"})
    """

    def __init__(self, meter: Any) -> None:
        """Initialize metric instruments.

        Args:
            meter: OpenTelemetry meter instance for creating instruments.
        """
        # Counters
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

        # Histograms
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

        # Observable-gauge registration state (idempotence tracking).
        self.connection_gauge_registered = False
        self.lag_gauge_registered = False


def register_connection_gauge(
    meter: Any,
    metrics: KafkaEventBusMetrics,
    is_connected: Callable[[], bool],
    consumer_group: str,
) -> bool:
    """Register connection status as an observable gauge.

    Reports 1 when connected, 0 when disconnected. This provides
    visibility into connection uptime and disconnection events.

    The gauge is registered once and the callback is invoked by the
    OpenTelemetry SDK at its configured collection interval.

    Args:
        meter: OpenTelemetry meter, or None/falsy if metrics are unavailable.
        metrics: The metrics container tracking registration state.
        is_connected: Callable returning the current connection status.
        consumer_group: Consumer group name attached to the gauge attributes.

    Returns:
        True if the gauge is registered (either just now or previously).
        False when no meter is available and nothing was registered.
    """
    if not meter:
        return False

    if metrics.connection_gauge_registered:
        return True  # Gauges can only be registered once

    def connection_callback(options: CallbackOptions) -> Iterable[Observation]:
        """Callback to report connection status.

        Args:
            options: Callback options from OpenTelemetry SDK.

        Yields:
            Observation with connection status (0 or 1).
        """
        yield Observation(
            1 if is_connected() else 0,
            attributes={
                "messaging.kafka.consumer_group": consumer_group,
            },
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
    """Register consumer lag as an observable gauge.

    The gauge reports lag per partition. ``lag_supplier`` is responsible
    for computing the per-partition lag (difference between high watermark
    and current position) and yielding an ``Observation`` per partition;
    it should yield nothing when lag cannot currently be determined (e.g.
    not consuming, no assignment, or mid-rebalance).

    Args:
        meter: OpenTelemetry meter, or None/falsy if metrics are unavailable.
        metrics: The metrics container tracking registration state.
        lag_supplier: Callable yielding an Observation per partition with
            its current lag.

    Returns:
        True if the gauge is registered (either just now or previously).
        False when no meter is available and nothing was registered.
    """
    if not meter:
        return False

    if metrics.lag_gauge_registered:
        return True  # Gauges can only be registered once

    def lag_callback(options: CallbackOptions) -> Iterable[Observation]:
        """Callback to report consumer lag per partition.

        Args:
            options: Callback options from OpenTelemetry SDK.

        Yields:
            Observation objects with lag values and partition attributes.
        """
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
