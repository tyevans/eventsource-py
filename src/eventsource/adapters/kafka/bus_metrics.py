"""Metrics and observable gauge mixin for KafkaEventBus.

Governed by ADR-0002 (<500 lines per module).
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from eventsource.adapters.kafka.config import KafkaEventBusConfig
    from eventsource.adapters.kafka.connection import KafkaConnectionManager
    from eventsource.adapters.kafka.consumer import KafkaConsumerLoop
    from eventsource.adapters.kafka.metrics import KafkaEventBusMetrics

try:
    from opentelemetry.metrics import Observation
except ImportError:
    Observation = None  # type: ignore[assignment, misc]

logger = logging.getLogger("eventsource.bus.kafka")


class KafkaBusMetricsMixin:
    """Mixin providing OpenTelemetry metrics wiring and observable gauges."""

    _config: KafkaEventBusConfig
    _connection_manager: KafkaConnectionManager
    _consumer_loop: KafkaConsumerLoop
    _metrics: KafkaEventBusMetrics | None
    _meter: Any
    _connection_gauge_registered: bool
    _lag_gauge_registered: bool
    _connected: bool
    _consuming: bool
    _consumer: Any

    def _wire_metrics(self) -> None:
        """Register observable gauges after a successful connect.

        Called once from ``connect()`` after the connection is fully
        established. Kept separate from ``connect()`` itself so gauge
        registration is never a side effect of a failed connection attempt.
        """
        self._register_connection_gauge()
        self._register_consumer_lag_gauge()

    def _register_connection_gauge(self) -> None:
        """Register connection status as an observable gauge.

        Reports 1 when connected, 0 when disconnected. This provides
        visibility into connection uptime and disconnection events.
        """
        if not self._meter or not self._config.enable_metrics or self._metrics is None:
            return

        import eventsource.adapters.kafka.bus as bus_mod

        self._connection_gauge_registered = bus_mod.register_connection_gauge(
            self._meter,
            self._metrics,
            lambda: self._connected,
            self._config.consumer_group,
        )

    def _register_consumer_lag_gauge(self) -> None:
        """Register consumer lag as an observable gauge.

        The gauge reports lag per partition, calculated as the difference
        between the high watermark (latest offset) and the current position.
        """
        if not self._meter or not self._config.enable_metrics or self._metrics is None:
            return

        import eventsource.adapters.kafka.bus as bus_mod

        self._lag_gauge_registered = bus_mod.register_consumer_lag_gauge(
            self._meter,
            self._metrics,
            self._lag_observations,
        )

    def _lag_observations(self) -> Iterable[Observation]:
        """Compute per-partition consumer lag observations.

        Yields:
            Observation objects with lag values and partition attributes.
            Yields nothing when not consuming, mid-rebalance, or in an
            invalid consumer state.
        """
        if not self._consuming or not self._consumer or not self._connected:
            return

        try:
            assignment = self._consumer.assignment()
            if not assignment:
                return

            for tp in assignment:
                try:
                    highwater = self._consumer.highwater(tp)
                    position = self._consumer.position(tp)

                    if highwater is not None and position is not None:
                        lag = max(0, highwater - position)
                        if Observation is not None:
                            yield Observation(
                                lag,
                                attributes={
                                    "messaging.kafka.partition": tp.partition,
                                    "messaging.kafka.consumer_group": self._config.consumer_group,
                                    "messaging.destination": tp.topic,
                                },
                            )
                except Exception as e:
                    logger.debug(
                        "Skipping partition %s lag metric due to error: %s",
                        tp,
                        e,
                    )
        except Exception as e:
            logger.debug("Unable to collect consumer lag metrics: %s", e)
