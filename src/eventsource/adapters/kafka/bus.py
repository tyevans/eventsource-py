"""Kafka event bus implementation using aiokafka (ADR-0002, ADR-0007)."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from types import TracebackType
from typing import TYPE_CHECKING, Any

from eventsource.adapters._bus.base import DEFAULT_MAX_BACKGROUND_TASKS, BaseEventBus
from eventsource.adapters._bus.retry import RetryPolicy
from eventsource.adapters.kafka.bus_consume import KafkaBusConsumeMixin
from eventsource.adapters.kafka.bus_dlq import KafkaBusDLQMixin
from eventsource.adapters.kafka.bus_metrics import KafkaBusMetricsMixin
from eventsource.adapters.kafka.bus_publish import KafkaBusPublishMixin
from eventsource.adapters.kafka.config import KafkaEventBusConfig
from eventsource.adapters.kafka.connection import KafkaConnectionManager
from eventsource.adapters.kafka.consumer import KafkaConsumerLoop
from eventsource.adapters.kafka.dlq import KafkaDLQAdmin
from eventsource.adapters.kafka.metrics import (  # fmt: skip
    KafkaEventBusMetrics,
    register_connection_gauge,
    register_consumer_lag_gauge,
)
from eventsource.adapters.kafka.models import (  # fmt: skip
    DeserializationError,
    KafkaEventBusStats,
    KafkaNotAvailableError,
)
from eventsource.adapters.kafka.publisher import KafkaPublisher
from eventsource.adapters.kafka.serialization import EventSerializer
from eventsource.observability import OTEL_AVAILABLE, Tracer, create_tracer
from eventsource.ports.exceptions import EventBusConnectionError

if TYPE_CHECKING:
    from eventsource.domain.event_registry import EventRegistry

try:
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
    from aiokafka.errors import KafkaError

    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    AIOKafkaProducer = AIOKafkaConsumer = None
    KafkaError = Exception

try:
    from opentelemetry import metrics as otel_metrics
except ImportError:
    otel_metrics = None  # type: ignore[assignment]

logger = logging.getLogger("eventsource.bus.kafka")
_meter: Any = None


def _get_meter() -> Any:
    global _meter
    if OTEL_AVAILABLE and _meter is None and otel_metrics is not None:
        _meter = otel_metrics.get_meter("eventsource.bus.kafka")
    return _meter if OTEL_AVAILABLE else None


class KafkaEventBus(KafkaBusPublishMixin, KafkaBusConsumeMixin, KafkaBusDLQMixin, KafkaBusMetricsMixin, BaseEventBus):  # fmt: skip
    """Kafka implementation of the EventBus interface with consumer groups, DLQ, and tracing."""

    def __init__(self, config: KafkaEventBusConfig | None = None, event_registry: EventRegistry | None = None, serializer: EventSerializer | None = None, *, tracer: Tracer | None = None, max_background_tasks: int | None = DEFAULT_MAX_BACKGROUND_TASKS) -> None:  # fmt: skip
        if not KAFKA_AVAILABLE:
            raise KafkaNotAvailableError()
        super().__init__(event_registry=event_registry, max_background_tasks=max_background_tasks)
        cfg, self._serializer = config or KafkaEventBusConfig(), serializer or EventSerializer()
        self._config, tr = cfg, tracer or create_tracer(__name__, cfg.enable_tracing)
        self._tracer, et = tr, tr.enabled
        self._enable_tracing = et
        rp = self._retry_policy = RetryPolicy(cfg.retry_base_delay, cfg.retry_max_delay, cfg.retry_jitter, cfg.max_retries)  # fmt: skip
        self._stats, m = KafkaEventBusStats(), _get_meter() if cfg.enable_metrics else None
        self._meter, metrics = m, KafkaEventBusMetrics(m) if m else None
        self._metrics_instance = metrics
        self._connection_manager = KafkaConnectionManager(cfg, self._stats, metrics)
        deps = (cfg, self._connection_manager, self._serializer, self._stats)
        self._publisher, se = KafkaPublisher(*deps, metrics, tr, et), asyncio.Event()
        self._shutdown_event = se
        self._connection_gauge_registered = self._lag_gauge_registered = False
        hf, rec, lag = self._handlers_for, self._resolve_event_class, self._register_consumer_lag_gauge  # fmt: skip
        self._consumer_loop = KafkaConsumerLoop(*deps, metrics, rp, hf, rec, tr, et, se, lag)
        self._dlq_admin = KafkaDLQAdmin(*deps)
        logger.debug("KafkaEventBus initialized", extra=cfg.get_sanitized_config())

    @property
    def is_connected(self) -> bool: return self._connection_manager.is_connected  # fmt: skip

    @property
    def is_consuming(self) -> bool: return self._consumer_loop.is_consuming  # fmt: skip

    @property
    def config(self) -> KafkaEventBusConfig: return self._config  # fmt: skip

    @property
    def stats(self) -> KafkaEventBusStats: return self._stats  # fmt: skip

    async def connect(self) -> None:
        await self._connection_manager.connect()
        if self._connection_manager.is_connected:
            self._wire_metrics()

    async def disconnect(self) -> None:
        if not self._connection_manager.is_connected:
            logger.debug("KafkaEventBus not connected, nothing to disconnect")
            return
        if self._consumer_loop.is_consuming:
            await self.stop_consuming()
        await self._connection_manager.disconnect()

    async def __aenter__(self) -> KafkaEventBus:
        await self.connect()
        return self

    async def __aexit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None) -> None:  # fmt: skip
        await self.shutdown(timeout=self._config.shutdown_timeout)

    async def shutdown(self, timeout: float | None = None) -> None:
        timeout = timeout or self._config.shutdown_timeout
        logger.info("Shutting down KafkaEventBus", extra={"timeout": timeout})
        self._shutdown_event.set()
        if (task := self._consumer_loop._consume_task) and not task.done():
            try:
                await asyncio.wait_for(task, timeout=timeout)
            except TimeoutError:
                logger.warning("Shutdown timed out, cancelling consume task")
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        await self._drain_background(timeout or self._config.shutdown_timeout)
        await self.disconnect()
        logger.info("KafkaEventBus shutdown complete")

    def _get_security_config(self) -> dict[str, Any]: return self._connection_manager.get_security_config()  # fmt: skip

    def get_stats_dict(self) -> dict[str, Any]: return self._stats.get_stats_dict()  # fmt: skip

    async def get_topic_info(self) -> dict[str, Any]:
        if not self.is_connected or not (consumer := self._connection_manager.consumer):
            raise EventBusConnectionError("Not connected to Kafka")
        partitions = consumer.partitions_for_topic(self._config.topic_name)
        parts, t, g = list(partitions) if partitions else [], self._config.topic_name, self._config.consumer_group  # fmt: skip
        return {"topic": t, "partitions": parts, "consumer_group": g, "connected": self.is_connected, "consuming": self.is_consuming}  # fmt: skip


__all__ = ["DeserializationError", "EventSerializer", "KAFKA_AVAILABLE", "KafkaEventBus", "KafkaEventBusConfig", "KafkaEventBusMetrics", "KafkaEventBusStats", "KafkaNotAvailableError", "OTEL_AVAILABLE", "_get_meter", "register_connection_gauge", "register_consumer_lag_gauge"]  # fmt: skip
