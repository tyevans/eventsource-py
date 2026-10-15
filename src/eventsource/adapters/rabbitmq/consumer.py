"""Consume loop, handler dispatch, and retry/DLQ write path for the RabbitMQ bus."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any

from eventsource.adapters._bus.retry_scheduler import RetryScheduler
from eventsource.adapters.rabbitmq.consumer_dispatch import RabbitMQConsumerDispatchMixin
from eventsource.adapters.rabbitmq.consumer_retry import RabbitMQConsumerRetryMixin
from eventsource.observability import OTEL_AVAILABLE
from eventsource.observability.attributes import (
    ATTR_AGGREGATE_ID,  # noqa: F401
    ATTR_HANDLER_NAME,  # noqa: F401
    ATTR_HANDLER_SUCCESS,  # noqa: F401
)

try:
    from opentelemetry.propagate import extract
    from opentelemetry.trace import Status, StatusCode

    PROPAGATION_AVAILABLE = OTEL_AVAILABLE
except ImportError:
    extract = None  # type: ignore[assignment]
    Status = None  # type: ignore[assignment, misc]
    StatusCode = None  # type: ignore[assignment, misc]
    PROPAGATION_AVAILABLE = False

if TYPE_CHECKING:
    from eventsource.adapters._bus.handler_adapter import HandlerAdapter
    from eventsource.adapters._bus.retry import RetryPolicy
    from eventsource.adapters.rabbitmq.config import RabbitMQEventBusConfig
    from eventsource.adapters.rabbitmq.connection import RabbitMQConnectionManager
    from eventsource.adapters.rabbitmq.models import RabbitMQEventBusStats
    from eventsource.adapters.rabbitmq.topology import RabbitMQTopology
    from eventsource.domain.event import DomainEvent
    from eventsource.observability import Tracer

logger = logging.getLogger("eventsource.adapters.rabbitmq")


class RabbitMQConsumer(RabbitMQConsumerDispatchMixin, RabbitMQConsumerRetryMixin):
    """Owns the RabbitMQ consume loop (eventsource.event_bus.consume / eventsource.event_bus.handle)."""

    def __init__(
        self,
        config: RabbitMQEventBusConfig,
        connection: RabbitMQConnectionManager,
        topology: RabbitMQTopology,
        stats: RabbitMQEventBusStats,
        retry_policy: RetryPolicy,
        handlers_for: Callable[[type[DomainEvent]], tuple[HandlerAdapter, ...]],
        resolve_event_class: Callable[[str], type[DomainEvent] | None],
        tracer: Tracer | None,
        enable_tracing: bool,
    ) -> None:
        self._config, self._connection, self._topology = config, connection, topology
        self._stats, self._retry_policy = stats, retry_policy
        self._handlers_for, self._resolve_event_class = handlers_for, resolve_event_class
        self._tracer, self._enable_tracing = tracer, enable_tracing
        self._consuming = False
        self._consumer_task: asyncio.Task[None] | None = None
        self._logger = logging.getLogger("eventsource.adapters.rabbitmq")
        self._retry_scheduler = RetryScheduler(custom_logger=self._logger)

    @property
    def is_consuming(self) -> bool:
        return self._consuming

    @property
    def consumer_task(self) -> asyncio.Task[None] | None:
        return self._consumer_task

    async def start(self) -> None:
        """Start consuming events from the RabbitMQ queue until stopped or disconnected."""
        if not self._topology.consumer_queue:
            raise RuntimeError("Consumer queue not initialized -- internal call-order bug: start() was reached without topology having been declared first")  # fmt: skip
        self._consuming = True
        cname, qname = self._config.consumer_name, self._config.queue_name
        base_extra = {"consumer_name": cname, "queue": qname}
        self._logger.info(f"Starting RabbitMQ consumer: {cname}", extra={**base_extra, "prefetch_count": self._config.prefetch_count})  # fmt: skip
        try:
            async with self._topology.consumer_queue.iterator() as queue_iter:
                async for message in queue_iter:
                    if not self._consuming:
                        break
                    await self._process_message(message)
        except asyncio.CancelledError:
            self._logger.info("Consumer loop cancelled", extra=base_extra)
        except Exception as e:
            err_extra = {**base_extra, "error": str(e), "error_type": type(e).__name__}
            self._logger.error(f"Error in consumer loop: {e}", exc_info=True, extra=err_extra)
            raise
        finally:
            self._consuming = False
            s = self._stats
            stop_extra = {**base_extra, "events_consumed": s.events_consumed, "events_processed_success": s.events_processed_success, "events_processed_failed": s.events_processed_failed}  # fmt: skip
            self._logger.info("Consumer loop stopped", extra=stop_extra)

    async def stop(self) -> None:
        """Stop the consumer loop gracefully."""
        self._consuming = False
        self._logger.info("Stop consuming requested", extra={"consumer_name": self._config.consumer_name, "queue": self._config.queue_name})  # fmt: skip

    def start_in_background(
        self, runner: Callable[[], Coroutine[Any, Any, None]] | None = None
    ) -> asyncio.Task[None]:
        """Start consuming in a background task."""
        if self._consumer_task is not None:
            raise RuntimeError("Consumer already running in background")
        self._consumer_task = asyncio.create_task(
            self.start() if runner is None else runner(),
            name=f"rabbitmq-consumer-{self._config.consumer_name}",
        )
        return self._consumer_task

    async def resume_if_was_consuming(self) -> None:
        """Restart consuming iff it was active before the connection dropped."""
        if not self._connection._was_consuming:
            return
        self._connection._was_consuming = False
        self._logger.info("Resuming consumer after reconnection", extra={"consumer_name": self._config.consumer_name, "queue": self._config.queue_name})  # fmt: skip
        self.start_in_background()

    async def stop_gracefully(self, timeout: float) -> None:
        """Stop consuming and wait for consumer task to finish."""
        if not self._consuming:
            self._logger.debug("Not consuming, skipping consumer stop")
            return
        self._logger.debug("Stopping consumer...")
        self._consuming = False
        if self._consumer_task:
            try:
                await asyncio.wait_for(asyncio.shield(self._consumer_task), timeout=timeout / 2)
                self._logger.debug("Consumer task completed gracefully")
            except TimeoutError:
                self._logger.warning("Consumer task did not stop in time, cancelling", extra={"timeout": timeout / 2})  # fmt: skip
                self._consumer_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._consumer_task
                self._logger.debug("Consumer task cancelled successfully")
            except asyncio.CancelledError:
                self._logger.debug("Consumer task was already cancelled")
            finally:
                self._consumer_task = None
        self._logger.debug("Consumer stopped")

    async def drain_in_flight(self, timeout: float) -> None:
        """Wait for any in-flight message processing and retries to complete."""
        await self.drain_retries(timeout)
        if (drain_time := min(timeout / 4, 5.0)) > 0:
            await asyncio.sleep(drain_time)

    async def drain_retries(self, timeout: float | None = None) -> None:
        """Wait for all pending non-blocking retry tasks to complete."""
        await self._retry_scheduler.drain(timeout)


__all__ = ["PROPAGATION_AVAILABLE", "RabbitMQConsumer", "extract"]
