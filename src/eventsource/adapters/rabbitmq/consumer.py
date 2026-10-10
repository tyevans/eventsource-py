"""Consume loop, handler dispatch, and retry/DLQ write path for the RabbitMQ bus.

``RabbitMQConsumer`` owns everything on the delivery path: the queue iterator
loop, per-message processing (deserialize -> dispatch -> ack), the error path
(retry republish / DLQ routing), and graceful stop/drain.

Error isolation (ADR 0011) lives here: :meth:`_dispatch_event` runs *every*
handler for a delivery, collects failures, and raises a single
:class:`HandlerDispatchError`; :meth:`_process_message` treats that as a
processing failure and routes to retry/DLQ.

The consumer never touches the bus's subscription registry -- it is handed
``handlers_for`` and ``resolve_event_class`` callables instead.
"""

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

# OpenTelemetry propagation import - kept separate for distributed tracing.
try:
    from opentelemetry.propagate import extract
    from opentelemetry.trace import Status, StatusCode

    PROPAGATION_AVAILABLE = OTEL_AVAILABLE
except ImportError:
    extract = None  # type: ignore[assignment]
    Status = None  # type: ignore[assignment, misc]
    StatusCode = None  # type: ignore[assignment, misc]
    PROPAGATION_AVAILABLE = False

# Conventions tested by test_rabbitmq_tracing:
# Standard span names: "eventsource.event_bus.consume", "eventsource.event_bus.handle"

if TYPE_CHECKING:
    from eventsource.adapters._bus.handler_adapter import HandlerAdapter
    from eventsource.adapters._bus.retry import RetryPolicy
    from eventsource.adapters.rabbitmq.config import RabbitMQEventBusConfig
    from eventsource.adapters.rabbitmq.connection import RabbitMQConnectionManager
    from eventsource.adapters.rabbitmq.models import RabbitMQEventBusStats
    from eventsource.adapters.rabbitmq.topology import RabbitMQTopology
    from eventsource.domain.event import DomainEvent
    from eventsource.observability import Tracer

# Named explicitly so the logger name stays stable across the extraction.
logger = logging.getLogger("eventsource.adapters.rabbitmq")


class RabbitMQConsumer(RabbitMQConsumerDispatchMixin, RabbitMQConsumerRetryMixin):
    """Owns the RabbitMQ consume loop and the retry/DLQ write path."""

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
        self._config = config
        self._connection = connection
        self._topology = topology
        self._stats = stats
        self._retry_policy = retry_policy
        self._handlers_for = handlers_for
        self._resolve_event_class = resolve_event_class
        self._tracer = tracer
        self._enable_tracing = enable_tracing

        self._consuming = False
        self._consumer_task: asyncio.Task[None] | None = None

        self._logger = logging.getLogger("eventsource.adapters.rabbitmq")
        self._retry_scheduler = RetryScheduler(custom_logger=self._logger)

    # =========================================================================
    # State
    # =========================================================================

    @property
    def is_consuming(self) -> bool:
        """Check if currently consuming events."""
        return self._consuming

    @property
    def consumer_task(self) -> asyncio.Task[None] | None:
        """The background consumer task, if one is running."""
        return self._consumer_task

    # =========================================================================
    # Consume loop
    # =========================================================================

    async def start(self) -> None:
        """Start consuming events from the RabbitMQ queue.

        Runs continuously, consuming messages from the queue and dispatching
        them to registered handlers, until :meth:`stop` is called or the
        connection is lost.

        Raises:
            RuntimeError: If the consumer queue is not initialized. The
                public ``RabbitMQEventBus.start_consuming()`` always
                connects first, and ``connect()`` always declares topology
                (including the consumer queue) before returning -- so this
                should be unreachable through that path. Tripping it means
                an internal invariant between the connection manager and
                this consumer was violated (e.g. this ``start()`` was
                called directly, bypassing the facade), not that the
                caller forgot to connect.
        """
        if not self._topology.consumer_queue:
            raise RuntimeError(
                "Consumer queue not initialized -- internal call-order bug: "
                "start() was reached without topology having been declared first"
            )

        self._consuming = True
        consumer_name = self._config.consumer_name

        self._logger.info(
            f"Starting RabbitMQ consumer: {consumer_name}",
            extra={
                "consumer_name": consumer_name,
                "queue": self._config.queue_name,
                "prefetch_count": self._config.prefetch_count,
            },
        )

        try:
            # Use queue iterator for consuming
            async with self._topology.consumer_queue.iterator() as queue_iter:
                async for message in queue_iter:
                    if not self._consuming:
                        break

                    await self._process_message(message)

        except asyncio.CancelledError:
            self._logger.info(
                "Consumer loop cancelled",
                extra={
                    "consumer_name": consumer_name,
                    "queue": self._config.queue_name,
                },
            )
        except Exception as e:
            self._logger.error(
                f"Error in consumer loop: {e}",
                exc_info=True,
                extra={
                    "consumer_name": consumer_name,
                    "queue": self._config.queue_name,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
            )
            raise
        finally:
            self._consuming = False
            self._logger.info(
                "Consumer loop stopped",
                extra={
                    "consumer_name": consumer_name,
                    "queue": self._config.queue_name,
                    "events_consumed": self._stats.events_consumed,
                    "events_processed_success": self._stats.events_processed_success,
                    "events_processed_failed": self._stats.events_processed_failed,
                },
            )

    async def stop(self) -> None:
        """Stop the consumer loop gracefully.

        Sets the consuming flag to False, which will cause the consumer loop
        to exit after processing the current message.
        """
        self._consuming = False
        self._logger.info(
            "Stop consuming requested",
            extra={
                "consumer_name": self._config.consumer_name,
                "queue": self._config.queue_name,
            },
        )

    def start_in_background(
        self,
        runner: Callable[[], Coroutine[Any, Any, None]] | None = None,
    ) -> asyncio.Task[None]:
        """Start consuming in a background task.

        Args:
            runner: Optional coroutine factory to run instead of :meth:`start`.
                The facade passes its own ``start_consuming`` so the background
                task keeps its auto-connect behavior.

        Returns:
            The background task running the consumer.

        Raises:
            RuntimeError: If consumer is already running in background.
        """
        if self._consumer_task is not None:
            raise RuntimeError("Consumer already running in background")

        self._consumer_task = asyncio.create_task(
            self.start() if runner is None else runner(),
            name=f"rabbitmq-consumer-{self._config.consumer_name}",
        )
        return self._consumer_task

    async def resume_if_was_consuming(self) -> None:
        """Restart consuming iff it was active before the connection dropped.

        Intended as a reconnect hook. Reads the ``_was_consuming`` flag the
        connection manager sets from its close callbacks, clears it, and
        restarts the consume loop in the background.

        Note:
            Not currently registered via ``RabbitMQConnectionManager.on_reconnect``
            -- the facade only wires ``RabbitMQTopology.redeclare``. This
            matches the original (pre-decomposition) behavior, which never
            resumed consuming automatically after a reconnect. Registering
            this method would change that behavior and needs an explicit
            decision, not a silent side effect of refactoring.
        """
        if not self._connection._was_consuming:
            return

        self._connection._was_consuming = False
        self._logger.info(
            "Resuming consumer after reconnection",
            extra={
                "consumer_name": self._config.consumer_name,
                "queue": self._config.queue_name,
            },
        )
        self.start_in_background()

    # =========================================================================
    # Graceful shutdown
    # =========================================================================

    async def stop_gracefully(self, timeout: float) -> None:
        """Stop consuming and wait for consumer task to finish.

        This method signals the consumer loop to stop by setting the consuming
        flag to False, then waits for the consumer task to complete. If the
        task doesn't complete within the timeout, it is cancelled.

        Args:
            timeout: Maximum time to wait for the consumer to stop.
                    Half of this time is used for graceful stop, the other
                    half for cancellation if needed.

        Raises:
            asyncio.TimeoutError: If the consumer doesn't stop within timeout
        """
        if not self._consuming:
            self._logger.debug("Not consuming, skipping consumer stop")
            return

        self._logger.debug("Stopping consumer...")

        # Signal consumer to stop
        self._consuming = False

        # Wait for consumer task if running
        if self._consumer_task:
            try:
                # Use half timeout for graceful wait, reserve half for cleanup
                await asyncio.wait_for(
                    asyncio.shield(self._consumer_task),
                    timeout=timeout / 2,
                )
                self._logger.debug("Consumer task completed gracefully")
            except TimeoutError:
                self._logger.warning(
                    "Consumer task did not stop in time, cancelling",
                    extra={"timeout": timeout / 2},
                )
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
        """Wait for any in-flight message processing and retries to complete.

        Args:
            timeout: Maximum time available for draining.
                    Actual drain time is min(timeout / 4, 5.0) seconds.
        """
        await self.drain_retries(timeout)
        drain_time = min(timeout / 4, 5.0)
        if drain_time > 0:
            await asyncio.sleep(drain_time)

    async def drain_retries(self, timeout: float | None = None) -> None:
        """Wait for all pending non-blocking retry tasks to complete.

        Args:
            timeout: Maximum seconds to wait. If None, waits indefinitely.
        """
        await self._retry_scheduler.drain(timeout)


__all__ = [
    "PROPAGATION_AVAILABLE",
    "RabbitMQConsumer",
    "extract",
]
