"""Shutdown operations mixin for RabbitMQEventBus."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from eventsource.adapters.rabbitmq.connection import RabbitMQConnectionManager
    from eventsource.adapters.rabbitmq.consumer import RabbitMQConsumer
    from eventsource.adapters.rabbitmq.topology import RabbitMQTopology


class RabbitMQEventBusShutdownMixin:
    """Mixin providing graceful and forced shutdown for RabbitMQEventBus."""

    _shutdown_initiated: bool
    _logger: logging.Logger
    _consumer: RabbitMQConsumer
    _connection_manager: RabbitMQConnectionManager
    _topology: RabbitMQTopology

    if TYPE_CHECKING:

        async def disconnect(self) -> None: ...
        async def _drain_background(self, timeout: float = 30.0) -> None: ...

    async def shutdown(self, timeout: float = 30.0) -> None:
        """Shutdown the event bus gracefully (stop consuming, drain in-flight/background, disconnect)."""
        if self._shutdown_initiated:
            self._logger.debug("Shutdown already initiated, skipping")
            return

        self._shutdown_initiated = True
        shutdown_start = datetime.now(UTC)
        self._logger.info(
            f"Initiating graceful shutdown (timeout={timeout}s)",
            extra={"timeout": timeout},
        )

        try:
            await self._stop_consuming_gracefully(timeout)
            await self._drain_in_flight(timeout)
            await self._drain_background(timeout)
            await self.disconnect()

            shutdown_duration = (datetime.now(UTC) - shutdown_start).total_seconds()
            self._logger.info(
                f"Graceful shutdown completed in {shutdown_duration:.2f}s",
                extra={"duration_seconds": shutdown_duration},
            )
        except TimeoutError:
            shutdown_duration = (datetime.now(UTC) - shutdown_start).total_seconds()
            self._logger.error(
                f"Graceful shutdown timed out after {shutdown_duration:.2f}s",
                extra={"timeout": timeout, "duration_seconds": shutdown_duration},
            )
            raise TimeoutError(
                f"Graceful shutdown timed out after {shutdown_duration:.2f}s. "
                f"In-flight messages may still be processing. "
                f"Call disconnect() or _force_disconnect() to force close."
            ) from None
        except Exception as e:
            shutdown_duration = (datetime.now(UTC) - shutdown_start).total_seconds()
            self._logger.error(
                f"Error during shutdown after {shutdown_duration:.2f}s: {e}",
                exc_info=True,
                extra={"duration_seconds": shutdown_duration, "error": str(e)},
            )
            raise

    async def _stop_consuming_gracefully(self, timeout: float) -> None:
        """Stop consuming and wait for consumer task to finish."""
        await self._consumer.stop_gracefully(timeout)

    async def _drain_in_flight(self, timeout: float) -> None:
        """Wait for any in-flight message processing to complete."""
        await self._consumer.drain_in_flight(timeout)

    async def _force_disconnect(self) -> None:
        """Force disconnect without waiting for graceful completion."""
        self._logger.warning("Forcing disconnect")
        self._consumer._consuming = False
        self._shutdown_initiated = True

        if self._consumer._consumer_task:
            self._consumer._consumer_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._consumer._consumer_task
            self._consumer._consumer_task = None

        await self._connection_manager.force_disconnect()

        self._topology._exchange = None
        self._topology._dlq_exchange = None
        self._topology._consumer_queue = None
        self._topology._dlq_queue = None

        self._logger.info("Forced disconnect completed")

    @property
    def is_shutdown(self) -> bool:
        """Check if shutdown has been initiated."""
        return self._shutdown_initiated
