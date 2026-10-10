"""
Shutdown operations mixin for RabbitMQEventBus.

Provides graceful shutdown, message draining, and forced disconnect logic.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from eventsource.adapters.rabbitmq.connection import RabbitMQConnectionManager
    from eventsource.adapters.rabbitmq.consumer import RabbitMQConsumer


class RabbitMQEventBusShutdownMixin:
    """Mixin providing graceful and forced shutdown for RabbitMQEventBus."""

    _shutdown_initiated: bool
    _logger: logging.Logger
    _consumer: RabbitMQConsumer
    _consuming: bool
    _consumer_task: asyncio.Task[None] | None
    _connection_manager: RabbitMQConnectionManager
    _exchange: Any
    _dlq_exchange: Any
    _consumer_queue: Any
    _dlq_queue: Any

    if TYPE_CHECKING:

        async def disconnect(self) -> None: ...
        async def _drain_background(self, timeout: float = 30.0) -> None: ...

    async def shutdown(self, timeout: float = 30.0) -> None:
        """Shutdown the event bus gracefully.

        Stops consuming new messages, waits for in-flight messages to complete
        processing, and closes all connections. This method is idempotent - calling
        it multiple times is safe.

        The shutdown process follows these steps:
        1. Stop accepting new messages (set _consuming flag to False)
        2. Wait for the consumer task to finish processing current messages
        3. Wait for any in-flight message processing to complete
        4. Drain outstanding background publish tasks
        5. Disconnect from RabbitMQ (close channel and connection)

        After shutdown is initiated, the event bus cannot be reused without
        creating a new instance. Attempting to publish or start consuming after
        shutdown will raise an error.

        Args:
            timeout: Maximum time to wait for graceful shutdown in seconds.
                    If the timeout is exceeded, a TimeoutError is raised.
                    The timeout is split between the consumer stop and drain phases.
                    Default is 30.0 seconds.

        Raises:
            TimeoutError: If the shutdown process exceeds the timeout.
                         The error message includes details about what phase timed out.
                         Connection and channel are NOT force-closed on timeout.

        Example:
            >>> await bus.shutdown(timeout=10.0)

            # With context manager (uses config.shutdown_timeout):
            >>> async with RabbitMQEventBus(config=config) as bus:
            ...     await bus.publish([event])
            # Graceful shutdown happens automatically on exit
        """
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
            # Step 1: Stop accepting new messages
            await self._stop_consuming_gracefully(timeout)

            # Step 2: Wait for in-flight processing to complete
            await self._drain_in_flight(timeout)

            # Step 2b: Wait for publisher background tasks (a different
            # concern from consumer message processing above)
            await self._drain_background(timeout)

            # Step 3: Disconnect from RabbitMQ
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
                extra={
                    "timeout": timeout,
                    "duration_seconds": shutdown_duration,
                },
            )
            # Re-raise TimeoutError as specified in task requirements
            # Connection is NOT force-closed - caller can decide what to do
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
                extra={
                    "duration_seconds": shutdown_duration,
                    "error": str(e),
                },
            )
            raise

    async def _stop_consuming_gracefully(self, timeout: float) -> None:
        """Stop consuming and wait for consumer task to finish.

        Delegates to :class:`RabbitMQConsumer`.

        Args:
            timeout: Maximum time to wait for the consumer to stop.
        """
        await self._consumer.stop_gracefully(timeout)

    async def _drain_in_flight(self, timeout: float) -> None:
        """Wait for any in-flight message processing to complete.

        Delegates to :class:`RabbitMQConsumer`.

        Args:
            timeout: Maximum time available for draining.
        """
        await self._consumer.drain_in_flight(timeout)

    async def _force_disconnect(self) -> None:
        """Force disconnect without waiting for graceful completion.

        This method immediately cancels any running consumer task and closes
        the channel and connection without waiting. It suppresses all exceptions
        during cleanup to ensure the disconnect completes.

        Use this method when graceful shutdown has timed out or failed and
        you need to immediately release resources.

        Note:
            This method may result in unacknowledged messages being redelivered
            by RabbitMQ to other consumers. It should only be used as a last resort.

        Example:
            >>> try:
            ...     await bus.shutdown(timeout=5.0)
            ... except TimeoutError:
            ...     await bus._force_disconnect()
        """
        self._logger.warning("Forcing disconnect")

        self._consuming = False
        self._shutdown_initiated = True

        # Cancel consumer task immediately
        if self._consumer_task:
            self._consumer_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._consumer_task
            self._consumer_task = None

        await self._connection_manager.force_disconnect()

        # Clear exchange/queue references
        self._exchange = None
        self._dlq_exchange = None
        self._consumer_queue = None
        self._dlq_queue = None

        self._logger.info("Forced disconnect completed")

    @property
    def is_shutdown(self) -> bool:
        """Check if shutdown has been initiated.

        Returns:
            True if shutdown() has been called on this instance,
            False otherwise. Once shutdown is initiated, the event bus
            cannot be reused.
        """
        return self._shutdown_initiated
