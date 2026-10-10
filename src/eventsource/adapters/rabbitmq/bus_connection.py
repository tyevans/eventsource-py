"""
Connection lifecycle operations mixin for RabbitMQEventBus.

Provides connect, disconnect, SSL configuration, and reconnection callbacks.
"""

from __future__ import annotations

import asyncio
import contextlib
import ssl
from typing import TYPE_CHECKING, Any, Self

if TYPE_CHECKING:
    from aio_pika.abc import (
        AbstractChannel,
        AbstractQueue,
        AbstractRobustConnection,
    )

    from eventsource.adapters.rabbitmq.config import RabbitMQEventBusConfig
    from eventsource.adapters.rabbitmq.connection import RabbitMQConnectionManager


class RabbitMQEventBusConnectionMixin:
    """Mixin providing connection lifecycle and reconnection handling for RabbitMQEventBus."""

    _connection_manager: RabbitMQConnectionManager
    _consumer_task: asyncio.Task[None] | None
    _consuming: bool
    _exchange: Any
    _dlq_exchange: Any
    _consumer_queue: AbstractQueue | None
    _dlq_queue: AbstractQueue | None
    _config: RabbitMQEventBusConfig

    if TYPE_CHECKING:

        async def shutdown(self, timeout: float = 30.0) -> None: ...

    def _create_ssl_context(self) -> ssl.SSLContext | None:
        """Create an SSL context based on configuration.

        Delegates to :class:`RabbitMQConnectionManager`.

        Returns:
            SSLContext configured according to config settings, or None
            if SSL is not configured or not using amqps://
        """
        return self._connection_manager._create_ssl_context()

    async def connect(self) -> None:
        """Connect to RabbitMQ and set up exchanges/queues.

        Establishes connection, creates channel, declares exchanges
        and queues, and sets up bindings.

        Uses aio-pika's RobustConnection for automatic reconnection support.
        Sets up the channel with configured prefetch count for flow control.
        Supports TLS/SSL connections via amqps:// URLs and ssl_context configuration.

        Raises:
            Exception: If connection or setup fails
            ssl.SSLError: If SSL/TLS configuration or handshake fails
        """
        await self._connection_manager.connect()

    async def disconnect(self) -> None:
        """Disconnect from RabbitMQ.

        Closes channel and connection cleanly. Cancels any running consumer
        task before closing the connection.
        """
        # Stop consumer if running
        if self._consumer_task:
            self._consumer_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._consumer_task
            self._consumer_task = None

        await self._connection_manager.disconnect()

        self._consuming = False

        # Clear exchange/queue references
        self._exchange = None
        self._dlq_exchange = None
        self._consumer_queue = None
        self._dlq_queue = None

    async def __aenter__(self: Self) -> Self:
        """Async context manager entry.

        Connects to RabbitMQ when entering the context.

        Returns:
            The connected event bus instance.
        """
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        """Async context manager exit with graceful shutdown.

        Performs graceful shutdown when exiting the context, regardless
        of whether an exception occurred. Uses the configured shutdown_timeout.

        Args:
            exc_type: The exception type if an exception was raised
            exc_val: The exception instance if an exception was raised
            exc_tb: The traceback if an exception was raised
        """
        await self.shutdown(timeout=self._config.shutdown_timeout)

    def _sanitize_url(self, url: str) -> str:
        """Remove credentials from URL for logging.

        Delegates to :class:`RabbitMQConnectionManager`.

        Args:
            url: The RabbitMQ connection URL

        Returns:
            URL with credentials replaced by ***
        """
        return self._connection_manager._sanitize_url(url)

    async def _on_reconnect(self, connection: AbstractRobustConnection) -> None:
        """Handle connection restoration after disconnection.

        Delegates to :class:`RabbitMQConnectionManager`.

        Args:
            connection: The restored RobustConnection instance
        """
        await self._connection_manager._on_reconnect(connection)

    def _on_connection_close(
        self,
        connection: AbstractRobustConnection | None,
        exception: BaseException | None,
    ) -> None:
        """Handle connection closure.

        Delegates to :class:`RabbitMQConnectionManager`.

        Args:
            connection: The closed connection instance (may be None)
            exception: The exception that caused the closure, or None
                      if closed gracefully
        """
        self._connection_manager._on_connection_close(connection, exception)

    def _on_channel_close(
        self,
        channel: AbstractChannel | None,
        exception: BaseException | None,
    ) -> None:
        """Handle channel closure.

        Delegates to :class:`RabbitMQConnectionManager`.

        Args:
            channel: The closed channel instance (may be None)
            exception: The exception that caused the closure, or None
                      if closed gracefully
        """
        self._connection_manager._on_channel_close(channel, exception)
