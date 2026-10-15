"""Connection lifecycle operations mixin for RabbitMQEventBus."""

from __future__ import annotations

import asyncio
import contextlib
import ssl
from typing import TYPE_CHECKING, Any, Self

if TYPE_CHECKING:
    from aio_pika.abc import AbstractChannel, AbstractRobustConnection

    from eventsource.adapters.rabbitmq.config import RabbitMQEventBusConfig
    from eventsource.adapters.rabbitmq.connection import RabbitMQConnectionManager
    from eventsource.adapters.rabbitmq.consumer import RabbitMQConsumer
    from eventsource.adapters.rabbitmq.topology import RabbitMQTopology


class RabbitMQEventBusConnectionMixin:
    """Mixin providing connection lifecycle and reconnection handling for RabbitMQEventBus."""

    _connection_manager: RabbitMQConnectionManager
    _consumer: RabbitMQConsumer
    _topology: RabbitMQTopology
    _config: RabbitMQEventBusConfig

    if TYPE_CHECKING:

        async def shutdown(self, timeout: float = 30.0) -> None: ...

    def _create_ssl_context(self) -> ssl.SSLContext | None:
        """Create an SSL context based on configuration."""
        return self._connection_manager._create_ssl_context()

    async def connect(self) -> None:
        """Connect to RabbitMQ and set up exchanges/queues."""
        await self._connection_manager.connect()

    async def disconnect(self) -> None:
        """Disconnect from RabbitMQ cleanly, cancelling any running consumer task."""
        if self._consumer._consumer_task:
            self._consumer._consumer_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._consumer._consumer_task
            self._consumer._consumer_task = None

        await self._connection_manager.disconnect()
        self._consumer._consuming = False
        self._topology._exchange = None
        self._topology._dlq_exchange = None
        self._topology._consumer_queue = None
        self._topology._dlq_queue = None

    async def __aenter__(self: Self) -> Self:
        """Connect to RabbitMQ when entering the async context."""
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        """Perform graceful shutdown when exiting the async context."""
        await self.shutdown(timeout=self._config.shutdown_timeout)

    def _sanitize_url(self, url: str) -> str:
        """Remove credentials from URL for logging."""
        return self._connection_manager._sanitize_url(url)

    async def _on_reconnect(self, connection: AbstractRobustConnection) -> None:
        """Handle connection restoration after disconnection."""
        await self._connection_manager._on_reconnect(connection)

    def _on_connection_close(
        self,
        connection: AbstractRobustConnection | None,
        exception: BaseException | None,
    ) -> None:
        """Handle connection closure."""
        self._connection_manager._on_connection_close(connection, exception)

    def _on_channel_close(
        self,
        channel: AbstractChannel | None,
        exception: BaseException | None,
    ) -> None:
        """Handle channel closure."""
        self._connection_manager._on_channel_close(channel, exception)
