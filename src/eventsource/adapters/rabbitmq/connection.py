"""RabbitMQ connection lifecycle management."""

from __future__ import annotations

import asyncio
import logging
import ssl
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from eventsource.adapters.rabbitmq.config import RabbitMQEventBusConfig
from eventsource.adapters.rabbitmq.connection_callbacks import RabbitMQConnectionCallbacksMixin
from eventsource.adapters.rabbitmq.connection_ssl import create_ssl_context, sanitize_url
from eventsource.adapters.rabbitmq.models import RabbitMQEventBusStats

if TYPE_CHECKING:
    from aio_pika.abc import AbstractChannel, AbstractRobustConnection

try:
    import aio_pika
except ImportError:  # pragma: no cover - guarded by RabbitMQEventBus construction
    aio_pika = None  # type: ignore[assignment]

logger = logging.getLogger("eventsource.adapters.rabbitmq.connection")


class RabbitMQConnectionManager(RabbitMQConnectionCallbacksMixin):
    """Owns the aio-pika connection/channel lifecycle for RabbitMQEventBus."""

    def __init__(self, config: RabbitMQEventBusConfig, stats: RabbitMQEventBusStats) -> None:
        self._config = config
        self._stats = stats
        self._connection: AbstractRobustConnection | None = None
        self._channel: AbstractChannel | None = None
        self._connected = False
        self._reconnecting: bool = False
        self._was_consuming: bool = False
        self._lock = asyncio.Lock()
        self._reconnect_callbacks: list[Callable[[], Awaitable[None]]] = []
        self._is_consuming: Callable[[], bool] | None = None

    @property
    def is_connected(self) -> bool:
        """Check if connected to RabbitMQ (connection open and marked connected)."""
        return self._connected and self._connection is not None and not self._connection.is_closed

    @property
    def is_reconnecting(self) -> bool:
        """Check if a reconnection is currently in progress."""
        return self._reconnecting

    @property
    def connection(self) -> AbstractRobustConnection | None:
        """Get the current robust connection, if any."""
        return self._connection

    @property
    def channel(self) -> AbstractChannel | None:
        """Get the current channel, if any."""
        return self._channel

    def require_channel(self) -> AbstractChannel:
        """Return the live channel or raise if not connected."""
        if self._channel is None:
            raise RuntimeError("Not connected to RabbitMQ")
        return self._channel

    def _create_ssl_context(self) -> ssl.SSLContext | None:
        """Create SSL context from configuration."""
        return create_ssl_context(self._config)

    def _sanitize_url(self, url: str) -> str:
        """Remove credentials from URL for logging."""
        return sanitize_url(url)

    async def connect(self) -> None:
        """Connect to RabbitMQ, create the channel, and run reconnect callbacks."""
        async with self._lock:
            if self._connected:
                logger.warning("RabbitMQEventBus already connected")
                return
            ssl_context: ssl.SSLContext | None = None
            try:
                ssl_context = self._create_ssl_context()
                connect_kwargs: dict[str, Any] = {
                    "heartbeat": self._config.heartbeat,
                    "reconnect_interval": self._config.reconnect_delay,
                }
                if ssl_context is not None:
                    connect_kwargs["ssl_context"] = ssl_context
                if self._config.ssl_options:
                    connect_kwargs.update(self._config.ssl_options)
                self._connection = await aio_pika.connect_robust(
                    self._config.rabbitmq_url, **connect_kwargs
                )
                self._connection.reconnect_callbacks.add(self._on_reconnect)  # type: ignore[arg-type]
                self._connection.close_callbacks.add(self._on_connection_close)  # type: ignore[arg-type]
                self._channel = await self._connection.channel()
                self._channel.close_callbacks.add(self._on_channel_close)
                await self._channel.set_qos(prefetch_count=self._config.prefetch_count)
                await self._run_reconnect_callbacks()
                self._connected = True
                self._stats.connected_at = datetime.now(UTC)
                tls_status = "TLS" if ssl_context is not None else "plaintext"
                logger.info(
                    f"Connected to RabbitMQ ({tls_status}) and initialized topology",
                    extra={
                        "rabbitmq_url": self._sanitize_url(self._config.rabbitmq_url),
                        "exchange": self._config.exchange_name,
                        "queue": self._config.queue_name,
                        "consumer_group": self._config.consumer_group,
                        "dlq_enabled": self._config.enable_dlq,
                        "tls_enabled": ssl_context is not None,
                    },
                )
            except ssl.SSLError as e:
                logger.error(
                    f"SSL error connecting to RabbitMQ: {e}",
                    exc_info=True,
                    extra={
                        "rabbitmq_url": self._sanitize_url(self._config.rabbitmq_url),
                        "error": str(e),
                        "error_type": type(e).__name__,
                        "tls_enabled": ssl_context is not None,
                    },
                )
                await self.disconnect()
                raise
            except Exception as e:
                logger.error(
                    f"Failed to connect to RabbitMQ: {e}",
                    exc_info=True,
                    extra={
                        "rabbitmq_url": self._sanitize_url(self._config.rabbitmq_url),
                        "exchange": self._config.exchange_name,
                        "error": str(e),
                        "error_type": type(e).__name__,
                    },
                )
                await self.disconnect()
                raise

    async def disconnect(self) -> None:
        """Disconnect from RabbitMQ, closing channel and connection cleanly."""
        if self._channel and not self._channel.is_closed:
            await self._channel.close()
            self._channel = None
        if self._connection and not self._connection.is_closed:
            await self._connection.close()
            self._connection = None
        self._connected = False
        self._stats.connected_at = None
        logger.info(
            "Disconnected from RabbitMQ",
            extra={
                "exchange": self._config.exchange_name,
                "queue": self._config.queue_name,
                "consumer_group": self._config.consumer_group,
            },
        )

    async def force_disconnect(self) -> None:
        """Force close channel and connection, suppressing all errors."""
        if self._channel:
            try:
                if not self._channel.is_closed:
                    await self._channel.close()
            except Exception:  # nosec B110 - intentionally suppress during force disconnect
                pass
            self._channel = None
        if self._connection:
            try:
                if not self._connection.is_closed:
                    await self._connection.close()
            except Exception:  # nosec B110 - intentionally suppress during force disconnect
                pass
            self._connection = None
        self._connected = False

    def health_slice(self) -> dict[str, Any]:
        """Connection/channel health used by ``RabbitMQEventBus.health_check()``."""
        healthy = True
        errors: list[str] = []
        if not self._connection:
            connection_status = "disconnected"
            healthy = False
            errors.append("Not connected to RabbitMQ")
        elif self._connection.is_closed:
            connection_status = "closed"
            healthy = False
            errors.append("RabbitMQ connection is closed")
        else:
            connection_status = "connected"
        if not self._channel:
            channel_status = "not_initialized"
            healthy = False
            errors.append("Channel not initialized")
        elif self._channel.is_closed:
            channel_status = "closed"
            healthy = False
            errors.append("AMQP channel is closed")
        else:
            channel_status = "open"
        return {
            "healthy": healthy,
            "connection_status": connection_status,
            "channel_status": channel_status,
            "errors": errors,
        }


__all__ = [
    "RabbitMQConnectionCallbacksMixin",
    "RabbitMQConnectionManager",
    "create_ssl_context",
    "sanitize_url",
]
