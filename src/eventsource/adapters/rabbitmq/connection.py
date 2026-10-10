"""RabbitMQ connection lifecycle management.

Extracted from ``RabbitMQEventBus`` (bus.py) as part of the bus god-class
decomposition. Owns the aio-pika robust connection/channel, the connect
lock, SSL context creation, URL sanitizing for logging, and the aio-pika
close/reconnect callbacks.

Topology (exchange/queue declaration) and consumer resumption are not yet
extracted (see Tasks 5/7 of the decomposition). Callers that need those
behaviors to run after a (re)connect register them via :meth:`on_reconnect`.
"""

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

# Named explicitly so the logger name is stable and independent of the
# facade's "eventsource.adapters.rabbitmq" logger.
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

        # Optional hook set by the facade so close-callback bookkeeping can
        # still observe consumer state that hasn't been extracted yet.
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
        """Create SSL context from configuration.

        Delegates to :func:`create_ssl_context`.
        """
        return create_ssl_context(self._config)

    def _sanitize_url(self, url: str) -> str:
        """Remove credentials from URL for logging.

        Delegates to :func:`sanitize_url`.
        """
        return sanitize_url(url)

    async def connect(self) -> None:
        """Connect to RabbitMQ, create the channel, and run reconnect callbacks.

        Establishes connection, creates channel, and runs any registered
        reconnect callbacks (topology declaration / consumer resumption)
        for the initial connect too, since those are identical in shape
        to what happens on reconnect.

        Uses aio-pika's RobustConnection for automatic reconnection support.
        Sets up the channel with configured prefetch count for flow control.
        Supports TLS/SSL connections via amqps:// URLs and ssl_context configuration.

        Raises:
            Exception: If connection or setup fails
            ssl.SSLError: If SSL/TLS configuration or handshake fails
        """
        async with self._lock:
            if self._connected:
                logger.warning("RabbitMQEventBus already connected")
                return

            ssl_context: ssl.SSLContext | None = None
            try:
                # Prepare SSL context if needed
                ssl_context = self._create_ssl_context()

                # Build connection parameters
                connect_kwargs: dict[str, Any] = {
                    "heartbeat": self._config.heartbeat,
                    "reconnect_interval": self._config.reconnect_delay,
                }

                # Add SSL context if TLS is configured
                if ssl_context is not None:
                    connect_kwargs["ssl_context"] = ssl_context

                # Add additional SSL options if provided
                if self._config.ssl_options:
                    connect_kwargs.update(self._config.ssl_options)

                # Create robust connection (handles reconnection automatically)
                self._connection = await aio_pika.connect_robust(
                    self._config.rabbitmq_url,
                    **connect_kwargs,
                )

                # Register connection callbacks for reconnection handling
                # Note: aio-pika's type hints are inconsistent with actual usage
                self._connection.reconnect_callbacks.add(self._on_reconnect)  # type: ignore[arg-type]
                self._connection.close_callbacks.add(self._on_connection_close)  # type: ignore[arg-type]

                # Create channel (RobustConnection returns RobustChannel)
                self._channel = await self._connection.channel()

                # Register channel close callback
                self._channel.close_callbacks.add(self._on_channel_close)

                # Set prefetch count for consumer flow control
                await self._channel.set_qos(prefetch_count=self._config.prefetch_count)

                # Run registered reconnect callbacks (topology declare, etc.)
                await self._run_reconnect_callbacks()

                self._connected = True
                self._stats.connected_at = datetime.now(UTC)

                # Log connection status with TLS info
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
                # Clean up partial connection
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
                # Clean up partial connection
                await self.disconnect()
                raise

    async def disconnect(self) -> None:
        """Disconnect from RabbitMQ, closing channel and connection cleanly."""
        # Close channel
        if self._channel and not self._channel.is_closed:
            await self._channel.close()
            self._channel = None

        # Close connection
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
        """Connection/channel health used by ``RabbitMQEventBus.health_check()``.

        Mirrors the connection- and channel-status checks that used to be
        inline in the facade's ``health_check`` body. Returns a dict rather
        than a dataclass so the facade can freely merge it with the
        queue/DLQ slices without introducing another public model type.

        Returns:
            Dict with ``healthy`` (bool), ``connection_status`` (str),
            ``channel_status`` (str), and ``errors`` (list[str]) -- the
            error messages contributed by this slice, in check order.
        """
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
