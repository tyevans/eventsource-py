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
import re
import ssl
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from eventsource.adapters.rabbitmq.config import RabbitMQEventBusConfig
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


class RabbitMQConnectionManager:
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

    def on_reconnect(self, callback: Callable[[], Awaitable[None]]) -> None:
        """Register an async callback fired after a successful reconnect.

        Callbacks run in registration order.
        """
        self._reconnect_callbacks.append(callback)

    async def _run_reconnect_callbacks(self) -> None:
        """Invoke all registered reconnect callbacks, in order."""
        for cb in self._reconnect_callbacks:
            await cb()

    def _create_ssl_context(self) -> ssl.SSLContext | None:
        """Create SSL context from configuration.

        Creates an SSL context based on the TLS configuration options. The
        method follows this priority order:
        1. Use ssl_context if explicitly provided
        2. Create context from ca_file/cert_file/key_file if provided
        3. Create default context if URL uses amqps://
        4. Return None for non-TLS connections

        Returns:
            SSLContext if TLS is configured or required by URL, None otherwise.

        Raises:
            ssl.SSLError: If certificate files cannot be loaded
            FileNotFoundError: If certificate files don't exist
        """
        # Check if URL is amqps:// (TLS required)
        is_tls_url = self._config.rabbitmq_url.startswith("amqps://")

        # Determine if TLS is needed
        needs_tls = (
            is_tls_url
            or self._config.ssl_context is not None
            or self._config.ca_file is not None
            or self._config.cert_file is not None
        )

        if not needs_tls:
            return None

        # Use provided context if available (takes precedence)
        if self._config.ssl_context is not None:
            logger.debug("Using pre-configured SSL context")
            return self._config.ssl_context

        # Create context from configuration
        ctx = ssl.create_default_context(
            purpose=ssl.Purpose.SERVER_AUTH,
        )

        # Load CA certificate if provided
        if self._config.ca_file:
            logger.debug(
                "Loading CA certificate",
                extra={"ca_file": self._config.ca_file},
            )
            ctx.load_verify_locations(cafile=self._config.ca_file)

        # Load client certificate for mTLS
        if self._config.cert_file and self._config.key_file:
            logger.debug(
                "Loading client certificate for mutual TLS",
                extra={
                    "cert_file": self._config.cert_file,
                    "key_file": self._config.key_file,
                },
            )
            ctx.load_cert_chain(
                certfile=self._config.cert_file,
                keyfile=self._config.key_file,
            )
        elif self._config.cert_file or self._config.key_file:
            # Warn if only one of cert_file/key_file is provided
            logger.warning(
                "Both cert_file and key_file must be provided for mutual TLS. "
                "Client certificate authentication will not be used.",
                extra={
                    "cert_file": self._config.cert_file,
                    "key_file": self._config.key_file,
                },
            )

        # Configure verification
        if not self._config.verify_ssl:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            logger.warning(
                "SSL certificate verification disabled - NOT RECOMMENDED for production",
                extra={"verify_ssl": False},
            )

        return ctx

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

    def _sanitize_url(self, url: str) -> str:
        """Remove credentials from URL for logging.

        Args:
            url: The RabbitMQ connection URL

        Returns:
            URL with credentials replaced by ***
        """
        return re.sub(r"://[^:]+:[^@]+@", "://***:***@", url)

    # =========================================================================
    # Reconnection Callback Methods (P2-004)
    # =========================================================================

    async def _on_reconnect(self, connection: AbstractRobustConnection) -> None:
        """Handle connection restoration after disconnection.

        This callback is invoked by aio-pika's RobustConnection when the
        connection is restored after a disconnection. It re-establishes
        the channel and runs registered reconnect callbacks (topology
        redeclare / consumer resume).

        Args:
            connection: The restored RobustConnection instance
        """
        self._stats.reconnections += 1
        self._reconnecting = True

        logger.info(
            "RabbitMQ connection restored, re-establishing topology",
            extra={
                "reconnections": self._stats.reconnections,
                "was_consuming": self._was_consuming,
            },
        )

        try:
            # Recreate channel (RobustConnection returns RobustChannel)
            self._channel = await connection.channel()

            # Register channel close callback on new channel
            self._channel.close_callbacks.add(self._on_channel_close)

            # Set prefetch count for consumer flow control
            await self._channel.set_qos(prefetch_count=self._config.prefetch_count)

            # Run registered reconnect callbacks (topology redeclare, etc.)
            await self._run_reconnect_callbacks()

            self._connected = True
            self._reconnecting = False

            logger.info(
                "Topology restored after reconnection",
                extra={
                    "reconnections": self._stats.reconnections,
                    "exchange": self._config.exchange_name,
                    "queue": self._config.queue_name,
                    "was_consuming": self._was_consuming,
                },
            )

        except Exception as e:
            self._connected = False
            self._reconnecting = False

            logger.error(
                f"Failed to restore topology after reconnection: {e}",
                exc_info=True,
                extra={
                    "reconnections": self._stats.reconnections,
                    "error": str(e),
                },
            )

    def _on_connection_close(
        self,
        connection: AbstractRobustConnection | None,
        exception: BaseException | None,
    ) -> None:
        """Handle connection closure.

        This callback is invoked when the connection is closed, either
        gracefully or due to an error. Updates the connection state and
        logs the event.

        Note: This is a synchronous callback as required by aio-pika's
        close_callbacks interface.

        Args:
            connection: The closed connection instance (may be None)
            exception: The exception that caused the closure, or None
                      if closed gracefully
        """
        # Track if we were consuming before disconnect (for potential resumption)
        if self._is_consuming is not None and self._is_consuming():
            self._was_consuming = True

        self._connected = False

        if exception:
            logger.warning(
                f"RabbitMQ connection closed unexpectedly: {exception}",
                extra={
                    "error": str(exception),
                    "error_type": type(exception).__name__,
                    "was_consuming": self._was_consuming,
                    "reconnections": self._stats.reconnections,
                },
            )
        else:
            logger.info(
                "RabbitMQ connection closed",
                extra={
                    "was_consuming": self._was_consuming,
                    "reconnections": self._stats.reconnections,
                },
            )

    def _on_channel_close(
        self,
        channel: AbstractChannel | None,
        exception: BaseException | None,
    ) -> None:
        """Handle channel closure.

        This callback is invoked when the channel is closed. The channel
        will be recreated automatically on reconnection or the next
        operation that requires it.

        Note: This is a synchronous callback as required by aio-pika's
        close_callbacks interface.

        Args:
            channel: The closed channel instance (may be None)
            exception: The exception that caused the closure, or None
                      if closed gracefully
        """
        if exception:
            logger.warning(
                f"RabbitMQ channel closed: {exception}",
                extra={
                    "error": str(exception),
                    "error_type": type(exception).__name__,
                    "exchange": self._config.exchange_name,
                    "queue": self._config.queue_name,
                },
            )
        else:
            logger.debug(
                "RabbitMQ channel closed normally",
                extra={
                    "exchange": self._config.exchange_name,
                    "queue": self._config.queue_name,
                },
            )
