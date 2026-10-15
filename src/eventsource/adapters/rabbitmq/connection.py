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
        self._config, self._stats = config, stats
        self._connection: AbstractRobustConnection | None = None
        self._channel: AbstractChannel | None = None
        self._connected = self._reconnecting = self._was_consuming = False
        self._lock = asyncio.Lock()
        self._reconnect_callbacks: list[Callable[[], Awaitable[None]]] = []
        self._is_consuming: Callable[[], bool] | None = None

    @property
    def is_connected(self) -> bool:
        return self._connected and self._connection is not None and not self._connection.is_closed

    @property
    def is_reconnecting(self) -> bool:
        return self._reconnecting

    @property
    def connection(self) -> AbstractRobustConnection | None:
        return self._connection

    @property
    def channel(self) -> AbstractChannel | None:
        return self._channel

    def require_channel(self) -> AbstractChannel:
        if self._channel is None:
            raise RuntimeError("Not connected to RabbitMQ")
        return self._channel

    def _create_ssl_context(self) -> ssl.SSLContext | None:
        return create_ssl_context(self._config)

    def _sanitize_url(self, url: str) -> str:
        return sanitize_url(url)

    async def connect(self) -> None:
        """Connect to RabbitMQ, create the channel, and run reconnect callbacks."""
        async with self._lock:
            if self._connected:
                logger.warning("RabbitMQEventBus already connected")
                return
            ssl_context: ssl.SSLContext | None = None
            cfg = self._config
            s_url = self._sanitize_url(cfg.rabbitmq_url)
            try:
                ssl_context = self._create_ssl_context()
                connect_kwargs: dict[str, Any] = {
                    "heartbeat": cfg.heartbeat,
                    "reconnect_interval": cfg.reconnect_delay,
                }
                if ssl_context is not None:
                    connect_kwargs["ssl_context"] = ssl_context
                if cfg.ssl_options:
                    connect_kwargs.update(cfg.ssl_options)
                self._connection = await aio_pika.connect_robust(cfg.rabbitmq_url, **connect_kwargs)
                self._connection.reconnect_callbacks.add(self._on_reconnect)  # type: ignore[arg-type]
                self._connection.close_callbacks.add(self._on_connection_close)  # type: ignore[arg-type]
                self._channel = await self._connection.channel()
                self._channel.close_callbacks.add(self._on_channel_close)
                await self._channel.set_qos(prefetch_count=cfg.prefetch_count)
                await self._run_reconnect_callbacks()
                self._connected = True
                self._stats.connected_at = datetime.now(UTC)
                tls_on = ssl_context is not None
                tls_status = "TLS" if tls_on else "plaintext"
                info_ex = {"rabbitmq_url": s_url, "exchange": cfg.exchange_name}
                info_ex.update(queue=cfg.queue_name, consumer_group=cfg.consumer_group)
                info_ex.update(dlq_enabled=cfg.enable_dlq, tls_enabled=tls_on)
                msg = f"Connected to RabbitMQ ({tls_status}) and initialized topology"
                logger.info(msg, extra=info_ex)
            except ssl.SSLError as e:
                ex = {"rabbitmq_url": s_url, "error": str(e), "error_type": type(e).__name__}
                ex["tls_enabled"] = ssl_context is not None
                logger.error(f"SSL error connecting to RabbitMQ: {e}", exc_info=True, extra=ex)
                await self.disconnect()
                raise
            except Exception as e:
                ex = {"rabbitmq_url": s_url, "exchange": cfg.exchange_name, "error": str(e)}
                ex["error_type"] = type(e).__name__
                logger.error(f"Failed to connect to RabbitMQ: {e}", exc_info=True, extra=ex)
                await self.disconnect()
                raise

    async def disconnect(self) -> None:
        if self._channel and not self._channel.is_closed:
            await self._channel.close()
            self._channel = None
        if self._connection and not self._connection.is_closed:
            await self._connection.close()
            self._connection = None
        self._connected, self._stats.connected_at = False, None
        cfg = self._config
        ex = {"exchange": cfg.exchange_name, "queue": cfg.queue_name}
        ex["consumer_group"] = cfg.consumer_group
        logger.info("Disconnected from RabbitMQ", extra=ex)

    async def force_disconnect(self) -> None:
        for attr in ("_channel", "_connection"):
            obj = getattr(self, attr)
            if obj:
                try:
                    if not obj.is_closed:
                        await obj.close()
                except Exception:  # nosec B110 - intentionally suppress during force disconnect
                    pass
                setattr(self, attr, None)
        self._connected = False

    def health_slice(self) -> dict[str, Any]:
        errors: list[str] = []
        if not self._connection:
            connection_status = "disconnected"
            errors.append("Not connected to RabbitMQ")
        elif self._connection.is_closed:
            connection_status = "closed"
            errors.append("RabbitMQ connection is closed")
        else:
            connection_status = "connected"
        if not self._channel:
            channel_status = "not_initialized"
            errors.append("Channel not initialized")
        elif self._channel.is_closed:
            channel_status = "closed"
            errors.append("AMQP channel is closed")
        else:
            channel_status = "open"
        return {
            "healthy": not errors,
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
