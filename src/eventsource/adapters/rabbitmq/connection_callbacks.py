"""Connection and channel callbacks mixin for RabbitMQ connection manager."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aio_pika.abc import AbstractChannel, AbstractRobustConnection

    from eventsource.adapters.rabbitmq.config import RabbitMQEventBusConfig
    from eventsource.adapters.rabbitmq.models import RabbitMQEventBusStats

logger = logging.getLogger("eventsource.adapters.rabbitmq.connection")


class RabbitMQConnectionCallbacksMixin:
    """Mixin providing callback management and handlers for RabbitMQConnectionManager."""

    _config: RabbitMQEventBusConfig
    _stats: RabbitMQEventBusStats
    _connection: AbstractRobustConnection | None
    _channel: AbstractChannel | None
    _connected: bool
    _reconnecting: bool
    _was_consuming: bool
    _reconnect_callbacks: list[Callable[[], Awaitable[None]]]
    _is_consuming: Callable[[], bool] | None

    def on_reconnect(self, callback: Callable[[], Awaitable[None]]) -> None:
        """Register an async callback fired after a successful reconnect."""
        self._reconnect_callbacks.append(callback)

    async def _run_reconnect_callbacks(self) -> None:
        """Invoke all registered reconnect callbacks, in order."""
        for cb in self._reconnect_callbacks:
            await cb()

    async def _on_reconnect(self, connection: AbstractRobustConnection) -> None:
        """Handle connection restoration after disconnection."""
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
            self._channel = await connection.channel()
            self._channel.close_callbacks.add(self._on_channel_close)
            await self._channel.set_qos(prefetch_count=self._config.prefetch_count)
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
                extra={"reconnections": self._stats.reconnections, "error": str(e)},
            )

    def _on_connection_close(
        self,
        connection: AbstractRobustConnection | None,
        exception: BaseException | None,
    ) -> None:
        """Handle connection closure."""
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
        """Handle channel closure."""
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
                extra={"exchange": self._config.exchange_name, "queue": self._config.queue_name},
            )
