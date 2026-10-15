"""RabbitMQ dead letter queue replay mixin."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aio_pika import DeliveryMode, Message
    from aio_pika.abc import AbstractIncomingMessage

    from eventsource.adapters.rabbitmq.config import RabbitMQEventBusConfig
    from eventsource.adapters.rabbitmq.connection import RabbitMQConnectionManager
    from eventsource.adapters.rabbitmq.topology import RabbitMQTopology
else:
    try:
        from aio_pika import DeliveryMode, Message
    except ImportError:  # pragma: no cover - guarded by RabbitMQEventBus construction
        DeliveryMode = Message = None


class RabbitMQDLQReplayMixin:
    """Provides DLQ replay capabilities for RabbitMQDLQAdmin."""

    _config: RabbitMQEventBusConfig
    _connection: RabbitMQConnectionManager
    _topology: RabbitMQTopology
    _logger: logging.Logger

    async def replay_message(self, message_id: str) -> bool:
        """Replay a specific message from the DLQ back to the main exchange."""
        if not self._connection._connected or not self._topology.exchange:
            self._logger.warning(
                "Cannot replay DLQ message: not connected or exchange not initialized",
                extra={
                    "message_id": message_id,
                    "dlq_queue": self._config.dlq_queue_name,
                    "is_connected": self._connection._connected,
                    "exchange_initialized": self._topology.exchange is not None,
                },
            )
            return False

        if not self._connection.channel or not self._config.enable_dlq:
            self._logger.warning(
                "Cannot replay DLQ message: channel not initialized or DLQ disabled",
                extra={
                    "message_id": message_id,
                    "dlq_queue": self._config.dlq_queue_name,
                    "dlq_enabled": self._config.enable_dlq,
                    "channel_initialized": self._connection.channel is not None,
                },
            )
            return False

        try:
            channel = self._connection.require_channel()
            dlq_queue = await channel.get_queue(self._config.dlq_queue_name)
            max_search = 1000
            found = False

            for _ in range(max_search):
                message = await dlq_queue.get(no_ack=False)
                if message is None:
                    break

                if message.message_id == message_id:
                    await self._replay_message(message)
                    await message.ack()
                    found = True
                    self._logger.info(
                        f"Replayed DLQ message: {message_id}",
                        extra={
                            "message_id": message_id,
                            "event_type": (message.headers or {}).get("event_type"),
                            "dlq_queue": self._config.dlq_queue_name,
                        },
                    )
                    break
                else:
                    await message.reject(requeue=True)

            if not found:
                self._logger.warning(
                    f"DLQ message not found for replay: {message_id}",
                    extra={
                        "message_id": message_id,
                        "dlq_queue": self._config.dlq_queue_name,
                        "max_search": max_search,
                    },
                )
            return found

        except Exception as e:
            self._logger.error(
                f"Failed to replay DLQ message {message_id}: {e}",
                exc_info=True,
                extra={
                    "message_id": message_id,
                    "dlq_queue": self._config.dlq_queue_name,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
            )
            return False

    async def _replay_message(self, message: AbstractIncomingMessage) -> None:
        """Republish a DLQ message to the main exchange with DLQ headers stripped."""
        exchange = self._topology.exchange
        if not exchange:
            raise RuntimeError("Exchange not initialized")

        headers = dict(message.headers or {})
        for key in (
            "x-dlq-reason",
            "x-dlq-error-type",
            "x-dlq-retry-count",
            "x-dlq-timestamp",
            "x-original-routing-key",
            "x-death",
        ):
            headers.pop(key, None)

        headers["x-retry-count"] = 0
        headers["x-replayed-from-dlq"] = datetime.now(UTC).isoformat()

        original_headers = message.headers or {}
        original_routing_key = original_headers.get(
            "x-original-routing-key", message.routing_key or ""
        )

        replay_message = Message(
            body=message.body,
            content_type=message.content_type,
            content_encoding=message.content_encoding,
            delivery_mode=DeliveryMode.PERSISTENT,
            message_id=message.message_id,
            headers=headers,
        )

        await exchange.publish(replay_message, routing_key=str(original_routing_key))
        self._logger.debug(
            "Republished message to exchange",
            extra={
                "message_id": message.message_id,
                "routing_key": original_routing_key,
                "exchange": self._config.exchange_name,
            },
        )


__all__ = ["RabbitMQDLQReplayMixin"]
