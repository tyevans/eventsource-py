"""RabbitMQ dead letter queue replay mixin.

Extracted from ``RabbitMQDLQAdmin`` (dlq.py) to keep file sizes strictly under
the 400-line warning threshold (ADR-0002).
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aio_pika.abc import AbstractIncomingMessage

    from eventsource.adapters.rabbitmq.config import RabbitMQEventBusConfig
    from eventsource.adapters.rabbitmq.connection import RabbitMQConnectionManager
    from eventsource.adapters.rabbitmq.topology import RabbitMQTopology

try:
    from aio_pika import DeliveryMode, Message
except ImportError:  # pragma: no cover - guarded by RabbitMQEventBus construction
    DeliveryMode = None  # type: ignore[assignment, misc]
    Message = None  # type: ignore[assignment, misc]


class RabbitMQDLQReplayMixin:
    """Provides DLQ replay capabilities for RabbitMQDLQAdmin."""

    _config: RabbitMQEventBusConfig
    _connection: RabbitMQConnectionManager
    _topology: RabbitMQTopology
    _logger: logging.Logger

    async def replay_message(
        self,
        message_id: str,
    ) -> bool:
        """Replay a specific message from the DLQ back to the main exchange.

        Finds a message in the DLQ by its message_id, removes DLQ-specific
        headers, resets the retry count to 0, and republishes it to the
        main exchange for reprocessing.

        The replayed message includes an 'x-replayed-from-dlq' header with
        the timestamp of when it was replayed, allowing tracking of message
        replay history.

        Note: This operation searches through the DLQ sequentially. For
        queues with many messages, this may be slow. The search is limited
        to 1000 messages to prevent excessive iteration.

        Args:
            message_id: The message_id of the DLQ message to replay

        Returns:
            True if the message was found and replayed successfully,
            False otherwise.

        Example:
            >>> success = await bus.replay_dlq_message("abc-123-def")
            >>> if success:
            ...     print("Message replayed successfully")
        """
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

            dlq_queue = await channel.get_queue(
                self._config.dlq_queue_name,
            )

            # Search for the message (with iteration limit to prevent infinite loops)
            max_search = 1000
            found = False

            for _ in range(max_search):
                message = await dlq_queue.get(no_ack=False)
                if message is None:
                    # Reached end of queue
                    break

                if message.message_id == message_id:
                    # Found the message - replay it
                    await self._replay_message(message)
                    await message.ack()  # Remove from DLQ
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
                    # Not the message we want - put back in queue
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

    async def _replay_message(
        self,
        message: AbstractIncomingMessage,
    ) -> None:
        """Republish a DLQ message to the main exchange.

        Internal helper method that creates a new message from a DLQ message
        with DLQ-specific headers removed and retry count reset.

        Headers removed:
        - x-dlq-reason
        - x-dlq-error-type
        - x-dlq-retry-count
        - x-dlq-timestamp
        - x-original-routing-key
        - x-death (RabbitMQ's built-in death header)

        Headers added/modified:
        - x-retry-count: Reset to 0
        - x-replayed-from-dlq: Timestamp of replay

        Args:
            message: The DLQ message to replay

        Raises:
            RuntimeError: If exchange is not initialized
        """
        exchange = self._topology.exchange
        if not exchange:
            raise RuntimeError("Exchange not initialized")

        # Copy headers and remove DLQ-specific ones
        headers = dict(message.headers or {})
        dlq_headers_to_remove = [
            "x-dlq-reason",
            "x-dlq-error-type",
            "x-dlq-retry-count",
            "x-dlq-timestamp",
            "x-original-routing-key",
            "x-death",  # RabbitMQ's built-in death header
        ]
        for key in dlq_headers_to_remove:
            headers.pop(key, None)

        # Reset retry count and add replay marker
        headers["x-retry-count"] = 0
        headers["x-replayed-from-dlq"] = datetime.now(UTC).isoformat()

        # Get original routing key (from our custom header or message routing key)
        original_headers = message.headers or {}
        original_routing_key = original_headers.get(
            "x-original-routing-key", message.routing_key or ""
        )

        # Create replay message
        replay_message = Message(
            body=message.body,
            content_type=message.content_type,
            content_encoding=message.content_encoding,
            delivery_mode=DeliveryMode.PERSISTENT,
            message_id=message.message_id,
            headers=headers,
        )

        await exchange.publish(
            replay_message,
            routing_key=str(original_routing_key),
        )

        self._logger.debug(
            "Republished message to exchange",
            extra={
                "message_id": message.message_id,
                "routing_key": original_routing_key,
                "exchange": self._config.exchange_name,
            },
        )


__all__ = ["RabbitMQDLQReplayMixin"]
