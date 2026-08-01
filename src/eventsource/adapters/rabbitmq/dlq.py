"""RabbitMQ dead letter queue administration.

Extracted from ``RabbitMQEventBus`` (bus.py) as part of the bus god-class
decomposition (Task 8). Owns inspecting, counting, replaying, and purging
messages in the dead letter queue.

The facade still owns the public ``get_dlq_messages`` / ``get_dlq_message_count``
/ ``replay_dlq_message`` / ``purge_dlq`` signatures and delegates to this
collaborator, which reads the live channel via
:meth:`RabbitMQConnectionManager.require_channel` and the main exchange via
:attr:`RabbitMQTopology.exchange`.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from eventsource.adapters.rabbitmq.config import RabbitMQEventBusConfig
from eventsource.adapters.rabbitmq.models import DLQMessage, RabbitMQEventBusStats

if TYPE_CHECKING:
    from aio_pika.abc import AbstractIncomingMessage

    from eventsource.adapters.rabbitmq.connection import RabbitMQConnectionManager
    from eventsource.adapters.rabbitmq.topology import RabbitMQTopology

try:
    from aio_pika import DeliveryMode, Message
except ImportError:  # pragma: no cover - guarded by RabbitMQEventBus construction
    DeliveryMode = None  # type: ignore[assignment, misc]
    Message = None  # type: ignore[assignment, misc]

# Named explicitly so the logger name is stable and matches the facade's
# pre-extraction "eventsource.adapters.rabbitmq" logger.
logger = logging.getLogger("eventsource.adapters.rabbitmq")


class RabbitMQDLQAdmin:
    """Owns dead letter queue inspection, replay, and purge operations."""

    def __init__(
        self,
        config: RabbitMQEventBusConfig,
        connection: RabbitMQConnectionManager,
        topology: RabbitMQTopology,
        stats: RabbitMQEventBusStats,
    ) -> None:
        self._config = config
        self._connection = connection
        self._topology = topology
        self._stats = stats

        self._logger = logging.getLogger("eventsource.adapters.rabbitmq")

    async def get_messages(
        self,
        limit: int = 100,
    ) -> list[DLQMessage]:
        """Get messages from the dead letter queue for inspection.

        Retrieves messages from the DLQ without removing them. Messages are
        retrieved using basic.get and then rejected with requeue=True to
        preserve them in the queue.

        Note: This operation is not atomic. If another consumer is reading
        from the DLQ concurrently, some messages may be missed or duplicated.
        For production use, consider using a dedicated DLQ consumer.

        Args:
            limit: Maximum number of messages to retrieve (default: 100)

        Returns:
            List of DLQMessage objects containing message content and metadata.
            Returns empty list if:
            - Not connected
            - DLQ is not enabled
            - Channel is not initialized
            - An error occurs during retrieval

        Example:
            >>> messages = await bus.get_dlq_messages(limit=10)
            >>> for msg in messages:
            ...     print(f"{msg.message_id}: {msg.event_type} - {msg.dlq_reason}")
        """
        if not self._connection._connected or not self._config.enable_dlq:
            return []

        if not self._connection.channel:
            self._logger.warning(
                "Cannot get DLQ messages: channel not initialized",
                extra={
                    "dlq_queue": self._config.dlq_queue_name,
                },
            )
            return []

        messages: list[DLQMessage] = []

        try:
            channel = self._connection.require_channel()

            # Get queue reference - declare passively to ensure it exists
            dlq_queue = await channel.get_queue(
                self._config.dlq_queue_name,
            )

            for _ in range(limit):
                # Get message without auto-ack
                message = await dlq_queue.get(no_ack=False)
                if message is None:
                    # No more messages in queue
                    break

                headers = dict(message.headers or {})
                body = message.body.decode("utf-8")

                # Extract retry count with type safety
                dlq_retry_count_value = headers.get("x-dlq-retry-count")
                if dlq_retry_count_value is None:
                    dlq_retry_count = None
                elif isinstance(dlq_retry_count_value, int):
                    dlq_retry_count = dlq_retry_count_value
                else:
                    dlq_retry_count = int(str(dlq_retry_count_value))

                dlq_message = DLQMessage(
                    message_id=message.message_id,
                    routing_key=message.routing_key,
                    body=body,
                    headers=headers,
                    event_type=str(headers.get("event_type"))
                    if headers.get("event_type")
                    else None,
                    dlq_reason=str(headers.get("x-dlq-reason"))
                    if headers.get("x-dlq-reason")
                    else None,
                    dlq_error_type=str(headers.get("x-dlq-error-type"))
                    if headers.get("x-dlq-error-type")
                    else None,
                    dlq_retry_count=dlq_retry_count,
                    dlq_timestamp=str(headers.get("x-dlq-timestamp"))
                    if headers.get("x-dlq-timestamp")
                    else None,
                    original_routing_key=str(headers.get("x-original-routing-key"))
                    if headers.get("x-original-routing-key")
                    else None,
                )
                messages.append(dlq_message)

                # Reject with requeue to put message back in queue (non-destructive read)
                await message.reject(requeue=True)

            self._logger.info(
                f"Retrieved {len(messages)} messages from DLQ",
                extra={
                    "dlq_queue": self._config.dlq_queue_name,
                    "message_count": len(messages),
                    "limit": limit,
                },
            )

        except Exception as e:
            self._logger.error(
                f"Failed to get DLQ messages: {e}",
                exc_info=True,
                extra={
                    "dlq_queue": self._config.dlq_queue_name,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
            )

        return messages

    async def get_message_count(self) -> int:
        """Get the number of messages in the dead letter queue.

        Returns the current count of messages waiting in the DLQ.
        Uses passive queue declaration to query the message count
        without modifying the queue.

        Returns:
            Number of messages in the DLQ.
            Returns 0 if:
            - Not connected
            - DLQ is not enabled
            - Channel is not initialized
            - An error occurs during retrieval

        Example:
            >>> count = await bus.get_dlq_message_count()
            >>> if count > 0:
            ...     print(f"Warning: {count} messages in DLQ")
        """
        if not self._connection._connected or not self._config.enable_dlq:
            return 0

        if not self._connection.channel:
            self._logger.warning(
                "Cannot get DLQ count: channel not initialized",
                extra={
                    "dlq_queue": self._config.dlq_queue_name,
                },
            )
            return 0

        try:
            channel = self._connection.require_channel()

            # Declare queue passively to get message count
            # This will fail if queue doesn't exist, which is fine
            queue_info = await channel.declare_queue(
                name=self._config.dlq_queue_name,
                passive=True,
            )

            count = queue_info.declaration_result.message_count or 0
            self._logger.debug(
                f"DLQ message count: {count}",
                extra={
                    "dlq_queue": self._config.dlq_queue_name,
                    "message_count": count,
                },
            )
            return count

        except Exception as e:
            self._logger.error(
                f"Failed to get DLQ message count: {e}",
                exc_info=True,
                extra={
                    "dlq_queue": self._config.dlq_queue_name,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
            )
            return 0

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

    async def purge(self) -> int:
        """Remove all messages from the dead letter queue.

        Purges all messages from the DLQ. This operation is irreversible -
        all messages will be permanently deleted.

        Use with caution in production environments. Consider archiving
        or reviewing DLQ messages before purging.

        Returns:
            Number of messages that were purged.
            Returns 0 if:
            - Not connected
            - DLQ is not enabled
            - Channel is not initialized
            - An error occurs during purge

        Example:
            >>> count = await bus.purge_dlq()
            >>> print(f"Purged {count} messages from DLQ")
        """
        if not self._connection._connected or not self._config.enable_dlq:
            return 0

        if not self._connection.channel:
            self._logger.warning(
                "Cannot purge DLQ: channel not initialized",
                extra={
                    "dlq_queue": self._config.dlq_queue_name,
                },
            )
            return 0

        try:
            channel = self._connection.require_channel()

            # Get queue reference
            dlq_queue = await channel.get_queue(
                self._config.dlq_queue_name,
            )

            # Purge the queue - purge() returns PurgeOk with message_count attribute
            purge_result = await dlq_queue.purge()
            purged_count = purge_result.message_count or 0

            self._logger.info(
                f"Purged {purged_count} messages from DLQ",
                extra={
                    "dlq_queue": self._config.dlq_queue_name,
                    "purged_count": purged_count,
                },
            )

            return purged_count

        except Exception as e:
            self._logger.error(
                f"Failed to purge DLQ: {e}",
                exc_info=True,
                extra={
                    "dlq_queue": self._config.dlq_queue_name,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
            )
            return 0
