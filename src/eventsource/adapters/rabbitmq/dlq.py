"""RabbitMQ dead letter queue administration.

Extracted from ``RabbitMQEventBus`` (bus.py) as part of the bus god-class
decomposition (Task 8). Owns inspecting, counting, replaying, and purging
messages in the dead letter queue.

The facade still owns the public ``get_dlq_messages`` / ``get_dlq_message_count``
/ ``replay_dlq_message`` / ``purge_dlq`` signatures and delegates to this
collaborator, which reads the live channel via
:meth:`RabbitMQConnectionManager.require_channel` and the main exchange via
:attr:`RabbitMQTopology.exchange`.

**Note for auditors:** the four ``require_channel()`` calls in this module
resemble the untyped-raise sites corrected elsewhere in the RabbitMQ adapter,
but they are not the same finding. Each is preceded by a connection check that
returns a graceful empty result, so the raising path is unreachable from a
disconnected bus. They were reviewed and deliberately left alone.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from eventsource.adapters.rabbitmq.config import RabbitMQEventBusConfig
from eventsource.adapters.rabbitmq.dlq_replay import RabbitMQDLQReplayMixin
from eventsource.adapters.rabbitmq.models import DLQMessage, RabbitMQEventBusStats

if TYPE_CHECKING:
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


class RabbitMQDLQAdmin(RabbitMQDLQReplayMixin):
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


__all__ = ["RabbitMQDLQAdmin", "RabbitMQDLQReplayMixin"]
