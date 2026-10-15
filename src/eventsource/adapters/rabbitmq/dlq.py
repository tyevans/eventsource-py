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

    async def get_messages(self, limit: int = 100) -> list[DLQMessage]:
        """Get messages from the dead letter queue for inspection without removing them.

        Retrieves messages using basic.get and rejects with requeue=True to preserve them.
        Note: Not atomic under concurrent DLQ consumers. Returns empty list if disconnected,
        DLQ disabled, channel uninitialized, or on error.
        """
        if not self._connection._connected or not self._config.enable_dlq:
            return []

        if not self._connection.channel:
            self._logger.warning(
                "Cannot get DLQ messages: channel not initialized",
                extra={"dlq_queue": self._config.dlq_queue_name},
            )
            return []

        messages: list[DLQMessage] = []
        try:
            channel = self._connection.require_channel()
            dlq_queue = await channel.get_queue(self._config.dlq_queue_name)

            for _ in range(limit):
                message = await dlq_queue.get(no_ack=False)
                if message is None:
                    break

                headers: dict[str, object] = dict(message.headers or {})
                body = message.body.decode("utf-8")

                dlq_retry_count_value = headers.get("x-dlq-retry-count")
                if dlq_retry_count_value is None:
                    dlq_retry_count = None
                elif isinstance(dlq_retry_count_value, int):
                    dlq_retry_count = dlq_retry_count_value
                else:
                    dlq_retry_count = int(str(dlq_retry_count_value))

                def _hdr(key: str, h: dict[str, object] = headers) -> str | None:
                    val = h.get(key)
                    return str(val) if val else None

                messages.append(
                    DLQMessage(
                        message_id=message.message_id,
                        routing_key=message.routing_key,
                        body=body,
                        headers=headers,
                        event_type=_hdr("event_type"),
                        dlq_reason=_hdr("x-dlq-reason"),
                        dlq_error_type=_hdr("x-dlq-error-type"),
                        dlq_retry_count=dlq_retry_count,
                        dlq_timestamp=_hdr("x-dlq-timestamp"),
                        original_routing_key=_hdr("x-original-routing-key"),
                    )
                )
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
        """Get the number of messages waiting in the dead letter queue via passive declaration.

        Returns 0 if disconnected, DLQ disabled, channel uninitialized, or on error.
        """
        if not self._connection._connected or not self._config.enable_dlq:
            return 0

        if not self._connection.channel:
            self._logger.warning(
                "Cannot get DLQ count: channel not initialized",
                extra={"dlq_queue": self._config.dlq_queue_name},
            )
            return 0

        try:
            channel = self._connection.require_channel()
            queue_info = await channel.declare_queue(name=self._config.dlq_queue_name, passive=True)
            count = queue_info.declaration_result.message_count or 0
            self._logger.debug(
                f"DLQ message count: {count}",
                extra={"dlq_queue": self._config.dlq_queue_name, "message_count": count},
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
        """Irreversibly remove all messages from the dead letter queue and return purged count.

        Returns 0 if disconnected, DLQ disabled, channel uninitialized, or on error.
        """
        if not self._connection._connected or not self._config.enable_dlq:
            return 0

        if not self._connection.channel:
            self._logger.warning(
                "Cannot purge DLQ: channel not initialized",
                extra={"dlq_queue": self._config.dlq_queue_name},
            )
            return 0

        try:
            channel = self._connection.require_channel()
            dlq_queue = await channel.get_queue(self._config.dlq_queue_name)
            purge_result = await dlq_queue.purge()
            purged_count = purge_result.message_count or 0
            self._logger.info(
                f"Purged {purged_count} messages from DLQ",
                extra={"dlq_queue": self._config.dlq_queue_name, "purged_count": purged_count},
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
