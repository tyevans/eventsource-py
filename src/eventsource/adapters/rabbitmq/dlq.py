"""RabbitMQ dead letter queue administration (inspecting, counting, replaying, purging).

Note: ``require_channel()`` calls below are preceded by connection/channel checks
that return graceful empty results when disconnected.
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

    def _log_dlq_error(self, message: str, exc: Exception) -> None:
        self._logger.error(
            f"{message}: {exc}",
            exc_info=True,
            extra={
                "dlq_queue": self._config.dlq_queue_name,
                "error": str(exc),
                "error_type": type(exc).__name__,
            },
        )

    async def get_messages(self, limit: int = 100) -> list[DLQMessage]:
        """Get messages from the dead letter queue for inspection without removing them."""
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
                retry_val = headers.get("x-dlq-retry-count")
                if retry_val is None:
                    dlq_retry_count = None
                elif isinstance(retry_val, int):
                    dlq_retry_count = retry_val
                else:
                    dlq_retry_count = int(str(retry_val))

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
            self._log_dlq_error("Failed to get DLQ messages", e)

        return messages

    async def get_message_count(self) -> int:
        """Get the number of messages waiting in the dead letter queue via passive declaration."""
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
            self._log_dlq_error("Failed to get DLQ message count", e)
            return 0

    async def purge(self) -> int:
        """Irreversibly remove all messages from the dead letter queue and return purged count."""
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
            self._log_dlq_error("Failed to purge DLQ", e)
            return 0


__all__ = ["RabbitMQDLQAdmin", "RabbitMQDLQReplayMixin"]
