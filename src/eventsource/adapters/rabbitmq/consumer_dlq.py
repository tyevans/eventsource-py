"""Dead letter write path and unexpected-error containment for the RabbitMQ consumer.

Event payloads never reach log message text or DLQ reason headers. Errors are
reduced with ``error_summary`` and no traceback is logged on paths that handle
message payloads (see ``_contain_unexpected_error``).
"""

from __future__ import annotations

import contextlib
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from eventsource.adapters._bus.headers import read_header_int, read_header_str
from eventsource.observability.error_summary import error_summary

if TYPE_CHECKING:
    from aio_pika import Message
    from aio_pika.abc import AbstractIncomingMessage

    from eventsource.adapters.rabbitmq.config import RabbitMQEventBusConfig
    from eventsource.adapters.rabbitmq.models import RabbitMQEventBusStats
    from eventsource.adapters.rabbitmq.topology import RabbitMQTopology

try:
    from aio_pika import DeliveryMode, Message
except ImportError:
    Message = None  # type: ignore[assignment, misc]
    DeliveryMode = None  # type: ignore[assignment, misc]


class RabbitMQConsumerDLQMixin:
    """Mixin providing the DLQ write path and unexpected-error containment for RabbitMQConsumer."""

    _config: RabbitMQEventBusConfig
    _topology: RabbitMQTopology
    _stats: RabbitMQEventBusStats
    _logger: logging.Logger

    def _build_persistent_message(
        self, source: AbstractIncomingMessage, headers: dict[str, Any]
    ) -> Message:
        """Clone an incoming message with updated headers and persistent delivery mode."""
        return Message(
            body=source.body,
            content_type=source.content_type,
            content_encoding=source.content_encoding,
            delivery_mode=DeliveryMode.PERSISTENT,
            message_id=source.message_id,
            headers=headers,
        )

    async def _contain_unexpected_error(
        self, message: AbstractIncomingMessage, error: Exception
    ) -> None:
        """Dead-letter a message whose processing raised outside the normal error path.

        Keeps the consume loop alive: the message is sent to the DLQ and acknowledged.
        If that also fails, it is rejected without requeue so it cannot loop forever.

        No ``exc_info``: this path catches errors from any stage, including handler
        and decode failures. A traceback also prints the chained ``__context__``,
        and that context can carry payload text.
        """
        self._stats.events_processed_failed += 1
        summary = error_summary(error)
        extra = {"message_id": message.message_id, "error": summary, "error_type": type(error).__name__}  # fmt: skip
        self._logger.error("Unexpected error while processing message; dead-lettering and continuing", extra=extra)  # fmt: skip
        try:
            retry_count = read_header_int((message.headers or {}).get("x-retry-count"), 0)
            await self._send_to_dlq(message, error, retry_count)
            await message.ack()
        except Exception:
            # No exc_info: the failure is chained to `error` (this runs inside its handler),
            # so a traceback would print the payload-bearing original error.
            self._logger.error("Could not dead-letter message; rejecting without requeue", extra=extra)  # fmt: skip
            with contextlib.suppress(Exception):
                await message.reject(requeue=False)

    async def _send_to_dlq(
        self,
        message: AbstractIncomingMessage,
        error: Exception,
        retry_count: int,
        *,
        reason: str | None = None,
    ) -> None:
        """Send a failed message to the dead letter queue with failure metadata headers.

        ``reason`` overrides the summarized error for the ``x-dlq-reason`` header.
        """
        if not self._config.enable_dlq or not self._topology.dlq_exchange:
            self._logger.warning(
                "DLQ not enabled or not initialized, message will be lost",
                extra={"message_id": message.message_id},
            )
            return

        headers = dict(message.headers or {})
        event_type_name = read_header_str(headers.get("event_type"), "unknown") or "unknown"
        summary = reason if reason is not None else error_summary(error)
        headers["x-dlq-reason"] = summary
        headers["x-dlq-error-type"] = type(error).__name__
        headers["x-dlq-retry-count"] = retry_count
        headers["x-dlq-timestamp"] = datetime.now(UTC).isoformat()
        headers["x-original-routing-key"] = message.routing_key or ""

        dlq_message = self._build_persistent_message(message, headers)
        await self._topology.dlq_exchange.publish(dlq_message, routing_key=self._config.queue_name)
        self._stats.messages_sent_to_dlq += 1
        # event_type is publisher-controlled: it travels in extra only, never in the message text.
        self._logger.warning(
            f"Sent message to DLQ after {retry_count} retries",
            extra={
                "message_id": message.message_id,
                "event_type": event_type_name,
                "retry_count": retry_count,
                "error": summary,
                "error_type": type(error).__name__,
                "dlq_queue": self._config.dlq_queue_name,
            },
        )


__all__ = ["RabbitMQConsumerDLQMixin"]
