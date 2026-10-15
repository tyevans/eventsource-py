"""Retry and DLQ write path mixin for RabbitMQ consumer."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from aio_pika import DeliveryMode, Message
    from aio_pika.abc import AbstractIncomingMessage

    from eventsource.adapters._bus.retry import RetryPolicy
    from eventsource.adapters._bus.retry_scheduler import RetryScheduler
    from eventsource.adapters.rabbitmq.config import RabbitMQEventBusConfig
    from eventsource.adapters.rabbitmq.models import RabbitMQEventBusStats
    from eventsource.adapters.rabbitmq.topology import RabbitMQTopology

try:
    from aio_pika import DeliveryMode, Message
except ImportError:
    Message = None  # type: ignore[assignment, misc]
    DeliveryMode = None  # type: ignore[assignment, misc]


class RabbitMQConsumerRetryMixin:
    """Mixin providing retry handling and DLQ write path for RabbitMQConsumer."""

    _config: RabbitMQEventBusConfig
    _topology: RabbitMQTopology
    _stats: RabbitMQEventBusStats
    _retry_policy: RetryPolicy
    _logger: logging.Logger
    _retry_scheduler: RetryScheduler

    def _calculate_retry_delay(self, retry_count: int) -> float:
        """Calculate the delay in seconds before the next retry via shared RetryPolicy."""
        return self._retry_policy.delay_for(retry_count)

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

    async def _handle_failed_message(
        self, message: AbstractIncomingMessage, error: Exception, retry_count: int
    ) -> None:
        """Handle a failed message: retry with exponential backoff or route to DLQ."""
        headers = message.headers or {}
        event_type_name = str(headers.get("event_type", "unknown"))

        if retry_count >= self._config.max_retries:
            await self._send_to_dlq(message, error, retry_count)
            await message.ack()
            self._logger.warning(
                f"Message sent to DLQ after {retry_count} retries: {event_type_name}",
                extra={
                    "message_id": message.message_id,
                    "event_type": event_type_name,
                    "retry_count": retry_count,
                    "error": str(error),
                    "dlq_queue": self._config.dlq_queue_name,
                },
            )
            return

        delay = self._calculate_retry_delay(retry_count)
        self._logger.info(
            f"Scheduling retry {retry_count + 1}/{self._config.max_retries} "
            f"for {event_type_name} after {delay:.2f}s delay",
            extra={
                "message_id": message.message_id,
                "event_type": event_type_name,
                "retry_count": retry_count,
                "next_retry": retry_count + 1,
                "max_retries": self._config.max_retries,
                "delay_seconds": delay,
            },
        )

        async def _do_retry() -> None:
            await self._republish_for_retry(message, retry_count + 1)
            await message.ack()
            self._logger.info(
                f"Republished message for retry {retry_count + 1}",
                extra={
                    "message_id": message.message_id,
                    "event_type": event_type_name,
                    "retry_count": retry_count + 1,
                },
            )

        if delay > 0:
            self._retry_scheduler.schedule(
                delay, _do_retry, name=f"rabbitmq-retry-{message.message_id}"
            )
        else:
            await _do_retry()

    async def _republish_for_retry(
        self, original_message: AbstractIncomingMessage, new_retry_count: int
    ) -> None:
        """Republish a message with incremented retry count and retry timestamp."""
        if not self._topology.exchange:
            raise RuntimeError("Exchange not initialized")

        headers = dict(original_message.headers or {})
        headers["x-retry-count"] = new_retry_count
        headers["x-last-retry-at"] = datetime.now(UTC).isoformat()
        retry_message = self._build_persistent_message(original_message, headers)
        await self._topology.exchange.publish(
            retry_message, routing_key=original_message.routing_key or ""
        )

    async def _send_to_dlq(
        self, message: AbstractIncomingMessage, error: Exception, retry_count: int
    ) -> None:
        """Send a failed message to the dead letter queue with failure metadata headers."""
        if not self._config.enable_dlq or not self._topology.dlq_exchange:
            self._logger.warning(
                "DLQ not enabled or not initialized, message will be lost",
                extra={"message_id": message.message_id},
            )
            return

        headers = dict(message.headers or {})
        event_type_name = str(headers.get("event_type", "unknown"))
        headers["x-dlq-reason"] = str(error)
        headers["x-dlq-error-type"] = type(error).__name__
        headers["x-dlq-retry-count"] = retry_count
        headers["x-dlq-timestamp"] = datetime.now(UTC).isoformat()
        headers["x-original-routing-key"] = message.routing_key or ""

        dlq_message = self._build_persistent_message(message, headers)
        await self._topology.dlq_exchange.publish(dlq_message, routing_key=self._config.queue_name)
        self._stats.messages_sent_to_dlq += 1
        self._logger.warning(
            f"Sent message to DLQ after {retry_count} retries: {event_type_name}",
            extra={
                "message_id": message.message_id,
                "event_type": event_type_name,
                "retry_count": retry_count,
                "error": str(error),
                "error_type": type(error).__name__,
                "dlq_queue": self._config.dlq_queue_name,
            },
        )
