"""Retry scheduling and republish path mixin for RabbitMQ consumer."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from eventsource.adapters._bus.headers import read_header_str
from eventsource.adapters.rabbitmq.consumer_dlq import RabbitMQConsumerDLQMixin
from eventsource.observability.error_summary import error_summary

if TYPE_CHECKING:
    from aio_pika.abc import AbstractIncomingMessage

    from eventsource.adapters._bus.retry import RetryPolicy
    from eventsource.adapters._bus.retry_scheduler import RetryScheduler
    from eventsource.adapters.rabbitmq.config import RabbitMQEventBusConfig
    from eventsource.adapters.rabbitmq.topology import RabbitMQTopology


RETRY_CAPACITY_EXCEEDED = "retry_capacity_exceeded"


class RabbitMQConsumerRetryMixin(RabbitMQConsumerDLQMixin):
    """Mixin providing retry handling for RabbitMQConsumer."""

    _config: RabbitMQEventBusConfig
    _topology: RabbitMQTopology
    _retry_policy: RetryPolicy
    _logger: logging.Logger
    _retry_scheduler: RetryScheduler

    def _calculate_retry_delay(self, retry_count: int) -> float:
        """Calculate the delay in seconds before the next retry via shared RetryPolicy."""
        return self._retry_policy.delay_for(retry_count)

    async def _handle_failed_message(
        self, message: AbstractIncomingMessage, error: Exception, retry_count: int
    ) -> None:
        """Handle a failed message: retry with exponential backoff or route to DLQ."""
        headers = message.headers or {}
        # event_type is publisher-controlled: it goes in extra only, never in message text.
        event_type_name = read_header_str(headers.get("event_type"), "unknown") or "unknown"

        if retry_count >= self._config.max_retries:
            await self._send_to_dlq(message, error, retry_count)
            await message.ack()
            self._logger.warning(
                f"Message sent to DLQ after {retry_count} retries",
                extra={
                    "message_id": message.message_id,
                    "event_type": event_type_name,
                    "retry_count": retry_count,
                    "error": error_summary(error),
                    "dlq_queue": self._config.dlq_queue_name,
                },
            )
            return

        delay = self._calculate_retry_delay(retry_count)
        if delay > 0 and self._retry_scheduler.is_full:
            await self._dead_letter_at_capacity(message, error, retry_count)
            return
        self._logger.info(
            f"Scheduling retry {retry_count + 1}/{self._config.max_retries} after {delay:.2f}s delay",
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

    async def _dead_letter_at_capacity(
        self, message: AbstractIncomingMessage, error: Exception, retry_count: int
    ) -> None:
        """Dead-letter a deferred retry when the retry scheduler is full, then acknowledge."""
        self._logger.error(
            "Retry scheduler full, sending message to DLQ",
            extra={"message_id": message.message_id, "retry_count": retry_count},
        )
        await self._send_to_dlq(message, error, retry_count, reason=RETRY_CAPACITY_EXCEEDED)
        await message.ack()

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


__all__ = ["RabbitMQConsumerRetryMixin"]
