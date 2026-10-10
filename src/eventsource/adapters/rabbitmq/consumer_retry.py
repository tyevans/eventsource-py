"""Retry and DLQ write path mixin for RabbitMQ consumer.

Provides retry delay calculation, retry republishing with exponential backoff,
and DLQ routing with error metadata.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

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
        """Calculate the delay before the next retry.

        Delegates to the shared RetryPolicy so Kafka and RabbitMQ cannot drift
        apart again.

        Args:
            retry_count: Zero-based retry attempt number.

        Returns:
            Delay in seconds, with symmetric jitter applied.
        """
        return self._retry_policy.delay_for(retry_count)

    async def _handle_failed_message(
        self,
        message: AbstractIncomingMessage,
        error: Exception,
        retry_count: int,
    ) -> None:
        """Handle a failed message - retry with backoff or route to DLQ.

        This method implements the retry logic with exponential backoff. When
        a message fails processing:
        1. If retry_count < max_retries: republish with incremented retry count
           after applying exponential backoff delay
        2. If retry_count >= max_retries: send to DLQ with failure metadata

        Args:
            message: The failed message
            error: The exception that caused the failure
            retry_count: Current retry count (from x-retry-count header)
        """
        headers = message.headers or {}
        event_type_name = str(headers.get("event_type", "unknown"))

        if retry_count >= self._config.max_retries:
            # Max retries exceeded - send to DLQ
            await self._send_to_dlq(message, error, retry_count)
            await message.ack()  # Ack to remove from main queue

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
        else:
            # Calculate backoff delay
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
                await message.ack()  # Ack original, republished copy will be processed

                self._logger.info(
                    f"Republished message for retry {retry_count + 1}",
                    extra={
                        "message_id": message.message_id,
                        "event_type": event_type_name,
                        "retry_count": retry_count + 1,
                    },
                )

            # Non-blocking async retry scheduling: do not block the consume loop
            if delay > 0:
                self._retry_scheduler.schedule(
                    delay,
                    _do_retry,
                    name=f"rabbitmq-retry-{message.message_id}",
                )
            else:
                await _do_retry()

    async def _republish_for_retry(
        self,
        original_message: AbstractIncomingMessage,
        new_retry_count: int,
    ) -> None:
        """Republish a message with incremented retry count.

        Creates a new message with updated headers containing the incremented
        retry count and timestamp of the retry attempt. The message body and
        other properties are preserved from the original message.

        Args:
            original_message: The original message to retry
            new_retry_count: The new retry count value

        Raises:
            RuntimeError: If exchange is not initialized
        """
        if not self._topology.exchange:
            raise RuntimeError("Exchange not initialized")

        # Copy headers and update retry count
        headers = dict(original_message.headers or {})
        headers["x-retry-count"] = new_retry_count
        headers["x-last-retry-at"] = datetime.now(UTC).isoformat()

        # Create new message with updated headers
        retry_message = Message(
            body=original_message.body,
            content_type=original_message.content_type,
            content_encoding=original_message.content_encoding,
            delivery_mode=DeliveryMode.PERSISTENT,
            message_id=original_message.message_id,
            headers=headers,
        )

        # Republish to exchange with original routing key
        routing_key = original_message.routing_key or ""
        await self._topology.exchange.publish(retry_message, routing_key=routing_key)

    async def _send_to_dlq(
        self,
        message: AbstractIncomingMessage,
        error: Exception,
        retry_count: int,
    ) -> None:
        """Send a failed message to the dead letter queue.

        Publishes the failed message to the DLQ exchange with additional
        headers containing failure metadata:
        - x-dlq-reason: Error message
        - x-dlq-error-type: Exception class name
        - x-dlq-retry-count: Number of retries before DLQ
        - x-dlq-timestamp: When message was sent to DLQ
        - x-original-routing-key: Original routing key

        Args:
            message: The failed message
            error: The exception that caused the failure
            retry_count: Final retry count before DLQ

        Note:
            If DLQ is not enabled or DLQ exchange is not initialized,
            this method logs a warning and returns without action.
        """
        if not self._config.enable_dlq or not self._topology.dlq_exchange:
            self._logger.warning(
                "DLQ not enabled or not initialized, message will be lost",
                extra={"message_id": message.message_id},
            )
            return

        headers = dict(message.headers or {})
        event_type_name = str(headers.get("event_type", "unknown"))

        # Add failure metadata to headers
        headers["x-dlq-reason"] = str(error)
        headers["x-dlq-error-type"] = type(error).__name__
        headers["x-dlq-retry-count"] = retry_count
        headers["x-dlq-timestamp"] = datetime.now(UTC).isoformat()
        headers["x-original-routing-key"] = message.routing_key or ""

        # Create DLQ message with failure metadata
        dlq_message = Message(
            body=message.body,
            content_type=message.content_type,
            content_encoding=message.content_encoding,
            delivery_mode=DeliveryMode.PERSISTENT,
            message_id=message.message_id,
            headers=headers,
        )

        # Publish to DLQ exchange with queue name as routing key
        await self._topology.dlq_exchange.publish(
            dlq_message,
            routing_key=self._config.queue_name,
        )

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
