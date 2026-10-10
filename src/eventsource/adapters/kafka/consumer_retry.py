"""Retry and DLQ routing mixin for KafkaConsumerLoop.

Governed by ADR-0002 (<500 lines per module).
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from eventsource.adapters.kafka.consumer_helpers import (
    create_dlq_headers,
    get_header_value,
    warn_uncommitted,
)

if TYPE_CHECKING:
    from eventsource.adapters._bus.retry import RetryPolicy
    from eventsource.adapters.kafka.config import KafkaEventBusConfig
    from eventsource.adapters.kafka.metrics import KafkaEventBusMetrics
    from eventsource.adapters.kafka.models import KafkaEventBusStats

logger = logging.getLogger("eventsource.bus.kafka")


class KafkaConsumerRetryMixin:
    """Mixin providing retry handling, delay calculation, and DLQ dispatch."""

    _config: KafkaEventBusConfig
    _stats: KafkaEventBusStats
    _metrics: KafkaEventBusMetrics | None
    _retry_policy: RetryPolicy
    _producer: Any
    _consumer: Any

    def _get_header_value(
        self,
        headers: list[tuple[str, bytes]] | tuple[tuple[str, bytes], ...] | None,
        key: str,
    ) -> str | None:
        return get_header_value(headers, key)

    async def _handle_processing_error(
        self,
        message: Any,
        error: Exception,
        retry_count: int,
    ) -> None:
        """Handle a message processing error.

        Implements non-blocking retry by republishing the message with an
        incremented retry count. After max_retries, the message is sent to DLQ.
        """
        self._stats.events_processed_failed += 1
        self._stats.last_error_at = datetime.now(UTC)

        event_type = self._get_header_value(message.headers, "event_type")
        event_id = self._get_header_value(message.headers, "event_id")

        if retry_count >= self._config.max_retries:
            logger.error(
                "Max retries exceeded, message will be sent to DLQ",
                extra={
                    "event_type": event_type,
                    "event_id": event_id,
                    "retry_count": retry_count,
                    "max_retries": self._config.max_retries,
                    "error": str(error),
                },
            )
            retained = await self._send_to_dlq(message, error, retry_count)
            if retained and self._consumer:
                await self._consumer.commit()
            elif not retained:
                self._warn_uncommitted(message, "max_retries_exceeded")
            return

        delay = self._calculate_retry_delay(retry_count)

        logger.warning(
            "Message processing failed, republishing for retry",
            extra={
                "event_type": event_type,
                "event_id": event_id,
                "retry_count": retry_count + 1,
                "max_retries": self._config.max_retries,
                "retry_delay": delay,
                "error": str(error),
            },
        )

        retained = await self._republish_for_retry(message, retry_count + 1, delay)

        if retained and self._consumer:
            await self._consumer.commit()
        elif not retained:
            self._warn_uncommitted(message, "retry_republish")

    async def _republish_for_retry(
        self,
        message: Any,
        new_retry_count: int,
        delay: float,
    ) -> bool:
        """Republish a failed message for retry."""
        if not self._producer:
            logger.error(
                "Cannot republish for retry: producer not connected; sending to DLQ",
                extra={"retry_count": new_retry_count},
            )
            return await self._send_to_dlq(
                message,
                RuntimeError("producer not connected; cannot republish for retry"),
                new_retry_count - 1,
                reason="republish_failed",
            )

        new_headers: list[tuple[str, bytes]] = []
        if message.headers:
            for key, value in message.headers:
                if key != "retry_count" and key != "retry_after":
                    new_headers.append((key, value))

        new_headers.append(("retry_count", str(new_retry_count).encode("utf-8")))
        retry_after = datetime.now(UTC).timestamp() + delay
        new_headers.append(("retry_after", str(retry_after).encode("utf-8")))

        try:
            await self._producer.send(
                topic=self._config.topic_name,
                key=message.key,
                value=message.value,
                headers=new_headers,
            )

            logger.debug(
                "Message republished for retry",
                extra={
                    "event_type": self._get_header_value(message.headers, "event_type"),
                    "retry_count": new_retry_count,
                    "retry_after": retry_after,
                },
            )
            return True
        except Exception as e:
            logger.error(
                "Failed to republish message for retry, sending to DLQ",
                extra={"error": str(e)},
                exc_info=True,
            )
            return await self._send_to_dlq(
                message, e, new_retry_count - 1, reason="republish_failed"
            )

    def _warn_uncommitted(self, message: Any, stage: str) -> None:
        """Log that an offset was deliberately left uncommitted."""
        warn_uncommitted(
            logger,
            message,
            stage,
            self._get_header_value(message.headers, "event_id"),
        )

    def _calculate_retry_delay(self, retry_count: int) -> float:
        """Calculate delay for retry with exponential backoff and jitter."""
        return self._retry_policy.delay_for(retry_count)

    async def _send_to_dlq(
        self,
        message: Any,
        error: Exception,
        retry_count: int,
        reason: str = "max_retries_exceeded",
    ) -> bool:
        """Send a failed message to the dead letter queue."""
        if not self._config.enable_dlq:
            logger.warning(
                "DLQ disabled, dropping failed message",
                extra={
                    "event_type": self._get_header_value(message.headers, "event_type"),
                    "error": str(error),
                },
            )
            return True

        if not self._producer:
            logger.error(
                "Cannot send to DLQ: producer not connected",
                extra={
                    "event_type": self._get_header_value(message.headers, "event_type"),
                },
            )
            return False

        dlq_headers = self._create_dlq_headers(message, error, retry_count, reason)
        original_headers = list(message.headers) if message.headers else []
        all_headers = original_headers + dlq_headers

        try:
            await self._producer.send(
                topic=self._config.dlq_topic_name,
                key=message.key,
                value=message.value,
                headers=all_headers,
            )

            self._stats.messages_sent_to_dlq += 1

            if self._metrics:
                self._metrics.dlq_messages.add(
                    1,
                    attributes={
                        "dlq.reason": reason,
                        "error.type": type(error).__name__,
                    },
                )

            logger.info(
                "Message sent to DLQ",
                extra={
                    "dlq_topic": self._config.dlq_topic_name,
                    "event_type": self._get_header_value(message.headers, "event_type"),
                    "event_id": self._get_header_value(message.headers, "event_id"),
                    "reason": reason,
                    "error": str(error)[:200],
                },
            )
            return True

        except Exception as e:
            logger.error(
                "Failed to send message to DLQ",
                extra={
                    "error": str(e),
                    "original_error": str(error),
                },
                exc_info=True,
            )
            raise

    def _create_dlq_headers(
        self,
        message: Any,
        error: Exception,
        retry_count: int,
        reason: str,
    ) -> list[tuple[str, bytes]]:
        """Create DLQ-specific headers."""
        return create_dlq_headers(
            message,
            error,
            retry_count,
            reason,
            self._config.consumer_group,
        )
