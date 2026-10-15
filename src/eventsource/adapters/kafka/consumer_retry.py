"""Retry and DLQ routing mixin for KafkaConsumerLoop."""

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
        self, headers: list[tuple[str, bytes]] | tuple[tuple[str, bytes], ...] | None, key: str
    ) -> str | None:
        return get_header_value(headers, key)

    async def _handle_processing_error(
        self, message: Any, error: Exception, retry_count: int
    ) -> None:
        """Handle a message processing error by republishing for retry or sending to DLQ."""
        self._stats.events_processed_failed += 1
        self._stats.last_error_at = datetime.now(UTC)
        event_type = self._get_header_value(message.headers, "event_type")
        event_id = self._get_header_value(message.headers, "event_id")
        base_extra = {
            "event_type": event_type,
            "event_id": event_id,
            "max_retries": self._config.max_retries,
            "error": str(error),
        }

        if retry_count >= self._config.max_retries:
            logger.error(
                "Max retries exceeded, message will be sent to DLQ",
                extra={**base_extra, "retry_count": retry_count},
            )
            retained = await self._send_to_dlq(message, error, retry_count)
            stage = "max_retries_exceeded"
        else:
            delay = self._calculate_retry_delay(retry_count)
            logger.warning(
                "Message processing failed, republishing for retry",
                extra={**base_extra, "retry_count": retry_count + 1, "retry_delay": delay},
            )
            retained = await self._republish_for_retry(message, retry_count + 1, delay)
            stage = "retry_republish"

        if retained and self._consumer:
            await self._consumer.commit()
        elif not retained:
            self._warn_uncommitted(message, stage)

    async def _republish_for_retry(self, message: Any, new_retry_count: int, delay: float) -> bool:
        """Republish a failed message for retry."""
        if not self._producer:
            logger.error(
                "Cannot republish for retry: producer not connected; sending to DLQ",
                extra={"retry_count": new_retry_count},
            )
            err = RuntimeError("producer not connected; cannot republish for retry")
            return await self._send_to_dlq(
                message, err, new_retry_count - 1, reason="republish_failed"
            )

        new_headers = [
            (k, v) for k, v in (message.headers or ()) if k not in ("retry_count", "retry_after")
        ]
        retry_after = datetime.now(UTC).timestamp() + delay
        new_headers.extend(
            [
                ("retry_count", str(new_retry_count).encode("utf-8")),
                ("retry_after", str(retry_after).encode("utf-8")),
            ]
        )

        try:
            await self._producer.send(
                topic=self._config.topic_name,
                key=message.key,
                value=message.value,
                headers=new_headers,
            )
            extra = {
                "event_type": self._get_header_value(message.headers, "event_type"),
                "retry_count": new_retry_count,
                "retry_after": retry_after,
            }
            logger.debug("Message republished for retry", extra=extra)
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
        warn_uncommitted(
            logger, message, stage, self._get_header_value(message.headers, "event_id")
        )

    def _calculate_retry_delay(self, retry_count: int) -> float:
        return self._retry_policy.delay_for(retry_count)

    async def _send_to_dlq(
        self,
        message: Any,
        error: Exception,
        retry_count: int,
        reason: str = "max_retries_exceeded",
    ) -> bool:
        """Send a failed message to the dead letter queue."""
        event_type = self._get_header_value(message.headers, "event_type")
        if not self._config.enable_dlq:
            logger.warning(
                "DLQ disabled, dropping failed message",
                extra={"event_type": event_type, "error": str(error)},
            )
            return True

        if not self._producer:
            logger.error(
                "Cannot send to DLQ: producer not connected", extra={"event_type": event_type}
            )
            return False

        dlq_headers = self._create_dlq_headers(message, error, retry_count, reason)
        all_headers = list(message.headers or ()) + dlq_headers

        try:
            await self._producer.send(
                topic=self._config.dlq_topic_name,
                key=message.key,
                value=message.value,
                headers=all_headers,
            )
            self._stats.messages_sent_to_dlq += 1
            if self._metrics:
                attrs = {"dlq.reason": reason, "error.type": type(error).__name__}
                self._metrics.dlq_messages.add(1, attributes=attrs)
            logger.info(
                "Message sent to DLQ",
                extra={
                    "dlq_topic": self._config.dlq_topic_name,
                    "event_type": event_type,
                    "event_id": self._get_header_value(message.headers, "event_id"),
                    "reason": reason,
                    "error": str(error)[:200],
                },
            )
            return True
        except Exception as e:
            logger.error(
                "Failed to send message to DLQ",
                extra={"error": str(e), "original_error": str(error)},
                exc_info=True,
            )
            raise

    def _create_dlq_headers(
        self, message: Any, error: Exception, retry_count: int, reason: str
    ) -> list[tuple[str, bytes]]:
        return create_dlq_headers(message, error, retry_count, reason, self._config.consumer_group)
