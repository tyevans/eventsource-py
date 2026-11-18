"""Retry and DLQ routing mixin for KafkaConsumerLoop."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from eventsource.adapters._bus.headers import read_header_int
from eventsource.adapters.kafka.consumer_helpers import (
    create_dlq_headers,
    get_header_value,
    warn_uncommitted,
)
from eventsource.observability.error_summary import error_summary

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
            "error": error_summary(error),
        }
        if retry_count >= self._config.max_retries:
            logger.error("Max retries exceeded, message will be sent to DLQ", extra={**base_extra, "retry_count": retry_count})  # fmt: skip
            retained = await self._send_to_dlq(message, error, retry_count)
            stage = "max_retries_exceeded"
        else:
            delay = self._calculate_retry_delay(retry_count)
            logger.warning("Message processing failed, republishing for retry", extra={**base_extra, "retry_count": retry_count + 1, "retry_delay": delay})  # fmt: skip
            retained = await self._republish_for_retry(message, retry_count + 1, delay)
            stage = "retry_republish"

        if retained and self._consumer:
            await self._consumer.commit()
        elif not retained:
            self._warn_uncommitted(message, stage)

    async def _republish_for_retry(self, message: Any, new_retry_count: int, delay: float) -> bool:
        """Republish a failed message for retry."""
        if not self._producer:
            logger.error("Cannot republish for retry: producer not connected; sending to DLQ", extra={"retry_count": new_retry_count})  # fmt: skip
            err = RuntimeError("producer not connected; cannot republish for retry")
            return await self._send_to_dlq(message, err, new_retry_count - 1, reason="republish_failed")  # fmt: skip

        retry_after = datetime.now(UTC).timestamp() + delay
        new_headers = [
            (k, v) for k, v in (message.headers or ()) if k not in ("retry_count", "retry_after")
        ]
        new_headers.append(("retry_count", str(new_retry_count).encode("utf-8")))
        new_headers.append(("retry_after", str(retry_after).encode("utf-8")))
        try:
            await self._producer.send(topic=self._config.topic_name, key=message.key, value=message.value, headers=new_headers)  # fmt: skip
            extra = {"event_type": self._get_header_value(message.headers, "event_type"), "retry_count": new_retry_count, "retry_after": retry_after}  # fmt: skip
            logger.debug("Message republished for retry", extra=extra)
            return True
        except Exception as e:
            # No exc_info: the message value is re-sent here, so the traceback can echo payload.
            logger.error("Failed to republish message for retry, sending to DLQ", extra={"error": error_summary(e)})  # fmt: skip
            return await self._send_to_dlq(message, e, new_retry_count - 1, reason="republish_failed")  # fmt: skip

    async def _dead_letter_at_capacity(self, message: Any) -> None:
        """Route a deferred retry to the DLQ when the retry scheduler is full, then commit."""
        stage = "retry_capacity_exceeded"
        error = RuntimeError("retry scheduler at capacity; deferred retry sent to DLQ")
        logger.error("Retry scheduler full, sending message to DLQ", extra={"event_id": self._get_header_value(message.headers, "event_id")})  # fmt: skip
        retry_count = read_header_int(self._get_header_value(message.headers, "retry_count"), 0)
        retained = await self._send_to_dlq(message, error, retry_count, reason=stage)
        if retained and self._consumer:
            await self._consumer.commit()
        elif not retained:
            self._warn_uncommitted(message, stage)

    def _warn_uncommitted(self, message: Any, stage: str) -> None:
        warn_uncommitted(logger, message, stage, self._get_header_value(message.headers, "event_id"))  # fmt: skip

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
            logger.warning("DLQ disabled, dropping failed message", extra={"event_type": event_type, "error": error_summary(error)})  # fmt: skip
            return True
        if not self._producer:
            logger.error("Cannot send to DLQ: producer not connected", extra={"event_type": event_type})  # fmt: skip
            return False

        dlq_headers = self._create_dlq_headers(message, error, retry_count, reason)
        # Inbound dlq_* headers are publisher-controlled. Drop them so only this writer's
        # dlq_* metadata reaches the DLQ (a forged dlq_replay_count would bypass the cap).
        inbound = [(k, v) for k, v in (message.headers or ()) if not k.startswith("dlq_")]
        all_headers = inbound + dlq_headers
        try:
            await self._producer.send(topic=self._config.dlq_topic_name, key=message.key, value=message.value, headers=all_headers)  # fmt: skip
            self._stats.messages_sent_to_dlq += 1
            if self._metrics:
                attrs = {"dlq.reason": reason, "error.type": type(error).__name__}
                self._metrics.dlq_messages.add(1, attributes=attrs)
            dlq_extra = {"dlq_topic": self._config.dlq_topic_name, "event_type": event_type, "event_id": self._get_header_value(message.headers, "event_id"), "reason": reason, "error": error_summary(error)[:200]}  # fmt: skip
            logger.info("Message sent to DLQ", extra=dlq_extra)
            return True
        except Exception as e:
            # No exc_info: the traceback repeats the messages, which can echo payload.
            logger.error("Failed to send message to DLQ", extra={"error": error_summary(e), "original_error": error_summary(error)})  # fmt: skip
            raise

    def _create_dlq_headers(
        self, message: Any, error: Exception, retry_count: int, reason: str
    ) -> list[tuple[str, bytes]]:
        return create_dlq_headers(message, error, retry_count, reason, self._config.consumer_group)
