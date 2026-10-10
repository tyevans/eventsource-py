"""Consumption mixin and delegating shims for KafkaEventBus.

Governed by ADR-0002 (<500 lines per module).
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from eventsource.adapters._bus.handler_adapter import HandlerAdapter
from eventsource.domain.event import DomainEvent

if TYPE_CHECKING:
    from eventsource.adapters.kafka.connection import KafkaConnectionManager
    from eventsource.adapters.kafka.consumer import KafkaConsumerLoop


class KafkaBusConsumeMixin:
    """Mixin providing consumer loop lifecycle and delegating helpers."""

    _consumer_loop: KafkaConsumerLoop
    _connection_manager: KafkaConnectionManager

    async def start_consuming(self, auto_reconnect: bool = True) -> None:
        """Start consuming events from Kafka.

        Blocks and continuously polls for messages, dispatching them to
        registered handlers. Use stop_consuming() from another coroutine to stop.

        Args:
            auto_reconnect: If True, automatically reconnect on errors.

        Raises:
            RuntimeError: If not connected or already consuming.
        """
        await self._consumer_loop.start(auto_reconnect=auto_reconnect)

    async def _reconnect_consumer(self) -> None:
        """Attempt to reconnect the consumer after an error."""
        await self._connection_manager.reconnect_consumer()

    def start_consuming_in_background(self) -> asyncio.Task[None]:
        """Start consuming in a background task.

        Returns:
            The background task running the consumer.

        Raises:
            RuntimeError: If consumer is already running in background.
        """
        return self._consumer_loop.start_in_background()

    async def stop_consuming(self) -> None:
        """Stop the consumer loop gracefully."""
        await self._consumer_loop.stop()

    async def _process_message(self, message: Any) -> None:
        """Delegate a single message to the consume loop."""
        await self._consumer_loop._process_message(message)

    def _deserialize_message(self, message: Any) -> DomainEvent:
        """Delegate message deserialization to the consume loop."""
        return self._consumer_loop._deserialize_message(message)

    def _get_header_value(
        self,
        headers: list[tuple[str, bytes]] | None,
        key: str,
    ) -> str | None:
        """Delegate header lookup to the consume loop."""
        return self._consumer_loop._get_header_value(headers, key)

    def _get_retry_count(self, headers: list[tuple[str, bytes]] | None) -> int:
        """Delegate retry-count extraction to the consume loop."""
        return self._consumer_loop._get_retry_count(headers)

    async def _dispatch_to_handlers(
        self,
        event: DomainEvent,
        handlers: tuple[HandlerAdapter, ...],
    ) -> None:
        """Delegate handler dispatch to the consume loop."""
        await self._consumer_loop._dispatch_to_handlers(event, handlers)

    def _calculate_retry_delay(self, retry_count: int) -> float:
        """Delegate retry-delay calculation to the consume loop."""
        return self._consumer_loop._calculate_retry_delay(retry_count)

    async def _send_to_dlq(
        self,
        message: Any,
        error: Exception,
        retry_count: int,
        reason: str = "max_retries_exceeded",
    ) -> None:
        """Delegate DLQ routing to the consume loop."""
        await self._consumer_loop._send_to_dlq(message, error, retry_count, reason)
