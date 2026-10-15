"""Consumption mixin for KafkaEventBus.

Governed by ADR-0002 (<500 lines per module).
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from eventsource.adapters.kafka.connection import KafkaConnectionManager
    from eventsource.adapters.kafka.consumer import KafkaConsumerLoop


class KafkaBusConsumeMixin:
    """Mixin providing consumer loop lifecycle methods."""

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
