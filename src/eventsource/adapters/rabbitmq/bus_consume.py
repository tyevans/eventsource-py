"""
Consumer operations mixin for RabbitMQEventBus.

Provides consuming loop, message processing, event dispatch, and retry operations.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from eventsource.domain.event import DomainEvent

if TYPE_CHECKING:
    from aio_pika.abc import AbstractIncomingMessage

    from eventsource.adapters.rabbitmq.consumer import RabbitMQConsumer


class RabbitMQEventBusConsumeMixin:
    """Mixin providing consumer loop and retry handling for RabbitMQEventBus."""

    _connected: bool
    _consumer: RabbitMQConsumer

    if TYPE_CHECKING:

        async def connect(self) -> None: ...

    def _calculate_retry_delay(self, retry_count: int) -> float:
        """Calculate the delay before the next retry.

        Delegates to :class:`RabbitMQConsumer`.

        Args:
            retry_count: Zero-based retry attempt number.

        Returns:
            Delay in seconds, with symmetric jitter applied.
        """
        return self._consumer._calculate_retry_delay(retry_count)

    async def _handle_failed_message(
        self,
        message: AbstractIncomingMessage,
        error: Exception,
        retry_count: int,
    ) -> None:
        """Handle a failed message - retry with backoff or route to DLQ.

        Delegates to :class:`RabbitMQConsumer`.

        Args:
            message: The failed message
            error: The exception that caused the failure
            retry_count: Current retry count (from x-retry-count header)
        """
        await self._consumer._handle_failed_message(message, error, retry_count)

    async def _republish_for_retry(
        self,
        original_message: AbstractIncomingMessage,
        new_retry_count: int,
    ) -> None:
        """Republish a message with incremented retry count.

        Delegates to :class:`RabbitMQConsumer`.

        Args:
            original_message: The original message to retry
            new_retry_count: The new retry count value
        """
        await self._consumer._republish_for_retry(original_message, new_retry_count)

    async def start_consuming(self) -> None:
        """Start consuming events from the RabbitMQ queue.

        Connects if necessary, then delegates the consume loop to
        :class:`RabbitMQConsumer`.

        Raises:
            RuntimeError: If not connected and connection fails, or if
                         consumer queue not initialized
        """
        if not self._connected:
            await self.connect()

        await self._consumer.start()

    async def stop_consuming(self) -> None:
        """Stop the consumer loop gracefully.

        Delegates to :class:`RabbitMQConsumer`.
        """
        await self._consumer.stop()

    def start_consuming_in_background(self) -> asyncio.Task[None]:
        """Start consuming in a background task.

        The background task runs :meth:`start_consuming` (so it auto-connects
        exactly as a direct call would); the task handle itself is owned by
        :class:`RabbitMQConsumer`.

        Returns:
            The background task running the consumer

        Raises:
            RuntimeError: If consumer is already running in background
        """
        return self._consumer.start_in_background(self.start_consuming)

    async def _process_message(
        self,
        message: AbstractIncomingMessage,
    ) -> None:
        """Process a single message from the queue.

        Delegates to :class:`RabbitMQConsumer`.

        Args:
            message: The incoming AMQP message
        """
        await self._consumer._process_message(message)

    async def _dispatch_event(
        self,
        event: DomainEvent,
        message: AbstractIncomingMessage,
        parent_span: Any = None,
    ) -> None:
        """Dispatch an event to all matching handlers.

        Delegates to :class:`RabbitMQConsumer`.

        Args:
            event: The deserialized domain event
            message: Original AMQP message for context
            parent_span: Optional parent span for tracing

        Raises:
            HandlerDispatchError: If one or more handlers raise.
        """
        await self._consumer._dispatch_event(event, message, parent_span)
