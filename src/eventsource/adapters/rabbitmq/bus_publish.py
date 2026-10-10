"""
Publishing operations mixin for RabbitMQEventBus.

Provides event serialization and single/batch publishing methods.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from eventsource.adapters.rabbitmq import serialization
from eventsource.domain.event import DomainEvent

if TYPE_CHECKING:
    from aio_pika import Message
    from aio_pika.abc import AbstractExchange, AbstractIncomingMessage

    from eventsource.adapters.rabbitmq.publisher import RabbitMQPublisher


class RabbitMQEventBusPublishMixin:
    """Mixin providing publishing and serialization for RabbitMQEventBus."""

    _connected: bool
    _exchange: AbstractExchange | None
    _publisher: RabbitMQPublisher
    _logger: logging.Logger

    if TYPE_CHECKING:

        async def connect(self) -> None: ...
        def _resolve_event_class(self, event_type: str) -> type[DomainEvent] | None: ...

    @staticmethod
    def _get_event_field_default(
        event_type: type[DomainEvent], field_name: str, default: str
    ) -> str:
        """Get the default value for a field from a DomainEvent subclass.

        Thin wrapper -- see `serialization.get_event_field_default`.
        """
        return serialization.get_event_field_default(event_type, field_name, default)

    def _get_routing_key(self, event: DomainEvent) -> str:
        """Generate routing key for an event.

        Thin wrapper -- see `serialization.get_routing_key`.
        """
        return serialization.get_routing_key(event)

    def _serialize_event(self, event: DomainEvent) -> tuple[bytes, dict[str, Any]]:
        """Serialize a domain event to JSON bytes and message headers.

        Thin wrapper -- see `serialization.serialize_event`.
        """
        return serialization.serialize_event(event)

    def _create_message(self, event: DomainEvent) -> Message:
        """Create an AMQP message from a domain event.

        Thin wrapper -- see `serialization.create_message`.
        """
        return serialization.create_message(event)

    def _create_message_with_tracing(
        self,
        event: DomainEvent,
        span: Any = None,
    ) -> Message:
        """Create an AMQP message with optional trace context injection.

        Thin wrapper -- see `serialization.create_message_with_tracing`.
        """
        return serialization.create_message_with_tracing(event, span)

    def _deserialize_event(
        self,
        message: AbstractIncomingMessage,
    ) -> DomainEvent | None:
        """Deserialize an AMQP message to a domain event.

        Thin wrapper -- see `serialization.deserialize_event`.
        """
        return serialization.deserialize_event(message, self._resolve_event_class, self._logger)

    async def publish(
        self,
        events: list[DomainEvent],
        background: bool = False,
    ) -> None:
        """Publish events to RabbitMQ exchange.

        Events are serialized to JSON and published with routing keys
        based on aggregate type and event type.

        For single events, publishes directly. For multiple events, uses
        batch optimization with concurrent publishing via asyncio.gather()
        for improved performance.

        Args:
            events: List of events to publish
            background: If True, publish without waiting for confirms.
                       Default is False (wait for confirms).
                       Note: Unlike InMemoryEventBus which uses asyncio tasks
                       for background, RabbitMQ is inherently async. The background
                       parameter controls confirmation waiting rather than task spawning.

        Raises:
            RuntimeError: If not connected and connection fails, or if exchange
                         not initialized after connection
            Exception: If publishing fails (exceptions are logged and re-raised)

        Example:
            >>> await bus.publish([OrderCreated(...), OrderShipped(...)])
        """
        if not events:
            return

        # Auto-connect if needed
        if not self._connected:
            await self.connect()

        if not self._exchange:
            raise RuntimeError("Exchange not initialized")

        if len(events) == 1:
            # Single event - no batch optimization needed
            await self._publish_single(events[0], wait_for_confirm=not background)
        else:
            # Multiple events - use batch optimization
            await self._publisher.publish_many(events, wait_for_confirm=not background)

    async def publish_batch(
        self,
        events: list[DomainEvent],
        preserve_order: bool = False,
    ) -> dict[str, int]:
        """Publish multiple events with batch optimization.

        This method provides optimized batch publishing using concurrent
        asyncio.gather() to publish multiple events in parallel. Large batches
        are automatically chunked based on config.publish_chunk_size to prevent
        overwhelming the broker.

        Args:
            events: List of events to publish
            preserve_order: If True, publishes events sequentially to maintain
                          order guarantees. Default is False (concurrent publishing).
                          Use True when event ordering within the batch is critical.

        Returns:
            Dictionary with batch publishing statistics:
            - total: Total number of events in the batch
            - published: Number of events successfully published
            - failed: Number of events that failed to publish
            - chunks: Number of chunks the batch was split into

        Raises:
            RuntimeError: If not connected and connection fails, or if exchange
                         not initialized after connection
            BatchPublishError: If any events failed to publish (contains partial results)

        Example:
            >>> events = [OrderCreated(...) for _ in range(1000)]
            >>> result = await bus.publish_batch(events)
            >>> print(f"Published {result['published']}/{result['total']} events")
        """
        if not events:
            return {"total": 0, "published": 0, "failed": 0, "chunks": 0}

        # Auto-connect if needed
        if not self._connected:
            await self.connect()

        if not self._exchange:
            raise RuntimeError("Exchange not initialized")

        return await self._publisher.publish_batch(events, preserve_order=preserve_order)

    async def _publish_single(
        self,
        event: DomainEvent,
        wait_for_confirm: bool = True,
    ) -> None:
        """Publish a single event to the exchange.

        Thin wrapper -- see `RabbitMQPublisher.publish_one`. Kept as a
        facade method because tests exercise it directly.
        """
        await self._publisher.publish_one(event, wait_for_confirm=wait_for_confirm)
