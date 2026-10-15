"""Publishing operations mixin for RabbitMQEventBus."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from eventsource.adapters.rabbitmq import serialization
from eventsource.domain.event import DomainEvent

if TYPE_CHECKING:
    from aio_pika import Message
    from aio_pika.abc import AbstractIncomingMessage

    from eventsource.adapters.rabbitmq.connection import RabbitMQConnectionManager
    from eventsource.adapters.rabbitmq.publisher import RabbitMQPublisher
    from eventsource.adapters.rabbitmq.topology import RabbitMQTopology


class RabbitMQEventBusPublishMixin:
    """Mixin providing publishing and serialization for RabbitMQEventBus."""

    _connection_manager: RabbitMQConnectionManager
    _topology: RabbitMQTopology
    _publisher: RabbitMQPublisher
    _logger: logging.Logger

    if TYPE_CHECKING:

        async def connect(self) -> None: ...
        def _resolve_event_class(self, event_type: str) -> type[DomainEvent] | None: ...

    @staticmethod
    def _get_event_field_default(
        event_type: type[DomainEvent], field_name: str, default: str
    ) -> str:
        """Get the default value for a field from a DomainEvent subclass."""
        return serialization.get_event_field_default(event_type, field_name, default)

    def _get_routing_key(self, event: DomainEvent) -> str:
        """Generate routing key for an event."""
        return serialization.get_routing_key(event)

    def _serialize_event(self, event: DomainEvent) -> tuple[bytes, dict[str, Any]]:
        """Serialize a domain event to JSON bytes and message headers."""
        return serialization.serialize_event(event)

    def _create_message(self, event: DomainEvent) -> Message:
        """Create an AMQP message from a domain event."""
        return serialization.create_message(event)

    def _create_message_with_tracing(self, event: DomainEvent, span: Any = None) -> Message:
        """Create an AMQP message with optional trace context injection."""
        return serialization.create_message_with_tracing(event, span)

    def _deserialize_event(self, message: AbstractIncomingMessage) -> DomainEvent | None:
        """Deserialize an AMQP message to a domain event."""
        return serialization.deserialize_event(message, self._resolve_event_class, self._logger)

    async def publish(
        self,
        events: list[DomainEvent],
        background: bool = False,
    ) -> None:
        """Publish events to RabbitMQ exchange (single or batch-optimized)."""
        if not events:
            return
        if not self._connection_manager._connected:
            await self.connect()
        if not self._topology.exchange:
            raise RuntimeError("Exchange not initialized")
        if len(events) == 1:
            await self._publish_single(events[0], wait_for_confirm=not background)
        else:
            await self._publisher.publish_many(events, wait_for_confirm=not background)

    async def publish_batch(
        self,
        events: list[DomainEvent],
        preserve_order: bool = False,
    ) -> dict[str, int]:
        """Publish multiple events with chunked batch optimization and return stats."""
        if not events:
            return {"total": 0, "published": 0, "failed": 0, "chunks": 0}
        if not self._connection_manager._connected:
            await self.connect()
        if not self._topology.exchange:
            raise RuntimeError("Exchange not initialized")
        return await self._publisher.publish_batch(events, preserve_order=preserve_order)

    async def _publish_single(
        self,
        event: DomainEvent,
        wait_for_confirm: bool = True,
    ) -> None:
        """Publish a single event to the exchange."""
        await self._publisher.publish_one(event, wait_for_confirm=wait_for_confirm)
