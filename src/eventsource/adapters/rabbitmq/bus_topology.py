"""
Topology management mixin for RabbitMQEventBus.

Provides exchange and queue declarations, and event/routing key bindings.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from eventsource.ports.exceptions import EventBusConnectionError

if TYPE_CHECKING:
    from eventsource.adapters.rabbitmq.topology import RabbitMQTopology
    from eventsource.domain.event import DomainEvent


class RabbitMQEventBusTopologyMixin:
    """Mixin providing topology declarations and queue bindings for RabbitMQEventBus."""

    _topology: RabbitMQTopology
    _connected: bool

    async def _declare_exchange(self) -> None:
        """Declare the main event exchange.

        Delegates to :class:`RabbitMQTopology`.
        """
        await self._topology._declare_exchange()

    async def _declare_queue(self) -> None:
        """Declare the consumer queue with optional DLQ configuration.

        Delegates to :class:`RabbitMQTopology`.
        """
        await self._topology._declare_queue()

    async def _bind_queue(self) -> None:
        """Bind consumer queue to exchange based on exchange type.

        Delegates to :class:`RabbitMQTopology`.
        """
        await self._topology._bind_queue()

    async def bind_event_type(self, event_type: type[DomainEvent]) -> None:
        """Bind queue to receive messages for a specific event type.

        This method creates an additional binding for the queue to receive
        messages published with a routing key matching the event type pattern.
        Useful for direct exchanges when you want to selectively receive
        specific event types rather than all messages.

        For direct exchanges, this creates an exact-match binding for the
        event type's routing key (format: "{aggregate_type}.{event_type_name}").

        For topic exchanges, this is usually not needed since the default "#"
        binding already receives all messages. However, it can be useful if
        you've configured a more restrictive routing_key_pattern.

        Args:
            event_type: The DomainEvent subclass to bind for.

        Raises:
            EventBusConnectionError: If not connected or queue/exchange not initialized.

        Example:
            >>> # For direct exchange - only receive OrderCreated events
            >>> config = RabbitMQEventBusConfig(
            ...     exchange_type="direct",
            ...     routing_key_pattern="",  # No default binding
            ... )
            >>> bus = RabbitMQEventBus(config=config)
            >>> await bus.connect()
            >>> await bus.bind_event_type(OrderCreated)
            >>> # Now queue will receive OrderCreated events
        """
        if not self._connected:
            raise EventBusConnectionError("Not connected or queue/exchange not initialized")
        await self._topology.bind_event_type(event_type)

    async def bind_routing_key(self, routing_key: str) -> None:
        """Bind queue to receive messages with a specific routing key.

        Creates an additional binding for the queue to receive messages
        matching the specified routing key. This is a lower-level method
        than bind_event_type, useful when you need precise control over
        routing key patterns.

        Args:
            routing_key: The routing key pattern to bind. For topic exchanges,
                this can include wildcards (* for one word, # for zero or more).
                For direct exchanges, this must be an exact match.

        Raises:
            EventBusConnectionError: If not connected or queue/exchange not initialized.

        Example:
            >>> # Bind to all Order events on topic exchange
            >>> await bus.bind_routing_key("Order.*")
            >>> # Bind to specific routing key on direct exchange
            >>> await bus.bind_routing_key("Order.OrderCreated")
        """
        if not self._connected:
            raise EventBusConnectionError("Not connected or queue/exchange not initialized")
        await self._topology.bind_routing_key(routing_key)
