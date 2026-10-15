"""RabbitMQ queue binding operations mixin for RabbitMQTopology.

Extracted from ``RabbitMQTopology`` (topology.py) to keep file sizes strictly
under the 400-line warning threshold (ADR-0002).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from eventsource.adapters.rabbitmq import serialization
from eventsource.domain.event import DomainEvent
from eventsource.ports.exceptions import EventBusConnectionError

if TYPE_CHECKING:
    from aio_pika.abc import AbstractExchange, AbstractQueue

    from eventsource.adapters.rabbitmq.config import RabbitMQEventBusConfig


class RabbitMQTopologyBindingsMixin:
    """Provides queue binding operations for RabbitMQTopology."""

    _config: RabbitMQEventBusConfig
    _exchange: AbstractExchange | None
    _consumer_queue: AbstractQueue | None
    _logger: logging.Logger

    async def _bind_queue(self) -> None:
        """Bind consumer queue to exchange based on exchange type.

        The binding behavior varies by exchange type:
        - topic: Binds with routing key pattern (default "#" for all messages).
          Supports wildcards: "*" matches one word, "#" matches zero or more.
        - direct: Binds with exact routing key (default is queue_name).
          Only messages with matching routing key are delivered.
          Multiple consumers on the same queue = competing consumers (load balanced).
        - fanout: Routing key is ignored. All bound queues receive all messages.
        - headers: Routing key is ignored. Routing is based on message headers.

        The routing key pattern can be customized via config.routing_key_pattern.
        If not set, an appropriate default is chosen based on exchange type.

        Raises:
            RuntimeError: If called before ``_declare_exchange()``/
                ``_declare_queue()`` in the same ``declare_all()`` pass --
                an internal call-order bug (see module docstring), not a
                condition reachable by a caller of the public API.
        """
        if not self._consumer_queue or not self._exchange:
            raise RuntimeError(
                "_bind_queue() called before exchange/queue were declared -- "
                "internal call-order bug, not a public-API condition"
            )

        # Get effective routing key based on exchange type and config
        routing_key = self._config.get_effective_routing_key()

        # Bind queue to exchange
        await self._consumer_queue.bind(
            exchange=self._exchange,
            routing_key=routing_key,
        )

        # Log binding with exchange-type-specific information
        exchange_type_lower = self._config.exchange_type.lower()
        if exchange_type_lower == "fanout":
            # Fanout-specific logging to clarify broadcast behavior
            self._logger.info(
                f"Bound queue {self._config.queue_name} to fanout exchange "
                f"{self._config.exchange_name} (broadcast mode - all messages will "
                f"be delivered to this queue regardless of routing key)",
                extra={
                    "queue_name": self._config.queue_name,
                    "exchange_name": self._config.exchange_name,
                    "exchange_type": self._config.exchange_type,
                    "routing_key": routing_key,
                    "broadcast_mode": True,
                    "routing_key_ignored": True,
                },
            )
        else:
            self._logger.info(
                f"Bound queue {self._config.queue_name} to exchange "
                f"{self._config.exchange_name} with routing key '{routing_key}'",
                extra={
                    "queue_name": self._config.queue_name,
                    "exchange_name": self._config.exchange_name,
                    "exchange_type": self._config.exchange_type,
                    "routing_key": routing_key,
                },
            )

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
            EventBusConnectionError: If queue/exchange not initialized.
        """
        if not self._consumer_queue or not self._exchange:
            raise EventBusConnectionError("Not connected or queue/exchange not initialized")

        # Generate routing key for this event type
        # Use the same pattern as publish: {aggregate_type}.{event_type}
        # For Pydantic models, we need to check model_fields for default values
        aggregate_type = serialization.get_event_field_default(
            event_type, "aggregate_type", "Unknown"
        )
        event_type_name = event_type.event_type_name()
        routing_key = f"{aggregate_type}.{event_type_name}"

        await self._consumer_queue.bind(
            exchange=self._exchange,
            routing_key=routing_key,
        )

        self._logger.info(
            f"Added binding for event type {event_type.__name__}",
            extra={
                "queue_name": self._config.queue_name,
                "exchange_name": self._config.exchange_name,
                "exchange_type": self._config.exchange_type,
                "event_type": event_type.__name__,
                "routing_key": routing_key,
            },
        )

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
            EventBusConnectionError: If queue/exchange not initialized.
        """
        if not self._consumer_queue or not self._exchange:
            raise EventBusConnectionError("Not connected or queue/exchange not initialized")

        await self._consumer_queue.bind(
            exchange=self._exchange,
            routing_key=routing_key,
        )

        self._logger.info(
            f"Added binding with routing key '{routing_key}'",
            extra={
                "queue_name": self._config.queue_name,
                "exchange_name": self._config.exchange_name,
                "exchange_type": self._config.exchange_type,
                "routing_key": routing_key,
            },
        )
