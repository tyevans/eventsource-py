"""RabbitMQ queue binding operations mixin for RabbitMQTopology."""

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
        """Bind consumer queue to exchange based on exchange type."""
        if not self._consumer_queue or not self._exchange:
            raise RuntimeError(
                "_bind_queue() called before exchange/queue were declared -- "
                "internal call-order bug, not a public-API condition"
            )

        routing_key = self._config.get_effective_routing_key()
        await self._consumer_queue.bind(exchange=self._exchange, routing_key=routing_key)

        if self._config.exchange_type.lower() == "fanout":
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
        """Bind queue to receive messages for a specific event type."""
        if not self._consumer_queue or not self._exchange:
            raise EventBusConnectionError("Not connected or queue/exchange not initialized")

        aggregate_type = serialization.get_event_field_default(
            event_type, "aggregate_type", "Unknown"
        )
        routing_key = f"{aggregate_type}.{event_type.event_type_name()}"
        await self._consumer_queue.bind(exchange=self._exchange, routing_key=routing_key)

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
        """Bind queue to receive messages with a specific routing key."""
        if not self._consumer_queue or not self._exchange:
            raise EventBusConnectionError("Not connected or queue/exchange not initialized")

        await self._consumer_queue.bind(exchange=self._exchange, routing_key=routing_key)

        self._logger.info(
            f"Added binding with routing key '{routing_key}'",
            extra={
                "queue_name": self._config.queue_name,
                "exchange_name": self._config.exchange_name,
                "exchange_type": self._config.exchange_type,
                "routing_key": routing_key,
            },
        )
