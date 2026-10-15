"""Dead letter queue (DLQ) operations mixin for RabbitMQEventBus."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from eventsource.adapters.rabbitmq import death_headers
from eventsource.adapters.rabbitmq.dlq import RabbitMQDLQAdmin
from eventsource.adapters.rabbitmq.models import DLQMessage
from eventsource.adapters.rabbitmq.topology import RabbitMQTopology

if TYPE_CHECKING:
    from aio_pika.abc import AbstractIncomingMessage

    from eventsource.adapters.rabbitmq.consumer import RabbitMQConsumer


class RabbitMQEventBusDLQMixin:
    """Mixin providing DLQ operations for RabbitMQEventBus."""

    _topology: RabbitMQTopology
    _consumer: RabbitMQConsumer
    _dlq_admin: RabbitMQDLQAdmin

    async def _declare_dlq(self) -> None:
        """Declare dead letter exchange and queue via RabbitMQTopology."""
        await self._topology._declare_dlq()

    # Permanent public aliases: the pure implementations live in death_headers.py.
    get_death_count = staticmethod(death_headers.get_death_count)
    get_first_death_queue = staticmethod(death_headers.get_first_death_queue)
    get_first_death_reason = staticmethod(death_headers.get_first_death_reason)
    get_first_death_exchange = staticmethod(death_headers.get_first_death_exchange)
    get_original_routing_key = staticmethod(death_headers.get_original_routing_key)
    is_from_dlq = staticmethod(death_headers.is_from_dlq)
    get_death_info = staticmethod(death_headers.get_death_info)

    async def _send_to_dlq(
        self,
        message: AbstractIncomingMessage,
        error: Exception,
        retry_count: int,
    ) -> None:
        """Send a failed message to the dead letter queue via RabbitMQConsumer."""
        await self._consumer._send_to_dlq(message, error, retry_count)

    async def get_dlq_messages(self, limit: int = 100) -> list[DLQMessage]:
        """Get messages from the dead letter queue for inspection without removing them."""
        return await self._dlq_admin.get_messages(limit=limit)

    async def get_dlq_message_count(self) -> int:
        """Get the number of messages currently waiting in the dead letter queue."""
        return await self._dlq_admin.get_message_count()

    async def replay_dlq_message(self, message_id: str) -> bool:
        """Replay a specific message from the DLQ back to the main exchange."""
        return await self._dlq_admin.replay_message(message_id)

    async def _replay_message(self, message: Any) -> None:
        """Republish a DLQ message to the main exchange with DLQ headers stripped."""
        await self._dlq_admin._replay_message(message)

    async def purge_dlq(self) -> int:
        """Remove all messages from the dead letter queue and return purged count."""
        return await self._dlq_admin.purge()
