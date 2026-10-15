"""RabbitMQ exchange/queue topology declaration."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from eventsource.adapters.rabbitmq.config import RabbitMQEventBusConfig
from eventsource.adapters.rabbitmq.models import QueueInfo
from eventsource.adapters.rabbitmq.topology_bindings import RabbitMQTopologyBindingsMixin

if TYPE_CHECKING:
    from aio_pika.abc import AbstractExchange, AbstractQueue

    from eventsource.adapters.rabbitmq.connection import RabbitMQConnectionManager

try:
    from aio_pika import ExchangeType
except ImportError:  # pragma: no cover - guarded by RabbitMQEventBus construction
    ExchangeType = None  # type: ignore[assignment, misc]

logger = logging.getLogger("eventsource.adapters.rabbitmq")


class RabbitMQTopology(RabbitMQTopologyBindingsMixin):
    """Declares and owns the RabbitMQ exchange/queue topology."""

    def __init__(
        self,
        config: RabbitMQEventBusConfig,
        connection: RabbitMQConnectionManager,
    ) -> None:
        self._config = config
        self._connection = connection
        self._exchange: AbstractExchange | None = None
        self._dlq_exchange: AbstractExchange | None = None
        self._consumer_queue: AbstractQueue | None = None
        self._dlq_queue: AbstractQueue | None = None
        self._logger = logging.getLogger("eventsource.adapters.rabbitmq")

    @property
    def exchange(self) -> AbstractExchange | None:
        return self._exchange

    @property
    def dlq_exchange(self) -> AbstractExchange | None:
        return self._dlq_exchange

    @property
    def consumer_queue(self) -> AbstractQueue | None:
        return self._consumer_queue

    @property
    def dlq_queue(self) -> AbstractQueue | None:
        return self._dlq_queue

    async def declare_all(self) -> None:
        """Declare exchange, consumer queue, bindings, and DLQ per config flags."""
        if self._config.enable_dlq:
            await self._declare_dlq()
        await self._declare_exchange()
        await self._declare_queue()
        await self._bind_queue()

    async def redeclare(self) -> None:
        """Re-declare topology after reconnection."""
        await self.declare_all()

    async def queue_health(self, queue_name: str) -> QueueInfo | None:
        """Passively query a queue's message and consumer counts."""
        channel = self._connection.channel
        if channel is None or channel.is_closed:
            return None
        try:
            declared = await channel.declare_queue(name=queue_name, passive=True)
            message_count = declared.declaration_result.message_count or 0
            consumer_count = declared.declaration_result.consumer_count or 0
            state = "running" if consumer_count > 0 else "idle"
            self._logger.debug(
                f"Queue info retrieved: {queue_name}",
                extra={
                    "queue_name": queue_name,
                    "message_count": message_count,
                    "consumer_count": consumer_count,
                    "state": state,
                },
            )
            return QueueInfo(
                name=queue_name,
                message_count=message_count,
                consumer_count=consumer_count,
                state=state,
            )
        except Exception as e:
            self._logger.error(
                f"Failed to get queue info: {e}",
                exc_info=True,
                extra={
                    "queue_name": queue_name,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
            )
            return QueueInfo(
                name=queue_name,
                message_count=0,
                consumer_count=0,
                state="error",
                error=str(e),
            )

    async def _declare_exchange(self) -> None:
        """Declare the main event exchange with the configured exchange type."""
        channel = self._connection.require_channel()
        exchange_type_map = {
            "topic": ExchangeType.TOPIC,
            "direct": ExchangeType.DIRECT,
            "fanout": ExchangeType.FANOUT,
            "headers": ExchangeType.HEADERS,
        }
        exchange_type = exchange_type_map.get(
            self._config.exchange_type.lower(), ExchangeType.TOPIC
        )
        self._exchange = await channel.declare_exchange(
            name=self._config.exchange_name,
            type=exchange_type,
            durable=self._config.durable,
            auto_delete=self._config.auto_delete,
        )
        self._logger.info(
            f"Declared exchange: {self._config.exchange_name}",
            extra={
                "exchange_name": self._config.exchange_name,
                "exchange_type": self._config.exchange_type,
                "durable": self._config.durable,
                "auto_delete": self._config.auto_delete,
            },
        )

    async def _declare_queue(self) -> None:
        """Declare the consumer queue with optional DLQ arguments."""
        channel = self._connection.require_channel()
        arguments: dict[str, Any] = {}
        if self._config.enable_dlq:
            arguments["x-dead-letter-exchange"] = self._config.dlq_exchange_name
            arguments["x-dead-letter-routing-key"] = self._config.queue_name
        self._consumer_queue = await channel.declare_queue(
            name=self._config.queue_name,
            durable=self._config.durable,
            auto_delete=self._config.auto_delete,
            arguments=arguments if arguments else None,
        )
        self._logger.info(
            f"Declared queue: {self._config.queue_name}",
            extra={
                "queue_name": self._config.queue_name,
                "durable": self._config.durable,
                "auto_delete": self._config.auto_delete,
                "dlq_enabled": self._config.enable_dlq,
            },
        )

    async def _declare_dlq(self) -> None:
        """Declare the dead letter direct exchange, queue, and binding."""
        channel = self._connection.require_channel()
        self._dlq_exchange = await channel.declare_exchange(
            name=self._config.dlq_exchange_name,
            type=ExchangeType.DIRECT,
            durable=self._config.durable,
            auto_delete=self._config.auto_delete,
        )
        self._logger.info(
            f"Declared DLQ exchange: {self._config.dlq_exchange_name}",
            extra={
                "dlq_exchange_name": self._config.dlq_exchange_name,
                "durable": self._config.durable,
            },
        )
        dlq_arguments: dict[str, Any] = {}
        if self._config.dlq_message_ttl is not None:
            dlq_arguments["x-message-ttl"] = self._config.dlq_message_ttl
        if self._config.dlq_max_length is not None:
            dlq_arguments["x-max-length"] = self._config.dlq_max_length
        self._dlq_queue = await channel.declare_queue(
            name=self._config.dlq_queue_name,
            durable=self._config.durable,
            auto_delete=self._config.auto_delete,
            arguments=dlq_arguments if dlq_arguments else None,
        )
        self._logger.info(
            f"Declared DLQ queue: {self._config.dlq_queue_name}",
            extra={
                "dlq_queue_name": self._config.dlq_queue_name,
                "durable": self._config.durable,
                "dlq_message_ttl": self._config.dlq_message_ttl,
                "dlq_max_length": self._config.dlq_max_length,
            },
        )
        await self._dlq_queue.bind(
            exchange=self._dlq_exchange,
            routing_key=self._config.queue_name,
        )
        self._logger.info(
            f"Bound DLQ queue {self._config.dlq_queue_name} to DLQ exchange "
            f"{self._config.dlq_exchange_name} with routing key '{self._config.queue_name}'",
            extra={
                "dlq_queue_name": self._config.dlq_queue_name,
                "dlq_exchange_name": self._config.dlq_exchange_name,
                "routing_key": self._config.queue_name,
            },
        )
