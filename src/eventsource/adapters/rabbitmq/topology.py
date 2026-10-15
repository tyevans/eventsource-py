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
            msg_cnt = declared.declaration_result.message_count or 0
            con_cnt = declared.declaration_result.consumer_count or 0
            state = "running" if con_cnt > 0 else "idle"
            self._logger.debug(
                f"Queue info retrieved: {queue_name}",
                extra={
                    "queue_name": queue_name,
                    "message_count": msg_cnt,
                    "consumer_count": con_cnt,
                    "state": state,
                },
            )
            return QueueInfo(
                name=queue_name, message_count=msg_cnt, consumer_count=con_cnt, state=state
            )
        except Exception as e:
            err_extra = {"queue_name": queue_name, "error": str(e), "error_type": type(e).__name__}
            self._logger.error(f"Failed to get queue info: {e}", exc_info=True, extra=err_extra)
            return QueueInfo(
                name=queue_name, message_count=0, consumer_count=0, state="error", error=str(e)
            )

    async def _declare_exchange(self) -> None:
        """Declare the main event exchange with the configured exchange type."""
        channel, cfg = self._connection.require_channel(), self._config
        exchange_type_map = {
            "topic": ExchangeType.TOPIC,
            "direct": ExchangeType.DIRECT,
            "fanout": ExchangeType.FANOUT,
            "headers": ExchangeType.HEADERS,
        }
        ex_type = exchange_type_map.get(cfg.exchange_type.lower(), ExchangeType.TOPIC)
        self._exchange = await channel.declare_exchange(
            name=cfg.exchange_name, type=ex_type, durable=cfg.durable, auto_delete=cfg.auto_delete
        )
        self._logger.info(
            f"Declared exchange: {cfg.exchange_name}",
            extra={
                "exchange_name": cfg.exchange_name,
                "exchange_type": cfg.exchange_type,
                "durable": cfg.durable,
                "auto_delete": cfg.auto_delete,
            },
        )

    async def _declare_queue(self) -> None:
        """Declare the consumer queue with optional DLQ arguments."""
        channel, cfg = self._connection.require_channel(), self._config
        arguments: dict[str, Any] = {}
        if cfg.enable_dlq:
            arguments["x-dead-letter-exchange"] = cfg.dlq_exchange_name
            arguments["x-dead-letter-routing-key"] = cfg.queue_name
        self._consumer_queue = await channel.declare_queue(
            name=cfg.queue_name,
            durable=cfg.durable,
            auto_delete=cfg.auto_delete,
            arguments=arguments or None,
        )
        self._logger.info(
            f"Declared queue: {cfg.queue_name}",
            extra={
                "queue_name": cfg.queue_name,
                "durable": cfg.durable,
                "auto_delete": cfg.auto_delete,
                "dlq_enabled": cfg.enable_dlq,
            },
        )

    async def _declare_dlq(self) -> None:
        """Declare the dead letter direct exchange, queue, and binding."""
        channel, cfg = self._connection.require_channel(), self._config
        self._dlq_exchange = await channel.declare_exchange(
            name=cfg.dlq_exchange_name,
            type=ExchangeType.DIRECT,
            durable=cfg.durable,
            auto_delete=cfg.auto_delete,
        )
        self._logger.info(
            f"Declared DLQ exchange: {cfg.dlq_exchange_name}",
            extra={"dlq_exchange_name": cfg.dlq_exchange_name, "durable": cfg.durable},
        )
        dlq_args: dict[str, Any] = {}
        if cfg.dlq_message_ttl is not None:
            dlq_args["x-message-ttl"] = cfg.dlq_message_ttl
        if cfg.dlq_max_length is not None:
            dlq_args["x-max-length"] = cfg.dlq_max_length
        self._dlq_queue = await channel.declare_queue(
            name=cfg.dlq_queue_name,
            durable=cfg.durable,
            auto_delete=cfg.auto_delete,
            arguments=dlq_args or None,
        )
        self._logger.info(
            f"Declared DLQ queue: {cfg.dlq_queue_name}",
            extra={
                "dlq_queue_name": cfg.dlq_queue_name,
                "durable": cfg.durable,
                "dlq_message_ttl": cfg.dlq_message_ttl,
                "dlq_max_length": cfg.dlq_max_length,
            },
        )
        await self._dlq_queue.bind(exchange=self._dlq_exchange, routing_key=cfg.queue_name)
        self._logger.info(
            f"Bound DLQ queue {cfg.dlq_queue_name} to DLQ exchange "
            f"{cfg.dlq_exchange_name} with routing key '{cfg.queue_name}'",
            extra={
                "dlq_queue_name": cfg.dlq_queue_name,
                "dlq_exchange_name": cfg.dlq_exchange_name,
                "routing_key": cfg.queue_name,
            },
        )
