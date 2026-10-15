"""RabbitMQ exchange/queue topology declaration.

Extracted from ``RabbitMQEventBus`` (bus.py) as part of the bus god-class
decomposition (Task 5). Owns declaring the main exchange, the consumer
queue, DLQ exchange/queue, and the bindings between them -- plus the
additional event-type/routing-key bindings exposed on the facade.

The facade still owns the "is connected" concept; this collaborator only
needs a live channel, obtained via
:meth:`RabbitMQConnectionManager.require_channel`.

Declaration-ordering invariant: ``declare_all()``/``redeclare()`` are the
only callers reachable from the public API (via
``RabbitMQConnectionManager.connect()``'s reconnect-callback run, before
``_connected`` is set), and they always run after the channel exists and in
dependency order (DLQ, then exchange, then queue, then binding). The
``RuntimeError``s raised by ``_declare_exchange``/``_declare_queue``/
``_declare_dlq`` (no channel) and ``_bind_queue`` (no queue/exchange yet)
are therefore unreachable through that path -- they exist only because
these are also invoked directly by tests/monkeypatched callers (see the
``_declare_*`` delegation methods on ``RabbitMQEventBus``, kept for
backward compatibility). They are deliberately plain ``RuntimeError``, not
``EventBusConnectionError``: nothing a well-behaved caller of the *public*
API can trigger raises them, so there is no user-facing contract to type --
tripping one means this module's own call-order invariant was violated,
which is a bug in the caller (test, monkeypatch, or a future internal
change), not a "you forgot to call connect()" situation.
"""

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

# Named explicitly (not via __name__) so the logger name is stable and
# matches the facade's pre-extraction "eventsource.adapters.rabbitmq" logger --
# callers that configure logging by name keep working unchanged.
logger = logging.getLogger("eventsource.adapters.rabbitmq")


class RabbitMQTopology(RabbitMQTopologyBindingsMixin):
    """Declares and owns the RabbitMQ exchange/queue topology.

    Declaration order (matching what ``connect()`` did before extraction):
    DLQ exchange+queue (if enabled), then main exchange, then consumer
    queue, then the binding between queue and exchange.
    """

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
        """Get the declared main exchange, if any."""
        return self._exchange

    @property
    def dlq_exchange(self) -> AbstractExchange | None:
        """Get the declared DLQ exchange, if any."""
        return self._dlq_exchange

    @property
    def consumer_queue(self) -> AbstractQueue | None:
        """Get the declared consumer queue, if any."""
        return self._consumer_queue

    @property
    def dlq_queue(self) -> AbstractQueue | None:
        """Get the declared DLQ queue, if any."""
        return self._dlq_queue

    async def declare_all(self) -> None:
        """Declare exchange, consumer queue, bindings, and DLQ per config flags.

        Runs in the same order ``connect()`` declared topology before
        extraction: DLQ first (if enabled), then main exchange, then
        consumer queue, then binding.
        """
        if self._config.enable_dlq:
            await self._declare_dlq()
        await self._declare_exchange()
        await self._declare_queue()
        await self._bind_queue()

    async def redeclare(self) -> None:
        """Reconnect path: re-declare everything (current ``_on_reconnect`` behavior)."""
        await self.declare_all()

    async def queue_health(self, queue_name: str) -> QueueInfo | None:
        """Passively query a queue's message/consumer counts.

        Used by both the facade's public ``get_queue_info()`` (for the
        consumer queue) and its ``health_check()`` (for the consumer queue
        and, separately, the DLQ queue). Uses passive declaration, so it
        never creates or modifies the queue.

        Args:
            queue_name: Name of the queue to query.

        Returns:
            ``QueueInfo`` with ``state`` "running"/"idle" on success, or
            ``state="error"`` with ``error`` set if the passive declare
            raises. Returns ``None`` only if there is no live channel to
            query with (caller decides what that means -- e.g. "not
            connected" vs. "not initialized").
        """
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

    # =========================================================================
    # Exchange and Queue Declaration Methods (moved from bus.py P1-006)
    # =========================================================================

    async def _declare_exchange(self) -> None:
        """Declare the main event exchange.

        Creates an exchange with the configured type (topic, direct, fanout, or headers)
        for event routing. The exchange type determines how messages are routed to queues.

        - topic: Route based on routing key patterns (e.g., "order.*", "*.created")
        - direct: Route to queues with exact routing key match
        - fanout: Broadcast to all bound queues regardless of routing key
        - headers: Route based on message header attributes

        Raises:
            RuntimeError: If called with no channel available -- an
                internal call-order bug (see module docstring), not a
                condition reachable by a caller of the public API.
        """
        channel = self._connection.require_channel()

        # Map string to ExchangeType enum
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
        """Declare the consumer queue with optional DLQ configuration.

        Creates a durable queue for this consumer group. If DLQ is enabled,
        the queue is configured with dead letter exchange arguments so that
        rejected messages are automatically routed to the DLQ.

        Raises:
            RuntimeError: If called with no channel available -- an
                internal call-order bug (see module docstring), not a
                condition reachable by a caller of the public API.
        """
        channel = self._connection.require_channel()

        # Queue arguments for DLQ routing
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
        """Declare dead letter exchange and queue.

        Creates a DLQ exchange and queue for handling messages that fail
        after max_retries attempts. The DLQ uses a direct exchange type
        for simple routing based on the original queue name.

        This allows failed messages to be inspected and potentially replayed
        after fixing the underlying issue.

        Raises:
            RuntimeError: If called with no channel available -- an
                internal call-order bug (see module docstring), not a
                condition reachable by a caller of the public API.
        """
        channel = self._connection.require_channel()

        # Declare DLQ exchange as direct type
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

        # Build DLQ queue arguments
        dlq_arguments: dict[str, Any] = {}

        if self._config.dlq_message_ttl is not None:
            dlq_arguments["x-message-ttl"] = self._config.dlq_message_ttl

        if self._config.dlq_max_length is not None:
            dlq_arguments["x-max-length"] = self._config.dlq_max_length

        # Declare DLQ queue with optional TTL and max_length
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

        # Bind DLQ queue to DLQ exchange
        # Uses the main queue name as routing key (matches x-dead-letter-routing-key)
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
