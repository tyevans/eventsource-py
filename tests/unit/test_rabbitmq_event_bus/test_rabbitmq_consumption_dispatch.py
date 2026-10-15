"""Unit tests extracted for test_rabbitmq_consumption_dispatch.py."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.adapters.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig
from eventsource.domain.event import DomainEvent
from eventsource.domain.exceptions import HandlerDispatchError

from ._fixtures import ConsumerTestEvent


def _evt_and_msg() -> tuple[ConsumerTestEvent, MagicMock]:
    return ConsumerTestEvent(aggregate_id=uuid4(), data="test"), MagicMock(message_id="test-id")


class TestRabbitMQDispatchEvent:
    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        return RabbitMQEventBusConfig(rabbitmq_url="amqp://test:test@localhost/")

    @pytest.fixture
    def bus(self, config: RabbitMQEventBusConfig) -> RabbitMQEventBus:
        bus = RabbitMQEventBus(config=config)
        bus._connection_manager._connected = True
        return bus

    @pytest.mark.asyncio
    async def test_dispatch_to_specific_handlers(self, bus: RabbitMQEventBus) -> None:
        received = []

        async def handler(event: DomainEvent) -> None:
            received.append(event)

        bus.subscribe(ConsumerTestEvent, handler)
        event, mock_message = _evt_and_msg()
        await bus._dispatch_event(event, mock_message)
        assert received == [event]

    @pytest.mark.asyncio
    async def test_dispatch_to_wildcard_handlers(self, bus: RabbitMQEventBus) -> None:
        received = []

        async def wildcard_handler(event: DomainEvent) -> None:
            received.append(event)

        bus.subscribe_to_all_events(wildcard_handler)
        event, mock_message = _evt_and_msg()
        await bus._dispatch_event(event, mock_message)
        assert received == [event]

    @pytest.mark.asyncio
    async def test_dispatch_to_both_specific_and_wildcard(self, bus: RabbitMQEventBus) -> None:
        specific_received, wildcard_received = [], []

        async def specific_handler(event: DomainEvent) -> None:
            specific_received.append(event)

        async def wildcard_handler(event: DomainEvent) -> None:
            wildcard_received.append(event)

        bus.subscribe(ConsumerTestEvent, specific_handler)
        bus.subscribe_to_all_events(wildcard_handler)
        event, mock_message = _evt_and_msg()
        await bus._dispatch_event(event, mock_message)
        assert specific_received == [event]
        assert wildcard_received == [event]

    @pytest.mark.asyncio
    async def test_dispatch_with_no_handlers(self, bus: RabbitMQEventBus) -> None:
        event, mock_message = _evt_and_msg()
        await bus._dispatch_event(event, mock_message)

    @pytest.mark.asyncio
    async def test_dispatch_raises_on_handler_error(self, bus: RabbitMQEventBus) -> None:
        async def failing_handler(event: DomainEvent) -> None:
            raise ValueError("Handler error")

        bus.subscribe(ConsumerTestEvent, failing_handler)
        event, mock_message = _evt_and_msg()
        with pytest.raises(HandlerDispatchError) as exc_info:
            await bus._dispatch_event(event, mock_message)
        assert len(exc_info.value.failures) == 1
        _, failed_exc = exc_info.value.failures[0]
        assert isinstance(failed_exc, ValueError)
        assert bus.stats.handler_errors == 1

    @pytest.mark.asyncio
    async def test_dispatch_isolates_handler_errors(self, bus: RabbitMQEventBus) -> None:
        call_order = []

        async def first_handler(event: DomainEvent) -> None:
            call_order.append("first")
            raise ValueError("First handler error")

        async def second_handler(event: DomainEvent) -> None:
            call_order.append("second")

        bus.subscribe(ConsumerTestEvent, first_handler)
        bus.subscribe(ConsumerTestEvent, second_handler)
        event, mock_message = _evt_and_msg()
        with pytest.raises(HandlerDispatchError):
            await bus._dispatch_event(event, mock_message)
        assert call_order == ["first", "second"]

    @pytest.mark.asyncio
    async def test_dispatch_calls_handlers_sequentially(self, bus: RabbitMQEventBus) -> None:
        call_order = []

        async def handler1(event: DomainEvent) -> None:
            call_order.append("handler1")

        async def handler2(event: DomainEvent) -> None:
            call_order.append("handler2")

        async def wildcard_handler(event: DomainEvent) -> None:
            call_order.append("wildcard")

        bus.subscribe(ConsumerTestEvent, handler1)
        bus.subscribe(ConsumerTestEvent, handler2)
        bus.subscribe_to_all_events(wildcard_handler)
        event, mock_message = _evt_and_msg()
        await bus._dispatch_event(event, mock_message)
        assert call_order == ["handler1", "handler2", "wildcard"]

    @pytest.mark.asyncio
    async def test_dispatch_with_sync_handler(self, bus: RabbitMQEventBus) -> None:
        received = []

        def sync_handler(event: DomainEvent) -> None:
            received.append(event)

        bus.subscribe(ConsumerTestEvent, sync_handler)
        event, mock_message = _evt_and_msg()
        await bus._dispatch_event(event, mock_message)
        assert len(received) == 1


class TestRabbitMQConsumerLoopErrorHandling:
    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            consumer_name="error-test-consumer",
        )

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_consumer_loop_handles_general_exception(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        mock_connection = AsyncMock(is_closed=False)
        mock_channel = AsyncMock(is_closed=False)
        mock_queue_iter = AsyncMock(
            __aenter__=AsyncMock(side_effect=RuntimeError("Queue iteration failed")),
            __aexit__=AsyncMock(return_value=None),
        )
        mock_queue = AsyncMock(iterator=MagicMock(return_value=mock_queue_iter))
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=AsyncMock())
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        bus = RabbitMQEventBus(config=config)
        with pytest.raises(RuntimeError, match="Queue iteration failed"):
            await bus.start_consuming()
        assert bus.is_consuming is False

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_consumer_stops_when_consuming_flag_cleared(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        mock_connection = AsyncMock(is_closed=False)
        mock_channel = AsyncMock(is_closed=False)
        message_count = 0

        async def controlled_iterator():
            nonlocal message_count
            while message_count < 5:
                message_count += 1
                yield AsyncMock(
                    headers={"event_type": "UnknownEvent"},
                    body=b'{"data": "test"}',
                    message_id=f"msg-{message_count}",
                    routing_key="test",
                )
                if message_count >= 2:
                    bus._consumer._consuming = False

        mock_queue_iter = AsyncMock(
            __aenter__=AsyncMock(return_value=controlled_iterator()),
            __aexit__=AsyncMock(return_value=None),
        )
        mock_queue = AsyncMock(iterator=MagicMock(return_value=mock_queue_iter))
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=AsyncMock())
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        bus = RabbitMQEventBus(config=config)
        await bus.start_consuming()
        assert message_count >= 2
        assert bus.is_consuming is False
