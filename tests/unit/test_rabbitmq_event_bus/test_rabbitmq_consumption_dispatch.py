"""Unit tests extracted for test_rabbitmq_consumption_dispatch.py."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
)
from eventsource.domain.event import DomainEvent

from ._fixtures import ConsumerTestEvent


class TestRabbitMQDispatchEvent:
    """Tests for _dispatch_event() method."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
        )

    @pytest.fixture
    def bus(self, config: RabbitMQEventBusConfig) -> RabbitMQEventBus:
        """Create bus instance for testing."""
        bus = RabbitMQEventBus(config=config)
        bus._connected = True
        return bus

    @pytest.mark.asyncio
    async def test_dispatch_to_specific_handlers(self, bus: RabbitMQEventBus) -> None:
        """Test that _dispatch_event calls specific event handlers."""
        received = []

        async def handler(event: DomainEvent) -> None:
            received.append(event)

        bus.subscribe(ConsumerTestEvent, handler)

        event = ConsumerTestEvent(aggregate_id=uuid4(), data="test")
        mock_message = MagicMock()
        mock_message.message_id = "test-id"

        await bus._dispatch_event(event, mock_message)

        assert len(received) == 1
        assert received[0] is event

    @pytest.mark.asyncio
    async def test_dispatch_to_wildcard_handlers(self, bus: RabbitMQEventBus) -> None:
        """Test that _dispatch_event calls wildcard handlers."""
        received = []

        async def wildcard_handler(event: DomainEvent) -> None:
            received.append(event)

        bus.subscribe_to_all_events(wildcard_handler)

        event = ConsumerTestEvent(aggregate_id=uuid4(), data="test")
        mock_message = MagicMock()
        mock_message.message_id = "test-id"

        await bus._dispatch_event(event, mock_message)

        assert len(received) == 1
        assert received[0] is event

    @pytest.mark.asyncio
    async def test_dispatch_to_both_specific_and_wildcard(self, bus: RabbitMQEventBus) -> None:
        """Test that _dispatch_event calls both specific and wildcard handlers."""
        specific_received = []
        wildcard_received = []

        async def specific_handler(event: DomainEvent) -> None:
            specific_received.append(event)

        async def wildcard_handler(event: DomainEvent) -> None:
            wildcard_received.append(event)

        bus.subscribe(ConsumerTestEvent, specific_handler)
        bus.subscribe_to_all_events(wildcard_handler)

        event = ConsumerTestEvent(aggregate_id=uuid4(), data="test")
        mock_message = MagicMock()
        mock_message.message_id = "test-id"

        await bus._dispatch_event(event, mock_message)

        assert len(specific_received) == 1
        assert len(wildcard_received) == 1
        assert specific_received[0] is event
        assert wildcard_received[0] is event

    @pytest.mark.asyncio
    async def test_dispatch_with_no_handlers(self, bus: RabbitMQEventBus) -> None:
        """Test that _dispatch_event handles no handlers gracefully."""
        event = ConsumerTestEvent(aggregate_id=uuid4(), data="test")
        mock_message = MagicMock()
        mock_message.message_id = "test-id"

        # Should not raise
        await bus._dispatch_event(event, mock_message)

    @pytest.mark.asyncio
    async def test_dispatch_raises_on_handler_error(self, bus: RabbitMQEventBus) -> None:
        """Test that _dispatch_event raises an aggregate HandlerDispatchError."""
        from eventsource.domain.exceptions import HandlerDispatchError

        async def failing_handler(event: DomainEvent) -> None:
            raise ValueError("Handler error")

        bus.subscribe(ConsumerTestEvent, failing_handler)

        event = ConsumerTestEvent(aggregate_id=uuid4(), data="test")
        mock_message = MagicMock()
        mock_message.message_id = "test-id"

        with pytest.raises(HandlerDispatchError) as exc_info:
            await bus._dispatch_event(event, mock_message)

        assert len(exc_info.value.failures) == 1
        failed_name, failed_exc = exc_info.value.failures[0]
        assert isinstance(failed_exc, ValueError)
        assert bus.stats.handler_errors == 1

    @pytest.mark.asyncio
    async def test_dispatch_isolates_handler_errors(self, bus: RabbitMQEventBus) -> None:
        """A failing handler must not prevent other handlers from running.

        Both handlers should be invoked even though the first one raises;
        the failure is reported once dispatch has run every handler, via an
        aggregate HandlerDispatchError (not the underlying exception
        directly), so the message is still left unacked for redelivery.
        """
        from eventsource.domain.exceptions import HandlerDispatchError

        call_order = []

        async def first_handler(event: DomainEvent) -> None:
            call_order.append("first")
            raise ValueError("First handler error")

        async def second_handler(event: DomainEvent) -> None:
            call_order.append("second")

        bus.subscribe(ConsumerTestEvent, first_handler)
        bus.subscribe(ConsumerTestEvent, second_handler)

        event = ConsumerTestEvent(aggregate_id=uuid4(), data="test")
        mock_message = MagicMock()
        mock_message.message_id = "test-id"

        with pytest.raises(HandlerDispatchError):
            await bus._dispatch_event(event, mock_message)

        assert call_order == ["first", "second"]

    @pytest.mark.asyncio
    async def test_dispatch_calls_handlers_sequentially(self, bus: RabbitMQEventBus) -> None:
        """Test that _dispatch_event calls handlers in order."""
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

        event = ConsumerTestEvent(aggregate_id=uuid4(), data="test")
        mock_message = MagicMock()
        mock_message.message_id = "test-id"

        await bus._dispatch_event(event, mock_message)

        # Specific handlers first, then wildcard
        assert call_order == ["handler1", "handler2", "wildcard"]

    @pytest.mark.asyncio
    async def test_dispatch_with_sync_handler(self, bus: RabbitMQEventBus) -> None:
        """Test that _dispatch_event handles synchronous handlers."""
        received = []

        def sync_handler(event: DomainEvent) -> None:
            received.append(event)

        bus.subscribe(ConsumerTestEvent, sync_handler)

        event = ConsumerTestEvent(aggregate_id=uuid4(), data="test")
        mock_message = MagicMock()
        mock_message.message_id = "test-id"

        await bus._dispatch_event(event, mock_message)

        assert len(received) == 1


class TestRabbitMQConsumerLoopErrorHandling:
    """Tests for consumer loop error handling and edge cases."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            consumer_name="error-test-consumer",
        )

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_consumer_loop_handles_general_exception(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that consumer loop handles and re-raises general exceptions."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()

        async def failing_iterator() -> None:
            raise RuntimeError("Queue iteration failed")

        mock_queue_iter = AsyncMock()
        mock_queue_iter.__aenter__ = AsyncMock(side_effect=RuntimeError("Queue iteration failed"))
        mock_queue_iter.__aexit__ = AsyncMock(return_value=None)
        mock_queue.iterator = MagicMock(return_value=mock_queue_iter)

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        bus = RabbitMQEventBus(config=config)

        with pytest.raises(RuntimeError, match="Queue iteration failed"):
            await bus.start_consuming()

        # Consuming flag should be reset after exception
        assert bus.is_consuming is False

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_consumer_stops_when_consuming_flag_cleared(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that consumer loop stops when is_consuming is set to False."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()

        message_count = 0

        async def controlled_iterator():
            nonlocal message_count
            while message_count < 5:
                message_count += 1
                mock_msg = AsyncMock()
                mock_msg.headers = {"event_type": "UnknownEvent"}
                mock_msg.body = b'{"data": "test"}'
                mock_msg.message_id = f"msg-{message_count}"
                mock_msg.routing_key = "test"
                yield mock_msg
                if message_count >= 2:
                    # Simulate stop being called
                    bus._consuming = False

        mock_queue_iter = AsyncMock()
        mock_queue_iter.__aenter__ = AsyncMock(return_value=controlled_iterator())
        mock_queue_iter.__aexit__ = AsyncMock(return_value=None)
        mock_queue.iterator = MagicMock(return_value=mock_queue_iter)

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        bus = RabbitMQEventBus(config=config)

        await bus.start_consuming()

        # Should have processed some messages then stopped
        assert message_count >= 2
        assert bus.is_consuming is False
