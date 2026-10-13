"""Unit tests extracted for test_rabbitmq_consumption_stats.py."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
)
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry

from ._fixtures import ConsumerTestEvent


class TestRabbitMQConsumerStatistics:
    """Tests for consumer loop statistics tracking."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
        )

    @pytest.fixture
    def registry(self) -> EventRegistry:
        """Create event registry with test event."""
        registry = EventRegistry()
        registry.register(ConsumerTestEvent)
        return registry

    @pytest.fixture
    def bus(self, config: RabbitMQEventBusConfig, registry: EventRegistry) -> RabbitMQEventBus:
        """Create bus instance for testing."""
        bus = RabbitMQEventBus(config=config, event_registry=registry)
        bus._connected = True
        return bus

    @pytest.mark.asyncio
    async def test_events_consumed_incremented_on_success(self, bus: RabbitMQEventBus) -> None:
        """Test that events_consumed is incremented on successful processing."""
        event = ConsumerTestEvent(aggregate_id=uuid4(), data="test")
        mock_message = AsyncMock()
        mock_message.headers = {"event_type": "ConsumerTestEvent"}
        mock_message.body = event.model_dump_json().encode("utf-8")
        mock_message.message_id = "test-id"
        mock_message.routing_key = "test"

        async def handler(e: DomainEvent) -> None:
            pass

        bus.subscribe(ConsumerTestEvent, handler)

        initial_consumed = bus.stats.events_consumed

        await bus._process_message(mock_message)

        assert bus.stats.events_consumed == initial_consumed + 1

    @pytest.mark.asyncio
    async def test_events_processed_success_incremented(self, bus: RabbitMQEventBus) -> None:
        """Test that events_processed_success is incremented."""
        event = ConsumerTestEvent(aggregate_id=uuid4(), data="test")
        mock_message = AsyncMock()
        mock_message.headers = {"event_type": "ConsumerTestEvent"}
        mock_message.body = event.model_dump_json().encode("utf-8")
        mock_message.message_id = "test-id"
        mock_message.routing_key = "test"

        async def handler(e: DomainEvent) -> None:
            pass

        bus.subscribe(ConsumerTestEvent, handler)

        initial_success = bus.stats.events_processed_success

        await bus._process_message(mock_message)

        assert bus.stats.events_processed_success == initial_success + 1

    @pytest.mark.asyncio
    async def test_events_processed_failed_incremented_on_error(
        self, bus: RabbitMQEventBus
    ) -> None:
        """Test that events_processed_failed is incremented on handler error."""
        event = ConsumerTestEvent(aggregate_id=uuid4(), data="test")
        mock_message = AsyncMock()
        mock_message.headers = {"event_type": "ConsumerTestEvent", "x-retry-count": 0}
        mock_message.body = event.model_dump_json().encode("utf-8")
        mock_message.content_type = "application/json"
        mock_message.content_encoding = "utf-8"
        mock_message.message_id = "test-id"
        mock_message.routing_key = "test"

        # Set up exchange for retry republish
        mock_exchange = AsyncMock()
        bus._exchange = mock_exchange

        async def failing_handler(e: DomainEvent) -> None:
            raise ValueError("Error")

        bus.subscribe(ConsumerTestEvent, failing_handler)

        initial_failed = bus.stats.events_processed_failed

        await bus._process_message(mock_message)

        assert bus.stats.events_processed_failed == initial_failed + 1

    @pytest.mark.asyncio
    async def test_handler_errors_incremented(self, bus: RabbitMQEventBus) -> None:
        """Test that handler_errors is incremented on handler exception."""
        event = ConsumerTestEvent(aggregate_id=uuid4(), data="test")
        mock_message = MagicMock()
        mock_message.message_id = "test-id"

        async def failing_handler(e: DomainEvent) -> None:
            raise ValueError("Error")

        bus.subscribe(ConsumerTestEvent, failing_handler)

        initial_errors = bus.stats.handler_errors

        from eventsource.domain.exceptions import HandlerDispatchError

        with pytest.raises(HandlerDispatchError):
            await bus._dispatch_event(event, mock_message)

        assert bus.stats.handler_errors == initial_errors + 1

    @pytest.mark.asyncio
    async def test_stats_not_incremented_for_unknown_event(self, bus: RabbitMQEventBus) -> None:
        """Test that stats are not incremented for unknown event types."""
        mock_message = AsyncMock()
        mock_message.headers = {"event_type": "UnknownEvent"}
        mock_message.body = b'{"aggregate_id": "test-id"}'
        mock_message.message_id = "test-id"
        mock_message.routing_key = "test"

        initial_consumed = bus.stats.events_consumed
        initial_success = bus.stats.events_processed_success

        await bus._process_message(mock_message)

        # Stats should not change for unknown events
        assert bus.stats.events_consumed == initial_consumed
        assert bus.stats.events_processed_success == initial_success
