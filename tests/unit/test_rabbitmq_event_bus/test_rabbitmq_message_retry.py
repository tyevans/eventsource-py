"""Unit tests extracted for test_rabbitmq_message_retry.py."""

from __future__ import annotations

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
)
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry

from ._fixtures import ConsumerTestEvent


class TestHandleFailedMessage:
    """Tests for the _handle_failed_message method."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            max_retries=3,
            retry_base_delay=0.01,  # Very short for testing
            retry_max_delay=1.0,
            retry_jitter=0.0,
        )

    @pytest.fixture
    def bus(self, config: RabbitMQEventBusConfig) -> RabbitMQEventBus:
        """Create bus instance."""
        return RabbitMQEventBus(config=config)

    @pytest.fixture
    def mock_message(self) -> AsyncMock:
        """Create mock incoming message."""
        message = AsyncMock()
        message.headers = {
            "event_type": "TestEvent",
            "aggregate_type": "TestAggregate",
            "x-retry-count": 0,
        }
        message.body = b'{"test": "data"}'
        message.content_type = "application/json"
        message.content_encoding = "utf-8"
        message.message_id = "test-message-id"
        message.routing_key = "TestAggregate.TestEvent"
        return message

    @pytest.mark.asyncio
    async def test_retry_when_under_max_retries(
        self, bus: RabbitMQEventBus, mock_message: AsyncMock
    ) -> None:
        """Test that message is republished when under max_retries."""
        mock_exchange = AsyncMock()
        bus._exchange = mock_exchange

        error = ValueError("Test error")
        await bus._handle_failed_message(mock_message, error, retry_count=0)
        await bus._consumer.drain_retries()
        # Should republish, not send to DLQ
        mock_exchange.publish.assert_called_once()
        mock_message.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_dlq_when_at_max_retries(
        self, bus: RabbitMQEventBus, mock_message: AsyncMock
    ) -> None:
        """Test that message is sent to DLQ at max_retries."""
        mock_exchange = AsyncMock()
        mock_dlq_exchange = AsyncMock()
        bus._exchange = mock_exchange
        bus._dlq_exchange = mock_dlq_exchange

        error = ValueError("Test error")
        await bus._handle_failed_message(mock_message, error, retry_count=3)

        # Should send to DLQ, not republish to main exchange
        mock_dlq_exchange.publish.assert_called_once()
        mock_exchange.publish.assert_not_called()
        mock_message.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_dlq_when_over_max_retries(
        self, bus: RabbitMQEventBus, mock_message: AsyncMock
    ) -> None:
        """Test that message is sent to DLQ when over max_retries."""
        mock_exchange = AsyncMock()
        mock_dlq_exchange = AsyncMock()
        bus._exchange = mock_exchange
        bus._dlq_exchange = mock_dlq_exchange

        error = ValueError("Test error")
        await bus._handle_failed_message(mock_message, error, retry_count=5)

        # Should send to DLQ
        mock_dlq_exchange.publish.assert_called_once()

    @pytest.mark.asyncio
    async def test_retry_increments_count(
        self, bus: RabbitMQEventBus, mock_message: AsyncMock
    ) -> None:
        """Test that retry count is incremented in republished message."""
        mock_exchange = AsyncMock()
        bus._exchange = mock_exchange

        error = ValueError("Test error")
        await bus._handle_failed_message(mock_message, error, retry_count=1)
        await bus._consumer.drain_retries()

        call_args = mock_exchange.publish.call_args
        new_message = call_args[0][0]
        assert new_message.headers["x-retry-count"] == 2


class TestProcessMessageWithRetry:
    """Tests for _process_message with retry handling."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            max_retries=3,
            retry_base_delay=0.01,
            retry_jitter=0.0,
        )

    @pytest.fixture
    def registry(self) -> EventRegistry:
        """Create event registry with test event."""
        registry = EventRegistry()
        registry.register(ConsumerTestEvent)
        return registry

    @pytest.fixture
    def bus(self, config: RabbitMQEventBusConfig, registry: EventRegistry) -> RabbitMQEventBus:
        """Create bus instance."""
        return RabbitMQEventBus(config=config, event_registry=registry)

    @pytest.fixture
    def mock_message(self) -> AsyncMock:
        """Create mock incoming message with proper event JSON."""
        event = ConsumerTestEvent(aggregate_id=uuid4(), data="test-data")
        event_json = event.model_dump_json()

        message = AsyncMock()
        message.headers = {
            "event_type": "ConsumerTestEvent",
            "aggregate_type": "ConsumerTest",
            "x-retry-count": 0,
        }
        message.body = event_json.encode("utf-8")
        message.content_type = "application/json"
        message.content_encoding = "utf-8"
        message.message_id = "test-message-id"
        message.routing_key = "ConsumerTest.ConsumerTestEvent"
        return message

    @pytest.mark.asyncio
    async def test_reads_retry_count_from_header(
        self, bus: RabbitMQEventBus, mock_message: AsyncMock
    ) -> None:
        """Test that retry count is read from x-retry-count header."""
        mock_message.headers["x-retry-count"] = 2

        mock_exchange = AsyncMock()
        bus._exchange = mock_exchange

        # Create a failing handler
        async def failing_handler(event: DomainEvent) -> None:
            raise ValueError("Intentional failure")

        bus.subscribe_to_all_events(failing_handler)
        await bus._process_message(mock_message)
        await bus._consumer.drain_retries()

        # Should have republished with retry_count + 1 = 3
        call_args = mock_exchange.publish.call_args
        new_message = call_args[0][0]
        assert new_message.headers["x-retry-count"] == 3

    @pytest.mark.asyncio
    async def test_default_retry_count_is_zero(
        self, bus: RabbitMQEventBus, mock_message: AsyncMock
    ) -> None:
        """Test that default retry count is 0 when header is missing."""
        del mock_message.headers["x-retry-count"]

        mock_exchange = AsyncMock()
        bus._exchange = mock_exchange

        async def failing_handler(event: DomainEvent) -> None:
            raise ValueError("Intentional failure")

        bus.subscribe_to_all_events(failing_handler)
        await bus._process_message(mock_message)
        await bus._consumer.drain_retries()

        # Should have republished with retry_count = 1
        call_args = mock_exchange.publish.call_args
        new_message = call_args[0][0]
        assert new_message.headers["x-retry-count"] == 1

    @pytest.mark.asyncio
    async def test_success_does_not_trigger_retry(
        self, bus: RabbitMQEventBus, mock_message: AsyncMock
    ) -> None:
        """Test that successful processing does not trigger retry logic."""

        mock_exchange = AsyncMock()
        bus._exchange = mock_exchange

        # Create a successful handler
        async def success_handler(event: DomainEvent) -> None:
            pass

        bus.subscribe_to_all_events(success_handler)

        await bus._process_message(mock_message)

        # Should just ack, not republish
        mock_exchange.publish.assert_not_called()
        mock_message.ack.assert_called_once()
        assert bus.stats.events_processed_success == 1

    @pytest.mark.asyncio
    async def test_failure_increments_failed_stats(
        self, bus: RabbitMQEventBus, mock_message: AsyncMock
    ) -> None:
        """Test that failure increments events_processed_failed stat."""
        mock_exchange = AsyncMock()
        bus._exchange = mock_exchange

        async def failing_handler(event: DomainEvent) -> None:
            raise ValueError("Intentional failure")

        bus.subscribe_to_all_events(failing_handler)

        await bus._process_message(mock_message)

        assert bus.stats.events_processed_failed == 1
