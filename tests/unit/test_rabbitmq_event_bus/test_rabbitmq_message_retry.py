"""Unit tests extracted for test_rabbitmq_message_retry.py."""

from __future__ import annotations

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from eventsource.adapters.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry

from ._fixtures import ConsumerTestEvent


class TestHandleFailedMessage:
    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        return RabbitMQEventBusConfig(
            max_retries=3, retry_base_delay=0.01, retry_max_delay=1.0, retry_jitter=0.0
        )

    @pytest.fixture
    def bus(self, config: RabbitMQEventBusConfig) -> RabbitMQEventBus:
        return RabbitMQEventBus(config=config)

    @pytest.fixture
    def mock_message(self) -> AsyncMock:
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
        mock_exchange = AsyncMock()
        bus._topology._exchange = mock_exchange
        await bus._handle_failed_message(mock_message, ValueError("Test error"), retry_count=0)
        await bus._consumer.drain_retries()
        mock_exchange.publish.assert_called_once()
        mock_message.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_dlq_when_at_max_retries(
        self, bus: RabbitMQEventBus, mock_message: AsyncMock
    ) -> None:
        mock_exchange = AsyncMock()
        mock_dlq_exchange = AsyncMock()
        bus._topology._exchange = mock_exchange
        bus._topology._dlq_exchange = mock_dlq_exchange
        await bus._handle_failed_message(mock_message, ValueError("Test error"), retry_count=3)
        mock_dlq_exchange.publish.assert_called_once()
        mock_exchange.publish.assert_not_called()
        mock_message.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_dlq_when_over_max_retries(
        self, bus: RabbitMQEventBus, mock_message: AsyncMock
    ) -> None:
        mock_exchange = AsyncMock()
        mock_dlq_exchange = AsyncMock()
        bus._topology._exchange = mock_exchange
        bus._topology._dlq_exchange = mock_dlq_exchange
        await bus._handle_failed_message(mock_message, ValueError("Test error"), retry_count=5)
        mock_dlq_exchange.publish.assert_called_once()

    @pytest.mark.asyncio
    async def test_retry_increments_count(
        self, bus: RabbitMQEventBus, mock_message: AsyncMock
    ) -> None:
        mock_exchange = AsyncMock()
        bus._topology._exchange = mock_exchange
        await bus._handle_failed_message(mock_message, ValueError("Test error"), retry_count=1)
        await bus._consumer.drain_retries()
        new_message = mock_exchange.publish.call_args[0][0]
        assert new_message.headers["x-retry-count"] == 2


class TestProcessMessageWithRetry:
    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        return RabbitMQEventBusConfig(max_retries=3, retry_base_delay=0.01, retry_jitter=0.0)

    @pytest.fixture
    def registry(self) -> EventRegistry:
        registry = EventRegistry()
        registry.register(ConsumerTestEvent)
        return registry

    @pytest.fixture
    def bus(self, config: RabbitMQEventBusConfig, registry: EventRegistry) -> RabbitMQEventBus:
        return RabbitMQEventBus(config=config, event_registry=registry)

    @pytest.fixture
    def mock_message(self) -> AsyncMock:
        event = ConsumerTestEvent(aggregate_id=uuid4(), data="test-data")
        message = AsyncMock()
        message.headers = {
            "event_type": "ConsumerTestEvent",
            "aggregate_type": "ConsumerTest",
            "x-retry-count": 0,
        }
        message.body = event.model_dump_json().encode("utf-8")
        message.content_type = "application/json"
        message.content_encoding = "utf-8"
        message.message_id = "test-message-id"
        message.routing_key = "ConsumerTest.ConsumerTestEvent"
        return message

    @pytest.mark.asyncio
    async def test_reads_retry_count_from_header(
        self, bus: RabbitMQEventBus, mock_message: AsyncMock
    ) -> None:
        mock_message.headers["x-retry-count"] = 2
        mock_exchange = AsyncMock()
        bus._topology._exchange = mock_exchange

        async def failing_handler(event: DomainEvent) -> None:
            raise ValueError("Intentional failure")

        bus.subscribe_to_all_events(failing_handler)
        await bus._process_message(mock_message)
        await bus._consumer.drain_retries()
        new_message = mock_exchange.publish.call_args[0][0]
        assert new_message.headers["x-retry-count"] == 3

    @pytest.mark.asyncio
    async def test_default_retry_count_is_zero(
        self, bus: RabbitMQEventBus, mock_message: AsyncMock
    ) -> None:
        del mock_message.headers["x-retry-count"]
        mock_exchange = AsyncMock()
        bus._topology._exchange = mock_exchange

        async def failing_handler(event: DomainEvent) -> None:
            raise ValueError("Intentional failure")

        bus.subscribe_to_all_events(failing_handler)
        await bus._process_message(mock_message)
        await bus._consumer.drain_retries()
        new_message = mock_exchange.publish.call_args[0][0]
        assert new_message.headers["x-retry-count"] == 1

    @pytest.mark.asyncio
    async def test_success_does_not_trigger_retry(
        self, bus: RabbitMQEventBus, mock_message: AsyncMock
    ) -> None:
        mock_exchange = AsyncMock()
        bus._topology._exchange = mock_exchange

        async def success_handler(event: DomainEvent) -> None:
            pass

        bus.subscribe_to_all_events(success_handler)
        await bus._process_message(mock_message)
        mock_exchange.publish.assert_not_called()
        mock_message.ack.assert_called_once()
        assert bus.stats.events_processed_success == 1

    @pytest.mark.asyncio
    async def test_failure_increments_failed_stats(
        self, bus: RabbitMQEventBus, mock_message: AsyncMock
    ) -> None:
        bus._topology._exchange = AsyncMock()

        async def failing_handler(event: DomainEvent) -> None:
            raise ValueError("Intentional failure")

        bus.subscribe_to_all_events(failing_handler)
        await bus._process_message(mock_message)
        assert bus.stats.events_processed_failed == 1
