"""Unit tests extracted for test_rabbitmq_message_processing.py."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.adapters.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry

from ._fixtures import ConsumerTestEvent


class TestRabbitMQProcessMessage:
    """Tests for _process_message() method."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        return RabbitMQEventBusConfig(rabbitmq_url="amqp://test:test@localhost/")

    @pytest.fixture
    def registry(self) -> EventRegistry:
        registry = EventRegistry()
        registry.register(ConsumerTestEvent)
        return registry

    @pytest.fixture
    def bus(self, config: RabbitMQEventBusConfig, registry: EventRegistry) -> RabbitMQEventBus:
        bus = RabbitMQEventBus(config=config, event_registry=registry)
        bus._connection_manager._connected = True
        return bus

    @pytest.mark.asyncio
    async def test_process_message_acks_on_success(self, bus: RabbitMQEventBus) -> None:
        event = ConsumerTestEvent(aggregate_id=uuid4(), data="test-data")
        mock_message = AsyncMock(
            headers={"event_type": "ConsumerTestEvent"},
            body=event.model_dump_json().encode("utf-8"),
            message_id="test-msg-id",
            routing_key="ConsumerTest.ConsumerTestEvent",
        )
        received_events = []

        async def test_handler(e: DomainEvent) -> None:
            received_events.append(e)

        bus.subscribe(ConsumerTestEvent, test_handler)
        await bus._process_message(mock_message)

        mock_message.ack.assert_called_once()
        mock_message.reject.assert_not_called()
        assert len(received_events) == 1
        assert bus.stats.events_consumed == 1
        assert bus.stats.events_processed_success == 1

    @pytest.mark.asyncio
    async def test_process_message_acks_unknown_event_type(self, bus: RabbitMQEventBus) -> None:
        mock_message = AsyncMock(
            headers={"event_type": "UnknownEvent"},
            body=b'{"aggregate_id": "test-id"}',
            message_id="test-msg-id",
            routing_key="Unknown.UnknownEvent",
        )
        await bus._process_message(mock_message)
        mock_message.ack.assert_called_once()
        mock_message.reject.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_message_retries_on_handler_error(self, bus: RabbitMQEventBus) -> None:
        event = ConsumerTestEvent(aggregate_id=uuid4(), data="test-data")
        mock_message = AsyncMock(
            headers={"event_type": "ConsumerTestEvent", "x-retry-count": 0},
            body=event.model_dump_json().encode("utf-8"),
            content_type="application/json",
            content_encoding="utf-8",
            message_id="test-msg-id",
            routing_key="ConsumerTest.ConsumerTestEvent",
        )
        mock_exchange = AsyncMock()
        bus._topology._exchange = mock_exchange

        async def failing_handler(e: DomainEvent) -> None:
            raise ValueError("Handler failed")

        bus.subscribe(ConsumerTestEvent, failing_handler)
        await bus._process_message(mock_message)
        await bus._consumer.drain_retries()

        mock_message.ack.assert_called_once()
        mock_exchange.publish.assert_called_once()
        assert bus.stats.events_processed_failed == 1
        new_message = mock_exchange.publish.call_args[0][0]
        assert new_message.headers["x-retry-count"] == 1

    @pytest.mark.asyncio
    async def test_process_message_extracts_event_type_from_headers(
        self, bus: RabbitMQEventBus
    ) -> None:
        event = ConsumerTestEvent(aggregate_id=uuid4(), data="test-data")
        mock_message = AsyncMock(
            headers={"event_type": "ConsumerTestEvent", "other": "header"},
            body=event.model_dump_json().encode("utf-8"),
            message_id="test-msg-id",
            routing_key="test",
        )
        received_events = []

        async def test_handler(e: DomainEvent) -> None:
            received_events.append(e)

        bus.subscribe(ConsumerTestEvent, test_handler)
        await bus._process_message(mock_message)

        assert len(received_events) == 1
        assert received_events[0].event_type == "ConsumerTestEvent"

    @pytest.mark.asyncio
    async def test_process_message_handles_missing_headers(self, bus: RabbitMQEventBus) -> None:
        mock_message = AsyncMock(
            headers=None,
            body=b'{"aggregate_id": "test-id"}',
            message_id="test-msg-id",
            routing_key="test",
        )
        await bus._process_message(mock_message)
        mock_message.ack.assert_called_once()


class TestRabbitMQProcessMessageWithDLQTracking:
    """Tests for _process_message with DLQ tracking (P2-001)."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        return RabbitMQEventBusConfig(enable_dlq=True)

    @pytest.fixture
    def bus(self, config: RabbitMQEventBusConfig) -> RabbitMQEventBus:
        return RabbitMQEventBus(config=config)

    @pytest.fixture
    def mock_normal_message(self) -> MagicMock:
        return AsyncMock(
            headers={"event_type": "TestEvent", "aggregate_type": "TestAggregate"},
            message_id="test-message-id",
            routing_key="TestAggregate.TestEvent",
            body=b'{"event_type": "TestEvent", "aggregate_type": "TestAggregate"}',
        )

    @pytest.fixture
    def mock_dlq_message(self) -> MagicMock:
        return AsyncMock(
            headers={
                "event_type": "TestEvent",
                "aggregate_type": "TestAggregate",
                "x-death": [
                    {
                        "count": 1,
                        "reason": "rejected",
                        "queue": "events.default",
                        "exchange": "events",
                        "routing-keys": ["TestAggregate.TestEvent"],
                    }
                ],
                "x-first-death-queue": "events.default",
                "x-first-death-reason": "rejected",
                "x-first-death-exchange": "events",
            },
            message_id="test-message-id",
            routing_key="events.default",
            body=b'{"event_type": "TestEvent", "aggregate_type": "TestAggregate"}',
        )

    @pytest.mark.asyncio
    async def test_process_message_increments_dlq_stats_on_max_retries(
        self, bus: RabbitMQEventBus, mock_normal_message: MagicMock
    ) -> None:
        mock_normal_message.headers["x-retry-count"] = bus._config.max_retries

        async def failing_handler(event: DomainEvent) -> None:
            raise ValueError("Intentional failure")

        bus.subscribe_to_all_events(failing_handler)
        mock_event = MagicMock(spec=DomainEvent, event_id=uuid4(), event_type="TestEvent")
        mock_dlq_exchange = AsyncMock()
        bus._topology._dlq_exchange = mock_dlq_exchange
        initial_dlq_count = bus.stats.messages_sent_to_dlq

        with patch.object(bus._consumer, "_deserialize_event", return_value=mock_event):
            await bus._process_message(mock_normal_message)

        assert bus.stats.messages_sent_to_dlq == initial_dlq_count + 1
        mock_dlq_exchange.publish.assert_called_once()
        mock_normal_message.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_message_no_dlq_stats_when_dlq_disabled(
        self, mock_normal_message: MagicMock
    ) -> None:
        bus = RabbitMQEventBus(config=RabbitMQEventBusConfig(enable_dlq=False, max_retries=0))

        async def failing_handler(event: DomainEvent) -> None:
            raise ValueError("Intentional failure")

        bus.subscribe_to_all_events(failing_handler)
        mock_event = MagicMock(spec=DomainEvent, event_id=uuid4(), event_type="TestEvent")
        initial_dlq_count = bus.stats.messages_sent_to_dlq

        with patch.object(bus._consumer, "_deserialize_event", return_value=mock_event):
            await bus._process_message(mock_normal_message)

        assert bus.stats.messages_sent_to_dlq == initial_dlq_count
