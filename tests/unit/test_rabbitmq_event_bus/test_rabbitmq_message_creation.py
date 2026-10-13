"""Unit tests extracted for test_rabbitmq_message_creation.py."""

from __future__ import annotations

import json
from uuid import uuid4

import pytest

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
)

from ._fixtures import SerializationTestEvent


class TestRabbitMQCreateMessage:
    """Tests for message creation in RabbitMQEventBus."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig()

    @pytest.fixture
    def bus(self, config: RabbitMQEventBusConfig) -> RabbitMQEventBus:
        """Create bus instance for testing."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        return RabbitMQEventBus(config=config)

    @pytest.fixture
    def sample_event(self) -> SerializationTestEvent:
        """Create a sample event for testing."""
        return SerializationTestEvent(
            aggregate_id=uuid4(),
            data="test-data",
        )

    def test_create_message_returns_message(
        self, bus: RabbitMQEventBus, sample_event: SerializationTestEvent
    ) -> None:
        """Test that _create_message returns an aio_pika.Message."""
        from aio_pika import Message

        message = bus._create_message(sample_event)

        assert isinstance(message, Message)

    def test_create_message_content_type(
        self, bus: RabbitMQEventBus, sample_event: SerializationTestEvent
    ) -> None:
        """Test that message has correct content_type."""
        message = bus._create_message(sample_event)

        assert message.content_type == "application/json"

    def test_create_message_content_encoding(
        self, bus: RabbitMQEventBus, sample_event: SerializationTestEvent
    ) -> None:
        """Test that message has correct content_encoding."""
        message = bus._create_message(sample_event)

        assert message.content_encoding == "utf-8"

    def test_create_message_delivery_mode_persistent(
        self, bus: RabbitMQEventBus, sample_event: SerializationTestEvent
    ) -> None:
        """Test that message uses persistent delivery mode."""
        from aio_pika import DeliveryMode

        message = bus._create_message(sample_event)

        assert message.delivery_mode == DeliveryMode.PERSISTENT

    def test_create_message_message_id(
        self, bus: RabbitMQEventBus, sample_event: SerializationTestEvent
    ) -> None:
        """Test that message_id is set to event_id."""
        message = bus._create_message(sample_event)

        assert message.message_id == str(sample_event.event_id)

    def test_create_message_timestamp(
        self, bus: RabbitMQEventBus, sample_event: SerializationTestEvent
    ) -> None:
        """Test that timestamp is set to occurred_at."""
        message = bus._create_message(sample_event)

        assert message.timestamp == sample_event.occurred_at

    def test_create_message_headers(
        self, bus: RabbitMQEventBus, sample_event: SerializationTestEvent
    ) -> None:
        """Test that headers are set correctly."""
        message = bus._create_message(sample_event)

        assert message.headers is not None
        assert message.headers["event_type"] == "SerializationTestEvent"
        assert message.headers["aggregate_type"] == "TestAggregate"
        assert message.headers["x-retry-count"] == 0

    def test_create_message_body_is_json(
        self, bus: RabbitMQEventBus, sample_event: SerializationTestEvent
    ) -> None:
        """Test that body is valid JSON."""
        message = bus._create_message(sample_event)

        parsed = json.loads(message.body.decode("utf-8"))
        assert parsed["data"] == "test-data"


class TestDLQMessageDataclass:
    """Tests for DLQMessage dataclass."""

    def test_dlq_message_with_all_fields(self) -> None:
        """Test DLQMessage with all fields populated."""
        from eventsource.adapters.rabbitmq import DLQMessage

        msg = DLQMessage(
            message_id="test-id-123",
            routing_key="Order.OrderCreated",
            body='{"event_type": "OrderCreated"}',
            headers={"event_type": "OrderCreated", "x-dlq-reason": "Error"},
            event_type="OrderCreated",
            dlq_reason="Processing failed",
            dlq_error_type="ValueError",
            dlq_retry_count=3,
            dlq_timestamp="2024-01-15T10:30:00Z",
            original_routing_key="Order.OrderCreated",
        )

        assert msg.message_id == "test-id-123"
        assert msg.routing_key == "Order.OrderCreated"
        assert msg.body == '{"event_type": "OrderCreated"}'
        assert msg.headers == {"event_type": "OrderCreated", "x-dlq-reason": "Error"}
        assert msg.event_type == "OrderCreated"
        assert msg.dlq_reason == "Processing failed"
        assert msg.dlq_error_type == "ValueError"
        assert msg.dlq_retry_count == 3
        assert msg.dlq_timestamp == "2024-01-15T10:30:00Z"
        assert msg.original_routing_key == "Order.OrderCreated"

    def test_dlq_message_with_minimal_fields(self) -> None:
        """Test DLQMessage with only required fields."""
        from eventsource.adapters.rabbitmq import DLQMessage

        msg = DLQMessage(
            message_id=None,
            routing_key=None,
            body="{}",
            headers={},
        )

        assert msg.message_id is None
        assert msg.routing_key is None
        assert msg.body == "{}"
        assert msg.headers == {}
        assert msg.event_type is None
        assert msg.dlq_reason is None
        assert msg.dlq_error_type is None
        assert msg.dlq_retry_count is None
        assert msg.dlq_timestamp is None
        assert msg.original_routing_key is None

    def test_dlq_message_default_values(self) -> None:
        """Test DLQMessage default values for optional fields."""
        from eventsource.adapters.rabbitmq import DLQMessage

        msg = DLQMessage(
            message_id="id",
            routing_key="key",
            body="body",
            headers={"x": "y"},
        )

        # Default values should be None
        assert msg.event_type is None
        assert msg.dlq_reason is None
        assert msg.dlq_error_type is None
        assert msg.dlq_retry_count is None
        assert msg.dlq_timestamp is None
        assert msg.original_routing_key is None


class TestDLQMessageExport:
    """Tests for DLQMessage export in __all__."""

    def test_dlq_message_exported(self) -> None:
        """Test that DLQMessage is exported from rabbitmq module."""
        from eventsource.adapters.rabbitmq import DLQMessage

        assert DLQMessage is not None

    def test_dlq_message_in_all(self) -> None:
        """Test that DLQMessage is in __all__."""
        from eventsource.adapters import rabbitmq

        assert "DLQMessage" in rabbitmq.__all__
