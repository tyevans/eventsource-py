"""Unit tests extracted for test_rabbitmq_deserialization.py."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
)
from eventsource.domain.event_registry import EventRegistry

from ._fixtures import SerializationOrderCreatedEvent, SerializationTestEvent


class TestRabbitMQEventDeserialization:
    """Tests for event deserialization in RabbitMQEventBus."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig()

    @pytest.fixture
    def registry(self) -> EventRegistry:
        """Create event registry with test events."""
        registry = EventRegistry()
        registry.register(SerializationTestEvent)
        registry.register(SerializationOrderCreatedEvent)
        return registry

    @pytest.fixture
    def bus(self, config: RabbitMQEventBusConfig, registry: EventRegistry) -> RabbitMQEventBus:
        """Create bus instance with event registry."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        return RabbitMQEventBus(config=config, event_registry=registry)

    @pytest.fixture
    def mock_message(self) -> MagicMock:
        """Create a mock incoming message."""
        message = MagicMock()
        message.message_id = "test-message-id"
        return message

    def test_deserialize_event_success(
        self, bus: RabbitMQEventBus, mock_message: MagicMock
    ) -> None:
        """Test successful event deserialization."""
        aggregate_id = uuid4()
        event_data = {
            "event_id": str(uuid4()),
            "event_type": "SerializationTestEvent",
            "aggregate_type": "TestAggregate",
            "aggregate_id": str(aggregate_id),
            "aggregate_version": 1,
            "data": "test-data",
            "occurred_at": datetime.now(UTC).isoformat(),
            "correlation_id": str(uuid4()),
        }
        mock_message.headers = {"event_type": "SerializationTestEvent"}
        mock_message.body = json.dumps(event_data).encode("utf-8")

        event = bus._deserialize_event(mock_message)

        assert event is not None
        assert isinstance(event, SerializationTestEvent)
        assert event.data == "test-data"
        assert event.aggregate_id == aggregate_id

    def test_deserialize_event_missing_header_returns_none(
        self, bus: RabbitMQEventBus, mock_message: MagicMock
    ) -> None:
        """Test that missing event_type header returns None."""
        mock_message.headers = {}  # No event_type
        mock_message.body = b"{}"

        event = bus._deserialize_event(mock_message)

        assert event is None

    def test_deserialize_event_none_headers_returns_none(
        self, bus: RabbitMQEventBus, mock_message: MagicMock
    ) -> None:
        """Test that None headers returns None."""
        mock_message.headers = None
        mock_message.body = b"{}"

        event = bus._deserialize_event(mock_message)

        assert event is None

    def test_deserialize_event_unknown_type_returns_none(
        self, bus: RabbitMQEventBus, mock_message: MagicMock
    ) -> None:
        """Test that unknown event type returns None."""
        mock_message.headers = {"event_type": "UnknownEventType"}
        mock_message.body = b"{}"

        event = bus._deserialize_event(mock_message)

        assert event is None

    def test_deserialize_event_invalid_json_returns_none(
        self, bus: RabbitMQEventBus, mock_message: MagicMock
    ) -> None:
        """Test that invalid JSON returns None."""
        mock_message.headers = {"event_type": "SerializationTestEvent"}
        mock_message.body = b"not valid json"

        event = bus._deserialize_event(mock_message)

        assert event is None

    def test_deserialize_event_validation_error_returns_none(
        self, bus: RabbitMQEventBus, mock_message: MagicMock
    ) -> None:
        """Test that Pydantic validation error returns None."""
        mock_message.headers = {"event_type": "SerializationTestEvent"}
        # Missing required fields
        mock_message.body = json.dumps({"data": "test"}).encode("utf-8")

        event = bus._deserialize_event(mock_message)

        assert event is None

    def test_deserialize_order_created_event(
        self, bus: RabbitMQEventBus, mock_message: MagicMock
    ) -> None:
        """Test deserializing a more complex event."""
        aggregate_id = uuid4()
        event_data = {
            "event_id": str(uuid4()),
            "event_type": "SerializationOrderCreated",
            "aggregate_type": "Order",
            "aggregate_id": str(aggregate_id),
            "aggregate_version": 1,
            "order_number": "ORD-001",
            "customer_id": "customer-123",
            "occurred_at": datetime.now(UTC).isoformat(),
            "correlation_id": str(uuid4()),
        }
        mock_message.headers = {"event_type": "SerializationOrderCreated"}
        mock_message.body = json.dumps(event_data).encode("utf-8")

        event = bus._deserialize_event(mock_message)

        assert event is not None
        assert isinstance(event, SerializationOrderCreatedEvent)
        assert event.order_number == "ORD-001"
        assert event.customer_id == "customer-123"
