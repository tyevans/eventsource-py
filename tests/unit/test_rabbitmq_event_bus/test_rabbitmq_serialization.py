"""Unit tests extracted for test_rabbitmq_serialization.py."""

from __future__ import annotations

import json
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
)
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry

from ._fixtures import SerializationOrderCreatedEvent, SerializationTestEvent


class TestRabbitMQRoutingKey:
    """Tests for routing key generation in RabbitMQEventBus."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig()

    @pytest.fixture
    def bus(self, config: RabbitMQEventBusConfig) -> RabbitMQEventBus:
        """Create bus instance for testing."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        return RabbitMQEventBus(config=config)

    def test_routing_key_format(self, bus: RabbitMQEventBus) -> None:
        """Test routing key follows {aggregate_type}.{event_type} format."""
        event = SerializationTestEvent(aggregate_id=uuid4())

        routing_key = bus._get_routing_key(event)

        assert routing_key == "TestAggregate.SerializationTestEvent"

    def test_routing_key_with_order_event(self, bus: RabbitMQEventBus) -> None:
        """Test routing key for order event."""
        event = SerializationOrderCreatedEvent(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id="customer-123",
        )

        routing_key = bus._get_routing_key(event)

        assert routing_key == "Order.SerializationOrderCreated"

    def test_routing_key_with_custom_aggregate_type(self, bus: RabbitMQEventBus) -> None:
        """Test routing key with custom aggregate type."""

        class CustomEvent(DomainEvent):
            aggregate_type: str = "CustomAggregate"

        event = CustomEvent(aggregate_id=uuid4())

        routing_key = bus._get_routing_key(event)

        assert routing_key == "CustomAggregate.CustomEvent"


class TestRabbitMQEventSerialization:
    """Tests for event serialization in RabbitMQEventBus."""

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

    def test_serialize_event_returns_tuple(
        self, bus: RabbitMQEventBus, sample_event: SerializationTestEvent
    ) -> None:
        """Test that _serialize_event returns tuple of (bytes, dict)."""
        result = bus._serialize_event(sample_event)

        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], bytes)
        assert isinstance(result[1], dict)

    def test_serialize_event_body_is_valid_json(
        self, bus: RabbitMQEventBus, sample_event: SerializationTestEvent
    ) -> None:
        """Test that serialized body is valid JSON."""
        body, _ = bus._serialize_event(sample_event)

        # Should not raise
        parsed = json.loads(body.decode("utf-8"))
        assert isinstance(parsed, dict)

    def test_serialize_event_body_contains_event_data(
        self, bus: RabbitMQEventBus, sample_event: SerializationTestEvent
    ) -> None:
        """Test that serialized body contains event data."""
        body, _ = bus._serialize_event(sample_event)

        parsed = json.loads(body.decode("utf-8"))
        assert parsed["data"] == "test-data"
        assert parsed["aggregate_type"] == "TestAggregate"
        assert parsed["event_type"] == "SerializationTestEvent"

    def test_serialize_event_headers_contain_event_type(
        self, bus: RabbitMQEventBus, sample_event: SerializationTestEvent
    ) -> None:
        """Test that headers contain event_type."""
        _, headers = bus._serialize_event(sample_event)

        assert headers["event_type"] == "SerializationTestEvent"

    def test_serialize_event_headers_contain_aggregate_type(
        self, bus: RabbitMQEventBus, sample_event: SerializationTestEvent
    ) -> None:
        """Test that headers contain aggregate_type."""
        _, headers = bus._serialize_event(sample_event)

        assert headers["aggregate_type"] == "TestAggregate"

    def test_serialize_event_headers_contain_aggregate_id(
        self, bus: RabbitMQEventBus, sample_event: SerializationTestEvent
    ) -> None:
        """Test that headers contain aggregate_id as string."""
        _, headers = bus._serialize_event(sample_event)

        assert headers["aggregate_id"] == str(sample_event.aggregate_id)

    def test_serialize_event_headers_contain_aggregate_version(
        self, bus: RabbitMQEventBus, sample_event: SerializationTestEvent
    ) -> None:
        """Test that headers contain aggregate_version."""
        _, headers = bus._serialize_event(sample_event)

        assert headers["aggregate_version"] == sample_event.aggregate_version

    def test_serialize_event_headers_contain_retry_count(
        self, bus: RabbitMQEventBus, sample_event: SerializationTestEvent
    ) -> None:
        """Test that headers contain x-retry-count initialized to 0."""
        _, headers = bus._serialize_event(sample_event)

        assert headers["x-retry-count"] == 0

    def test_serialize_event_headers_contain_correlation_id(self, bus: RabbitMQEventBus) -> None:
        """Test that headers contain correlation_id when present."""
        correlation_id = uuid4()
        event = SerializationTestEvent(
            aggregate_id=uuid4(),
            correlation_id=correlation_id,
        )

        _, headers = bus._serialize_event(event)

        assert headers["correlation_id"] == str(correlation_id)

    def test_serialize_event_headers_contain_causation_id(self, bus: RabbitMQEventBus) -> None:
        """Test that headers contain causation_id when present."""
        causation_id = uuid4()
        event = SerializationTestEvent(
            aggregate_id=uuid4(),
            causation_id=causation_id,
        )

        _, headers = bus._serialize_event(event)

        assert headers["causation_id"] == str(causation_id)

    def test_serialize_event_headers_contain_tenant_id(self, bus: RabbitMQEventBus) -> None:
        """Test that headers contain tenant_id when present."""
        tenant_id = uuid4()
        event = SerializationTestEvent(
            aggregate_id=uuid4(),
            tenant_id=tenant_id,
        )

        _, headers = bus._serialize_event(event)

        assert headers["tenant_id"] == str(tenant_id)

    def test_serialize_event_headers_omit_optional_when_none(
        self, bus: RabbitMQEventBus, sample_event: SerializationTestEvent
    ) -> None:
        """Test that optional headers are omitted when None."""
        # sample_event has causation_id=None and tenant_id=None
        _, headers = bus._serialize_event(sample_event)

        assert "causation_id" not in headers
        assert "tenant_id" not in headers
        # But correlation_id should always be present (has default_factory)
        assert "correlation_id" in headers


class TestRabbitMQSerializationRoundTrip:
    """Tests for serialization and deserialization round-trip."""

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

    def test_round_trip_preserves_event_data(self, bus: RabbitMQEventBus) -> None:
        """Test that serialize -> deserialize preserves event data."""
        original_event = SerializationTestEvent(
            aggregate_id=uuid4(),
            data="round-trip-test",
        )

        # Serialize
        message = bus._create_message(original_event)

        # Create mock incoming message from serialized data
        mock_message = MagicMock()
        mock_message.message_id = message.message_id
        mock_message.headers = message.headers
        mock_message.body = message.body

        # Deserialize
        deserialized_event = bus._deserialize_event(mock_message)

        assert deserialized_event is not None
        assert deserialized_event.event_id == original_event.event_id
        assert deserialized_event.event_type == original_event.event_type
        assert deserialized_event.aggregate_id == original_event.aggregate_id
        assert deserialized_event.data == original_event.data

    def test_round_trip_preserves_all_fields(self, bus: RabbitMQEventBus) -> None:
        """Test that all fields are preserved in round-trip."""
        tenant_id = uuid4()
        causation_id = uuid4()
        correlation_id = uuid4()

        original_event = SerializationOrderCreatedEvent(
            aggregate_id=uuid4(),
            order_number="ORD-999",
            customer_id="customer-abc",
            tenant_id=tenant_id,
            causation_id=causation_id,
            correlation_id=correlation_id,
            aggregate_version=5,
        )

        # Serialize and deserialize
        message = bus._create_message(original_event)
        mock_message = MagicMock()
        mock_message.message_id = message.message_id
        mock_message.headers = message.headers
        mock_message.body = message.body

        deserialized = bus._deserialize_event(mock_message)

        assert deserialized is not None
        assert deserialized.order_number == "ORD-999"
        assert deserialized.customer_id == "customer-abc"
        assert deserialized.tenant_id == tenant_id
        assert deserialized.causation_id == causation_id
        assert deserialized.correlation_id == correlation_id
        assert deserialized.aggregate_version == 5
