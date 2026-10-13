"""Tests for event deserialization using the registry."""

from uuid import uuid4

import pytest

from eventsource.domain.event_registry import (
    EventRegistry,
    EventTypeNotFoundError,
)
from tests.unit.domain.test_event_registry._fixtures import OrderCreated


class TestEventDeserialization:
    """Tests demonstrating event deserialization use case."""

    def test_deserialize_event_from_storage(self) -> None:
        """Can deserialize events from storage using registry."""
        registry = EventRegistry()
        registry.register(OrderCreated)

        # Simulate data from storage/database
        stored_data = {
            "event_id": str(uuid4()),
            "event_type": "OrderCreated",
            "event_version": 1,
            "aggregate_id": str(uuid4()),
            "aggregate_type": "Order",
            "aggregate_version": 1,
            "tenant_id": None,
            "actor_id": "user:123",
            "correlation_id": str(uuid4()),
            "causation_id": None,
            "occurred_at": "2024-01-15T12:00:00Z",
            "metadata": {},
            "order_number": "ORD-001",
            "customer_id": str(uuid4()),
        }

        # Use registry to get class and deserialize
        event_class = registry.get(stored_data["event_type"])
        event = event_class.model_validate(stored_data)

        assert isinstance(event, OrderCreated)
        assert event.order_number == "ORD-001"
        assert event.event_type == "OrderCreated"

    def test_deserialize_unknown_event_type_fails(self) -> None:
        """Deserializing unknown event type fails with helpful error."""
        registry = EventRegistry()
        registry.register(OrderCreated)

        stored_data = {
            "event_type": "UnknownEvent",
            # ... other fields
        }

        with pytest.raises(EventTypeNotFoundError) as exc_info:
            registry.get(stored_data["event_type"])

        assert "UnknownEvent" in str(exc_info.value)
        assert "OrderCreated" in str(exc_info.value)  # Shows available
