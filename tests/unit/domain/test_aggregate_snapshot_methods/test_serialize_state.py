"""Tests for _serialize_state method."""

import json
from uuid import uuid4

import pytest

from tests.unit.domain.test_aggregate_snapshot_methods._fixtures import (
    ItemAdded,
    OrderAggregate,
    OrderCreated,
)


class TestSerializeState:
    """Test _serialize_state method for snapshot creation."""

    def test_serialize_returns_dict(self) -> None:
        """_serialize_state should return a dictionary."""
        aggregate = OrderAggregate(uuid4())
        event = OrderCreated(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Order",
            aggregate_version=1,
            customer_id="cust-123",
        )
        aggregate.apply_event(event)

        result = aggregate._serialize_state()

        assert isinstance(result, dict)
        assert result["customer_id"] == "cust-123"
        assert result["status"] == "created"

    def test_serialize_handles_nested_models(self) -> None:
        """_serialize_state should properly serialize nested Pydantic models."""
        aggregate = OrderAggregate(uuid4())

        # Create order
        event1 = OrderCreated(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Order",
            aggregate_version=1,
            customer_id="cust-123",
        )
        aggregate.apply_event(event1)

        # Add items
        event2 = ItemAdded(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Order",
            aggregate_version=2,
            product_id="prod-456",
            quantity=2,
            price=19.99,
        )
        aggregate.apply_event(event2)

        event3 = ItemAdded(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Order",
            aggregate_version=3,
            product_id="prod-789",
            quantity=1,
            price=29.99,
        )
        aggregate.apply_event(event3)

        result = aggregate._serialize_state()

        # Check nested models are serialized to dicts
        assert len(result["items"]) == 2
        assert result["items"][0]["product_id"] == "prod-456"
        assert result["items"][0]["quantity"] == 2
        assert result["items"][1]["product_id"] == "prod-789"
        assert result["total"] == pytest.approx(69.97)

    def test_serialize_returns_empty_dict_when_state_is_none(self) -> None:
        """_serialize_state should return {} when state is None."""
        aggregate = OrderAggregate(uuid4())
        # No events applied, state is None

        result = aggregate._serialize_state()

        assert result == {}

    def test_serialize_is_json_compatible(self) -> None:
        """Serialized state should be JSON-serializable."""
        aggregate = OrderAggregate(uuid4())
        event = OrderCreated(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Order",
            aggregate_version=1,
            customer_id="cust-123",
        )
        aggregate.apply_event(event)

        result = aggregate._serialize_state()

        # Should not raise
        json_str = json.dumps(result)
        assert json_str is not None
        assert "cust-123" in json_str

    def test_serialize_handles_datetime(self) -> None:
        """_serialize_state should serialize datetime fields to ISO format."""
        aggregate = OrderAggregate(uuid4())
        event = OrderCreated(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Order",
            aggregate_version=1,
            customer_id="cust-123",
        )
        aggregate.apply_event(event)

        result = aggregate._serialize_state()

        # created_at should be serialized as ISO string
        assert result["created_at"] is not None
        assert isinstance(result["created_at"], str)

        # Should be valid JSON
        json_str = json.dumps(result)
        assert json_str is not None

    def test_serialize_handles_empty_collections(self) -> None:
        """_serialize_state should handle empty lists and dicts."""
        aggregate = OrderAggregate(uuid4())
        event = OrderCreated(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Order",
            aggregate_version=1,
            customer_id="cust-123",
        )
        aggregate.apply_event(event)

        result = aggregate._serialize_state()

        assert result["items"] == []
        assert result["metadata"] == {}
