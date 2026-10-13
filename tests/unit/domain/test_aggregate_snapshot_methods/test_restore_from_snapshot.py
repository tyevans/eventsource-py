"""Tests for _restore_from_snapshot method."""

from uuid import uuid4

import pytest
from pydantic import ValidationError

from tests.unit.domain.test_aggregate_snapshot_methods._fixtures import (
    ItemAdded,
    OrderAggregate,
    OrderItem,
)


class TestRestoreFromSnapshot:
    """Test _restore_from_snapshot method for aggregate hydration."""

    def test_restore_sets_state_and_version(self) -> None:
        """_restore_from_snapshot should set state and version."""
        aggregate = OrderAggregate(uuid4())

        state_dict = {
            "order_id": str(aggregate.aggregate_id),
            "customer_id": "cust-123",
            "status": "shipped",
            "items": [{"product_id": "p1", "quantity": 1, "price": 10.0}],
            "total": 10.0,
            "created_at": None,
            "metadata": {},
        }

        aggregate._restore_from_snapshot(state_dict, version=50)

        assert aggregate.version == 50
        assert aggregate.state is not None
        assert aggregate.state.customer_id == "cust-123"
        assert aggregate.state.status == "shipped"
        assert len(aggregate.state.items) == 1
        assert aggregate.state.items[0].product_id == "p1"

    def test_restore_with_empty_dict(self) -> None:
        """_restore_from_snapshot with empty dict should set version only."""
        aggregate = OrderAggregate(uuid4())

        aggregate._restore_from_snapshot({}, version=10)

        assert aggregate.version == 10
        assert aggregate.state is None  # State remains None

    def test_restore_validates_state(self) -> None:
        """_restore_from_snapshot should raise ValidationError for invalid state."""
        aggregate = OrderAggregate(uuid4())

        invalid_state = {
            "order_id": str(aggregate.aggregate_id),
            # Missing required field: customer_id
            "status": "active",
        }

        with pytest.raises(ValidationError):
            aggregate._restore_from_snapshot(invalid_state, version=1)

    def test_restore_then_apply_events(self) -> None:
        """After restore, aggregate should correctly apply subsequent events."""
        aggregate = OrderAggregate(uuid4())

        state_dict = {
            "order_id": str(aggregate.aggregate_id),
            "customer_id": "cust-123",
            "status": "created",
            "items": [],
            "total": 0.0,
            "created_at": None,
            "metadata": {},
        }
        aggregate._restore_from_snapshot(state_dict, version=5)

        # Apply event after snapshot
        event = ItemAdded(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Order",
            aggregate_version=6,
            product_id="prod-789",
            quantity=3,
            price=15.0,
        )
        aggregate.apply_event(event, is_new=False)

        assert aggregate.version == 6
        assert len(aggregate.state.items) == 1
        assert aggregate.state.total == 45.0

    def test_restore_preserves_nested_model_types(self) -> None:
        """_restore_from_snapshot should properly restore nested model types."""
        aggregate = OrderAggregate(uuid4())

        state_dict = {
            "order_id": str(aggregate.aggregate_id),
            "customer_id": "cust-123",
            "status": "created",
            "items": [
                {"product_id": "p1", "quantity": 2, "price": 10.0},
                {"product_id": "p2", "quantity": 1, "price": 25.0},
            ],
            "total": 45.0,
            "created_at": None,
            "metadata": {"priority": "high"},
        }
        aggregate._restore_from_snapshot(state_dict, version=10)

        # Items should be OrderItem instances
        assert isinstance(aggregate.state.items[0], OrderItem)
        assert isinstance(aggregate.state.items[1], OrderItem)
        assert aggregate.state.metadata["priority"] == "high"
