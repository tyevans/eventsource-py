"""Tests for complete snapshot round-trip serialization."""

import json
from uuid import uuid4

from tests.unit.domain.test_aggregate_snapshot_methods._fixtures import (
    ItemAdded,
    OrderAggregate,
    OrderCreated,
    OrderItem,
)


class TestRoundTrip:
    """Test complete serialize -> restore cycle."""

    def test_full_round_trip(self) -> None:
        """Serialize and restore should produce equivalent state."""
        # Create and populate original
        original = OrderAggregate(uuid4())
        event1 = OrderCreated(
            aggregate_id=original.aggregate_id,
            aggregate_type="Order",
            aggregate_version=1,
            customer_id="cust-123",
        )
        original.apply_event(event1)

        event2 = ItemAdded(
            aggregate_id=original.aggregate_id,
            aggregate_type="Order",
            aggregate_version=2,
            product_id="prod-456",
            quantity=2,
            price=25.0,
        )
        original.apply_event(event2)

        # Serialize
        state_dict = original._serialize_state()
        version = original.version

        # Restore into new aggregate
        restored = OrderAggregate(original.aggregate_id)
        restored._restore_from_snapshot(state_dict, version)

        # Verify equivalence
        assert restored.version == original.version
        assert restored.state.customer_id == original.state.customer_id
        assert restored.state.status == original.state.status
        assert len(restored.state.items) == len(original.state.items)
        assert restored.state.total == original.state.total

    def test_round_trip_with_json(self) -> None:
        """Round trip through actual JSON serialization."""
        original = OrderAggregate(uuid4())
        event1 = OrderCreated(
            aggregate_id=original.aggregate_id,
            aggregate_type="Order",
            aggregate_version=1,
            customer_id="cust-123",
        )
        original.apply_event(event1)

        event2 = ItemAdded(
            aggregate_id=original.aggregate_id,
            aggregate_type="Order",
            aggregate_version=2,
            product_id="prod-456",
            quantity=2,
            price=25.0,
        )
        original.apply_event(event2)

        # Serialize to JSON and back
        state_dict = original._serialize_state()
        json_str = json.dumps(state_dict)
        restored_dict = json.loads(json_str)

        # Restore
        restored = OrderAggregate(original.aggregate_id)
        restored._restore_from_snapshot(restored_dict, original.version)

        assert restored.state.customer_id == original.state.customer_id
        assert restored.state.total == original.state.total
        assert len(restored.state.items) == len(original.state.items)

    def test_round_trip_preserves_nested_models(self) -> None:
        """Nested models should be fully preserved through round-trip."""
        original = OrderAggregate(uuid4())

        # Create order with multiple items
        event1 = OrderCreated(
            aggregate_id=original.aggregate_id,
            aggregate_type="Order",
            aggregate_version=1,
            customer_id="cust-123",
        )
        original.apply_event(event1)

        for i in range(3):
            event = ItemAdded(
                aggregate_id=original.aggregate_id,
                aggregate_type="Order",
                aggregate_version=i + 2,
                product_id=f"prod-{i}",
                quantity=i + 1,
                price=10.0 * (i + 1),
            )
            original.apply_event(event)

        # Round-trip
        state_dict = original._serialize_state()
        json_str = json.dumps(state_dict)
        restored_dict = json.loads(json_str)

        restored = OrderAggregate(original.aggregate_id)
        restored._restore_from_snapshot(restored_dict, original.version)

        # Verify items
        assert len(restored.state.items) == 3
        for i, item in enumerate(restored.state.items):
            assert isinstance(item, OrderItem)
            assert item.product_id == f"prod-{i}"
            assert item.quantity == i + 1

    def test_round_trip_handles_datetime(self) -> None:
        """Datetime fields should survive round-trip through JSON."""
        original = OrderAggregate(uuid4())
        event = OrderCreated(
            aggregate_id=original.aggregate_id,
            aggregate_type="Order",
            aggregate_version=1,
            customer_id="cust-123",
        )
        original.apply_event(event)

        # Capture original created_at
        original_created_at = original.state.created_at

        # Round-trip
        state_dict = original._serialize_state()
        json_str = json.dumps(state_dict)
        restored_dict = json.loads(json_str)

        restored = OrderAggregate(original.aggregate_id)
        restored._restore_from_snapshot(restored_dict, original.version)

        # Datetime should be restored (as datetime object)
        assert restored.state.created_at is not None
        # Compare as strings since restoration creates a new datetime
        if original_created_at:
            assert str(restored.state.created_at) == str(original_created_at)
