"""Tests for replaying events after restoring from snapshot."""

from uuid import uuid4

from tests.unit.domain.test_aggregate_snapshot_methods._fixtures import (
    ItemAdded,
    OrderAggregate,
    StatusChanged,
)


class TestRestoreThenReplay:
    """Test replaying events after restoring from snapshot."""

    def test_replay_events_after_restore(self) -> None:
        """Events after snapshot should correctly update state."""
        aggregate = OrderAggregate(uuid4())

        # Restore from snapshot at version 2
        state_dict = {
            "order_id": str(aggregate.aggregate_id),
            "customer_id": "cust-123",
            "status": "created",
            "items": [{"product_id": "p1", "quantity": 1, "price": 10.0}],
            "total": 10.0,
            "created_at": None,
            "metadata": {},
        }
        aggregate._restore_from_snapshot(state_dict, version=2)

        # Simulate loading subsequent events
        events = [
            ItemAdded(
                aggregate_id=aggregate.aggregate_id,
                aggregate_type="Order",
                aggregate_version=3,
                product_id="p2",
                quantity=2,
                price=20.0,
            ),
            StatusChanged(
                aggregate_id=aggregate.aggregate_id,
                aggregate_type="Order",
                aggregate_version=4,
                new_status="shipped",
            ),
        ]

        aggregate.load_from_history(events)

        assert aggregate.version == 4
        assert aggregate.state.status == "shipped"
        assert len(aggregate.state.items) == 2
        assert aggregate.state.total == 50.0

    def test_no_uncommitted_events_after_restore_and_replay(self) -> None:
        """Restore + replay should not create uncommitted events."""
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
        aggregate._restore_from_snapshot(state_dict, version=1)

        events = [
            ItemAdded(
                aggregate_id=aggregate.aggregate_id,
                aggregate_type="Order",
                aggregate_version=2,
                product_id="p1",
                quantity=1,
                price=10.0,
            ),
        ]
        aggregate.load_from_history(events)

        assert not aggregate.has_uncommitted_events
