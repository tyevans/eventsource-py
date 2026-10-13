"""Tests for edge cases and boundary conditions in snapshot methods."""

from uuid import uuid4

from tests.unit.domain.test_aggregate_snapshot_methods._fixtures import (
    DeclarativeOrderAggregate,
    OrderAggregate,
    OrderCreated,
    SimpleAggregate,
    ValueSet,
)


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_restore_at_version_zero(self) -> None:
        """Restore at version 0 with empty state."""
        aggregate = OrderAggregate(uuid4())
        aggregate._restore_from_snapshot({}, version=0)

        assert aggregate.version == 0
        assert aggregate.state is None

    def test_serialize_after_many_events(self) -> None:
        """Serialize should work after many events."""
        aggregate = SimpleAggregate(uuid4())

        for i in range(100):
            event = ValueSet(
                aggregate_id=aggregate.aggregate_id,
                aggregate_type="Simple",
                aggregate_version=i + 1,
                value=f"value-{i}",
            )
            aggregate.apply_event(event)

        result = aggregate._serialize_state()

        assert result["value"] == "value-99"
        assert result["count"] == 100

    def test_restore_overwrites_existing_state(self) -> None:
        """Restore should overwrite any existing state."""
        aggregate = OrderAggregate(uuid4())

        # First, apply some events
        event = OrderCreated(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Order",
            aggregate_version=1,
            customer_id="original-customer",
        )
        aggregate.apply_event(event)

        assert aggregate.state.customer_id == "original-customer"

        # Now restore from snapshot with different state
        state_dict = {
            "order_id": str(aggregate.aggregate_id),
            "customer_id": "snapshot-customer",
            "status": "shipped",
            "items": [],
            "total": 0.0,
            "created_at": None,
            "metadata": {},
        }
        aggregate._restore_from_snapshot(state_dict, version=10)

        # State should be from snapshot
        assert aggregate.state.customer_id == "snapshot-customer"
        assert aggregate.state.status == "shipped"
        assert aggregate.version == 10

    def test_declarative_aggregate_snapshot_methods(self) -> None:
        """Snapshot methods should work with DeclarativeAggregate."""
        aggregate = DeclarativeOrderAggregate(uuid4())

        event = OrderCreated(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Order",
            aggregate_version=1,
            customer_id="decl-customer",
        )
        aggregate.apply_event(event)

        # Serialize
        state_dict = aggregate._serialize_state()
        assert state_dict["customer_id"] == "decl-customer"

        # Restore
        new_aggregate = DeclarativeOrderAggregate(aggregate.aggregate_id)
        new_aggregate._restore_from_snapshot(state_dict, 1)
        assert new_aggregate.state.customer_id == "decl-customer"
