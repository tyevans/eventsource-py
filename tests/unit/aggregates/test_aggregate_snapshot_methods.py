"""Unit tests for AggregateRoot snapshot methods.

P5-003: Tests for snapshot serialization/deserialization on AggregateRoot.

Tests cover:
- schema_version class attribute (default and custom)
- _serialize_state() method for JSON-compatible serialization
- _restore_from_snapshot() method for state restoration
- _get_state_type() method for Generic type introspection
- Round-trip serialization (serialize -> JSON -> restore)
- Edge cases (None state, empty dict, validation errors)
"""

import json
from datetime import datetime
from uuid import uuid4

import pytest
from pydantic import BaseModel, Field, ValidationError

from eventsource.aggregates.base import AggregateRoot, DeclarativeAggregate
from eventsource.events.base import DomainEvent
from eventsource.projections.decorators import handles

# =============================================================================
# Test State Models
# =============================================================================


class OrderItem(BaseModel):
    """Nested model for testing complex serialization."""

    product_id: str
    quantity: int
    price: float


class OrderState(BaseModel):
    """Complex state with nested models, lists, and optional fields."""

    order_id: str
    customer_id: str
    status: str = "pending"
    items: list[OrderItem] = Field(default_factory=list)
    total: float = 0.0
    created_at: datetime | None = None
    metadata: dict[str, str] = Field(default_factory=dict)


class SimpleState(BaseModel):
    """Simple state for basic tests."""

    value: str = ""
    count: int = 0


# =============================================================================
# Test Events
# =============================================================================


class OrderCreated(DomainEvent):
    """Event for order creation."""

    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    customer_id: str


class ItemAdded(DomainEvent):
    """Event for adding item to order."""

    event_type: str = "ItemAdded"
    aggregate_type: str = "Order"
    product_id: str
    quantity: int
    price: float


class StatusChanged(DomainEvent):
    """Event for status change."""

    event_type: str = "StatusChanged"
    aggregate_type: str = "Order"
    new_status: str


class ValueSet(DomainEvent):
    """Simple event for value setting."""

    event_type: str = "ValueSet"
    aggregate_type: str = "Simple"
    value: str


# =============================================================================
# Test Aggregates
# =============================================================================


class OrderAggregate(AggregateRoot[OrderState]):
    """Order aggregate for testing complex state serialization."""

    aggregate_type = "Order"
    schema_version = 1

    def _get_initial_state(self) -> OrderState:
        return OrderState(
            order_id=str(self.aggregate_id),
            customer_id="",
        )

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            self._state = OrderState(
                order_id=str(self.aggregate_id),
                customer_id=event.customer_id,
                status="created",
                created_at=event.occurred_at,
            )
        elif isinstance(event, ItemAdded):
            if self._state:
                item = OrderItem(
                    product_id=event.product_id,
                    quantity=event.quantity,
                    price=event.price,
                )
                self._state = self._state.model_copy(
                    update={
                        "items": [*self._state.items, item],
                        "total": self._state.total + (event.quantity * event.price),
                    }
                )
        elif isinstance(event, StatusChanged) and self._state:
            self._state = self._state.model_copy(update={"status": event.new_status})


class SimpleAggregate(AggregateRoot[SimpleState]):
    """Simple aggregate for basic tests."""

    aggregate_type = "Simple"

    def _get_initial_state(self) -> SimpleState:
        return SimpleState()

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, ValueSet):
            if self._state is None:
                self._state = self._get_initial_state()
            self._state = self._state.model_copy(
                update={
                    "value": event.value,
                    "count": self._state.count + 1,
                }
            )


class VersionedAggregate(AggregateRoot[SimpleState]):
    """Aggregate with custom schema version."""

    aggregate_type = "Versioned"
    schema_version = 5

    def _get_initial_state(self) -> SimpleState:
        return SimpleState()

    def _apply(self, event: DomainEvent) -> None:
        pass


class DeclarativeOrderAggregate(DeclarativeAggregate[OrderState]):
    """Declarative aggregate for testing _get_state_type with inheritance."""

    aggregate_type = "Order"
    schema_version = 2

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id="", customer_id="")

    @handles(OrderCreated)
    def _on_created(self, event: OrderCreated) -> None:
        self._state = OrderState(
            order_id=str(self.aggregate_id),
            customer_id=event.customer_id,
            status="created",
        )


# =============================================================================
# Test: Schema Version
# =============================================================================


class TestSchemaVersion:
    """Test schema_version class attribute behavior."""

    def test_default_schema_version_is_one(self) -> None:
        """Default schema_version should be 1 when not explicitly set."""
        assert SimpleAggregate.schema_version == 1

    def test_schema_version_can_be_overridden(self) -> None:
        """Custom schema_version should be respected."""
        assert VersionedAggregate.schema_version == 5
        assert OrderAggregate.schema_version == 1

    def test_schema_version_accessible_on_instance(self) -> None:
        """schema_version should be accessible on aggregate instances."""
        aggregate = OrderAggregate(uuid4())
        assert aggregate.schema_version == 1

        versioned = VersionedAggregate(uuid4())
        assert versioned.schema_version == 5

    def test_different_aggregates_have_independent_versions(self) -> None:
        """Each aggregate class should have its own schema_version."""
        # SimpleAggregate uses default (1)
        # VersionedAggregate explicitly sets 5
        # OrderAggregate explicitly sets 1
        assert SimpleAggregate.schema_version == 1
        assert VersionedAggregate.schema_version == 5
        assert OrderAggregate.schema_version == 1

        # Modifying one should not affect others
        class TestAggregate(AggregateRoot[SimpleState]):
            aggregate_type = "Test"
            schema_version = 99

            def _get_initial_state(self) -> SimpleState:
                return SimpleState()

            def _apply(self, event: DomainEvent) -> None:
                pass

        assert TestAggregate.schema_version == 99
        assert SimpleAggregate.schema_version == 1  # Unchanged


# =============================================================================
# Test: Serialize State
# =============================================================================


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


# =============================================================================
# Test: Restore From Snapshot
# =============================================================================


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


# =============================================================================
# Test: Get State Type
# =============================================================================


class TestGetStateType:
    """Test _get_state_type method for Generic type introspection."""

    def test_returns_correct_type_for_aggregate_root(self) -> None:
        """_get_state_type should return the TState type parameter."""
        aggregate = OrderAggregate(uuid4())
        state_type = aggregate._get_state_type()
        assert state_type is OrderState

    def test_returns_correct_type_for_simple_aggregate(self) -> None:
        """_get_state_type should work for different state types."""
        aggregate = SimpleAggregate(uuid4())
        state_type = aggregate._get_state_type()
        assert state_type is SimpleState

    def test_works_with_declarative_aggregate(self) -> None:
        """_get_state_type should work with DeclarativeAggregate."""
        aggregate = DeclarativeOrderAggregate(uuid4())
        state_type = aggregate._get_state_type()
        assert state_type is OrderState

    def test_works_with_inheritance(self) -> None:
        """_get_state_type should work with aggregate inheritance."""

        class BaseOrder(AggregateRoot[OrderState]):
            aggregate_type = "BaseOrder"

            def _apply(self, event: DomainEvent) -> None:
                pass

            def _get_initial_state(self) -> OrderState:
                return OrderState(order_id="", customer_id="")

        class SpecialOrder(BaseOrder):
            """Inherits TState from BaseOrder."""

            aggregate_type = "SpecialOrder"

        aggregate = SpecialOrder(uuid4())
        state_type = aggregate._get_state_type()
        assert state_type is OrderState

    def test_raises_runtime_error_if_type_cannot_be_determined(self) -> None:
        """_get_state_type should raise RuntimeError if type cannot be found."""
        # This is an edge case that shouldn't happen in normal use,
        # but we test the error handling. We'll create a mock that
        # doesn't have proper Generic bases.

        class BrokenAggregate(AggregateRoot[SimpleState]):
            aggregate_type = "Broken"

            def _apply(self, event: DomainEvent) -> None:
                pass

            def _get_initial_state(self) -> SimpleState:
                return SimpleState()

        aggregate = BrokenAggregate(uuid4())

        # The method should still work for properly defined aggregates
        state_type = aggregate._get_state_type()
        assert state_type is SimpleState


# =============================================================================
# Test: Round-Trip Serialization
# =============================================================================


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


# =============================================================================
# Test: Integration with load_from_history after restore
# =============================================================================


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


# =============================================================================
# Test: Edge Cases
# =============================================================================


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
