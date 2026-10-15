"""Tests for event factory, event types, and state models in shared test fixtures."""

from decimal import Decimal
from uuid import UUID

from tests.fixtures import (
    CounterDecremented,
    CounterIncremented,
    CounterNamed,
    CounterState,
    OrderCreated,
    OrderItemAdded,
    OrderShipped,
    OrderState,
    SampleEvent,
    create_event,
)

# =============================================================================
# Event Factory Tests
# =============================================================================


class TestEventFactory:
    """Tests for the create_event factory function."""

    def test_creates_event_with_defaults(self) -> None:
        """Factory creates valid event with sensible defaults."""
        event = create_event()

        assert event.aggregate_id is not None
        assert isinstance(event.aggregate_id, UUID)
        assert event.aggregate_type == "TestAggregate"
        assert event.aggregate_version == 1
        assert isinstance(event, SampleEvent)

    def test_accepts_custom_aggregate_id(self) -> None:
        """Factory uses provided aggregate_id."""
        from uuid import uuid4

        custom_id = uuid4()
        event = create_event(aggregate_id=custom_id)

        assert event.aggregate_id == custom_id

    def test_accepts_custom_version(self) -> None:
        """Factory uses provided version."""
        event = create_event(aggregate_version=5)

        assert event.aggregate_version == 5

    def test_creates_counter_event(self) -> None:
        """Factory can create CounterIncremented events."""
        event = create_event(CounterIncremented, increment=10)

        assert isinstance(event, CounterIncremented)
        assert event.increment == 10
        assert event.aggregate_type == "Counter"

    def test_creates_order_event(self) -> None:
        """Factory can create OrderCreated events."""
        from uuid import uuid4

        customer_id = uuid4()
        event = create_event(OrderCreated, customer_id=customer_id)

        assert isinstance(event, OrderCreated)
        assert event.customer_id == customer_id
        assert event.aggregate_type == "Order"

    def test_accepts_tenant_id(self) -> None:
        """Factory can set tenant_id for multi-tenant tests."""
        from uuid import uuid4

        tenant_id = uuid4()
        event = create_event(tenant_id=tenant_id)

        assert event.tenant_id == tenant_id


# =============================================================================
# Event Type Tests
# =============================================================================


class TestCounterEvents:
    """Tests for counter event types."""

    def test_counter_incremented_has_correct_defaults(self) -> None:
        """CounterIncremented has correct default values."""
        from uuid import uuid4

        event = CounterIncremented(aggregate_id=uuid4(), aggregate_version=1)

        assert event.event_type == "CounterIncremented"
        assert event.aggregate_type == "Counter"
        assert event.increment == 1

    def test_counter_decremented_has_correct_defaults(self) -> None:
        """CounterDecremented has correct default values."""
        from uuid import uuid4

        event = CounterDecremented(aggregate_id=uuid4(), aggregate_version=1)

        assert event.event_type == "CounterDecremented"
        assert event.aggregate_type == "Counter"
        assert event.decrement == 1

    def test_counter_named_requires_name(self) -> None:
        """CounterNamed requires a name."""
        from uuid import uuid4

        event = CounterNamed(aggregate_id=uuid4(), aggregate_version=1, name="MyCounter")

        assert event.name == "MyCounter"


class TestOrderEvents:
    """Tests for order event types."""

    def test_order_created_requires_customer_id(self) -> None:
        """OrderCreated requires a customer_id."""
        from uuid import uuid4

        customer_id = uuid4()
        event = OrderCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            customer_id=customer_id,
        )

        assert event.customer_id == customer_id

    def test_order_item_added_requires_item_details(self) -> None:
        """OrderItemAdded requires item_name and price."""
        from uuid import uuid4

        event = OrderItemAdded(
            aggregate_id=uuid4(),
            aggregate_version=1,
            item_name="Widget",
            price=Decimal("10.50"),
        )

        assert event.item_name == "Widget"
        assert event.price == Decimal("10.50")

    def test_order_shipped_requires_tracking_number(self) -> None:
        """OrderShipped requires a tracking_number."""
        from uuid import uuid4

        event = OrderShipped(
            aggregate_id=uuid4(),
            aggregate_version=1,
            tracking_number="TRACK123",
        )

        assert event.tracking_number == "TRACK123"


# =============================================================================
# State Model Tests
# =============================================================================


class TestCounterState:
    """Tests for CounterState model."""

    def test_counter_state_creation(self) -> None:
        """CounterState can be created with required fields."""
        from uuid import uuid4

        state = CounterState(counter_id=uuid4())

        assert state.value == 0
        assert state.name == ""

    def test_counter_state_with_values(self) -> None:
        """CounterState can be created with all fields."""
        from uuid import uuid4

        counter_id = uuid4()
        state = CounterState(counter_id=counter_id, value=42, name="MyCounter")

        assert state.counter_id == counter_id
        assert state.value == 42
        assert state.name == "MyCounter"


class TestOrderState:
    """Tests for OrderState model."""

    def test_order_state_creation(self) -> None:
        """OrderState defaults describe an order that does not exist yet."""
        state = OrderState()

        assert state.customer_id is None
        assert state.status == "draft"
        assert state.total == Decimal("0")
        assert state.items == []

    def test_order_state_with_items(self) -> None:
        """OrderState can have items."""
        from uuid import uuid4

        state = OrderState(
            customer_id=uuid4(),
            status="created",
            total=Decimal("25.00"),
            items=["Widget A", "Widget B"],
        )

        assert len(state.items) == 2
        assert state.total == Decimal("25.00")
