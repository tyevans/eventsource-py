"""Tests for AggregateRoot.create_event() method with Order aggregate."""

from unittest.mock import patch
from uuid import uuid4

import pytest

from eventsource.domain.exceptions import AggregateIdMismatchError
from tests.unit.domain.test_create_event._fixtures import (
    DeclarativeOrderAggregate,
    OrderAggregate,
    OrderCancelled,
    OrderCreated,
    OrderShipped,
    OrderState,
)

__all__ = [
    "DeclarativeOrderAggregate",
    "OrderAggregate",
    "OrderCancelled",
    "OrderCreated",
    "OrderShipped",
    "OrderState",
    "TestBackwardCompatibility",
    "TestCreateEventApplication",
    "TestCreateEventAutoPopulation",
    "TestCreateEventOverrides",
    "TestCreateEventReturnValue",
    "TestCreateEventTenantContext",
]


class TestCreateEventAutoPopulation:
    """Tests for auto-population of aggregate fields."""

    @pytest.fixture
    def order(self) -> OrderAggregate:
        return OrderAggregate(uuid4())

    def test_auto_populates_aggregate_id(self, order: OrderAggregate) -> None:
        """create_event sets aggregate_id from aggregate."""
        event = order.create(customer_id=uuid4())
        assert event.aggregate_id == order.aggregate_id

    def test_auto_populates_aggregate_type(self, order: OrderAggregate) -> None:
        """create_event sets aggregate_type from aggregate."""
        event = order.create(customer_id=uuid4())
        assert event.aggregate_type == "Order"

    def test_auto_populates_version_first_event(self, order: OrderAggregate) -> None:
        """create_event sets aggregate_version to 1 for first event."""
        event = order.create(customer_id=uuid4())
        assert event.aggregate_version == 1

    def test_auto_populates_version_subsequent_events(self, order: OrderAggregate) -> None:
        """create_event increments aggregate_version for subsequent events."""
        event1 = order.create(customer_id=uuid4())
        assert event1.aggregate_version == 1

        event2 = order.ship(tracking_number="TRACK123")
        assert event2.aggregate_version == 2

        event3 = order.cancel(reason="Customer request")
        assert event3.aggregate_version == 3


class TestCreateEventReturnValue:
    """Tests for create_event return value."""

    @pytest.fixture
    def order(self) -> OrderAggregate:
        return OrderAggregate(uuid4())

    def test_returns_correct_event_type(self, order: OrderAggregate) -> None:
        """create_event returns the correct event type."""
        event = order.create(customer_id=uuid4())
        assert isinstance(event, OrderCreated)

    def test_returns_event_with_payload(self, order: OrderAggregate) -> None:
        """create_event returns event with correct payload fields."""
        customer_id = uuid4()
        event = order.create(customer_id=customer_id)
        assert event.customer_id == customer_id

    def test_returns_event_with_tracking_number(self, order: OrderAggregate) -> None:
        """create_event returns event with correct tracking number."""
        order.create(customer_id=uuid4())
        event = order.ship(tracking_number="TRACK456")
        assert event.tracking_number == "TRACK456"


class TestCreateEventApplication:
    """Tests for event application behavior."""

    @pytest.fixture
    def order(self) -> OrderAggregate:
        return OrderAggregate(uuid4())

    def test_applies_event_to_state(self, order: OrderAggregate) -> None:
        """create_event applies event to aggregate state."""
        customer_id = uuid4()
        order.create(customer_id=customer_id)
        assert order.state is not None
        assert order.state.status == "created"
        assert order.state.customer_id == customer_id

    def test_updates_aggregate_version(self, order: OrderAggregate) -> None:
        """create_event updates aggregate version."""
        assert order.version == 0
        order.create(customer_id=uuid4())
        assert order.version == 1
        order.ship(tracking_number="TRACK123")
        assert order.version == 2

    def test_adds_to_uncommitted_events(self, order: OrderAggregate) -> None:
        """create_event adds event to uncommitted_events."""
        event = order.create(customer_id=uuid4())
        assert event in order.uncommitted_events

    def test_uncommitted_events_accumulate(self, order: OrderAggregate) -> None:
        """Multiple create_event calls accumulate in uncommitted_events."""
        event1 = order.create(customer_id=uuid4())
        event2 = order.ship(tracking_number="TRACK123")
        assert len(order.uncommitted_events) == 2
        assert event1 in order.uncommitted_events
        assert event2 in order.uncommitted_events


class TestCreateEventOverrides:
    """Tests for explicit override of auto-populated fields."""

    @pytest.fixture
    def order(self) -> OrderAggregate:
        return OrderAggregate(uuid4())

    def test_explicit_foreign_aggregate_id_is_rejected(self, order: OrderAggregate) -> None:
        """aggregate_id is the one auto-populated field that cannot be overridden.

        It used to override, and the aggregate went on tracking the event --
        so the event was appended to a stream neither aggregate reads back.
        See `test_aggregate_id_targeting.py`.
        """
        with pytest.raises(AggregateIdMismatchError):
            order.create_event(
                OrderCreated,
                aggregate_id=uuid4(),
                customer_id=uuid4(),
            )

        assert order.uncommitted_events == []

    def test_explicit_override_aggregate_type(self, order: OrderAggregate) -> None:
        """Explicit aggregate_type overrides auto-population."""
        event = order.create_event(
            OrderCreated,
            aggregate_type="CustomOrder",
            customer_id=uuid4(),
        )
        assert event.aggregate_type == "CustomOrder"

    def test_explicit_override_version(self, order: OrderAggregate) -> None:
        """Explicit aggregate_version overrides auto-population."""
        # Note: This will still pass because validate_versions is True by default
        # But the event will be created with the specified version
        # To test this properly, we need to disable version validation
        order.validate_versions = False
        event = order.create_event(
            OrderCreated,
            aggregate_version=99,
            customer_id=uuid4(),
        )
        assert event.aggregate_version == 99

    def test_explicit_override_tenant_id(self, order: OrderAggregate) -> None:
        """Explicit tenant_id overrides any auto-population from context."""
        custom_tenant = uuid4()
        event = order.create_event(
            OrderCreated,
            tenant_id=custom_tenant,
            customer_id=uuid4(),
        )
        assert event.tenant_id == custom_tenant


class TestCreateEventTenantContext:
    """Tests for tenant_id auto-population from context."""

    @pytest.fixture
    def order(self) -> OrderAggregate:
        return OrderAggregate(uuid4())

    def test_no_tenant_when_context_not_set(self, order: OrderAggregate) -> None:
        """tenant_id is None when no tenant context is set."""
        event = order.create(customer_id=uuid4())
        assert event.tenant_id is None

    def test_tenant_from_context_when_available(self, order: OrderAggregate) -> None:
        """tenant_id is populated from the current tenant context."""
        tenant_id = uuid4()

        with patch.object(
            OrderAggregate,
            "_get_tenant_from_context",
            return_value=tenant_id,
        ):
            event = order.create(customer_id=uuid4())
            assert event.tenant_id == tenant_id

    def test_explicit_tenant_overrides_context(self, order: OrderAggregate) -> None:
        """Explicit tenant_id kwarg overrides context value."""
        context_tenant = uuid4()
        explicit_tenant = uuid4()

        with patch.object(
            OrderAggregate,
            "_get_tenant_from_context",
            return_value=context_tenant,
        ):
            event = order.create_event(
                OrderCreated,
                tenant_id=explicit_tenant,
                customer_id=uuid4(),
            )
            # Explicit value should win
            assert event.tenant_id == explicit_tenant

    def test_get_tenant_from_context_reads_current_tenant(self, order: OrderAggregate) -> None:
        """_get_tenant_from_context delegates to get_current_tenant()."""
        assert order._get_tenant_from_context() is None


class TestBackwardCompatibility:
    """Tests to ensure backward compatibility with existing patterns."""

    def test_apply_event_still_works(self) -> None:
        """Existing apply_event usage continues to work."""
        order = OrderAggregate(uuid4())

        # Manual event creation (old pattern)
        event = OrderCreated(
            aggregate_id=order.aggregate_id,
            aggregate_type="Order",
            aggregate_version=order.get_next_version(),
            customer_id=uuid4(),
        )
        order.apply_event(event, is_new=True)

        assert order.version == 1
        assert event in order.uncommitted_events
        assert order.state is not None
        assert order.state.status == "created"

    def test_raise_event_still_works(self) -> None:
        """Existing _raise_event usage continues to work."""
        order = OrderAggregate(uuid4())

        # Using _raise_event (old convenience pattern)
        event = OrderCreated(
            aggregate_id=order.aggregate_id,
            aggregate_type="Order",
            aggregate_version=order.get_next_version(),
            customer_id=uuid4(),
        )
        order._raise_event(event)

        assert order.version == 1
        assert event in order.uncommitted_events

    def test_can_mix_patterns(self) -> None:
        """Can mix create_event with manual apply_event."""
        order = OrderAggregate(uuid4())

        # Use create_event for first event
        order.create(customer_id=uuid4())
        assert order.version == 1

        # Use manual approach for second event
        event = OrderShipped(
            aggregate_id=order.aggregate_id,
            aggregate_type="Order",
            aggregate_version=order.get_next_version(),
            tracking_number="MANUAL-TRACK",
        )
        order.apply_event(event, is_new=True)
        assert order.version == 2

        # Use create_event again
        order.cancel(reason="Changed mind")
        assert order.version == 3

        assert len(order.uncommitted_events) == 3
