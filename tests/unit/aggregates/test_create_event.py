"""
Comprehensive unit tests for AggregateRoot.create_event() method.

Tests cover:
- Auto-population of aggregate_id, aggregate_type, aggregate_version
- Explicit override behavior for auto-populated fields
- Event application and uncommitted event tracking
- Version incrementing across multiple events
- Backward compatibility with existing apply_event() usage
- Tenant context integration (graceful handling when unavailable)

DX-007: Auto Version Management (create_event)
"""

from unittest.mock import patch
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from eventsource.aggregates.base import AggregateRoot, DeclarativeAggregate
from eventsource.events.base import DomainEvent
from eventsource.handlers import handles

# =============================================================================
# Test fixtures: State models and Events
# =============================================================================


class OrderState(BaseModel):
    """State model for Order aggregate."""

    order_id: UUID
    status: str = "pending"
    customer_id: UUID | None = None
    tracking_number: str | None = None


class OrderCreated(DomainEvent):
    """Event for order creation."""

    aggregate_type: str = "Order"
    event_type: str = "OrderCreated"
    customer_id: UUID


class OrderShipped(DomainEvent):
    """Event for shipping order."""

    aggregate_type: str = "Order"
    event_type: str = "OrderShipped"
    tracking_number: str


class OrderCancelled(DomainEvent):
    """Event for cancelling order."""

    aggregate_type: str = "Order"
    event_type: str = "OrderCancelled"
    reason: str


# =============================================================================
# Test Aggregate Implementations
# =============================================================================


class OrderAggregate(AggregateRoot[OrderState]):
    """Order aggregate for testing create_event()."""

    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            self._state = OrderState(
                order_id=event.aggregate_id,
                customer_id=event.customer_id,
                status="created",
            )
        elif isinstance(event, OrderShipped):
            if self._state:
                self._state = self._state.model_copy(
                    update={"status": "shipped", "tracking_number": event.tracking_number}
                )
        elif isinstance(event, OrderCancelled) and self._state:
            self._state = self._state.model_copy(update={"status": "cancelled"})

    # Command methods using create_event
    def create(self, customer_id: UUID) -> OrderCreated:
        """Create order using create_event convenience method."""
        return self.create_event(OrderCreated, customer_id=customer_id)

    def ship(self, tracking_number: str) -> OrderShipped:
        """Ship order using create_event convenience method."""
        return self.create_event(OrderShipped, tracking_number=tracking_number)

    def cancel(self, reason: str) -> OrderCancelled:
        """Cancel order using create_event convenience method."""
        return self.create_event(OrderCancelled, reason=reason)


class DeclarativeOrderAggregate(DeclarativeAggregate[OrderState]):
    """Declarative order aggregate for testing create_event() with @handles decorator."""

    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    @handles(OrderCreated)
    def _on_order_created(self, event: OrderCreated) -> None:
        self._state = OrderState(
            order_id=event.aggregate_id,
            customer_id=event.customer_id,
            status="created",
        )

    @handles(OrderShipped)
    def _on_order_shipped(self, event: OrderShipped) -> None:
        if self._state:
            self._state = self._state.model_copy(
                update={"status": "shipped", "tracking_number": event.tracking_number}
            )

    def create(self, customer_id: UUID) -> OrderCreated:
        return self.create_event(OrderCreated, customer_id=customer_id)

    def ship(self, tracking_number: str) -> OrderShipped:
        return self.create_event(OrderShipped, tracking_number=tracking_number)


# =============================================================================
# Tests for create_event() auto-population
# =============================================================================


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


# =============================================================================
# Tests for explicit override behavior
# =============================================================================


class TestCreateEventOverrides:
    """Tests for explicit override of auto-populated fields."""

    @pytest.fixture
    def order(self) -> OrderAggregate:
        return OrderAggregate(uuid4())

    def test_explicit_override_aggregate_id(self, order: OrderAggregate) -> None:
        """Explicit aggregate_id overrides auto-population."""
        custom_id = uuid4()
        event = order.create_event(
            OrderCreated,
            aggregate_id=custom_id,
            customer_id=uuid4(),
        )
        assert event.aggregate_id == custom_id
        # Note: the aggregate still tracks the event
        assert event in order.uncommitted_events

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


# =============================================================================
# Tests for tenant context integration
# =============================================================================


class TestCreateEventTenantContext:
    """Tests for tenant_id auto-population from context."""

    @pytest.fixture
    def order(self) -> OrderAggregate:
        return OrderAggregate(uuid4())

    def test_no_tenant_when_module_not_available(self, order: OrderAggregate) -> None:
        """tenant_id is None when multitenancy module is not available."""
        event = order.create(customer_id=uuid4())
        assert event.tenant_id is None

    def test_tenant_from_context_when_available(self, order: OrderAggregate) -> None:
        """tenant_id is populated from context when multitenancy is available."""
        tenant_id = uuid4()

        # Mock the multitenancy module import and get_current_tenant function
        with (
            patch.dict(
                "sys.modules",
                {
                    "eventsource.multitenancy": type(
                        "MockModule", (), {"get_current_tenant": staticmethod(lambda: tenant_id)}
                    )()
                },
            ),
            patch(
                "eventsource.aggregates.base.AggregateRoot._get_tenant_from_context",
                return_value=tenant_id,
            ),
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

    def test_handles_import_error_gracefully(self, order: OrderAggregate) -> None:
        """_get_tenant_from_context handles ImportError gracefully."""
        # This should not raise - returns None when import fails
        tenant = order._get_tenant_from_context()
        assert tenant is None


# =============================================================================
# Tests for backward compatibility
# =============================================================================


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


# =============================================================================
# Tests with DeclarativeAggregate
# =============================================================================


class TestCreateEventWithDeclarativeAggregate:
    """Tests for create_event() with DeclarativeAggregate."""

    @pytest.fixture
    def order(self) -> DeclarativeOrderAggregate:
        return DeclarativeOrderAggregate(uuid4())

    def test_create_event_works_with_declarative(self, order: DeclarativeOrderAggregate) -> None:
        """create_event works with DeclarativeAggregate."""
        customer_id = uuid4()
        event = order.create(customer_id=customer_id)

        assert event.aggregate_id == order.aggregate_id
        assert event.aggregate_type == "Order"
        assert event.aggregate_version == 1
        assert event.customer_id == customer_id
        assert order.state is not None
        assert order.state.status == "created"

    def test_version_increments_correctly(self, order: DeclarativeOrderAggregate) -> None:
        """Version increments correctly with DeclarativeAggregate."""
        order.create(customer_id=uuid4())
        assert order.version == 1

        order.ship(tracking_number="TRACK-DECL")
        assert order.version == 2


# =============================================================================
# Edge cases and error conditions
# =============================================================================


class TestCreateEventEdgeCases:
    """Tests for edge cases and potential error conditions."""

    @pytest.fixture
    def order(self) -> OrderAggregate:
        return OrderAggregate(uuid4())

    def test_event_id_is_unique(self, order: OrderAggregate) -> None:
        """Each created event has a unique event_id."""
        event1 = order.create(customer_id=uuid4())
        event2 = order.ship(tracking_number="TRACK1")
        event3 = order.cancel(reason="Test")

        event_ids = {event1.event_id, event2.event_id, event3.event_id}
        assert len(event_ids) == 3  # All unique

    def test_correlation_id_is_set(self, order: OrderAggregate) -> None:
        """Created events have correlation_id set."""
        event = order.create(customer_id=uuid4())
        assert event.correlation_id is not None

    def test_occurred_at_is_set(self, order: OrderAggregate) -> None:
        """Created events have occurred_at timestamp."""
        event = order.create(customer_id=uuid4())
        assert event.occurred_at is not None

    def test_empty_kwargs(self) -> None:
        """Event with only auto-populated fields works (when event has defaults)."""

        class SimpleEvent(DomainEvent):
            aggregate_type: str = "Simple"
            event_type: str = "SimpleEvent"

        class SimpleState(BaseModel):
            id: UUID

        class SimpleAggregate(AggregateRoot[SimpleState]):
            aggregate_type = "Simple"

            def _get_initial_state(self) -> SimpleState:
                return SimpleState(id=self.aggregate_id)

            def _apply(self, event: DomainEvent) -> None:
                pass

        agg = SimpleAggregate(uuid4())
        event = agg.create_event(SimpleEvent)

        assert event.aggregate_id == agg.aggregate_id
        assert event.aggregate_type == "Simple"
        assert event.aggregate_version == 1

    def test_create_event_with_metadata(self, order: OrderAggregate) -> None:
        """create_event can include metadata."""
        event = order.create_event(
            OrderCreated,
            customer_id=uuid4(),
            metadata={"source": "api", "request_id": "req-123"},
        )
        assert event.metadata == {"source": "api", "request_id": "req-123"}

    def test_create_event_with_actor_id(self, order: OrderAggregate) -> None:
        """create_event can include actor_id."""
        event = order.create_event(
            OrderCreated,
            customer_id=uuid4(),
            actor_id="user-456",
        )
        assert event.actor_id == "user-456"

    def test_create_event_with_causation_id(self, order: OrderAggregate) -> None:
        """create_event can include causation_id."""
        causation = uuid4()
        event = order.create_event(
            OrderCreated,
            customer_id=uuid4(),
            causation_id=causation,
        )
        assert event.causation_id == causation

    def test_multiple_aggregates_independent(self) -> None:
        """Events created on different aggregates are independent."""
        order1 = OrderAggregate(uuid4())
        order2 = OrderAggregate(uuid4())

        event1 = order1.create(customer_id=uuid4())
        event2 = order2.create(customer_id=uuid4())

        assert event1.aggregate_id != event2.aggregate_id
        assert event1.aggregate_version == 1
        assert event2.aggregate_version == 1
        assert event1 not in order2.uncommitted_events
        assert event2 not in order1.uncommitted_events


# =============================================================================
# Performance/stress tests (lightweight)
# =============================================================================


class TestCreateEventPerformance:
    """Lightweight performance tests."""

    def test_many_events(self) -> None:
        """Can create many events in sequence."""

        class IncrementEvent(DomainEvent):
            aggregate_type: str = "Counter"
            event_type: str = "Incremented"

        class CounterState(BaseModel):
            value: int = 0

        class Counter(AggregateRoot[CounterState]):
            aggregate_type = "Counter"

            def _get_initial_state(self) -> CounterState:
                return CounterState()

            def _apply(self, event: DomainEvent) -> None:
                if self._state is None:
                    self._state = CounterState()
                self._state = self._state.model_copy(update={"value": self._state.value + 1})

            def increment(self) -> IncrementEvent:
                return self.create_event(IncrementEvent)

        counter = Counter(uuid4())

        for i in range(100):
            event = counter.increment()
            assert event.aggregate_version == i + 1

        assert counter.version == 100
        assert len(counter.uncommitted_events) == 100
        assert counter.state is not None
        assert counter.state.value == 100
