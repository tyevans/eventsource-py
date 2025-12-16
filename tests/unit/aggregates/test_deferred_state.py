"""
Comprehensive unit tests for deferred initial state in DeclarativeAggregate.

Tests cover:
- Aggregates with requires_creation_event=True (deferred state)
- Traditional aggregates with _get_initial_state() (backward compatibility)
- AggregateNotCreatedError exception
- state, state_or_none, is_created properties
- _get_initial_state() behavior

DX-008: Deferred Initial State
"""

from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from eventsource.aggregates.base import DeclarativeAggregate
from eventsource.events.base import DomainEvent
from eventsource.exceptions import AggregateNotCreatedError
from eventsource.handlers import handles

# =============================================================================
# Test fixtures: State models and Events for deferred state aggregate
# =============================================================================


class ExtractionState(BaseModel):
    """State model for Extraction aggregate."""

    page_id: UUID
    status: str


class ExtractionRequested(DomainEvent):
    """Event for extraction request."""

    aggregate_type: str = "Extraction"
    event_type: str = "ExtractionRequested"
    page_id: UUID
    config: dict[str, Any]


class ExtractionCompleted(DomainEvent):
    """Event for extraction completion."""

    aggregate_type: str = "Extraction"
    event_type: str = "ExtractionCompleted"
    result: str


# =============================================================================
# Test fixtures: State models and Events for traditional aggregate
# =============================================================================


class OrderState(BaseModel):
    """State model for Order aggregate."""

    order_id: UUID
    status: str = "pending"


class OrderCreated(DomainEvent):
    """Event for order creation."""

    aggregate_type: str = "Order"
    event_type: str = "OrderCreated"
    customer_id: UUID


class OrderShipped(DomainEvent):
    """Event for order shipment."""

    aggregate_type: str = "Order"
    event_type: str = "OrderShipped"
    tracking_number: str


# =============================================================================
# Test Aggregate Implementations
# =============================================================================


class ExtractionProcess(DeclarativeAggregate[ExtractionState]):
    """Aggregate that requires a creation event (deferred state)."""

    aggregate_type = "Extraction"
    requires_creation_event = True

    def request(self, page_id: UUID, config: dict[str, Any]) -> ExtractionRequested:
        """Request an extraction."""
        return self.create_event(ExtractionRequested, page_id=page_id, config=config)

    def complete(self, result: str) -> ExtractionCompleted:
        """Complete the extraction."""
        return self.create_event(ExtractionCompleted, result=result)

    @handles(ExtractionRequested)
    def _on_requested(self, event: ExtractionRequested) -> None:
        self._state = ExtractionState(
            page_id=event.page_id,
            status="requested",
        )

    @handles(ExtractionCompleted)
    def _on_completed(self, event: ExtractionCompleted) -> None:
        if self._state:
            self._state = self._state.model_copy(update={"status": "completed"})


class Order(DeclarativeAggregate[OrderState]):
    """Traditional aggregate with initial state."""

    aggregate_type = "Order"
    # requires_creation_event = False (default)

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    def create(self, customer_id: UUID) -> OrderCreated:
        """Create the order."""
        return self.create_event(OrderCreated, customer_id=customer_id)

    def ship(self, tracking_number: str) -> OrderShipped:
        """Ship the order."""
        return self.create_event(OrderShipped, tracking_number=tracking_number)

    @handles(OrderCreated)
    def _on_created(self, event: OrderCreated) -> None:
        # Traditional pattern: check for None and initialize
        if self._state is None:
            self._state = self._get_initial_state()
        self._state = self._state.model_copy(update={"status": "created"})

    @handles(OrderShipped)
    def _on_shipped(self, event: OrderShipped) -> None:
        if self._state:
            self._state = self._state.model_copy(update={"status": "shipped"})


# =============================================================================
# Tests for aggregates with requires_creation_event=True
# =============================================================================


class TestDeferredStateAggregate:
    """Tests for aggregates with requires_creation_event=True."""

    def test_state_raises_before_creation(self) -> None:
        """Accessing state before creation raises error."""
        process = ExtractionProcess(uuid4())

        with pytest.raises(AggregateNotCreatedError) as exc_info:
            _ = process.state

        assert "ExtractionProcess" in str(exc_info.value)
        assert "has not been created" in str(exc_info.value)

    def test_state_works_after_creation(self) -> None:
        """Accessing state after creation works."""
        process = ExtractionProcess(uuid4())
        page_id = uuid4()

        process.request(page_id=page_id, config={})

        assert process.state.page_id == page_id
        assert process.state.status == "requested"

    def test_state_or_none_returns_none_before_creation(self) -> None:
        """state_or_none returns None before creation."""
        process = ExtractionProcess(uuid4())
        assert process.state_or_none is None

    def test_state_or_none_returns_state_after_creation(self) -> None:
        """state_or_none returns state after creation."""
        process = ExtractionProcess(uuid4())
        page_id = uuid4()

        process.request(page_id=page_id, config={})

        assert process.state_or_none is not None
        assert process.state_or_none.page_id == page_id

    def test_is_created_false_before_creation(self) -> None:
        """is_created returns False before any events."""
        process = ExtractionProcess(uuid4())
        assert process.is_created is False

    def test_is_created_true_after_creation(self) -> None:
        """is_created returns True after events applied."""
        process = ExtractionProcess(uuid4())
        process.request(page_id=uuid4(), config={})
        assert process.is_created is True

    def test_get_initial_state_returns_none(self) -> None:
        """_get_initial_state returns None for deferred state."""
        process = ExtractionProcess(uuid4())
        assert process._get_initial_state() is None

    def test_multiple_events_after_creation(self) -> None:
        """Can apply multiple events after creation."""
        process = ExtractionProcess(uuid4())
        page_id = uuid4()

        process.request(page_id=page_id, config={"key": "value"})
        assert process.state.status == "requested"

        process.complete(result="success")
        assert process.state.status == "completed"
        assert process.version == 2

    def test_version_zero_before_creation(self) -> None:
        """Version is 0 before any events."""
        process = ExtractionProcess(uuid4())
        assert process.version == 0

    def test_version_increments_after_creation(self) -> None:
        """Version increments with each event."""
        process = ExtractionProcess(uuid4())

        process.request(page_id=uuid4(), config={})
        assert process.version == 1

        process.complete(result="done")
        assert process.version == 2

    def test_uncommitted_events_tracked(self) -> None:
        """Uncommitted events are tracked correctly."""
        process = ExtractionProcess(uuid4())

        event1 = process.request(page_id=uuid4(), config={})
        event2 = process.complete(result="done")

        assert len(process.uncommitted_events) == 2
        assert event1 in process.uncommitted_events
        assert event2 in process.uncommitted_events


class TestDeferredStateAggregateWithReplay:
    """Tests for deferred state aggregates with event replay."""

    def test_load_from_history(self) -> None:
        """Can load aggregate from event history."""
        aggregate_id = uuid4()
        page_id = uuid4()

        # Create events as if from event store
        event1 = ExtractionRequested(
            aggregate_id=aggregate_id,
            aggregate_type="Extraction",
            aggregate_version=1,
            page_id=page_id,
            config={"key": "value"},
        )
        event2 = ExtractionCompleted(
            aggregate_id=aggregate_id,
            aggregate_type="Extraction",
            aggregate_version=2,
            result="success",
        )

        # Load from history
        process = ExtractionProcess(aggregate_id)
        process.load_from_history([event1, event2])

        assert process.is_created is True
        assert process.state.page_id == page_id
        assert process.state.status == "completed"
        assert process.version == 2
        assert len(process.uncommitted_events) == 0


# =============================================================================
# Tests for traditional aggregates (requires_creation_event=False)
# =============================================================================


class TestTraditionalAggregate:
    """Tests for aggregates with requires_creation_event=False (default)."""

    def test_state_is_none_before_events(self) -> None:
        """State is None before any events are applied.

        This matches the existing library behavior where _get_initial_state()
        is called manually by event handlers, not automatically.
        """
        order = Order(uuid4())
        # State starts as None - handlers call _get_initial_state()
        assert order.state is None

    def test_is_created_false_before_events(self) -> None:
        """is_created is False before any events."""
        order = Order(uuid4())
        # No state until events are applied
        assert order.is_created is False

    def test_is_created_true_after_events(self) -> None:
        """is_created is True after events are applied."""
        order = Order(uuid4())
        order.create(uuid4())
        assert order.is_created is True

    def test_state_or_none_returns_none_before_events(self) -> None:
        """state_or_none returns None before events."""
        order = Order(uuid4())
        assert order.state_or_none is None

    def test_state_or_none_returns_state_after_events(self) -> None:
        """state_or_none returns state after events."""
        order = Order(uuid4())
        order.create(uuid4())
        assert order.state_or_none is not None
        assert order.state_or_none.status == "created"

    def test_events_update_state(self) -> None:
        """Events update the state correctly."""
        order = Order(uuid4())
        customer_id = uuid4()

        order.create(customer_id)
        assert order.state is not None
        assert order.state.status == "created"

        order.ship(tracking_number="TRACK123")
        assert order.state.status == "shipped"

    def test_get_initial_state_required(self) -> None:
        """_get_initial_state must be implemented when requires_creation_event=False.

        The error is raised when _get_initial_state() is called, not at init time.
        """

        class BrokenAggregate(DeclarativeAggregate[OrderState]):
            aggregate_type = "Broken"
            # Missing _get_initial_state, missing requires_creation_event = True

        # Can create the aggregate - no error yet
        agg = BrokenAggregate(uuid4())

        # Error occurs when we try to call _get_initial_state()
        with pytest.raises(NotImplementedError) as exc_info:
            agg._get_initial_state()

        assert "must implement _get_initial_state" in str(exc_info.value)
        assert "or set requires_creation_event = True" in str(exc_info.value)

    def test_requires_creation_event_default_is_false(self) -> None:
        """Default value of requires_creation_event is False."""
        order = Order(uuid4())
        assert order.requires_creation_event is False


# =============================================================================
# Tests for AggregateNotCreatedError exception
# =============================================================================


class TestAggregateNotCreatedError:
    """Tests for AggregateNotCreatedError exception."""

    def test_error_message_includes_class_name(self) -> None:
        """Error message includes the aggregate class name."""
        error = AggregateNotCreatedError("MyAggregate")
        assert "MyAggregate" in str(error)
        assert "has not been created" in str(error)

    def test_error_message_includes_suggestion(self) -> None:
        """Error message includes suggestion when provided."""
        error = AggregateNotCreatedError("MyAggregate", suggestion="Call create() first")
        assert "Call create() first" in str(error)
        assert "Hint:" in str(error)

    def test_error_attributes(self) -> None:
        """Error has correct attributes."""
        error = AggregateNotCreatedError("MyAggregate", suggestion="hint")
        assert error.aggregate_class == "MyAggregate"
        assert error.suggestion == "hint"

    def test_error_without_suggestion(self) -> None:
        """Error without suggestion has no hint in message."""
        error = AggregateNotCreatedError("MyAggregate")
        assert error.suggestion is None
        assert "Hint:" not in str(error)

    def test_error_is_event_source_error(self) -> None:
        """AggregateNotCreatedError is an EventSourceError."""
        from eventsource.exceptions import EventSourceError

        error = AggregateNotCreatedError("MyAggregate")
        assert isinstance(error, EventSourceError)


class TestAggregateNotCreatedErrorFromAggregate:
    """Tests for AggregateNotCreatedError raised from aggregates."""

    def test_error_includes_class_name_from_aggregate(self) -> None:
        """Error includes the actual aggregate class name."""
        process = ExtractionProcess(uuid4())

        with pytest.raises(AggregateNotCreatedError) as exc_info:
            _ = process.state

        assert exc_info.value.aggregate_class == "ExtractionProcess"

    def test_error_includes_helpful_suggestion(self) -> None:
        """Error includes a helpful suggestion."""
        process = ExtractionProcess(uuid4())

        with pytest.raises(AggregateNotCreatedError) as exc_info:
            _ = process.state

        assert "Call a creation method on ExtractionProcess first" in str(exc_info.value)


# =============================================================================
# Tests for backward compatibility
# =============================================================================


class TestBackwardCompatibility:
    """Tests to ensure backward compatibility with existing patterns."""

    def test_existing_aggregate_pattern_unchanged(self) -> None:
        """Existing aggregates with _get_initial_state continue to work.

        The existing pattern is: state is None until first event handler sets it.
        """
        order = Order(uuid4())
        # State starts as None - this is the existing behavior
        assert order.state is None

        # Apply event - handler calls _get_initial_state()
        customer_id = uuid4()
        order.create(customer_id)

        assert order.state is not None
        assert order.state.status == "created"

    def test_apply_event_manually(self) -> None:
        """Manual event application continues to work."""
        order = Order(uuid4())

        event = OrderCreated(
            aggregate_id=order.aggregate_id,
            aggregate_type="Order",
            aggregate_version=1,
            customer_id=uuid4(),
        )
        order.apply_event(event, is_new=True)

        assert order.state is not None
        assert order.state.status == "created"
        assert event in order.uncommitted_events

    def test_load_from_history_traditional(self) -> None:
        """Load from history works with traditional aggregates."""
        aggregate_id = uuid4()

        event1 = OrderCreated(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            aggregate_version=1,
            customer_id=uuid4(),
        )
        event2 = OrderShipped(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            aggregate_version=2,
            tracking_number="TRACK123",
        )

        order = Order(aggregate_id)
        order.load_from_history([event1, event2])

        assert order.state is not None
        assert order.state.status == "shipped"
        assert order.version == 2
        assert len(order.uncommitted_events) == 0


# =============================================================================
# Tests for edge cases
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_deferred_aggregate_with_empty_history(self) -> None:
        """Deferred aggregate with empty history has no state."""
        process = ExtractionProcess(uuid4())
        process.load_from_history([])

        assert process.is_created is False
        assert process.state_or_none is None
        assert process.version == 0

    def test_multiple_aggregates_independent(self) -> None:
        """Multiple aggregate instances are independent."""
        process1 = ExtractionProcess(uuid4())
        process2 = ExtractionProcess(uuid4())

        process1.request(page_id=uuid4(), config={"key": "1"})

        assert process1.is_created is True
        assert process2.is_created is False

    def test_class_attribute_is_class_var(self) -> None:
        """requires_creation_event is a class variable, not instance."""
        process1 = ExtractionProcess(uuid4())
        process2 = ExtractionProcess(uuid4())

        # Same class attribute
        assert process1.requires_creation_event is process2.requires_creation_event
        assert ExtractionProcess.requires_creation_event is True

    def test_traditional_aggregate_class_attribute(self) -> None:
        """Traditional aggregate has requires_creation_event=False."""
        order1 = Order(uuid4())
        order2 = Order(uuid4())

        assert order1.requires_creation_event is False
        assert order2.requires_creation_event is False
        assert Order.requires_creation_event is False


class TestDeferredStateWithInheritance:
    """Tests for deferred state with aggregate inheritance."""

    def test_subclass_inherits_requires_creation_event(self) -> None:
        """Subclass can inherit requires_creation_event."""

        class BaseExtraction(DeclarativeAggregate[ExtractionState]):
            aggregate_type = "Extraction"
            requires_creation_event = True

            def _get_initial_state(self) -> ExtractionState | None:
                return None

        class SpecialExtraction(BaseExtraction):
            pass

        process = SpecialExtraction(uuid4())
        assert process.requires_creation_event is True
        assert process.is_created is False

    def test_subclass_can_override_requires_creation_event(self) -> None:
        """Subclass can override requires_creation_event."""

        class BaseAggregate(DeclarativeAggregate[OrderState]):
            aggregate_type = "Base"
            requires_creation_event = True

            def _get_initial_state(self) -> OrderState | None:
                return None

        class TraditionalSubclass(BaseAggregate):
            requires_creation_event = False

            def _get_initial_state(self) -> OrderState:
                return OrderState(order_id=self.aggregate_id)

        agg = TraditionalSubclass(uuid4())
        assert agg.requires_creation_event is False
        # State is still None until events are applied (existing library behavior)
        assert agg.is_created is False
        assert agg.state is None
        # But _get_initial_state() returns a valid state when called
        assert agg._get_initial_state() is not None


# =============================================================================
# Tests for state property behavior consistency
# =============================================================================


class TestStatePropertyConsistency:
    """Tests for state property behavior consistency."""

    def test_state_and_state_or_none_consistent_when_created(self) -> None:
        """state and state_or_none return same value when created."""
        process = ExtractionProcess(uuid4())
        page_id = uuid4()
        process.request(page_id=page_id, config={})

        assert process.state == process.state_or_none
        assert process.state is process.state_or_none

    def test_state_and_state_or_none_consistent_traditional_after_events(self) -> None:
        """state and state_or_none return same value for traditional aggregate after events."""
        order = Order(uuid4())
        order.create(uuid4())

        assert order.state == order.state_or_none
        assert order.state is order.state_or_none

    def test_state_and_state_or_none_both_none_before_events(self) -> None:
        """state and state_or_none both return None for traditional aggregate before events."""
        order = Order(uuid4())

        # Both should be None before events
        assert order.state is None
        assert order.state_or_none is None
        assert order.state is order.state_or_none

    def test_is_created_consistent_with_state_or_none(self) -> None:
        """is_created is consistent with state_or_none being not None."""
        process = ExtractionProcess(uuid4())

        assert (process.state_or_none is not None) == process.is_created

        process.request(page_id=uuid4(), config={})

        assert (process.state_or_none is not None) == process.is_created

    def test_is_created_consistent_traditional(self) -> None:
        """is_created is consistent for traditional aggregates."""
        order = Order(uuid4())

        assert (order.state_or_none is not None) == order.is_created

        order.create(uuid4())

        assert (order.state_or_none is not None) == order.is_created
