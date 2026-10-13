"""Tests for traditional aggregates and backward compatibility."""

from uuid import uuid4

import pytest

from eventsource.domain.aggregate import DeclarativeAggregate
from tests.unit.domain.test_deferred_state._fixtures import (
    Order,
    OrderCreated,
    OrderShipped,
    OrderState,
)


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
