"""Unit tests for EventFilter matching rules (exact, patterns, aggregates, combined, event type names)."""

from uuid import uuid4

from eventsource.application.subscriptions.filtering import EventFilter
from eventsource.domain.event import DomainEvent
from tests.unit.application.subscriptions.test_filtering._fixtures import (
    OrderCancelled,
    OrderCreated,
    OrderShipped,
    PaymentReceived,
    UserRegistered,
    UserUpdatedEvent,
)

# --- Exact Type Matching Tests ---


class TestEventFilterExactMatching:
    """Tests for exact event type matching."""

    def test_matches_exact_type(self, order_created: OrderCreated):
        """Test that filter matches configured event types."""
        filter = EventFilter(event_types=(OrderCreated,))

        assert filter.matches(order_created) is True

    def test_rejects_unmatched_type(
        self,
        order_created: OrderCreated,
        payment_received: PaymentReceived,
    ):
        """Test that filter rejects events not in configured types."""
        filter = EventFilter(event_types=(OrderCreated,))

        assert filter.matches(order_created) is True
        assert filter.matches(payment_received) is False

    def test_matches_multiple_types(
        self,
        order_created: OrderCreated,
        order_shipped: OrderShipped,
        order_cancelled: OrderCancelled,
    ):
        """Test filter with multiple event types."""
        filter = EventFilter(event_types=(OrderCreated, OrderShipped))

        assert filter.matches(order_created) is True
        assert filter.matches(order_shipped) is True
        assert filter.matches(order_cancelled) is False


# --- Pattern Matching Tests ---


class TestEventFilterPatternMatching:
    """Tests for wildcard pattern matching."""

    def test_star_pattern_matches_prefix(
        self,
        order_created: OrderCreated,
        order_shipped: OrderShipped,
        payment_received: PaymentReceived,
    ):
        """Test that 'Order*' matches events starting with 'Order'."""
        filter = EventFilter(event_type_patterns=("Order*",))

        assert filter.matches(order_created) is True
        assert filter.matches(order_shipped) is True
        assert filter.matches(payment_received) is False

    def test_star_pattern_matches_suffix(
        self,
        order_created: OrderCreated,
        payment_received: PaymentReceived,
    ):
        """Test that '*Created' matches events ending with 'Created'."""
        filter = EventFilter(event_type_patterns=("*Created",))

        assert filter.matches(order_created) is True
        assert filter.matches(payment_received) is False

    def test_question_mark_pattern(
        self,
        order_created: OrderCreated,
    ):
        """Test that '?' matches any single character."""
        # OrderCreated has 12 characters
        filter = EventFilter(event_type_patterns=("Order?reated",))

        # Should match 'OrderCreated' since 'C' is one character
        assert filter.matches(order_created) is True

    def test_multiple_patterns_or_logic(
        self,
        order_created: OrderCreated,
        payment_received: PaymentReceived,
        user_registered: UserRegistered,
    ):
        """Test that multiple patterns use OR logic."""
        filter = EventFilter(event_type_patterns=("Order*", "Payment*"))

        assert filter.matches(order_created) is True
        assert filter.matches(payment_received) is True
        assert filter.matches(user_registered) is False

    def test_wildcard_matches_all(
        self,
        order_created: OrderCreated,
        payment_received: PaymentReceived,
        user_registered: UserRegistered,
    ):
        """Test that '*' pattern matches all event types."""
        filter = EventFilter(event_type_patterns=("*",))

        assert filter.matches(order_created) is True
        assert filter.matches(payment_received) is True
        assert filter.matches(user_registered) is True

    def test_suffix_event_pattern(self):
        """Test matching events with specific suffix."""
        event = UserUpdatedEvent(aggregate_id=uuid4())
        filter = EventFilter(event_type_patterns=("*Event",))

        assert filter.matches(event) is True


# --- Aggregate Type Filtering Tests ---


class TestEventFilterAggregateTypes:
    """Tests for aggregate type filtering."""

    def test_matches_aggregate_type(
        self,
        order_created: OrderCreated,
        payment_received: PaymentReceived,
    ):
        """Test filtering by aggregate type."""
        filter = EventFilter(aggregate_types=("Order",))

        assert filter.matches(order_created) is True
        assert filter.matches(payment_received) is False

    def test_matches_multiple_aggregate_types(
        self,
        order_created: OrderCreated,
        payment_received: PaymentReceived,
        user_registered: UserRegistered,
    ):
        """Test filtering by multiple aggregate types."""
        filter = EventFilter(aggregate_types=("Order", "Payment"))

        assert filter.matches(order_created) is True
        assert filter.matches(payment_received) is True
        assert filter.matches(user_registered) is False


# --- Combined Filter Tests ---


class TestEventFilterCombined:
    """Tests for combined filter criteria."""

    def test_event_type_and_aggregate_type(
        self,
        order_created: OrderCreated,
        order_shipped: OrderShipped,
        payment_received: PaymentReceived,
    ):
        """Test that both event type and aggregate type must match."""
        filter = EventFilter(
            event_types=(OrderCreated, PaymentReceived),
            aggregate_types=("Order",),
        )

        # OrderCreated: matches both event type and aggregate
        assert filter.matches(order_created) is True

        # OrderShipped: matches aggregate but not event type
        assert filter.matches(order_shipped) is False

        # PaymentReceived: matches event type but not aggregate
        assert filter.matches(payment_received) is False

    def test_event_types_and_patterns_both_must_match(
        self,
        order_created: OrderCreated,
        order_shipped: OrderShipped,
    ):
        """Test that both event types and patterns must match when both are set."""
        filter = EventFilter(
            event_types=(OrderCreated, OrderShipped),
            event_type_patterns=("*Created",),
        )

        # OrderCreated: matches both event type set and pattern
        assert filter.matches(order_created) is True

        # OrderShipped: matches event type set but not pattern
        assert filter.matches(order_shipped) is False


# --- Event Type Names Property Tests ---


class TestEventFilterEventTypeNames:
    """Tests for event_type_names property."""

    def test_event_type_names_from_types(self):
        """Test getting event type names from configured types."""
        filter = EventFilter(event_types=(OrderCreated, OrderShipped))

        names = filter.event_type_names
        assert names is not None
        assert "OrderCreated" in names
        assert "OrderShipped" in names

    def test_event_type_names_none_when_no_types(self):
        """Test that event_type_names is None when no types configured."""
        filter = EventFilter()

        assert filter.event_type_names is None

    def test_event_type_names_includes_auto_named_classes(self):
        """Regression test: classes without an explicit event_type field must
        still contribute their auto-derived name (event_type_name())."""

        class AutoNamedOrderCancelled(DomainEvent):
            aggregate_type: str = "Order"

        filter = EventFilter(event_types=(OrderCreated, AutoNamedOrderCancelled))

        names = filter.event_type_names
        assert names is not None
        assert "OrderCreated" in names
        assert "AutoNamedOrderCancelled" in names
