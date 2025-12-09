"""
Unit tests for event filtering functionality.

Tests cover:
- EventFilter creation from config, subscriber, patterns
- Exact event type matching
- Wildcard pattern matching (*, ?)
- Aggregate type filtering
- Combined filter criteria
- Filter statistics tracking
- Utility functions
"""

from uuid import uuid4

import pytest

from eventsource.events.base import DomainEvent
from eventsource.subscriptions.config import SubscriptionConfig
from eventsource.subscriptions.filtering import (
    EventFilter,
    FilterStats,
    matches_event_type,
    matches_pattern,
)

# --- Sample Event Classes ---


class OrderCreated(DomainEvent):
    """Order created event."""

    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    customer_id: str = "test"


class OrderShipped(DomainEvent):
    """Order shipped event."""

    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"
    tracking_number: str = "123"


class OrderCancelled(DomainEvent):
    """Order cancelled event."""

    event_type: str = "OrderCancelled"
    aggregate_type: str = "Order"
    reason: str = ""


class PaymentReceived(DomainEvent):
    """Payment received event."""

    event_type: str = "PaymentReceived"
    aggregate_type: str = "Payment"
    amount: float = 0.0


class UserRegistered(DomainEvent):
    """User registered event."""

    event_type: str = "UserRegistered"
    aggregate_type: str = "User"
    email: str = "test@example.com"


class UserUpdatedEvent(DomainEvent):
    """User updated event with 'Event' suffix."""

    event_type: str = "UserUpdatedEvent"
    aggregate_type: str = "User"
    name: str = ""


# --- Mock Subscriber ---


class MockOrderSubscriber:
    """Subscriber for order events only."""

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderCreated, OrderShipped]

    async def handle(self, event: DomainEvent) -> None:
        pass


class MockAllEventsSubscriber:
    """Subscriber for all events (empty subscribed_to)."""

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return []

    async def handle(self, event: DomainEvent) -> None:
        pass


# --- Fixtures ---


@pytest.fixture
def order_created() -> OrderCreated:
    """Create an OrderCreated event."""
    return OrderCreated(aggregate_id=uuid4())


@pytest.fixture
def order_shipped() -> OrderShipped:
    """Create an OrderShipped event."""
    return OrderShipped(aggregate_id=uuid4())


@pytest.fixture
def order_cancelled() -> OrderCancelled:
    """Create an OrderCancelled event."""
    return OrderCancelled(aggregate_id=uuid4())


@pytest.fixture
def payment_received() -> PaymentReceived:
    """Create a PaymentReceived event."""
    return PaymentReceived(aggregate_id=uuid4())


@pytest.fixture
def user_registered() -> UserRegistered:
    """Create a UserRegistered event."""
    return UserRegistered(aggregate_id=uuid4())


# --- FilterStats Tests ---


class TestFilterStats:
    """Tests for FilterStats dataclass."""

    def test_default_values(self):
        """Test default values are all zero."""
        stats = FilterStats()
        assert stats.events_evaluated == 0
        assert stats.events_matched == 0
        assert stats.events_skipped == 0

    def test_match_rate_with_no_events(self):
        """Test match rate returns 1.0 when no events evaluated."""
        stats = FilterStats()
        assert stats.match_rate == 1.0

    def test_match_rate_calculation(self):
        """Test match rate calculation."""
        stats = FilterStats(
            events_evaluated=100,
            events_matched=75,
            events_skipped=25,
        )
        assert stats.match_rate == 0.75

    def test_record_match(self):
        """Test record_match updates counters."""
        stats = FilterStats()
        stats.record_match()
        stats.record_match()

        assert stats.events_evaluated == 2
        assert stats.events_matched == 2
        assert stats.events_skipped == 0

    def test_record_skip(self):
        """Test record_skip updates counters."""
        stats = FilterStats()
        stats.record_skip()
        stats.record_skip()
        stats.record_skip()

        assert stats.events_evaluated == 3
        assert stats.events_matched == 0
        assert stats.events_skipped == 3

    def test_to_dict(self):
        """Test dictionary serialization."""
        stats = FilterStats(
            events_evaluated=10,
            events_matched=8,
            events_skipped=2,
        )
        result = stats.to_dict()

        assert result["events_evaluated"] == 10
        assert result["events_matched"] == 8
        assert result["events_skipped"] == 2
        assert result["match_rate"] == 0.8


# --- EventFilter Creation Tests ---


class TestEventFilterCreation:
    """Tests for EventFilter creation methods."""

    def test_default_filter_matches_all(self, order_created: OrderCreated):
        """Test default filter with no criteria matches all events."""
        filter = EventFilter()

        assert filter.is_configured is False
        assert filter.matches(order_created) is True

    def test_from_config_with_event_types(self):
        """Test creating filter from config with event_types."""
        config = SubscriptionConfig(
            event_types=(OrderCreated, OrderShipped),
        )
        filter = EventFilter.from_config(config)

        assert filter.event_types == (OrderCreated, OrderShipped)
        assert filter.is_configured is True

    def test_from_config_with_aggregate_types(self):
        """Test creating filter from config with aggregate_types."""
        config = SubscriptionConfig(
            aggregate_types=("Order", "Payment"),
        )
        filter = EventFilter.from_config(config)

        assert filter.aggregate_types == ("Order", "Payment")
        assert filter.is_configured is True

    def test_from_subscriber(self):
        """Test creating filter from subscriber's subscribed_to()."""
        subscriber = MockOrderSubscriber()
        filter = EventFilter.from_subscriber(subscriber)

        assert filter.event_types == (OrderCreated, OrderShipped)
        assert filter.is_configured is True

    def test_from_subscriber_empty_returns_no_filter(self):
        """Test that subscriber with empty subscribed_to creates pass-through filter."""
        subscriber = MockAllEventsSubscriber()
        filter = EventFilter.from_subscriber(subscriber)

        assert filter.event_types is None
        assert filter.is_configured is False

    def test_from_config_and_subscriber_prefers_config(self):
        """Test that config event_types takes precedence over subscriber."""
        config = SubscriptionConfig(
            event_types=(OrderCancelled,),
        )
        subscriber = MockOrderSubscriber()

        filter = EventFilter.from_config_and_subscriber(config, subscriber)

        # Config takes precedence
        assert filter.event_types == (OrderCancelled,)

    def test_from_config_and_subscriber_falls_back_to_subscriber(self):
        """Test that subscriber is used when config has no event_types."""
        config = SubscriptionConfig()  # No event_types
        subscriber = MockOrderSubscriber()

        filter = EventFilter.from_config_and_subscriber(config, subscriber)

        # Falls back to subscriber
        assert filter.event_types == (OrderCreated, OrderShipped)

    def test_from_patterns(self):
        """Test creating filter from patterns."""
        filter = EventFilter.from_patterns("Order*", "Payment*")

        assert filter.event_type_patterns == ("Order*", "Payment*")
        assert filter.is_configured is True


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


# --- Statistics Tests ---


class TestEventFilterStatistics:
    """Tests for filter statistics tracking."""

    def test_stats_track_matches(
        self,
        order_created: OrderCreated,
        order_shipped: OrderShipped,
    ):
        """Test that statistics track matches."""
        filter = EventFilter(event_types=(OrderCreated,))

        filter.matches(order_created)  # Match
        filter.matches(order_shipped)  # Skip

        assert filter.stats.events_evaluated == 2
        assert filter.stats.events_matched == 1
        assert filter.stats.events_skipped == 1

    def test_stats_for_pass_through_filter(
        self,
        order_created: OrderCreated,
        payment_received: PaymentReceived,
    ):
        """Test that pass-through filter counts all as matches."""
        filter = EventFilter()

        filter.matches(order_created)
        filter.matches(payment_received)

        assert filter.stats.events_matched == 2
        assert filter.stats.events_skipped == 0

    def test_reset_stats(
        self,
        order_created: OrderCreated,
    ):
        """Test resetting statistics."""
        filter = EventFilter()
        filter.matches(order_created)

        assert filter.stats.events_evaluated > 0

        filter.reset_stats()

        assert filter.stats.events_evaluated == 0
        assert filter.stats.events_matched == 0
        assert filter.stats.events_skipped == 0


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


# --- Repr Tests ---


class TestEventFilterRepr:
    """Tests for EventFilter string representation."""

    def test_repr_with_event_types(self):
        """Test repr shows event types."""
        filter = EventFilter(event_types=(OrderCreated,))
        repr_str = repr(filter)

        assert "EventFilter" in repr_str
        assert "OrderCreated" in repr_str

    def test_repr_with_patterns(self):
        """Test repr shows patterns."""
        filter = EventFilter(event_type_patterns=("Order*",))
        repr_str = repr(filter)

        assert "EventFilter" in repr_str
        assert "patterns" in repr_str
        assert "Order*" in repr_str

    def test_repr_with_aggregates(self):
        """Test repr shows aggregate types."""
        filter = EventFilter(aggregate_types=("Order",))
        repr_str = repr(filter)

        assert "EventFilter" in repr_str
        assert "aggregates" in repr_str
        assert "Order" in repr_str

    def test_repr_no_filters(self):
        """Test repr for filter with no criteria."""
        filter = EventFilter()
        repr_str = repr(filter)

        assert "EventFilter(all)" in repr_str


# --- Utility Functions Tests ---


class TestUtilityFunctions:
    """Tests for utility functions."""

    def test_matches_event_type_with_types(self, order_created: OrderCreated):
        """Test matches_event_type utility function."""
        assert matches_event_type(order_created, (OrderCreated,)) is True
        assert matches_event_type(order_created, (OrderShipped,)) is False

    def test_matches_event_type_with_none(self, order_created: OrderCreated):
        """Test matches_event_type with None (matches all)."""
        assert matches_event_type(order_created, None) is True

    def test_matches_pattern_star(self):
        """Test matches_pattern with star wildcard."""
        assert matches_pattern("OrderCreated", "Order*") is True
        assert matches_pattern("PaymentReceived", "Order*") is False

    def test_matches_pattern_question(self):
        """Test matches_pattern with question mark wildcard."""
        assert matches_pattern("OrderCreated", "Order?reated") is True
        assert matches_pattern("OrderCreated", "?rderCreated") is True

    def test_matches_pattern_exact(self):
        """Test matches_pattern with exact match."""
        assert matches_pattern("OrderCreated", "OrderCreated") is True
        assert matches_pattern("OrderCreated", "OrderShipped") is False


# --- Edge Cases Tests ---


class TestEventFilterEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_empty_event_types_tuple_is_pass_through(self, order_created: OrderCreated):
        """Test with empty event types tuple - treated as pass-through.

        An empty tuple () is falsy in Python, so `if self.event_types:` evaluates
        to False and no internal set is created. This means we fall through to
        the pass-through behavior.

        However, is_configured still returns True because the tuple is not None.
        The actual filtering behavior uses the internal set, which is None for
        empty tuples, so events pass through.
        """
        filter = EventFilter(event_types=())

        # Technically "configured" because event_types is not None
        assert filter.is_configured is True
        # But internal set is None (empty tuple doesn't create set)
        # So events pass through
        assert filter.matches(order_created) is True

    def test_empty_patterns_tuple_rejects_all(self, order_created: OrderCreated):
        """Test with empty patterns tuple - rejects all events.

        Note: Empty patterns tuple IS considered configured, and since no
        patterns can match, all events are rejected.
        """
        filter = EventFilter(event_type_patterns=())

        # Empty patterns IS configured
        assert filter.is_configured is True
        # But no patterns match anything
        assert filter.matches(order_created) is False

    def test_empty_aggregate_types_tuple_is_pass_through(self, order_created: OrderCreated):
        """Test with empty aggregate types tuple - treated as pass-through.

        Same behavior as empty event_types tuple - the empty tuple doesn't
        create an internal set, so events pass through.
        """
        filter = EventFilter(aggregate_types=())

        # Technically "configured" because aggregate_types is not None
        assert filter.is_configured is True
        # But internal set is None (empty tuple doesn't create set)
        # So events pass through
        assert filter.matches(order_created) is True

    def test_filter_is_reusable(
        self,
        order_created: OrderCreated,
        order_shipped: OrderShipped,
    ):
        """Test that filter can be used multiple times."""
        filter = EventFilter(event_types=(OrderCreated,))

        # Call matches multiple times
        for _ in range(100):
            filter.matches(order_created)
            filter.matches(order_shipped)

        assert filter.stats.events_evaluated == 200
        assert filter.stats.events_matched == 100
        assert filter.stats.events_skipped == 100


# --- Integration with CatchUpRunner and LiveRunner Tests ---


class TestEventFilterIntegration:
    """Tests for filter integration scenarios."""

    def test_subscriber_pattern_matching(self):
        """Test creating filter from subscriber for typical projection use case."""
        subscriber = MockOrderSubscriber()
        filter = EventFilter.from_subscriber(subscriber)

        # Should match OrderCreated and OrderShipped
        order_created = OrderCreated(aggregate_id=uuid4())
        order_shipped = OrderShipped(aggregate_id=uuid4())
        order_cancelled = OrderCancelled(aggregate_id=uuid4())

        assert filter.matches(order_created) is True
        assert filter.matches(order_shipped) is True
        assert filter.matches(order_cancelled) is False

    def test_config_override_subscriber(self):
        """Test that config can restrict subscriber's event types."""
        config = SubscriptionConfig(
            event_types=(OrderCreated,),  # Only OrderCreated, not OrderShipped
        )
        subscriber = MockOrderSubscriber()  # Subscribes to both

        filter = EventFilter.from_config_and_subscriber(config, subscriber)

        order_created = OrderCreated(aggregate_id=uuid4())
        order_shipped = OrderShipped(aggregate_id=uuid4())

        # Config restricts to only OrderCreated
        assert filter.matches(order_created) is True
        assert filter.matches(order_shipped) is False
