"""Unit tests for FilterStats, EventFilter creation, repr, and utility functions."""

from eventsource.application.subscriptions.config import SubscriptionConfig
from eventsource.application.subscriptions.filtering import (
    EventFilter,
    FilterStats,
    matches_event_type,
    matches_pattern,
)
from tests.unit.application.subscriptions.test_filtering._fixtures import (
    MockAllEventsSubscriber,
    MockOrderSubscriber,
    OrderCancelled,
    OrderCreated,
    OrderShipped,
)

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
