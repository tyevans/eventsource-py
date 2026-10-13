"""Unit tests for EventFilter statistics, edge cases, and integration scenarios."""

from uuid import uuid4

from eventsource.application.subscriptions.config import SubscriptionConfig
from eventsource.application.subscriptions.filtering import EventFilter
from tests.unit.application.subscriptions.test_filtering._fixtures import (
    MockOrderSubscriber,
    OrderCancelled,
    OrderCreated,
    OrderShipped,
    PaymentReceived,
)

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
