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


class TestFilterStats:
    """Tests for FilterStats dataclass."""

    def test_default_values(self):
        stats = FilterStats()
        assert stats.events_evaluated == 0
        assert stats.events_matched == 0
        assert stats.events_skipped == 0

    def test_match_rate_with_no_events(self):
        assert FilterStats().match_rate == 1.0

    def test_match_rate_calculation(self):
        stats = FilterStats(events_evaluated=100, events_matched=75, events_skipped=25)
        assert stats.match_rate == 0.75

    def test_record_match(self):
        stats = FilterStats()
        stats.record_match()
        stats.record_match()
        assert stats.events_evaluated == 2
        assert stats.events_matched == 2
        assert stats.events_skipped == 0

    def test_record_skip(self):
        stats = FilterStats()
        stats.record_skip()
        stats.record_skip()
        stats.record_skip()
        assert stats.events_evaluated == 3
        assert stats.events_matched == 0
        assert stats.events_skipped == 3

    def test_to_dict(self):
        stats = FilterStats(events_evaluated=10, events_matched=8, events_skipped=2)
        result = stats.to_dict()
        assert result == {
            "events_evaluated": 10,
            "events_matched": 8,
            "events_skipped": 2,
            "match_rate": 0.8,
        }


class TestEventFilterCreation:
    """Tests for EventFilter creation methods."""

    def test_default_filter_matches_all(self, order_created: OrderCreated):
        filter = EventFilter()
        assert filter.is_configured is False
        assert filter.matches(order_created) is True

    def test_from_config_with_event_types(self):
        config = SubscriptionConfig(event_types=(OrderCreated, OrderShipped))
        filter = EventFilter.from_config(config)
        assert filter.event_types == (OrderCreated, OrderShipped)
        assert filter.is_configured is True

    def test_from_config_with_aggregate_types(self):
        config = SubscriptionConfig(aggregate_types=("Order", "Payment"))
        filter = EventFilter.from_config(config)
        assert filter.aggregate_types == ("Order", "Payment")
        assert filter.is_configured is True

    def test_from_subscriber(self):
        filter = EventFilter.from_subscriber(MockOrderSubscriber())
        assert filter.event_types == (OrderCreated, OrderShipped)
        assert filter.is_configured is True

    def test_from_subscriber_empty_returns_no_filter(self):
        filter = EventFilter.from_subscriber(MockAllEventsSubscriber())
        assert filter.event_types is None
        assert filter.is_configured is False

    def test_from_config_and_subscriber_prefers_config(self):
        config = SubscriptionConfig(event_types=(OrderCancelled,))
        filter = EventFilter.from_config_and_subscriber(config, MockOrderSubscriber())
        assert filter.event_types == (OrderCancelled,)

    def test_from_config_and_subscriber_falls_back_to_subscriber(self):
        filter = EventFilter.from_config_and_subscriber(SubscriptionConfig(), MockOrderSubscriber())
        assert filter.event_types == (OrderCreated, OrderShipped)

    def test_from_patterns(self):
        filter = EventFilter.from_patterns("Order*", "Payment*")
        assert filter.event_type_patterns == ("Order*", "Payment*")
        assert filter.is_configured is True

    def test_normalizes_list_and_set_inputs_to_tuples(self, order_created: OrderCreated):
        filter = EventFilter(
            event_types=[OrderCreated, OrderShipped],
            event_type_patterns=["Order*"],
            aggregate_types={"Order"},
        )
        assert filter.event_types == (OrderCreated, OrderShipped)
        assert filter.event_type_patterns == ("Order*",)
        assert filter.aggregate_types == ("Order",)
        assert filter.matches(order_created) is True

    def test_callable_and_filter_with_events_and_envelopes(
        self,
        order_created: OrderCreated,
        order_shipped: OrderShipped,
        order_cancelled: OrderCancelled,
    ):
        from datetime import UTC, datetime

        from eventsource.domain import StreamId
        from eventsource.ports.envelopes import EventEnvelope

        ef = EventFilter(event_types=[OrderCreated, OrderShipped])
        assert ef(order_created) is True
        assert ef(order_cancelled) is False
        assert ef.filter([order_created, order_cancelled, order_shipped]) == [
            order_created,
            order_shipped,
        ]

        now = datetime.now(UTC)
        envelopes = [
            EventEnvelope(
                order_created, StreamId(order_created.aggregate_id, "Order"), 1, None, now
            ),
            EventEnvelope(
                order_cancelled, StreamId(order_cancelled.aggregate_id, "Order"), 2, None, now
            ),
            EventEnvelope(
                order_shipped, StreamId(order_shipped.aggregate_id, "Order"), 3, None, now
            ),
        ]
        assert ef(envelopes[0]) is True and ef(envelopes[1]) is False
        assert ef.filter(envelopes) == [envelopes[0], envelopes[2]]


class TestEventFilterRepr:
    """Tests for EventFilter string representation."""

    def test_repr_with_event_types(self):
        repr_str = repr(EventFilter(event_types=(OrderCreated,)))
        assert "EventFilter" in repr_str and "OrderCreated" in repr_str

    def test_repr_with_patterns(self):
        repr_str = repr(EventFilter(event_type_patterns=("Order*",)))
        assert "EventFilter" in repr_str and "patterns" in repr_str and "Order*" in repr_str

    def test_repr_with_aggregates(self):
        repr_str = repr(EventFilter(aggregate_types=("Order",)))
        assert "EventFilter" in repr_str and "aggregates" in repr_str and "Order" in repr_str

    def test_repr_no_filters(self):
        assert "EventFilter(all)" in repr(EventFilter())


class TestUtilityFunctions:
    """Tests for utility functions."""

    def test_matches_event_type_with_types(self, order_created: OrderCreated):
        assert matches_event_type(order_created, (OrderCreated,)) is True
        assert matches_event_type(order_created, (OrderShipped,)) is False

    def test_matches_event_type_with_none(self, order_created: OrderCreated):
        assert matches_event_type(order_created, None) is True

    def test_matches_pattern_star(self):
        assert matches_pattern("OrderCreated", "Order*") is True
        assert matches_pattern("PaymentReceived", "Order*") is False

    def test_matches_pattern_question(self):
        assert matches_pattern("OrderCreated", "Order?reated") is True
        assert matches_pattern("OrderCreated", "?rderCreated") is True

    def test_matches_pattern_exact(self):
        assert matches_pattern("OrderCreated", "OrderCreated") is True
        assert matches_pattern("OrderCreated", "OrderShipped") is False
