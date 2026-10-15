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


class TestEventFilterExactMatching:
    """Tests for exact event type matching."""

    def test_matches_exact_type(self, order_created: OrderCreated):
        filter = EventFilter(event_types=(OrderCreated,))
        assert filter.matches(order_created) is True

    def test_rejects_unmatched_type(
        self, order_created: OrderCreated, payment_received: PaymentReceived
    ):
        filter = EventFilter(event_types=(OrderCreated,))
        assert filter.matches(order_created) is True
        assert filter.matches(payment_received) is False

    def test_matches_multiple_types(
        self,
        order_created: OrderCreated,
        order_shipped: OrderShipped,
        order_cancelled: OrderCancelled,
    ):
        filter = EventFilter(event_types=(OrderCreated, OrderShipped))
        assert filter.matches(order_created) is True
        assert filter.matches(order_shipped) is True
        assert filter.matches(order_cancelled) is False


class TestEventFilterPatternMatching:
    """Tests for wildcard pattern matching."""

    def test_star_pattern_matches_prefix(
        self,
        order_created: OrderCreated,
        order_shipped: OrderShipped,
        payment_received: PaymentReceived,
    ):
        filter = EventFilter(event_type_patterns=("Order*",))
        assert filter.matches(order_created) is True
        assert filter.matches(order_shipped) is True
        assert filter.matches(payment_received) is False

    def test_star_pattern_matches_suffix(
        self, order_created: OrderCreated, payment_received: PaymentReceived
    ):
        filter = EventFilter(event_type_patterns=("*Created",))
        assert filter.matches(order_created) is True
        assert filter.matches(payment_received) is False

    def test_question_mark_pattern(self, order_created: OrderCreated):
        filter = EventFilter(event_type_patterns=("Order?reated",))
        assert filter.matches(order_created) is True

    def test_multiple_patterns_or_logic(
        self,
        order_created: OrderCreated,
        payment_received: PaymentReceived,
        user_registered: UserRegistered,
    ):
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
        filter = EventFilter(event_type_patterns=("*",))
        assert filter.matches(order_created) is True
        assert filter.matches(payment_received) is True
        assert filter.matches(user_registered) is True

    def test_suffix_event_pattern(self):
        event = UserUpdatedEvent(aggregate_id=uuid4())
        filter = EventFilter(event_type_patterns=("*Event",))
        assert filter.matches(event) is True


class TestEventFilterAggregateTypes:
    """Tests for aggregate type filtering."""

    def test_matches_aggregate_type(
        self, order_created: OrderCreated, payment_received: PaymentReceived
    ):
        filter = EventFilter(aggregate_types=("Order",))
        assert filter.matches(order_created) is True
        assert filter.matches(payment_received) is False

    def test_matches_multiple_aggregate_types(
        self,
        order_created: OrderCreated,
        payment_received: PaymentReceived,
        user_registered: UserRegistered,
    ):
        filter = EventFilter(aggregate_types=("Order", "Payment"))
        assert filter.matches(order_created) is True
        assert filter.matches(payment_received) is True
        assert filter.matches(user_registered) is False


class TestEventFilterCombined:
    """Tests for combined filter criteria."""

    def test_event_type_and_aggregate_type(
        self,
        order_created: OrderCreated,
        order_shipped: OrderShipped,
        payment_received: PaymentReceived,
    ):
        filter = EventFilter(
            event_types=(OrderCreated, PaymentReceived),
            aggregate_types=("Order",),
        )
        assert filter.matches(order_created) is True
        assert filter.matches(order_shipped) is False
        assert filter.matches(payment_received) is False

    def test_event_types_and_patterns_both_must_match(
        self, order_created: OrderCreated, order_shipped: OrderShipped
    ):
        filter = EventFilter(
            event_types=(OrderCreated, OrderShipped),
            event_type_patterns=("*Created",),
        )
        assert filter.matches(order_created) is True
        assert filter.matches(order_shipped) is False


class TestEventFilterEventTypeNames:
    """Tests for event_type_names property."""

    def test_event_type_names_from_types(self):
        filter = EventFilter(event_types=(OrderCreated, OrderShipped))
        names = filter.event_type_names
        assert names is not None
        assert "OrderCreated" in names and "OrderShipped" in names

    def test_event_type_names_none_when_no_types(self):
        assert EventFilter().event_type_names is None

    def test_event_type_names_includes_auto_named_classes(self):
        class AutoNamedOrderCancelled(DomainEvent):
            aggregate_type: str = "Order"

        filter = EventFilter(event_types=(OrderCreated, AutoNamedOrderCancelled))
        names = filter.event_type_names
        assert names is not None
        assert "OrderCreated" in names and "AutoNamedOrderCancelled" in names
