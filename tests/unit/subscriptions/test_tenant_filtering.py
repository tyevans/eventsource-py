"""
Unit tests for tenant filtering functionality in subscriptions.

Tests cover:
- SubscriptionConfig tenant_id field
- EventFilter tenant_id filtering
- Integration with CatchUpRunner ReadOptions
- Combined filter criteria with tenant_id
- Backward compatibility when tenant_id is None
"""

from uuid import UUID, uuid4

import pytest

from eventsource.events.base import DomainEvent
from eventsource.subscriptions.config import SubscriptionConfig
from eventsource.subscriptions.filtering import EventFilter

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


class PaymentReceived(DomainEvent):
    """Payment received event."""

    event_type: str = "PaymentReceived"
    aggregate_type: str = "Payment"
    amount: float = 0.0


# --- Mock Subscriber ---


class MockOrderSubscriber:
    """Subscriber for order events only."""

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderCreated, OrderShipped]

    async def handle(self, event: DomainEvent) -> None:
        pass


# --- Fixtures ---


@pytest.fixture
def tenant_id_1() -> UUID:
    """First test tenant ID."""
    return UUID("11111111-1111-1111-1111-111111111111")


@pytest.fixture
def tenant_id_2() -> UUID:
    """Second test tenant ID."""
    return UUID("22222222-2222-2222-2222-222222222222")


@pytest.fixture
def order_created_tenant_1(tenant_id_1: UUID) -> OrderCreated:
    """Create an OrderCreated event for tenant 1."""
    return OrderCreated(aggregate_id=uuid4(), tenant_id=tenant_id_1)


@pytest.fixture
def order_created_tenant_2(tenant_id_2: UUID) -> OrderCreated:
    """Create an OrderCreated event for tenant 2."""
    return OrderCreated(aggregate_id=uuid4(), tenant_id=tenant_id_2)


@pytest.fixture
def order_created_no_tenant() -> OrderCreated:
    """Create an OrderCreated event with no tenant."""
    return OrderCreated(aggregate_id=uuid4(), tenant_id=None)


@pytest.fixture
def order_shipped_tenant_1(tenant_id_1: UUID) -> OrderShipped:
    """Create an OrderShipped event for tenant 1."""
    return OrderShipped(aggregate_id=uuid4(), tenant_id=tenant_id_1)


@pytest.fixture
def payment_received_tenant_1(tenant_id_1: UUID) -> PaymentReceived:
    """Create a PaymentReceived event for tenant 1."""
    return PaymentReceived(aggregate_id=uuid4(), tenant_id=tenant_id_1)


# --- SubscriptionConfig Tests ---


class TestSubscriptionConfigTenantId:
    """Tests for SubscriptionConfig tenant_id field."""

    def test_default_tenant_id_is_none(self):
        """Test that default tenant_id is None for backward compatibility."""
        config = SubscriptionConfig()
        assert config.tenant_id is None

    def test_tenant_id_can_be_set(self, tenant_id_1: UUID):
        """Test that tenant_id can be set."""
        config = SubscriptionConfig(tenant_id=tenant_id_1)
        assert config.tenant_id == tenant_id_1

    def test_tenant_id_with_other_filtering(self, tenant_id_1: UUID):
        """Test tenant_id works with other filter settings."""
        config = SubscriptionConfig(
            tenant_id=tenant_id_1,
            event_types=(OrderCreated, OrderShipped),
            aggregate_types=("Order",),
        )
        assert config.tenant_id == tenant_id_1
        assert config.event_types == (OrderCreated, OrderShipped)
        assert config.aggregate_types == ("Order",)

    def test_config_is_immutable(self, tenant_id_1: UUID):
        """Test that config is frozen and immutable."""
        config = SubscriptionConfig(tenant_id=tenant_id_1)
        with pytest.raises(AttributeError):
            config.tenant_id = tenant_id_1  # type: ignore


# --- EventFilter Tenant Filtering Tests ---


class TestEventFilterTenantId:
    """Tests for EventFilter tenant_id filtering."""

    def test_no_tenant_filter_matches_all(
        self,
        order_created_tenant_1: OrderCreated,
        order_created_tenant_2: OrderCreated,
        order_created_no_tenant: OrderCreated,
    ):
        """Test that filter without tenant_id matches all events."""
        filter = EventFilter()

        assert filter.matches(order_created_tenant_1) is True
        assert filter.matches(order_created_tenant_2) is True
        assert filter.matches(order_created_no_tenant) is True

    def test_tenant_filter_matches_same_tenant(
        self,
        tenant_id_1: UUID,
        order_created_tenant_1: OrderCreated,
    ):
        """Test that filter matches events from same tenant."""
        filter = EventFilter(tenant_id=tenant_id_1)

        assert filter.matches(order_created_tenant_1) is True

    def test_tenant_filter_rejects_different_tenant(
        self,
        tenant_id_1: UUID,
        order_created_tenant_2: OrderCreated,
    ):
        """Test that filter rejects events from different tenant."""
        filter = EventFilter(tenant_id=tenant_id_1)

        assert filter.matches(order_created_tenant_2) is False

    def test_tenant_filter_rejects_no_tenant(
        self,
        tenant_id_1: UUID,
        order_created_no_tenant: OrderCreated,
    ):
        """Test that filter rejects events with no tenant when tenant_id is set."""
        filter = EventFilter(tenant_id=tenant_id_1)

        assert filter.matches(order_created_no_tenant) is False

    def test_tenant_filter_is_configured(self, tenant_id_1: UUID):
        """Test that is_configured returns True when tenant_id is set."""
        filter = EventFilter(tenant_id=tenant_id_1)

        assert filter.is_configured is True

    def test_tenant_filter_stats_tracking(
        self,
        tenant_id_1: UUID,
        order_created_tenant_1: OrderCreated,
        order_created_tenant_2: OrderCreated,
    ):
        """Test that filter stats track tenant filtering."""
        filter = EventFilter(tenant_id=tenant_id_1)

        filter.matches(order_created_tenant_1)  # Match
        filter.matches(order_created_tenant_2)  # Skip

        assert filter.stats.events_evaluated == 2
        assert filter.stats.events_matched == 1
        assert filter.stats.events_skipped == 1


class TestEventFilterTenantIdCombined:
    """Tests for combined filter criteria with tenant_id."""

    def test_tenant_and_event_type_filtering(
        self,
        tenant_id_1: UUID,
        order_created_tenant_1: OrderCreated,
        order_shipped_tenant_1: OrderShipped,
        order_created_tenant_2: OrderCreated,
    ):
        """Test that both tenant_id and event_types must match."""
        filter = EventFilter(
            tenant_id=tenant_id_1,
            event_types=(OrderCreated,),
        )

        # Matches: tenant 1 + OrderCreated
        assert filter.matches(order_created_tenant_1) is True

        # No match: tenant 1 + OrderShipped (wrong event type)
        assert filter.matches(order_shipped_tenant_1) is False

        # No match: tenant 2 + OrderCreated (wrong tenant)
        assert filter.matches(order_created_tenant_2) is False

    def test_tenant_and_aggregate_type_filtering(
        self,
        tenant_id_1: UUID,
        order_created_tenant_1: OrderCreated,
        payment_received_tenant_1: PaymentReceived,
        order_created_tenant_2: OrderCreated,
    ):
        """Test that both tenant_id and aggregate_types must match."""
        filter = EventFilter(
            tenant_id=tenant_id_1,
            aggregate_types=("Order",),
        )

        # Matches: tenant 1 + Order aggregate
        assert filter.matches(order_created_tenant_1) is True

        # No match: tenant 1 + Payment aggregate (wrong aggregate)
        assert filter.matches(payment_received_tenant_1) is False

        # No match: tenant 2 + Order aggregate (wrong tenant)
        assert filter.matches(order_created_tenant_2) is False

    def test_tenant_and_pattern_filtering(
        self,
        tenant_id_1: UUID,
        order_created_tenant_1: OrderCreated,
        order_shipped_tenant_1: OrderShipped,
        order_created_tenant_2: OrderCreated,
    ):
        """Test that both tenant_id and patterns must match."""
        filter = EventFilter(
            tenant_id=tenant_id_1,
            event_type_patterns=("*Created",),
        )

        # Matches: tenant 1 + *Created pattern
        assert filter.matches(order_created_tenant_1) is True

        # No match: tenant 1 + *Shipped (wrong pattern)
        assert filter.matches(order_shipped_tenant_1) is False

        # No match: tenant 2 + *Created (wrong tenant)
        assert filter.matches(order_created_tenant_2) is False

    def test_all_filters_combined(
        self,
        tenant_id_1: UUID,
        order_created_tenant_1: OrderCreated,
        order_created_tenant_2: OrderCreated,
        payment_received_tenant_1: PaymentReceived,
    ):
        """Test that all filter criteria must match together."""
        filter = EventFilter(
            tenant_id=tenant_id_1,
            event_types=(OrderCreated,),
            aggregate_types=("Order",),
        )

        # Matches all: tenant 1 + OrderCreated + Order aggregate
        assert filter.matches(order_created_tenant_1) is True

        # No match: wrong tenant
        assert filter.matches(order_created_tenant_2) is False

        # No match: wrong event type and aggregate
        assert filter.matches(payment_received_tenant_1) is False


class TestEventFilterFromConfig:
    """Tests for EventFilter creation from SubscriptionConfig with tenant_id."""

    def test_from_config_includes_tenant_id(self, tenant_id_1: UUID):
        """Test that from_config includes tenant_id."""
        config = SubscriptionConfig(tenant_id=tenant_id_1)
        filter = EventFilter.from_config(config)

        assert filter.tenant_id == tenant_id_1

    def test_from_config_without_tenant_id(self):
        """Test that from_config works without tenant_id."""
        config = SubscriptionConfig()
        filter = EventFilter.from_config(config)

        assert filter.tenant_id is None
        assert filter.is_configured is False

    def test_from_config_with_all_filters(self, tenant_id_1: UUID):
        """Test that from_config includes all filter settings."""
        config = SubscriptionConfig(
            tenant_id=tenant_id_1,
            event_types=(OrderCreated,),
            aggregate_types=("Order",),
        )
        filter = EventFilter.from_config(config)

        assert filter.tenant_id == tenant_id_1
        assert filter.event_types == (OrderCreated,)
        assert filter.aggregate_types == ("Order",)

    def test_from_config_and_subscriber_includes_tenant_id(self, tenant_id_1: UUID):
        """Test that from_config_and_subscriber includes tenant_id from config."""
        config = SubscriptionConfig(tenant_id=tenant_id_1)
        subscriber = MockOrderSubscriber()

        filter = EventFilter.from_config_and_subscriber(config, subscriber)

        assert filter.tenant_id == tenant_id_1
        # Should also include event types from subscriber
        assert filter.event_types == (OrderCreated, OrderShipped)


class TestEventFilterRepr:
    """Tests for EventFilter repr with tenant_id."""

    def test_repr_includes_tenant_id(self, tenant_id_1: UUID):
        """Test that repr shows tenant_id when set."""
        filter = EventFilter(tenant_id=tenant_id_1)
        repr_str = repr(filter)

        assert "EventFilter" in repr_str
        assert "tenant_id" in repr_str
        assert str(tenant_id_1) in repr_str

    def test_repr_without_tenant_id(self):
        """Test that repr doesn't show tenant_id when not set."""
        filter = EventFilter()
        repr_str = repr(filter)

        assert "EventFilter(all)" in repr_str
        assert "tenant_id" not in repr_str


# --- Backward Compatibility Tests ---


class TestBackwardCompatibility:
    """Tests for backward compatibility with existing code."""

    def test_existing_config_without_tenant_works(self):
        """Test that existing configs without tenant_id continue to work."""
        config = SubscriptionConfig(
            start_from="beginning",
            batch_size=500,
            event_types=(OrderCreated,),
        )

        assert config.tenant_id is None
        filter = EventFilter.from_config(config)
        assert filter.tenant_id is None

    def test_filter_without_tenant_matches_all_events(
        self,
        order_created_tenant_1: OrderCreated,
        order_created_tenant_2: OrderCreated,
        order_created_no_tenant: OrderCreated,
    ):
        """Test that filter without tenant_id is backward compatible."""
        filter = EventFilter(event_types=(OrderCreated,))

        # Should match all tenants when no tenant_id filter
        assert filter.matches(order_created_tenant_1) is True
        assert filter.matches(order_created_tenant_2) is True
        assert filter.matches(order_created_no_tenant) is True


# --- Edge Cases ---


class TestEdgeCases:
    """Tests for edge cases in tenant filtering."""

    def test_tenant_id_none_matches_event_tenant_none(self):
        """Test that filter tenant_id=None matches event tenant_id=None."""
        filter = EventFilter(tenant_id=None)
        event = OrderCreated(aggregate_id=uuid4(), tenant_id=None)

        # No tenant filter configured, should match
        assert filter.matches(event) is True

    def test_filter_only_tenant_id(
        self,
        tenant_id_1: UUID,
        order_created_tenant_1: OrderCreated,
        order_shipped_tenant_1: OrderShipped,
        payment_received_tenant_1: PaymentReceived,
    ):
        """Test filter with only tenant_id matches all event types for that tenant."""
        filter = EventFilter(tenant_id=tenant_id_1)

        # All events from tenant 1 should match regardless of type
        assert filter.matches(order_created_tenant_1) is True
        assert filter.matches(order_shipped_tenant_1) is True
        assert filter.matches(payment_received_tenant_1) is True

    def test_multiple_tenant_filters_on_different_filters(
        self,
        tenant_id_1: UUID,
        tenant_id_2: UUID,
        order_created_tenant_1: OrderCreated,
        order_created_tenant_2: OrderCreated,
    ):
        """Test using separate filters for different tenants."""
        filter_tenant_1 = EventFilter(tenant_id=tenant_id_1)
        filter_tenant_2 = EventFilter(tenant_id=tenant_id_2)

        # Each filter should only match its tenant
        assert filter_tenant_1.matches(order_created_tenant_1) is True
        assert filter_tenant_1.matches(order_created_tenant_2) is False

        assert filter_tenant_2.matches(order_created_tenant_1) is False
        assert filter_tenant_2.matches(order_created_tenant_2) is True

    def test_stats_reset_clears_tenant_filter_stats(self, tenant_id_1: UUID):
        """Test that reset_stats clears statistics for tenant-filtered events."""
        filter = EventFilter(tenant_id=tenant_id_1)
        event = OrderCreated(aggregate_id=uuid4(), tenant_id=tenant_id_1)

        filter.matches(event)
        assert filter.stats.events_evaluated == 1

        filter.reset_stats()
        assert filter.stats.events_evaluated == 0
        assert filter.stats.events_matched == 0
        assert filter.stats.events_skipped == 0
