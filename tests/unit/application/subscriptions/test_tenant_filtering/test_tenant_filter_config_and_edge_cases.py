"""Unit tests for EventFilter from_config, repr, backward compatibility, and edge cases with tenant_id."""

from uuid import UUID, uuid4

from eventsource.application.subscriptions.config import SubscriptionConfig
from eventsource.application.subscriptions.filtering import EventFilter
from tests.unit.application.subscriptions.test_tenant_filtering._fixtures import (
    MockOrderSubscriber,
    OrderCreated,
    OrderShipped,
    PaymentReceived,
)


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

        assert filter.matches(order_created_tenant_1) is True
        assert filter.matches(order_created_tenant_2) is True
        assert filter.matches(order_created_no_tenant) is True


class TestEdgeCases:
    """Tests for edge cases in tenant filtering."""

    def test_tenant_id_none_matches_event_tenant_none(self):
        """Test that filter tenant_id=None matches event tenant_id=None."""
        filter = EventFilter(tenant_id=None)
        event = OrderCreated(aggregate_id=uuid4(), tenant_id=None)

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
