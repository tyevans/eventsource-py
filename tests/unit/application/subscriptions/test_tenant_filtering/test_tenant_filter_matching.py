"""Unit tests for SubscriptionConfig tenant_id and EventFilter tenant matching."""

from uuid import UUID

import pytest

from eventsource.application.subscriptions.config import SubscriptionConfig
from eventsource.application.subscriptions.filtering import EventFilter
from tests.unit.application.subscriptions.test_tenant_filtering._fixtures import (
    OrderCreated,
    OrderShipped,
    PaymentReceived,
)


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

        assert filter.matches(order_created_tenant_1) is True
        assert filter.matches(order_shipped_tenant_1) is False
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

        assert filter.matches(order_created_tenant_1) is True
        assert filter.matches(payment_received_tenant_1) is False
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

        assert filter.matches(order_created_tenant_1) is True
        assert filter.matches(order_shipped_tenant_1) is False
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

        assert filter.matches(order_created_tenant_1) is True
        assert filter.matches(order_created_tenant_2) is False
        assert filter.matches(payment_received_tenant_1) is False
