"""Tests for FilteringSubscriber base class."""

from uuid import uuid4

import pytest

from eventsource.application.subscriptions.subscriber import FilteringSubscriber
from eventsource.domain.event import DomainEvent
from tests.unit.application.subscriptions.test_subscriber._fixtures import (
    OrderCreated,
    OrderShipped,
)


class TestFilteringSubscriber:
    """Tests for FilteringSubscriber base class."""

    def test_cannot_instantiate_directly(self) -> None:
        """FilteringSubscriber requires subclass implementation."""
        with pytest.raises(TypeError, match="abstract"):
            FilteringSubscriber()  # type: ignore[abstract]

    @pytest.mark.asyncio
    async def test_filters_events_with_should_handle(self) -> None:
        """Events are filtered using should_handle."""
        processed: list[DomainEvent] = []

        class MySubscriber(FilteringSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            def should_handle(self, event: DomainEvent) -> bool:
                if isinstance(event, OrderCreated):
                    return event.order_number.startswith("VIP")
                return False

            async def _process_event(self, event: DomainEvent) -> None:
                processed.append(event)

        subscriber = MySubscriber()

        vip_event = OrderCreated(aggregate_id=uuid4(), order_number="VIP-001")
        regular_event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")

        await subscriber.handle(vip_event)
        await subscriber.handle(regular_event)

        assert len(processed) == 1
        assert processed[0] == vip_event

    @pytest.mark.asyncio
    async def test_default_should_handle_uses_can_handle(self) -> None:
        """Default should_handle delegates to can_handle."""
        processed: list[DomainEvent] = []

        class MySubscriber(FilteringSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            async def _process_event(self, event: DomainEvent) -> None:
                processed.append(event)

        subscriber = MySubscriber()

        created = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        shipped = OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001")

        await subscriber.handle(created)
        await subscriber.handle(shipped)

        # Only OrderCreated should be processed (in subscribed_to)
        assert len(processed) == 1
        assert isinstance(processed[0], OrderCreated)

    @pytest.mark.asyncio
    async def test_handle_batch_applies_filter(self) -> None:
        """handle_batch filters events before processing."""
        processed: list[DomainEvent] = []

        class VIPOnlySubscriber(FilteringSubscriber):
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            def should_handle(self, event: DomainEvent) -> bool:
                if isinstance(event, OrderCreated):
                    return event.order_number.startswith("VIP")
                return False

            async def _process_event(self, event: DomainEvent) -> None:
                processed.append(event)

        subscriber = VIPOnlySubscriber()
        events = [
            OrderCreated(aggregate_id=uuid4(), order_number="VIP-001"),
            OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"),
            OrderCreated(aggregate_id=uuid4(), order_number="VIP-002"),
        ]

        await subscriber.handle_batch(events)

        assert len(processed) == 2
        assert all(
            isinstance(e, OrderCreated) and e.order_number.startswith("VIP") for e in processed
        )

    @pytest.mark.asyncio
    async def test_filter_by_tenant(self) -> None:
        """Demonstrates tenant-based filtering."""
        processed: list[DomainEvent] = []
        target_tenant = uuid4()

        class TenantSubscriber(FilteringSubscriber):
            def __init__(self, tenant_id):
                self.tenant_id = tenant_id

            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [OrderCreated]

            def should_handle(self, event: DomainEvent) -> bool:
                return event.tenant_id == self.tenant_id

            async def _process_event(self, event: DomainEvent) -> None:
                processed.append(event)

        subscriber = TenantSubscriber(target_tenant)

        events = [
            OrderCreated(aggregate_id=uuid4(), order_number="ORD-001", tenant_id=target_tenant),
            OrderCreated(aggregate_id=uuid4(), order_number="ORD-002", tenant_id=uuid4()),
            OrderCreated(aggregate_id=uuid4(), order_number="ORD-003", tenant_id=target_tenant),
        ]

        for event in events:
            await subscriber.handle(event)

        assert len(processed) == 2
        assert all(e.tenant_id == target_tenant for e in processed)
