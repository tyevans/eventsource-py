"""Integration tests for event type filtering functionality."""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

from eventsource.application.subscriptions import (
    SubscriptionConfig,
    SubscriptionManager,
)
from eventsource.application.subscriptions.filtering import EventFilter
from eventsource.domain import StreamId
from eventsource.ports.positions import ExpectedVersion
from tests.integration.subscriptions.conftest import (
    SubTestOrderCancelled,
    SubTestOrderCreated,
    SubTestOrderShipped,
    populate_event_store_with_types,
    publish_live_event,
)

from .helpers import AdvTestUserRegistered, FilteredProjection

pytestmark = pytest.mark.asyncio


class TestEventTypeFiltering:
    """Tests for event type filtering functionality."""

    async def test_exact_event_type_filtering_in_catchup(
        self, in_memory_event_store, in_memory_event_bus, in_memory_checkpoint_repo
    ):
        await populate_event_store_with_types(
            in_memory_event_store, created_count=20, shipped_count=15, cancelled_count=10
        )
        projection = FilteredProjection(event_types=[SubTestOrderCreated])
        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )
        await manager.subscribe(projection, SubscriptionConfig(start_from="beginning"))
        await manager.start()
        await projection.wait_for_events(20, timeout=10.0)
        await manager.stop()

        assert projection.event_count == 20
        assert all(isinstance(e, SubTestOrderCreated) for e in projection.events)

    async def test_multiple_event_type_filtering(
        self, in_memory_event_store, in_memory_event_bus, in_memory_checkpoint_repo
    ):
        await populate_event_store_with_types(
            in_memory_event_store, created_count=15, shipped_count=15, cancelled_count=15
        )
        projection = FilteredProjection(event_types=[SubTestOrderCreated, SubTestOrderShipped])
        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )
        await manager.subscribe(projection, SubscriptionConfig(start_from="beginning"))
        await manager.start()
        await projection.wait_for_events(30, timeout=10.0)
        await manager.stop()

        assert projection.event_count == 30
        assert all(not isinstance(e, SubTestOrderCancelled) for e in projection.events)

    async def test_config_event_types_overrides_subscriber(
        self, in_memory_event_store, in_memory_event_bus, in_memory_checkpoint_repo
    ):
        await populate_event_store_with_types(
            in_memory_event_store, created_count=20, shipped_count=20, cancelled_count=0
        )
        projection = FilteredProjection(event_types=[SubTestOrderCreated, SubTestOrderShipped])
        config = SubscriptionConfig(start_from="beginning", event_types=(SubTestOrderCreated,))
        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )
        await manager.subscribe(projection, config)
        await manager.start()
        await projection.wait_for_events(20, timeout=10.0)
        await manager.stop()

        assert projection.event_count == 20
        assert all(isinstance(e, SubTestOrderCreated) for e in projection.events)

    async def test_event_filter_in_live_mode(
        self, in_memory_event_store, in_memory_event_bus, in_memory_checkpoint_repo
    ):
        projection = FilteredProjection(event_types=[SubTestOrderCreated])
        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )
        await manager.subscribe(projection, SubscriptionConfig(start_from="beginning"))
        await manager.start()
        await asyncio.sleep(0.2)

        for i in range(10):
            await publish_live_event(in_memory_event_store, in_memory_event_bus, f"LIVE-{i:03d}")

        for i in range(5):
            event = SubTestOrderShipped(aggregate_id=uuid4(), tracking_number=f"TRK-{i:03d}")
            await in_memory_event_store.append(
                StreamId(aggregate_id=event.aggregate_id, category="SubTestOrder"),
                [event],
                ExpectedVersion.no_stream(),
            )
            await in_memory_event_bus.publish([event])

        await projection.wait_for_events(10, timeout=5.0)
        await manager.stop()

        assert projection.event_count == 10
        assert all(isinstance(e, SubTestOrderCreated) for e in projection.events)

    async def test_wildcard_pattern_filtering(
        self, in_memory_event_store, in_memory_event_bus, in_memory_checkpoint_repo
    ):
        filter = EventFilter.from_patterns("SubTestOrder*")
        assert filter.matches(SubTestOrderCreated(aggregate_id=uuid4())) is True
        assert filter.matches(SubTestOrderShipped(aggregate_id=uuid4())) is True
        assert filter.matches(SubTestOrderCancelled(aggregate_id=uuid4())) is True
        assert filter.matches(AdvTestUserRegistered(aggregate_id=uuid4())) is False

    async def test_aggregate_type_filtering(
        self, in_memory_event_store, in_memory_event_bus, in_memory_checkpoint_repo
    ):
        filter = EventFilter(aggregate_types=("SubTestOrder",))
        assert filter.matches(SubTestOrderCreated(aggregate_id=uuid4())) is True
        assert filter.matches(AdvTestUserRegistered(aggregate_id=uuid4())) is False
