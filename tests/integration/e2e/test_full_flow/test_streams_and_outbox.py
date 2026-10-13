"""Integration tests for event stream operations, projection rebuilding, and outbox."""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID, uuid4

import pytest

from eventsource import AggregateRepository
from eventsource.adapters.postgresql import PostgreSQLEventStore
from eventsource.domain import StreamId
from eventsource.ports import ExpectedVersion, StreamReadOptions
from tests.integration.conftest import (
    TestOrderAggregate,
    TestOrderCreated,
    skip_if_no_postgres_infra,
)

from .test_order_projection import TestOrderProjection

if TYPE_CHECKING:
    pass


pytestmark = [
    pytest.mark.integration,
    pytest.mark.postgres,
    pytest.mark.e2e,
    skip_if_no_postgres_infra,
]


class TestEventStreamOperations:
    """Tests for event stream operations."""

    async def test_read_events_from_position(
        self,
        postgres_event_store: PostgreSQLEventStore,
        sample_customer_id: UUID,
    ) -> None:
        """Test reading events from a specific position."""
        repo = AggregateRepository(
            event_store=postgres_event_store,
            aggregate_factory=TestOrderAggregate,
        )

        # Create order with multiple events
        order_id = uuid4()
        order = TestOrderAggregate(order_id)
        order.create_order(customer_id=sample_customer_id)
        await repo.save(order)

        order = await repo.load(order_id)
        for i in range(3):
            order.add_item(uuid4(), f"Item {i}", 1, 10.0)
        await repo.save(order)

        # Read all events
        stream_id = StreamId(aggregate_id=order_id, category="TestOrder")
        events = [e async for e in postgres_event_store.read_stream(stream_id)]
        assert len(events) == 4  # 1 created + 3 items

        # Read from version 2 (legacy "skip first 2" -> ports inclusive from_version=3)
        events_partial = [
            e
            async for e in postgres_event_store.read_stream(
                stream_id, StreamReadOptions(from_version=3)
            )
        ]
        assert len(events_partial) == 2  # Events 3 and 4

    async def test_read_all_events_global(
        self,
        postgres_event_store: PostgreSQLEventStore,
        sample_customer_id: UUID,
    ) -> None:
        """Test reading all events across aggregates."""
        # Create multiple aggregates with events
        for _ in range(3):
            order_id = uuid4()
            event = TestOrderCreated(
                aggregate_id=order_id,
                aggregate_version=1,
                customer_id=sample_customer_id,
                total_amount=0.0,
            )
            await postgres_event_store.append(
                StreamId(aggregate_id=order_id, category="TestOrder"),
                [event],
                ExpectedVersion.no_stream(),
            )

        # Read all events
        all_events = []
        async for stored_event in postgres_event_store.read_all():
            all_events.append(stored_event)

        assert len(all_events) >= 3

        # Verify global ordering
        positions = [e.position for e in all_events]
        assert positions == sorted(positions)


class TestProjectionRebuilding:
    """Tests for projection rebuilding scenarios."""

    async def test_rebuild_projection_from_events(
        self,
        postgres_event_store: PostgreSQLEventStore,
        sample_customer_id: UUID,
    ) -> None:
        """Test rebuilding a projection by replaying all events."""
        repo = AggregateRepository(
            event_store=postgres_event_store,
            aggregate_factory=TestOrderAggregate,
        )

        # Create some orders without projection
        order_ids = []
        for i in range(3):
            order_id = uuid4()
            order_ids.append(order_id)

            order = TestOrderAggregate(order_id)
            order.create_order(customer_id=sample_customer_id)
            await repo.save(order)

            order = await repo.load(order_id)
            order.add_item(uuid4(), f"Item {i}", i + 1, 10.0)
            await repo.save(order)

        # Now "rebuild" projection by reading all events
        projection = TestOrderProjection()

        async for stored_event in postgres_event_store.read_all():
            await projection.handle(stored_event.event)

        # Verify projection has correct state
        assert len(projection.orders) == 3
        assert projection.events_processed >= 6  # 3 creates + 3 items


class TestOutboxIntegration:
    """Tests for outbox pattern integration."""

    async def test_outbox_enabled_stores_events(
        self,
        postgres_event_store_with_outbox: PostgreSQLEventStore,
        postgres_outbox_repo,
        sample_customer_id: UUID,
    ) -> None:
        """Test that events are stored in outbox when enabled."""
        order_id = uuid4()
        event = TestOrderCreated(
            aggregate_id=order_id,
            aggregate_version=1,
            customer_id=sample_customer_id,
            total_amount=100.0,
        )

        await postgres_event_store_with_outbox.append(
            StreamId(aggregate_id=order_id, category="TestOrder"),
            [event],
            ExpectedVersion.no_stream(),
        )

        # Verify event is in outbox
        pending = await postgres_outbox_repo.get_pending_events()
        matching = [e for e in pending if e.event_id == event.event_id]
        assert len(matching) == 1
        assert matching[0].event_type == "TestOrderCreated"
