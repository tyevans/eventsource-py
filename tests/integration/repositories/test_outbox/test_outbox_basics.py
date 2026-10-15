"""
Integration tests for PostgreSQL Outbox Repository basics, pending retrieval, and event types.

These tests verify actual database operations for the outbox pattern including:
- Adding events to outbox
- Retrieving pending events
- Handling multiple event types
"""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from eventsource import PostgreSQLOutboxRepository
from eventsource.testing.conformance_ports import OutboxRepositoryConformance
from tests.integration.conftest import (
    TestItemCreated,
    TestOrderCreated,
    skip_if_no_postgres_infra,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.postgres,
    skip_if_no_postgres_infra,
]


class TestPostgreSQLOutboxRepositoryBasics(OutboxRepositoryConformance):
    """Basic outbox repository operations."""

    @pytest.fixture
    async def store(
        self, postgres_outbox_repo: PostgreSQLOutboxRepository
    ) -> PostgreSQLOutboxRepository:
        return postgres_outbox_repo

    async def test_add_event_to_outbox(
        self,
        postgres_outbox_repo: PostgreSQLOutboxRepository,
        sample_aggregate_id: UUID,
        sample_tenant_id: UUID,
    ) -> None:
        """Test adding an event to the outbox."""
        event = TestItemCreated(
            aggregate_id=sample_aggregate_id,
            tenant_id=sample_tenant_id,
            aggregate_version=1,
            name="Test Item",
            quantity=10,
        )

        outbox_id = await postgres_outbox_repo.add_event(event)

        assert outbox_id is not None
        assert isinstance(outbox_id, UUID)

    async def test_add_event_without_tenant(
        self,
        postgres_outbox_repo: PostgreSQLOutboxRepository,
        sample_aggregate_id: UUID,
    ) -> None:
        """Test adding an event without tenant ID."""
        event = TestItemCreated(
            aggregate_id=sample_aggregate_id,
            tenant_id=None,
            aggregate_version=1,
            name="No Tenant Item",
            quantity=5,
        )

        outbox_id = await postgres_outbox_repo.add_event(event)
        assert outbox_id is not None

        # Verify it can be retrieved
        pending = await postgres_outbox_repo.get_pending_events(limit=10)
        matching = [e for e in pending if e.event_id == event.event_id]
        assert len(matching) == 1
        assert matching[0].tenant_id is None


class TestPostgreSQLOutboxRepositoryPending:
    """Tests for retrieving pending events."""

    async def test_get_pending_events_empty(
        self,
        postgres_outbox_repo: PostgreSQLOutboxRepository,
    ) -> None:
        """Test getting pending events when outbox is empty."""
        pending = await postgres_outbox_repo.get_pending_events()
        assert pending == []

    async def test_get_pending_events_ordering(
        self,
        postgres_outbox_repo: PostgreSQLOutboxRepository,
    ) -> None:
        """Test that pending events are ordered by created_at (oldest first)."""
        # Add multiple events
        event_ids = []
        for i in range(5):
            agg_id = uuid4()
            event = TestItemCreated(
                aggregate_id=agg_id,
                aggregate_version=1,
                name=f"Item {i}",
                quantity=i,
            )
            await postgres_outbox_repo.add_event(event)
            event_ids.append(str(event.event_id))

        # Get pending
        pending = await postgres_outbox_repo.get_pending_events()

        # Should be in creation order (oldest first)
        pending_event_ids = [str(e.event_id) for e in pending]
        assert pending_event_ids == event_ids

    async def test_get_pending_events_with_limit(
        self,
        postgres_outbox_repo: PostgreSQLOutboxRepository,
    ) -> None:
        """Test limiting number of pending events returned."""
        # Add 10 events
        for i in range(10):
            event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name=f"Item {i}",
                quantity=i,
            )
            await postgres_outbox_repo.add_event(event)

        # Get with limit
        pending = await postgres_outbox_repo.get_pending_events(limit=3)
        assert len(pending) == 3

    async def test_pending_event_fields(
        self,
        postgres_outbox_repo: PostgreSQLOutboxRepository,
        sample_aggregate_id: UUID,
        sample_tenant_id: UUID,
    ) -> None:
        """Test that pending events have all expected fields."""
        event = TestItemCreated(
            aggregate_id=sample_aggregate_id,
            tenant_id=sample_tenant_id,
            aggregate_version=1,
            name="Field Test Item",
            quantity=42,
        )

        await postgres_outbox_repo.add_event(event)

        pending = await postgres_outbox_repo.get_pending_events()
        assert len(pending) == 1

        entry = pending[0]
        assert entry.id is not None
        assert entry.event_id == event.event_id
        assert entry.event_type == "TestItemCreated"
        assert entry.aggregate_id == sample_aggregate_id
        assert entry.aggregate_type == "TestItem"
        assert entry.tenant_id == sample_tenant_id
        assert entry.event_data is not None
        assert entry.created_at is not None
        assert entry.retry_count == 0


class TestPostgreSQLOutboxRepositoryMultipleEventTypes:
    """Tests with different event types."""

    async def test_different_event_types(
        self,
        postgres_outbox_repo: PostgreSQLOutboxRepository,
        sample_customer_id: UUID,
    ) -> None:
        """Test outbox handles different event types correctly."""
        # Add different event types
        item_event = TestItemCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            name="Test Item",
            quantity=5,
        )
        order_event = TestOrderCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            customer_id=sample_customer_id,
            total_amount=100.0,
        )

        await postgres_outbox_repo.add_event(item_event)
        await postgres_outbox_repo.add_event(order_event)

        pending = await postgres_outbox_repo.get_pending_events()

        assert len(pending) == 2

        event_types = {e.event_type for e in pending}
        assert "TestItemCreated" in event_types
        assert "TestOrderCreated" in event_types

        aggregate_types = {e.aggregate_type for e in pending}
        assert "TestItem" in aggregate_types
        assert "TestOrder" in aggregate_types
