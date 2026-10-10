"""Tests for DLQEntry typed returns and attribute access."""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource.adapters.memory.dlq import InMemoryDLQRepository
from eventsource.ports.dlq import DLQEntry


class TestDLQEntryTypedReturns:
    """Tests for typed returns and backward compatibility."""

    @pytest.fixture
    def repo(self) -> InMemoryDLQRepository:
        """Create a fresh repository for each test."""
        return InMemoryDLQRepository()

    @pytest.mark.asyncio
    async def test_get_failed_events_returns_dlq_entries(self, repo: InMemoryDLQRepository):
        """get_failed_events returns list of DLQEntry instances."""
        event_id = uuid4()
        await repo.add_failed_event(
            event_id=event_id,
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data={},
            error=Exception("Test error"),
        )

        entries = await repo.get_failed_events()

        assert len(entries) == 1
        assert isinstance(entries[0], DLQEntry)
        assert hasattr(entries[0], "event_id")
        assert hasattr(entries[0], "projection_name")
        assert hasattr(entries[0], "error_message")

    @pytest.mark.asyncio
    async def test_get_failed_event_by_id_returns_dlq_entry(self, repo: InMemoryDLQRepository):
        """get_failed_event_by_id returns DLQEntry instance."""
        event_id = uuid4()
        await repo.add_failed_event(
            event_id=event_id,
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data={},
            error=Exception("Test error"),
        )

        events = await repo.get_failed_events()
        dlq_id = events[0].id

        entry = await repo.get_failed_event_by_id(dlq_id)

        assert entry is not None
        assert isinstance(entry, DLQEntry)
        assert entry.event_id == event_id

    @pytest.mark.asyncio
    async def test_attribute_access_works(self, repo: InMemoryDLQRepository):
        """Can access DLQEntry fields as attributes."""
        event_id = uuid4()
        await repo.add_failed_event(
            event_id=event_id,
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data={},
            error=Exception("Test error"),
        )

        entries = await repo.get_failed_events()
        entry = entries[0]

        assert entry.event_id == event_id
        assert entry.projection_name == "TestProjection"
        assert entry.event_type == "TestEvent"
        assert entry.status == "failed"
        assert "Test error" in entry.error_message
