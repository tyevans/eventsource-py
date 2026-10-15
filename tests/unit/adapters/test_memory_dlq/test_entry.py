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

    @pytest.mark.asyncio
    async def test_memory_repositories_support_len_and_o1_dlq_id_index(
        self, repo: InMemoryDLQRepository
    ) -> None:
        from datetime import UTC, datetime

        from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
        from eventsource.adapters.memory.outbox import InMemoryOutboxRepository
        from eventsource.adapters.memory.snapshots import InMemorySnapshotStore
        from eventsource.domain.event import DomainEvent
        from eventsource.ports.snapshots import Snapshot

        class SampleEvent(DomainEvent):
            aggregate_type: str = "Sample"

        assert len(repo) == 0
        await repo.add_failed_event(uuid4(), "Proj", "SampleEvent", {}, Exception("boom"))
        assert len(repo) == 1
        entry = (await repo.get_failed_events())[0]
        await repo.mark_retrying(entry.id)
        retrying = await repo.get_failed_event_by_id(entry.id)
        assert retrying is not None and retrying.status == "retrying"
        await repo.mark_resolved(entry.id, "tester")
        resolved = await repo.get_failed_event_by_id(entry.id)
        assert resolved is not None and resolved.status == "resolved"
        await repo.clear()
        assert len(repo) == 0
        assert await repo.get_failed_event_by_id(entry.id) is None

        cp_repo = InMemoryCheckpointRepository(enable_tracing=False)
        assert len(cp_repo) == 0
        await cp_repo.update_checkpoint("Proj", uuid4(), "SampleEvent")
        assert len(cp_repo) == 1

        outbox = InMemoryOutboxRepository(enable_tracing=False)
        assert len(outbox) == 0
        await outbox.add_event(SampleEvent(aggregate_id=uuid4()))
        assert len(outbox) == 1

        snaps = InMemorySnapshotStore(enable_tracing=False)
        assert len(snaps) == 0
        await snaps.save_snapshot(
            Snapshot(
                aggregate_id=uuid4(),
                aggregate_type="Sample",
                version=1,
                state={"ok": True},
                schema_version=1,
                created_at=datetime.now(UTC),
            )
        )
        assert len(snaps) == 1
