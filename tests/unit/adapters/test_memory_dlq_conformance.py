"""Conformance tests for InMemoryDLQRepository against the port suites."""

from collections.abc import AsyncIterator
from uuid import uuid4

import pytest

from eventsource.adapters.memory import InMemoryDLQRepository
from eventsource.testing.conformance_ports import DLQRepositoryConformance


class TestMemoryDLQRepository(DLQRepositoryConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[InMemoryDLQRepository]:
        yield InMemoryDLQRepository()

    async def test_memory_delete_resolved_events_cutoff_is_truncated_to_midnight(
        self, store: InMemoryDLQRepository
    ) -> None:
        # The in-memory adapter's cutoff is `today at 00:00 UTC` minus
        # `older_than_days`, not `now` minus `older_than_days` (unlike the
        # SQL adapter). An entry resolved moments ago is *not* past an
        # `older_than_days=0` cutoff on this backend -- it survives.
        await store.add_failed_event(
            event_id=uuid4(),
            projection_name="P",
            event_type="Created",
            event_data={},
            error=RuntimeError("boom"),
        )
        (entry,) = await store.get_failed_events()
        await store.mark_resolved(entry.id, "alice")

        deleted = await store.delete_resolved_events(older_than_days=0)

        assert deleted == 0
        resolved = await store.get_failed_event_by_id(entry.id)
        assert resolved is not None
        assert resolved.status == "resolved"

    async def test_clear_empties_the_repository(self, store: InMemoryDLQRepository) -> None:
        await store.add_failed_event(
            event_id=uuid4(),
            projection_name="P",
            event_type="Created",
            event_data={},
            error=RuntimeError("boom"),
        )
        await store.clear()
        assert await store.get_failed_events() == []
