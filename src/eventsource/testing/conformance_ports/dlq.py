"""Conformance suite for the `DLQRepository` port.

Subclass and provide a `store` fixture yielding a fresh adapter instance.
Time-sensitive cleanup semantics that differ per backend (the memory
adapter truncates its cutoff to midnight; the SQL adapter does not) are
intentionally excluded here -- see the backend-specific test modules.
"""

from abc import ABC, abstractmethod
from uuid import uuid4

import pytest

from eventsource.ports.dlq import DLQRepository


class DLQRepositoryConformance(ABC):
    """Conformance suite for `DLQRepository` implementations."""

    @abstractmethod
    @pytest.fixture
    def store(self) -> object:
        """Yield a fresh adapter instance implementing `DLQRepository`."""
        raise NotImplementedError

    async def test_add_then_list_round_trips_fields(self, store: DLQRepository) -> None:
        event_id = uuid4()
        await store.add_failed_event(
            event_id=event_id,
            projection_name="P",
            event_type="Created",
            event_data={"x": 1},
            error=RuntimeError("boom"),
        )

        (entry,) = await store.get_failed_events()

        assert entry.event_id == event_id
        assert entry.projection_name == "P"
        assert entry.event_type == "Created"
        assert entry.error_message == "boom"
        assert entry.retry_count == 0

    async def test_second_add_for_same_key_upserts_rather_than_duplicates(
        self, store: DLQRepository
    ) -> None:
        event_id = uuid4()
        await store.add_failed_event(
            event_id=event_id,
            projection_name="P",
            event_type="Created",
            event_data={},
            error=RuntimeError("first"),
        )
        first_entries = await store.get_failed_events()
        first_failed_at = first_entries[0].first_failed_at

        await store.add_failed_event(
            event_id=event_id,
            projection_name="P",
            event_type="Created",
            event_data={},
            error=RuntimeError("second"),
            retry_count=1,
        )

        entries = await store.get_failed_events()
        assert len(entries) == 1
        assert entries[0].retry_count == 1
        assert entries[0].first_failed_at == first_failed_at

    async def test_same_event_id_different_projection_creates_a_second_entry(
        self, store: DLQRepository
    ) -> None:
        event_id = uuid4()
        await store.add_failed_event(
            event_id=event_id,
            projection_name="A",
            event_type="Created",
            event_data={},
            error=RuntimeError("boom"),
        )
        await store.add_failed_event(
            event_id=event_id,
            projection_name="B",
            event_type="Created",
            event_data={},
            error=RuntimeError("boom"),
        )

        entries = await store.get_failed_events()
        assert len(entries) == 2

    async def test_get_failed_events_filters_by_status(self, store: DLQRepository) -> None:
        event_id = uuid4()
        await store.add_failed_event(
            event_id=event_id,
            projection_name="P",
            event_type="Created",
            event_data={},
            error=RuntimeError("boom"),
        )
        (entry,) = await store.get_failed_events()
        await store.mark_retrying(entry.id)

        assert await store.get_failed_events(status="failed") == []
        retrying = await store.get_failed_events(status="retrying")
        assert len(retrying) == 1
        assert retrying[0].id == entry.id

    async def test_get_failed_events_filters_by_projection_name(self, store: DLQRepository) -> None:
        await store.add_failed_event(
            event_id=uuid4(),
            projection_name="A",
            event_type="Created",
            event_data={},
            error=RuntimeError("boom"),
        )
        await store.add_failed_event(
            event_id=uuid4(),
            projection_name="B",
            event_type="Created",
            event_data={},
            error=RuntimeError("boom"),
        )

        entries = await store.get_failed_events(projection_name="A")
        assert len(entries) == 1
        assert entries[0].projection_name == "A"

    async def test_get_failed_events_respects_limit(self, store: DLQRepository) -> None:
        for _ in range(3):
            await store.add_failed_event(
                event_id=uuid4(),
                projection_name="P",
                event_type="Created",
                event_data={},
                error=RuntimeError("boom"),
            )

        entries = await store.get_failed_events(limit=2)
        assert len(entries) == 2

    async def test_mark_retrying_moves_entry_between_status_filters(
        self, store: DLQRepository
    ) -> None:
        await store.add_failed_event(
            event_id=uuid4(),
            projection_name="P",
            event_type="Created",
            event_data={},
            error=RuntimeError("boom"),
        )
        (entry,) = await store.get_failed_events()

        await store.mark_retrying(entry.id)

        assert await store.get_failed_events(status="failed") == []
        (retrying,) = await store.get_failed_events(status="retrying")
        assert retrying.status == "retrying"

    async def test_mark_resolved_sets_status_and_resolved_by(self, store: DLQRepository) -> None:
        await store.add_failed_event(
            event_id=uuid4(),
            projection_name="P",
            event_type="Created",
            event_data={},
            error=RuntimeError("boom"),
        )
        (entry,) = await store.get_failed_events()

        await store.mark_resolved(entry.id, "alice")

        resolved = await store.get_failed_event_by_id(entry.id)
        assert resolved is not None
        assert resolved.status == "resolved"
        assert resolved.resolved_by == "alice"

    async def test_get_failure_stats_counts_failed_and_retrying_separately(
        self, store: DLQRepository
    ) -> None:
        await store.add_failed_event(
            event_id=uuid4(),
            projection_name="A",
            event_type="Created",
            event_data={},
            error=RuntimeError("boom"),
        )
        await store.add_failed_event(
            event_id=uuid4(),
            projection_name="B",
            event_type="Created",
            event_data={},
            error=RuntimeError("boom"),
        )
        (retrying_entry,) = [e for e in await store.get_failed_events() if e.projection_name == "B"]
        await store.mark_retrying(retrying_entry.id)

        stats = await store.get_failure_stats()

        assert stats.total_failed == 1
        assert stats.total_retrying == 1
        assert stats.affected_projections == 2

    async def test_get_projection_failure_counts_orders_by_count_descending(
        self, store: DLQRepository
    ) -> None:
        for _ in range(2):
            await store.add_failed_event(
                event_id=uuid4(),
                projection_name="Busy",
                event_type="Created",
                event_data={},
                error=RuntimeError("boom"),
            )
        await store.add_failed_event(
            event_id=uuid4(),
            projection_name="Quiet",
            event_type="Created",
            event_data={},
            error=RuntimeError("boom"),
        )

        counts = await store.get_projection_failure_counts()

        assert [c.projection_name for c in counts] == ["Busy", "Quiet"]
        assert counts[0].failure_count == 2
        assert counts[1].failure_count == 1

    async def test_delete_resolved_events_leaves_failed_entries_intact(
        self, store: DLQRepository
    ) -> None:
        await store.add_failed_event(
            event_id=uuid4(),
            projection_name="P",
            event_type="Created",
            event_data={},
            error=RuntimeError("boom"),
        )

        deleted = await store.delete_resolved_events(older_than_days=0)

        assert deleted == 0
        assert len(await store.get_failed_events()) == 1
