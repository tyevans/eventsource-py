"""Unit tests for BulkCopier.run() execution and progress reporting."""

from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from eventsource.application.migration.bulk_copier import BulkCopier, BulkCopyProgress
from eventsource.application.migration.exceptions import BulkCopyError
from eventsource.application.migration.metrics import clear_metrics_registry, get_migration_metrics
from eventsource.domain import StreamId
from eventsource.domain.exceptions import DuplicateEventError
from eventsource.ports import AppendResult, EventEnvelope
from eventsource.ports.migration.models import Migration, MigrationConfig
from tests.unit.application.migration.test_bulk_copier._fixtures import (
    _pos,
    create_migration,
    create_test_events,
    empty_event_generator,
    make_append_result,
    make_event_generator,
)


class TestBulkCopierRun:
    @pytest.fixture
    def migration(self) -> Migration:
        return create_migration(batch_size=10, max_bulk_copy_rate=10000)

    @pytest.fixture
    def source_store(self) -> AsyncMock:
        return AsyncMock()

    @pytest.fixture
    def target_store(self) -> AsyncMock:
        return AsyncMock()

    @pytest.fixture
    def migration_repo(self) -> AsyncMock:
        return AsyncMock()

    @pytest.fixture
    def copier(self, source_store, target_store, migration_repo) -> BulkCopier:
        return BulkCopier(source_store, target_store, migration_repo, enable_tracing=False)

    def _create_events(
        self, count: int, tenant_id: UUID, aggregate_id: UUID | None = None, start: int = 1
    ) -> list[EventEnvelope]:
        return create_test_events(count, tenant_id, aggregate_id, start, "source")

    def _setup_stores(self, source_store, target_store, events: list[EventEnvelope]) -> None:
        source_store.read_all = make_event_generator(events)
        target_store.get_stream_version = AsyncMock(return_value=0)
        n = len(events)
        target_store.append = AsyncMock(return_value=make_append_result(events[0].stream_id, n, n))

    @pytest.mark.asyncio
    async def test_run_with_no_events(self, migration, source_store, copier) -> None:
        source_store.read_all = empty_event_generator
        updates = [p async for p in copier.run(migration)]
        assert len(updates) >= 1
        assert (updates[-1].is_complete, updates[-1].events_copied) == (True, 0)

    @pytest.mark.asyncio
    async def test_run_with_events_single_batch(
        self, migration, source_store, target_store, copier
    ) -> None:
        self._setup_stores(source_store, target_store, self._create_events(5, migration.tenant_id))
        updates = [p async for p in copier.run(migration)]
        assert len(updates) >= 1
        assert (updates[-1].is_complete, updates[-1].events_copied) == (True, 5)

    @pytest.mark.asyncio
    async def test_run_records_events_copied_metric(
        self, migration, source_store, target_store, copier
    ) -> None:
        clear_metrics_registry()
        self._setup_stores(source_store, target_store, self._create_events(5, migration.tenant_id))
        _ = [p async for p in copier.run(migration)]
        metrics = get_migration_metrics(str(migration.id), str(migration.tenant_id))
        assert metrics.get_snapshot().events_copied == 5
        clear_metrics_registry()

    @pytest.mark.asyncio
    async def test_run_with_events_multiple_batches(
        self, migration, source_store, target_store, copier
    ) -> None:
        events = self._create_events(25, migration.tenant_id)
        source_store.read_all = make_event_generator(events)
        target_store.get_stream_version = AsyncMock(return_value=0)
        stream = events[0].stream_id
        target_store.append = AsyncMock(
            side_effect=[make_append_result(stream, v, v) for v in (10, 20, 25)]
        )
        migration.config = MigrationConfig(batch_size=10, max_bulk_copy_rate=100000)
        updates = [p async for p in copier.run(migration)]
        assert len(updates) >= 2
        assert (updates[-1].is_complete, updates[-1].events_copied) == (True, 25)

    @pytest.mark.asyncio
    async def test_run_with_progress_callback(
        self, migration, source_store, target_store, copier
    ) -> None:
        self._setup_stores(source_store, target_store, self._create_events(5, migration.tenant_id))
        cb_res: list[BulkCopyProgress] = []
        _ = [p async for p in copier.run(migration, progress_callback=cb_res.append)]
        assert len(cb_res) >= 1

    @pytest.mark.asyncio
    async def test_run_respects_cancellation(
        self, migration, source_store, target_store, copier
    ) -> None:
        events = self._create_events(100, migration.tenant_id)
        source_store.read_all = make_event_generator(events)
        target_store.get_stream_version = AsyncMock(return_value=0)
        target_store.append = AsyncMock(
            return_value=make_append_result(events[0].stream_id, 10, 10)
        )
        migration.config = MigrationConfig(batch_size=10, max_bulk_copy_rate=100000)
        updates = []
        async for progress in copier.run(migration):
            updates.append(progress)
            if len(updates) == 1:
                copier.cancel()
        assert updates[-1].is_complete is False and updates[-1].events_copied < 100

    @pytest.mark.asyncio
    async def test_run_resumes_from_checkpoint(self, source_store, target_store, copier) -> None:
        tenant_id = uuid4()
        events = self._create_events(25, tenant_id, start=11)

        async def event_generator(from_position, options):
            for event in events:
                if from_position is None or event.position > from_position:
                    yield event

        source_store.read_all = event_generator
        target_store.get_stream_version = AsyncMock(return_value=10)
        target_store.append = AsyncMock(
            return_value=make_append_result(events[0].stream_id, 35, 35)
        )
        migration = create_migration(
            tenant_id=tenant_id,
            events_total=35,
            events_copied=10,
            last_source_position=_pos("source", 10),
            last_target_position=_pos("target", 5),
            batch_size=100,
            max_bulk_copy_rate=100000,
        )
        updates = [p async for p in copier.run(migration)]
        assert updates[-1].events_copied == 35

    @pytest.mark.asyncio
    async def test_run_handles_error(
        self, migration, source_store, target_store, migration_repo, copier
    ) -> None:
        source_store.read_all = make_event_generator(self._create_events(5, migration.tenant_id))
        target_store.get_stream_version = AsyncMock(return_value=0)
        target_store.append = AsyncMock(side_effect=Exception("Connection failed"))
        with pytest.raises(BulkCopyError) as exc_info:
            _ = [p async for p in copier.run(migration)]
        assert exc_info.value.migration_id == migration.id
        assert "Connection failed" in exc_info.value.original_error
        migration_repo.record_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_counts_events_when_total_not_set(
        self, source_store, target_store, migration_repo, copier
    ) -> None:
        tenant_id = uuid4()
        self._setup_stores(source_store, target_store, self._create_events(15, tenant_id))
        migration = create_migration(
            tenant_id=tenant_id, events_total=0, batch_size=100, max_bulk_copy_rate=100000
        )
        _ = [p async for p in copier.run(migration)]
        migration_repo.set_events_total.assert_called_once_with(migration.id, 15)


class TestBulkCopierRunProgress:
    @pytest.mark.asyncio
    async def test_all_duplicate_batch_keeps_earlier_target_position(self) -> None:
        tenant_id, migration_id = uuid4(), uuid4()
        agg_ids = [uuid4(), uuid4()]
        streams = [StreamId(aggregate_id=a, category="TestAggregate") for a in agg_ids]
        events = [
            create_test_events(1, tenant_id, agg_ids[i], start=i + 1, stream_id=streams[i])[0]
            for i in range(2)
        ]
        source_store = MagicMock(read_all=make_event_generator(events))
        side_effects = [
            AppendResult(stream=streams[0], new_version=1, position=_pos("target", 4)),
            DuplicateEventError("already present"),
            DuplicateEventError("already present"),
        ]
        target_store = AsyncMock(
            get_stream_version=AsyncMock(return_value=0),
            append=AsyncMock(side_effect=side_effects),
        )
        migration = create_migration(
            id=migration_id, tenant_id=tenant_id, events_total=2, batch_size=1
        )
        copier = BulkCopier(source_store, target_store, AsyncMock(), enable_tracing=False)
        progresses = [p async for p in copier.run(migration)]
        assert progresses[-1].last_target_position == _pos("target", 4)
