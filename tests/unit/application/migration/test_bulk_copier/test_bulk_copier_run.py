"""Unit tests for BulkCopier.run() execution and progress reporting."""

from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from eventsource.application.migration.bulk_copier import BulkCopier, BulkCopyProgress
from eventsource.application.migration.exceptions import BulkCopyError
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
    """Tests for BulkCopier.run() method."""

    @pytest.fixture
    def migration(self) -> Migration:
        """Create a sample migration for testing."""
        return create_migration(batch_size=10, max_bulk_copy_rate=10000)

    @pytest.fixture
    def source_store(self) -> AsyncMock:
        """Create a mock source store."""
        return AsyncMock()

    @pytest.fixture
    def target_store(self) -> AsyncMock:
        """Create a mock target store."""
        return AsyncMock()

    @pytest.fixture
    def migration_repo(self) -> AsyncMock:
        """Create a mock migration repository."""
        return AsyncMock()

    def _create_events(
        self,
        count: int,
        tenant_id: UUID,
        aggregate_id: UUID | None = None,
        start: int = 1,
        store_id: str = "source",
    ) -> list[EventEnvelope]:
        """Create a list of EventEnvelope instances for testing."""
        return create_test_events(count, tenant_id, aggregate_id, start, store_id)

    def _make_copier(
        self,
        source_store: AsyncMock,
        target_store: AsyncMock,
        migration_repo: AsyncMock,
    ) -> BulkCopier:
        return BulkCopier(
            source_store=source_store,
            target_store=target_store,
            migration_repo=migration_repo,
            enable_tracing=False,
        )

    @pytest.mark.asyncio
    async def test_run_with_no_events(
        self,
        migration: Migration,
        source_store: AsyncMock,
        target_store: AsyncMock,
        migration_repo: AsyncMock,
    ) -> None:
        """Test run with no events to copy."""
        source_store.read_all = empty_event_generator
        migration_repo.set_events_total = AsyncMock()
        migration_repo.update_progress = AsyncMock()

        copier = self._make_copier(source_store, target_store, migration_repo)
        progress_updates = [p async for p in copier.run(migration)]

        assert len(progress_updates) >= 1
        final = progress_updates[-1]
        assert final.is_complete is True
        assert final.events_copied == 0

    @pytest.mark.asyncio
    async def test_run_with_events_single_batch(
        self,
        migration: Migration,
        source_store: AsyncMock,
        target_store: AsyncMock,
        migration_repo: AsyncMock,
    ) -> None:
        """Test run with events fitting in single batch."""
        events = self._create_events(5, migration.tenant_id)
        source_store.read_all = make_event_generator(events)
        target_store.get_stream_version = AsyncMock(return_value=0)
        target_store.append = AsyncMock(return_value=make_append_result(events[0].stream_id, 5, 5))
        migration_repo.set_events_total = AsyncMock()
        migration_repo.update_progress = AsyncMock()

        copier = self._make_copier(source_store, target_store, migration_repo)
        progress_updates = [p async for p in copier.run(migration)]

        assert len(progress_updates) >= 1
        final = progress_updates[-1]
        assert final.is_complete is True
        assert final.events_copied == 5

    @pytest.mark.asyncio
    async def test_run_records_events_copied_metric(
        self,
        migration: Migration,
        source_store: AsyncMock,
        target_store: AsyncMock,
        migration_repo: AsyncMock,
    ) -> None:
        """run() reports events copied through the real MigrationMetrics instance."""
        from eventsource.application.migration.metrics import (
            clear_metrics_registry,
            get_migration_metrics,
        )

        clear_metrics_registry()
        events = self._create_events(5, migration.tenant_id)
        source_store.read_all = make_event_generator(events)
        target_store.get_stream_version = AsyncMock(return_value=0)
        target_store.append = AsyncMock(return_value=make_append_result(events[0].stream_id, 5, 5))
        migration_repo.set_events_total = AsyncMock()
        migration_repo.update_progress = AsyncMock()

        copier = self._make_copier(source_store, target_store, migration_repo)
        async for _ in copier.run(migration):
            pass

        metrics = get_migration_metrics(str(migration.id), str(migration.tenant_id))
        snapshot = metrics.get_snapshot()
        assert snapshot.events_copied == 5
        clear_metrics_registry()

    @pytest.mark.asyncio
    async def test_run_with_events_multiple_batches(
        self,
        migration: Migration,
        source_store: AsyncMock,
        target_store: AsyncMock,
        migration_repo: AsyncMock,
    ) -> None:
        """Test run with events spanning multiple batches."""
        events = self._create_events(25, migration.tenant_id)
        source_store.read_all = make_event_generator(events)
        target_store.get_stream_version = AsyncMock(return_value=0)
        stream = events[0].stream_id
        target_store.append = AsyncMock(
            side_effect=[make_append_result(stream, v, v) for v in (10, 20, 25)]
        )
        migration_repo.set_events_total = AsyncMock()
        migration_repo.update_progress = AsyncMock()

        migration.config = MigrationConfig(batch_size=10, max_bulk_copy_rate=100000)
        copier = self._make_copier(source_store, target_store, migration_repo)
        progress_updates = [p async for p in copier.run(migration)]

        assert len(progress_updates) >= 2
        assert progress_updates[-1].is_complete is True
        assert progress_updates[-1].events_copied == 25

    @pytest.mark.asyncio
    async def test_run_with_progress_callback(
        self,
        migration: Migration,
        source_store: AsyncMock,
        target_store: AsyncMock,
        migration_repo: AsyncMock,
    ) -> None:
        """Test run calls progress callback."""
        events = self._create_events(5, migration.tenant_id)
        source_store.read_all = make_event_generator(events)
        target_store.get_stream_version = AsyncMock(return_value=0)
        target_store.append = AsyncMock(return_value=make_append_result(events[0].stream_id, 5, 5))
        migration_repo.set_events_total = AsyncMock()
        migration_repo.update_progress = AsyncMock()

        copier = self._make_copier(source_store, target_store, migration_repo)
        callback_results = []

        def callback(progress: BulkCopyProgress) -> None:
            callback_results.append(progress)

        async for _ in copier.run(migration, progress_callback=callback):
            pass

        assert len(callback_results) >= 1

    @pytest.mark.asyncio
    async def test_run_respects_cancellation(
        self,
        migration: Migration,
        source_store: AsyncMock,
        target_store: AsyncMock,
        migration_repo: AsyncMock,
    ) -> None:
        """Test run stops when cancelled."""
        events = self._create_events(100, migration.tenant_id)
        source_store.read_all = make_event_generator(events)
        target_store.get_stream_version = AsyncMock(return_value=0)
        target_store.append = AsyncMock(
            return_value=make_append_result(events[0].stream_id, 10, 10)
        )
        migration_repo.set_events_total = AsyncMock()
        migration_repo.update_progress = AsyncMock()

        migration.config = MigrationConfig(batch_size=10, max_bulk_copy_rate=100000)
        copier = self._make_copier(source_store, target_store, migration_repo)

        progress_updates = []
        async for progress in copier.run(migration):
            progress_updates.append(progress)
            if len(progress_updates) == 1:
                copier.cancel()

        final = progress_updates[-1]
        assert final.is_complete is False
        assert final.events_copied < 100

    @pytest.mark.asyncio
    async def test_run_resumes_from_checkpoint(
        self,
        source_store: AsyncMock,
        target_store: AsyncMock,
        migration_repo: AsyncMock,
    ) -> None:
        """Test run resumes from last checkpoint."""
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
        migration_repo.set_events_total = AsyncMock()
        migration_repo.update_progress = AsyncMock()

        migration = create_migration(
            tenant_id=tenant_id,
            events_total=35,
            events_copied=10,
            last_source_position=_pos("source", 10),
            last_target_position=_pos("target", 5),
            batch_size=100,
            max_bulk_copy_rate=100000,
        )

        copier = self._make_copier(source_store, target_store, migration_repo)
        progress_updates = [p async for p in copier.run(migration)]

        final = progress_updates[-1]
        assert final.events_copied == 10 + 25

    @pytest.mark.asyncio
    async def test_run_handles_error(
        self,
        migration: Migration,
        source_store: AsyncMock,
        target_store: AsyncMock,
        migration_repo: AsyncMock,
    ) -> None:
        """Test run handles errors and raises BulkCopyError."""
        events = self._create_events(5, migration.tenant_id)
        source_store.read_all = make_event_generator(events)
        target_store.get_stream_version = AsyncMock(return_value=0)
        target_store.append = AsyncMock(side_effect=Exception("Connection failed"))
        migration_repo.set_events_total = AsyncMock()
        migration_repo.record_error = AsyncMock()

        copier = self._make_copier(source_store, target_store, migration_repo)

        with pytest.raises(BulkCopyError) as exc_info:
            async for _ in copier.run(migration):
                pass

        assert exc_info.value.migration_id == migration.id
        assert "Connection failed" in exc_info.value.original_error
        migration_repo.record_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_counts_events_when_total_not_set(
        self,
        source_store: AsyncMock,
        target_store: AsyncMock,
        migration_repo: AsyncMock,
    ) -> None:
        """Test run counts events when total is not set."""
        tenant_id = uuid4()
        events = self._create_events(15, tenant_id)
        source_store.read_all = make_event_generator(events)
        target_store.get_stream_version = AsyncMock(return_value=0)
        target_store.append = AsyncMock(
            return_value=make_append_result(events[0].stream_id, 15, 15)
        )
        migration_repo.set_events_total = AsyncMock()
        migration_repo.update_progress = AsyncMock()

        migration = create_migration(
            tenant_id=tenant_id,
            events_total=0,
            batch_size=100,
            max_bulk_copy_rate=100000,
        )

        copier = self._make_copier(source_store, target_store, migration_repo)
        async for _ in copier.run(migration):
            pass

        migration_repo.set_events_total.assert_called_once_with(migration.id, 15)


class TestBulkCopierRunProgress:
    """Progress reporting across batches."""

    @pytest.mark.asyncio
    async def test_all_duplicate_batch_keeps_earlier_target_position(self) -> None:
        """A later all-duplicate batch must not null a real position."""
        tenant_id = uuid4()
        migration_id = uuid4()
        agg_ids = [uuid4(), uuid4()]
        streams = [StreamId(aggregate_id=a, category="TestAggregate") for a in agg_ids]

        source_store = MagicMock()
        events = [
            create_test_events(1, tenant_id, agg_ids[0], start=1, stream_id=streams[0])[0],
            create_test_events(1, tenant_id, agg_ids[1], start=2, stream_id=streams[1])[0],
        ]
        source_store.read_all = make_event_generator(events)

        target_store = AsyncMock()
        target_store.get_stream_version = AsyncMock(return_value=0)
        target_store.append = AsyncMock(
            side_effect=[
                AppendResult(stream=streams[0], new_version=1, position=_pos("target", 4)),
                DuplicateEventError("already present"),
                DuplicateEventError("already present"),
            ]
        )

        migration_repo = AsyncMock()
        migration = create_migration(
            id=migration_id,
            tenant_id=tenant_id,
            events_total=2,
            batch_size=1,
        )

        copier = BulkCopier(
            source_store=source_store,
            target_store=target_store,
            migration_repo=migration_repo,
            enable_tracing=False,
        )

        progresses = [p async for p in copier.run(migration)]

        assert progresses[-1].last_target_position == _pos("target", 4)
