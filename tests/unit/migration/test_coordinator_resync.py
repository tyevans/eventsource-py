"""The shared copier seam: position-mapper wiring and in-phase resync.

`_build_copier` is the single construction site for `BulkCopier`, used by
the automated bulk-copy path and by the operator-triggered
`run_resync_pass`, so the two can never diverge on wiring.
"""

from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.migration.coordinator import MigrationCoordinator
from eventsource.migration.dual_write import DualWriteInterceptor
from eventsource.migration.exceptions import (
    MigrationError,
    MigrationNotFoundError,
    MigrationStateError,
)
from eventsource.migration.models import Migration, MigrationConfig, MigrationPhase
from eventsource.ports import ExpectedVersion


@pytest.fixture
def coordinator_deps() -> dict:
    return {
        "source_store": AsyncMock(),
        "migration_repo": AsyncMock(),
        "routing_repo": AsyncMock(),
        "router": MagicMock(),
    }


def _migration(phase: MigrationPhase, config: MigrationConfig | None = None) -> Migration:
    return Migration(
        id=uuid4(),
        tenant_id=uuid4(),
        source_store_id="default",
        target_store_id="dedicated",
        phase=phase,
        config=config or MigrationConfig(),
    )


async def _record_one_mirror_failure(
    interceptor: DualWriteInterceptor, stream: StreamId, event: DomainEvent
) -> None:
    """Force exactly one mirror failure through `interceptor`, then restore it.

    Swaps `interceptor._target` for a stand-in whose `append` raises, drives
    a single append through the interceptor so the failure is recorded via
    `DualWriteInterceptor.append`'s normal best-effort-target-write path,
    then restores the real target.
    """
    real_target = interceptor._target
    failing_target = AsyncMock()
    failing_target.append = AsyncMock(side_effect=Exception("mirror unavailable"))
    interceptor._target = failing_target
    try:
        await interceptor.append(stream, [event], ExpectedVersion.any_())
    finally:
        interceptor._target = real_target


class TestBuildCopierWiring:
    def test_mapper_is_wired_when_the_config_enables_mapping(self, coordinator_deps: dict) -> None:
        mapper = AsyncMock()
        coordinator = MigrationCoordinator(
            **coordinator_deps, position_mapper=mapper, enable_tracing=False
        )
        migration = _migration(MigrationPhase.BULK_COPY)

        copier = coordinator._build_copier(migration, AsyncMock())

        assert copier._position_mapper is mapper

    def test_mapper_is_withheld_when_mapping_is_disabled(self, coordinator_deps: dict) -> None:
        coordinator = MigrationCoordinator(
            **coordinator_deps, position_mapper=AsyncMock(), enable_tracing=False
        )
        migration = _migration(
            MigrationPhase.BULK_COPY,
            MigrationConfig(position_mapping_enabled=False),
        )

        copier = coordinator._build_copier(migration, AsyncMock())

        assert copier._position_mapper is None

    def test_no_mapper_on_the_coordinator_means_no_mapper_on_the_copier(
        self, coordinator_deps: dict
    ) -> None:
        coordinator = MigrationCoordinator(**coordinator_deps, enable_tracing=False)

        copier = coordinator._build_copier(_migration(MigrationPhase.BULK_COPY), AsyncMock())

        assert copier._position_mapper is None

    def test_target_store_is_resolved_from_the_registry_when_omitted(
        self, coordinator_deps: dict
    ) -> None:
        coordinator = MigrationCoordinator(**coordinator_deps, enable_tracing=False)
        migration = _migration(MigrationPhase.DUAL_WRITE)
        target = AsyncMock()
        coordinator._target_stores[migration.id] = target

        copier = coordinator._build_copier(migration)

        assert copier._target is target

    def test_missing_target_store_raises(self, coordinator_deps: dict) -> None:
        coordinator = MigrationCoordinator(**coordinator_deps, enable_tracing=False)

        with pytest.raises(MigrationError):
            coordinator._build_copier(_migration(MigrationPhase.DUAL_WRITE))


class TestRunResyncPassGuards:
    async def test_unknown_migration_raises(self, coordinator_deps: dict) -> None:
        coordinator_deps["migration_repo"].get = AsyncMock(return_value=None)
        coordinator = MigrationCoordinator(**coordinator_deps, enable_tracing=False)

        with pytest.raises(MigrationNotFoundError):
            await coordinator.run_resync_pass(uuid4())

    async def test_wrong_phase_raises(self, coordinator_deps: dict) -> None:
        migration = _migration(MigrationPhase.BULK_COPY)
        coordinator_deps["migration_repo"].get = AsyncMock(return_value=migration)
        coordinator = MigrationCoordinator(**coordinator_deps, enable_tracing=False)

        with pytest.raises(MigrationStateError):
            await coordinator.run_resync_pass(migration.id)

    async def test_active_copier_raises(self, coordinator_deps: dict) -> None:
        migration = _migration(MigrationPhase.DUAL_WRITE)
        coordinator_deps["migration_repo"].get = AsyncMock(return_value=migration)
        coordinator = MigrationCoordinator(**coordinator_deps, enable_tracing=False)
        coordinator._target_stores[migration.id] = AsyncMock()
        coordinator._active_copiers[migration.id] = MagicMock()

        with pytest.raises(MigrationError):
            await coordinator.run_resync_pass(migration.id)


class TestDefaultPathRecordsMappings:
    async def _run(self, coordinator_deps: dict, config: MigrationConfig) -> AsyncMock:
        from eventsource.adapters.memory.store import InMemoryEventStore
        from eventsource.domain import StreamId
        from eventsource.ports import ExpectedVersion
        from eventsource.testing.conformance_ports._fixtures import ConformanceEvent

        migration = _migration(MigrationPhase.BULK_COPY, config)

        source = InMemoryEventStore()
        target = InMemoryEventStore()
        aggregate_id = uuid4()
        stream = StreamId(aggregate_id=aggregate_id, category="Conformance")
        # The bulk copier counts and reads events scoped to the migration's
        # tenant (`FeedReadOptions(tenant_id=...)`), so the events must
        # carry a matching `tenant_id` or the copy pass sees nothing.
        await source.append(
            stream,
            [
                ConformanceEvent(aggregate_id=aggregate_id, tenant_id=migration.tenant_id)
                for _ in range(3)
            ],
            ExpectedVersion.no_stream(),
        )

        coordinator_deps["source_store"] = source
        coordinator_deps["migration_repo"].get = AsyncMock(return_value=migration)
        coordinator_deps["migration_repo"].set_events_total = AsyncMock()
        coordinator_deps["migration_repo"].update_progress = AsyncMock()
        coordinator_deps["migration_repo"].record_error = AsyncMock()

        mapper = AsyncMock()
        mapper.record_mapping = AsyncMock()
        coordinator = MigrationCoordinator(
            **coordinator_deps, position_mapper=mapper, enable_tracing=False
        )

        await coordinator._run_bulk_copy(migration, target)
        return mapper

    async def test_default_config_records_a_mapping_per_copied_event(
        self, coordinator_deps: dict
    ) -> None:
        mapper = await self._run(coordinator_deps, MigrationConfig())

        assert mapper.record_mapping.await_count == 3

    async def test_disabling_the_flag_records_nothing(self, coordinator_deps: dict) -> None:
        mapper = await self._run(coordinator_deps, MigrationConfig(position_mapping_enabled=False))

        assert mapper.record_mapping.await_count == 0


class TestResyncThenStrictCutover:
    async def test_a_clamped_anchor_is_recovered_by_a_resync_pass(
        self, coordinator_deps: dict
    ) -> None:
        from eventsource.adapters.memory.store import InMemoryEventStore
        from eventsource.domain import StreamId
        from eventsource.ports import ExpectedVersion
        from eventsource.testing.conformance_ports._fixtures import ConformanceEvent

        migration = _migration(MigrationPhase.DUAL_WRITE)

        source = InMemoryEventStore()
        target = InMemoryEventStore()
        aggregate_id = uuid4()
        stream = StreamId(aggregate_id=aggregate_id, category="Conformance")
        # The bulk copier scopes its feed read to the migration's tenant
        # (`FeedReadOptions(tenant_id=...)`), so events must carry a
        # matching `tenant_id` or the resync pass sees nothing to copy.
        await source.append(
            stream,
            [ConformanceEvent(aggregate_id=aggregate_id, tenant_id=migration.tenant_id)],
            ExpectedVersion.no_stream(),
        )

        async def _update_progress(
            migration_id: UUID,
            events_copied: int,
            last_source_position: object,
            last_target_position: object,
        ) -> None:
            # The real repository persists the checkpoint; here the same
            # `migration` object stands in for that store, so mutate it
            # directly -- `get()` below always returns this instance.
            migration.last_source_position = last_source_position  # type: ignore[assignment]

        coordinator_deps["source_store"] = source
        coordinator_deps["migration_repo"].get = AsyncMock(return_value=migration)
        coordinator_deps["migration_repo"].set_events_total = AsyncMock()
        coordinator_deps["migration_repo"].update_progress = AsyncMock(side_effect=_update_progress)
        coordinator_deps["migration_repo"].record_error = AsyncMock()

        coordinator = MigrationCoordinator(**coordinator_deps, enable_tracing=False)
        coordinator._target_stores[migration.id] = target
        interceptor = coordinator._install_interceptor(migration, target)

        # Force one mirror failure, then let the mirror recover.
        await _record_one_mirror_failure(
            interceptor,
            stream,
            ConformanceEvent(aggregate_id=aggregate_id, tenant_id=migration.tenant_id),
        )

        # The anchor is clamped: cutover would refuse (correctly -- the
        # data really is missing), and before run_resync_pass existed the
        # only exit was abort-and-restart.
        assert interceptor.safe_lag_anchor(migration.last_source_position) != (
            await source.current_position()
        )

        remaining = await coordinator.run_resync_pass(migration.id)

        assert remaining == 0
        # With the clamp released the anchor tracks the checkpoint again.
        assert coordinator._lag_anchor(migration) == migration.last_source_position
