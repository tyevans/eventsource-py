"""The shared copier seam: position-mapper wiring and in-phase resync."""

from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.migration.coordinator import MigrationCoordinator
from eventsource.application.migration.dual_write import DualWriteInterceptor
from eventsource.application.migration.exceptions import (
    MigrationError,
    MigrationNotFoundError,
    MigrationStateError,
)
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.ports import ExpectedVersion
from eventsource.ports.migration.models import Migration, MigrationConfig, MigrationPhase
from eventsource.testing.conformance_ports._fixtures import (
    ConformanceEvent,
    make_conformance_registry,
)


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
    real_target = interceptor._target
    interceptor._target = AsyncMock(append=AsyncMock(side_effect=Exception("mirror unavailable")))
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
        copier = coordinator._build_copier(_migration(MigrationPhase.BULK_COPY), AsyncMock())
        assert copier._position_mapper is mapper

    def test_mapper_is_withheld_when_mapping_is_disabled(self, coordinator_deps: dict) -> None:
        coordinator = MigrationCoordinator(
            **coordinator_deps, position_mapper=AsyncMock(), enable_tracing=False
        )
        migration = _migration(
            MigrationPhase.BULK_COPY, MigrationConfig(position_mapping_enabled=False)
        )
        assert coordinator._build_copier(migration, AsyncMock())._position_mapper is None

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
        migration, target = _migration(MigrationPhase.DUAL_WRITE), AsyncMock()
        coordinator._target_stores[migration.id] = target
        assert coordinator._build_copier(migration)._target is target

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
        migration = _migration(MigrationPhase.BULK_COPY, config)
        source = InMemoryEventStore(event_registry=make_conformance_registry())
        target = InMemoryEventStore(event_registry=make_conformance_registry())
        agg_id = uuid4()
        stream = StreamId(aggregate_id=agg_id, category="Conformance")
        evts = [
            ConformanceEvent(aggregate_id=agg_id, tenant_id=migration.tenant_id) for _ in (1, 2, 3)
        ]
        await source.append(stream, evts, ExpectedVersion.no_stream())
        coordinator_deps["source_store"] = source
        repo = coordinator_deps["migration_repo"]
        repo.get = AsyncMock(return_value=migration)
        repo.set_events_total, repo.update_progress, repo.record_error = (
            AsyncMock(),
            AsyncMock(),
            AsyncMock(),
        )
        mapper = AsyncMock(record_mapping=AsyncMock())
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
        migration = _migration(MigrationPhase.DUAL_WRITE)
        source = InMemoryEventStore(event_registry=make_conformance_registry())
        target = InMemoryEventStore(event_registry=make_conformance_registry())
        agg_id = uuid4()
        stream = StreamId(aggregate_id=agg_id, category="Conformance")
        evt = ConformanceEvent(aggregate_id=agg_id, tenant_id=migration.tenant_id)
        await source.append(stream, [evt], ExpectedVersion.no_stream())

        async def _update_progress(
            migration_id: UUID, events_copied: int, last_src: object, last_tgt: object
        ) -> None:
            migration.last_source_position = last_src  # type: ignore[assignment]

        coordinator_deps["source_store"] = source
        repo = coordinator_deps["migration_repo"]
        repo.get = AsyncMock(return_value=migration)
        repo.set_events_total, repo.record_error = AsyncMock(), AsyncMock()
        repo.update_progress = AsyncMock(side_effect=_update_progress)
        coordinator = MigrationCoordinator(**coordinator_deps, enable_tracing=False)
        coordinator._target_stores[migration.id] = target
        interceptor = coordinator._install_interceptor(migration, target)
        await _record_one_mirror_failure(
            interceptor,
            stream,
            ConformanceEvent(aggregate_id=agg_id, tenant_id=migration.tenant_id),
        )
        assert interceptor.safe_lag_anchor(migration.last_source_position) != (
            await source.current_position()
        )
        assert await coordinator.run_resync_pass(migration.id) == 0
        assert coordinator._lag_anchor(migration) == migration.last_source_position
