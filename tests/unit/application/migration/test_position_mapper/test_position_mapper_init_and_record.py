"""Unit tests for PositionMapper initialization, recording, and helper methods."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from eventsource.application.migration.exceptions import PositionMappingError
from eventsource.application.migration.position_mapper import PositionMapper
from eventsource.ports.migration.models import PositionMapping

from ._fixtures import PositionMapperTestBase, pos


class TestPositionMapperInit:
    def test_init_with_repository(self) -> None:
        mock_repo = MagicMock()
        mapper = PositionMapper(mock_repo)
        assert mapper._repo == mock_repo

    def test_init_with_tracing_enabled(self) -> None:
        mock_repo = MagicMock()
        mapper = PositionMapper(mock_repo, enable_tracing=True)
        assert mapper._repo == mock_repo

    def test_init_with_tracing_disabled(self) -> None:
        mock_repo = MagicMock()
        mapper = PositionMapper(mock_repo, enable_tracing=False)
        assert mapper._repo == mock_repo


class TestPositionMapperRecordMapping:
    @pytest.fixture
    def mock_repo(self) -> MagicMock:
        repo = MagicMock()
        repo.create = AsyncMock(return_value=1)
        return repo

    @pytest.fixture
    def mapper(self, mock_repo: MagicMock) -> PositionMapper:
        return PositionMapper(mock_repo, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_record_mapping_creates_mapping(
        self, mapper: PositionMapper, mock_repo: MagicMock
    ) -> None:
        migration_id, event_id = uuid4(), uuid4()
        source_position, target_position = pos(1000, "src"), pos(500, "tgt")

        await mapper.record_mapping(
            migration_id=migration_id,
            source_position=source_position,
            target_position=target_position,
            event_id=event_id,
        )

        mock_repo.create.assert_called_once()
        call_arg = mock_repo.create.call_args[0][0]
        assert isinstance(call_arg, PositionMapping)
        assert call_arg.migration_id == migration_id
        assert call_arg.source_position == source_position
        assert call_arg.target_position == target_position
        assert call_arg.event_id == event_id

    @pytest.mark.asyncio
    async def test_record_mapping_with_custom_timestamp(
        self, mapper: PositionMapper, mock_repo: MagicMock
    ) -> None:
        migration_id, event_id = uuid4(), uuid4()
        custom_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)

        await mapper.record_mapping(
            migration_id=migration_id,
            source_position=pos(1000, "src"),
            target_position=pos(500, "tgt"),
            event_id=event_id,
            mapped_at=custom_time,
        )

        call_arg = mock_repo.create.call_args[0][0]
        assert call_arg.mapped_at == custom_time

    @pytest.mark.asyncio
    async def test_record_mapping_raises_on_error(
        self, mapper: PositionMapper, mock_repo: MagicMock
    ) -> None:
        migration_id = uuid4()
        mock_repo.create.side_effect = Exception("Database error")
        source_position = pos(1000, "src")

        with pytest.raises(PositionMappingError) as exc_info:
            await mapper.record_mapping(
                migration_id=migration_id,
                source_position=source_position,
                target_position=pos(500, "tgt"),
                event_id=uuid4(),
            )

        assert exc_info.value.migration_id == migration_id
        assert exc_info.value.source_position == source_position
        assert "Database error" in str(exc_info.value)


class TestPositionMapperRecordMappingsBatch:
    @pytest.fixture
    def mock_repo(self) -> MagicMock:
        repo = MagicMock()
        repo.create_batch = AsyncMock(return_value=5)
        return repo

    @pytest.fixture
    def mapper(self, mock_repo: MagicMock) -> PositionMapper:
        return PositionMapper(mock_repo, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_record_batch_with_empty_list(
        self, mapper: PositionMapper, mock_repo: MagicMock
    ) -> None:
        result = await mapper.record_mappings_batch(uuid4(), [])
        assert result == 0
        mock_repo.create_batch.assert_not_called()

    @pytest.mark.asyncio
    async def test_record_batch_creates_mappings_in_ascending_order(
        self, mapper: PositionMapper, mock_repo: MagicMock
    ) -> None:
        migration_id = uuid4()
        mappings = [
            (pos(100, "src"), pos(50, "tgt"), uuid4()),
            (pos(200, "src"), pos(100, "tgt"), uuid4()),
            (pos(300, "src"), pos(150, "tgt"), uuid4()),
        ]

        mock_repo.create_batch.return_value = 3
        result = await mapper.record_mappings_batch(migration_id, mappings)

        assert result == 3
        mock_repo.create_batch.assert_called_once()
        call_arg = mock_repo.create_batch.call_args[0][0]
        assert len(call_arg) == 3
        assert [m.source_position for m in call_arg] == [
            pos(100, "src"),
            pos(200, "src"),
            pos(300, "src"),
        ]

    @pytest.mark.asyncio
    async def test_record_batch_raises_on_error(
        self, mapper: PositionMapper, mock_repo: MagicMock
    ) -> None:
        migration_id = uuid4()
        mock_repo.create_batch.side_effect = Exception("Batch error")

        with pytest.raises(PositionMappingError) as exc_info:
            await mapper.record_mappings_batch(
                migration_id, [(pos(100, "src"), pos(50, "tgt"), uuid4())]
            )

        assert exc_info.value.migration_id == migration_id
        assert "Batch error" in str(exc_info.value)


class TestPositionMapperHelperMethods(PositionMapperTestBase):
    @pytest.mark.asyncio
    async def test_get_mapping_by_event_id(
        self, mapper: PositionMapper, mock_repo: MagicMock
    ) -> None:
        migration_id, event_id, now = uuid4(), uuid4(), datetime.now(UTC)
        expected_mapping = PositionMapping(
            migration_id=migration_id,
            source_position=pos(1000, "src"),
            target_position=pos(500, "tgt"),
            event_id=event_id,
            mapped_at=now,
        )
        mock_repo.find_by_event_id = AsyncMock(return_value=expected_mapping)

        result = await mapper.get_mapping_by_event_id(migration_id, event_id)
        assert result == expected_mapping
        mock_repo.find_by_event_id.assert_called_once_with(migration_id, event_id)

    @pytest.mark.asyncio
    async def test_get_position_bounds(self, mapper: PositionMapper, mock_repo: MagicMock) -> None:
        migration_id = uuid4()
        bounds = (pos(100, "src"), pos(10000, "src"))
        mock_repo.get_position_bounds = AsyncMock(return_value=bounds)

        result = await mapper.get_position_bounds(migration_id)
        assert result == bounds
        mock_repo.get_position_bounds.assert_called_once_with(migration_id)

    @pytest.mark.asyncio
    async def test_get_position_bounds_returns_none(
        self, mapper: PositionMapper, mock_repo: MagicMock
    ) -> None:
        migration_id = uuid4()
        mock_repo.get_position_bounds = AsyncMock(return_value=None)

        result = await mapper.get_position_bounds(migration_id)
        assert result is None

    @pytest.mark.asyncio
    async def test_get_mapping_count(self, mapper: PositionMapper, mock_repo: MagicMock) -> None:
        migration_id = uuid4()
        mock_repo.count_by_migration = AsyncMock(return_value=1500)

        result = await mapper.get_mapping_count(migration_id)
        assert result == 1500
        mock_repo.count_by_migration.assert_called_once_with(migration_id)

    @pytest.mark.asyncio
    async def test_clear_mappings(self, mapper: PositionMapper, mock_repo: MagicMock) -> None:
        migration_id = uuid4()
        mock_repo.delete_by_migration = AsyncMock(return_value=500)

        result = await mapper.clear_mappings(migration_id)
        assert result == 500
        mock_repo.delete_by_migration.assert_called_once_with(migration_id)
