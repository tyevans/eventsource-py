"""Basic CRUD and upsert tests for PostgreSQLSnapshotStore."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from eventsource.adapters.postgresql.snapshots import PostgreSQLSnapshotStore
from eventsource.domain.exceptions import SnapshotDeserializationError
from eventsource.ports.snapshots import Snapshot

from .test_postgresql_snapshots_create import (
    create_mock_result,
    create_scalar_result,
    create_snapshot_row,
)


class TestPostgreSQLSnapshotStoreBasic:
    """Tests for basic CRUD operations."""

    @pytest.mark.asyncio
    async def test_save_snapshot(
        self,
        store: PostgreSQLSnapshotStore,
        mock_session: AsyncMock,
        sample_snapshot: Snapshot,
    ) -> None:
        mock_session.execute.return_value = create_mock_result()
        await store.save_snapshot(sample_snapshot)
        mock_session.execute.assert_called_once()
        sql_text = str(mock_session.execute.call_args[0][0])
        assert "INSERT INTO snapshots" in sql_text
        assert "ON CONFLICT" in sql_text
        assert "DO UPDATE SET" in sql_text

    @pytest.mark.asyncio
    async def test_save_snapshot_params(
        self,
        store: PostgreSQLSnapshotStore,
        mock_session: AsyncMock,
        sample_snapshot: Snapshot,
    ) -> None:
        mock_session.execute.return_value = create_mock_result()
        await store.save_snapshot(sample_snapshot)
        params = mock_session.execute.call_args[0][1]
        assert params["aggregate_id"] == sample_snapshot.aggregate_id
        assert params["aggregate_type"] == sample_snapshot.aggregate_type
        assert params["version"] == sample_snapshot.version
        assert params["schema_version"] == sample_snapshot.schema_version
        assert params["created_at"] == sample_snapshot.created_at
        assert '"value":"test"' in params["state"] or '"value": "test"' in params["state"]

    @pytest.mark.asyncio
    async def test_async_context_manager(self, store: PostgreSQLSnapshotStore) -> None:
        from eventsource.ports.lifecycle import SupportsAsyncContextManager, SupportsClose

        assert isinstance(store, SupportsClose)
        assert isinstance(store, SupportsAsyncContextManager)
        async with store as entered:
            assert entered is store

    @pytest.mark.asyncio
    async def test_get_snapshot_found(
        self,
        store: PostgreSQLSnapshotStore,
        mock_session: AsyncMock,
        sample_snapshot: Snapshot,
    ) -> None:
        row = create_snapshot_row(sample_snapshot)
        mock_session.execute.return_value = create_mock_result([row])
        result = await store.get_snapshot(
            sample_snapshot.aggregate_id,
            sample_snapshot.aggregate_type,
        )
        assert result is not None
        assert result.aggregate_id == sample_snapshot.aggregate_id
        assert result.aggregate_type == sample_snapshot.aggregate_type
        assert result.version == sample_snapshot.version
        assert result.schema_version == sample_snapshot.schema_version
        assert result.state == sample_snapshot.state

    @pytest.mark.asyncio
    async def test_get_snapshot_not_found(
        self,
        store: PostgreSQLSnapshotStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        mock_result = MagicMock()
        mock_result.fetchone.return_value = None
        mock_session.execute.return_value = mock_result
        result = await store.get_snapshot(aggregate_id, "TestAggregate")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_snapshot_json_string_state(
        self,
        store: PostgreSQLSnapshotStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        row = MagicMock()
        row.aggregate_id = aggregate_id
        row.aggregate_type = "TestAggregate"
        row.version = 5
        row.schema_version = 1
        row.state = '{"value": "from_json", "count": 10}'
        row.created_at = datetime.now(UTC)
        mock_session.execute.return_value = create_mock_result([row])
        result = await store.get_snapshot(aggregate_id, "TestAggregate")
        assert result is not None
        assert result.state == {"value": "from_json", "count": 10}

    @pytest.mark.asyncio
    async def test_get_snapshot_raises_deserialization_error_on_malformed_state(
        self,
        store: PostgreSQLSnapshotStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        row = MagicMock()
        row.aggregate_id = aggregate_id
        row.aggregate_type = "TestAggregate"
        row.version = 5
        row.schema_version = 1
        row.state = "{not valid json"
        row.created_at = datetime.now(UTC)
        mock_session.execute.return_value = create_mock_result([row])
        with pytest.raises(SnapshotDeserializationError) as exc_info:
            await store.get_snapshot(aggregate_id, "TestAggregate")
        assert exc_info.value.aggregate_id == aggregate_id
        assert exc_info.value.aggregate_type == "TestAggregate"

    @pytest.mark.asyncio
    async def test_delete_snapshot_exists(
        self,
        store: PostgreSQLSnapshotStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        mock_session.execute.return_value = create_mock_result(rowcount=1)
        result = await store.delete_snapshot(aggregate_id, "TestAggregate")
        assert result is True
        mock_session.execute.assert_called_once()
        sql_text = str(mock_session.execute.call_args[0][0])
        assert "DELETE FROM snapshots" in sql_text

    @pytest.mark.asyncio
    async def test_delete_snapshot_not_exists(
        self,
        store: PostgreSQLSnapshotStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        mock_session.execute.return_value = create_mock_result(rowcount=0)
        result = await store.delete_snapshot(aggregate_id, "TestAggregate")
        assert result is False

    @pytest.mark.asyncio
    async def test_snapshot_exists_true(
        self,
        store: PostgreSQLSnapshotStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        mock_session.execute.return_value = create_scalar_result(True)
        result = await store.snapshot_exists(aggregate_id, "TestAggregate")
        assert result is True
        sql_text = str(mock_session.execute.call_args[0][0])
        assert "SELECT EXISTS" in sql_text

    @pytest.mark.asyncio
    async def test_snapshot_exists_false(
        self,
        store: PostgreSQLSnapshotStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        mock_session.execute.return_value = create_scalar_result(False)
        result = await store.snapshot_exists(aggregate_id, "TestAggregate")
        assert result is False


class TestUpsertSemantics:
    """Tests for upsert (INSERT ON CONFLICT) behavior."""

    @pytest.mark.asyncio
    async def test_save_overwrites_existing(
        self,
        store: PostgreSQLSnapshotStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        snapshot1 = Snapshot(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            version=5,
            schema_version=1,
            state={"value": "first"},
            created_at=datetime.now(UTC),
        )
        mock_session.execute.return_value = create_mock_result()
        await store.save_snapshot(snapshot1)

        snapshot2 = Snapshot(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            version=10,
            schema_version=1,
            state={"value": "second"},
            created_at=datetime.now(UTC),
        )
        await store.save_snapshot(snapshot2)

        assert mock_session.execute.call_count == 2
        for call in mock_session.execute.call_args_list:
            sql_text = str(call[0][0])
            assert "ON CONFLICT" in sql_text

    @pytest.mark.asyncio
    async def test_different_aggregate_types_are_separate(
        self,
        store: PostgreSQLSnapshotStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        mock_session.execute.return_value = create_mock_result()
        snapshot1 = Snapshot(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            version=5,
            schema_version=1,
            state={"status": "pending"},
            created_at=datetime.now(UTC),
        )
        await store.save_snapshot(snapshot1)

        snapshot2 = Snapshot(
            aggregate_id=aggregate_id,
            aggregate_type="User",
            version=3,
            schema_version=1,
            state={"name": "test"},
            created_at=datetime.now(UTC),
        )
        await store.save_snapshot(snapshot2)
        assert mock_session.execute.call_count == 2
