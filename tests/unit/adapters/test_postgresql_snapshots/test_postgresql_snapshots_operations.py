"""Bulk delete, properties, imports, and complex state tests for PostgreSQLSnapshotStore."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from eventsource.adapters.postgresql.snapshots import PostgreSQLSnapshotStore
from eventsource.ports.snapshots import Snapshot

from .test_postgresql_snapshots_create import (
    create_mock_result,
)


class TestBulkDelete:
    """Tests for delete_snapshots_by_type."""

    @pytest.mark.asyncio
    async def test_delete_by_type(
        self,
        store: PostgreSQLSnapshotStore,
        mock_session: AsyncMock,
    ) -> None:
        """Test deleting all snapshots of a type."""
        mock_session.execute.return_value = create_mock_result(rowcount=5)

        count = await store.delete_snapshots_by_type("TestAggregate")

        assert count == 5
        sql_text = str(mock_session.execute.call_args[0][0])
        assert "DELETE FROM snapshots" in sql_text
        assert "aggregate_type = :aggregate_type" in sql_text
        assert "schema_version" not in sql_text

    @pytest.mark.asyncio
    async def test_delete_by_type_with_schema_version_filter(
        self,
        store: PostgreSQLSnapshotStore,
        mock_session: AsyncMock,
    ) -> None:
        """Test deleting snapshots by type with schema version filter."""
        mock_session.execute.return_value = create_mock_result(rowcount=3)

        count = await store.delete_snapshots_by_type(
            "TestAggregate",
            schema_version_below=2,
        )

        assert count == 3
        sql_text = str(mock_session.execute.call_args[0][0])
        assert "DELETE FROM snapshots" in sql_text
        assert "schema_version < :schema_version_below" in sql_text

    @pytest.mark.asyncio
    async def test_delete_by_type_no_matches(
        self,
        store: PostgreSQLSnapshotStore,
        mock_session: AsyncMock,
    ) -> None:
        """Test deleting when no snapshots match returns 0."""
        mock_session.execute.return_value = create_mock_result(rowcount=0)

        count = await store.delete_snapshots_by_type("NonExistentType")

        assert count == 0


class TestProperties:
    """Tests for store properties."""

    def test_session_factory_property(
        self,
        store: PostgreSQLSnapshotStore,
        mock_session_factory: MagicMock,
    ) -> None:
        """Test session_factory property returns the factory."""
        assert store.session_factory == mock_session_factory

    def test_repr_without_tracing(
        self,
        store: PostgreSQLSnapshotStore,
    ) -> None:
        """Test __repr__ without tracing."""
        assert "PostgreSQLSnapshotStore" in repr(store)
        assert "tracing=disabled" in repr(store)

    def test_repr_with_tracing(
        self,
        mock_session_factory: MagicMock,
    ) -> None:
        """Test __repr__ with tracing enabled."""
        store = PostgreSQLSnapshotStore(
            session_factory=mock_session_factory,
            enable_tracing=True,
        )
        assert "PostgreSQLSnapshotStore" in repr(store)


class TestImports:
    """Tests for verifying correct module exports."""

    def test_import_from_snapshots_module(self) -> None:
        """Test importing PostgreSQLSnapshotStore from snapshots module."""
        from eventsource.adapters.postgresql.snapshots import PostgreSQLSnapshotStore

        assert PostgreSQLSnapshotStore is not None

    def test_import_from_adapters_package(self) -> None:
        """Test importing PostgreSQLSnapshotStore from the adapters package."""
        from eventsource.adapters.postgresql import PostgreSQLSnapshotStore

        assert PostgreSQLSnapshotStore is not None


class TestComplexState:
    """Tests for handling complex state objects."""

    @pytest.mark.asyncio
    async def test_save_complex_nested_state(
        self,
        store: PostgreSQLSnapshotStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test saving snapshot with deeply nested state."""
        complex_state = {
            "user": {
                "name": "John",
                "address": {
                    "street": "123 Main St",
                    "city": "Test City",
                },
            },
            "items": [
                {"id": 1, "name": "Item 1"},
                {"id": 2, "name": "Item 2"},
            ],
            "metadata": {
                "created_by": "system",
                "tags": ["important", "verified"],
            },
        }
        snapshot = Snapshot(
            aggregate_id=aggregate_id,
            aggregate_type="ComplexAggregate",
            version=1,
            schema_version=1,
            state=complex_state,
            created_at=datetime.now(UTC),
        )
        mock_session.execute.return_value = create_mock_result()

        await store.save_snapshot(snapshot)

        params = mock_session.execute.call_args[0][1]
        decoded = json.loads(params["state"])
        assert decoded == complex_state

    @pytest.mark.asyncio
    async def test_get_complex_nested_state(
        self,
        store: PostgreSQLSnapshotStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test retrieving snapshot with deeply nested state."""
        complex_state = {
            "orders": [{"id": 1, "total": 100.50}, {"id": 2, "total": 200.00}],
            "settings": {"notifications": True},
        }
        row = MagicMock()
        row.aggregate_id = aggregate_id
        row.aggregate_type = "ComplexAggregate"
        row.version = 5
        row.schema_version = 1
        row.state = complex_state  # Dict state
        row.created_at = datetime.now(UTC)

        mock_session.execute.return_value = create_mock_result([row])

        result = await store.get_snapshot(aggregate_id, "ComplexAggregate")

        assert result is not None
        assert result.state == complex_state
        assert result.state["orders"][0]["total"] == 100.50

    @pytest.mark.asyncio
    async def test_empty_state(
        self,
        store: PostgreSQLSnapshotStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test handling empty state dict."""
        snapshot = Snapshot(
            aggregate_id=aggregate_id,
            aggregate_type="EmptyAggregate",
            version=1,
            schema_version=1,
            state={},
            created_at=datetime.now(UTC),
        )
        mock_session.execute.return_value = create_mock_result()

        await store.save_snapshot(snapshot)

        params = mock_session.execute.call_args[0][1]
        assert params["state"] == "{}"
