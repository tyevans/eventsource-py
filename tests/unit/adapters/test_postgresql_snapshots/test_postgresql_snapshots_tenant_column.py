"""Upgrade safety for snapshots.tenant_id on PostgreSQLSnapshotStore (ADR-0113).

The store probes once per instance whether the column exists. Present: tenant_id
is written and read. Absent (database not yet upgraded with updates/005): the
store omits tenant_id, reads it back as None, and logs one warning.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from eventsource.adapters.postgresql.snapshots import PostgreSQLSnapshotStore
from eventsource.ports.snapshots import Snapshot

from .test_postgresql_snapshots_create import (
    TENANT_PROBE_MARKER,
    create_mock_result,
    create_snapshot_row,
    route_tenant_probe,
)

_PROBE_LOGGER = "eventsource.adapters.postgresql.snapshot_tenant"


def _snapshot(aggregate_id: UUID, tenant_id: UUID | None) -> Snapshot:
    return Snapshot(
        aggregate_id=aggregate_id,
        aggregate_type="TestAggregate",
        version=3,
        schema_version=1,
        state={"value": "x"},
        created_at=datetime.now(UTC),
        tenant_id=tenant_id,
    )


def _sql_calls(mock_session: AsyncMock, marker: str) -> list[object]:
    return [c for c in mock_session.execute.call_args_list if marker in str(c[0][0])]


def _probe_calls(mock_session: AsyncMock) -> list[object]:
    return _sql_calls(mock_session, TENANT_PROBE_MARKER)


class TestColumnPresent:
    @pytest.mark.asyncio
    async def test_save_writes_tenant_id(
        self, store: PostgreSQLSnapshotStore, mock_session: AsyncMock
    ) -> None:
        tenant_id = uuid4()
        mock_session.execute.return_value = create_mock_result()
        await store.save_snapshot(_snapshot(uuid4(), tenant_id))

        upsert = _sql_calls(mock_session, "INSERT INTO snapshots")[0]
        assert "tenant_id" in str(upsert[0][0])
        assert upsert[0][1]["tenant_id"] == tenant_id

    @pytest.mark.asyncio
    async def test_get_reads_tenant_id(
        self, store: PostgreSQLSnapshotStore, mock_session: AsyncMock
    ) -> None:
        aggregate_id = uuid4()
        tenant_id = uuid4()
        snapshot = _snapshot(aggregate_id, tenant_id)
        row = create_snapshot_row(snapshot)
        row.tenant_id = tenant_id
        mock_session.execute.return_value = create_mock_result([row])

        result = await store.get_snapshot(aggregate_id, "TestAggregate")

        assert result is not None
        assert result.tenant_id == tenant_id
        select = _sql_calls(mock_session, "FROM snapshots WHERE")[0]
        assert "NULL AS tenant_id" not in str(select[0][0])

    @pytest.mark.asyncio
    async def test_probe_runs_once_per_instance(
        self, store: PostgreSQLSnapshotStore, mock_session: AsyncMock
    ) -> None:
        mock_session.execute.return_value = create_mock_result()
        await store.save_snapshot(_snapshot(uuid4(), None))
        await store.save_snapshot(_snapshot(uuid4(), None))

        probes = _probe_calls(mock_session)
        assert len(probes) == 1
        probe_sql = str(probes[0][0][0])
        assert "table_name = 'snapshots'" in probe_sql
        assert "column_name = 'tenant_id'" in probe_sql

    @pytest.mark.asyncio
    async def test_no_warning_when_column_present(
        self,
        store: PostgreSQLSnapshotStore,
        mock_session: AsyncMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        mock_session.execute.return_value = create_mock_result()
        with caplog.at_level(logging.WARNING, logger=_PROBE_LOGGER):
            await store.save_snapshot(_snapshot(uuid4(), None))
        assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


class TestColumnAbsent:
    @pytest.mark.asyncio
    async def test_save_omits_tenant_id(
        self, store: PostgreSQLSnapshotStore, mock_session: AsyncMock
    ) -> None:
        route_tenant_probe(mock_session, column_present=False)
        mock_session.execute.return_value = create_mock_result()
        await store.save_snapshot(_snapshot(uuid4(), uuid4()))

        upsert = _sql_calls(mock_session, "INSERT INTO snapshots")[0]
        assert "tenant_id" not in str(upsert[0][0])
        assert "tenant_id" not in upsert[0][1]
        assert upsert[0][1]["version"] == 3

    @pytest.mark.asyncio
    async def test_get_reads_tenant_as_none(
        self, store: PostgreSQLSnapshotStore, mock_session: AsyncMock
    ) -> None:
        route_tenant_probe(mock_session, column_present=False)
        aggregate_id = uuid4()
        row = create_snapshot_row(_snapshot(aggregate_id, None))
        row.tenant_id = None  # what the NULL AS tenant_id alias yields
        mock_session.execute.return_value = create_mock_result([row])

        result = await store.get_snapshot(aggregate_id, "TestAggregate")

        assert result is not None
        assert result.tenant_id is None
        select = _sql_calls(mock_session, "FROM snapshots WHERE")[0]
        assert "NULL AS tenant_id" in str(select[0][0])

    @pytest.mark.asyncio
    async def test_warning_logged_once_per_instance(
        self,
        store: PostgreSQLSnapshotStore,
        mock_session: AsyncMock,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        route_tenant_probe(mock_session, column_present=False)
        aggregate_id = uuid4()
        mock_session.execute.return_value = create_mock_result(
            [create_snapshot_row(_snapshot(aggregate_id, None))]
        )
        with caplog.at_level(logging.WARNING, logger=_PROBE_LOGGER):
            await store.save_snapshot(_snapshot(uuid4(), None))
            await store.save_snapshot(_snapshot(uuid4(), None))
            await store.get_snapshot(aggregate_id, "TestAggregate")

        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert "updates/005_add_snapshot_tenant.sql" in warnings[0].getMessage()
        assert len(_probe_calls(mock_session)) == 1

    @pytest.mark.asyncio
    async def test_save_with_tenant_succeeds_without_column(
        self, store: PostgreSQLSnapshotStore, mock_session: AsyncMock
    ) -> None:
        """The tenant is dropped on write; the save must not fail."""
        route_tenant_probe(mock_session, column_present=False)
        mock_session.execute.return_value = MagicMock()
        await store.save_snapshot(_snapshot(uuid4(), uuid4()))
        assert _sql_calls(mock_session, "INSERT INTO snapshots")
