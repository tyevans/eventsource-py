"""OpenTelemetry tracing tests for PostgreSQLSnapshotStore."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from eventsource.adapters.postgresql.snapshots import PostgreSQLSnapshotStore
from eventsource.observability import MockTracer, NullTracer
from eventsource.ports.snapshots import Snapshot

from .test_postgresql_snapshots_create import (
    create_mock_result,
    create_scalar_result,
    create_snapshot_row,
)


class TestOpenTelemetryTracing:
    """Tests for optional OpenTelemetry tracing using Tracer composition."""

    @pytest.mark.asyncio
    async def test_save_creates_span_when_tracing_enabled(
        self,
        mock_session_factory: MagicMock,
        mock_session: AsyncMock,
        sample_snapshot: Snapshot,
    ) -> None:
        """Test that save_snapshot creates a span when tracing is enabled."""
        mock_tracer = MockTracer()

        store = PostgreSQLSnapshotStore(
            session_factory=mock_session_factory,
            tracer=mock_tracer,
        )
        mock_session.execute.return_value = create_mock_result()

        await store.save_snapshot(sample_snapshot)

        assert "eventsource.snapshot.save" in mock_tracer.span_names
        # Find the save span and check attributes
        save_spans = [(n, a) for n, a in mock_tracer.spans if n == "eventsource.snapshot.save"]
        assert len(save_spans) > 0
        _, attributes = save_spans[0]
        assert "eventsource.aggregate.id" in attributes

    @pytest.mark.asyncio
    async def test_get_creates_span_when_tracing_enabled(
        self,
        mock_session_factory: MagicMock,
        mock_session: AsyncMock,
        sample_snapshot: Snapshot,
    ) -> None:
        """Test that get_snapshot creates a span when tracing is enabled."""
        mock_tracer = MockTracer()

        store = PostgreSQLSnapshotStore(
            session_factory=mock_session_factory,
            tracer=mock_tracer,
        )
        row = create_snapshot_row(sample_snapshot)
        mock_session.execute.return_value = create_mock_result([row])

        await store.get_snapshot(
            sample_snapshot.aggregate_id,
            sample_snapshot.aggregate_type,
        )

        assert "eventsource.snapshot.get" in mock_tracer.span_names
        # Find the get span and check attributes
        get_spans = [(n, a) for n, a in mock_tracer.spans if n == "eventsource.snapshot.get"]
        assert len(get_spans) > 0
        _, attributes = get_spans[0]
        assert "eventsource.aggregate.id" in attributes

    @pytest.mark.asyncio
    async def test_delete_creates_span_when_tracing_enabled(
        self,
        mock_session_factory: MagicMock,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test that delete_snapshot creates a span when tracing is enabled."""
        mock_tracer = MockTracer()

        store = PostgreSQLSnapshotStore(
            session_factory=mock_session_factory,
            tracer=mock_tracer,
        )
        mock_session.execute.return_value = create_mock_result(rowcount=1)

        await store.delete_snapshot(aggregate_id, "TestAggregate")

        assert "eventsource.snapshot.delete" in mock_tracer.span_names
        # Find the delete span and check attributes
        delete_spans = [(n, a) for n, a in mock_tracer.spans if n == "eventsource.snapshot.delete"]
        assert len(delete_spans) > 0
        _, attributes = delete_spans[0]
        assert "eventsource.aggregate.id" in attributes

    @pytest.mark.asyncio
    async def test_exists_creates_span_when_tracing_enabled(
        self,
        mock_session_factory: MagicMock,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test that snapshot_exists creates a span when tracing is enabled."""
        mock_tracer = MockTracer()

        store = PostgreSQLSnapshotStore(
            session_factory=mock_session_factory,
            tracer=mock_tracer,
        )
        mock_session.execute.return_value = create_scalar_result(True)

        await store.snapshot_exists(aggregate_id, "TestAggregate")

        assert "eventsource.snapshot.exists" in mock_tracer.span_names
        # Find the exists span and check attributes
        exists_spans = [(n, a) for n, a in mock_tracer.spans if n == "eventsource.snapshot.exists"]
        assert len(exists_spans) > 0
        _, attributes = exists_spans[0]
        assert "eventsource.aggregate.id" in attributes

    @pytest.mark.asyncio
    async def test_delete_by_type_creates_span_when_tracing_enabled(
        self,
        mock_session_factory: MagicMock,
        mock_session: AsyncMock,
    ) -> None:
        """Test that delete_snapshots_by_type creates a span when tracing is enabled."""
        mock_tracer = MockTracer()

        store = PostgreSQLSnapshotStore(
            session_factory=mock_session_factory,
            tracer=mock_tracer,
        )
        mock_session.execute.return_value = create_mock_result(rowcount=5)

        await store.delete_snapshots_by_type("TestAggregate", schema_version_below=2)

        assert "eventsource.snapshot.delete_by_type" in mock_tracer.span_names
        # Find the delete_by_type span and check attributes
        delete_spans = [
            (n, a) for n, a in mock_tracer.spans if n == "eventsource.snapshot.delete_by_type"
        ]
        assert len(delete_spans) > 0
        _, attributes = delete_spans[0]
        assert "eventsource.aggregate.type" in attributes

    def test_no_span_created_when_tracing_disabled(
        self,
        store: PostgreSQLSnapshotStore,
    ) -> None:
        """Test that no tracing occurs when tracing is disabled."""
        # Store was created with enable_tracing=False
        assert store._enable_tracing is False
        assert isinstance(store._tracer, NullTracer)
