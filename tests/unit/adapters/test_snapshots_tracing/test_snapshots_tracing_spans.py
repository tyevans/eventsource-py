"""Span creation and disabled tracing tests for InMemorySnapshotStore."""

from __future__ import annotations

import pytest

from eventsource.adapters.memory.snapshots import InMemorySnapshotStore
from eventsource.observability import (
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_VERSION,
)


class TestInMemorySnapshotStoreSpanCreation:
    """Tests for span creation in InMemorySnapshotStore operations."""

    @pytest.fixture
    def traced_store(self, mock_tracer):
        """Create a store with injected mock tracer."""
        store = InMemorySnapshotStore(tracer=mock_tracer)
        return store

    @pytest.mark.asyncio
    async def test_save_creates_span(self, traced_store, mock_tracer, sample_snapshot):
        """save_snapshot creates a span with correct name."""
        await traced_store.save_snapshot(sample_snapshot)

        # Verify span was created with correct name
        assert "eventsource.snapshot.save" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_save_span_attributes(self, traced_store, mock_tracer, sample_snapshot):
        """save_snapshot span includes correct standard attributes."""
        await traced_store.save_snapshot(sample_snapshot)

        # Find the save span and verify attributes
        save_spans = [(n, a) for n, a in mock_tracer.spans if n == "eventsource.snapshot.save"]
        assert len(save_spans) > 0
        _, attributes = save_spans[0]

        assert ATTR_AGGREGATE_ID in attributes
        assert attributes[ATTR_AGGREGATE_ID] == str(sample_snapshot.aggregate_id)
        assert ATTR_AGGREGATE_TYPE in attributes
        assert attributes[ATTR_AGGREGATE_TYPE] == "Order"
        assert ATTR_VERSION in attributes
        assert attributes[ATTR_VERSION] == 100

    @pytest.mark.asyncio
    async def test_get_creates_span(self, traced_store, mock_tracer, sample_snapshot):
        """get_snapshot creates a span with correct name."""
        await traced_store.save_snapshot(sample_snapshot)
        mock_tracer.clear()

        await traced_store.get_snapshot(
            sample_snapshot.aggregate_id,
            sample_snapshot.aggregate_type,
        )

        # Verify span was created with correct name
        assert "eventsource.snapshot.get" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_get_span_attributes(self, traced_store, mock_tracer, sample_snapshot):
        """get_snapshot span includes correct standard attributes."""
        await traced_store.save_snapshot(sample_snapshot)
        mock_tracer.clear()

        await traced_store.get_snapshot(
            sample_snapshot.aggregate_id,
            sample_snapshot.aggregate_type,
        )

        # Find the get span and verify attributes
        get_spans = [(n, a) for n, a in mock_tracer.spans if n == "eventsource.snapshot.get"]
        assert len(get_spans) > 0
        _, attributes = get_spans[0]

        assert ATTR_AGGREGATE_ID in attributes
        assert attributes[ATTR_AGGREGATE_ID] == str(sample_snapshot.aggregate_id)
        assert ATTR_AGGREGATE_TYPE in attributes
        assert attributes[ATTR_AGGREGATE_TYPE] == "Order"

    @pytest.mark.asyncio
    async def test_delete_creates_span(self, traced_store, mock_tracer, sample_snapshot):
        """delete_snapshot creates a span with correct name."""
        await traced_store.save_snapshot(sample_snapshot)
        mock_tracer.clear()

        await traced_store.delete_snapshot(
            sample_snapshot.aggregate_id,
            sample_snapshot.aggregate_type,
        )

        # Verify span was created with correct name
        assert "eventsource.snapshot.delete" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_delete_span_attributes(self, traced_store, mock_tracer, sample_snapshot):
        """delete_snapshot span includes correct standard attributes."""
        await traced_store.save_snapshot(sample_snapshot)
        mock_tracer.clear()

        await traced_store.delete_snapshot(
            sample_snapshot.aggregate_id,
            sample_snapshot.aggregate_type,
        )

        # Find the delete span and verify attributes
        delete_spans = [(n, a) for n, a in mock_tracer.spans if n == "eventsource.snapshot.delete"]
        assert len(delete_spans) > 0
        _, attributes = delete_spans[0]

        assert ATTR_AGGREGATE_ID in attributes
        assert attributes[ATTR_AGGREGATE_ID] == str(sample_snapshot.aggregate_id)
        assert ATTR_AGGREGATE_TYPE in attributes
        assert attributes[ATTR_AGGREGATE_TYPE] == "Order"

    @pytest.mark.asyncio
    async def test_exists_creates_span(self, traced_store, mock_tracer, sample_snapshot):
        """snapshot_exists creates a span with correct name."""
        await traced_store.snapshot_exists(
            sample_snapshot.aggregate_id,
            sample_snapshot.aggregate_type,
        )

        # Verify span was created with correct name
        assert "eventsource.snapshot.exists" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_exists_span_attributes(self, traced_store, mock_tracer, sample_snapshot):
        """snapshot_exists span includes correct standard attributes."""
        await traced_store.snapshot_exists(
            sample_snapshot.aggregate_id,
            sample_snapshot.aggregate_type,
        )

        # Find the exists span and verify attributes
        exists_spans = [(n, a) for n, a in mock_tracer.spans if n == "eventsource.snapshot.exists"]
        assert len(exists_spans) > 0
        _, attributes = exists_spans[0]

        assert ATTR_AGGREGATE_ID in attributes
        assert attributes[ATTR_AGGREGATE_ID] == str(sample_snapshot.aggregate_id)
        assert ATTR_AGGREGATE_TYPE in attributes
        assert attributes[ATTR_AGGREGATE_TYPE] == "Order"

    @pytest.mark.asyncio
    async def test_delete_by_type_creates_span(self, traced_store, mock_tracer):
        """delete_snapshots_by_type creates a span with correct name."""
        await traced_store.delete_snapshots_by_type("Order")

        # Verify span was created with correct name
        assert "eventsource.snapshot.delete_by_type" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_delete_by_type_span_attributes(self, traced_store, mock_tracer):
        """delete_snapshots_by_type span includes correct attributes."""
        await traced_store.delete_snapshots_by_type("Order")

        # Find the delete_by_type span and verify attributes
        delete_spans = [
            (n, a) for n, a in mock_tracer.spans if n == "eventsource.snapshot.delete_by_type"
        ]
        assert len(delete_spans) > 0
        _, attributes = delete_spans[0]

        assert ATTR_AGGREGATE_TYPE in attributes
        assert attributes[ATTR_AGGREGATE_TYPE] == "Order"

    @pytest.mark.asyncio
    async def test_clear_creates_span(self, traced_store, mock_tracer):
        """clear creates a span with correct name."""
        await traced_store.clear()

        # Verify span was created with correct name
        assert "eventsource.snapshot.clear" in mock_tracer.span_names


class TestInMemorySnapshotStoreTracingDisabled:
    """Tests for InMemorySnapshotStore behavior when tracing is disabled."""

    @pytest.mark.asyncio
    async def test_save_works_without_tracing(self, sample_snapshot):
        """save_snapshot works correctly when tracing is disabled."""
        store = InMemorySnapshotStore(enable_tracing=False)

        await store.save_snapshot(sample_snapshot)

        # Should be retrievable
        loaded = await store.get_snapshot(
            sample_snapshot.aggregate_id,
            sample_snapshot.aggregate_type,
        )
        assert loaded == sample_snapshot

    @pytest.mark.asyncio
    async def test_get_works_without_tracing(self, sample_snapshot):
        """get_snapshot works correctly when tracing is disabled."""
        store = InMemorySnapshotStore(enable_tracing=False)
        await store.save_snapshot(sample_snapshot)

        loaded = await store.get_snapshot(
            sample_snapshot.aggregate_id,
            sample_snapshot.aggregate_type,
        )

        assert loaded == sample_snapshot

    @pytest.mark.asyncio
    async def test_delete_works_without_tracing(self, sample_snapshot):
        """delete_snapshot works correctly when tracing is disabled."""
        store = InMemorySnapshotStore(enable_tracing=False)
        await store.save_snapshot(sample_snapshot)

        result = await store.delete_snapshot(
            sample_snapshot.aggregate_id,
            sample_snapshot.aggregate_type,
        )

        assert result is True
        assert (
            await store.get_snapshot(
                sample_snapshot.aggregate_id,
                sample_snapshot.aggregate_type,
            )
            is None
        )

    @pytest.mark.asyncio
    async def test_exists_works_without_tracing(self, sample_snapshot):
        """snapshot_exists works correctly when tracing is disabled."""
        store = InMemorySnapshotStore(enable_tracing=False)

        # Should be False initially
        assert (
            await store.snapshot_exists(
                sample_snapshot.aggregate_id,
                sample_snapshot.aggregate_type,
            )
            is False
        )

        await store.save_snapshot(sample_snapshot)

        # Should be True after saving
        assert (
            await store.snapshot_exists(
                sample_snapshot.aggregate_id,
                sample_snapshot.aggregate_type,
            )
            is True
        )

    @pytest.mark.asyncio
    async def test_delete_by_type_works_without_tracing(self, sample_snapshot):
        """delete_snapshots_by_type works correctly when tracing is disabled."""
        store = InMemorySnapshotStore(enable_tracing=False)
        await store.save_snapshot(sample_snapshot)

        deleted = await store.delete_snapshots_by_type("Order")

        assert deleted == 1

    @pytest.mark.asyncio
    async def test_clear_works_without_tracing(self, sample_snapshot):
        """clear works correctly when tracing is disabled."""
        store = InMemorySnapshotStore(enable_tracing=False)
        await store.save_snapshot(sample_snapshot)

        await store.clear()

        assert store.snapshot_count == 0
