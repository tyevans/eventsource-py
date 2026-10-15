"""Span creation and disabled tracing tests for InMemorySnapshotStore."""

from __future__ import annotations

import pytest

from eventsource.adapters.memory.snapshots import InMemorySnapshotStore
from eventsource.observability import ATTR_AGGREGATE_ID, ATTR_AGGREGATE_TYPE, ATTR_VERSION


class TestInMemorySnapshotStoreSpanCreation:
    """Tests for span creation in InMemorySnapshotStore operations."""

    @pytest.fixture
    def traced_store(self, mock_tracer):
        return InMemorySnapshotStore(tracer=mock_tracer)

    @pytest.mark.asyncio
    async def test_save_creates_span(self, traced_store, mock_tracer, sample_snapshot):
        await traced_store.save_snapshot(sample_snapshot)
        assert "eventsource.snapshot.save" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_save_span_attributes(self, traced_store, mock_tracer, sample_snapshot):
        await traced_store.save_snapshot(sample_snapshot)
        save_spans = [(n, a) for n, a in mock_tracer.spans if n == "eventsource.snapshot.save"]
        assert len(save_spans) > 0
        _, attributes = save_spans[0]
        assert attributes.get(ATTR_AGGREGATE_ID) == str(sample_snapshot.aggregate_id)
        assert attributes.get(ATTR_AGGREGATE_TYPE) == "Order"
        assert attributes.get(ATTR_VERSION) == 100

    @pytest.mark.asyncio
    async def test_get_creates_span(self, traced_store, mock_tracer, sample_snapshot):
        await traced_store.save_snapshot(sample_snapshot)
        mock_tracer.clear()
        await traced_store.get_snapshot(
            sample_snapshot.aggregate_id, sample_snapshot.aggregate_type
        )
        assert "eventsource.snapshot.get" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_get_span_attributes(self, traced_store, mock_tracer, sample_snapshot):
        await traced_store.save_snapshot(sample_snapshot)
        mock_tracer.clear()
        await traced_store.get_snapshot(
            sample_snapshot.aggregate_id, sample_snapshot.aggregate_type
        )
        get_spans = [(n, a) for n, a in mock_tracer.spans if n == "eventsource.snapshot.get"]
        assert len(get_spans) > 0
        _, attributes = get_spans[0]
        assert attributes.get(ATTR_AGGREGATE_ID) == str(sample_snapshot.aggregate_id)
        assert attributes.get(ATTR_AGGREGATE_TYPE) == "Order"

    @pytest.mark.asyncio
    async def test_delete_creates_span(self, traced_store, mock_tracer, sample_snapshot):
        await traced_store.save_snapshot(sample_snapshot)
        mock_tracer.clear()
        await traced_store.delete_snapshot(
            sample_snapshot.aggregate_id, sample_snapshot.aggregate_type
        )
        assert "eventsource.snapshot.delete" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_delete_span_attributes(self, traced_store, mock_tracer, sample_snapshot):
        await traced_store.save_snapshot(sample_snapshot)
        mock_tracer.clear()
        await traced_store.delete_snapshot(
            sample_snapshot.aggregate_id, sample_snapshot.aggregate_type
        )
        delete_spans = [(n, a) for n, a in mock_tracer.spans if n == "eventsource.snapshot.delete"]
        assert len(delete_spans) > 0
        _, attributes = delete_spans[0]
        assert attributes.get(ATTR_AGGREGATE_ID) == str(sample_snapshot.aggregate_id)
        assert attributes.get(ATTR_AGGREGATE_TYPE) == "Order"

    @pytest.mark.asyncio
    async def test_exists_creates_span(self, traced_store, mock_tracer, sample_snapshot):
        await traced_store.snapshot_exists(
            sample_snapshot.aggregate_id, sample_snapshot.aggregate_type
        )
        assert "eventsource.snapshot.exists" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_exists_span_attributes(self, traced_store, mock_tracer, sample_snapshot):
        await traced_store.snapshot_exists(
            sample_snapshot.aggregate_id, sample_snapshot.aggregate_type
        )
        exists_spans = [(n, a) for n, a in mock_tracer.spans if n == "eventsource.snapshot.exists"]
        assert len(exists_spans) > 0
        _, attributes = exists_spans[0]
        assert attributes.get(ATTR_AGGREGATE_ID) == str(sample_snapshot.aggregate_id)
        assert attributes.get(ATTR_AGGREGATE_TYPE) == "Order"

    @pytest.mark.asyncio
    async def test_delete_by_type_creates_span(self, traced_store, mock_tracer):
        await traced_store.delete_snapshots_by_type("Order")
        assert "eventsource.snapshot.delete_by_type" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_delete_by_type_span_attributes(self, traced_store, mock_tracer):
        await traced_store.delete_snapshots_by_type("Order")
        delete_spans = [
            (n, a) for n, a in mock_tracer.spans if n == "eventsource.snapshot.delete_by_type"
        ]
        assert len(delete_spans) > 0
        _, attributes = delete_spans[0]
        assert attributes.get(ATTR_AGGREGATE_TYPE) == "Order"

    @pytest.mark.asyncio
    async def test_clear_creates_span(self, traced_store, mock_tracer):
        await traced_store.clear()
        assert "eventsource.snapshot.clear" in mock_tracer.span_names


class TestInMemorySnapshotStoreTracingDisabled:
    """Tests for InMemorySnapshotStore behavior when tracing is disabled."""

    @pytest.mark.asyncio
    async def test_save_works_without_tracing(self, sample_snapshot):
        store = InMemorySnapshotStore(enable_tracing=False)
        await store.save_snapshot(sample_snapshot)
        loaded = await store.get_snapshot(
            sample_snapshot.aggregate_id, sample_snapshot.aggregate_type
        )
        assert loaded == sample_snapshot

    @pytest.mark.asyncio
    async def test_get_works_without_tracing(self, sample_snapshot):
        store = InMemorySnapshotStore(enable_tracing=False)
        await store.save_snapshot(sample_snapshot)
        loaded = await store.get_snapshot(
            sample_snapshot.aggregate_id, sample_snapshot.aggregate_type
        )
        assert loaded == sample_snapshot

    @pytest.mark.asyncio
    async def test_delete_works_without_tracing(self, sample_snapshot):
        store = InMemorySnapshotStore(enable_tracing=False)
        await store.save_snapshot(sample_snapshot)
        result = await store.delete_snapshot(
            sample_snapshot.aggregate_id, sample_snapshot.aggregate_type
        )
        assert result is True
        assert (
            await store.get_snapshot(sample_snapshot.aggregate_id, sample_snapshot.aggregate_type)
            is None
        )

    @pytest.mark.asyncio
    async def test_exists_works_without_tracing(self, sample_snapshot):
        store = InMemorySnapshotStore(enable_tracing=False)
        assert (
            await store.snapshot_exists(
                sample_snapshot.aggregate_id, sample_snapshot.aggregate_type
            )
            is False
        )
        await store.save_snapshot(sample_snapshot)
        assert (
            await store.snapshot_exists(
                sample_snapshot.aggregate_id, sample_snapshot.aggregate_type
            )
            is True
        )

    @pytest.mark.asyncio
    async def test_delete_by_type_works_without_tracing(self, sample_snapshot):
        store = InMemorySnapshotStore(enable_tracing=False)
        await store.save_snapshot(sample_snapshot)
        assert await store.delete_snapshots_by_type("Order") == 1

    @pytest.mark.asyncio
    async def test_clear_works_without_tracing(self, sample_snapshot):
        store = InMemorySnapshotStore(enable_tracing=False)
        await store.save_snapshot(sample_snapshot)
        await store.clear()
        assert store.snapshot_count == 0
