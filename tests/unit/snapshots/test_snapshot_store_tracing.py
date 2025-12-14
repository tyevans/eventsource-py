"""
Unit tests for SnapshotStore tracing functionality.

O11Y-010: Tests for SnapshotStore Tracer composition integration.

Tests cover:
- Tracer composition for all implementations
- enable_tracing parameter in constructor
- Span creation for save, get, delete, exists methods
- Correct span attributes using standard ATTR_* constants
- Tracing disabled behavior
- Backward compatible constructor (default tracing enabled)
"""

from __future__ import annotations

import inspect
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest

from eventsource.observability import (
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_VERSION,
    MockTracer,
    NullTracer,
)
from eventsource.snapshots import InMemorySnapshotStore, Snapshot

# ============================================================================
# Test Fixtures
# ============================================================================


@pytest.fixture
def sample_snapshot():
    """Create a sample snapshot for testing."""
    return Snapshot(
        aggregate_id=uuid4(),
        aggregate_type="Order",
        version=100,
        state={"status": "shipped", "items": ["item1", "item2"]},
        schema_version=1,
        created_at=datetime.now(UTC),
    )


@pytest.fixture
def mock_tracer():
    """Create a MockTracer for testing span creation."""
    return MockTracer()


# ============================================================================
# InMemorySnapshotStore TracingComposition Tests
# ============================================================================


class TestInMemorySnapshotStoreTracingComposition:
    """Tests for InMemorySnapshotStore Tracer composition integration."""

    def test_uses_tracer_composition(self):
        """InMemorySnapshotStore uses Tracer composition pattern."""
        sig = inspect.signature(InMemorySnapshotStore.__init__)
        params = sig.parameters

        # Should accept tracer parameter for dependency injection
        assert "tracer" in params

    def test_tracing_enabled_by_default(self):
        """Tracing is enabled by default when OTEL is available."""
        store = InMemorySnapshotStore()

        # Check that tracing was initialized
        assert hasattr(store, "_enable_tracing")
        assert hasattr(store, "_tracer")
        assert store._tracer is not None

    def test_tracing_disabled_uses_null_tracer(self):
        """When tracing is disabled, NullTracer is used."""
        store = InMemorySnapshotStore(enable_tracing=False)

        assert store._enable_tracing is False
        assert isinstance(store._tracer, NullTracer)

    def test_custom_tracer_can_be_injected(self):
        """Custom tracer can be injected via constructor."""
        custom_tracer = NullTracer()
        store = InMemorySnapshotStore(tracer=custom_tracer)

        assert store._tracer is custom_tracer

    def test_backward_compatible_constructor(self):
        """Constructor without enable_tracing should work (default True)."""
        store = InMemorySnapshotStore()
        # Should not raise, tracing defaults to enabled
        assert hasattr(store, "_enable_tracing")
        assert hasattr(store, "_tracer")


# ============================================================================
# InMemorySnapshotStore Span Creation Tests
# ============================================================================


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


# ============================================================================
# InMemorySnapshotStore Tracing Disabled Tests
# ============================================================================


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


# ============================================================================
# Standard Attributes Tests
# ============================================================================


class TestSnapshotStoreStandardAttributes:
    """Tests for standard attribute usage in SnapshotStore implementations."""

    def test_inmemory_imports_from_observability_module(self):
        """Verify InMemorySnapshotStore imports tracing from observability module."""
        import subprocess

        result = subprocess.run(
            [
                "grep",
                "-c",
                "from eventsource.observability import",
                "src/eventsource/snapshots/in_memory.py",
            ],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parents[3],
        )
        # Should be at least 1 - imports from observability
        count = int(result.stdout.strip())
        assert count >= 1, "InMemorySnapshotStore should import from eventsource.observability"

    def test_inmemory_uses_standard_attribute_constants(self):
        """Verify InMemorySnapshotStore uses ATTR_* constants."""
        import subprocess

        result = subprocess.run(
            ["grep", "-c", "ATTR_", "src/eventsource/snapshots/in_memory.py"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parents[3],
        )
        # Should find multiple ATTR_* usages
        count = int(result.stdout.strip())
        assert count >= 3, f"Expected at least 3 ATTR_* usages, found {count}"

    def test_postgresql_imports_from_observability_module(self):
        """Verify PostgreSQLSnapshotStore imports tracing from observability module."""
        import subprocess

        result = subprocess.run(
            [
                "grep",
                "-c",
                "from eventsource.observability import",
                "src/eventsource/snapshots/postgresql.py",
            ],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parents[3],
        )
        # Should be at least 1 - imports from observability
        count = int(result.stdout.strip())
        assert count >= 1, "PostgreSQLSnapshotStore should import from eventsource.observability"

    def test_postgresql_uses_standard_attribute_constants(self):
        """Verify PostgreSQLSnapshotStore uses ATTR_* constants."""
        import subprocess

        result = subprocess.run(
            ["grep", "-c", "ATTR_", "src/eventsource/snapshots/postgresql.py"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parents[3],
        )
        # Should find multiple ATTR_* usages
        count = int(result.stdout.strip())
        assert count >= 3, f"Expected at least 3 ATTR_* usages, found {count}"

    def test_sqlite_imports_from_observability_module(self):
        """Verify SQLiteSnapshotStore imports tracing from observability module."""
        import subprocess

        result = subprocess.run(
            [
                "grep",
                "-c",
                "from eventsource.observability import",
                "src/eventsource/snapshots/sqlite.py",
            ],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parents[3],
        )
        # Should be at least 1 - imports from observability
        count = int(result.stdout.strip())
        assert count >= 1, "SQLiteSnapshotStore should import from eventsource.observability"

    def test_sqlite_uses_standard_attribute_constants(self):
        """Verify SQLiteSnapshotStore uses ATTR_* constants."""
        import subprocess

        result = subprocess.run(
            ["grep", "-c", "ATTR_", "src/eventsource/snapshots/sqlite.py"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parents[3],
        )
        # Should find multiple ATTR_* usages
        count = int(result.stdout.strip())
        assert count >= 3, f"Expected at least 3 ATTR_* usages, found {count}"


# ============================================================================
# PostgreSQL and SQLite TracerComposition Tests
# ============================================================================


class TestPostgreSQLSnapshotStoreTracerComposition:
    """Tests for PostgreSQLSnapshotStore Tracer composition integration."""

    def test_uses_tracer_composition(self):
        """PostgreSQLSnapshotStore uses Tracer composition pattern."""
        from eventsource.snapshots import PostgreSQLSnapshotStore

        sig = inspect.signature(PostgreSQLSnapshotStore.__init__)
        params = sig.parameters

        # Should accept tracer parameter for dependency injection
        assert "tracer" in params


class TestSQLiteSnapshotStoreTracerComposition:
    """Tests for SQLiteSnapshotStore Tracer composition integration."""

    def test_uses_tracer_composition(self):
        """SQLiteSnapshotStore uses Tracer composition pattern."""
        from eventsource.snapshots import SQLiteSnapshotStore

        if SQLiteSnapshotStore is not None:
            sig = inspect.signature(SQLiteSnapshotStore.__init__)
            params = sig.parameters

            # Should accept tracer parameter for dependency injection
            assert "tracer" in params
