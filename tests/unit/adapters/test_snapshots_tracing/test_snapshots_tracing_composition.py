"""Tests for SnapshotStore Tracer composition and attribute standards."""

from __future__ import annotations

import inspect
import subprocess
from pathlib import Path

from eventsource.adapters.memory.snapshots import InMemorySnapshotStore
from eventsource.observability import NullTracer


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


class TestSnapshotStoreStandardAttributes:
    """Tests for standard attribute usage in SnapshotStore implementations."""

    @property
    def _repo_root(self) -> Path:
        return Path(__file__).resolve().parents[4]

    def test_inmemory_imports_from_observability_module(self):
        """Verify InMemorySnapshotStore imports tracing from observability module."""
        result = subprocess.run(
            [
                "grep",
                "-c",
                "from eventsource.observability import",
                "src/eventsource/adapters/memory/snapshots.py",
            ],
            capture_output=True,
            text=True,
            cwd=self._repo_root,
        )
        count = int(result.stdout.strip())
        assert count >= 1, "InMemorySnapshotStore should import from eventsource.observability"

    def test_inmemory_uses_standard_attribute_constants(self):
        """Verify InMemorySnapshotStore uses ATTR_* constants."""
        result = subprocess.run(
            ["grep", "-c", "ATTR_", "src/eventsource/adapters/memory/snapshots.py"],
            capture_output=True,
            text=True,
            cwd=self._repo_root,
        )
        count = int(result.stdout.strip())
        assert count >= 3, f"Expected at least 3 ATTR_* usages, found {count}"

    def test_postgresql_imports_from_observability_module(self):
        """Verify PostgreSQLSnapshotStore imports tracing from observability module."""
        result = subprocess.run(
            [
                "grep",
                "-c",
                "from eventsource.observability import",
                "src/eventsource/adapters/postgresql/snapshots.py",
            ],
            capture_output=True,
            text=True,
            cwd=self._repo_root,
        )
        count = int(result.stdout.strip())
        assert count >= 1, "PostgreSQLSnapshotStore should import from eventsource.observability"

    def test_postgresql_uses_standard_attribute_constants(self):
        """Verify PostgreSQLSnapshotStore uses ATTR_* constants."""
        result = subprocess.run(
            ["grep", "-c", "ATTR_", "src/eventsource/adapters/postgresql/snapshots.py"],
            capture_output=True,
            text=True,
            cwd=self._repo_root,
        )
        count = int(result.stdout.strip())
        assert count >= 3, f"Expected at least 3 ATTR_* usages, found {count}"

    def test_sqlite_imports_from_observability_module(self):
        """Verify SQLiteSnapshotStore imports tracing from observability module."""
        result = subprocess.run(
            [
                "grep",
                "-c",
                "from eventsource.observability import",
                "src/eventsource/adapters/sqlite/snapshots.py",
            ],
            capture_output=True,
            text=True,
            cwd=self._repo_root,
        )
        count = int(result.stdout.strip())
        assert count >= 1, "SQLiteSnapshotStore should import from eventsource.observability"

    def test_sqlite_uses_standard_attribute_constants(self):
        """Verify SQLiteSnapshotStore uses ATTR_* constants."""
        result = subprocess.run(
            ["grep", "-c", "ATTR_", "src/eventsource/adapters/sqlite/snapshots.py"],
            capture_output=True,
            text=True,
            cwd=self._repo_root,
        )
        count = int(result.stdout.strip())
        assert count >= 3, f"Expected at least 3 ATTR_* usages, found {count}"


class TestPostgreSQLSnapshotStoreTracerComposition:
    """Tests for PostgreSQLSnapshotStore Tracer composition integration."""

    def test_uses_tracer_composition(self):
        """PostgreSQLSnapshotStore uses Tracer composition pattern."""
        from eventsource.adapters.postgresql.snapshots import PostgreSQLSnapshotStore

        sig = inspect.signature(PostgreSQLSnapshotStore.__init__)
        params = sig.parameters

        # Should accept tracer parameter for dependency injection
        assert "tracer" in params


class TestSQLiteSnapshotStoreTracerComposition:
    """Tests for SQLiteSnapshotStore Tracer composition integration."""

    def test_uses_tracer_composition(self):
        """SQLiteSnapshotStore uses Tracer composition pattern."""
        from eventsource.adapters.sqlite.snapshots import SQLiteSnapshotStore

        if SQLiteSnapshotStore is not None:
            sig = inspect.signature(SQLiteSnapshotStore.__init__)
            params = sig.parameters

            # Should accept tracer parameter for dependency injection
            assert "tracer" in params
