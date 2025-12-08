"""Tests for the snapshots module exports."""


class TestSnapshotsModuleExports:
    """Tests for module-level exports."""

    def test_import_snapshot(self):
        """Test Snapshot can be imported."""
        from eventsource.snapshots import Snapshot

        assert Snapshot is not None

    def test_import_snapshot_store(self):
        """Test SnapshotStore can be imported."""
        from eventsource.snapshots import SnapshotStore

        assert SnapshotStore is not None

    def test_import_in_memory_snapshot_store(self):
        """Test InMemorySnapshotStore can be imported."""
        from eventsource.snapshots import InMemorySnapshotStore

        assert InMemorySnapshotStore is not None

    def test_import_exceptions(self):
        """Test all exceptions can be imported."""
        from eventsource.snapshots import (
            SnapshotDeserializationError,
            SnapshotError,
            SnapshotNotFoundError,
            SnapshotSchemaVersionError,
        )

        assert all(
            [
                SnapshotError,
                SnapshotDeserializationError,
                SnapshotSchemaVersionError,
                SnapshotNotFoundError,
            ]
        )

    def test_all_exports_are_accessible(self):
        """Test __all__ contains valid exports."""
        import eventsource.snapshots as snapshots_module

        for name in snapshots_module.__all__:
            assert hasattr(snapshots_module, name), f"Missing export: {name}"

    def test_module_has_docstring(self):
        """Test module has documentation."""
        import eventsource.snapshots as snapshots_module

        assert snapshots_module.__doc__ is not None
        assert len(snapshots_module.__doc__) > 100  # Substantial docstring

    def test_expected_exports_in_all(self):
        """Test that expected exports are in __all__."""
        import eventsource.snapshots as snapshots_module

        expected = [
            "Snapshot",
            "SnapshotStore",
            "InMemorySnapshotStore",
            "SnapshotError",
            "SnapshotDeserializationError",
            "SnapshotSchemaVersionError",
            "SnapshotNotFoundError",
        ]

        for name in expected:
            assert name in snapshots_module.__all__, f"{name} should be in __all__"

    def test_docstring_contains_key_info(self):
        """Test module docstring contains important information."""
        import eventsource.snapshots as snapshots_module

        docstring = snapshots_module.__doc__

        # Should mention key concepts
        assert "Snapshot" in docstring
        assert "aggregate" in docstring.lower()
        assert "event" in docstring.lower()

    def test_import_from_submodules(self):
        """Test direct imports from submodules work."""
        from eventsource.snapshots.exceptions import (
            SnapshotDeserializationError,
            SnapshotError,
            SnapshotNotFoundError,
            SnapshotSchemaVersionError,
        )
        from eventsource.snapshots.in_memory import InMemorySnapshotStore
        from eventsource.snapshots.interface import Snapshot, SnapshotStore

        # Verify all are importable
        assert Snapshot is not None
        assert SnapshotStore is not None
        assert InMemorySnapshotStore is not None
        assert SnapshotError is not None
        assert SnapshotDeserializationError is not None
        assert SnapshotSchemaVersionError is not None
        assert SnapshotNotFoundError is not None

    def test_in_memory_store_is_subclass_of_store(self):
        """Test that InMemorySnapshotStore inherits from SnapshotStore."""
        from eventsource.snapshots import InMemorySnapshotStore, SnapshotStore

        assert issubclass(InMemorySnapshotStore, SnapshotStore)

    def test_exceptions_inherit_from_snapshot_error(self):
        """Test all exceptions inherit from SnapshotError."""
        from eventsource.snapshots import (
            SnapshotDeserializationError,
            SnapshotError,
            SnapshotNotFoundError,
            SnapshotSchemaVersionError,
        )

        assert issubclass(SnapshotDeserializationError, SnapshotError)
        assert issubclass(SnapshotSchemaVersionError, SnapshotError)
        assert issubclass(SnapshotNotFoundError, SnapshotError)
