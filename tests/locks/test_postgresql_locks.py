"""
Tests for PostgreSQL advisory lock utilities.

This module contains both unit tests (that don't require PostgreSQL)
and integration tests (that require a real PostgreSQL database).
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from eventsource.locks import (
    LockAcquisitionError,
    LockInfo,
    LockNotHeldError,
    PostgreSQLLockManager,
    migration_lock_key,
)

# =============================================================================
# Unit Tests - No database required
# =============================================================================


class TestLockInfo:
    """Tests for LockInfo dataclass."""

    def test_lock_info_creation(self) -> None:
        """Test creating LockInfo with all fields."""
        now = datetime.now(UTC)
        info = LockInfo(
            key="test:key",
            lock_id=12345,
            acquired_at=now,
            holder_id="worker-1",
        )

        assert info.key == "test:key"
        assert info.lock_id == 12345
        assert info.acquired_at == now
        assert info.holder_id == "worker-1"

    def test_lock_info_optional_holder_id(self) -> None:
        """Test creating LockInfo without holder_id."""
        info = LockInfo(
            key="test:key",
            lock_id=12345,
            acquired_at=datetime.now(UTC),
        )

        assert info.holder_id is None

    def test_lock_info_is_frozen(self) -> None:
        """Test that LockInfo is immutable (frozen dataclass)."""
        info = LockInfo(
            key="test:key",
            lock_id=12345,
            acquired_at=datetime.now(UTC),
        )

        with pytest.raises(AttributeError):
            info.key = "new:key"  # type: ignore[misc]


class TestLockAcquisitionError:
    """Tests for LockAcquisitionError exception."""

    def test_error_creation(self) -> None:
        """Test creating LockAcquisitionError with required fields."""
        error = LockAcquisitionError(
            key="test:key",
            reason="Lock held by another session",
        )

        assert error.key == "test:key"
        assert error.reason == "Lock held by another session"
        assert error.timeout is None
        assert "test:key" in str(error)
        assert "Lock held by another session" in str(error)

    def test_error_with_timeout(self) -> None:
        """Test creating LockAcquisitionError with timeout."""
        error = LockAcquisitionError(
            key="test:key",
            reason="Timeout after 5.0s",
            timeout=5.0,
        )

        assert error.timeout == 5.0


class TestLockNotHeldError:
    """Tests for LockNotHeldError exception."""

    def test_error_creation(self) -> None:
        """Test creating LockNotHeldError."""
        error = LockNotHeldError(key="test:key")

        assert error.key == "test:key"
        assert "test:key" in str(error)
        assert "not held" in str(error)


class TestKeyToLockId:
    """Tests for key-to-lock-id conversion."""

    def test_deterministic_conversion(self) -> None:
        """Test that the same key always produces the same lock ID."""
        key = "migration:tenant-abc"
        lock_id_1 = PostgreSQLLockManager._key_to_lock_id(key)
        lock_id_2 = PostgreSQLLockManager._key_to_lock_id(key)

        assert lock_id_1 == lock_id_2

    def test_different_keys_produce_different_ids(self) -> None:
        """Test that different keys produce different lock IDs."""
        key_1 = "migration:tenant-abc"
        key_2 = "migration:tenant-xyz"

        lock_id_1 = PostgreSQLLockManager._key_to_lock_id(key_1)
        lock_id_2 = PostgreSQLLockManager._key_to_lock_id(key_2)

        assert lock_id_1 != lock_id_2

    def test_lock_id_is_63_bit(self) -> None:
        """Test that lock ID fits in 63 bits (PostgreSQL signed bigint)."""
        # Test with various keys
        keys = [
            "migration:tenant-abc",
            "cutover:tenant-xyz",
            "lock:" + "a" * 1000,  # Long key
            "",  # Empty key
            str(uuid4()),  # UUID
        ]

        for key in keys:
            lock_id = PostgreSQLLockManager._key_to_lock_id(key)
            # 63 bits max = 2^63 - 1 = 9223372036854775807
            assert 0 <= lock_id <= 9223372036854775807
            # Verify it's positive (no sign bit used)
            assert lock_id >= 0

    def test_unicode_keys_work(self) -> None:
        """Test that unicode keys are handled correctly."""
        key = "migration:tenant-\u00e9\u00e0\u00fc"
        lock_id = PostgreSQLLockManager._key_to_lock_id(key)

        assert isinstance(lock_id, int)
        assert lock_id > 0


class TestMigrationLockKey:
    """Tests for migration_lock_key helper function."""

    def test_default_operation(self) -> None:
        """Test migration_lock_key with default operation."""
        tenant_id = uuid4()
        key = migration_lock_key(tenant_id)

        assert key == f"migration:{tenant_id}"

    def test_custom_operation(self) -> None:
        """Test migration_lock_key with custom operation."""
        tenant_id = uuid4()
        key = migration_lock_key(tenant_id, "cutover")

        assert key == f"cutover:{tenant_id}"

    def test_key_format_is_consistent(self) -> None:
        """Test that key format follows expected pattern."""
        tenant_id = uuid4()
        key = migration_lock_key(tenant_id, "dual-write")

        parts = key.split(":")
        assert len(parts) == 2
        assert parts[0] == "dual-write"
        assert parts[1] == str(tenant_id)


# =============================================================================
# Integration Tests - Require PostgreSQL
# =============================================================================

# Note: These tests are in a separate file or marked to run only when
# PostgreSQL is available. See test_postgresql_locks_integration.py
