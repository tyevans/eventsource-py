"""Unit tests for snapshot exceptions."""

from uuid import uuid4

import pytest

from eventsource.domain.exceptions import (
    SnapshotDeserializationError,
    SnapshotError,
    SnapshotNotFoundError,
    SnapshotSchemaVersionError,
)


class TestSnapshotError:
    """Tests for the base SnapshotError exception."""

    def test_base_exception(self):
        with pytest.raises(SnapshotError):
            raise SnapshotError("test error")

    def test_base_exception_message(self):
        error = SnapshotError("test error message")
        assert str(error) == "test error message"

    def test_all_exceptions_inherit_from_base(self):
        aggregate_id = uuid4()
        exceptions = [
            SnapshotDeserializationError(aggregate_id, "Order"),
            SnapshotSchemaVersionError(aggregate_id, "Order", 1, 2),
            SnapshotNotFoundError(aggregate_id, "Order"),
        ]
        for exc in exceptions:
            assert isinstance(exc, SnapshotError)
            assert isinstance(exc, Exception)


class TestSnapshotDeserializationError:
    """Tests for SnapshotDeserializationError."""

    def test_basic_creation(self):
        aggregate_id = uuid4()
        error = SnapshotDeserializationError(aggregate_id, "Order")
        assert error.aggregate_id == aggregate_id
        assert error.aggregate_type == "Order"
        assert error.original_error is None
        assert "Order" in str(error)
        assert str(aggregate_id) in str(error)

    def test_with_original_error(self):
        aggregate_id = uuid4()
        original = ValueError("invalid data")
        error = SnapshotDeserializationError(aggregate_id, "Order", original_error=original)
        assert error.original_error == original
        assert "invalid data" in str(error)

    def test_with_custom_message(self):
        aggregate_id = uuid4()
        error = SnapshotDeserializationError(aggregate_id, "Order", message="Custom error message")
        assert str(error) == "Custom error message"

    def test_repr(self):
        aggregate_id = uuid4()
        error = SnapshotDeserializationError(aggregate_id, "Order")
        repr_str = repr(error)
        assert "SnapshotDeserializationError" in repr_str
        assert "aggregate_id=" in repr_str
        assert "aggregate_type=" in repr_str
        assert "original_error=" in repr_str

    def test_default_message_format(self):
        aggregate_id = uuid4()
        error = SnapshotDeserializationError(aggregate_id, "User")
        expected_prefix = f"Failed to deserialize snapshot for User/{aggregate_id}"
        assert str(error).startswith(expected_prefix)

    def test_message_with_original_error_details(self):
        aggregate_id = uuid4()
        original = RuntimeError("corrupted state")
        error = SnapshotDeserializationError(aggregate_id, "Order", original_error=original)
        assert "corrupted state" in str(error)
        assert "Order" in str(error)


class TestSnapshotSchemaVersionError:
    """Tests for SnapshotSchemaVersionError."""

    def test_creation(self):
        aggregate_id = uuid4()
        error = SnapshotSchemaVersionError(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            snapshot_schema_version=1,
            expected_schema_version=2,
        )
        assert error.aggregate_id == aggregate_id
        assert error.aggregate_type == "Order"
        assert error.snapshot_schema_version == 1
        assert error.expected_schema_version == 2

    def test_message_contains_versions(self):
        error = SnapshotSchemaVersionError(
            aggregate_id=uuid4(),
            aggregate_type="Order",
            snapshot_schema_version=1,
            expected_schema_version=3,
        )
        message = str(error)
        assert "schema_version=1" in message
        assert "schema_version=3" in message
        assert "Order" in message
        assert "mismatch" in message.lower()

    def test_repr(self):
        aggregate_id = uuid4()
        error = SnapshotSchemaVersionError(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            snapshot_schema_version=1,
            expected_schema_version=2,
        )
        repr_str = repr(error)
        assert "SnapshotSchemaVersionError" in repr_str
        assert "snapshot_schema_version=1" in repr_str
        assert "expected_schema_version=2" in repr_str
        assert "aggregate_id=" in repr_str
        assert "aggregate_type=" in repr_str

    def test_large_version_numbers(self):
        error = SnapshotSchemaVersionError(
            aggregate_id=uuid4(),
            aggregate_type="Order",
            snapshot_schema_version=999,
            expected_schema_version=1000,
        )
        assert error.snapshot_schema_version == 999
        assert error.expected_schema_version == 1000
        assert "999" in str(error)
        assert "1000" in str(error)


class TestSnapshotNotFoundError:
    """Tests for SnapshotNotFoundError."""

    def test_creation(self):
        aggregate_id = uuid4()
        error = SnapshotNotFoundError(aggregate_id, "Order")
        assert error.aggregate_id == aggregate_id
        assert error.aggregate_type == "Order"

    def test_message(self):
        aggregate_id = uuid4()
        error = SnapshotNotFoundError(aggregate_id, "Order")
        assert "No snapshot found" in str(error)
        assert "Order" in str(error)
        assert str(aggregate_id) in str(error)

    def test_repr(self):
        aggregate_id = uuid4()
        error = SnapshotNotFoundError(aggregate_id, "Order")
        repr_str = repr(error)
        assert "SnapshotNotFoundError" in repr_str
        assert "aggregate_id=" in repr_str
        assert "aggregate_type=" in repr_str

    def test_different_aggregate_types(self):
        aggregate_id = uuid4()
        for agg_type in ["Order", "User", "Account", "Product"]:
            error = SnapshotNotFoundError(aggregate_id, agg_type)
            assert agg_type in str(error)
            assert error.aggregate_type == agg_type


class TestExceptionHierarchy:
    """Tests for exception hierarchy and catching behavior."""

    def test_catch_all_with_base_class(self):
        aggregate_id = uuid4()
        exceptions = [
            SnapshotDeserializationError(aggregate_id, "Order"),
            SnapshotSchemaVersionError(aggregate_id, "Order", 1, 2),
            SnapshotNotFoundError(aggregate_id, "Order"),
        ]
        for exc in exceptions:
            try:
                raise exc
            except SnapshotError as caught:
                assert caught is exc

    def test_specific_exception_not_caught_by_sibling(self):
        aggregate_id = uuid4()
        with pytest.raises(SnapshotDeserializationError):
            try:
                raise SnapshotDeserializationError(aggregate_id, "Order")
            except SnapshotSchemaVersionError:
                pytest.fail("Should not catch SnapshotDeserializationError")

    def test_exception_inheritance_chain(self):
        error = SnapshotDeserializationError(uuid4(), "Order")
        assert isinstance(error, SnapshotDeserializationError)
        assert isinstance(error, SnapshotError)
        assert isinstance(error, Exception)
        assert isinstance(error, BaseException)

    def test_raise_and_reraise_preserves_info(self):
        aggregate_id = uuid4()
        original = ValueError("test")
        try:
            try:
                raise SnapshotDeserializationError(aggregate_id, "Order", original_error=original)
            except SnapshotError as e:
                raise e
        except SnapshotDeserializationError as caught:
            assert caught.aggregate_id == aggregate_id
            assert caught.original_error is original
