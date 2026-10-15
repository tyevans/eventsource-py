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
        assert str(SnapshotError("test error message")) == "test error message"

    def test_all_exceptions_inherit_from_base(self):
        aggregate_id = uuid4()
        exceptions = [
            SnapshotDeserializationError(aggregate_id, "Order"),
            SnapshotSchemaVersionError(aggregate_id, "Order", 1, 2),
            SnapshotNotFoundError(aggregate_id, "Order"),
        ]
        for exc in exceptions:
            assert isinstance(exc, SnapshotError) and isinstance(exc, Exception)


class TestSnapshotDeserializationError:
    """Tests for SnapshotDeserializationError."""

    def test_basic_creation(self):
        aggregate_id = uuid4()
        error = SnapshotDeserializationError(aggregate_id, "Order")
        assert error.aggregate_id == aggregate_id and error.aggregate_type == "Order"
        assert error.original_error is None
        assert "Order" in str(error) and str(aggregate_id) in str(error)

    def test_with_original_error(self):
        aggregate_id = uuid4()
        original = ValueError("invalid data")
        error = SnapshotDeserializationError(aggregate_id, "Order", original_error=original)
        assert error.original_error == original and "invalid data" in str(error)

    def test_with_custom_message(self):
        error = SnapshotDeserializationError(uuid4(), "Order", message="Custom error message")
        assert str(error) == "Custom error message"

    def test_repr(self):
        repr_str = repr(SnapshotDeserializationError(uuid4(), "Order"))
        for substr in (
            "SnapshotDeserializationError",
            "aggregate_id=",
            "aggregate_type=",
            "original_error=",
        ):
            assert substr in repr_str

    def test_default_message_format(self):
        aggregate_id = uuid4()
        error = SnapshotDeserializationError(aggregate_id, "User")
        assert str(error).startswith(f"Failed to deserialize snapshot for User/{aggregate_id}")

    def test_message_with_original_error_details(self):
        original = RuntimeError("corrupted state")
        error = SnapshotDeserializationError(uuid4(), "Order", original_error=original)
        assert "corrupted state" in str(error) and "Order" in str(error)


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
        assert error.aggregate_id == aggregate_id and error.aggregate_type == "Order"
        assert (error.snapshot_schema_version, error.expected_schema_version) == (1, 2)

    def test_message_contains_versions(self):
        error = SnapshotSchemaVersionError(
            aggregate_id=uuid4(),
            aggregate_type="Order",
            snapshot_schema_version=1,
            expected_schema_version=3,
        )
        message = str(error)
        assert "schema_version=1" in message and "schema_version=3" in message
        assert "Order" in message and "mismatch" in message.lower()

    def test_repr(self):
        error = SnapshotSchemaVersionError(
            aggregate_id=uuid4(),
            aggregate_type="Order",
            snapshot_schema_version=1,
            expected_schema_version=2,
        )
        repr_str = repr(error)
        for substr in (
            "SnapshotSchemaVersionError",
            "snapshot_schema_version=1",
            "expected_schema_version=2",
            "aggregate_id=",
            "aggregate_type=",
        ):
            assert substr in repr_str

    def test_large_version_numbers(self):
        error = SnapshotSchemaVersionError(
            aggregate_id=uuid4(),
            aggregate_type="Order",
            snapshot_schema_version=999,
            expected_schema_version=1000,
        )
        assert (error.snapshot_schema_version, error.expected_schema_version) == (999, 1000)
        assert "999" in str(error) and "1000" in str(error)


class TestSnapshotNotFoundError:
    """Tests for SnapshotNotFoundError."""

    def test_creation(self):
        aggregate_id = uuid4()
        error = SnapshotNotFoundError(aggregate_id, "Order")
        assert error.aggregate_id == aggregate_id and error.aggregate_type == "Order"

    def test_message(self):
        aggregate_id = uuid4()
        error = SnapshotNotFoundError(aggregate_id, "Order")
        assert "No snapshot found" in str(error) and "Order" in str(error)
        assert str(aggregate_id) in str(error)

    def test_repr(self):
        repr_str = repr(SnapshotNotFoundError(uuid4(), "Order"))
        for substr in ("SnapshotNotFoundError", "aggregate_id=", "aggregate_type="):
            assert substr in repr_str

    def test_different_aggregate_types(self):
        aggregate_id = uuid4()
        for agg_type in ["Order", "User", "Account", "Product"]:
            error = SnapshotNotFoundError(aggregate_id, agg_type)
            assert agg_type in str(error) and error.aggregate_type == agg_type


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
        with pytest.raises(SnapshotDeserializationError):
            try:
                raise SnapshotDeserializationError(uuid4(), "Order")
            except SnapshotSchemaVersionError:
                pytest.fail("Should not catch SnapshotDeserializationError")

    def test_exception_inheritance_chain(self):
        error = SnapshotDeserializationError(uuid4(), "Order")
        assert isinstance(error, SnapshotDeserializationError)
        assert isinstance(error, SnapshotError)
        assert isinstance(error, Exception) and isinstance(error, BaseException)

    def test_raise_and_reraise_preserves_info(self):
        aggregate_id, original = uuid4(), ValueError("test")
        try:
            try:
                raise SnapshotDeserializationError(aggregate_id, "Order", original_error=original)
            except SnapshotError as e:
                raise e
        except SnapshotDeserializationError as caught:
            assert caught.aggregate_id == aggregate_id and caught.original_error is original
