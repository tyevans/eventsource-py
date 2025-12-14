"""
Unit tests for the JSON serialization module.

Tests for:
- EventSourceJSONEncoder class
- json_dumps convenience function
- json_loads convenience function
- Backward compatibility with deprecated imports
"""

import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from eventsource.serialization import (
    EventSourceJSONEncoder,
    json_dumps,
    json_loads,
)


class TestEventSourceJSONEncoder:
    """Tests for EventSourceJSONEncoder."""

    def test_encodes_uuid(self):
        """Test encoding UUID to string."""
        test_uuid = uuid4()
        result = json_dumps({"id": test_uuid})
        assert str(test_uuid) in result

    def test_encodes_datetime(self):
        """Test encoding datetime to ISO format string."""
        now = datetime.now(UTC)
        result = json_dumps({"timestamp": now})
        assert now.isoformat() in result

    def test_encodes_datetime_with_microseconds(self):
        """Test encoding datetime with microseconds."""
        test_dt = datetime(2024, 1, 15, 10, 30, 45, 123456, tzinfo=UTC)
        result = json_dumps({"timestamp": test_dt})
        assert "123456" in result

    def test_regular_types_unchanged(self):
        """Test that regular types are encoded normally."""
        data = {"string": "hello", "number": 42, "boolean": True}
        result = json_dumps(data)
        parsed = json_loads(result)
        assert parsed == data

    def test_encode_nested_structure(self):
        """Test encoding nested structures with UUID and datetime."""
        test_uuid = uuid4()
        test_dt = datetime(2024, 1, 15, 10, 30, 45, tzinfo=UTC)

        data = {
            "event": {
                "id": test_uuid,
                "occurred_at": test_dt,
                "items": [{"sub_id": uuid4(), "created_at": datetime.now(UTC)}],
            }
        }

        result = json.dumps(data, cls=EventSourceJSONEncoder)
        parsed = json.loads(result)

        assert parsed["event"]["id"] == str(test_uuid)

    def test_encode_unsupported_type_raises(self):
        """Test that unsupported types raise TypeError."""

        class CustomClass:
            pass

        data = {"custom": CustomClass()}

        with pytest.raises(TypeError):
            json.dumps(data, cls=EventSourceJSONEncoder)


class TestJsonDumps:
    """Tests for json_dumps convenience function."""

    def test_dumps_uuid(self):
        """Test json_dumps with UUID."""
        test_uuid = uuid4()
        result = json_dumps({"id": test_uuid})

        assert str(test_uuid) in result

    def test_dumps_datetime(self):
        """Test json_dumps with datetime."""
        test_dt = datetime(2024, 1, 15, 10, 30, 45, tzinfo=UTC)
        result = json_dumps({"timestamp": test_dt})

        assert "2024-01-15" in result

    def test_dumps_complex_structure(self):
        """Test json_dumps with complex nested structure."""
        data = {
            "event_id": uuid4(),
            "aggregate_id": uuid4(),
            "occurred_at": datetime.now(UTC),
            "payload": {
                "items": [{"id": uuid4()}],
                "metadata": {"created_at": datetime.now(UTC)},
            },
        }

        result = json_dumps(data)

        # Should be valid JSON
        parsed = json.loads(result)
        assert "event_id" in parsed
        assert "payload" in parsed


class TestJsonLoads:
    """Tests for json_loads convenience function."""

    def test_loads_basic(self):
        """Test json_loads with basic JSON string."""
        data = '{"key": "value", "number": 42}'
        result = json_loads(data)

        assert result["key"] == "value"
        assert result["number"] == 42

    def test_loads_roundtrip(self):
        """Test roundtrip with json_dumps and json_loads."""
        original = {
            "id": uuid4(),
            "timestamp": datetime.now(UTC),
            "data": {"nested": "value"},
        }

        json_str = json_dumps(original)
        loaded = json_loads(json_str)

        # UUIDs and datetimes become strings after roundtrip
        assert loaded["id"] == str(original["id"])
        assert loaded["data"]["nested"] == "value"

    def test_loads_array(self):
        """Test json_loads with array."""
        data = '[1, 2, 3, "four"]'
        result = json_loads(data)

        assert result == [1, 2, 3, "four"]


class TestDeprecatedImports:
    """Tests for backward compatibility with deprecated import paths."""

    def test_deprecated_import_warns(self):
        """Test that importing from deprecated path shows warning."""
        with pytest.warns(DeprecationWarning, match="eventsource.serialization"):
            from eventsource.repositories._json import json_dumps as _  # noqa: F401

    def test_deprecated_import_works(self):
        """Test that deprecated imports still work correctly."""
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            from eventsource.repositories._json import json_dumps

            result = json_dumps({"test": "data"})
            assert "test" in result

    def test_deprecated_encoder_import_warns(self):
        """Test that importing encoder from deprecated path shows warning."""
        with pytest.warns(DeprecationWarning, match="eventsource.serialization"):
            from eventsource.repositories._json import EventSourceJSONEncoder as _  # noqa: F401

    def test_deprecated_loads_import_warns(self):
        """Test that importing json_loads from deprecated path shows warning."""
        with pytest.warns(DeprecationWarning, match="eventsource.serialization"):
            from eventsource.repositories._json import json_loads as _  # noqa: F401

    def test_deprecated_module_dir(self):
        """Test that deprecated module's __dir__ returns expected names."""
        from eventsource.repositories import _json

        available = dir(_json)
        assert "EventSourceJSONEncoder" in available
        assert "json_dumps" in available
        assert "json_loads" in available

    def test_deprecated_module_unknown_attr_raises(self):
        """Test that accessing unknown attr in deprecated module raises AttributeError."""
        with pytest.raises(AttributeError, match="has no attribute"):
            from eventsource.repositories import _json

            _ = _json.unknown_attribute


class TestNewModuleExports:
    """Tests to verify the new module structure works correctly."""

    def test_import_from_serialization_module(self):
        """Test importing from eventsource.serialization works."""
        from eventsource.serialization import (
            EventSourceJSONEncoder,
            json_dumps,
            json_loads,
        )

        # Verify functions are callable
        result = json_dumps({"id": uuid4()})
        assert isinstance(result, str)

        parsed = json_loads(result)
        assert isinstance(parsed, dict)

        # Verify encoder is a class
        assert isinstance(EventSourceJSONEncoder, type)

    def test_import_from_serialization_json_submodule(self):
        """Test importing from eventsource.serialization.json works."""
        from eventsource.serialization.json import (
            EventSourceJSONEncoder,
            json_dumps,
            json_loads,
        )

        # Verify functions are callable
        result = json_dumps({"id": uuid4()})
        assert isinstance(result, str)

        parsed = json_loads(result)
        assert isinstance(parsed, dict)

        # Verify encoder is a class
        assert isinstance(EventSourceJSONEncoder, type)

    def test_encoder_still_exported_from_repositories(self):
        """Test that EventSourceJSONEncoder is still exported from repositories."""
        from eventsource.repositories import EventSourceJSONEncoder

        # Verify encoder is a class
        assert isinstance(EventSourceJSONEncoder, type)

        # Verify it works
        result = json.dumps({"id": uuid4()}, cls=EventSourceJSONEncoder)
        assert isinstance(result, str)

    def test_encoder_exported_from_top_level(self):
        """Test that EventSourceJSONEncoder is exported from eventsource package."""
        from eventsource import EventSourceJSONEncoder

        # Verify encoder is a class
        assert isinstance(EventSourceJSONEncoder, type)

        # Verify it works
        result = json.dumps({"id": uuid4()}, cls=EventSourceJSONEncoder)
        assert isinstance(result, str)
