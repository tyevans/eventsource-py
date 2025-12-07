"""
Unit tests for the JSON encoder utilities.

Tests for:
- UUID serialization
- datetime serialization
- Convenience functions
"""

import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from eventsource.repositories._json import (
    EventSourceJSONEncoder,
    json_dumps,
    json_loads,
)


class TestEventSourceJSONEncoder:
    """Tests for EventSourceJSONEncoder."""

    def test_encode_uuid(self):
        """Test encoding UUID to string."""
        test_uuid = uuid4()
        data = {"id": test_uuid}

        result = json.dumps(data, cls=EventSourceJSONEncoder)
        assert str(test_uuid) in result

    def test_encode_datetime(self):
        """Test encoding datetime to ISO format string."""
        test_dt = datetime(2024, 1, 15, 10, 30, 45, tzinfo=UTC)
        data = {"timestamp": test_dt}

        result = json.dumps(data, cls=EventSourceJSONEncoder)
        assert "2024-01-15" in result
        assert "10:30:45" in result

    def test_encode_datetime_with_microseconds(self):
        """Test encoding datetime with microseconds."""
        test_dt = datetime(2024, 1, 15, 10, 30, 45, 123456, tzinfo=UTC)
        data = {"timestamp": test_dt}

        result = json.dumps(data, cls=EventSourceJSONEncoder)
        assert "123456" in result

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

    def test_encode_regular_types(self):
        """Test that regular types are encoded normally."""
        data = {
            "string": "hello",
            "number": 42,
            "float": 3.14,
            "bool": True,
            "null": None,
            "array": [1, 2, 3],
            "object": {"nested": "value"},
        }

        result = json.dumps(data, cls=EventSourceJSONEncoder)
        parsed = json.loads(result)

        assert parsed == data

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
