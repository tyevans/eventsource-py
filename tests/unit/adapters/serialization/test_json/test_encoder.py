"""Tests for EventSourceJSONEncoder and module export compatibility."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from eventsource.adapters.serialization import (
    EventSourceJSONEncoder,
)


class TestEventSourceJSONEncoder:
    """
    Tests for EventSourceJSONEncoder.

    This class is no longer used by json_dumps/json_loads (orjson, now a
    core dependency, serializes UUID/datetime natively) but remains public
    API used directly by eventsource.adapters.postgresql.outbox -- see the class
    docstring in serialization/json.py. These tests exercise it the same
    way outbox.py does: via stdlib json.dumps(..., cls=EventSourceJSONEncoder).
    """

    def test_encodes_uuid(self) -> None:
        """Test encoding UUID to string."""
        test_uuid = uuid4()
        result = json.dumps({"id": test_uuid}, cls=EventSourceJSONEncoder)
        assert str(test_uuid) in result

    def test_encodes_datetime(self) -> None:
        """Test encoding datetime to ISO format string."""
        now = datetime.now(UTC)
        result = json.dumps({"timestamp": now}, cls=EventSourceJSONEncoder)
        assert now.isoformat() in result

    def test_encodes_datetime_with_microseconds(self) -> None:
        """Test encoding datetime with microseconds."""
        test_dt = datetime(2024, 1, 15, 10, 30, 45, 123456, tzinfo=UTC)
        result = json.dumps({"timestamp": test_dt}, cls=EventSourceJSONEncoder)
        assert "123456" in result

    def test_regular_types_unchanged(self) -> None:
        """Test that regular types are encoded normally."""
        data = {"string": "hello", "number": 42, "boolean": True}
        result = json.dumps(data, cls=EventSourceJSONEncoder)
        parsed = json.loads(result)
        assert parsed == data

    def test_encode_nested_structure(self) -> None:
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

    def test_encode_unsupported_type_raises(self) -> None:
        """Test that unsupported types raise TypeError."""

        class CustomClass:
            pass

        data = {"custom": CustomClass()}

        with pytest.raises(TypeError):
            json.dumps(data, cls=EventSourceJSONEncoder)


class TestNewModuleExports:
    """Tests to verify the new module structure works correctly."""

    def test_import_from_serialization_module(self) -> None:
        """Test importing from eventsource.adapters.serialization works."""
        from eventsource.adapters.serialization import (
            EventSourceJSONEncoder as SubEncoder,
        )
        from eventsource.adapters.serialization import (
            json_dumps as sub_dumps,
        )
        from eventsource.adapters.serialization import (
            json_loads as sub_loads,
        )

        # Verify functions are callable
        result = sub_dumps({"id": uuid4()})
        assert isinstance(result, str)

        parsed = sub_loads(result)
        assert isinstance(parsed, dict)

        # Verify encoder is a class
        assert isinstance(SubEncoder, type)

    def test_import_from_serialization_json_submodule(self) -> None:
        """Test importing from eventsource.adapters.serialization.json works."""
        from eventsource.adapters.serialization.json import (
            EventSourceJSONEncoder as SubEncoder,
        )
        from eventsource.adapters.serialization.json import (
            json_dumps as sub_dumps,
        )
        from eventsource.adapters.serialization.json import (
            json_loads as sub_loads,
        )

        # Verify functions are callable
        result = sub_dumps({"id": uuid4()})
        assert isinstance(result, str)

        parsed = sub_loads(result)
        assert isinstance(parsed, dict)

        # Verify encoder is a class
        assert isinstance(SubEncoder, type)

    def test_encoder_exported_from_top_level(self) -> None:
        """Test that EventSourceJSONEncoder is exported from eventsource package."""
        from eventsource import EventSourceJSONEncoder as TopEncoder

        # Verify encoder is a class
        assert isinstance(TopEncoder, type)

        # Verify it works
        result = json.dumps({"id": uuid4()}, cls=TopEncoder)
        assert isinstance(result, str)
