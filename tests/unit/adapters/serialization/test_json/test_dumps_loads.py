"""Tests for json_dumps and json_loads convenience functions."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from eventsource.adapters.serialization import (
    json_dumps,
    json_loads,
)


class TestJsonDumps:
    """Tests for json_dumps convenience function."""

    def test_dumps_uuid(self) -> None:
        """Test json_dumps with UUID."""
        test_uuid = uuid4()
        result = json_dumps({"id": test_uuid})

        assert str(test_uuid) in result

    def test_dumps_datetime(self) -> None:
        """Test json_dumps with datetime."""
        test_dt = datetime(2024, 1, 15, 10, 30, 45, tzinfo=UTC)
        result = json_dumps({"timestamp": test_dt})

        assert "2024-01-15" in result

    def test_dumps_complex_structure(self) -> None:
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

    def test_loads_basic(self) -> None:
        """Test json_loads with basic JSON string."""
        data = '{"key": "value", "number": 42}'
        result = json_loads(data)

        assert result["key"] == "value"
        assert result["number"] == 42

    def test_loads_roundtrip(self) -> None:
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

    def test_loads_array(self) -> None:
        """Test json_loads with array."""
        data = '[1, 2, 3, "four"]'
        result = json_loads(data)

        assert result == [1, 2, 3, "four"]

    def test_loads_accepts_bytes(self) -> None:
        """json_loads accepts bytes directly (orjson natively supports both)."""
        data = b'{"key": "value", "number": 42}'
        result = json_loads(data)

        assert result == {"key": "value", "number": 42}

    def test_loads_bytes_and_str_agree(self) -> None:
        """Decoding the same payload as bytes or str yields identical results."""
        payload = '{"id": "abc", "items": [1, 2, 3]}'

        assert json_loads(payload) == json_loads(payload.encode())


class TestJsonDumpsUnsupportedTypes:
    """json_dumps has no special-case handling for Decimal -- it raises."""

    def test_dumps_decimal_raises_type_error(self) -> None:
        """Decimal is not natively supported by orjson and has no default
        handler here, so it raises TypeError (undocumented as a supported
        type -- this pins the current, unsupported behavior)."""
        from decimal import Decimal

        with pytest.raises(TypeError):
            json_dumps({"amount": Decimal("1.50")})


class TestJsonDumpsPydanticModels:
    """json_dumps on nested pydantic models (via model_dump(), not model instances directly)."""

    def test_dumps_nested_pydantic_model_dump(self) -> None:
        from pydantic import BaseModel

        class Inner(BaseModel):
            value: int
            created_at: datetime

        class Outer(BaseModel):
            id: object
            inner: Inner

        outer = Outer(id=uuid4(), inner=Inner(value=1, created_at=datetime.now(UTC)))
        result = json_dumps(outer.model_dump())
        parsed = json_loads(result)

        assert parsed["inner"]["value"] == 1
        assert "created_at" in parsed["inner"]
