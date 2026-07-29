"""Unit tests for the SQL dialect adapter."""

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from eventsource.repositories._dialect import (
    Dialect,
    json_param,
    json_result,
    now_expr,
    ts_param,
    ts_result,
    uuid_param,
    uuid_result,
)


def test_uuid_param_postgres_passes_through():
    value = uuid4()
    assert uuid_param(value, Dialect.POSTGRESQL) is value


def test_uuid_param_sqlite_stringifies():
    value = uuid4()
    assert uuid_param(value, Dialect.SQLITE) == str(value)


def test_uuid_param_handles_none():
    assert uuid_param(None, Dialect.SQLITE) is None
    assert uuid_param(None, Dialect.POSTGRESQL) is None


def test_uuid_result_accepts_both_representations():
    value = uuid4()
    assert uuid_result(value) == value
    assert uuid_result(str(value)) == value
    assert uuid_result(None) is None


def test_ts_param_sqlite_is_iso_and_roundtrips():
    value = datetime(2026, 7, 28, 12, 30, tzinfo=UTC)
    encoded = ts_param(value, Dialect.SQLITE)
    assert isinstance(encoded, str)
    assert ts_result(encoded) == value


def test_ts_result_attaches_utc_to_naive_values():
    """SQLite returns naive strings; comparisons must not raise."""
    result = ts_result("2026-07-28T12:30:00")
    assert result is not None
    assert result.tzinfo is UTC


def test_json_roundtrip_sqlite():
    payload = {"a": 1, "b": ["x"]}
    assert json_result(json_param(payload, Dialect.SQLITE)) == payload


def test_now_expr_per_dialect():
    assert now_expr(Dialect.POSTGRESQL) == "NOW()"
    assert now_expr(Dialect.SQLITE) == "CURRENT_TIMESTAMP"


def test_uuid_result_accepts_bytes_bytearray_memoryview():
    value = uuid4()
    assert uuid_result(value.bytes) == value
    assert uuid_result(bytearray(value.bytes)) == value
    assert uuid_result(memoryview(value.bytes)) == value


def test_json_param_none_returns_none_not_json_null():
    assert json_param(None, Dialect.SQLITE) is None
    assert json_param(None, Dialect.POSTGRESQL) is None


def test_json_param_none_roundtrips_as_sql_null():
    """A None value must bind as SQL NULL and read back as None, not 'null'."""
    encoded = json_param(None, Dialect.SQLITE)
    # Simulate what the DB driver does with a None bound parameter: nothing
    # is written but SQL NULL, so the column read comes back as None.
    assert encoded is None
    assert json_result(encoded) is None


def test_json_param_encodes_uuid_and_datetime_payload():
    event_id = uuid4()
    occurred_at = datetime(2026, 7, 28, 3, 25, 32, 733094, tzinfo=UTC)
    payload = {"id": event_id, "at": occurred_at}

    encoded = json_param(payload, Dialect.SQLITE)
    assert encoded is not None
    decoded = json_result(encoded)

    assert decoded == {"id": str(event_id), "at": occurred_at.isoformat()}
