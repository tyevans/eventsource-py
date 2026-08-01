"""
Property-based tests for `eventsource.adapters._sql.dialect`.

Complements the hand-enumerated cases in `test_dialect.py`. That suite
covers `uuid_result` for `str` and `UUID` inputs but not `bytes`,
`bytearray`, or `memoryview` -- exactly the class of gap Hypothesis
generates for free by sweeping representations. See
`.superpowers/sdd/2026-07-28-m0-sqlalchemy-unification/task-2c-brief.md`.

Naive datetimes are documented (`ts_result` docstring) as coming back
UTC-attached rather than naive. The round-trip property below excludes
naive datetimes from the equality check and asserts the documented
UTC-attachment behavior separately, rather than loosening the equality
property to paper over the difference.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

from hypothesis import given
from hypothesis import strategies as st

from eventsource.adapters._sql.dialect import (
    Dialect,
    ts_param,
    ts_result,
    uuid_param,
    uuid_result,
)


@given(value=st.uuids())
def test_uuid_roundtrip_postgresql(value: UUID) -> None:
    assert uuid_result(uuid_param(value, Dialect.POSTGRESQL)) == value


@given(value=st.uuids())
def test_uuid_roundtrip_sqlite(value: UUID) -> None:
    assert uuid_result(uuid_param(value, Dialect.SQLITE)) == value


@given(value=st.uuids())
def test_uuid_result_agrees_across_representations(value: UUID) -> None:
    """
    `uuid_result` must return the same UUID regardless of which
    representation a driver hands back: str, UUID, bytes, bytearray, or
    memoryview of the UUID's 16-byte form.
    """
    as_str = uuid_result(str(value))
    as_uuid = uuid_result(value)
    as_bytes = uuid_result(value.bytes)
    as_bytearray = uuid_result(bytearray(value.bytes))
    as_memoryview = uuid_result(memoryview(value.bytes))

    assert as_str == value
    assert as_uuid == value
    assert as_bytes == value
    assert as_bytearray == value
    assert as_memoryview == value


@given(value=st.datetimes(timezones=st.timezones()))
def test_ts_roundtrip_sqlite_preserves_instant(value: datetime) -> None:
    """
    Aware datetimes round-trip to an equal instant through the SQLite path.

    Compared as instants (both sides normalized to UTC), not with bare `==`:
    for wall times whose UTC offset is fold-dependent (a DST gap or fold),
    PEP 495 defines interzone `==` as always False even when the instants
    match, so a bare `==` would reject correct round-trips of such inputs
    (e.g. 02:00 inside a spring-forward gap).
    """
    result = ts_result(ts_param(value, Dialect.SQLITE))
    assert result is not None
    assert result.tzinfo is not None
    assert result.astimezone(UTC) == value.astimezone(UTC)


@given(value=st.datetimes())
def test_ts_roundtrip_naive_datetime_becomes_utc_attached(value: datetime) -> None:
    """
    Documented behavior: a naive datetime does NOT round-trip to an equal
    (still-naive) datetime. `ts_result` attaches UTC, so the result is aware
    while the input was naive -- they are unequal, and the result's tzinfo
    is UTC.
    """
    result = ts_result(ts_param(value, Dialect.SQLITE))
    assert result is not None
    assert result.tzinfo is not None
    assert result != value
    assert result.replace(tzinfo=None) == value
