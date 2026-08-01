"""
Unit tests for timestamp type validation.

This module verified `stores/_compat.py::validate_timestamp` and the legacy
stores' timestamp handling. Both die with the legacy surface (spec §5.2):

- The `validate_timestamp`-specific cases died with the helper.
- `test_get_events_by_type_with_datetime` is already covered by
  `CategoryQueryConformance.test_from_timestamp_honored` (which additionally
  asserts inclusivity, ordering, and tenant/limit interaction the legacy
  case never checked) and is dropped as duplicate.
- `test_get_events_uses_datetime_consistently` exercised `from_timestamp` on
  a *stream* read; per spec §1.2 that pushdown has no ports equivalent and
  died with the parameter.
- `TestPostgreSQLEventStoreTimestampTypes` mocked the legacy
  `PostgreSQLEventStore`'s session factory directly to assert it forwarded a
  `datetime` (not a float/int) to the query; the adapter has no such
  compatibility shim to test.

`test_get_events_by_type_none_timestamp` is retained below: "no filter
returns every event" is a genuinely uncovered case in
`CategoryQueryConformance` (its suite always supplies a `from_timestamp`
when testing the filter), so it is retargeted onto `InMemoryEventStore`
rather than deleted.
"""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from eventsource.adapters.memory import InMemoryEventStore
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.ports import ExpectedVersion, collect


class SampleEvent(DomainEvent):
    """Sample event for testing."""

    event_type: str = "SampleEvent"
    aggregate_type: str = "TestAggregate"
    data: str = "test"


class TestInMemoryEventStoreTimestampTypes:
    """Tests for timestamp handling in InMemoryEventStore."""

    @pytest.mark.asyncio
    async def test_get_events_by_type_none_timestamp(self) -> None:
        """read_category with no from_timestamp filter returns all events."""
        store = InMemoryEventStore()

        event1 = SampleEvent(
            aggregate_id=uuid4(),
            aggregate_type="TestAggregate",
            occurred_at=datetime.now(UTC) - timedelta(hours=1),
            data="first",
        )
        event2 = SampleEvent(
            aggregate_id=uuid4(),
            aggregate_type="TestAggregate",
            occurred_at=datetime.now(UTC),
            data="second",
        )

        await store.append(
            StreamId(aggregate_id=event1.aggregate_id, category="TestAggregate"),
            [event1],
            ExpectedVersion.no_stream(),
        )
        await store.append(
            StreamId(aggregate_id=event2.aggregate_id, category="TestAggregate"),
            [event2],
            ExpectedVersion.no_stream(),
        )

        # No timestamp filter - should return all events
        envelopes = await collect(store.read_category("TestAggregate"))

        assert len(envelopes) == 2
