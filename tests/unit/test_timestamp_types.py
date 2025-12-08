"""
Unit tests for timestamp type normalization and unification.

This module tests the normalize_timestamp helper and verifies that
both InMemoryEventStore and PostgreSQLEventStore correctly handle
datetime and deprecated float timestamps.
"""

import time
import warnings
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from eventsource.events.base import DomainEvent
from eventsource.stores._compat import normalize_timestamp
from eventsource.stores.in_memory import InMemoryEventStore


class TestNormalizeTimestamp:
    """Tests for the normalize_timestamp helper function."""

    def test_none_returns_none(self) -> None:
        """None timestamp passes through unchanged."""
        result = normalize_timestamp(None, "test_param")
        assert result is None

    def test_datetime_returns_unchanged(self) -> None:
        """datetime parameter passes through unchanged."""
        now = datetime.now(UTC)
        result = normalize_timestamp(now, "test_param")
        assert result is now
        assert result == now

    def test_datetime_no_warning(self) -> None:
        """datetime parameter does not emit deprecation warning."""
        now = datetime.now(UTC)
        with warnings.catch_warnings():
            warnings.simplefilter("error")  # Fail on any warning
            result = normalize_timestamp(now, "test_param")
        assert result == now

    def test_float_converts_to_datetime(self) -> None:
        """float timestamp is correctly converted to datetime."""
        now = datetime.now(UTC)
        unix_ts = now.timestamp()

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")  # Suppress the warning
            result = normalize_timestamp(unix_ts, "test_param")

        assert isinstance(result, datetime)
        # Allow for floating point precision differences
        assert abs((result - now).total_seconds()) < 0.001

    def test_float_emits_deprecation_warning(self) -> None:
        """float timestamp emits DeprecationWarning with correct message."""
        unix_ts = time.time()

        with pytest.warns(DeprecationWarning, match="float.*deprecated"):
            normalize_timestamp(unix_ts, "from_timestamp")

    def test_float_warning_mentions_param_name(self) -> None:
        """Deprecation warning includes the parameter name."""
        unix_ts = time.time()

        with pytest.warns(DeprecationWarning, match="from_timestamp"):
            normalize_timestamp(unix_ts, "from_timestamp")

    def test_float_warning_mentions_fix(self) -> None:
        """Deprecation warning includes the fix suggestion."""
        unix_ts = time.time()

        with pytest.warns(DeprecationWarning, match="datetime.fromtimestamp"):
            normalize_timestamp(unix_ts, "test_param")

    def test_int_converts_to_datetime(self) -> None:
        """int timestamp is correctly converted to datetime."""
        now = datetime.now(UTC)
        unix_ts = int(now.timestamp())

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            result = normalize_timestamp(unix_ts, "test_param")

        assert isinstance(result, datetime)
        # Allow for int truncation differences
        assert abs((result - now).total_seconds()) < 1

    def test_int_emits_deprecation_warning(self) -> None:
        """int timestamp emits DeprecationWarning."""
        unix_ts = int(time.time())

        with pytest.warns(DeprecationWarning):
            normalize_timestamp(unix_ts, "test_param")

    def test_invalid_type_raises_typeerror(self) -> None:
        """Invalid type raises TypeError with descriptive message."""
        with pytest.raises(TypeError, match="must be datetime"):
            normalize_timestamp("not-a-timestamp", "test_param")  # type: ignore[arg-type]

    def test_invalid_type_error_includes_actual_type(self) -> None:
        """TypeError message includes the actual type received."""
        with pytest.raises(TypeError, match="str"):
            normalize_timestamp("not-a-timestamp", "test_param")  # type: ignore[arg-type]


class SampleEvent(DomainEvent):
    """Sample event for testing."""

    event_type: str = "SampleEvent"
    aggregate_type: str = "TestAggregate"
    data: str = "test"


class TestInMemoryEventStoreTimestampTypes:
    """Tests for timestamp type handling in InMemoryEventStore."""

    @pytest.mark.asyncio
    async def test_get_events_by_type_with_datetime(self) -> None:
        """get_events_by_type works with datetime parameter."""
        store = InMemoryEventStore()

        # Create events at different times
        now = datetime.now(UTC)
        old_event = SampleEvent(
            aggregate_id=uuid4(),
            aggregate_type="TestAggregate",
            occurred_at=now - timedelta(hours=2),
            data="old",
        )
        new_event = SampleEvent(
            aggregate_id=uuid4(),
            aggregate_type="TestAggregate",
            occurred_at=now,
            data="new",
        )

        await store.append_events(
            old_event.aggregate_id,
            "TestAggregate",
            [old_event],
            expected_version=0,
        )
        await store.append_events(
            new_event.aggregate_id,
            "TestAggregate",
            [new_event],
            expected_version=0,
        )

        # Filter using datetime - should not emit warning
        one_hour_ago = now - timedelta(hours=1)
        with warnings.catch_warnings():
            warnings.simplefilter("error")  # Fail on any warning
            events = await store.get_events_by_type(
                "TestAggregate",
                from_timestamp=one_hour_ago,
            )

        assert len(events) == 1
        assert isinstance(events[0], SampleEvent)
        assert events[0].data == "new"

    @pytest.mark.asyncio
    async def test_get_events_by_type_with_float_emits_warning(self) -> None:
        """get_events_by_type with float timestamp emits deprecation warning."""
        store = InMemoryEventStore()

        now = datetime.now(UTC)
        event = SampleEvent(
            aggregate_id=uuid4(),
            aggregate_type="TestAggregate",
            occurred_at=now,
            data="test",
        )
        await store.append_events(
            event.aggregate_id,
            "TestAggregate",
            [event],
            expected_version=0,
        )

        # Filter using float - should emit warning
        one_hour_ago = (now - timedelta(hours=1)).timestamp()
        with pytest.warns(DeprecationWarning, match="from_timestamp.*deprecated"):
            events = await store.get_events_by_type(
                "TestAggregate",
                from_timestamp=one_hour_ago,
            )

        assert len(events) == 1

    @pytest.mark.asyncio
    async def test_get_events_by_type_float_works_correctly(self) -> None:
        """Float timestamp filters events correctly (backward compatibility)."""
        store = InMemoryEventStore()

        now = datetime.now(UTC)
        old_event = SampleEvent(
            aggregate_id=uuid4(),
            aggregate_type="TestAggregate",
            occurred_at=now - timedelta(hours=2),
            data="old",
        )
        new_event = SampleEvent(
            aggregate_id=uuid4(),
            aggregate_type="TestAggregate",
            occurred_at=now,
            data="new",
        )

        await store.append_events(
            old_event.aggregate_id,
            "TestAggregate",
            [old_event],
            expected_version=0,
        )
        await store.append_events(
            new_event.aggregate_id,
            "TestAggregate",
            [new_event],
            expected_version=0,
        )

        # Filter using float - should work despite warning
        one_hour_ago = (now - timedelta(hours=1)).timestamp()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            events = await store.get_events_by_type(
                "TestAggregate",
                from_timestamp=one_hour_ago,
            )

        # Should only get the new event
        assert len(events) == 1
        assert isinstance(events[0], SampleEvent)
        assert events[0].data == "new"

    @pytest.mark.asyncio
    async def test_get_events_by_type_none_timestamp(self) -> None:
        """get_events_by_type with None timestamp returns all events."""
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

        await store.append_events(
            event1.aggregate_id,
            "TestAggregate",
            [event1],
            expected_version=0,
        )
        await store.append_events(
            event2.aggregate_id,
            "TestAggregate",
            [event2],
            expected_version=0,
        )

        # No timestamp filter - should return all events
        events = await store.get_events_by_type(
            "TestAggregate",
            from_timestamp=None,
        )

        assert len(events) == 2

    @pytest.mark.asyncio
    async def test_get_events_uses_datetime_consistently(self) -> None:
        """get_events method uses datetime consistently (already correct)."""
        store = InMemoryEventStore()

        now = datetime.now(UTC)
        event = SampleEvent(
            aggregate_id=uuid4(),
            aggregate_type="TestAggregate",
            occurred_at=now,
            data="test",
        )

        await store.append_events(
            event.aggregate_id,
            "TestAggregate",
            [event],
            expected_version=0,
        )

        # get_events already uses datetime - no warning expected
        one_hour_ago = now - timedelta(hours=1)
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            stream = await store.get_events(
                event.aggregate_id,
                "TestAggregate",
                from_timestamp=one_hour_ago,
            )

        assert len(stream.events) == 1


class TestPostgreSQLEventStoreTimestampTypes:
    """Tests for timestamp type handling in PostgreSQLEventStore."""

    @pytest.mark.asyncio
    async def test_get_events_by_type_with_datetime(self) -> None:
        """get_events_by_type works with datetime parameter."""
        from eventsource.stores.postgresql import PostgreSQLEventStore

        # Create mock session factory
        mock_session = AsyncMock()
        mock_session.execute.return_value = MagicMock(fetchall=lambda: [])
        mock_session_factory = MagicMock()
        mock_session_factory.return_value.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session_factory.return_value.__aexit__ = AsyncMock(return_value=None)

        store = PostgreSQLEventStore(
            mock_session_factory,
            enable_tracing=False,
        )

        # Call with datetime - should not emit warning
        now = datetime.now(UTC)
        one_hour_ago = now - timedelta(hours=1)

        with warnings.catch_warnings():
            warnings.simplefilter("error")
            await store.get_events_by_type(
                "TestAggregate",
                from_timestamp=one_hour_ago,
            )

        # Verify the query was called with the datetime
        mock_session.execute.assert_called_once()
        call_args = mock_session.execute.call_args
        params = call_args[0][1]
        assert params["from_timestamp"] == one_hour_ago

    @pytest.mark.asyncio
    async def test_get_events_by_type_with_float_emits_warning(self) -> None:
        """get_events_by_type with float emits deprecation warning."""
        from eventsource.stores.postgresql import PostgreSQLEventStore

        # Create mock session factory
        mock_session = AsyncMock()
        mock_session.execute.return_value = MagicMock(fetchall=lambda: [])
        mock_session_factory = MagicMock()
        mock_session_factory.return_value.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session_factory.return_value.__aexit__ = AsyncMock(return_value=None)

        store = PostgreSQLEventStore(
            mock_session_factory,
            enable_tracing=False,
        )

        # Call with float - should emit warning
        now = datetime.now(UTC)
        one_hour_ago = (now - timedelta(hours=1)).timestamp()

        with pytest.warns(DeprecationWarning, match="from_timestamp.*deprecated"):
            await store.get_events_by_type(
                "TestAggregate",
                from_timestamp=one_hour_ago,
            )

    @pytest.mark.asyncio
    async def test_get_events_by_type_float_converts_correctly(self) -> None:
        """Float timestamp is converted to datetime for the query."""
        from eventsource.stores.postgresql import PostgreSQLEventStore

        # Create mock session factory
        mock_session = AsyncMock()
        mock_session.execute.return_value = MagicMock(fetchall=lambda: [])
        mock_session_factory = MagicMock()
        mock_session_factory.return_value.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session_factory.return_value.__aexit__ = AsyncMock(return_value=None)

        store = PostgreSQLEventStore(
            mock_session_factory,
            enable_tracing=False,
        )

        # Call with float
        now = datetime.now(UTC)
        one_hour_ago_float = (now - timedelta(hours=1)).timestamp()

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            await store.get_events_by_type(
                "TestAggregate",
                from_timestamp=one_hour_ago_float,
            )

        # Verify the query was called with a datetime (not float)
        mock_session.execute.assert_called_once()
        call_args = mock_session.execute.call_args
        params = call_args[0][1]
        assert isinstance(params["from_timestamp"], datetime)
