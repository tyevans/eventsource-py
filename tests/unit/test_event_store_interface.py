"""
Unit tests for event store interface and data structures.

Tests cover:
- StoredEvent data structure
- ReadOptions validation
- EventStream data structure
- AppendResult factory methods
- ExpectedVersion constants
- EventStore abstract methods (via mock implementation)
- EventPublisher protocol
"""

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from eventsource.events.base import DomainEvent
from eventsource.stores.interface import (
    AppendResult,
    EventPublisher,
    EventStore,
    EventStream,
    ExpectedVersion,
    ReadDirection,
    ReadOptions,
    StoredEvent,
)

# --- Test Fixtures ---


class SampleTestEvent(DomainEvent):
    """Sample event for unit tests."""

    event_type: str = "SampleTestEvent"
    aggregate_type: str = "TestAggregate"
    test_data: str = "test"


@pytest.fixture
def sample_event() -> SampleTestEvent:
    """Create a sample test event."""
    return SampleTestEvent(
        aggregate_id=uuid4(),
        test_data="sample",
    )


@pytest.fixture
def sample_events() -> list[SampleTestEvent]:
    """Create a list of sample test events."""
    aggregate_id = uuid4()
    return [SampleTestEvent(aggregate_id=aggregate_id, test_data=f"event_{i}") for i in range(3)]


# --- StoredEvent Tests ---


class TestStoredEvent:
    """Tests for StoredEvent data structure."""

    def test_create_stored_event(self, sample_event: SampleTestEvent) -> None:
        """Test creating a StoredEvent with all required fields."""
        stored_at = datetime.now(UTC)
        stored = StoredEvent(
            event=sample_event,
            stream_id="test-stream:TestAggregate",
            stream_position=1,
            global_position=100,
            stored_at=stored_at,
        )

        assert stored.event == sample_event
        assert stored.stream_id == "test-stream:TestAggregate"
        assert stored.stream_position == 1
        assert stored.global_position == 100
        assert stored.stored_at == stored_at

    def test_stored_event_properties(self, sample_event: SampleTestEvent) -> None:
        """Test StoredEvent convenience properties."""
        stored = StoredEvent(
            event=sample_event,
            stream_id="test-stream:TestAggregate",
            stream_position=1,
            global_position=100,
            stored_at=datetime.now(UTC),
        )

        assert stored.event_id == sample_event.event_id
        assert stored.event_type == sample_event.event_type
        assert stored.aggregate_id == sample_event.aggregate_id
        assert stored.aggregate_type == sample_event.aggregate_type

    def test_stored_event_str(self, sample_event: SampleTestEvent) -> None:
        """Test StoredEvent string representation."""
        stored = StoredEvent(
            event=sample_event,
            stream_id="test-stream:TestAggregate",
            stream_position=5,
            global_position=250,
            stored_at=datetime.now(UTC),
        )

        str_repr = str(stored)
        assert "SampleTestEvent" in str_repr
        assert "stream_pos=5" in str_repr
        assert "global_pos=250" in str_repr

    def test_stored_event_is_frozen(self, sample_event: SampleTestEvent) -> None:
        """Test that StoredEvent is immutable."""
        stored = StoredEvent(
            event=sample_event,
            stream_id="test-stream:TestAggregate",
            stream_position=1,
            global_position=100,
            stored_at=datetime.now(UTC),
        )

        with pytest.raises(AttributeError):
            stored.stream_position = 2  # type: ignore[misc]


# --- ReadOptions Tests ---


class TestReadOptions:
    """Tests for ReadOptions data structure."""

    def test_default_read_options(self) -> None:
        """Test ReadOptions with default values."""
        options = ReadOptions()

        assert options.direction == ReadDirection.FORWARD
        assert options.from_position == 0
        assert options.limit is None
        assert options.from_timestamp is None
        assert options.to_timestamp is None

    def test_custom_read_options(self) -> None:
        """Test ReadOptions with custom values."""
        from_ts = datetime.now(UTC)
        to_ts = datetime.now(UTC)
        options = ReadOptions(
            direction=ReadDirection.BACKWARD,
            from_position=50,
            limit=100,
            from_timestamp=from_ts,
            to_timestamp=to_ts,
        )

        assert options.direction == ReadDirection.BACKWARD
        assert options.from_position == 50
        assert options.limit == 100
        assert options.from_timestamp == from_ts
        assert options.to_timestamp == to_ts

    def test_read_options_negative_position_error(self) -> None:
        """Test that negative from_position (except -1) raises ValueError."""
        with pytest.raises(ValueError, match="from_position must be >= 0 or -1"):
            ReadOptions(from_position=-2)

    def test_read_options_end_position(self) -> None:
        """Test that -1 is accepted for from_position (meaning 'end')."""
        options = ReadOptions(from_position=-1)
        assert options.from_position == -1

    def test_read_options_negative_limit_error(self) -> None:
        """Test that negative limit raises ValueError."""
        with pytest.raises(ValueError, match="limit must be >= 0"):
            ReadOptions(limit=-1)

    def test_read_options_zero_limit(self) -> None:
        """Test that zero limit is accepted."""
        options = ReadOptions(limit=0)
        assert options.limit == 0

    def test_read_options_is_frozen(self) -> None:
        """Test that ReadOptions is immutable."""
        options = ReadOptions(limit=10)

        with pytest.raises(AttributeError):
            options.limit = 20  # type: ignore[misc]


class TestReadDirection:
    """Tests for ReadDirection enum."""

    def test_forward_direction(self) -> None:
        """Test forward direction value."""
        assert ReadDirection.FORWARD.value == "forward"

    def test_backward_direction(self) -> None:
        """Test backward direction value."""
        assert ReadDirection.BACKWARD.value == "backward"


# --- EventStream Tests ---


class TestEventStream:
    """Tests for EventStream data structure."""

    def test_create_empty_stream(self) -> None:
        """Test creating an empty EventStream."""
        aggregate_id = uuid4()
        stream = EventStream(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
        )

        assert stream.aggregate_id == aggregate_id
        assert stream.aggregate_type == "TestAggregate"
        assert stream.events == []
        assert stream.version == 0
        assert stream.is_empty is True
        assert stream.latest_event is None

    def test_create_stream_with_events(self, sample_events: list[SampleTestEvent]) -> None:
        """Test creating an EventStream with events."""
        aggregate_id = sample_events[0].aggregate_id
        stream = EventStream(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=sample_events,
            version=3,
        )

        assert stream.aggregate_id == aggregate_id
        assert stream.events == sample_events
        assert stream.version == 3
        assert stream.is_empty is False
        assert stream.latest_event == sample_events[-1]

    def test_event_stream_empty_factory(self) -> None:
        """Test EventStream.empty() factory method."""
        aggregate_id = uuid4()
        stream = EventStream.empty(aggregate_id, "TestAggregate")

        assert stream.aggregate_id == aggregate_id
        assert stream.aggregate_type == "TestAggregate"
        assert stream.events == []
        assert stream.version == 0

    def test_event_stream_is_frozen(self, sample_events: list[SampleTestEvent]) -> None:
        """Test that EventStream is immutable."""
        stream = EventStream(
            aggregate_id=uuid4(),
            aggregate_type="TestAggregate",
            events=sample_events,
            version=3,
        )

        with pytest.raises(AttributeError):
            stream.version = 5  # type: ignore[misc]

    def test_event_stream_handles_none_events(self) -> None:
        """Test that EventStream handles None events gracefully."""
        aggregate_id = uuid4()
        # Bypass type checking to test None handling
        stream = EventStream(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=None,  # type: ignore[arg-type]
            version=0,
        )

        assert stream.events == []
        assert stream.is_empty is True


# --- AppendResult Tests ---


class TestAppendResult:
    """Tests for AppendResult data structure."""

    def test_create_append_result(self) -> None:
        """Test creating an AppendResult manually."""
        result = AppendResult(
            success=True,
            new_version=5,
            global_position=100,
            conflict=False,
        )

        assert result.success is True
        assert result.new_version == 5
        assert result.global_position == 100
        assert result.conflict is False

    def test_successful_factory(self) -> None:
        """Test AppendResult.successful() factory method."""
        result = AppendResult.successful(new_version=10, global_position=500)

        assert result.success is True
        assert result.new_version == 10
        assert result.global_position == 500
        assert result.conflict is False

    def test_successful_factory_default_position(self) -> None:
        """Test AppendResult.successful() with default global position."""
        result = AppendResult.successful(new_version=10)

        assert result.success is True
        assert result.new_version == 10
        assert result.global_position == 0
        assert result.conflict is False

    def test_conflicted_factory(self) -> None:
        """Test AppendResult.conflicted() factory method."""
        result = AppendResult.conflicted(current_version=7)

        assert result.success is False
        assert result.new_version == 7
        assert result.global_position == 0
        assert result.conflict is True

    def test_append_result_is_frozen(self) -> None:
        """Test that AppendResult is immutable."""
        result = AppendResult.successful(new_version=5)

        with pytest.raises(AttributeError):
            result.success = False  # type: ignore[misc]


# --- ExpectedVersion Tests ---


class TestExpectedVersion:
    """Tests for ExpectedVersion constants."""

    def test_any_version(self) -> None:
        """Test ANY version constant."""
        assert ExpectedVersion.ANY == -1

    def test_no_stream_version(self) -> None:
        """Test NO_STREAM version constant."""
        assert ExpectedVersion.NO_STREAM == 0

    def test_stream_exists_version(self) -> None:
        """Test STREAM_EXISTS version constant."""
        assert ExpectedVersion.STREAM_EXISTS == -2


# --- EventStore Abstract Base Class Tests ---


class MockEventStore(EventStore):
    """Mock EventStore implementation for testing."""

    def __init__(self) -> None:
        self._events: dict[UUID, list[DomainEvent]] = {}
        self._event_ids: set[UUID] = set()

    async def append_events(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
        events: list[DomainEvent],
        expected_version: int,
    ) -> AppendResult:
        if aggregate_id not in self._events:
            self._events[aggregate_id] = []

        for event in events:
            self._events[aggregate_id].append(event)
            self._event_ids.add(event.event_id)

        return AppendResult.successful(
            new_version=len(self._events[aggregate_id]),
            global_position=sum(len(e) for e in self._events.values()),
        )

    async def get_events(
        self,
        aggregate_id: UUID,
        aggregate_type: str | None = None,
        from_version: int = 0,
        from_timestamp: datetime | None = None,
        to_timestamp: datetime | None = None,
    ) -> EventStream:
        events = self._events.get(aggregate_id, [])
        if from_version > 0:
            events = events[from_version:]
        return EventStream(
            aggregate_id=aggregate_id,
            aggregate_type=aggregate_type or "Unknown",
            events=events,
            version=len(events),
        )

    async def get_events_by_type(
        self,
        aggregate_type: str,
        tenant_id: UUID | None = None,
        from_timestamp: float | None = None,
    ) -> list[DomainEvent]:
        result: list[DomainEvent] = []
        for events in self._events.values():
            for event in events:
                if event.aggregate_type == aggregate_type and (
                    tenant_id is None or event.tenant_id == tenant_id
                ):
                    result.append(event)
        return result

    async def event_exists(self, event_id: UUID) -> bool:
        return event_id in self._event_ids

    async def get_global_position(self) -> int:
        return sum(len(e) for e in self._events.values())


class TestEventStore:
    """Tests for EventStore abstract base class."""

    @pytest.fixture
    def store(self) -> MockEventStore:
        """Create a mock event store."""
        return MockEventStore()

    @pytest.mark.asyncio
    async def test_append_events(
        self, store: MockEventStore, sample_events: list[SampleTestEvent]
    ) -> None:
        """Test appending events to the store."""
        aggregate_id = sample_events[0].aggregate_id

        result = await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=sample_events,
            expected_version=0,
        )

        assert result.success is True
        assert result.new_version == 3

    @pytest.mark.asyncio
    async def test_get_events(
        self, store: MockEventStore, sample_events: list[SampleTestEvent]
    ) -> None:
        """Test getting events from the store."""
        aggregate_id = sample_events[0].aggregate_id

        # First append
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=sample_events,
            expected_version=0,
        )

        # Then get
        stream = await store.get_events(aggregate_id, "TestAggregate")

        assert stream.aggregate_id == aggregate_id
        assert len(stream.events) == 3
        assert stream.version == 3

    @pytest.mark.asyncio
    async def test_get_events_empty_stream(self, store: MockEventStore) -> None:
        """Test getting events from empty stream."""
        stream = await store.get_events(uuid4(), "TestAggregate")

        assert stream.is_empty is True
        assert stream.version == 0

    @pytest.mark.asyncio
    async def test_get_stream_version(
        self, store: MockEventStore, sample_events: list[SampleTestEvent]
    ) -> None:
        """Test get_stream_version default implementation."""
        aggregate_id = sample_events[0].aggregate_id

        # First append
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=sample_events,
            expected_version=0,
        )

        # Check version
        version = await store.get_stream_version(aggregate_id, "TestAggregate")
        assert version == 3

    @pytest.mark.asyncio
    async def test_event_exists(
        self, store: MockEventStore, sample_events: list[SampleTestEvent]
    ) -> None:
        """Test checking if event exists."""
        aggregate_id = sample_events[0].aggregate_id

        # First append
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=sample_events,
            expected_version=0,
        )

        # Check existing
        exists = await store.event_exists(sample_events[0].event_id)
        assert exists is True

        # Check non-existing
        exists = await store.event_exists(uuid4())
        assert exists is False

    @pytest.mark.asyncio
    async def test_get_events_by_type(
        self, store: MockEventStore, sample_events: list[SampleTestEvent]
    ) -> None:
        """Test getting events by aggregate type."""
        aggregate_id = sample_events[0].aggregate_id

        # First append
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=sample_events,
            expected_version=0,
        )

        # Get by type
        events = await store.get_events_by_type("TestAggregate")
        assert len(events) == 3

        # Get by non-existing type
        events = await store.get_events_by_type("NonExistentAggregate")
        assert len(events) == 0

    @pytest.mark.asyncio
    async def test_read_stream(
        self, store: MockEventStore, sample_events: list[SampleTestEvent]
    ) -> None:
        """Test read_stream default implementation."""
        aggregate_id = sample_events[0].aggregate_id

        # First append
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=sample_events,
            expected_version=0,
        )

        # Read stream
        stream_id = f"{aggregate_id}:TestAggregate"
        stored_events = []
        async for stored in store.read_stream(stream_id):
            stored_events.append(stored)

        assert len(stored_events) == 3
        assert all(isinstance(e, StoredEvent) for e in stored_events)
        assert stored_events[0].stream_position == 1
        assert stored_events[1].stream_position == 2
        assert stored_events[2].stream_position == 3

    @pytest.mark.asyncio
    async def test_read_stream_with_limit(
        self, store: MockEventStore, sample_events: list[SampleTestEvent]
    ) -> None:
        """Test read_stream with limit option."""
        aggregate_id = sample_events[0].aggregate_id

        # First append
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=sample_events,
            expected_version=0,
        )

        # Read stream with limit
        stream_id = f"{aggregate_id}:TestAggregate"
        options = ReadOptions(limit=2)
        stored_events = []
        async for stored in store.read_stream(stream_id, options):
            stored_events.append(stored)

        assert len(stored_events) == 2

    @pytest.mark.asyncio
    async def test_read_stream_backward(
        self, store: MockEventStore, sample_events: list[SampleTestEvent]
    ) -> None:
        """Test read_stream in backward direction."""
        aggregate_id = sample_events[0].aggregate_id

        # First append
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=sample_events,
            expected_version=0,
        )

        # Read stream backward
        stream_id = f"{aggregate_id}:TestAggregate"
        options = ReadOptions(direction=ReadDirection.BACKWARD)
        stored_events = []
        async for stored in store.read_stream(stream_id, options):
            stored_events.append(stored)

        assert len(stored_events) == 3
        # Events should be in reverse order
        assert stored_events[0].event == sample_events[2]
        assert stored_events[1].event == sample_events[1]
        assert stored_events[2].event == sample_events[0]

    @pytest.mark.asyncio
    async def test_read_all_not_implemented(self, store: MockEventStore) -> None:
        """Test that read_all raises NotImplementedError by default."""
        with pytest.raises(NotImplementedError):
            async for _ in store.read_all():
                pass


# --- EventPublisher Protocol Tests ---


class MockPublisher:
    """Mock implementation of EventPublisher protocol."""

    def __init__(self) -> None:
        self.published_events: list[DomainEvent] = []

    async def publish(self, events: list[DomainEvent]) -> None:
        self.published_events.extend(events)


class TestEventPublisher:
    """Tests for EventPublisher protocol."""

    @pytest.mark.asyncio
    async def test_publisher_protocol(self, sample_events: list[SampleTestEvent]) -> None:
        """Test that a class implementing publish() satisfies the protocol."""
        publisher: EventPublisher = MockPublisher()

        # This should work because MockPublisher implements the protocol
        await publisher.publish(sample_events)

        # Verify events were published
        assert isinstance(publisher, MockPublisher)
        assert len(publisher.published_events) == 3


# --- Import Tests ---


class TestImports:
    """Tests to verify all exports are accessible."""

    def test_import_from_stores_module(self) -> None:
        """Test importing from eventsource.stores module."""
        from eventsource.stores import (
            AppendResult,
            EventPublisher,
            EventStore,
            EventStream,
            ExpectedVersion,
            ReadDirection,
            ReadOptions,
            StoredEvent,
        )

        assert AppendResult is not None
        assert EventPublisher is not None
        assert EventStore is not None
        assert EventStream is not None
        assert ExpectedVersion is not None
        assert ReadDirection is not None
        assert ReadOptions is not None
        assert StoredEvent is not None

    def test_import_from_main_module(self) -> None:
        """Test importing from main eventsource module."""
        from eventsource import (
            AppendResult,
            EventPublisher,
            EventStore,
            EventStream,
            ExpectedVersion,
            ReadDirection,
            ReadOptions,
            StoredEvent,
        )

        assert AppendResult is not None
        assert EventPublisher is not None
        assert EventStore is not None
        assert EventStream is not None
        assert ExpectedVersion is not None
        assert ReadDirection is not None
        assert ReadOptions is not None
        assert StoredEvent is not None

    def test_sync_event_store_not_present(self) -> None:
        """Verify SyncEventStore has been removed."""
        import eventsource.stores.interface as interface

        assert not hasattr(interface, "SyncEventStore")
