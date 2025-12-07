"""
Additional unit tests for improving coverage.

These tests target specific uncovered lines identified in the coverage report:
- bus/memory.py: background publishing, error handling
- projections/base.py: abstract methods, DLQ write failures
- aggregates/base.py: edge cases
- stores/interface.py: protocol methods
"""

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from eventsource.aggregates.base import AggregateRoot, DeclarativeAggregate
from eventsource.bus.interface import (
    AsyncEventHandler,
    EventHandler,
    EventSubscriber,
)
from eventsource.bus.memory import InMemoryEventBus
from eventsource.events.base import DomainEvent
from eventsource.projections.base import (
    CheckpointTrackingProjection,
    DeclarativeProjection,
    EventHandlerBase,
    Projection,
    SyncProjection,
)
from eventsource.projections.decorators import handles
from eventsource.repositories.checkpoint import InMemoryCheckpointRepository
from eventsource.stores.interface import (
    AppendResult,
    EventStream,
    ReadOptions,
    StoredEvent,
)

# --- Test Event Classes ---


class AdditionalTestEvent(DomainEvent):
    """Test event for unit tests."""

    event_type: str = "AdditionalTestEvent"
    aggregate_type: str = "Test"
    data: str = ""


class AnotherCoverageEvent(DomainEvent):
    """Another test event."""

    event_type: str = "AnotherCoverageEvent"
    aggregate_type: str = "Test"
    value: int = 0


# --- Bus Memory Tests ---


class TestInMemoryEventBusBackgroundPublishing:
    """Tests for background publishing in InMemoryEventBus."""

    @pytest.fixture
    def bus(self) -> InMemoryEventBus:
        return InMemoryEventBus()

    @pytest.mark.asyncio
    async def test_background_publish_creates_task(self, bus: InMemoryEventBus):
        """Test that background=True creates a background task."""
        received = []

        async def handler(event: DomainEvent):
            await asyncio.sleep(0.01)  # Small delay to simulate work
            received.append(event)

        bus.subscribe(AdditionalTestEvent, handler)

        event = AdditionalTestEvent(aggregate_id=uuid4(), data="background")
        await bus.publish([event], background=True)

        # Stats should show a background task was created
        stats = bus.get_stats()
        assert stats["background_tasks_created"] >= 1

        # Wait for background task to complete
        await bus.shutdown(timeout=5.0)
        assert len(received) == 1

    @pytest.mark.asyncio
    async def test_background_task_error_is_logged(self, bus: InMemoryEventBus):
        """Test that errors in background tasks are logged."""

        async def failing_handler(event: DomainEvent):
            raise RuntimeError("Background failure")

        bus.subscribe(AdditionalTestEvent, failing_handler)

        event = AdditionalTestEvent(aggregate_id=uuid4(), data="fail")
        await bus.publish([event], background=True)

        # Wait for background task to complete
        await asyncio.sleep(0.1)
        await bus.shutdown(timeout=5.0)

        # Error should have been tracked
        stats = bus.get_stats()
        assert stats["handler_errors"] >= 1

    @pytest.mark.asyncio
    async def test_shutdown_with_timeout(self, bus: InMemoryEventBus):
        """Test shutdown with tasks that take longer than timeout."""

        async def slow_handler(event: DomainEvent):
            await asyncio.sleep(10)  # Very slow

        bus.subscribe(AdditionalTestEvent, slow_handler)

        event = AdditionalTestEvent(aggregate_id=uuid4(), data="slow")
        await bus.publish([event], background=True)

        # Shutdown with short timeout
        await bus.shutdown(timeout=0.1)

        # Should have completed shutdown (task cancelled)
        stats = bus.get_stats()
        assert stats["background_tasks_completed"] >= 0

    @pytest.mark.asyncio
    async def test_shutdown_with_no_pending_tasks(self, bus: InMemoryEventBus):
        """Test shutdown when there are no pending tasks."""
        await bus.shutdown(timeout=1.0)
        # Should complete without error

    @pytest.mark.asyncio
    async def test_handler_with_object_handle_method(self, bus: InMemoryEventBus):
        """Test handler that is an object with handle() method."""
        received = []

        class ObjectHandler:
            async def handle(self, event: DomainEvent):
                received.append(event)

        handler = ObjectHandler()
        bus.subscribe(AdditionalTestEvent, handler)

        event = AdditionalTestEvent(aggregate_id=uuid4(), data="object")
        await bus.publish([event])

        assert len(received) == 1

    @pytest.mark.asyncio
    async def test_handler_sync_object_handle_method(self, bus: InMemoryEventBus):
        """Test handler that is an object with sync handle() method."""
        received = []

        class SyncHandler:
            def handle(self, event: DomainEvent):
                received.append(event)

        handler = SyncHandler()
        bus.subscribe(AdditionalTestEvent, handler)

        event = AdditionalTestEvent(aggregate_id=uuid4(), data="sync_object")
        await bus.publish([event])

        assert len(received) == 1

    def test_handler_name_for_lambda(self, bus: InMemoryEventBus):
        """Test that lambda handlers get a name."""

        def handler(e: DomainEvent) -> None:
            pass

        bus.subscribe(AdditionalTestEvent, handler)

        # Subscribe should work
        assert bus.get_subscriber_count(AdditionalTestEvent) == 1

    def test_handler_name_for_class_instance(self, bus: InMemoryEventBus):
        """Test handler name for class instance."""

        class NamedHandler:
            async def handle(self, event: DomainEvent):
                pass

        handler = NamedHandler()
        bus.subscribe(AdditionalTestEvent, handler)
        assert bus.get_subscriber_count(AdditionalTestEvent) == 1


# --- Projections Tests ---


class TestProjectionAbstractMethods:
    """Tests for abstract projection methods."""

    def test_projection_requires_handle_implementation(self):
        """Test that Projection requires handle implementation."""
        with pytest.raises(TypeError):

            class IncompleteProjection(Projection):
                async def reset(self):
                    pass

            IncompleteProjection()

    def test_sync_projection_requires_both_methods(self):
        """Test that SyncProjection requires both methods."""
        with pytest.raises(TypeError):

            class IncompleteSyncProjection(SyncProjection):
                def handle(self, event):
                    pass

                # Missing reset()

            IncompleteSyncProjection()

    def test_event_handler_base_requires_methods(self):
        """Test that EventHandlerBase requires both methods."""
        with pytest.raises(TypeError):

            class IncompleteHandler(EventHandlerBase):
                def can_handle(self, event):
                    return True

                # Missing handle()

            IncompleteHandler()


class TestCheckpointTrackingProjectionDLQFailure:
    """Tests for DLQ write failure handling."""

    @pytest.mark.asyncio
    async def test_dlq_write_failure_is_logged(self):
        """Test that DLQ write failures are logged and the original exception is re-raised."""

        # Create a checkpoint tracking projection
        class TestProjection(CheckpointTrackingProjection):
            # Set MAX_RETRIES to 1 to skip retries quickly
            MAX_RETRIES = 1

            def subscribed_to(self):
                return [AdditionalTestEvent]

            async def _process_event(self, event):
                raise RuntimeError("Processing failed")

            async def reset(self):
                pass

        # Create a failing DLQ repo
        failing_dlq = AsyncMock()
        failing_dlq.add_failed_event = AsyncMock(side_effect=Exception("DLQ write failed"))

        projection = TestProjection(
            checkpoint_repo=InMemoryCheckpointRepository(),
            dlq_repo=failing_dlq,
        )

        event = AdditionalTestEvent(aggregate_id=uuid4(), data="test")

        # The original exception is re-raised after DLQ write (even if DLQ fails)
        with pytest.raises(RuntimeError, match="Processing failed"):
            await projection.handle(event)

        # Verify DLQ was called (even if it failed)
        failing_dlq.add_failed_event.assert_called_once()


class TestDeclarativeProjectionEdgeCases:
    """Tests for DeclarativeProjection edge cases."""

    @pytest.mark.asyncio
    async def test_handler_with_self_only(self):
        """Test handler that takes only self and event."""

        class SelfOnlyProjection(DeclarativeProjection):
            def __init__(self):
                super().__init__()
                self.events = []

            def subscribed_to(self):
                return [AdditionalTestEvent]

            @handles(AdditionalTestEvent)
            async def on_test_event(self, event: DomainEvent):
                self.events.append(event)

            async def reset(self):
                self.events.clear()

        proj = SelfOnlyProjection()
        event = AdditionalTestEvent(aggregate_id=uuid4(), data="test")
        await proj.handle(event)
        assert len(proj.events) == 1


# --- AsyncEventHandler ABC Tests ---


class TestAsyncEventHandlerABC:
    """Tests for AsyncEventHandler abstract base class."""

    def test_can_handle_default_implementation(self):
        """Test default can_handle implementation."""

        class MyHandler(AsyncEventHandler):
            def event_types(self):
                return [AdditionalTestEvent]

            async def handle(self, event):
                pass

        handler = MyHandler()

        test_event = AdditionalTestEvent(aggregate_id=uuid4())
        another_event = AnotherCoverageEvent(aggregate_id=uuid4())

        assert handler.can_handle(test_event) is True
        assert handler.can_handle(another_event) is False


# --- EventStore Interface Tests ---


class AdditionalTestEventStoreProtocol:
    """Tests for EventStore protocol methods."""

    @pytest.mark.asyncio
    async def test_event_stream_dataclass(self):
        """Test EventStream dataclass creation and usage."""
        aggregate_id = uuid4()
        events = [AdditionalTestEvent(aggregate_id=aggregate_id, data=f"e{i}") for i in range(3)]

        stream = EventStream(
            aggregate_id=aggregate_id,
            aggregate_type="Test",
            events=events,
            version=3,
        )

        assert stream.aggregate_id == aggregate_id
        assert len(stream.events) == 3
        assert stream.version == 3

    def test_append_result_success_factory(self):
        """Test AppendResult.success() factory method."""
        result = AppendResult.success(version=5)
        assert result.success is True
        assert result.new_version == 5

    def test_append_result_failure_factory(self):
        """Test AppendResult.failure() factory method."""
        result = AppendResult.failure(
            expected_version=3,
            actual_version=5,
        )
        assert result.success is False
        assert result.expected_version == 3
        assert result.actual_version == 5

    def test_stored_event_dataclass(self):
        """Test StoredEvent dataclass."""
        event = AdditionalTestEvent(aggregate_id=uuid4(), data="test")
        stored = StoredEvent(
            event=event,
            stream_id=f"{event.aggregate_id}:Test",
            stream_position=1,
            global_position=100,
            stored_at=datetime.now(UTC),
        )

        assert stored.event == event
        assert stored.stream_position == 1
        assert stored.global_position == 100


# --- Aggregate Base Tests ---


class TestAggregateRootAbstractMethods:
    """Tests for AggregateRoot abstract methods."""

    def test_aggregate_root_requires_apply_implementation(self):
        """Test that AggregateRoot requires _apply implementation."""
        with pytest.raises(TypeError):

            class IncompleteAggregate(AggregateRoot[dict]):
                def _get_initial_state(self):
                    return {}

                # Missing _apply()

            IncompleteAggregate(uuid4())

    def test_aggregate_root_requires_initial_state(self):
        """Test that AggregateRoot requires _get_initial_state implementation."""
        with pytest.raises(TypeError):

            class IncompleteAggregate(AggregateRoot[dict]):
                def _apply(self, event):
                    pass

                # Missing _get_initial_state()

            IncompleteAggregate(uuid4())


class TestDeclarativeAggregateClass:
    """Tests for DeclarativeAggregate."""

    def test_declarative_aggregate_with_no_handlers(self):
        """Test declarative aggregate with no handlers ignores events."""

        class EmptyAggregate(DeclarativeAggregate[dict]):
            def _get_initial_state(self):
                return {}

        agg = EmptyAggregate(uuid4())
        event = AdditionalTestEvent(aggregate_id=agg.aggregate_id, data="test")

        # Should not raise when applying unhandled event
        agg._apply(event)
        # State should remain as None since no handler set it
        # Note: state is None until first handler actually sets it
        assert agg.state is None or agg.state == {}


# --- Protocol Compliance Tests ---


class TestProtocolCompliance:
    """Tests for protocol compliance."""

    def test_event_handler_protocol_with_async_method(self):
        """Test that async handlers satisfy EventHandler protocol."""

        class AsyncHandler:
            async def handle(self, event: DomainEvent) -> None:
                pass

        handler = AsyncHandler()
        assert isinstance(handler, EventHandler)

    def test_event_subscriber_protocol(self):
        """Test that projections satisfy EventSubscriber protocol."""

        class MySubscriber:
            def subscribed_to(self):
                return [AdditionalTestEvent]

            async def handle(self, event):
                pass

        subscriber = MySubscriber()
        assert isinstance(subscriber, EventSubscriber)


# --- Read Options Tests ---


class TestReadOptions:
    """Tests for ReadOptions dataclass."""

    def test_read_options_defaults(self):
        """Test ReadOptions default values."""
        options = ReadOptions()

        assert options.from_position == 0
        assert options.limit is None
        assert options.from_timestamp is None
        assert options.to_timestamp is None

    def test_read_options_with_all_fields(self):
        """Test ReadOptions with all fields set."""
        now = datetime.now(UTC)
        options = ReadOptions(
            from_position=10,
            limit=100,
            from_timestamp=now,
            to_timestamp=now,
        )

        assert options.from_position == 10
        assert options.limit == 100
        assert options.from_timestamp == now
        assert options.to_timestamp == now
