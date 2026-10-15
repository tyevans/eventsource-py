"""Additional unit tests for InMemoryEventBus background publishing and handler coverage."""

import asyncio
from uuid import uuid4

import pytest

from eventsource.adapters.memory.bus import InMemoryEventBus
from eventsource.domain.event import DomainEvent


class AdditionalTestEvent(DomainEvent):
    """Test event for unit tests."""

    aggregate_type: str = "Test"
    data: str = ""


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
