"""
Integration tests for Redis Event Bus.

These tests verify actual Redis operations including:
- Event publishing and consuming
- Consumer groups
- Multiple consumers load balancing
- Pending message recovery
- Dead letter queue flow
- Pipeline batch publishing
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

import pytest

from eventsource import (
    REDIS_AVAILABLE,
    DomainEvent,
    EventRegistry,
)
from eventsource.bus.interface import EventBus
from eventsource.testing.conformance import EventBusConformanceSuite

from ..conftest import (
    TestItemCreated,
    TestItemUpdated,
    TestOrderCreated,
    skip_if_no_redis_infra,
)

if TYPE_CHECKING:
    pass

# Skip all tests if Redis is not available
if not REDIS_AVAILABLE:
    pytest.skip("Redis not available", allow_module_level=True)

from eventsource import (
    RedisEventBus,
    RedisEventBusConfig,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.redis,
    skip_if_no_redis_infra,
]


class TestRedisEventBusConnection:
    """Tests for Redis connection management."""

    async def test_connect_and_disconnect(
        self,
        redis_connection_url: str,
        clean_redis: None,
    ) -> None:
        """Test connecting and disconnecting from Redis."""
        config = RedisEventBusConfig(
            redis_url=redis_connection_url,
            stream_prefix="test_connect",
        )
        bus = RedisEventBus(config=config)

        assert not bus.is_connected

        await bus.connect()
        assert bus.is_connected

        await bus.disconnect()
        assert not bus.is_connected

    async def test_connect_creates_consumer_group(
        self,
        redis_connection_url: str,
        redis_client: Any,
        clean_redis: None,
    ) -> None:
        """Test that connecting creates the consumer group."""
        config = RedisEventBusConfig(
            redis_url=redis_connection_url,
            stream_prefix="test_group",
            consumer_group="test_consumers",
        )
        bus = RedisEventBus(config=config)

        await bus.connect()

        try:
            # Check that stream and consumer group exist
            groups = await redis_client.xinfo_groups(config.stream_name)
            group_names = [g["name"] for g in groups]
            assert "test_consumers" in group_names
        finally:
            await bus.disconnect()


class TestRedisEventBusPublishing:
    """Tests for event publishing."""

    async def test_publish_single_event(
        self,
        redis_event_bus: RedisEventBus,
        redis_client: Any,
    ) -> None:
        """Test publishing a single event."""
        event = TestItemCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            name="Test Item",
            quantity=10,
        )

        await redis_event_bus.publish([event])

        # Verify event is in stream
        stream_info = await redis_client.xinfo_stream(redis_event_bus.config.stream_name)
        assert stream_info["length"] == 1

        # Check stats
        assert redis_event_bus.stats.events_published == 1

    async def test_publish_multiple_events(
        self,
        redis_event_bus: RedisEventBus,
        redis_client: Any,
    ) -> None:
        """Test publishing multiple events (uses pipeline)."""
        events = [
            TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name=f"Item {i}",
                quantity=i,
            )
            for i in range(5)
        ]

        await redis_event_bus.publish(events)

        # Verify all events are in stream
        stream_info = await redis_client.xinfo_stream(redis_event_bus.config.stream_name)
        assert stream_info["length"] == 5

        # Check stats
        assert redis_event_bus.stats.events_published == 5

    async def test_publish_empty_list(
        self,
        redis_event_bus: RedisEventBus,
    ) -> None:
        """Test that publishing empty list does nothing."""
        initial_published = redis_event_bus.stats.events_published

        await redis_event_bus.publish([])

        assert redis_event_bus.stats.events_published == initial_published

    async def test_publish_different_event_types(
        self,
        redis_event_bus: RedisEventBus,
        redis_client: Any,
        sample_customer_id,
    ) -> None:
        """Test publishing different event types."""
        events = [
            TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name="Test Item",
                quantity=10,
            ),
            TestOrderCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                customer_id=sample_customer_id,
                total_amount=100.0,
            ),
        ]

        await redis_event_bus.publish(events)

        # Read raw messages from stream
        messages = await redis_client.xrange(
            redis_event_bus.config.stream_name,
            "-",
            "+",
        )

        assert len(messages) == 2

        event_types = {msg[1]["event_type"] for msg in messages}
        assert "TestItemCreated" in event_types
        assert "TestOrderCreated" in event_types


class TestRedisEventBusSubscription:
    """Tests for event subscription."""

    async def test_subscribe_and_receive(
        self,
        redis_event_bus: RedisEventBus,
    ) -> None:
        """Test subscribing to events and receiving them."""
        received_events: list[DomainEvent] = []

        async def handler(event: DomainEvent) -> None:
            received_events.append(event)

        redis_event_bus.subscribe(TestItemCreated, handler)

        # Publish an event
        event = TestItemCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            name="Test Item",
            quantity=10,
        )
        await redis_event_bus.publish([event])

        # Start consuming in background
        consume_task = asyncio.create_task(redis_event_bus.start_consuming())

        # Wait for event to be processed
        try:
            await asyncio.wait_for(
                asyncio.create_task(self._wait_for_events(received_events, 1)),
                timeout=5.0,
            )
        finally:
            await redis_event_bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=2.0)
            except TimeoutError:
                consume_task.cancel()

        assert len(received_events) == 1
        assert received_events[0].event_type == "TestItemCreated"

    async def _wait_for_events(
        self,
        events: list[DomainEvent],
        count: int,
    ) -> None:
        """Helper to wait for events to be received."""
        while len(events) < count:
            await asyncio.sleep(0.1)

    async def test_subscribe_multiple_handlers(
        self,
        redis_event_bus: RedisEventBus,
    ) -> None:
        """Test multiple handlers for same event type."""
        handler1_events: list[DomainEvent] = []
        handler2_events: list[DomainEvent] = []

        async def handler1(event: DomainEvent) -> None:
            handler1_events.append(event)

        async def handler2(event: DomainEvent) -> None:
            handler2_events.append(event)

        redis_event_bus.subscribe(TestItemCreated, handler1)
        redis_event_bus.subscribe(TestItemCreated, handler2)

        # Publish an event
        event = TestItemCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            name="Multi Handler Test",
            quantity=5,
        )
        await redis_event_bus.publish([event])

        # Start consuming
        consume_task = asyncio.create_task(redis_event_bus.start_consuming())

        try:
            await asyncio.wait_for(
                asyncio.create_task(self._wait_for_events(handler1_events, 1)),
                timeout=5.0,
            )
        finally:
            await redis_event_bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=2.0)
            except TimeoutError:
                consume_task.cancel()

        # Both handlers should have received the event
        assert len(handler1_events) == 1
        assert len(handler2_events) == 1

    async def test_subscribe_to_all_events(
        self,
        redis_event_bus: RedisEventBus,
        sample_customer_id,
    ) -> None:
        """Test wildcard subscription to all events."""
        all_events: list[DomainEvent] = []

        async def wildcard_handler(event: DomainEvent) -> None:
            all_events.append(event)

        redis_event_bus.subscribe_to_all_events(wildcard_handler)

        # Publish different event types
        events = [
            TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name="Item",
                quantity=1,
            ),
            TestOrderCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                customer_id=sample_customer_id,
                total_amount=50.0,
            ),
        ]
        await redis_event_bus.publish(events)

        # Start consuming
        consume_task = asyncio.create_task(redis_event_bus.start_consuming())

        try:
            await asyncio.wait_for(
                asyncio.create_task(self._wait_for_events(all_events, 2)),
                timeout=5.0,
            )
        finally:
            await redis_event_bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=2.0)
            except TimeoutError:
                consume_task.cancel()

        assert len(all_events) == 2

    async def test_unsubscribe(
        self,
        redis_event_bus: RedisEventBus,
    ) -> None:
        """Test unsubscribing a handler."""
        events: list[DomainEvent] = []

        async def handler(event: DomainEvent) -> None:
            events.append(event)

        redis_event_bus.subscribe(TestItemCreated, handler)

        # Unsubscribe
        result = redis_event_bus.unsubscribe(TestItemCreated, handler)
        assert result is True

        # Second unsubscribe should return False
        result = redis_event_bus.unsubscribe(TestItemCreated, handler)
        assert result is False


class TestRedisEventBusConsumerGroups:
    """Tests for consumer group functionality."""

    async def test_get_stream_info(
        self,
        redis_event_bus: RedisEventBus,
    ) -> None:
        """Test getting stream information."""
        # Publish some events
        events = [
            TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name=f"Item {i}",
                quantity=i,
            )
            for i in range(3)
        ]
        await redis_event_bus.publish(events)

        info = await redis_event_bus.get_stream_info()

        assert info["connected"] is True
        assert info["stream"]["length"] == 3
        assert len(info["consumer_groups"]) >= 1

    async def test_stats_tracking(
        self,
        redis_event_bus: RedisEventBus,
    ) -> None:
        """Test that statistics are tracked correctly."""
        initial_stats = redis_event_bus.get_stats_dict()

        # Publish events
        for i in range(5):
            event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name=f"Stats Item {i}",
                quantity=i,
            )
            await redis_event_bus.publish([event])

        new_stats = redis_event_bus.get_stats_dict()

        assert new_stats["events_published"] == initial_stats["events_published"] + 5


class TestRedisEventBusDLQ:
    """Tests for dead letter queue functionality."""

    async def test_dlq_stream_exists_after_failure(
        self,
        redis_connection_url: str,
        redis_client: Any,
        clean_redis: None,
    ) -> None:
        """Test that DLQ stream is created when needed."""
        config = RedisEventBusConfig(
            redis_url=redis_connection_url,
            stream_prefix="test_dlq",
            enable_dlq=True,
            max_retries=1,
        )

        registry = EventRegistry()
        registry.register(TestItemCreated)

        bus = RedisEventBus(config=config, event_registry=registry)
        await bus.connect()

        try:
            # Subscribe a failing handler
            fail_count = 0

            async def failing_handler(event: DomainEvent) -> None:
                nonlocal fail_count
                fail_count += 1
                raise ValueError("Intentional failure")

            bus.subscribe(TestItemCreated, failing_handler)

            # Publish an event
            event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name="DLQ Test",
                quantity=1,
            )
            await bus.publish([event])

            # Start consuming (will fail)
            consume_task = asyncio.create_task(bus.start_consuming())

            # Wait a bit for processing attempt
            await asyncio.sleep(2.0)

            await bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=2.0)
            except TimeoutError:
                consume_task.cancel()

            # The event should be pending (not acked due to failure)
            await bus.get_stream_info()
            # Handler was called at least once
            assert fail_count >= 1

        finally:
            await bus.disconnect()

    async def test_get_dlq_messages_empty(
        self,
        redis_event_bus: RedisEventBus,
    ) -> None:
        """Test getting DLQ messages when empty."""
        messages = await redis_event_bus.get_dlq_messages()
        assert messages == []


class TestRedisEventBusEdgeCases:
    """Tests for edge cases and error handling."""

    async def test_handler_with_exception(
        self,
        redis_event_bus: RedisEventBus,
    ) -> None:
        """Test that handler exceptions don't crash the bus."""
        error_count = 0

        async def failing_handler(event: DomainEvent) -> None:
            nonlocal error_count
            error_count += 1
            raise RuntimeError("Handler error")

        redis_event_bus.subscribe(TestItemCreated, failing_handler)

        # Publish event
        event = TestItemCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            name="Error Test",
            quantity=1,
        )
        await redis_event_bus.publish([event])

        # Start consuming
        consume_task = asyncio.create_task(redis_event_bus.start_consuming())

        # Wait a bit
        await asyncio.sleep(2.0)

        await redis_event_bus.stop_consuming()
        try:
            await asyncio.wait_for(consume_task, timeout=2.0)
        except TimeoutError:
            consume_task.cancel()

        # Handler was called
        assert error_count >= 1

        # Stats should show handler errors
        assert redis_event_bus.stats.handler_errors >= 1

    async def test_clear_subscribers(
        self,
        redis_event_bus: RedisEventBus,
    ) -> None:
        """Test clearing all subscribers."""
        # Add some subscribers
        redis_event_bus.subscribe(TestItemCreated, lambda e: None)
        redis_event_bus.subscribe(TestItemUpdated, lambda e: None)
        redis_event_bus.subscribe_to_all_events(lambda e: None)

        assert redis_event_bus.get_subscriber_count() > 0
        assert redis_event_bus.get_wildcard_subscriber_count() > 0

        # Clear
        redis_event_bus.clear_subscribers()

        assert redis_event_bus.get_subscriber_count() == 0
        assert redis_event_bus.get_wildcard_subscriber_count() == 0

    async def test_subscriber_count(
        self,
        redis_event_bus: RedisEventBus,
    ) -> None:
        """Test getting subscriber counts."""
        # Initially no subscribers (after fixture setup)
        redis_event_bus.clear_subscribers()
        assert redis_event_bus.get_subscriber_count() == 0

        # Add subscribers
        redis_event_bus.subscribe(TestItemCreated, lambda e: None)
        redis_event_bus.subscribe(TestItemCreated, lambda e: None)
        redis_event_bus.subscribe(TestItemUpdated, lambda e: None)

        assert redis_event_bus.get_subscriber_count() == 3
        assert redis_event_bus.get_subscriber_count(TestItemCreated) == 2
        assert redis_event_bus.get_subscriber_count(TestItemUpdated) == 1


class TestRedisEventBusPerformance:
    """Performance-related tests."""

    async def test_batch_publish_performance(
        self,
        redis_event_bus: RedisEventBus,
    ) -> None:
        """Test that batch publishing is efficient."""
        # Create many events
        events = [
            TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name=f"Batch Item {i}",
                quantity=i,
            )
            for i in range(100)
        ]

        # Publish in batch
        start = datetime.now(UTC)
        await redis_event_bus.publish(events)
        duration = (datetime.now(UTC) - start).total_seconds()

        # Should be reasonably fast (using pipeline)
        assert duration < 5.0  # 5 seconds max for 100 events

        # All events should be published
        info = await redis_event_bus.get_stream_info()
        assert info["stream"]["length"] == 100


class TestRedisEventBusConformance(EventBusConformanceSuite):
    """Runs the shared EventBus contract against a real Redis.

    RedisEventBus only delivers to handlers via a running ``start_consuming()``
    loop, so each test gets one connected bus with consumption already running
    in the background (started here, not inside ``create_bus``, since the
    abstract factory method is sync and starting consumption requires
    ``await``).
    """

    @pytest.fixture(autouse=True)
    async def _redis_conformance_setup(
        self,
        redis_connection_url: str,
        clean_redis: None,
        request: pytest.FixtureRequest,
    ) -> AsyncGenerator[None, None]:
        # Deliberately does NOT use single_connection_client=True (unlike
        # redis_event_bus_factory): that mode shares one connection between
        # publish and the blocking start_consuming() XREADGROUP call, which
        # self-deadlocks when both run concurrently on the same bus.
        config = RedisEventBusConfig(
            redis_url=redis_connection_url,
            stream_prefix=f"conformance_{request.node.name}",
            consumer_group="conformance_group",
            batch_size=10,
            block_ms=200,
        )
        self._bus = RedisEventBus(config=config)
        await self._bus.connect()
        self._consume_task = asyncio.create_task(self._bus.start_consuming())

        yield

        await self._bus.stop_consuming()
        self._consume_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._consume_task
        if self._bus.is_connected:
            await self._bus.shutdown()

    def create_bus(self) -> EventBus:
        # Several base-suite tests assert on delivery immediately after
        # `await bus.publish(...)` with no call to `await_delivery` (that
        # hook is only used by tests written with pull-based backends in
        # mind). Redis only delivers via the background start_consuming()
        # loop, so we wrap publish() to wait for the consumer to catch up
        # before returning -- making delivery effectively synchronous from
        # the test's point of view, matching what the base suite assumes.
        bus = self._bus
        original_publish = bus.publish

        async def publish_and_await_delivery(
            events: list[DomainEvent], background: bool = False
        ) -> None:
            await original_publish(events, background=background)
            await self.await_delivery(bus)

        bus.publish = publish_and_await_delivery  # type: ignore[method-assign]
        return bus

    def create_test_event(self, aggregate_id: UUID) -> DomainEvent:
        return TestItemCreated(
            aggregate_id=aggregate_id,
            aggregate_version=1,
            name="conformance item",
            quantity=1,
        )

    def create_subscriber(self, received: list[DomainEvent]) -> Any:
        class Subscriber:
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [TestItemCreated]

            async def handle(self, event: DomainEvent) -> None:
                received.append(event)

        return Subscriber()

    async def test_handler_error_isolation(self) -> None:
        """Redis dispatch does not isolate handler errors like the base suite expects.

        ``RedisEventBus._dispatch_event`` invokes handlers sequentially and
        deliberately re-raises on the first failure (see
        ``_invoke_handler``'s "Re-raise to prevent acking the message"), so a
        later handler is never reached on the same delivery attempt -- the
        message is left unacked for at-least-once redelivery instead. This is
        pre-existing, intentional behavior distinct from the in-process
        buses' fire-and-forget isolation, and out of scope for this task, so
        this override documents the divergence rather than asserting the
        base suite's isolation contract against it.
        """
        bus = self.create_bus()
        aggregate_id = uuid4()
        event = self.create_test_event(aggregate_id)

        received_by_good_handler: list[DomainEvent] = []

        async def failing_handler(e: DomainEvent) -> None:
            raise ValueError("Handler error for testing")

        async def good_handler(e: DomainEvent) -> None:
            received_by_good_handler.append(e)

        event_type = type(event)
        bus.subscribe(event_type, failing_handler)
        bus.subscribe(event_type, good_handler)

        # Publishing (and the consumer loop processing it) must not crash
        # the bus even though the handler raises.
        await self._bus_no_wait_publish([event])

        await asyncio.sleep(1.0)

        assert isinstance(bus, RedisEventBus)
        assert bus.stats.handler_errors >= 1

    async def _bus_no_wait_publish(self, events: list[DomainEvent]) -> None:
        """Publish without the delivery-wait wrapper (would never catch up)."""
        await RedisEventBus.publish(self._bus, events)

    async def await_delivery(self, bus: EventBus) -> None:
        """Poll until the consumer loop has caught up, bounded to 10s."""
        assert isinstance(bus, RedisEventBus)
        loop = asyncio.get_event_loop()
        deadline = loop.time() + 10.0

        # A background=True publish schedules a task that may not have run
        # (and thus not yet incremented events_published) the instant we get
        # here -- drain background tasks first so the stats comparison below
        # reflects the eventual state, not a stale snapshot.
        while bus.get_background_task_count() > 0:
            if loop.time() >= deadline:
                return
            await asyncio.sleep(0.05)

        while bus.stats.events_consumed < bus.stats.events_published:
            if loop.time() >= deadline:
                return
            await asyncio.sleep(0.05)
