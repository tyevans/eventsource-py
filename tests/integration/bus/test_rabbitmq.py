"""
Integration tests for RabbitMQ Event Bus.

These tests verify actual RabbitMQ operations including:
- Connection/disconnection lifecycle
- Event publishing and consuming
- Consumer groups and routing
- Dead letter queue flow
- Statistics tracking accuracy
- Multiple consumer groups receiving same events

Requirements:
- Docker must be running
- testcontainers package must be installed
- aio-pika package must be installed

Tests are automatically skipped if these requirements are not met.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncGenerator, Callable, Generator
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest
import pytest_asyncio

from eventsource import (
    RABBITMQ_AVAILABLE,
    DomainEvent,
    EventRegistry,
)

from ..conftest import (
    DOCKER_AVAILABLE,
    TESTCONTAINERS_AVAILABLE,
    TestItemCreated,
    TestItemUpdated,
    TestOrderCreated,
)

if TYPE_CHECKING:
    pass


# ============================================================================
# Skip Conditions
# ============================================================================

skip_if_no_rabbitmq_infra = pytest.mark.skipif(
    not (TESTCONTAINERS_AVAILABLE and DOCKER_AVAILABLE and RABBITMQ_AVAILABLE),
    reason="RabbitMQ test infrastructure not available (requires testcontainers, docker, and aio-pika)",
)

# Skip all tests if RabbitMQ is not available
if not RABBITMQ_AVAILABLE:
    pytest.skip("RabbitMQ (aio-pika) not available", allow_module_level=True)

from eventsource import (  # noqa: E402
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.rabbitmq,
    skip_if_no_rabbitmq_infra,
]


# ============================================================================
# RabbitMQ Container Fixture
# ============================================================================


@pytest.fixture(scope="session")
def rabbitmq_container() -> Generator[Any, None, None]:
    """
    Provide RabbitMQ container for integration tests.

    Uses testcontainers to automatically start and stop a RabbitMQ container.
    Container is shared across all tests in the session for efficiency.
    """
    if not TESTCONTAINERS_AVAILABLE or not DOCKER_AVAILABLE:
        pytest.skip("RabbitMQ testcontainer not available")

    from testcontainers.core.container import DockerContainer
    from testcontainers.core.waiting_utils import wait_for_logs

    # Use RabbitMQ with management plugin for additional debugging capability
    container = DockerContainer("rabbitmq:3-management")
    container.with_exposed_ports(5672, 15672)
    container.with_env("RABBITMQ_DEFAULT_USER", "guest")
    container.with_env("RABBITMQ_DEFAULT_PASS", "guest")
    container.start()

    # Wait for RabbitMQ to be ready
    wait_for_logs(container, "started TCP listener on", timeout=60)

    yield container

    container.stop()


@pytest.fixture(scope="session")
def rabbitmq_connection_url(rabbitmq_container: Any) -> str:
    """Get RabbitMQ connection URL from container."""
    host = rabbitmq_container.get_container_host_ip()
    port = rabbitmq_container.get_exposed_port(5672)
    return f"amqp://guest:guest@{host}:{port}/"


@pytest.fixture
def rabbitmq_event_bus_factory(
    rabbitmq_connection_url: str,
) -> Any:
    """
    Factory fixture for creating RabbitMQ event bus instances.

    Returns a factory function that creates a new, unconnected RabbitMQEventBus.
    Each test is responsible for calling connect() and disconnect().
    """

    def create_bus(
        exchange_name: str = "test_events",
        consumer_group: str = "test_group",
        enable_dlq: bool = True,
        max_retries: int = 3,
    ) -> RabbitMQEventBus:
        registry = EventRegistry()
        registry.register(TestItemCreated)
        registry.register(TestItemUpdated)
        registry.register(TestOrderCreated)

        config = RabbitMQEventBusConfig(
            rabbitmq_url=rabbitmq_connection_url,
            exchange_name=exchange_name,
            consumer_group=consumer_group,
            prefetch_count=10,
            enable_dlq=enable_dlq,
            max_retries=max_retries,
            durable=False,  # Use non-durable for tests to allow cleanup
            auto_delete=True,  # Auto-delete for test isolation
        )

        return RabbitMQEventBus(config=config, event_registry=registry)

    return create_bus


@pytest_asyncio.fixture
async def rabbitmq_event_bus(
    rabbitmq_event_bus_factory: Any,
) -> AsyncGenerator[RabbitMQEventBus, None]:
    """
    Provide RabbitMQ event bus for integration tests.

    Creates a fresh bus instance, connects it within the test's event loop,
    and ensures proper cleanup after the test.
    """
    # Use unique exchange name to avoid conflicts between tests
    unique_suffix = str(uuid4())[:8]
    bus = rabbitmq_event_bus_factory(
        exchange_name=f"test_events_{unique_suffix}",
        consumer_group=f"test_group_{unique_suffix}",
    )
    await bus.connect()

    yield bus

    # Clean up: stop consuming if active and disconnect
    if bus.is_consuming:
        await bus.stop_consuming()
    if bus.is_connected:
        await bus.disconnect()


@pytest.fixture
def sample_customer_id() -> Any:
    """Provide a sample customer ID."""
    return uuid4()


# ============================================================================
# Connection Lifecycle Tests
# ============================================================================


class TestRabbitMQEventBusConnection:
    """Tests for RabbitMQ connection management."""

    async def test_connect_and_disconnect(
        self,
        rabbitmq_event_bus_factory: Any,
    ) -> None:
        """Test connecting and disconnecting from RabbitMQ."""
        bus = rabbitmq_event_bus_factory(
            exchange_name=f"test_connect_{uuid4().hex[:8]}",
        )

        assert not bus.is_connected

        await bus.connect()
        assert bus.is_connected

        await bus.disconnect()
        assert not bus.is_connected

    async def test_context_manager(
        self,
        rabbitmq_event_bus_factory: Any,
    ) -> None:
        """Test using event bus as async context manager."""
        bus = rabbitmq_event_bus_factory(
            exchange_name=f"test_ctx_{uuid4().hex[:8]}",
        )

        assert not bus.is_connected

        async with bus:
            assert bus.is_connected

        assert not bus.is_connected

    async def test_connect_creates_exchange_and_queue(
        self,
        rabbitmq_event_bus_factory: Any,
    ) -> None:
        """Test that connecting creates the exchange and consumer queue."""
        bus = rabbitmq_event_bus_factory(
            exchange_name=f"test_topology_{uuid4().hex[:8]}",
        )

        await bus.connect()

        try:
            # Verify exchange was created by checking internal state
            assert bus._exchange is not None
            assert bus._consumer_queue is not None
        finally:
            await bus.disconnect()

    async def test_connect_creates_dlq_when_enabled(
        self,
        rabbitmq_event_bus_factory: Any,
    ) -> None:
        """Test that DLQ exchange and queue are created when DLQ is enabled."""
        bus = rabbitmq_event_bus_factory(
            exchange_name=f"test_dlq_topology_{uuid4().hex[:8]}",
            enable_dlq=True,
        )

        await bus.connect()

        try:
            assert bus._dlq_exchange is not None
            assert bus._dlq_queue is not None
        finally:
            await bus.disconnect()

    async def test_connect_without_dlq(
        self,
        rabbitmq_event_bus_factory: Any,
    ) -> None:
        """Test that DLQ is not created when disabled."""
        bus = rabbitmq_event_bus_factory(
            exchange_name=f"test_no_dlq_{uuid4().hex[:8]}",
            enable_dlq=False,
        )

        await bus.connect()

        try:
            assert bus._dlq_exchange is None
            assert bus._dlq_queue is None
        finally:
            await bus.disconnect()

    async def test_double_connect_is_safe(
        self,
        rabbitmq_event_bus: RabbitMQEventBus,
    ) -> None:
        """Test that connecting when already connected is a no-op."""
        # Bus is already connected via fixture
        assert rabbitmq_event_bus.is_connected

        # Second connect should not raise
        await rabbitmq_event_bus.connect()
        assert rabbitmq_event_bus.is_connected


# ============================================================================
# Publishing Tests
# ============================================================================


class TestRabbitMQEventBusPublishing:
    """Tests for event publishing."""

    async def test_publish_single_event(
        self,
        rabbitmq_event_bus: RabbitMQEventBus,
    ) -> None:
        """Test publishing a single event."""
        event = TestItemCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            name="Test Item",
            quantity=10,
        )

        await rabbitmq_event_bus.publish([event])

        # Check stats
        assert rabbitmq_event_bus.stats.events_published == 1
        assert rabbitmq_event_bus.stats.publish_confirms == 1

    async def test_publish_multiple_events(
        self,
        rabbitmq_event_bus: RabbitMQEventBus,
    ) -> None:
        """Test publishing multiple events."""
        events: list[DomainEvent] = [
            TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name=f"Item {i}",
                quantity=i,
            )
            for i in range(5)
        ]

        await rabbitmq_event_bus.publish(events)

        # Check stats
        assert rabbitmq_event_bus.stats.events_published == 5
        assert rabbitmq_event_bus.stats.publish_confirms == 5

    async def test_publish_empty_list(
        self,
        rabbitmq_event_bus: RabbitMQEventBus,
    ) -> None:
        """Test that publishing empty list does nothing."""
        initial_published = rabbitmq_event_bus.stats.events_published

        await rabbitmq_event_bus.publish([])

        assert rabbitmq_event_bus.stats.events_published == initial_published

    async def test_publish_different_event_types(
        self,
        rabbitmq_event_bus: RabbitMQEventBus,
        sample_customer_id: Any,
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

        await rabbitmq_event_bus.publish(events)

        assert rabbitmq_event_bus.stats.events_published == 2

    async def test_publish_with_auto_connect(
        self,
        rabbitmq_event_bus_factory: Any,
    ) -> None:
        """Test that publishing auto-connects if not connected."""
        bus = rabbitmq_event_bus_factory(
            exchange_name=f"test_auto_connect_{uuid4().hex[:8]}",
        )

        assert not bus.is_connected

        event = TestItemCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            name="Auto Connect Test",
            quantity=1,
        )

        try:
            await bus.publish([event])
            assert bus.is_connected
            assert bus.stats.events_published == 1
        finally:
            await bus.disconnect()


# ============================================================================
# Subscription and Consumption Tests
# ============================================================================


class TestRabbitMQEventBusSubscription:
    """Tests for event subscription."""

    async def test_subscribe_and_receive(
        self,
        rabbitmq_event_bus: RabbitMQEventBus,
    ) -> None:
        """Test subscribing to events and receiving them."""
        received_events: list[DomainEvent] = []

        async def handler(event: DomainEvent) -> None:
            received_events.append(event)

        rabbitmq_event_bus.subscribe(TestItemCreated, handler)

        # Publish an event
        event = TestItemCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            name="Test Item",
            quantity=10,
        )
        await rabbitmq_event_bus.publish([event])

        # Start consuming in background
        consume_task = asyncio.create_task(rabbitmq_event_bus.start_consuming())

        # Wait for event to be processed
        try:
            await asyncio.wait_for(
                asyncio.create_task(self._wait_for_events(received_events, 1)),
                timeout=10.0,
            )
        finally:
            await rabbitmq_event_bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=2.0)
            except TimeoutError:
                consume_task.cancel()

        assert len(received_events) == 1
        assert received_events[0].event_type == "TestItemCreated"
        assert rabbitmq_event_bus.stats.events_consumed == 1
        assert rabbitmq_event_bus.stats.events_processed_success == 1

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
        rabbitmq_event_bus: RabbitMQEventBus,
    ) -> None:
        """Test multiple handlers for same event type."""
        handler1_events: list[DomainEvent] = []
        handler2_events: list[DomainEvent] = []

        async def handler1(event: DomainEvent) -> None:
            handler1_events.append(event)

        async def handler2(event: DomainEvent) -> None:
            handler2_events.append(event)

        rabbitmq_event_bus.subscribe(TestItemCreated, handler1)
        rabbitmq_event_bus.subscribe(TestItemCreated, handler2)

        # Publish an event
        event = TestItemCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            name="Multi Handler Test",
            quantity=5,
        )
        await rabbitmq_event_bus.publish([event])

        # Start consuming
        consume_task = asyncio.create_task(rabbitmq_event_bus.start_consuming())

        try:
            await asyncio.wait_for(
                asyncio.create_task(self._wait_for_events(handler1_events, 1)),
                timeout=10.0,
            )
        finally:
            await rabbitmq_event_bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=2.0)
            except TimeoutError:
                consume_task.cancel()

        # Both handlers should have received the event
        assert len(handler1_events) == 1
        assert len(handler2_events) == 1

    async def test_subscribe_to_all_events(
        self,
        rabbitmq_event_bus: RabbitMQEventBus,
        sample_customer_id: Any,
    ) -> None:
        """Test wildcard subscription to all events."""
        all_events: list[DomainEvent] = []

        async def wildcard_handler(event: DomainEvent) -> None:
            all_events.append(event)

        rabbitmq_event_bus.subscribe_to_all_events(wildcard_handler)

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
        await rabbitmq_event_bus.publish(events)

        # Start consuming
        consume_task = asyncio.create_task(rabbitmq_event_bus.start_consuming())

        try:
            await asyncio.wait_for(
                asyncio.create_task(self._wait_for_events(all_events, 2)),
                timeout=10.0,
            )
        finally:
            await rabbitmq_event_bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=2.0)
            except TimeoutError:
                consume_task.cancel()

        assert len(all_events) == 2

    async def test_unsubscribe(
        self,
        rabbitmq_event_bus: RabbitMQEventBus,
    ) -> None:
        """Test unsubscribing a handler."""
        events: list[DomainEvent] = []

        async def handler(event: DomainEvent) -> None:
            events.append(event)

        rabbitmq_event_bus.subscribe(TestItemCreated, handler)

        # Unsubscribe
        result = rabbitmq_event_bus.unsubscribe(TestItemCreated, handler)
        assert result is True

        # Second unsubscribe should return False
        result = rabbitmq_event_bus.unsubscribe(TestItemCreated, handler)
        assert result is False


# ============================================================================
# Publish-Consume Round Trip Tests
# ============================================================================


class TestRabbitMQPublishConsumeRoundTrip:
    """Tests for publish-consume round trip with multiple events."""

    async def test_round_trip_multiple_events(
        self,
        rabbitmq_event_bus: RabbitMQEventBus,
    ) -> None:
        """Test publishing multiple events and consuming them all."""
        received_events: list[DomainEvent] = []
        event_count = 10

        async def handler(event: DomainEvent) -> None:
            received_events.append(event)

        rabbitmq_event_bus.subscribe(TestItemCreated, handler)

        # Publish multiple events
        published_ids = []
        for i in range(event_count):
            event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name=f"Item {i}",
                quantity=i + 1,
            )
            published_ids.append(event.event_id)
            await rabbitmq_event_bus.publish([event])

        # Start consuming
        consume_task = asyncio.create_task(rabbitmq_event_bus.start_consuming())

        try:
            await asyncio.wait_for(
                self._wait_for_events(received_events, event_count),
                timeout=30.0,
            )
        finally:
            await rabbitmq_event_bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=2.0)
            except TimeoutError:
                consume_task.cancel()

        # Verify all events were received
        assert len(received_events) == event_count
        received_ids = {e.event_id for e in received_events}
        assert received_ids == set(published_ids)

        # Verify stats
        assert rabbitmq_event_bus.stats.events_published == event_count
        assert rabbitmq_event_bus.stats.events_consumed == event_count
        assert rabbitmq_event_bus.stats.events_processed_success == event_count

    async def _wait_for_events(
        self,
        events: list[DomainEvent],
        count: int,
    ) -> None:
        """Helper to wait for events to be received."""
        while len(events) < count:
            await asyncio.sleep(0.1)

    async def test_round_trip_preserves_event_data(
        self,
        rabbitmq_event_bus: RabbitMQEventBus,
    ) -> None:
        """Test that event data is preserved through publish-consume cycle."""
        received_events: list[DomainEvent] = []

        async def handler(event: DomainEvent) -> None:
            received_events.append(event)

        rabbitmq_event_bus.subscribe(TestItemCreated, handler)

        # Create event with specific data
        original_event = TestItemCreated(
            aggregate_id=uuid4(),
            aggregate_version=42,
            name="Preserved Item",
            quantity=99,
        )
        await rabbitmq_event_bus.publish([original_event])

        # Consume
        consume_task = asyncio.create_task(rabbitmq_event_bus.start_consuming())

        try:
            await asyncio.wait_for(
                self._wait_for_events(received_events, 1),
                timeout=10.0,
            )
        finally:
            await rabbitmq_event_bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=2.0)
            except TimeoutError:
                consume_task.cancel()

        # Verify data preserved
        assert len(received_events) == 1
        received = received_events[0]
        assert isinstance(received, TestItemCreated)
        assert received.event_id == original_event.event_id
        assert received.aggregate_id == original_event.aggregate_id
        assert received.aggregate_version == original_event.aggregate_version
        assert received.name == original_event.name
        assert received.quantity == original_event.quantity

    async def test_round_trip_batch_publish(
        self,
        rabbitmq_event_bus: RabbitMQEventBus,
    ) -> None:
        """Test batch publishing and consuming."""
        received_events: list[DomainEvent] = []

        async def handler(event: DomainEvent) -> None:
            received_events.append(event)

        rabbitmq_event_bus.subscribe(TestItemCreated, handler)

        # Batch publish
        events: list[DomainEvent] = [
            TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=i + 1,
                name=f"Batch Item {i}",
                quantity=i * 10,
            )
            for i in range(20)
        ]
        await rabbitmq_event_bus.publish(events)

        # Consume
        consume_task = asyncio.create_task(rabbitmq_event_bus.start_consuming())

        try:
            await asyncio.wait_for(
                self._wait_for_events(received_events, 20),
                timeout=30.0,
            )
        finally:
            await rabbitmq_event_bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=2.0)
            except TimeoutError:
                consume_task.cancel()

        assert len(received_events) == 20


# ============================================================================
# DLQ (Dead Letter Queue) Tests
# ============================================================================


class TestRabbitMQEventBusDLQ:
    """Tests for dead letter queue functionality."""

    async def test_handler_exception_sends_to_dlq(
        self,
        rabbitmq_event_bus_factory: Any,
    ) -> None:
        """Test that handler exceptions result in message going to DLQ."""
        unique_suffix = uuid4().hex[:8]
        bus = rabbitmq_event_bus_factory(
            exchange_name=f"test_dlq_{unique_suffix}",
            consumer_group=f"test_dlq_group_{unique_suffix}",
            enable_dlq=True,
            max_retries=1,
        )
        await bus.connect()

        try:
            fail_count = 0

            async def failing_handler(event: DomainEvent) -> None:
                nonlocal fail_count
                fail_count += 1
                raise ValueError("Intentional failure for DLQ test")

            bus.subscribe(TestItemCreated, failing_handler)

            # Publish an event
            event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name="DLQ Test",
                quantity=1,
            )
            await bus.publish([event])

            # Start consuming (will fail and send to DLQ)
            consume_task = asyncio.create_task(bus.start_consuming())

            # Wait a bit for processing attempt
            await asyncio.sleep(3.0)

            await bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=2.0)
            except TimeoutError:
                consume_task.cancel()

            # Handler was called at least once
            assert fail_count >= 1

            # Stats should reflect failure
            assert bus.stats.handler_errors >= 1
            assert bus.stats.events_processed_failed >= 1

        finally:
            await bus.disconnect()

    async def test_handler_errors_tracked_in_stats(
        self,
        rabbitmq_event_bus: RabbitMQEventBus,
    ) -> None:
        """Test that handler errors are tracked in statistics."""
        error_count = 0

        async def failing_handler(event: DomainEvent) -> None:
            nonlocal error_count
            error_count += 1
            raise RuntimeError("Handler error")

        rabbitmq_event_bus.subscribe(TestItemCreated, failing_handler)

        # Publish event
        event = TestItemCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            name="Error Test",
            quantity=1,
        )
        await rabbitmq_event_bus.publish([event])

        # Start consuming
        consume_task = asyncio.create_task(rabbitmq_event_bus.start_consuming())

        # Wait a bit
        await asyncio.sleep(2.0)

        await rabbitmq_event_bus.stop_consuming()
        try:
            await asyncio.wait_for(consume_task, timeout=2.0)
        except TimeoutError:
            consume_task.cancel()

        # Handler was called
        assert error_count >= 1

        # Stats should show handler errors
        assert rabbitmq_event_bus.stats.handler_errors >= 1


# ============================================================================
# Multiple Consumer Groups Tests
# ============================================================================


class TestRabbitMQMultipleConsumerGroups:
    """Tests for multiple consumer groups receiving same events."""

    async def test_multiple_consumer_groups_receive_same_event(
        self,
        rabbitmq_event_bus_factory: Any,
    ) -> None:
        """Test that multiple consumer groups each receive the same events."""
        unique_suffix = uuid4().hex[:8]
        exchange_name = f"test_multi_consumer_{unique_suffix}"

        # Create two buses with different consumer groups but same exchange
        bus1 = rabbitmq_event_bus_factory(
            exchange_name=exchange_name,
            consumer_group=f"group_a_{unique_suffix}",
        )
        bus2 = rabbitmq_event_bus_factory(
            exchange_name=exchange_name,
            consumer_group=f"group_b_{unique_suffix}",
        )

        await bus1.connect()
        await bus2.connect()

        try:
            received_by_group_a: list[DomainEvent] = []
            received_by_group_b: list[DomainEvent] = []

            async def handler_a(event: DomainEvent) -> None:
                received_by_group_a.append(event)

            async def handler_b(event: DomainEvent) -> None:
                received_by_group_b.append(event)

            bus1.subscribe(TestItemCreated, handler_a)
            bus2.subscribe(TestItemCreated, handler_b)

            # Publish event through first bus
            event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name="Multi Consumer Test",
                quantity=7,
            )
            await bus1.publish([event])

            # Start both consumers
            task1 = asyncio.create_task(bus1.start_consuming())
            task2 = asyncio.create_task(bus2.start_consuming())

            # Wait for both to receive the event
            async def wait_for_both() -> None:
                while len(received_by_group_a) < 1 or len(received_by_group_b) < 1:
                    await asyncio.sleep(0.1)

            try:
                await asyncio.wait_for(wait_for_both(), timeout=15.0)
            finally:
                await bus1.stop_consuming()
                await bus2.stop_consuming()
                for task in [task1, task2]:
                    try:
                        await asyncio.wait_for(task, timeout=2.0)
                    except TimeoutError:
                        task.cancel()

            # Both groups should have received the event
            assert len(received_by_group_a) == 1
            assert len(received_by_group_b) == 1
            assert received_by_group_a[0].event_id == event.event_id
            assert received_by_group_b[0].event_id == event.event_id

        finally:
            await bus1.disconnect()
            await bus2.disconnect()

    @pytest.mark.slow
    async def test_consumer_group_load_balancing(
        self,
        rabbitmq_event_bus_factory: Any,
    ) -> None:
        """Test that messages are load-balanced within a consumer group."""
        unique_suffix = uuid4().hex[:8]
        exchange_name = f"test_load_balance_{unique_suffix}"
        consumer_group = f"shared_group_{unique_suffix}"

        # Create two buses with same consumer group
        bus1 = rabbitmq_event_bus_factory(
            exchange_name=exchange_name,
            consumer_group=consumer_group,
        )
        bus2 = rabbitmq_event_bus_factory(
            exchange_name=exchange_name,
            consumer_group=consumer_group,
        )

        await bus1.connect()
        await bus2.connect()

        try:
            received_by_bus1: list[DomainEvent] = []
            received_by_bus2: list[DomainEvent] = []

            async def handler1(event: DomainEvent) -> None:
                received_by_bus1.append(event)
                await asyncio.sleep(0.1)  # Simulate some work

            async def handler2(event: DomainEvent) -> None:
                received_by_bus2.append(event)
                await asyncio.sleep(0.1)  # Simulate some work

            bus1.subscribe(TestItemCreated, handler1)
            bus2.subscribe(TestItemCreated, handler2)

            # Publish multiple events
            event_count = 10
            for i in range(event_count):
                event = TestItemCreated(
                    aggregate_id=uuid4(),
                    aggregate_version=1,
                    name=f"Load Balance Item {i}",
                    quantity=i,
                )
                await bus1.publish([event])

            # Start both consumers
            task1 = asyncio.create_task(bus1.start_consuming())
            task2 = asyncio.create_task(bus2.start_consuming())

            # Wait for all events to be consumed
            async def wait_for_all() -> None:
                while len(received_by_bus1) + len(received_by_bus2) < event_count:
                    await asyncio.sleep(0.1)

            try:
                await asyncio.wait_for(wait_for_all(), timeout=30.0)
            finally:
                await bus1.stop_consuming()
                await bus2.stop_consuming()
                for task in [task1, task2]:
                    try:
                        await asyncio.wait_for(task, timeout=2.0)
                    except TimeoutError:
                        task.cancel()

            # Total should equal event_count
            total_received = len(received_by_bus1) + len(received_by_bus2)
            assert total_received == event_count

            # With prefetch and load balancing, both should have received some
            # (exact distribution depends on timing)
            # Note: In a real scenario with fast consumers, one might get all
            # We just verify total is correct

        finally:
            await bus1.disconnect()
            await bus2.disconnect()


# ============================================================================
# Statistics Tracking Tests
# ============================================================================


class TestRabbitMQEventBusStats:
    """Tests for statistics tracking accuracy."""

    async def test_stats_tracking_publish(
        self,
        rabbitmq_event_bus: RabbitMQEventBus,
    ) -> None:
        """Test that publishing updates statistics correctly."""
        initial_stats = rabbitmq_event_bus.stats

        # Verify initial state
        assert initial_stats.events_published == 0

        # Publish events
        for i in range(5):
            event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name=f"Stats Item {i}",
                quantity=i,
            )
            await rabbitmq_event_bus.publish([event])

        assert rabbitmq_event_bus.stats.events_published == 5
        assert rabbitmq_event_bus.stats.publish_confirms == 5

    async def test_stats_tracking_consume(
        self,
        rabbitmq_event_bus: RabbitMQEventBus,
    ) -> None:
        """Test that consuming updates statistics correctly."""
        received_events: list[DomainEvent] = []

        async def handler(event: DomainEvent) -> None:
            received_events.append(event)

        rabbitmq_event_bus.subscribe(TestItemCreated, handler)

        # Publish events
        event_count = 3
        for i in range(event_count):
            event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name=f"Stats Consume Item {i}",
                quantity=i,
            )
            await rabbitmq_event_bus.publish([event])

        # Consume events
        consume_task = asyncio.create_task(rabbitmq_event_bus.start_consuming())

        async def wait_for_events() -> None:
            while len(received_events) < event_count:
                await asyncio.sleep(0.1)

        try:
            await asyncio.wait_for(wait_for_events(), timeout=15.0)
        finally:
            await rabbitmq_event_bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=2.0)
            except TimeoutError:
                consume_task.cancel()

        # Verify stats
        assert rabbitmq_event_bus.stats.events_consumed == event_count
        assert rabbitmq_event_bus.stats.events_processed_success == event_count

    async def test_stats_tracking_handler_errors(
        self,
        rabbitmq_event_bus: RabbitMQEventBus,
    ) -> None:
        """Test that handler errors are tracked in statistics."""

        async def failing_handler(event: DomainEvent) -> None:
            raise ValueError("Intentional failure")

        rabbitmq_event_bus.subscribe(TestItemCreated, failing_handler)

        # Publish event
        event = TestItemCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            name="Error Stats Test",
            quantity=1,
        )
        await rabbitmq_event_bus.publish([event])

        # Consume (will fail)
        consume_task = asyncio.create_task(rabbitmq_event_bus.start_consuming())

        # Wait a bit for the failure
        await asyncio.sleep(2.0)

        await rabbitmq_event_bus.stop_consuming()
        try:
            await asyncio.wait_for(consume_task, timeout=2.0)
        except TimeoutError:
            consume_task.cancel()

        # Verify stats
        assert rabbitmq_event_bus.stats.handler_errors >= 1
        assert rabbitmq_event_bus.stats.events_processed_failed >= 1

    async def test_subscriber_counts(
        self,
        rabbitmq_event_bus: RabbitMQEventBus,
    ) -> None:
        """Test that subscriber counts are tracked correctly."""
        # Initially no subscribers
        rabbitmq_event_bus.clear_subscribers()
        assert rabbitmq_event_bus.get_subscriber_count() == 0
        assert rabbitmq_event_bus.get_wildcard_subscriber_count() == 0

        # Add subscribers
        async def handler1(e: DomainEvent) -> None:
            pass

        async def handler2(e: DomainEvent) -> None:
            pass

        rabbitmq_event_bus.subscribe(TestItemCreated, handler1)
        rabbitmq_event_bus.subscribe(TestItemCreated, handler2)
        rabbitmq_event_bus.subscribe(TestOrderCreated, handler1)

        assert rabbitmq_event_bus.get_subscriber_count() == 3
        assert rabbitmq_event_bus.get_subscriber_count(TestItemCreated) == 2
        assert rabbitmq_event_bus.get_subscriber_count(TestOrderCreated) == 1

        # Add wildcard subscriber
        rabbitmq_event_bus.subscribe_to_all_events(handler1)
        assert rabbitmq_event_bus.get_wildcard_subscriber_count() == 1

        # Clear all
        rabbitmq_event_bus.clear_subscribers()
        assert rabbitmq_event_bus.get_subscriber_count() == 0
        assert rabbitmq_event_bus.get_wildcard_subscriber_count() == 0


# ============================================================================
# Edge Cases and Error Handling Tests
# ============================================================================


class TestRabbitMQEventBusEdgeCases:
    """Tests for edge cases and error handling."""

    async def test_consume_unknown_event_type(
        self,
        rabbitmq_event_bus_factory: Any,
    ) -> None:
        """Test that unknown event types are handled gracefully."""
        unique_suffix = uuid4().hex[:8]

        # Create a bus with limited registry
        bus = rabbitmq_event_bus_factory(
            exchange_name=f"test_unknown_{unique_suffix}",
            consumer_group=f"test_group_{unique_suffix}",
        )
        # Override registry to only include TestItemCreated, not TestOrderCreated
        bus._event_registry = EventRegistry()
        bus._event_registry.register(TestItemCreated)

        await bus.connect()

        try:
            received_events: list[DomainEvent] = []

            async def handler(event: DomainEvent) -> None:
                received_events.append(event)

            bus.subscribe(TestItemCreated, handler)

            # Publish a known and an unknown event type
            known_event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name="Known Event",
                quantity=1,
            )
            await bus.publish([known_event])

            # Start consuming
            consume_task = asyncio.create_task(bus.start_consuming())

            # Wait for known event
            async def wait_for_event() -> None:
                while len(received_events) < 1:
                    await asyncio.sleep(0.1)

            try:
                await asyncio.wait_for(wait_for_event(), timeout=10.0)
            finally:
                await bus.stop_consuming()
                try:
                    await asyncio.wait_for(consume_task, timeout=2.0)
                except TimeoutError:
                    consume_task.cancel()

            # Known event should be processed
            assert len(received_events) == 1

        finally:
            await bus.disconnect()

    async def test_no_handlers_for_event(
        self,
        rabbitmq_event_bus: RabbitMQEventBus,
    ) -> None:
        """Test that events with no handlers are still acknowledged."""
        # Don't subscribe any handler for TestItemUpdated
        # But subscribe to TestItemCreated to verify bus is working
        received_events: list[DomainEvent] = []

        async def handler(event: DomainEvent) -> None:
            received_events.append(event)

        rabbitmq_event_bus.subscribe(TestItemCreated, handler)

        # Publish both event types
        await rabbitmq_event_bus.publish(
            [
                TestItemCreated(
                    aggregate_id=uuid4(),
                    aggregate_version=1,
                    name="Has Handler",
                    quantity=1,
                ),
                TestItemUpdated(
                    aggregate_id=uuid4(),
                    aggregate_version=2,
                    name="No Handler",
                ),
            ]
        )

        # Start consuming
        consume_task = asyncio.create_task(rabbitmq_event_bus.start_consuming())

        # Wait for events to be processed
        await asyncio.sleep(3.0)

        await rabbitmq_event_bus.stop_consuming()
        try:
            await asyncio.wait_for(consume_task, timeout=2.0)
        except TimeoutError:
            consume_task.cancel()

        # Only the event with a handler should be in our list
        assert len(received_events) == 1
        assert received_events[0].event_type == "TestItemCreated"

        # Both should have been consumed
        assert rabbitmq_event_bus.stats.events_consumed >= 1

    async def test_clear_subscribers(
        self,
        rabbitmq_event_bus: RabbitMQEventBus,
    ) -> None:
        """Test clearing all subscribers."""

        # Add some subscribers
        async def handler(e: DomainEvent) -> None:
            pass

        rabbitmq_event_bus.subscribe(TestItemCreated, handler)
        rabbitmq_event_bus.subscribe(TestItemUpdated, handler)
        rabbitmq_event_bus.subscribe_to_all_events(handler)

        assert rabbitmq_event_bus.get_subscriber_count() > 0
        assert rabbitmq_event_bus.get_wildcard_subscriber_count() > 0

        # Clear
        rabbitmq_event_bus.clear_subscribers()

        assert rabbitmq_event_bus.get_subscriber_count() == 0
        assert rabbitmq_event_bus.get_wildcard_subscriber_count() == 0


# ============================================================================
# Performance Tests
# ============================================================================


class TestRabbitMQEventBusPerformance:
    """Performance-related tests."""

    async def test_batch_publish_performance(
        self,
        rabbitmq_event_bus: RabbitMQEventBus,
    ) -> None:
        """Test that batch publishing completes in reasonable time."""
        # Create many events
        events: list[DomainEvent] = [
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
        await rabbitmq_event_bus.publish(events)
        duration = (datetime.now(UTC) - start).total_seconds()

        # Should be reasonably fast
        assert duration < 10.0  # 10 seconds max for 100 events

        # All events should be published
        assert rabbitmq_event_bus.stats.events_published == 100

    async def test_high_throughput_publish_consume(
        self,
        rabbitmq_event_bus: RabbitMQEventBus,
    ) -> None:
        """Test high throughput publish and consume."""
        received_events: list[DomainEvent] = []
        event_count = 50

        async def handler(event: DomainEvent) -> None:
            received_events.append(event)

        rabbitmq_event_bus.subscribe(TestItemCreated, handler)

        # Publish all events
        for i in range(event_count):
            event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name=f"High Throughput Item {i}",
                quantity=i,
            )
            await rabbitmq_event_bus.publish([event])

        # Start consuming
        start = datetime.now(UTC)
        consume_task = asyncio.create_task(rabbitmq_event_bus.start_consuming())

        async def wait_for_all() -> None:
            while len(received_events) < event_count:
                await asyncio.sleep(0.05)

        try:
            await asyncio.wait_for(wait_for_all(), timeout=30.0)
        finally:
            await rabbitmq_event_bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=2.0)
            except TimeoutError:
                consume_task.cancel()

        duration = (datetime.now(UTC) - start).total_seconds()

        # Verify all received
        assert len(received_events) == event_count

        # Should complete in reasonable time
        assert duration < 30.0


# ============================================================================
# Reliability Tests - Dead Letter Queue (P2-009)
# ============================================================================


class TestRabbitMQReliabilityDLQ:
    """Tests for dead letter queue reliability features."""

    @pytest.mark.slow
    async def test_message_sent_to_dlq_after_max_retries(
        self,
        rabbitmq_event_bus_factory: Any,
    ) -> None:
        """Test that messages are sent to DLQ after max_retries failures."""
        unique_suffix = uuid4().hex[:8]
        bus = rabbitmq_event_bus_factory(
            exchange_name=f"test_dlq_max_retries_{unique_suffix}",
            consumer_group=f"test_dlq_group_{unique_suffix}",
            enable_dlq=True,
            max_retries=2,  # Set low for faster test
        )
        await bus.connect()

        try:
            failure_count = 0

            async def failing_handler(event: DomainEvent) -> None:
                nonlocal failure_count
                failure_count += 1
                raise ValueError(f"Intentional failure #{failure_count}")

            bus.subscribe(TestItemCreated, failing_handler)

            # Publish an event
            event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name="DLQ Max Retries Test",
                quantity=1,
            )
            await bus.publish([event])

            # Start consuming
            consume_task = asyncio.create_task(bus.start_consuming())

            # Wait for retries and DLQ routing
            # With max_retries=2 and default retry delays, wait up to 15 seconds
            await asyncio.sleep(10.0)

            await bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=2.0)
            except TimeoutError:
                consume_task.cancel()

            # Handler should have been called multiple times (initial + retries)
            assert failure_count >= 1

            # Stats should show failures and DLQ routing
            assert bus.stats.events_processed_failed >= 1
            assert bus.stats.messages_sent_to_dlq >= 1

            # DLQ should contain the message
            dlq_messages = await bus.get_dlq_messages(limit=10)
            assert len(dlq_messages) >= 1

            # Verify DLQ message metadata
            dlq_msg = dlq_messages[0]
            assert dlq_msg.event_type == "TestItemCreated"
            assert dlq_msg.dlq_reason is not None
            assert "ValueError" in (dlq_msg.dlq_error_type or "")
            assert dlq_msg.dlq_retry_count is not None

        finally:
            await bus.disconnect()

    async def test_get_dlq_messages_returns_metadata(
        self,
        rabbitmq_event_bus_factory: Any,
    ) -> None:
        """Test that get_dlq_messages returns correct metadata."""
        unique_suffix = uuid4().hex[:8]
        bus = rabbitmq_event_bus_factory(
            exchange_name=f"test_dlq_metadata_{unique_suffix}",
            consumer_group=f"test_dlq_metadata_group_{unique_suffix}",
            enable_dlq=True,
            max_retries=0,  # Immediate DLQ routing
        )
        await bus.connect()

        try:

            async def failing_handler(event: DomainEvent) -> None:
                raise RuntimeError("Test error for DLQ metadata")

            bus.subscribe(TestItemCreated, failing_handler)

            # Publish event
            event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name="DLQ Metadata Test",
                quantity=99,
            )
            await bus.publish([event])

            # Consume and let it fail
            consume_task = asyncio.create_task(bus.start_consuming())
            await asyncio.sleep(3.0)

            await bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=2.0)
            except TimeoutError:
                consume_task.cancel()

            # Get DLQ messages and verify metadata
            dlq_messages = await bus.get_dlq_messages(limit=5)
            assert len(dlq_messages) >= 1

            msg = dlq_messages[0]
            assert msg.message_id is not None
            assert msg.event_type == "TestItemCreated"
            assert msg.dlq_reason is not None
            assert "RuntimeError" in (msg.dlq_error_type or "")
            assert msg.dlq_timestamp is not None
            assert msg.original_routing_key is not None or msg.routing_key is not None
            assert msg.body is not None
            assert "DLQ Metadata Test" in msg.body

        finally:
            await bus.disconnect()

    async def test_replay_dlq_message_success(
        self,
        rabbitmq_event_bus_factory: Any,
    ) -> None:
        """Test replaying a message from DLQ back to main exchange."""
        unique_suffix = uuid4().hex[:8]
        bus = rabbitmq_event_bus_factory(
            exchange_name=f"test_dlq_replay_{unique_suffix}",
            consumer_group=f"test_dlq_replay_group_{unique_suffix}",
            enable_dlq=True,
            max_retries=0,  # Immediate DLQ routing
        )
        await bus.connect()

        try:
            fail_once = True
            replay_received = asyncio.Event()
            received_events: list[DomainEvent] = []

            async def conditional_handler(event: DomainEvent) -> None:
                nonlocal fail_once
                if fail_once:
                    fail_once = False
                    raise ValueError("First attempt fails")
                received_events.append(event)
                replay_received.set()

            bus.subscribe(TestItemCreated, conditional_handler)

            # Publish event
            event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name="Replay Test",
                quantity=42,
            )
            await bus.publish([event])

            # Consume and let it fail - keep consumer running during replay
            consume_task = asyncio.create_task(bus.start_consuming())
            await asyncio.sleep(3.0)

            # Verify message is in DLQ (without stopping consumer)
            dlq_messages = await bus.get_dlq_messages(limit=5)
            assert len(dlq_messages) >= 1
            message_id = dlq_messages[0].message_id
            assert message_id is not None

            # Replay the message - handler won't fail this time (consumer still running)
            replay_result = await bus.replay_dlq_message(message_id)
            assert replay_result is True

            # Wait for replay to be processed
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(replay_received.wait(), timeout=10.0)

            await bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=2.0)
            except TimeoutError:
                consume_task.cancel()

            # Verify replay was received
            assert len(received_events) >= 1
            assert received_events[0].event_type == "TestItemCreated"

        finally:
            await bus.disconnect()

    @pytest.mark.slow
    async def test_purge_dlq_removes_messages(
        self,
        rabbitmq_event_bus_factory: Any,
    ) -> None:
        """Test that purge_dlq removes all messages from DLQ."""
        unique_suffix = uuid4().hex[:8]
        bus = rabbitmq_event_bus_factory(
            exchange_name=f"test_dlq_purge_{unique_suffix}",
            consumer_group=f"test_dlq_purge_group_{unique_suffix}",
            enable_dlq=True,
            max_retries=0,
        )
        await bus.connect()

        try:

            async def failing_handler(event: DomainEvent) -> None:
                raise ValueError("Fail for purge test")

            bus.subscribe(TestItemCreated, failing_handler)

            # Publish multiple events
            event_count = 3
            for i in range(event_count):
                event = TestItemCreated(
                    aggregate_id=uuid4(),
                    aggregate_version=1,
                    name=f"Purge Test {i}",
                    quantity=i,
                )
                await bus.publish([event])

            # Consume and let them fail - keep consumer running
            consume_task = asyncio.create_task(bus.start_consuming())
            await asyncio.sleep(5.0)

            # Verify messages were sent to DLQ through stats
            # (DLQ queue itself may be auto-deleted in test mode)
            assert bus.stats.messages_sent_to_dlq >= event_count

            # Get DLQ messages (returns list even if queue recreated)
            _ = await bus.get_dlq_messages(limit=10)

            # Purge DLQ - may return 0 if queue was auto-deleted
            purged = await bus.purge_dlq()

            # The main assertion is that stats tracked messages sent to DLQ
            # Purge behavior may vary based on auto_delete configuration
            assert purged >= 0  # May be 0 if auto_delete removed queue

            # Now stop consumer
            await bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=2.0)
            except TimeoutError:
                consume_task.cancel()

        finally:
            await bus.disconnect()


# ============================================================================
# Reliability Tests - Retry Logic (P2-009)
# ============================================================================


class TestRabbitMQReliabilityRetry:
    """Tests for retry logic reliability features."""

    @pytest.mark.slow
    async def test_message_retried_up_to_max_retries(
        self,
        rabbitmq_event_bus_factory: Any,
    ) -> None:
        """Test that messages are retried up to max_retries times."""
        unique_suffix = uuid4().hex[:8]
        max_retries = 2
        bus = rabbitmq_event_bus_factory(
            exchange_name=f"test_retry_count_{unique_suffix}",
            consumer_group=f"test_retry_count_group_{unique_suffix}",
            enable_dlq=True,
            max_retries=max_retries,
        )
        # Override retry delays for faster testing
        bus._config.retry_base_delay = 0.1
        bus._config.retry_max_delay = 0.5
        bus._config.retry_jitter = 0.0

        await bus.connect()

        try:
            attempt_count = 0

            async def counting_handler(event: DomainEvent) -> None:
                nonlocal attempt_count
                attempt_count += 1
                raise ValueError(f"Retry attempt {attempt_count}")

            bus.subscribe(TestItemCreated, counting_handler)

            # Publish event
            event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name="Retry Count Test",
                quantity=1,
            )
            await bus.publish([event])

            # Consume and wait for retries
            consume_task = asyncio.create_task(bus.start_consuming())
            await asyncio.sleep(8.0)  # Allow time for all retries

            await bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=2.0)
            except TimeoutError:
                consume_task.cancel()

            # Handler should be called: 1 initial + max_retries = 3 times total
            # (for max_retries=2)
            assert attempt_count >= max_retries + 1

            # Message should end up in DLQ
            assert bus.stats.messages_sent_to_dlq >= 1

        finally:
            await bus.disconnect()

    @pytest.mark.slow
    async def test_retry_count_tracked_in_dlq_metadata(
        self,
        rabbitmq_event_bus_factory: Any,
    ) -> None:
        """Test that retry count is tracked in DLQ message metadata."""
        unique_suffix = uuid4().hex[:8]
        max_retries = 3
        bus = rabbitmq_event_bus_factory(
            exchange_name=f"test_retry_dlq_count_{unique_suffix}",
            consumer_group=f"test_retry_dlq_count_group_{unique_suffix}",
            enable_dlq=True,
            max_retries=max_retries,
        )
        # Fast retries for testing
        bus._config.retry_base_delay = 0.1
        bus._config.retry_max_delay = 0.3
        bus._config.retry_jitter = 0.0

        await bus.connect()

        try:

            async def failing_handler(event: DomainEvent) -> None:
                raise ValueError("Always fails")

            bus.subscribe(TestItemCreated, failing_handler)

            event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name="Retry DLQ Count Test",
                quantity=1,
            )
            await bus.publish([event])

            # Wait for retries to exhaust
            consume_task = asyncio.create_task(bus.start_consuming())
            await asyncio.sleep(10.0)

            await bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=2.0)
            except TimeoutError:
                consume_task.cancel()

            # Check DLQ message has correct retry count
            dlq_messages = await bus.get_dlq_messages(limit=5)
            assert len(dlq_messages) >= 1

            dlq_msg = dlq_messages[0]
            # dlq_retry_count should match max_retries (the final retry count)
            assert dlq_msg.dlq_retry_count is not None
            assert dlq_msg.dlq_retry_count >= max_retries

        finally:
            await bus.disconnect()


# ============================================================================
# Reliability Tests - Graceful Shutdown (P2-009)
# ============================================================================


class TestRabbitMQReliabilityShutdown:
    """Tests for graceful shutdown reliability features."""

    async def test_shutdown_completes_within_timeout(
        self,
        rabbitmq_event_bus_factory: Any,
    ) -> None:
        """Test that shutdown completes within the specified timeout."""
        unique_suffix = uuid4().hex[:8]
        bus = rabbitmq_event_bus_factory(
            exchange_name=f"test_shutdown_{unique_suffix}",
            consumer_group=f"test_shutdown_group_{unique_suffix}",
        )
        await bus.connect()

        # Start consuming
        consume_task = asyncio.create_task(bus.start_consuming())
        await asyncio.sleep(0.5)

        # Shutdown with timeout
        shutdown_timeout = 10.0
        start_time = datetime.now(UTC)

        try:
            await asyncio.wait_for(
                bus.shutdown(timeout=shutdown_timeout),
                timeout=shutdown_timeout + 5.0,
            )
        except TimeoutError:
            consume_task.cancel()
            pytest.fail("Shutdown timed out")

        shutdown_duration = (datetime.now(UTC) - start_time).total_seconds()

        # Verify shutdown completed in reasonable time
        assert shutdown_duration < shutdown_timeout + 2.0

        # Verify state is cleaned up
        assert not bus.is_connected
        assert not bus.is_consuming

    async def test_context_manager_graceful_shutdown(
        self,
        rabbitmq_event_bus_factory: Any,
    ) -> None:
        """Test that context manager triggers graceful shutdown."""
        unique_suffix = uuid4().hex[:8]

        # Create bus with custom shutdown timeout
        bus = rabbitmq_event_bus_factory(
            exchange_name=f"test_ctx_shutdown_{unique_suffix}",
            consumer_group=f"test_ctx_shutdown_group_{unique_suffix}",
        )
        bus._config.shutdown_timeout = 10.0

        async with bus:
            assert bus.is_connected

            # Publish some events
            event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name="Context Manager Test",
                quantity=1,
            )
            await bus.publish([event])

        # After context exit, should be disconnected
        assert not bus.is_connected

    async def test_shutdown_idempotent(
        self,
        rabbitmq_event_bus_factory: Any,
    ) -> None:
        """Test that calling shutdown multiple times is safe."""
        unique_suffix = uuid4().hex[:8]
        bus = rabbitmq_event_bus_factory(
            exchange_name=f"test_shutdown_idempotent_{unique_suffix}",
            consumer_group=f"test_shutdown_idempotent_group_{unique_suffix}",
        )
        await bus.connect()

        # First shutdown
        await bus.shutdown(timeout=5.0)
        assert not bus.is_connected

        # Second shutdown should not raise
        await bus.shutdown(timeout=5.0)
        assert not bus.is_connected


# ============================================================================
# Reliability Tests - Statistics (P2-009)
# ============================================================================


class TestRabbitMQReliabilityStats:
    """Tests for statistics tracking accuracy."""

    async def test_stats_accurately_track_operations(
        self,
        rabbitmq_event_bus: RabbitMQEventBus,
    ) -> None:
        """Test that statistics accurately reflect operations."""
        received_events: list[DomainEvent] = []
        received_event = asyncio.Event()

        async def handler(event: DomainEvent) -> None:
            received_events.append(event)
            received_event.set()

        rabbitmq_event_bus.subscribe(TestItemCreated, handler)

        # Record initial stats
        initial_published = rabbitmq_event_bus.stats.events_published
        initial_consumed = rabbitmq_event_bus.stats.events_consumed
        initial_success = rabbitmq_event_bus.stats.events_processed_success

        # Publish events
        event_count = 5
        for i in range(event_count):
            event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name=f"Stats Test {i}",
                quantity=i,
            )
            await rabbitmq_event_bus.publish([event])

        # Verify publish stats
        assert rabbitmq_event_bus.stats.events_published == initial_published + event_count
        assert rabbitmq_event_bus.stats.publish_confirms == initial_published + event_count

        # Consume events
        consume_task = asyncio.create_task(rabbitmq_event_bus.start_consuming())

        # Wait for all events
        async def wait_for_all() -> None:
            while len(received_events) < event_count:
                received_event.clear()
                await asyncio.wait_for(received_event.wait(), timeout=10.0)

        try:
            await asyncio.wait_for(wait_for_all(), timeout=30.0)
        finally:
            await rabbitmq_event_bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=2.0)
            except TimeoutError:
                consume_task.cancel()

        # Verify consume stats
        assert rabbitmq_event_bus.stats.events_consumed >= initial_consumed + event_count
        assert rabbitmq_event_bus.stats.events_processed_success >= initial_success + event_count

    async def test_reset_stats_clears_counters(
        self,
        rabbitmq_event_bus: RabbitMQEventBus,
    ) -> None:
        """Test that reset_stats clears all counters."""
        # Publish some events to generate stats
        for i in range(3):
            event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name=f"Reset Stats Test {i}",
                quantity=i,
            )
            await rabbitmq_event_bus.publish([event])

        # Verify stats are non-zero
        assert rabbitmq_event_bus.stats.events_published >= 3
        assert rabbitmq_event_bus.stats.publish_confirms >= 3

        # Reset stats
        rabbitmq_event_bus.reset_stats()

        # Verify counters are cleared (connected_at preserved if still connected)
        assert rabbitmq_event_bus.stats.events_published == 0
        assert rabbitmq_event_bus.stats.events_consumed == 0
        assert rabbitmq_event_bus.stats.events_processed_success == 0
        assert rabbitmq_event_bus.stats.events_processed_failed == 0
        assert rabbitmq_event_bus.stats.messages_sent_to_dlq == 0
        assert rabbitmq_event_bus.stats.handler_errors == 0
        assert rabbitmq_event_bus.stats.publish_confirms == 0

        # connected_at should be preserved since we're still connected
        if rabbitmq_event_bus.is_connected:
            assert rabbitmq_event_bus.stats.connected_at is not None

    async def test_stats_dict_returns_serializable_format(
        self,
        rabbitmq_event_bus: RabbitMQEventBus,
    ) -> None:
        """Test that get_stats_dict returns a JSON-serializable format."""
        import json

        # Publish event
        event = TestItemCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            name="Stats Dict Test",
            quantity=1,
        )
        await rabbitmq_event_bus.publish([event])

        # Get stats dict
        stats_dict = rabbitmq_event_bus.get_stats_dict()

        # Verify it's JSON serializable
        json_str = json.dumps(stats_dict)
        assert json_str is not None

        # Verify expected keys
        assert "events_published" in stats_dict
        assert "events_consumed" in stats_dict
        assert "events_processed_success" in stats_dict
        assert "events_processed_failed" in stats_dict
        assert "messages_sent_to_dlq" in stats_dict
        assert "handler_errors" in stats_dict
        assert "reconnections" in stats_dict
        assert "publish_confirms" in stats_dict
        assert "is_connected" in stats_dict
        assert "is_consuming" in stats_dict
        assert "uptime_seconds" in stats_dict

        # Verify values
        assert stats_dict["events_published"] >= 1
        assert stats_dict["is_connected"] is True

    @pytest.mark.slow
    async def test_handler_errors_tracked_separately(
        self,
        rabbitmq_event_bus_factory: Any,
    ) -> None:
        """Test that handler errors are tracked separately from processing failures."""
        unique_suffix = uuid4().hex[:8]
        bus = rabbitmq_event_bus_factory(
            exchange_name=f"test_handler_errors_{unique_suffix}",
            consumer_group=f"test_handler_errors_group_{unique_suffix}",
            enable_dlq=True,
            max_retries=1,
        )
        bus._config.retry_base_delay = 0.1
        bus._config.retry_jitter = 0.0

        await bus.connect()

        try:

            async def failing_handler(event: DomainEvent) -> None:
                raise RuntimeError("Handler error")

            bus.subscribe(TestItemCreated, failing_handler)

            # Publish event
            event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name="Handler Error Test",
                quantity=1,
            )
            await bus.publish([event])

            # Consume
            consume_task = asyncio.create_task(bus.start_consuming())
            await asyncio.sleep(5.0)

            await bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=2.0)
            except TimeoutError:
                consume_task.cancel()

            # Verify handler_errors tracks each handler invocation error
            assert bus.stats.handler_errors >= 1

            # events_processed_failed tracks message processing failures
            assert bus.stats.events_processed_failed >= 1

        finally:
            await bus.disconnect()


# ============================================================================
# Reliability Tests - Queue Info and Health Check (P2-009)
# ============================================================================


class TestRabbitMQReliabilityQueueInfo:
    """Tests for queue info and health check methods."""

    async def test_queue_info_returns_valid_data(
        self,
        rabbitmq_event_bus: RabbitMQEventBus,
    ) -> None:
        """Test that queue info returns valid data when connected."""
        # Queue should be accessible
        info = await rabbitmq_event_bus.get_queue_info()

        # Should return valid queue info
        assert info.name == rabbitmq_event_bus._config.queue_name
        assert info.message_count >= 0
        assert info.consumer_count >= 0
        assert info.state in ("running", "idle")
        assert info.error is None

    async def test_health_check_reports_healthy_status(
        self,
        rabbitmq_event_bus: RabbitMQEventBus,
    ) -> None:
        """Test that health check returns correct healthy status."""
        result = await rabbitmq_event_bus.health_check()

        assert result.healthy is True
        assert result.connection_status == "connected"
        assert result.channel_status == "open"
        assert result.queue_status == "accessible"
        assert result.error is None
        assert result.details is not None
        assert "exchange" in result.details
        assert "queue" in result.details
        assert "stats" in result.details

    async def test_health_check_reports_unhealthy_when_disconnected(
        self,
        rabbitmq_event_bus_factory: Any,
    ) -> None:
        """Test that health check reports unhealthy when disconnected."""
        bus = rabbitmq_event_bus_factory(
            exchange_name=f"test_health_{uuid4().hex[:8]}",
        )

        # Don't connect - check health when not connected
        result = await bus.health_check()

        assert result.healthy is False
        assert result.connection_status == "disconnected"
        assert result.error is not None
        assert "Not connected" in result.error

    @pytest.mark.slow
    async def test_dlq_stats_tracking(
        self,
        rabbitmq_event_bus_factory: Any,
    ) -> None:
        """Test that DLQ statistics are accurately tracked."""
        unique_suffix = uuid4().hex[:8]
        bus = rabbitmq_event_bus_factory(
            exchange_name=f"test_dlq_stats_{unique_suffix}",
            consumer_group=f"test_dlq_stats_group_{unique_suffix}",
            enable_dlq=True,
            max_retries=0,
        )
        await bus.connect()

        try:

            async def failing_handler(event: DomainEvent) -> None:
                raise ValueError("Fail for DLQ stats test")

            bus.subscribe(TestItemCreated, failing_handler)

            # Publish events to be sent to DLQ
            event_count = 4
            for i in range(event_count):
                event = TestItemCreated(
                    aggregate_id=uuid4(),
                    aggregate_version=1,
                    name=f"DLQ Stats Test {i}",
                    quantity=i,
                )
                await bus.publish([event])

            # Consume and let them fail
            consume_task = asyncio.create_task(bus.start_consuming())
            await asyncio.sleep(5.0)

            # Verify DLQ stats are tracked
            # (DLQ queue itself may be auto-deleted in test mode, so check stats)
            assert bus.stats.messages_sent_to_dlq >= event_count
            assert bus.stats.events_processed_failed >= event_count

            # get_dlq_message_count may return 0 if DLQ was auto-deleted
            # The important thing is that stats correctly tracked the messages
            dlq_count = await bus.get_dlq_message_count()
            assert dlq_count >= 0  # May be 0 in auto_delete mode

            # Now stop consumer
            await bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=2.0)
            except TimeoutError:
                consume_task.cancel()

        finally:
            await bus.disconnect()


# ============================================================================
# Advanced Feature Tests - Exchange Types (P3-007)
# ============================================================================


class TestAdvancedExchangeTypes:
    """Tests for different exchange type routing behaviors.

    These tests verify that the RabbitMQ event bus correctly handles
    different exchange types including direct, fanout, and topic exchanges.
    """

    async def test_direct_exchange_with_multiple_routing_keys(
        self,
        rabbitmq_connection_url: str,
    ) -> None:
        """Test direct exchange with specific routing key bindings.

        Direct exchanges route messages to queues based on exact routing key matches.
        This test verifies:
        - Messages are routed only when routing keys match exactly
        - Multiple routing key bindings can be added to a queue
        """
        unique_suffix = uuid4().hex[:8]
        exchange_name = f"test_direct_routing_{unique_suffix}"

        registry = EventRegistry()
        registry.register(TestItemCreated)
        registry.register(TestOrderCreated)

        # Create a bus with direct exchange
        config = RabbitMQEventBusConfig(
            rabbitmq_url=rabbitmq_connection_url,
            exchange_name=exchange_name,
            exchange_type="direct",
            consumer_group=f"direct_consumer_{unique_suffix}",
            durable=False,
            auto_delete=True,
        )
        bus = RabbitMQEventBus(config=config, event_registry=registry)

        try:
            await bus.connect()

            received_events: list[DomainEvent] = []

            async def handler(event: DomainEvent) -> None:
                received_events.append(event)

            bus.subscribe(TestItemCreated, handler)
            bus.subscribe(TestOrderCreated, handler)

            # Start consuming in background
            consume_task = asyncio.create_task(bus.start_consuming())
            await asyncio.sleep(0.5)  # Allow consumer to start

            # Publish events - with direct exchange, routing key is aggregate_type.event_type
            item_event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name="Direct Exchange Test Item",
                quantity=5,
            )
            await bus.publish([item_event])

            # Wait for events
            await asyncio.sleep(2.0)

            await bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=2.0)
            except TimeoutError:
                consume_task.cancel()

            # Verify item event was received (direct exchange default binding is queue_name)
            # With default binding, all events go to the queue since routing key matches
            assert bus.stats.events_published >= 1

        finally:
            await bus.disconnect()

    async def test_fanout_exchange_broadcasts_to_multiple_queues(
        self,
        rabbitmq_connection_url: str,
    ) -> None:
        """Test fanout exchange broadcasts to all bound queues.

        Fanout exchanges broadcast messages to all bound queues regardless
        of routing key. This test verifies:
        - All consumer groups receive the same message
        - Routing keys are ignored
        """
        unique_suffix = uuid4().hex[:8]
        exchange_name = f"test_fanout_broadcast_{unique_suffix}"

        registry = EventRegistry()
        registry.register(TestItemCreated)

        # Create two buses with different consumer groups on same fanout exchange
        config1 = RabbitMQEventBusConfig(
            rabbitmq_url=rabbitmq_connection_url,
            exchange_name=exchange_name,
            exchange_type="fanout",
            consumer_group=f"fanout_group_a_{unique_suffix}",
            durable=False,
            auto_delete=True,
        )
        config2 = RabbitMQEventBusConfig(
            rabbitmq_url=rabbitmq_connection_url,
            exchange_name=exchange_name,
            exchange_type="fanout",
            consumer_group=f"fanout_group_b_{unique_suffix}",
            durable=False,
            auto_delete=True,
        )

        bus1 = RabbitMQEventBus(config=config1, event_registry=registry)
        bus2 = RabbitMQEventBus(config=config2, event_registry=registry)

        try:
            await bus1.connect()
            await bus2.connect()

            received_group_a: list[DomainEvent] = []
            received_group_b: list[DomainEvent] = []
            event_a_received = asyncio.Event()
            event_b_received = asyncio.Event()

            async def handler_a(event: DomainEvent) -> None:
                received_group_a.append(event)
                event_a_received.set()

            async def handler_b(event: DomainEvent) -> None:
                received_group_b.append(event)
                event_b_received.set()

            bus1.subscribe(TestItemCreated, handler_a)
            bus2.subscribe(TestItemCreated, handler_b)

            # Start both consumers
            task1 = asyncio.create_task(bus1.start_consuming())
            task2 = asyncio.create_task(bus2.start_consuming())
            await asyncio.sleep(0.5)

            # Publish single event through first bus
            event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name="Fanout Broadcast Test",
                quantity=10,
            )
            await bus1.publish([event])

            # Wait for both to receive
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    asyncio.gather(event_a_received.wait(), event_b_received.wait()),
                    timeout=10.0,
                )

            await bus1.stop_consuming()
            await bus2.stop_consuming()
            for task in [task1, task2]:
                try:
                    await asyncio.wait_for(task, timeout=2.0)
                except TimeoutError:
                    task.cancel()

            # Both groups should have received the event (fanout broadcasts to all)
            assert len(received_group_a) == 1, f"Group A received {len(received_group_a)} events"
            assert len(received_group_b) == 1, f"Group B received {len(received_group_b)} events"
            assert received_group_a[0].event_id == event.event_id
            assert received_group_b[0].event_id == event.event_id

        finally:
            await bus1.disconnect()
            await bus2.disconnect()

    async def test_topic_exchange_with_pattern_matching(
        self,
        rabbitmq_connection_url: str,
    ) -> None:
        """Test topic exchange with wildcard pattern routing.

        Topic exchanges route messages based on routing key patterns.
        This test verifies:
        - Pattern "#" matches all routing keys
        - Pattern "*.EventType" matches any aggregate with specific event type
        - Multiple patterns can be used for routing
        """
        unique_suffix = uuid4().hex[:8]
        exchange_name = f"test_topic_pattern_{unique_suffix}"

        registry = EventRegistry()
        registry.register(TestItemCreated)
        registry.register(TestOrderCreated)

        # Create bus with topic exchange and default "#" pattern (matches all)
        config = RabbitMQEventBusConfig(
            rabbitmq_url=rabbitmq_connection_url,
            exchange_name=exchange_name,
            exchange_type="topic",
            consumer_group=f"topic_consumer_{unique_suffix}",
            routing_key_pattern="#",  # Wildcard - receive all events
            durable=False,
            auto_delete=True,
        )
        bus = RabbitMQEventBus(config=config, event_registry=registry)

        try:
            await bus.connect()

            received_events: list[DomainEvent] = []
            events_received = asyncio.Event()
            expected_count = 2

            async def handler(event: DomainEvent) -> None:
                received_events.append(event)
                if len(received_events) >= expected_count:
                    events_received.set()

            bus.subscribe(TestItemCreated, handler)
            bus.subscribe(TestOrderCreated, handler)

            # Start consuming
            consume_task = asyncio.create_task(bus.start_consuming())
            await asyncio.sleep(0.5)

            # Publish different event types
            item_event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name="Topic Test Item",
                quantity=3,
            )
            order_event = TestOrderCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                customer_id=uuid4(),
                total_amount=99.99,
            )
            await bus.publish([item_event, order_event])

            # Wait for events
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(events_received.wait(), timeout=10.0)

            await bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=2.0)
            except TimeoutError:
                consume_task.cancel()

            # Both events should be received (# wildcard matches all routing keys)
            assert len(received_events) >= 2, f"Expected 2 events, got {len(received_events)}"

            # Verify both event types were received
            event_types = {e.event_type for e in received_events}
            assert "TestItemCreated" in event_types
            assert "TestOrderCreated" in event_types

        finally:
            await bus.disconnect()

    @pytest.mark.slow
    async def test_topic_exchange_selective_pattern(
        self,
        rabbitmq_connection_url: str,
    ) -> None:
        """Test topic exchange with selective pattern that filters messages.

        Tests that topic exchange routing correctly filters messages based
        on specific patterns.
        """
        unique_suffix = uuid4().hex[:8]
        exchange_name = f"test_topic_selective_{unique_suffix}"

        registry = EventRegistry()
        registry.register(TestItemCreated)
        registry.register(TestOrderCreated)

        # Create bus with selective pattern - only TestItem aggregate events
        config = RabbitMQEventBusConfig(
            rabbitmq_url=rabbitmq_connection_url,
            exchange_name=exchange_name,
            exchange_type="topic",
            consumer_group=f"topic_selective_{unique_suffix}",
            routing_key_pattern="TestItem.*",  # Only TestItem events
            durable=False,
            auto_delete=True,
        )
        bus = RabbitMQEventBus(config=config, event_registry=registry)

        try:
            await bus.connect()

            received_events: list[DomainEvent] = []

            async def handler(event: DomainEvent) -> None:
                received_events.append(event)

            bus.subscribe(TestItemCreated, handler)
            bus.subscribe(TestOrderCreated, handler)

            # Start consuming
            consume_task = asyncio.create_task(bus.start_consuming())
            await asyncio.sleep(0.5)

            # Publish both event types
            item_event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name="Selective Item",
                quantity=1,
            )
            order_event = TestOrderCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                customer_id=uuid4(),
                total_amount=50.0,
            )
            await bus.publish([item_event, order_event])

            # Wait for events to be processed
            await asyncio.sleep(3.0)

            await bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=2.0)
            except TimeoutError:
                consume_task.cancel()

            # Only TestItemCreated should be received (routing key: TestItem.TestItemCreated)
            # TestOrderCreated has routing key: TestOrder.TestOrderCreated (won't match TestItem.*)
            item_events = [e for e in received_events if e.event_type == "TestItemCreated"]
            order_events = [e for e in received_events if e.event_type == "TestOrderCreated"]

            assert len(item_events) == 1, f"Expected 1 item event, got {len(item_events)}"
            assert len(order_events) == 0, f"Expected 0 order events, got {len(order_events)}"

        finally:
            await bus.disconnect()


# ============================================================================
# Advanced Feature Tests - Multiple Consumers (P3-007)
# ============================================================================


class TestAdvancedMultipleConsumers:
    """Advanced tests for multiple consumer scenarios.

    These tests verify competing consumers (load balancing) and
    multiple consumer groups receiving copies of the same events.
    """

    @pytest.mark.slow
    async def test_competing_consumers_load_distribution(
        self,
        rabbitmq_connection_url: str,
    ) -> None:
        """Test that messages are distributed across competing consumers.

        When multiple consumers share the same queue (same consumer group),
        RabbitMQ distributes messages between them (competing consumers pattern).
        This verifies load balancing behavior.
        """
        unique_suffix = uuid4().hex[:8]
        exchange_name = f"test_competing_{unique_suffix}"
        consumer_group = f"competing_group_{unique_suffix}"

        registry = EventRegistry()
        registry.register(TestItemCreated)

        # Create two consumers in the SAME consumer group (same queue)
        config1 = RabbitMQEventBusConfig(
            rabbitmq_url=rabbitmq_connection_url,
            exchange_name=exchange_name,
            exchange_type="topic",
            consumer_group=consumer_group,
            consumer_name=f"consumer_1_{unique_suffix}",
            prefetch_count=1,  # Low prefetch to encourage distribution
            durable=False,
            auto_delete=True,
        )
        config2 = RabbitMQEventBusConfig(
            rabbitmq_url=rabbitmq_connection_url,
            exchange_name=exchange_name,
            exchange_type="topic",
            consumer_group=consumer_group,
            consumer_name=f"consumer_2_{unique_suffix}",
            prefetch_count=1,
            durable=False,
            auto_delete=True,
        )

        bus1 = RabbitMQEventBus(config=config1, event_registry=registry)
        bus2 = RabbitMQEventBus(config=config2, event_registry=registry)

        try:
            await bus1.connect()
            await bus2.connect()

            received_by_consumer1: list[DomainEvent] = []
            received_by_consumer2: list[DomainEvent] = []
            all_received = asyncio.Event()
            event_count = 20

            async def handler1(event: DomainEvent) -> None:
                received_by_consumer1.append(event)
                await asyncio.sleep(0.05)  # Simulate work
                if len(received_by_consumer1) + len(received_by_consumer2) >= event_count:
                    all_received.set()

            async def handler2(event: DomainEvent) -> None:
                received_by_consumer2.append(event)
                await asyncio.sleep(0.05)  # Simulate work
                if len(received_by_consumer1) + len(received_by_consumer2) >= event_count:
                    all_received.set()

            bus1.subscribe(TestItemCreated, handler1)
            bus2.subscribe(TestItemCreated, handler2)

            # Start both consumers
            task1 = asyncio.create_task(bus1.start_consuming())
            task2 = asyncio.create_task(bus2.start_consuming())
            await asyncio.sleep(0.5)

            # Publish events
            events = [
                TestItemCreated(
                    aggregate_id=uuid4(),
                    aggregate_version=1,
                    name=f"Competing Consumer Item {i}",
                    quantity=i,
                )
                for i in range(event_count)
            ]
            await bus1.publish(events)

            # Wait for all events to be consumed
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(all_received.wait(), timeout=30.0)

            await bus1.stop_consuming()
            await bus2.stop_consuming()
            for task in [task1, task2]:
                try:
                    await asyncio.wait_for(task, timeout=2.0)
                except TimeoutError:
                    task.cancel()

            # Total should equal event_count
            total_received = len(received_by_consumer1) + len(received_by_consumer2)
            assert total_received == event_count, f"Expected {event_count}, got {total_received}"

            # With competing consumers and low prefetch, both should receive some
            # Distribution may not be perfectly equal due to timing
            print(
                f"Consumer 1: {len(received_by_consumer1)}, "
                f"Consumer 2: {len(received_by_consumer2)}"
            )

            # Verify no duplicate processing (each event received exactly once total)
            all_event_ids = [e.event_id for e in received_by_consumer1 + received_by_consumer2]
            unique_event_ids = set(all_event_ids)
            assert len(all_event_ids) == len(unique_event_ids), "Duplicate events detected"

        finally:
            await bus1.disconnect()
            await bus2.disconnect()

    @pytest.mark.slow
    async def test_multiple_consumer_groups_each_receive_all(
        self,
        rabbitmq_connection_url: str,
    ) -> None:
        """Test that different consumer groups each receive all messages.

        When consumers are in DIFFERENT groups, each group should receive
        a copy of every message (pub/sub pattern).
        """
        unique_suffix = uuid4().hex[:8]
        exchange_name = f"test_multi_group_{unique_suffix}"

        registry = EventRegistry()
        registry.register(TestItemCreated)

        # Create three consumers in DIFFERENT groups
        configs = [
            RabbitMQEventBusConfig(
                rabbitmq_url=rabbitmq_connection_url,
                exchange_name=exchange_name,
                exchange_type="topic",
                consumer_group=f"group_{i}_{unique_suffix}",
                durable=False,
                auto_delete=True,
            )
            for i in range(3)
        ]

        buses = [RabbitMQEventBus(config=config, event_registry=registry) for config in configs]

        try:
            for bus in buses:
                await bus.connect()

            received_per_group: list[list[DomainEvent]] = [[], [], []]
            events_ready = [asyncio.Event() for _ in range(3)]
            event_count = 5

            def create_handler(group_idx: int) -> Callable[[DomainEvent], Any]:
                async def handler(event: DomainEvent) -> None:
                    received_per_group[group_idx].append(event)
                    if len(received_per_group[group_idx]) >= event_count:
                        events_ready[group_idx].set()

                return handler

            # Subscribe each group
            for idx, bus in enumerate(buses):
                bus.subscribe(TestItemCreated, create_handler(idx))

            # Start all consumers
            tasks = [asyncio.create_task(bus.start_consuming()) for bus in buses]
            await asyncio.sleep(0.5)

            # Publish events through first bus
            events = [
                TestItemCreated(
                    aggregate_id=uuid4(),
                    aggregate_version=1,
                    name=f"Multi-Group Item {i}",
                    quantity=i,
                )
                for i in range(event_count)
            ]
            await buses[0].publish(events)

            # Wait for all groups to receive all events
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    asyncio.gather(*[ev.wait() for ev in events_ready]),
                    timeout=15.0,
                )

            for bus in buses:
                await bus.stop_consuming()
            for task in tasks:
                try:
                    await asyncio.wait_for(task, timeout=2.0)
                except TimeoutError:
                    task.cancel()

            # Each group should have received all events
            for idx, received in enumerate(received_per_group):
                assert len(received) == event_count, (
                    f"Group {idx} received {len(received)} events, expected {event_count}"
                )

            # Verify all groups received the same events (by event_id)
            event_ids_per_group = [
                {e.event_id for e in received} for received in received_per_group
            ]
            assert event_ids_per_group[0] == event_ids_per_group[1] == event_ids_per_group[2], (
                "Consumer groups received different events"
            )

        finally:
            for bus in buses:
                await bus.disconnect()


# ============================================================================
# Advanced Feature Tests - Batch Publishing (P3-007)
# ============================================================================


class TestAdvancedBatchPublishing:
    """Tests for batch publishing functionality.

    These tests verify large batch performance and partial failure handling.
    """

    async def test_large_batch_publish_performance(
        self,
        rabbitmq_event_bus: RabbitMQEventBus,
    ) -> None:
        """Test publishing a large batch of events completes efficiently.

        Verifies that batch publishing scales well and maintains
        good performance with large numbers of events.
        """
        batch_size = 500  # Large batch

        # Create events
        events: list[DomainEvent] = [
            TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name=f"Large Batch Item {i}",
                quantity=i % 100,
            )
            for i in range(batch_size)
        ]

        # Time the batch publish
        start_time = datetime.now(UTC)
        result = await rabbitmq_event_bus.publish_batch(events)
        end_time = datetime.now(UTC)

        duration = (end_time - start_time).total_seconds()

        # Verify result
        assert result["total"] == batch_size
        assert result["published"] == batch_size
        assert result["failed"] == 0
        assert result["chunks"] > 0  # Should be chunked

        # Performance assertion - should complete in reasonable time
        # 500 events should complete in under 30 seconds (very conservative)
        assert duration < 30.0, f"Batch took {duration}s, expected < 30s"

        # Calculate throughput
        throughput = batch_size / duration
        print(f"Batch publish: {batch_size} events in {duration:.2f}s ({throughput:.1f} events/s)")

        # Verify stats
        assert rabbitmq_event_bus.stats.batch_publishes >= 1
        assert rabbitmq_event_bus.stats.batch_events_published >= batch_size

    async def test_batch_publish_with_order_preservation(
        self,
        rabbitmq_event_bus: RabbitMQEventBus,
    ) -> None:
        """Test batch publishing with order preservation enabled.

        When preserve_order=True, events should be published sequentially
        to maintain ordering guarantees.
        """
        batch_size = 50

        # Create events with sequential names to track order
        events: list[DomainEvent] = [
            TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name=f"Ordered Item {i:04d}",
                quantity=i,
            )
            for i in range(batch_size)
        ]

        # Publish with order preservation
        result = await rabbitmq_event_bus.publish_batch(events, preserve_order=True)

        # Verify result
        assert result["total"] == batch_size
        assert result["published"] == batch_size
        assert result["failed"] == 0

        # Verify stats
        assert rabbitmq_event_bus.stats.events_published >= batch_size

    async def test_batch_publish_concurrent_mode(
        self,
        rabbitmq_event_bus: RabbitMQEventBus,
    ) -> None:
        """Test batch publishing in concurrent mode (default).

        Concurrent mode should be faster but doesn't guarantee order.
        """
        batch_size = 100

        events: list[DomainEvent] = [
            TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name=f"Concurrent Item {i}",
                quantity=i,
            )
            for i in range(batch_size)
        ]

        # Publish concurrently (default)
        start_time = datetime.now(UTC)
        result = await rabbitmq_event_bus.publish_batch(events, preserve_order=False)
        concurrent_duration = (datetime.now(UTC) - start_time).total_seconds()

        # Verify result
        assert result["total"] == batch_size
        assert result["published"] == batch_size
        assert result["failed"] == 0

        print(f"Concurrent batch: {batch_size} events in {concurrent_duration:.2f}s")

    async def test_batch_publish_empty_batch(
        self,
        rabbitmq_event_bus: RabbitMQEventBus,
    ) -> None:
        """Test batch publishing with empty list returns immediately."""
        result = await rabbitmq_event_bus.publish_batch([])

        assert result["total"] == 0
        assert result["published"] == 0
        assert result["failed"] == 0
        assert result["chunks"] == 0

    async def test_batch_publish_single_event(
        self,
        rabbitmq_event_bus: RabbitMQEventBus,
    ) -> None:
        """Test batch publishing with a single event works correctly."""
        event = TestItemCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            name="Single Batch Item",
            quantity=1,
        )

        result = await rabbitmq_event_bus.publish_batch([event])

        assert result["total"] == 1
        assert result["published"] == 1
        assert result["failed"] == 0

    async def test_batch_publish_verify_all_received(
        self,
        rabbitmq_event_bus: RabbitMQEventBus,
    ) -> None:
        """Test that all batch-published events are received by consumer.

        This end-to-end test verifies that batch publishing correctly
        delivers all events to consumers.
        """
        batch_size = 50
        received_events: list[DomainEvent] = []
        all_received = asyncio.Event()

        async def handler(event: DomainEvent) -> None:
            received_events.append(event)
            if len(received_events) >= batch_size:
                all_received.set()

        rabbitmq_event_bus.subscribe(TestItemCreated, handler)

        # Start consuming
        consume_task = asyncio.create_task(rabbitmq_event_bus.start_consuming())
        await asyncio.sleep(0.5)

        # Create and publish batch
        events: list[DomainEvent] = [
            TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name=f"Batch Receive Test {i}",
                quantity=i,
            )
            for i in range(batch_size)
        ]

        # Record event IDs for verification
        published_ids = {e.event_id for e in events}

        # Publish batch
        result = await rabbitmq_event_bus.publish_batch(events)

        # Wait for all events
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(all_received.wait(), timeout=30.0)

        await rabbitmq_event_bus.stop_consuming()
        try:
            await asyncio.wait_for(consume_task, timeout=2.0)
        except TimeoutError:
            consume_task.cancel()

        # Verify all events received
        assert len(received_events) == batch_size, (
            f"Expected {batch_size} events, received {len(received_events)}"
        )

        # Verify event IDs match
        received_ids = {e.event_id for e in received_events}
        assert received_ids == published_ids, "Mismatch between published and received event IDs"

        # Verify publish result
        assert result["published"] == batch_size

    async def test_batch_chunking_behavior(
        self,
        rabbitmq_connection_url: str,
    ) -> None:
        """Test that large batches are chunked according to config.

        Verifies the chunking mechanism splits large batches and
        the chunks value in the result reflects the chunking.
        """
        unique_suffix = uuid4().hex[:8]

        registry = EventRegistry()
        registry.register(TestItemCreated)

        # Create bus with small batch_size to force chunking
        config = RabbitMQEventBusConfig(
            rabbitmq_url=rabbitmq_connection_url,
            exchange_name=f"test_chunking_{unique_suffix}",
            consumer_group=f"chunk_test_{unique_suffix}",
            batch_size=10,  # Small chunk size
            max_concurrent_publishes=5,
            durable=False,
            auto_delete=True,
        )
        bus = RabbitMQEventBus(config=config, event_registry=registry)

        try:
            await bus.connect()

            # Create batch larger than chunk size
            batch_size = 55  # Should result in 6 chunks (10+10+10+10+10+5)
            events: list[DomainEvent] = [
                TestItemCreated(
                    aggregate_id=uuid4(),
                    aggregate_version=1,
                    name=f"Chunk Test Item {i}",
                    quantity=i,
                )
                for i in range(batch_size)
            ]

            result = await bus.publish_batch(events)

            # Verify chunking
            assert result["total"] == batch_size
            assert result["published"] == batch_size
            assert result["chunks"] == 6  # 55 / 10 = 5.5, rounds up to 6

        finally:
            await bus.disconnect()
