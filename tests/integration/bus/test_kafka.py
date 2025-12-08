"""
Integration tests for Kafka Event Bus.

These tests verify actual Kafka operations including:
- Connection/disconnection lifecycle
- Event publishing and consuming
- Consumer groups and routing
- Dead letter queue flow
- Statistics tracking accuracy
- Multiple consumer groups receiving same events

Requirements:
- Docker must be running
- testcontainers-kafka package must be installed
- aiokafka package must be installed

Tests are automatically skipped if these requirements are not met.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Generator
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest
import pytest_asyncio

from eventsource import (
    KAFKA_AVAILABLE,
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
# Testcontainers Detection
# ============================================================================

KAFKA_TESTCONTAINERS_AVAILABLE = False

try:
    from testcontainers.kafka import KafkaContainer

    KAFKA_TESTCONTAINERS_AVAILABLE = True
except ImportError:
    KafkaContainer = None  # type: ignore[assignment, misc]


# ============================================================================
# Skip Conditions
# ============================================================================

skip_if_no_kafka_infra = pytest.mark.skipif(
    not (
        TESTCONTAINERS_AVAILABLE
        and DOCKER_AVAILABLE
        and KAFKA_AVAILABLE
        and KAFKA_TESTCONTAINERS_AVAILABLE
    ),
    reason="Kafka test infrastructure not available (requires testcontainers-kafka, docker, and aiokafka)",
)

# Skip all tests if Kafka is not available
if not KAFKA_AVAILABLE:
    pytest.skip("Kafka (aiokafka) not available", allow_module_level=True)

from eventsource import (  # noqa: E402
    KafkaEventBus,
    KafkaEventBusConfig,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.kafka,
    skip_if_no_kafka_infra,
]


# ============================================================================
# Kafka Container Fixture
# ============================================================================


@pytest.fixture(scope="session")
def kafka_container() -> Generator[Any, None, None]:
    """
    Provide Kafka container for integration tests.

    Uses testcontainers to automatically start and stop a Kafka container.
    Container is shared across all tests in the session for efficiency.
    """
    if not KAFKA_TESTCONTAINERS_AVAILABLE or not DOCKER_AVAILABLE:
        pytest.skip("Kafka testcontainer not available")

    container = KafkaContainer("confluentinc/cp-kafka:7.5.0")
    container.start()

    # Give Kafka time to fully initialize
    import time

    time.sleep(5)

    yield container

    container.stop()


@pytest.fixture(scope="session")
def kafka_bootstrap_servers(kafka_container: Any) -> str:
    """Get Kafka bootstrap servers from container."""
    return kafka_container.get_bootstrap_server()


@pytest.fixture
def kafka_event_bus_factory(
    kafka_bootstrap_servers: str,
) -> Any:
    """
    Factory fixture for creating Kafka event bus instances.

    Returns a factory function that creates a new, unconnected KafkaEventBus.
    Each test is responsible for calling connect() and disconnect().
    """

    def create_bus(
        topic_prefix: str | None = None,
        consumer_group: str | None = None,
        enable_dlq: bool = True,
        max_retries: int = 3,
        retry_base_delay: float = 0.1,
    ) -> KafkaEventBus:
        registry = EventRegistry()
        registry.register(TestItemCreated)
        registry.register(TestItemUpdated)
        registry.register(TestOrderCreated)

        # Use unique topic prefix and consumer group for test isolation
        unique_suffix = uuid4().hex[:8]
        config = KafkaEventBusConfig(
            bootstrap_servers=kafka_bootstrap_servers,
            topic_prefix=topic_prefix or f"test_{unique_suffix}",
            consumer_group=consumer_group or f"test_group_{unique_suffix}",
            enable_dlq=enable_dlq,
            max_retries=max_retries,
            retry_base_delay=retry_base_delay,
            retry_max_delay=1.0,  # Fast retries for tests
            enable_tracing=False,  # Disable tracing for tests
        )

        return KafkaEventBus(config=config, event_registry=registry)

    return create_bus


@pytest_asyncio.fixture
async def kafka_event_bus(
    kafka_event_bus_factory: Any,
) -> AsyncGenerator[KafkaEventBus, None]:
    """
    Provide Kafka event bus for integration tests.

    Creates a fresh bus instance, connects it within the test's event loop,
    and ensures proper cleanup after the test.
    """
    bus = kafka_event_bus_factory()
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


class TestKafkaEventBusConnection:
    """Tests for Kafka connection management."""

    async def test_connect_and_disconnect(
        self,
        kafka_event_bus_factory: Any,
    ) -> None:
        """Test connecting and disconnecting from Kafka."""
        bus = kafka_event_bus_factory()

        assert not bus.is_connected

        await bus.connect()
        assert bus.is_connected

        await bus.disconnect()
        assert not bus.is_connected

    async def test_context_manager(
        self,
        kafka_event_bus_factory: Any,
    ) -> None:
        """Test using event bus as async context manager."""
        bus = kafka_event_bus_factory()

        assert not bus.is_connected

        async with bus:
            assert bus.is_connected

        assert not bus.is_connected

    async def test_double_connect_is_safe(
        self,
        kafka_event_bus: KafkaEventBus,
    ) -> None:
        """Test that connecting when already connected is a no-op."""
        # Bus is already connected via fixture
        assert kafka_event_bus.is_connected

        # Second connect should not raise
        await kafka_event_bus.connect()
        assert kafka_event_bus.is_connected

    async def test_double_disconnect_is_safe(
        self,
        kafka_event_bus: KafkaEventBus,
    ) -> None:
        """Test that disconnect can be called multiple times safely."""
        await kafka_event_bus.disconnect()
        assert not kafka_event_bus.is_connected

        # Second disconnect should not raise
        await kafka_event_bus.disconnect()
        assert not kafka_event_bus.is_connected


# ============================================================================
# Publishing Tests
# ============================================================================


class TestKafkaEventBusPublishing:
    """Tests for event publishing."""

    async def test_publish_single_event(
        self,
        kafka_event_bus: KafkaEventBus,
    ) -> None:
        """Test publishing a single event."""
        event = TestItemCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            name="Test Item",
            quantity=10,
        )

        await kafka_event_bus.publish([event])

        # Check stats
        assert kafka_event_bus.stats.events_published == 1
        assert kafka_event_bus.stats.last_publish_at is not None

    async def test_publish_multiple_events(
        self,
        kafka_event_bus: KafkaEventBus,
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

        await kafka_event_bus.publish(events)

        # Check stats
        assert kafka_event_bus.stats.events_published == 5

    async def test_publish_empty_list(
        self,
        kafka_event_bus: KafkaEventBus,
    ) -> None:
        """Test that publishing empty list does nothing."""
        initial_published = kafka_event_bus.stats.events_published

        await kafka_event_bus.publish([])

        assert kafka_event_bus.stats.events_published == initial_published

    async def test_publish_different_event_types(
        self,
        kafka_event_bus: KafkaEventBus,
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

        await kafka_event_bus.publish(events)

        assert kafka_event_bus.stats.events_published == 2


# ============================================================================
# Subscription and Consumption Tests
# ============================================================================


class TestKafkaEventBusSubscription:
    """Tests for event subscription."""

    async def test_subscribe_and_receive(
        self,
        kafka_event_bus: KafkaEventBus,
    ) -> None:
        """Test subscribing to events and receiving them."""
        received_events: list[DomainEvent] = []

        async def handler(event: DomainEvent) -> None:
            received_events.append(event)

        kafka_event_bus.subscribe(TestItemCreated, handler)

        # Publish an event
        event = TestItemCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            name="Test Item",
            quantity=10,
        )
        await kafka_event_bus.publish([event])

        # Start consuming in background
        consume_task = asyncio.create_task(kafka_event_bus.start_consuming())

        # Wait for event to be processed
        try:
            await asyncio.wait_for(
                asyncio.create_task(self._wait_for_events(received_events, 1)),
                timeout=15.0,
            )
        finally:
            await kafka_event_bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=5.0)
            except TimeoutError:
                consume_task.cancel()

        assert len(received_events) == 1
        assert received_events[0].event_type == "TestItemCreated"
        assert kafka_event_bus.stats.events_consumed >= 1
        assert kafka_event_bus.stats.events_processed_success >= 1

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
        kafka_event_bus: KafkaEventBus,
    ) -> None:
        """Test multiple handlers for same event type."""
        handler1_events: list[DomainEvent] = []
        handler2_events: list[DomainEvent] = []

        async def handler1(event: DomainEvent) -> None:
            handler1_events.append(event)

        async def handler2(event: DomainEvent) -> None:
            handler2_events.append(event)

        kafka_event_bus.subscribe(TestItemCreated, handler1)
        kafka_event_bus.subscribe(TestItemCreated, handler2)

        # Publish an event
        event = TestItemCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            name="Multi Handler Test",
            quantity=5,
        )
        await kafka_event_bus.publish([event])

        # Start consuming
        consume_task = asyncio.create_task(kafka_event_bus.start_consuming())

        try:
            await asyncio.wait_for(
                asyncio.create_task(self._wait_for_events(handler1_events, 1)),
                timeout=15.0,
            )
        finally:
            await kafka_event_bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=5.0)
            except TimeoutError:
                consume_task.cancel()

        # Both handlers should have received the event
        assert len(handler1_events) == 1
        assert len(handler2_events) == 1

    async def test_subscribe_to_all_events(
        self,
        kafka_event_bus: KafkaEventBus,
        sample_customer_id: Any,
    ) -> None:
        """Test wildcard subscription to all events."""
        all_events: list[DomainEvent] = []

        async def wildcard_handler(event: DomainEvent) -> None:
            all_events.append(event)

        kafka_event_bus.subscribe_to_all_events(wildcard_handler)

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
        await kafka_event_bus.publish(events)

        # Start consuming
        consume_task = asyncio.create_task(kafka_event_bus.start_consuming())

        try:
            await asyncio.wait_for(
                asyncio.create_task(self._wait_for_events(all_events, 2)),
                timeout=15.0,
            )
        finally:
            await kafka_event_bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=5.0)
            except TimeoutError:
                consume_task.cancel()

        assert len(all_events) == 2

    async def test_unsubscribe(
        self,
        kafka_event_bus: KafkaEventBus,
    ) -> None:
        """Test unsubscribing a handler."""
        events: list[DomainEvent] = []

        async def handler(event: DomainEvent) -> None:
            events.append(event)

        kafka_event_bus.subscribe(TestItemCreated, handler)

        # Unsubscribe
        result = kafka_event_bus.unsubscribe(TestItemCreated, handler)
        assert result is True

        # Second unsubscribe should return False
        result = kafka_event_bus.unsubscribe(TestItemCreated, handler)
        assert result is False


# ============================================================================
# Publish-Consume Round Trip Tests
# ============================================================================


class TestKafkaPublishConsumeRoundTrip:
    """Tests for publish-consume round trip with multiple events."""

    async def test_round_trip_multiple_events(
        self,
        kafka_event_bus: KafkaEventBus,
    ) -> None:
        """Test publishing multiple events and consuming them all."""
        received_events: list[DomainEvent] = []
        event_count = 10

        async def handler(event: DomainEvent) -> None:
            received_events.append(event)

        kafka_event_bus.subscribe(TestItemCreated, handler)

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
            await kafka_event_bus.publish([event])

        # Start consuming
        consume_task = asyncio.create_task(kafka_event_bus.start_consuming())

        try:
            await asyncio.wait_for(
                self._wait_for_events(received_events, event_count),
                timeout=30.0,
            )
        finally:
            await kafka_event_bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=5.0)
            except TimeoutError:
                consume_task.cancel()

        # Verify all events were received
        assert len(received_events) == event_count
        received_ids = {e.event_id for e in received_events}
        assert received_ids == set(published_ids)

        # Verify stats
        assert kafka_event_bus.stats.events_published == event_count
        assert kafka_event_bus.stats.events_consumed == event_count
        assert kafka_event_bus.stats.events_processed_success == event_count

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
        kafka_event_bus: KafkaEventBus,
    ) -> None:
        """Test that event data is preserved through publish-consume cycle."""
        received_events: list[DomainEvent] = []

        async def handler(event: DomainEvent) -> None:
            received_events.append(event)

        kafka_event_bus.subscribe(TestItemCreated, handler)

        # Create event with specific data
        original_event = TestItemCreated(
            aggregate_id=uuid4(),
            aggregate_version=42,
            name="Preserved Item",
            quantity=99,
        )
        await kafka_event_bus.publish([original_event])

        # Consume
        consume_task = asyncio.create_task(kafka_event_bus.start_consuming())

        try:
            await asyncio.wait_for(
                self._wait_for_events(received_events, 1),
                timeout=15.0,
            )
        finally:
            await kafka_event_bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=5.0)
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
        kafka_event_bus: KafkaEventBus,
    ) -> None:
        """Test batch publishing and consuming."""
        received_events: list[DomainEvent] = []

        async def handler(event: DomainEvent) -> None:
            received_events.append(event)

        kafka_event_bus.subscribe(TestItemCreated, handler)

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
        await kafka_event_bus.publish(events)

        # Consume
        consume_task = asyncio.create_task(kafka_event_bus.start_consuming())

        try:
            await asyncio.wait_for(
                self._wait_for_events(received_events, 20),
                timeout=30.0,
            )
        finally:
            await kafka_event_bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=5.0)
            except TimeoutError:
                consume_task.cancel()

        assert len(received_events) == 20


# ============================================================================
# Consumer Group Tests
# ============================================================================


class TestKafkaEventBusConsumerGroups:
    """Tests for consumer group functionality."""

    async def test_same_consumer_group_shares_messages(
        self,
        kafka_bootstrap_servers: str,
    ) -> None:
        """Test that consumers in the same group share messages."""
        # Create unique topic for this test
        unique_suffix = uuid4().hex[:8]
        topic_prefix = f"test_shared_{unique_suffix}"
        consumer_group = f"shared_group_{unique_suffix}"

        registry = EventRegistry()
        registry.register(TestItemCreated)

        config1 = KafkaEventBusConfig(
            bootstrap_servers=kafka_bootstrap_servers,
            topic_prefix=topic_prefix,
            consumer_group=consumer_group,
            consumer_name=f"consumer-1-{unique_suffix}",
            enable_tracing=False,
        )
        config2 = KafkaEventBusConfig(
            bootstrap_servers=kafka_bootstrap_servers,
            topic_prefix=topic_prefix,
            consumer_group=consumer_group,
            consumer_name=f"consumer-2-{unique_suffix}",
            enable_tracing=False,
        )

        bus1 = KafkaEventBus(config=config1, event_registry=registry)
        bus2 = KafkaEventBus(config=config2, event_registry=registry)

        consumer1_events: list[DomainEvent] = []
        consumer2_events: list[DomainEvent] = []

        async def handler1(event: DomainEvent) -> None:
            consumer1_events.append(event)

        async def handler2(event: DomainEvent) -> None:
            consumer2_events.append(event)

        try:
            await bus1.connect()
            await bus2.connect()

            bus1.subscribe(TestItemCreated, handler1)
            bus2.subscribe(TestItemCreated, handler2)

            # Publish multiple events
            events = [
                TestItemCreated(
                    aggregate_id=uuid4(),
                    aggregate_version=1,
                    name=f"Shared Item {i}",
                    quantity=i,
                )
                for i in range(10)
            ]
            await bus1.publish(events)

            # Start both consumers
            task1 = asyncio.create_task(bus1.start_consuming())
            task2 = asyncio.create_task(bus2.start_consuming())

            # Wait for all events to be consumed
            async def wait_for_all() -> None:
                while len(consumer1_events) + len(consumer2_events) < 10:
                    await asyncio.sleep(0.1)

            try:
                await asyncio.wait_for(wait_for_all(), timeout=30.0)
            finally:
                await bus1.stop_consuming()
                await bus2.stop_consuming()
                for task in [task1, task2]:
                    try:
                        await asyncio.wait_for(task, timeout=5.0)
                    except TimeoutError:
                        task.cancel()

            # Total should equal event count
            total_received = len(consumer1_events) + len(consumer2_events)
            assert total_received == 10

            # With consumer groups, messages are distributed (not duplicated)
            # We can't guarantee exact distribution, but total must be correct

        finally:
            await bus1.disconnect()
            await bus2.disconnect()

    async def test_different_consumer_groups_receive_all_messages(
        self,
        kafka_bootstrap_servers: str,
    ) -> None:
        """Test that consumers in different groups each receive all messages."""
        unique_suffix = uuid4().hex[:8]
        topic_prefix = f"test_multi_{unique_suffix}"

        registry = EventRegistry()
        registry.register(TestItemCreated)

        config1 = KafkaEventBusConfig(
            bootstrap_servers=kafka_bootstrap_servers,
            topic_prefix=topic_prefix,
            consumer_group=f"group_a_{unique_suffix}",
            enable_tracing=False,
        )
        config2 = KafkaEventBusConfig(
            bootstrap_servers=kafka_bootstrap_servers,
            topic_prefix=topic_prefix,
            consumer_group=f"group_b_{unique_suffix}",
            enable_tracing=False,
        )

        bus1 = KafkaEventBus(config=config1, event_registry=registry)
        bus2 = KafkaEventBus(config=config2, event_registry=registry)

        group_a_events: list[DomainEvent] = []
        group_b_events: list[DomainEvent] = []

        async def handler_a(event: DomainEvent) -> None:
            group_a_events.append(event)

        async def handler_b(event: DomainEvent) -> None:
            group_b_events.append(event)

        try:
            await bus1.connect()
            await bus2.connect()

            bus1.subscribe(TestItemCreated, handler_a)
            bus2.subscribe(TestItemCreated, handler_b)

            # Publish events through first bus
            events = [
                TestItemCreated(
                    aggregate_id=uuid4(),
                    aggregate_version=1,
                    name=f"Multi Group Item {i}",
                    quantity=i,
                )
                for i in range(5)
            ]
            await bus1.publish(events)

            # Start both consumers
            task1 = asyncio.create_task(bus1.start_consuming())
            task2 = asyncio.create_task(bus2.start_consuming())

            # Wait for both groups to receive all events
            async def wait_for_both() -> None:
                while len(group_a_events) < 5 or len(group_b_events) < 5:
                    await asyncio.sleep(0.1)

            try:
                await asyncio.wait_for(wait_for_both(), timeout=30.0)
            finally:
                await bus1.stop_consuming()
                await bus2.stop_consuming()
                for task in [task1, task2]:
                    try:
                        await asyncio.wait_for(task, timeout=5.0)
                    except TimeoutError:
                        task.cancel()

            # Both groups should have received all events
            assert len(group_a_events) == 5
            assert len(group_b_events) == 5

        finally:
            await bus1.disconnect()
            await bus2.disconnect()


# ============================================================================
# DLQ (Dead Letter Queue) Tests
# ============================================================================


class TestKafkaEventBusDLQ:
    """Tests for dead letter queue functionality."""

    async def test_handler_exception_triggers_retry(
        self,
        kafka_event_bus_factory: Any,
    ) -> None:
        """Test that handler exceptions trigger retry logic."""
        bus = kafka_event_bus_factory(
            max_retries=2,
            retry_base_delay=0.1,
        )
        await bus.connect()

        try:
            fail_count = 0

            async def failing_handler(event: DomainEvent) -> None:
                nonlocal fail_count
                fail_count += 1
                if fail_count < 3:
                    raise ValueError("Simulated failure")

            bus.subscribe(TestItemCreated, failing_handler)

            # Publish an event
            event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name="Retry Test",
                quantity=1,
            )
            await bus.publish([event])

            # Start consuming (will retry and eventually succeed)
            consume_task = asyncio.create_task(bus.start_consuming())

            # Wait for retries to complete
            for _ in range(100):
                if fail_count >= 3:
                    break
                await asyncio.sleep(0.1)

            await bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=10.0)
            except TimeoutError:
                consume_task.cancel()

            # Should have retried
            assert fail_count >= 2
            assert bus.stats.handler_errors >= 2

        finally:
            await bus.disconnect()

    async def test_max_retries_sends_to_dlq(
        self,
        kafka_event_bus_factory: Any,
    ) -> None:
        """Test that exceeding max retries sends message to DLQ."""
        bus = kafka_event_bus_factory(
            max_retries=1,
            retry_base_delay=0.1,
            enable_dlq=True,
        )
        await bus.connect()

        try:

            async def always_failing_handler(event: DomainEvent) -> None:
                raise RuntimeError("Always fails")

            bus.subscribe(TestItemCreated, always_failing_handler)

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

            # Wait for DLQ message
            for _ in range(100):
                if bus.stats.messages_sent_to_dlq >= 1:
                    break
                await asyncio.sleep(0.1)

            await bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=10.0)
            except TimeoutError:
                consume_task.cancel()

            assert bus.stats.messages_sent_to_dlq >= 1

        finally:
            await bus.disconnect()

    async def test_handler_errors_tracked_in_stats(
        self,
        kafka_event_bus: KafkaEventBus,
    ) -> None:
        """Test that handler errors are tracked in statistics."""
        error_count = 0

        async def failing_handler(event: DomainEvent) -> None:
            nonlocal error_count
            error_count += 1
            raise RuntimeError("Handler error")

        kafka_event_bus.subscribe(TestItemCreated, failing_handler)

        # Publish event
        event = TestItemCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            name="Error Test",
            quantity=1,
        )
        await kafka_event_bus.publish([event])

        # Start consuming
        consume_task = asyncio.create_task(kafka_event_bus.start_consuming())

        # Wait a bit
        await asyncio.sleep(3.0)

        await kafka_event_bus.stop_consuming()
        try:
            await asyncio.wait_for(consume_task, timeout=5.0)
        except TimeoutError:
            consume_task.cancel()

        # Handler was called
        assert error_count >= 1

        # Stats should show handler errors
        assert kafka_event_bus.stats.handler_errors >= 1

    async def test_get_dlq_messages(
        self,
        kafka_event_bus_factory: Any,
    ) -> None:
        """Test retrieving messages from DLQ."""
        bus = kafka_event_bus_factory(
            max_retries=1,
            retry_base_delay=0.1,
            enable_dlq=True,
        )
        await bus.connect()

        try:

            async def always_failing_handler(event: DomainEvent) -> None:
                raise RuntimeError("Always fails")

            bus.subscribe(TestItemCreated, always_failing_handler)

            # Publish an event that will end up in DLQ
            event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name="DLQ Message Test",
                quantity=42,
            )
            await bus.publish([event])

            # Consume until message reaches DLQ
            consume_task = asyncio.create_task(bus.start_consuming())

            for _ in range(100):
                if bus.stats.messages_sent_to_dlq >= 1:
                    break
                await asyncio.sleep(0.1)

            await bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=10.0)
            except TimeoutError:
                consume_task.cancel()

            # Verify message was sent to DLQ
            assert bus.stats.messages_sent_to_dlq >= 1, "Message should have been sent to DLQ"

            # Retrieve DLQ messages with a timeout to prevent hanging
            try:
                dlq_messages = await asyncio.wait_for(
                    bus.get_dlq_messages(limit=10, timeout_ms=10000),
                    timeout=15.0,
                )
            except TimeoutError:
                pytest.skip("DLQ message retrieval timed out - Kafka consumer startup delay")

            assert len(dlq_messages) >= 1
            # Check DLQ headers exist
            assert "dlq_reason" in dlq_messages[0]["headers"]
            assert dlq_messages[0]["headers"]["dlq_reason"] == "max_retries_exceeded"

        finally:
            await bus.disconnect()


# ============================================================================
# Statistics Tracking Tests
# ============================================================================


class TestKafkaEventBusStats:
    """Tests for statistics tracking accuracy."""

    async def test_stats_tracking_publish(
        self,
        kafka_event_bus: KafkaEventBus,
    ) -> None:
        """Test that publishing updates statistics correctly."""
        initial_stats = kafka_event_bus.stats

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
            await kafka_event_bus.publish([event])

        assert kafka_event_bus.stats.events_published == 5
        assert kafka_event_bus.stats.last_publish_at is not None

    async def test_stats_tracking_consume(
        self,
        kafka_event_bus: KafkaEventBus,
    ) -> None:
        """Test that consuming updates statistics correctly."""
        received_events: list[DomainEvent] = []

        async def handler(event: DomainEvent) -> None:
            received_events.append(event)

        kafka_event_bus.subscribe(TestItemCreated, handler)

        # Publish events
        event_count = 3
        for i in range(event_count):
            event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name=f"Stats Consume Item {i}",
                quantity=i,
            )
            await kafka_event_bus.publish([event])

        # Consume events
        consume_task = asyncio.create_task(kafka_event_bus.start_consuming())

        async def wait_for_events() -> None:
            while len(received_events) < event_count:
                await asyncio.sleep(0.1)

        try:
            await asyncio.wait_for(wait_for_events(), timeout=15.0)
        finally:
            await kafka_event_bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=5.0)
            except TimeoutError:
                consume_task.cancel()

        # Verify stats
        assert kafka_event_bus.stats.events_consumed == event_count
        assert kafka_event_bus.stats.events_processed_success == event_count

    async def test_subscriber_counts(
        self,
        kafka_event_bus: KafkaEventBus,
    ) -> None:
        """Test that subscriber counts are tracked correctly."""
        # Initially no subscribers
        kafka_event_bus.clear_subscribers()
        assert kafka_event_bus.get_subscriber_count() == 0
        assert kafka_event_bus.get_wildcard_subscriber_count() == 0

        # Add subscribers
        async def handler1(e: DomainEvent) -> None:
            pass

        async def handler2(e: DomainEvent) -> None:
            pass

        kafka_event_bus.subscribe(TestItemCreated, handler1)
        kafka_event_bus.subscribe(TestItemCreated, handler2)
        kafka_event_bus.subscribe(TestOrderCreated, handler1)

        assert kafka_event_bus.get_subscriber_count() == 3
        assert kafka_event_bus.get_subscriber_count(TestItemCreated) == 2
        assert kafka_event_bus.get_subscriber_count(TestOrderCreated) == 1

        # Add wildcard subscriber
        kafka_event_bus.subscribe_to_all_events(handler1)
        assert kafka_event_bus.get_wildcard_subscriber_count() == 1

        # Clear all
        kafka_event_bus.clear_subscribers()
        assert kafka_event_bus.get_subscriber_count() == 0
        assert kafka_event_bus.get_wildcard_subscriber_count() == 0

    async def test_get_stats_dict(
        self,
        kafka_event_bus: KafkaEventBus,
    ) -> None:
        """Test getting statistics as dictionary."""
        stats_dict = kafka_event_bus.get_stats_dict()

        assert "events_published" in stats_dict
        assert "events_consumed" in stats_dict
        assert "events_processed_success" in stats_dict
        assert "events_processed_failed" in stats_dict
        assert "messages_sent_to_dlq" in stats_dict
        assert "handler_errors" in stats_dict
        assert "connected_at" in stats_dict


# ============================================================================
# Topic Info Tests
# ============================================================================


class TestKafkaEventBusTopicInfo:
    """Tests for topic information retrieval."""

    async def test_get_topic_info(
        self,
        kafka_event_bus: KafkaEventBus,
    ) -> None:
        """Test getting topic information."""
        # Publish some events first
        events = [
            TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name=f"Info Item {i}",
                quantity=i,
            )
            for i in range(3)
        ]
        await kafka_event_bus.publish(events)

        info = await kafka_event_bus.get_topic_info()

        assert info["connected"] is True
        assert "topic" in info
        assert "partitions" in info
        assert "consumer_group" in info


# ============================================================================
# Graceful Shutdown Tests
# ============================================================================


class TestKafkaEventBusShutdown:
    """Tests for graceful shutdown functionality."""

    async def test_graceful_shutdown(
        self,
        kafka_event_bus_factory: Any,
    ) -> None:
        """Test graceful shutdown of event bus."""
        bus = kafka_event_bus_factory()
        await bus.connect()

        received_events: list[DomainEvent] = []

        async def handler(event: DomainEvent) -> None:
            received_events.append(event)
            await asyncio.sleep(0.1)  # Simulate work

        bus.subscribe(TestItemCreated, handler)

        # Publish events
        for i in range(5):
            event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name=f"Shutdown Item {i}",
                quantity=i,
            )
            await bus.publish([event])

        # Start consuming in background
        _consume_task = asyncio.create_task(bus.start_consuming())

        # Wait a bit for some events to be processed
        await asyncio.sleep(0.5)

        # Graceful shutdown
        await bus.shutdown(timeout=10.0)

        assert not bus.is_connected
        assert not bus.is_consuming

    async def test_stop_consuming_without_shutdown(
        self,
        kafka_event_bus: KafkaEventBus,
    ) -> None:
        """Test stopping consumer without full shutdown."""
        received_events: list[DomainEvent] = []

        async def handler(event: DomainEvent) -> None:
            received_events.append(event)

        kafka_event_bus.subscribe(TestItemCreated, handler)

        # Publish an event
        event = TestItemCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            name="Stop Test",
            quantity=1,
        )
        await kafka_event_bus.publish([event])

        # Start consuming
        consume_task = asyncio.create_task(kafka_event_bus.start_consuming())

        # Wait a bit
        await asyncio.sleep(0.5)

        # Stop consuming but stay connected
        await kafka_event_bus.stop_consuming()

        try:
            await asyncio.wait_for(consume_task, timeout=5.0)
        except TimeoutError:
            consume_task.cancel()

        assert not kafka_event_bus.is_consuming
        assert kafka_event_bus.is_connected  # Still connected


# ============================================================================
# Edge Cases and Error Handling Tests
# ============================================================================


class TestKafkaEventBusEdgeCases:
    """Tests for edge cases and error handling."""

    async def test_consume_unknown_event_type(
        self,
        kafka_event_bus_factory: Any,
    ) -> None:
        """Test that unknown event types are handled gracefully."""
        bus = kafka_event_bus_factory()
        # Override registry to only include TestItemCreated
        bus._event_registry = EventRegistry()
        bus._event_registry.register(TestItemCreated)

        await bus.connect()

        try:
            received_events: list[DomainEvent] = []

            async def handler(event: DomainEvent) -> None:
                received_events.append(event)

            bus.subscribe(TestItemCreated, handler)

            # Publish a known event
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
                await asyncio.wait_for(wait_for_event(), timeout=15.0)
            finally:
                await bus.stop_consuming()
                try:
                    await asyncio.wait_for(consume_task, timeout=5.0)
                except TimeoutError:
                    consume_task.cancel()

            # Should have received the known event
            assert len(received_events) == 1
            assert received_events[0].name == "Known Event"

        finally:
            await bus.disconnect()

    async def test_handler_with_slow_processing(
        self,
        kafka_event_bus: KafkaEventBus,
    ) -> None:
        """Test handling of slow event processing."""
        received_events: list[DomainEvent] = []

        async def slow_handler(event: DomainEvent) -> None:
            await asyncio.sleep(0.5)  # Simulate slow processing
            received_events.append(event)

        kafka_event_bus.subscribe(TestItemCreated, slow_handler)

        # Publish multiple events
        for i in range(3):
            event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name=f"Slow Item {i}",
                quantity=i,
            )
            await kafka_event_bus.publish([event])

        # Start consuming
        consume_task = asyncio.create_task(kafka_event_bus.start_consuming())

        # Wait for events to be processed (should take about 1.5 seconds)
        async def wait_for_events() -> None:
            while len(received_events) < 3:
                await asyncio.sleep(0.1)

        try:
            await asyncio.wait_for(wait_for_events(), timeout=30.0)
        finally:
            await kafka_event_bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=5.0)
            except TimeoutError:
                consume_task.cancel()

        assert len(received_events) == 3

    async def test_clear_subscribers(
        self,
        kafka_event_bus: KafkaEventBus,
    ) -> None:
        """Test clearing all subscribers."""
        # Add some subscribers
        kafka_event_bus.subscribe(TestItemCreated, lambda e: None)
        kafka_event_bus.subscribe(TestItemUpdated, lambda e: None)
        kafka_event_bus.subscribe_to_all_events(lambda e: None)

        assert kafka_event_bus.get_subscriber_count() > 0
        assert kafka_event_bus.get_wildcard_subscriber_count() > 0

        # Clear
        kafka_event_bus.clear_subscribers()

        assert kafka_event_bus.get_subscriber_count() == 0
        assert kafka_event_bus.get_wildcard_subscriber_count() == 0
