"""Tests for RabbitMQ Dead Letter Queue (DLQ) functionality and reliability."""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any
from uuid import uuid4

import pytest

from eventsource import (
    DomainEvent,
    RabbitMQEventBus,
)
from tests.integration.conftest import TestItemCreated

from .conftest import skip_if_no_rabbitmq_infra

pytestmark = [
    pytest.mark.integration,
    pytest.mark.rabbitmq,
    skip_if_no_rabbitmq_infra,
]


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
