"""Tests for RabbitMQ Dead Letter Queue (DLQ) functionality and reliability."""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any
from uuid import uuid4

import pytest

from eventsource import DomainEvent, RabbitMQEventBus
from tests.integration.conftest import TestItemCreated

from .conftest import skip_if_no_rabbitmq_infra

pytestmark = [
    pytest.mark.integration,
    pytest.mark.rabbitmq,
    skip_if_no_rabbitmq_infra,
]


async def _stop_consumer(bus: RabbitMQEventBus, consume_task: asyncio.Task[Any]) -> None:
    """Stop consuming and cleanly cancel task on timeout."""
    await bus.stop_consuming()
    try:
        await asyncio.wait_for(consume_task, timeout=2.0)
    except TimeoutError:
        consume_task.cancel()


def _make_item(name: str, quantity: int = 1) -> TestItemCreated:
    return TestItemCreated(
        aggregate_id=uuid4(),
        aggregate_version=1,
        name=name,
        quantity=quantity,
    )


class TestRabbitMQEventBusDLQ:
    """Tests for dead letter queue functionality."""

    async def test_handler_exception_sends_to_dlq(self, rabbitmq_event_bus_factory: Any) -> None:
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
            await bus.publish([_make_item("DLQ Test")])
            consume_task = asyncio.create_task(bus.start_consuming())
            await asyncio.sleep(3.0)
            await _stop_consumer(bus, consume_task)

            assert fail_count >= 1
            assert bus.stats.handler_errors >= 1
            assert bus.stats.events_processed_failed >= 1
        finally:
            await bus.disconnect()

    async def test_handler_errors_tracked_in_stats(
        self, rabbitmq_event_bus: RabbitMQEventBus
    ) -> None:
        """Test that handler errors are tracked in statistics."""
        error_count = 0

        async def failing_handler(event: DomainEvent) -> None:
            nonlocal error_count
            error_count += 1
            raise RuntimeError("Handler error")

        rabbitmq_event_bus.subscribe(TestItemCreated, failing_handler)
        await rabbitmq_event_bus.publish([_make_item("Error Test")])
        consume_task = asyncio.create_task(rabbitmq_event_bus.start_consuming())
        await asyncio.sleep(2.0)
        await _stop_consumer(rabbitmq_event_bus, consume_task)

        assert error_count >= 1
        assert rabbitmq_event_bus.stats.handler_errors >= 1


class TestRabbitMQReliabilityDLQ:
    """Tests for dead letter queue reliability features."""

    @pytest.mark.slow
    async def test_message_sent_to_dlq_after_max_retries(
        self, rabbitmq_event_bus_factory: Any
    ) -> None:
        """Test that messages are sent to DLQ after max_retries failures."""
        unique_suffix = uuid4().hex[:8]
        bus = rabbitmq_event_bus_factory(
            exchange_name=f"test_dlq_max_retries_{unique_suffix}",
            consumer_group=f"test_dlq_group_{unique_suffix}",
            enable_dlq=True,
            max_retries=2,
        )
        await bus.connect()
        try:
            failure_count = 0

            async def failing_handler(event: DomainEvent) -> None:
                nonlocal failure_count
                failure_count += 1
                raise ValueError(f"Intentional failure #{failure_count}")

            bus.subscribe(TestItemCreated, failing_handler)
            await bus.publish([_make_item("DLQ Max Retries Test")])
            consume_task = asyncio.create_task(bus.start_consuming())
            await asyncio.sleep(10.0)
            await _stop_consumer(bus, consume_task)

            assert failure_count >= 1
            assert bus.stats.events_processed_failed >= 1
            assert bus.stats.messages_sent_to_dlq >= 1

            dlq_messages = await bus.get_dlq_messages(limit=10)
            assert len(dlq_messages) >= 1
            dlq_msg = dlq_messages[0]
            assert dlq_msg.event_type == "TestItemCreated"
            assert dlq_msg.dlq_reason is not None
            assert "ValueError" in (dlq_msg.dlq_error_type or "")
            assert dlq_msg.dlq_retry_count is not None
        finally:
            await bus.disconnect()

    async def test_get_dlq_messages_returns_metadata(self, rabbitmq_event_bus_factory: Any) -> None:
        """Test that get_dlq_messages returns correct metadata."""
        unique_suffix = uuid4().hex[:8]
        bus = rabbitmq_event_bus_factory(
            exchange_name=f"test_dlq_metadata_{unique_suffix}",
            consumer_group=f"test_dlq_metadata_group_{unique_suffix}",
            enable_dlq=True,
            max_retries=0,
        )
        await bus.connect()
        try:

            async def failing_handler(event: DomainEvent) -> None:
                raise RuntimeError("Test error for DLQ metadata")

            bus.subscribe(TestItemCreated, failing_handler)
            await bus.publish([_make_item("DLQ Metadata Test", quantity=99)])
            consume_task = asyncio.create_task(bus.start_consuming())
            await asyncio.sleep(3.0)
            await _stop_consumer(bus, consume_task)

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

    async def test_replay_dlq_message_success(self, rabbitmq_event_bus_factory: Any) -> None:
        """Test replaying a message from DLQ back to main exchange."""
        unique_suffix = uuid4().hex[:8]
        bus = rabbitmq_event_bus_factory(
            exchange_name=f"test_dlq_replay_{unique_suffix}",
            consumer_group=f"test_dlq_replay_group_{unique_suffix}",
            enable_dlq=True,
            max_retries=0,
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
            await bus.publish([_make_item("Replay Test", quantity=42)])
            consume_task = asyncio.create_task(bus.start_consuming())
            await asyncio.sleep(3.0)

            dlq_messages = await bus.get_dlq_messages(limit=5)
            assert len(dlq_messages) >= 1
            message_id = dlq_messages[0].message_id
            assert message_id is not None

            replay_result = await bus.replay_dlq_message(message_id)
            assert replay_result is True

            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(replay_received.wait(), timeout=10.0)

            await _stop_consumer(bus, consume_task)
            assert len(received_events) >= 1
            assert received_events[0].event_type == "TestItemCreated"
        finally:
            await bus.disconnect()

    @pytest.mark.slow
    async def test_purge_dlq_removes_messages(self, rabbitmq_event_bus_factory: Any) -> None:
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
            event_count = 3
            for i in range(event_count):
                await bus.publish([_make_item(f"Purge Test {i}", quantity=i)])

            consume_task = asyncio.create_task(bus.start_consuming())
            await asyncio.sleep(5.0)

            assert bus.stats.messages_sent_to_dlq >= event_count
            _ = await bus.get_dlq_messages(limit=10)
            purged = await bus.purge_dlq()
            assert purged >= 0
            await _stop_consumer(bus, consume_task)
        finally:
            await bus.disconnect()
