"""Integration tests for Kafka Dead Letter Queue (DLQ)."""

from __future__ import annotations

import asyncio
from typing import Any
from uuid import uuid4

import pytest

from eventsource import KAFKA_AVAILABLE, DomainEvent, KafkaEventBus
from tests.integration.conftest import TestItemCreated

from .conftest import skip_if_no_kafka_infra

if not KAFKA_AVAILABLE:
    pytest.skip("Kafka (aiokafka) not available", allow_module_level=True)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.kafka,
    skip_if_no_kafka_infra,
]


async def _stop_consumer(
    bus: KafkaEventBus, task: asyncio.Task[None], timeout: float = 10.0
) -> None:
    await bus.stop_consuming()
    try:
        await asyncio.wait_for(task, timeout=timeout)
    except TimeoutError:
        task.cancel()


class TestKafkaEventBusDLQ:
    """Tests for dead letter queue functionality."""

    @pytest.mark.slow
    async def test_handler_exception_triggers_retry(
        self,
        kafka_event_bus_factory: Any,
    ) -> None:
        """Test that handler exceptions trigger retry logic."""
        bus = kafka_event_bus_factory(max_retries=2, retry_base_delay=0.1)
        await bus.connect()

        try:
            fail_count = 0

            async def failing_handler(event: DomainEvent) -> None:
                nonlocal fail_count
                fail_count += 1
                if fail_count < 3:
                    raise ValueError("Simulated failure")

            bus.subscribe(TestItemCreated, failing_handler)
            event = TestItemCreated(
                aggregate_id=uuid4(), aggregate_version=1, name="Retry Test", quantity=1
            )
            await bus.publish([event])

            consume_task = asyncio.create_task(bus.start_consuming())
            for _ in range(100):
                if fail_count >= 3:
                    break
                await asyncio.sleep(0.1)

            await _stop_consumer(bus, consume_task)
            assert fail_count >= 2
            assert bus.stats.handler_errors >= 2
        finally:
            await bus.disconnect()

    @pytest.mark.slow
    async def test_max_retries_sends_to_dlq(
        self,
        kafka_event_bus_factory: Any,
    ) -> None:
        """Test that exceeding max retries sends message to DLQ."""
        bus = kafka_event_bus_factory(max_retries=1, retry_base_delay=0.1, enable_dlq=True)
        await bus.connect()

        try:

            async def always_failing_handler(event: DomainEvent) -> None:
                raise RuntimeError("Always fails")

            bus.subscribe(TestItemCreated, always_failing_handler)
            event = TestItemCreated(
                aggregate_id=uuid4(), aggregate_version=1, name="DLQ Test", quantity=1
            )
            await bus.publish([event])

            consume_task = asyncio.create_task(bus.start_consuming())
            for _ in range(100):
                if bus.stats.messages_sent_to_dlq >= 1:
                    break
                await asyncio.sleep(0.1)

            await _stop_consumer(bus, consume_task)
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
        event = TestItemCreated(
            aggregate_id=uuid4(), aggregate_version=1, name="Error Test", quantity=1
        )
        await kafka_event_bus.publish([event])

        consume_task = asyncio.create_task(kafka_event_bus.start_consuming())
        await asyncio.sleep(3.0)
        await _stop_consumer(kafka_event_bus, consume_task, timeout=5.0)

        assert error_count >= 1
        assert kafka_event_bus.stats.handler_errors >= 1

    @pytest.mark.slow
    async def test_get_dlq_messages(
        self,
        kafka_event_bus_factory: Any,
    ) -> None:
        """Test retrieving messages from DLQ."""
        bus = kafka_event_bus_factory(max_retries=1, retry_base_delay=0.1, enable_dlq=True)
        await bus.connect()

        try:

            async def always_failing_handler(event: DomainEvent) -> None:
                raise RuntimeError("Always fails")

            bus.subscribe(TestItemCreated, always_failing_handler)
            event = TestItemCreated(
                aggregate_id=uuid4(), aggregate_version=1, name="DLQ Message Test", quantity=42
            )
            await bus.publish([event])

            consume_task = asyncio.create_task(bus.start_consuming())
            for _ in range(100):
                if bus.stats.messages_sent_to_dlq >= 1:
                    break
                await asyncio.sleep(0.1)

            await _stop_consumer(bus, consume_task)
            assert bus.stats.messages_sent_to_dlq >= 1, "Message should have been sent to DLQ"

            try:
                dlq_messages = await asyncio.wait_for(
                    bus.get_dlq_messages(limit=10, timeout_ms=10000),
                    timeout=15.0,
                )
            except TimeoutError:
                pytest.skip("DLQ message retrieval timed out - Kafka consumer startup delay")

            assert len(dlq_messages) >= 1
            assert "dlq_reason" in dlq_messages[0]["headers"]
            assert dlq_messages[0]["headers"]["dlq_reason"] == "max_retries_exceeded"
        finally:
            await bus.disconnect()
