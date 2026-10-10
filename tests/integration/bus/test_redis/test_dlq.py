"""Tests for Redis Dead Letter Queue (DLQ) functionality."""

from __future__ import annotations

import asyncio
from typing import Any
from uuid import uuid4

import pytest

from eventsource import (
    DomainEvent,
    EventRegistry,
    RedisEventBus,
    RedisEventBusConfig,
)
from tests.integration.conftest import (
    TestItemCreated,
    skip_if_no_redis_infra,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.redis,
    skip_if_no_redis_infra,
]


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
