"""Tests for Redis edge cases, error handling, and performance."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from eventsource import (
    DomainEvent,
    RedisEventBus,
)
from tests.integration.conftest import (
    TestItemCreated,
    TestItemUpdated,
    skip_if_no_redis_infra,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.redis,
    skip_if_no_redis_infra,
]


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
        await redis_event_bus.publish(events)
        duration = (datetime.now(UTC) - start).total_seconds()

        # Should be reasonably fast (using pipeline)
        assert duration < 5.0  # 5 seconds max for 100 events

        # All events should be published
        info = await redis_event_bus.get_stream_info()
        assert info["stream"]["length"] == 100
