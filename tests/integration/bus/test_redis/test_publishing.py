"""Tests for Redis event publishing."""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest

from eventsource import (
    DomainEvent,
    RedisEventBus,
)
from tests.integration.conftest import (
    TestItemCreated,
    TestOrderCreated,
    skip_if_no_redis_infra,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.redis,
    skip_if_no_redis_infra,
]


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
        events: list[DomainEvent] = [
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
