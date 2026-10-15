"""Tests for RabbitMQ statistics tracking accuracy and serialization."""

from __future__ import annotations

import asyncio
import json
from typing import Any
from uuid import uuid4

import pytest

from eventsource import DomainEvent, RabbitMQEventBus
from tests.integration.conftest import TestItemCreated, TestOrderCreated

from .conftest import skip_if_no_rabbitmq_infra

pytestmark = [pytest.mark.integration, pytest.mark.rabbitmq, skip_if_no_rabbitmq_infra]


class TestRabbitMQEventBusStats:
    """Tests for statistics tracking accuracy."""

    async def test_stats_tracking_publish(self, rabbitmq_event_bus: RabbitMQEventBus) -> None:
        """Test that publishing updates statistics correctly."""
        assert rabbitmq_event_bus.stats.events_published == 0

        for i in range(5):
            event = TestItemCreated(
                aggregate_id=uuid4(), aggregate_version=1, name=f"Stats Item {i}", quantity=i
            )
            await rabbitmq_event_bus.publish([event])

        assert rabbitmq_event_bus.stats.events_published == 5
        assert rabbitmq_event_bus.stats.publish_confirms == 5

    async def test_stats_tracking_consume(self, rabbitmq_event_bus: RabbitMQEventBus) -> None:
        """Test that consuming updates statistics correctly."""
        received_events: list[DomainEvent] = []

        async def handler(event: DomainEvent) -> None:
            received_events.append(event)

        rabbitmq_event_bus.subscribe(TestItemCreated, handler)

        event_count = 3
        for i in range(event_count):
            event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name=f"Stats Consume Item {i}",
                quantity=i,
            )
            await rabbitmq_event_bus.publish([event])

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

        assert rabbitmq_event_bus.stats.events_consumed == event_count
        assert rabbitmq_event_bus.stats.events_processed_success == event_count

    async def test_stats_tracking_handler_errors(
        self, rabbitmq_event_bus: RabbitMQEventBus
    ) -> None:
        """Test that handler errors are tracked in statistics."""

        async def failing_handler(event: DomainEvent) -> None:
            raise ValueError("Intentional failure")

        rabbitmq_event_bus.subscribe(TestItemCreated, failing_handler)
        event = TestItemCreated(
            aggregate_id=uuid4(), aggregate_version=1, name="Error Stats Test", quantity=1
        )
        await rabbitmq_event_bus.publish([event])

        consume_task = asyncio.create_task(rabbitmq_event_bus.start_consuming())
        await asyncio.sleep(2.0)
        await rabbitmq_event_bus.stop_consuming()
        try:
            await asyncio.wait_for(consume_task, timeout=2.0)
        except TimeoutError:
            consume_task.cancel()

        assert rabbitmq_event_bus.stats.handler_errors >= 1
        assert rabbitmq_event_bus.stats.events_processed_failed >= 1

    async def test_subscriber_counts(self, rabbitmq_event_bus: RabbitMQEventBus) -> None:
        """Test that subscriber counts are tracked correctly."""
        rabbitmq_event_bus.clear_subscribers()
        assert rabbitmq_event_bus.get_subscriber_count() == 0
        assert rabbitmq_event_bus.get_wildcard_subscriber_count() == 0

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

        rabbitmq_event_bus.subscribe_to_all_events(handler1)
        assert rabbitmq_event_bus.get_wildcard_subscriber_count() == 1

        rabbitmq_event_bus.clear_subscribers()
        assert rabbitmq_event_bus.get_subscriber_count() == 0
        assert rabbitmq_event_bus.get_wildcard_subscriber_count() == 0


class TestRabbitMQReliabilityStats:
    """Tests for statistics tracking accuracy."""

    async def test_stats_accurately_track_operations(
        self, rabbitmq_event_bus: RabbitMQEventBus
    ) -> None:
        """Test that statistics accurately reflect operations."""
        received_events: list[DomainEvent] = []
        received_event = asyncio.Event()

        async def handler(event: DomainEvent) -> None:
            received_events.append(event)
            received_event.set()

        rabbitmq_event_bus.subscribe(TestItemCreated, handler)

        initial_published = rabbitmq_event_bus.stats.events_published
        initial_consumed = rabbitmq_event_bus.stats.events_consumed
        initial_success = rabbitmq_event_bus.stats.events_processed_success

        event_count = 5
        for i in range(event_count):
            event = TestItemCreated(
                aggregate_id=uuid4(), aggregate_version=1, name=f"Stats Test {i}", quantity=i
            )
            await rabbitmq_event_bus.publish([event])

        assert rabbitmq_event_bus.stats.events_published == initial_published + event_count
        assert rabbitmq_event_bus.stats.publish_confirms == initial_published + event_count

        consume_task = asyncio.create_task(rabbitmq_event_bus.start_consuming())

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

        assert rabbitmq_event_bus.stats.events_consumed >= initial_consumed + event_count
        assert rabbitmq_event_bus.stats.events_processed_success >= initial_success + event_count

    async def test_reset_stats_clears_counters(self, rabbitmq_event_bus: RabbitMQEventBus) -> None:
        """Test that reset_stats clears all counters."""
        for i in range(3):
            event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name=f"Reset Stats Test {i}",
                quantity=i,
            )
            await rabbitmq_event_bus.publish([event])

        assert rabbitmq_event_bus.stats.events_published >= 3
        assert rabbitmq_event_bus.stats.publish_confirms >= 3

        rabbitmq_event_bus.reset_stats()

        assert rabbitmq_event_bus.stats.events_published == 0
        assert rabbitmq_event_bus.stats.events_consumed == 0
        assert rabbitmq_event_bus.stats.events_processed_success == 0
        assert rabbitmq_event_bus.stats.events_processed_failed == 0
        assert rabbitmq_event_bus.stats.messages_sent_to_dlq == 0
        assert rabbitmq_event_bus.stats.handler_errors == 0
        assert rabbitmq_event_bus.stats.publish_confirms == 0

        if rabbitmq_event_bus.is_connected:
            assert rabbitmq_event_bus.stats.connected_at is not None

    async def test_stats_dict_returns_serializable_format(
        self, rabbitmq_event_bus: RabbitMQEventBus
    ) -> None:
        """Test that get_stats_dict returns a JSON-serializable format."""
        event = TestItemCreated(
            aggregate_id=uuid4(), aggregate_version=1, name="Stats Dict Test", quantity=1
        )
        await rabbitmq_event_bus.publish([event])

        stats_dict = rabbitmq_event_bus.get_stats_dict()
        assert json.dumps(stats_dict) is not None

        for key in (
            "events_published",
            "events_consumed",
            "events_processed_success",
            "events_processed_failed",
            "messages_sent_to_dlq",
            "handler_errors",
            "reconnections",
            "publish_confirms",
            "is_connected",
            "is_consuming",
            "uptime_seconds",
        ):
            assert key in stats_dict

        assert stats_dict["events_published"] >= 1
        assert stats_dict["is_connected"] is True

    @pytest.mark.slow
    async def test_handler_errors_tracked_separately(self, rabbitmq_event_bus_factory: Any) -> None:
        """Test that handler errors are tracked separately from processing failures."""
        unique_suffix = uuid4().hex[:8]
        bus = rabbitmq_event_bus_factory(
            exchange_name=f"test_handler_errors_{unique_suffix}",
            consumer_group=f"test_handler_errors_group_{unique_suffix}",
            enable_dlq=True,
            max_retries=1,
        )
        bus.config.retry_base_delay = 0.1
        bus.config.retry_jitter = 0.0
        await bus.connect()

        try:

            async def failing_handler(event: DomainEvent) -> None:
                raise RuntimeError("Handler error")

            bus.subscribe(TestItemCreated, failing_handler)
            event = TestItemCreated(
                aggregate_id=uuid4(), aggregate_version=1, name="Handler Error Test", quantity=1
            )
            await bus.publish([event])

            consume_task = asyncio.create_task(bus.start_consuming())
            await asyncio.sleep(5.0)
            await bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=2.0)
            except TimeoutError:
                consume_task.cancel()

            assert bus.stats.handler_errors >= 1
            assert bus.stats.events_processed_failed >= 1
        finally:
            await bus.disconnect()
