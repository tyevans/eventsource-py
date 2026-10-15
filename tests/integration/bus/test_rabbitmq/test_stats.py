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
        initial_stats = rabbitmq_event_bus.stats
        # Verify initial state
        assert initial_stats.events_published == 0

        # Publish events
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
        self, rabbitmq_event_bus: RabbitMQEventBus
    ) -> None:
        """Test that handler errors are tracked in statistics."""

        async def failing_handler(event: DomainEvent) -> None:
            raise ValueError("Intentional failure")

        rabbitmq_event_bus.subscribe(TestItemCreated, failing_handler)

        # Publish event
        event = TestItemCreated(
            aggregate_id=uuid4(), aggregate_version=1, name="Error Stats Test", quantity=1
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

    async def test_subscriber_counts(self, rabbitmq_event_bus: RabbitMQEventBus) -> None:
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

        # Record initial stats
        initial_published = rabbitmq_event_bus.stats.events_published
        initial_consumed = rabbitmq_event_bus.stats.events_consumed
        initial_success = rabbitmq_event_bus.stats.events_processed_success

        # Publish events
        event_count = 5
        for i in range(event_count):
            event = TestItemCreated(
                aggregate_id=uuid4(), aggregate_version=1, name=f"Stats Test {i}", quantity=i
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

    async def test_reset_stats_clears_counters(self, rabbitmq_event_bus: RabbitMQEventBus) -> None:
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
        self, rabbitmq_event_bus: RabbitMQEventBus
    ) -> None:
        """Test that get_stats_dict returns a JSON-serializable format."""
        # Publish event
        event = TestItemCreated(
            aggregate_id=uuid4(), aggregate_version=1, name="Stats Dict Test", quantity=1
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

            # Publish event
            event = TestItemCreated(
                aggregate_id=uuid4(), aggregate_version=1, name="Handler Error Test", quantity=1
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
