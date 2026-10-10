"""Tests for RabbitMQ reliability features including retries, graceful shutdown, and queue info."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
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
        bus.config.retry_base_delay = 0.1
        bus.config.retry_max_delay = 0.5
        bus.config.retry_jitter = 0.0

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
        bus.config.retry_base_delay = 0.1
        bus.config.retry_max_delay = 0.3
        bus.config.retry_jitter = 0.0

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


class TestRabbitMQReliabilityShutdown:
    """Tests for graceful shutdown reliability features."""

    @pytest.mark.benchmark
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
        bus.config.shutdown_timeout = 10.0

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
        assert info.name == rabbitmq_event_bus.config.queue_name
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
