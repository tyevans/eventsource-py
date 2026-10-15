"""Tests for RabbitMQ reliability features including retries, graceful shutdown, and queue info."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest

from eventsource import DomainEvent, RabbitMQEventBus
from tests.integration.conftest import TestItemCreated

from .conftest import skip_if_no_rabbitmq_infra

pytestmark = [pytest.mark.integration, pytest.mark.rabbitmq, skip_if_no_rabbitmq_infra]


def _make_item(name: str, quantity: int = 1) -> TestItemCreated:
    return TestItemCreated(aggregate_id=uuid4(), aggregate_version=1, name=name, quantity=quantity)


async def _consume_for(bus: RabbitMQEventBus, duration: float) -> None:
    """Start consuming for `duration` seconds, then stop consuming cleanly."""
    consume_task = asyncio.create_task(bus.start_consuming())
    await asyncio.sleep(duration)
    await bus.stop_consuming()
    try:
        await asyncio.wait_for(consume_task, timeout=2.0)
    except TimeoutError:
        consume_task.cancel()


class TestRabbitMQReliabilityRetry:
    @pytest.mark.slow
    async def test_message_retried_up_to_max_retries(self, rabbitmq_event_bus_factory: Any) -> None:
        sfx, max_retries = uuid4().hex[:8], 2
        bus = rabbitmq_event_bus_factory(
            exchange_name=f"test_retry_count_{sfx}",
            consumer_group=f"test_retry_count_group_{sfx}",
            enable_dlq=True,
            max_retries=max_retries,
        )
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
            await bus.publish([_make_item("Retry Count Test")])
            await _consume_for(bus, 8.0)

            assert attempt_count >= max_retries + 1
            assert bus.stats.messages_sent_to_dlq >= 1
        finally:
            await bus.disconnect()

    @pytest.mark.slow
    async def test_retry_count_tracked_in_dlq_metadata(
        self, rabbitmq_event_bus_factory: Any
    ) -> None:
        sfx, max_retries = uuid4().hex[:8], 3
        bus = rabbitmq_event_bus_factory(
            exchange_name=f"test_retry_dlq_count_{sfx}",
            consumer_group=f"test_retry_dlq_count_group_{sfx}",
            enable_dlq=True,
            max_retries=max_retries,
        )
        bus.config.retry_base_delay = 0.1
        bus.config.retry_max_delay = 0.3
        bus.config.retry_jitter = 0.0

        await bus.connect()
        try:

            async def failing_handler(event: DomainEvent) -> None:
                raise ValueError("Always fails")

            bus.subscribe(TestItemCreated, failing_handler)
            await bus.publish([_make_item("Retry DLQ Count Test")])
            await _consume_for(bus, 10.0)

            dlq_messages = await bus.get_dlq_messages(limit=5)
            assert len(dlq_messages) >= 1
            assert dlq_messages[0].dlq_retry_count is not None
            assert dlq_messages[0].dlq_retry_count >= max_retries
        finally:
            await bus.disconnect()


class TestRabbitMQReliabilityShutdown:
    @pytest.mark.benchmark
    async def test_shutdown_completes_within_timeout(self, rabbitmq_event_bus_factory: Any) -> None:
        sfx = uuid4().hex[:8]
        bus = rabbitmq_event_bus_factory(
            exchange_name=f"test_shutdown_{sfx}",
            consumer_group=f"test_shutdown_group_{sfx}",
        )
        await bus.connect()
        consume_task = asyncio.create_task(bus.start_consuming())
        await asyncio.sleep(0.5)

        shutdown_timeout, start_time = 10.0, datetime.now(UTC)
        try:
            await asyncio.wait_for(
                bus.shutdown(timeout=shutdown_timeout), timeout=shutdown_timeout + 5.0
            )
        except TimeoutError:
            consume_task.cancel()
            pytest.fail("Shutdown timed out")

        assert (datetime.now(UTC) - start_time).total_seconds() < shutdown_timeout + 2.0
        assert not bus.is_connected and not bus.is_consuming

    async def test_context_manager_graceful_shutdown(self, rabbitmq_event_bus_factory: Any) -> None:
        sfx = uuid4().hex[:8]
        bus = rabbitmq_event_bus_factory(
            exchange_name=f"test_ctx_shutdown_{sfx}",
            consumer_group=f"test_ctx_shutdown_group_{sfx}",
        )
        bus.config.shutdown_timeout = 10.0

        async with bus:
            assert bus.is_connected
            await bus.publish([_make_item("Context Manager Test")])

        assert not bus.is_connected

    async def test_shutdown_idempotent(self, rabbitmq_event_bus_factory: Any) -> None:
        sfx = uuid4().hex[:8]
        bus = rabbitmq_event_bus_factory(
            exchange_name=f"test_shutdown_idempotent_{sfx}",
            consumer_group=f"test_shutdown_idempotent_group_{sfx}",
        )
        await bus.connect()
        await bus.shutdown(timeout=5.0)
        assert not bus.is_connected
        await bus.shutdown(timeout=5.0)
        assert not bus.is_connected


class TestRabbitMQReliabilityQueueInfo:
    async def test_queue_info_returns_valid_data(
        self, rabbitmq_event_bus: RabbitMQEventBus
    ) -> None:
        info = await rabbitmq_event_bus.get_queue_info()
        assert info.name == rabbitmq_event_bus.config.queue_name
        assert info.message_count >= 0 and info.consumer_count >= 0
        assert info.state in ("running", "idle") and info.error is None

    async def test_health_check_reports_healthy_status(
        self, rabbitmq_event_bus: RabbitMQEventBus
    ) -> None:
        result = await rabbitmq_event_bus.health_check()
        assert result.healthy is True and result.connection_status == "connected"
        assert result.channel_status == "open" and result.queue_status == "accessible"
        assert result.error is None and result.details is not None
        assert "exchange" in result.details and "queue" in result.details
        assert "stats" in result.details

    async def test_health_check_reports_unhealthy_when_disconnected(
        self, rabbitmq_event_bus_factory: Any
    ) -> None:
        bus = rabbitmq_event_bus_factory(exchange_name=f"test_health_{uuid4().hex[:8]}")
        result = await bus.health_check()
        assert result.healthy is False and result.connection_status == "disconnected"
        assert result.error is not None and "Not connected" in result.error

    @pytest.mark.slow
    async def test_dlq_stats_tracking(self, rabbitmq_event_bus_factory: Any) -> None:
        sfx = uuid4().hex[:8]
        bus = rabbitmq_event_bus_factory(
            exchange_name=f"test_dlq_stats_{sfx}",
            consumer_group=f"test_dlq_stats_group_{sfx}",
            enable_dlq=True,
            max_retries=0,
        )
        await bus.connect()
        try:

            async def failing_handler(event: DomainEvent) -> None:
                raise ValueError("Fail for DLQ stats test")

            bus.subscribe(TestItemCreated, failing_handler)
            for i in range(4):
                await bus.publish([_make_item(f"DLQ Stats Test {i}", i)])

            await _consume_for(bus, 5.0)
            assert bus.stats.messages_sent_to_dlq >= 4
            assert bus.stats.events_processed_failed >= 4
            assert await bus.get_dlq_message_count() >= 0
        finally:
            await bus.disconnect()
