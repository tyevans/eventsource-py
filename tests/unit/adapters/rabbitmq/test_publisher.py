"""Unit tests for RabbitMQPublisher (bus god-class decomposition, Task 6)."""

from __future__ import annotations

import asyncio
from unittest import mock
from uuid import UUID, uuid4

import pytest

from eventsource.adapters.rabbitmq import serialization
from eventsource.adapters.rabbitmq.config import RabbitMQEventBusConfig
from eventsource.adapters.rabbitmq.models import BatchPublishError, RabbitMQEventBusStats
from eventsource.adapters.rabbitmq.publisher import RabbitMQPublisher
from eventsource.domain.event import DomainEvent


class PublisherTestEvent(DomainEvent):
    aggregate_type: str = "Order"
    event_type: str = "OrderCreated"
    aggregate_id: UUID = uuid4()


@pytest.fixture
def config() -> RabbitMQEventBusConfig:
    return RabbitMQEventBusConfig(
        rabbitmq_url="amqp://guest:guest@localhost:5672/",
        exchange_name="test-events",
        consumer_group="test-group",
    )


@pytest.fixture
def stats() -> RabbitMQEventBusStats:
    return RabbitMQEventBusStats()


def _fake_connection() -> mock.MagicMock:
    return mock.MagicMock()


def _fake_topology(exchange: mock.AsyncMock | None = None) -> mock.MagicMock:
    topology = mock.MagicMock()
    topology.exchange = exchange if exchange is not None else mock.AsyncMock()
    return topology


def _publisher(
    config: RabbitMQEventBusConfig,
    stats: RabbitMQEventBusStats,
    exchange: mock.AsyncMock | None = None,
) -> tuple[RabbitMQPublisher, mock.MagicMock]:
    topology = _fake_topology(exchange)
    publisher = RabbitMQPublisher(
        config=config,
        connection=_fake_connection(),
        topology=topology,
        stats=stats,
        tracer=None,
        enable_tracing=False,
    )
    return publisher, topology


class TestPublishOne:
    async def test_publish_one_publishes_single_message_with_routing_key(
        self, config: RabbitMQEventBusConfig, stats: RabbitMQEventBusStats
    ) -> None:
        exchange = mock.AsyncMock()
        publisher, _ = _publisher(config, stats, exchange)
        event = PublisherTestEvent(aggregate_id=uuid4())
        await publisher.publish_one(event)
        exchange.publish.assert_awaited_once()
        _, kwargs = exchange.publish.call_args
        assert kwargs["routing_key"] == serialization.get_routing_key(event)

    async def test_publish_one_increments_events_published_stat(
        self, config: RabbitMQEventBusConfig, stats: RabbitMQEventBusStats
    ) -> None:
        publisher, _ = _publisher(config, stats)
        await publisher.publish_one(PublisherTestEvent(aggregate_id=uuid4()))
        assert stats.events_published == 1

    async def test_publish_one_wait_for_confirm_true_increments_confirms(
        self, config: RabbitMQEventBusConfig, stats: RabbitMQEventBusStats
    ) -> None:
        publisher, _ = _publisher(config, stats)
        await publisher.publish_one(PublisherTestEvent(aggregate_id=uuid4()), wait_for_confirm=True)
        assert stats.publish_confirms == 1

    async def test_publish_one_wait_for_confirm_false_does_not_increment_confirms(
        self, config: RabbitMQEventBusConfig, stats: RabbitMQEventBusStats
    ) -> None:
        publisher, _ = _publisher(config, stats)
        await publisher.publish_one(
            PublisherTestEvent(aggregate_id=uuid4()), wait_for_confirm=False
        )
        assert stats.publish_confirms == 0

    async def test_publish_one_raises_when_exchange_not_initialized(
        self, config: RabbitMQEventBusConfig, stats: RabbitMQEventBusStats
    ) -> None:
        publisher, topology = _publisher(config, stats)
        topology.exchange = None
        with pytest.raises(RuntimeError, match="Exchange not initialized"):
            await publisher.publish_one(PublisherTestEvent(aggregate_id=uuid4()))


class TestPublishBatch:
    async def test_publish_batch_raises_batch_publish_error_on_failures(
        self, config: RabbitMQEventBusConfig, stats: RabbitMQEventBusStats
    ) -> None:
        exchange = mock.AsyncMock()
        exchange.publish = mock.AsyncMock(side_effect=RuntimeError("boom"))
        publisher, _ = _publisher(config, stats, exchange)
        events = [PublisherTestEvent(aggregate_id=uuid4()) for _ in range(3)]
        with pytest.raises(BatchPublishError) as exc_info:
            await publisher.publish_batch(events)
        assert exc_info.value.results["total"] == 3 and exc_info.value.results["failed"] == 3
        assert len(exc_info.value.errors) == 3

    async def test_publish_batch_returns_stats_on_success(
        self, config: RabbitMQEventBusConfig, stats: RabbitMQEventBusStats
    ) -> None:
        publisher, _ = _publisher(config, stats)
        result = await publisher.publish_batch(
            [PublisherTestEvent(aggregate_id=uuid4()) for _ in range(4)]
        )
        assert (result["total"], result["published"], result["failed"]) == (4, 4, 0)

    async def test_publish_batch_sequential_preserves_order(
        self, config: RabbitMQEventBusConfig, stats: RabbitMQEventBusStats
    ) -> None:
        call_order: list[int] = []
        exchange = mock.AsyncMock()

        async def _record_publish(message: object, routing_key: str) -> None:
            call_order.append(len(call_order))

        exchange.publish = mock.AsyncMock(side_effect=_record_publish)
        publisher, _ = _publisher(config, stats, exchange)
        events = [PublisherTestEvent(aggregate_id=uuid4()) for _ in range(5)]
        result = await publisher.publish_batch(events, preserve_order=True)
        assert result["published"] == 5 and call_order == list(range(5))


class TestPublishConcurrencyBound:
    async def test_max_concurrent_publishes_holds_across_two_concurrent_publish_many_calls(
        self, stats: RabbitMQEventBusStats
    ) -> None:
        """max_concurrent_publishes bounds in-flight publishes across concurrent publish_many calls."""
        max_concurrent = 3
        config = RabbitMQEventBusConfig(
            rabbitmq_url="amqp://guest:guest@localhost:5672/",
            exchange_name="test-events",
            consumer_group="test-group",
            publish_chunk_size=100,
            max_concurrent_publishes=max_concurrent,
        )
        in_flight, peak_in_flight = 0, 0
        lock = asyncio.Lock()

        async def _slow_publish(message: object, routing_key: str) -> None:
            nonlocal in_flight, peak_in_flight
            async with lock:
                in_flight += 1
                peak_in_flight = max(peak_in_flight, in_flight)
            await asyncio.sleep(0.02)
            async with lock:
                in_flight -= 1

        exchange = mock.AsyncMock()
        exchange.publish = mock.AsyncMock(side_effect=_slow_publish)
        publisher, _ = _publisher(config, stats, exchange)
        events_a = [PublisherTestEvent(aggregate_id=uuid4()) for _ in range(10)]
        events_b = [PublisherTestEvent(aggregate_id=uuid4()) for _ in range(10)]
        await asyncio.gather(publisher.publish_many(events_a), publisher.publish_many(events_b))
        assert peak_in_flight <= max_concurrent
        assert peak_in_flight == max_concurrent
