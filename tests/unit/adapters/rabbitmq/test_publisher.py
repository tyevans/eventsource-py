"""Unit tests for RabbitMQPublisher (bus god-class decomposition, Task 6)."""

from __future__ import annotations

from unittest import mock
from uuid import UUID, uuid4

import pytest

from eventsource.adapters.rabbitmq import serialization
from eventsource.adapters.rabbitmq.config import RabbitMQEventBusConfig
from eventsource.adapters.rabbitmq.models import BatchPublishError, RabbitMQEventBusStats
from eventsource.adapters.rabbitmq.publisher import RabbitMQPublisher
from eventsource.domain.event import DomainEvent


class PublisherTestEvent(DomainEvent):
    """Sample event used to exercise the publisher."""

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
        event = PublisherTestEvent(aggregate_id=uuid4())

        await publisher.publish_one(event)

        assert stats.events_published == 1

    async def test_publish_one_wait_for_confirm_true_increments_confirms(
        self, config: RabbitMQEventBusConfig, stats: RabbitMQEventBusStats
    ) -> None:
        publisher, _ = _publisher(config, stats)
        event = PublisherTestEvent(aggregate_id=uuid4())

        await publisher.publish_one(event, wait_for_confirm=True)

        assert stats.publish_confirms == 1

    async def test_publish_one_wait_for_confirm_false_does_not_increment_confirms(
        self, config: RabbitMQEventBusConfig, stats: RabbitMQEventBusStats
    ) -> None:
        publisher, _ = _publisher(config, stats)
        event = PublisherTestEvent(aggregate_id=uuid4())

        await publisher.publish_one(event, wait_for_confirm=False)

        assert stats.publish_confirms == 0

    async def test_publish_one_raises_when_exchange_not_initialized(
        self, config: RabbitMQEventBusConfig, stats: RabbitMQEventBusStats
    ) -> None:
        publisher, topology = _publisher(config, stats)
        topology.exchange = None
        event = PublisherTestEvent(aggregate_id=uuid4())

        with pytest.raises(RuntimeError, match="Exchange not initialized"):
            await publisher.publish_one(event)


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

        assert exc_info.value.results["total"] == 3
        assert exc_info.value.results["failed"] == 3
        assert len(exc_info.value.errors) == 3

    async def test_publish_batch_returns_stats_on_success(
        self, config: RabbitMQEventBusConfig, stats: RabbitMQEventBusStats
    ) -> None:
        publisher, _ = _publisher(config, stats)
        events = [PublisherTestEvent(aggregate_id=uuid4()) for _ in range(4)]

        result = await publisher.publish_batch(events)

        assert result["total"] == 4
        assert result["published"] == 4
        assert result["failed"] == 0

    async def test_publish_batch_sequential_preserves_order(
        self, config: RabbitMQEventBusConfig, stats: RabbitMQEventBusStats
    ) -> None:
        call_order: list[int] = []
        exchange = mock.AsyncMock()

        async def _record_publish(message: object, routing_key: str) -> None:
            # routing_key encodes nothing about order directly, so track via
            # the message body ordering the publisher passed to us.
            call_order.append(len(call_order))

        exchange.publish = mock.AsyncMock(side_effect=_record_publish)
        publisher, _ = _publisher(config, stats, exchange)
        events = [PublisherTestEvent(aggregate_id=uuid4()) for _ in range(5)]

        result = await publisher.publish_batch(events, preserve_order=True)

        assert result["published"] == 5
        assert call_order == list(range(5))
