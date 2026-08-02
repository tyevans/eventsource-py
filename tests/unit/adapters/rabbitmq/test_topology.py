"""Unit tests for RabbitMQTopology (bus god-class decomposition, Task 5)."""

from __future__ import annotations

from unittest import mock
from uuid import UUID, uuid4

import pytest

from eventsource.adapters.rabbitmq.config import RabbitMQEventBusConfig
from eventsource.adapters.rabbitmq.topology import RabbitMQTopology
from eventsource.domain.event import DomainEvent


class OrderCreated(DomainEvent):
    """Sample event used to exercise bind_event_type."""

    aggregate_type: str = "Order"
    aggregate_id: UUID = uuid4()


class AutoNamedOrderShipped(DomainEvent):
    """Sample event with no explicit event_type, exercising the auto-name fallback."""

    aggregate_type: str = "Order"
    aggregate_id: UUID = uuid4()


def _fake_channel() -> mock.AsyncMock:
    channel = mock.AsyncMock()
    channel.declare_exchange = mock.AsyncMock(return_value=mock.AsyncMock())
    channel.declare_queue = mock.AsyncMock(return_value=mock.AsyncMock())
    return channel


def _fake_connection(channel: mock.AsyncMock) -> mock.MagicMock:
    connection = mock.MagicMock()
    connection.require_channel = mock.MagicMock(return_value=channel)
    return connection


@pytest.fixture
def config() -> RabbitMQEventBusConfig:
    return RabbitMQEventBusConfig(
        rabbitmq_url="amqp://guest:guest@localhost:5672/",
        exchange_name="test-events",
        exchange_type="topic",
        enable_dlq=False,
    )


@pytest.fixture
def dlq_config() -> RabbitMQEventBusConfig:
    return RabbitMQEventBusConfig(
        rabbitmq_url="amqp://guest:guest@localhost:5672/",
        exchange_name="test-events",
        exchange_type="topic",
        enable_dlq=True,
    )


@pytest.mark.asyncio
async def test_declare_all_declares_main_exchange(config: RabbitMQEventBusConfig) -> None:
    channel = _fake_channel()
    topology = RabbitMQTopology(config=config, connection=_fake_connection(channel))

    await topology.declare_all()

    channel.declare_exchange.assert_awaited_once()
    call_kwargs = channel.declare_exchange.call_args.kwargs
    assert call_kwargs["name"] == config.exchange_name
    assert call_kwargs["durable"] == config.durable
    assert topology.exchange is not None


@pytest.mark.asyncio
async def test_declare_all_skips_dlq_when_disabled(config: RabbitMQEventBusConfig) -> None:
    channel = _fake_channel()
    topology = RabbitMQTopology(config=config, connection=_fake_connection(channel))

    await topology.declare_all()

    assert topology.dlq_exchange is None
    assert topology.dlq_queue is None
    # Only the main exchange should have been declared.
    channel.declare_exchange.assert_awaited_once()


@pytest.mark.asyncio
async def test_declare_all_declares_dlq_when_enabled(dlq_config: RabbitMQEventBusConfig) -> None:
    channel = _fake_channel()
    topology = RabbitMQTopology(config=dlq_config, connection=_fake_connection(channel))

    await topology.declare_all()

    assert topology.dlq_exchange is not None
    assert topology.dlq_queue is not None
    # DLQ exchange + main exchange = 2 declare_exchange calls.
    assert channel.declare_exchange.await_count == 2


@pytest.mark.asyncio
async def test_redeclare_reissues_declarations(config: RabbitMQEventBusConfig) -> None:
    channel = _fake_channel()
    topology = RabbitMQTopology(config=config, connection=_fake_connection(channel))

    await topology.declare_all()
    await topology.redeclare()

    assert channel.declare_exchange.await_count == 2
    assert channel.declare_queue.await_count == 2


@pytest.mark.asyncio
async def test_bind_event_type_binds_derived_routing_key(config: RabbitMQEventBusConfig) -> None:
    channel = _fake_channel()
    topology = RabbitMQTopology(config=config, connection=_fake_connection(channel))
    await topology.declare_all()

    await topology.bind_event_type(OrderCreated)

    topology.consumer_queue.bind.assert_awaited_with(  # type: ignore[union-attr]
        exchange=topology.exchange,
        routing_key="Order.OrderCreated",
    )


@pytest.mark.asyncio
async def test_bind_event_type_auto_named_event_uses_class_name(
    config: RabbitMQEventBusConfig,
) -> None:
    """Regression test: auto-named events (no explicit event_type field) must not
    produce a binding key with an empty event-type segment (e.g. "Order.")."""
    channel = _fake_channel()
    topology = RabbitMQTopology(config=config, connection=_fake_connection(channel))
    await topology.declare_all()

    await topology.bind_event_type(AutoNamedOrderShipped)

    topology.consumer_queue.bind.assert_awaited_with(  # type: ignore[union-attr]
        exchange=topology.exchange,
        routing_key="Order.AutoNamedOrderShipped",
    )


@pytest.mark.asyncio
async def test_bind_event_type_without_topology_raises(config: RabbitMQEventBusConfig) -> None:
    channel = _fake_channel()
    topology = RabbitMQTopology(config=config, connection=_fake_connection(channel))

    with pytest.raises(RuntimeError, match="Not connected or queue/exchange not initialized"):
        await topology.bind_event_type(OrderCreated)


@pytest.mark.asyncio
async def test_bind_routing_key_binds_given_key(config: RabbitMQEventBusConfig) -> None:
    channel = _fake_channel()
    topology = RabbitMQTopology(config=config, connection=_fake_connection(channel))
    await topology.declare_all()

    await topology.bind_routing_key("Order.*")

    topology.consumer_queue.bind.assert_awaited_with(  # type: ignore[union-attr]
        exchange=topology.exchange,
        routing_key="Order.*",
    )


@pytest.mark.asyncio
async def test_declare_exchange_raises_without_channel(config: RabbitMQEventBusConfig) -> None:
    connection = mock.MagicMock()
    connection.require_channel = mock.MagicMock(
        side_effect=RuntimeError("Not connected to RabbitMQ")
    )
    topology = RabbitMQTopology(config=config, connection=connection)

    with pytest.raises(RuntimeError, match="Not connected to RabbitMQ"):
        await topology._declare_exchange()


def test_properties_reflect_declared_objects(config: RabbitMQEventBusConfig) -> None:
    topology = RabbitMQTopology(config=config, connection=_fake_connection(_fake_channel()))

    assert topology.exchange is None
    assert topology.dlq_exchange is None
    assert topology.consumer_queue is None
    assert topology.dlq_queue is None
