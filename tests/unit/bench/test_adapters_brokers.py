"""Broker adapters: registry membership and unreachable-service probing."""

from bench.adapters.buses import (
    BUS_ADAPTERS,
    KafkaBusAdapter,
    RabbitMQBusAdapter,
    RedisBusAdapter,
)


def test_registry_contains_all_buses() -> None:
    assert set(BUS_ADAPTERS) == {"memory", "redis", "kafka", "rabbitmq"}


async def test_redis_unavailable_without_service() -> None:
    adapter = RedisBusAdapter(url="redis://localhost:1")
    reason = await adapter.available()
    assert reason is not None


async def test_kafka_unavailable_without_service() -> None:
    adapter = KafkaBusAdapter(servers="localhost:1")
    reason = await adapter.available()
    assert reason is not None


async def test_rabbitmq_unavailable_without_service() -> None:
    adapter = RabbitMQBusAdapter(url="amqp://guest:guest@localhost:1/")
    reason = await adapter.available()
    assert reason is not None
