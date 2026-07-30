"""EventBus adapters: in-memory and broker backends (Redis, Kafka, RabbitMQ)."""

import asyncio
import os
from uuid import uuid4

from bench.adapters.base import BusAdapter
from bench.core.domain import make_registry
from eventsource import InMemoryEventBus
from eventsource.bus.interface import EventBus


class MemoryBusAdapter(BusAdapter):
    name = "memory"

    async def create(self) -> EventBus:
        return InMemoryEventBus(enable_tracing=False)

    async def destroy(self, resource: EventBus) -> None:
        await resource.shutdown()


async def _tcp_probe(host: str, port: int, service: str) -> str | None:
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=3)
        writer.close()
        await writer.wait_closed()
    except (TimeoutError, OSError) as exc:
        return f"{service} unreachable at {host}:{port}: {exc}"
    return None


class RedisBusAdapter(BusAdapter):
    """Consumption runs via `start_consuming()` as a background task and is
    stopped with `stop_consuming()`; RedisEventBus has no
    `start_consuming_in_background()` helper (unlike Kafka/RabbitMQ) -- see
    tests/integration/bus/test_redis.py's subscription tests, which all use
    `asyncio.create_task(bus.start_consuming())` / `await
    bus.stop_consuming()`. `shutdown()` alone tears the connection down (the
    conformance fixture in test_redis.py calls only `stop_consuming()` +
    cancel the task + `shutdown()`, never a separate `disconnect()`), so
    destroy() does not call disconnect() separately.
    """

    name = "redis"

    def __init__(self, url: str | None = None) -> None:
        self._url = url or os.environ.get("BENCH_REDIS_URL", "redis://localhost:6381")
        self._consume_tasks: dict[int, asyncio.Task[None]] = {}

    async def available(self) -> str | None:
        try:
            import redis  # noqa: F401
        except ImportError:
            return "redis extra not installed"
        host, _, port = self._url.removeprefix("redis://").partition(":")
        return await _tcp_probe(host or "localhost", int(port or 6379), "redis")

    async def create(self) -> EventBus:
        from eventsource.bus.redis import RedisEventBus, RedisEventBusConfig

        suffix = uuid4().hex[:8]
        config = RedisEventBusConfig(
            redis_url=self._url,
            stream_prefix=f"bench_{suffix}",
            consumer_group=f"bench_group_{suffix}",
            enable_tracing=False,
        )
        bus = RedisEventBus(config=config, event_registry=make_registry())
        await bus.connect()
        return bus

    async def start_delivery(self, bus: EventBus) -> None:
        from eventsource.bus.redis import RedisEventBus

        assert isinstance(bus, RedisEventBus)
        task = asyncio.create_task(bus.start_consuming())
        self._consume_tasks[id(bus)] = task
        await asyncio.sleep(0.5)  # let the consumer join the group

    async def stop_delivery(self, bus: EventBus) -> None:
        from eventsource.bus.redis import RedisEventBus

        assert isinstance(bus, RedisEventBus)
        if bus.is_consuming:
            await bus.stop_consuming()
        task = self._consume_tasks.pop(id(bus), None)
        if task is not None:
            task.cancel()

    async def destroy(self, resource: EventBus) -> None:
        await self.stop_delivery(resource)
        await resource.shutdown()


class KafkaBusAdapter(BusAdapter):
    name = "kafka"

    def __init__(self, servers: str | None = None) -> None:
        self._servers = servers or os.environ.get("BENCH_KAFKA_SERVERS", "localhost:9094")

    async def available(self) -> str | None:
        try:
            import aiokafka  # noqa: F401
        except ImportError:
            return "kafka extra not installed (aiokafka missing)"
        host, _, port = self._servers.partition(":")
        return await _tcp_probe(host or "localhost", int(port or 9092), "kafka")

    async def create(self) -> EventBus:
        from eventsource.bus.kafka import KafkaEventBus, KafkaEventBusConfig

        suffix = uuid4().hex[:8]
        config = KafkaEventBusConfig(
            bootstrap_servers=self._servers,
            topic_prefix=f"bench_{suffix}",
            consumer_group=f"bench_group_{suffix}",
            enable_dlq=True,
            enable_tracing=False,
        )
        bus = KafkaEventBus(config=config, event_registry=make_registry())
        await bus.connect()
        return bus

    async def start_delivery(self, bus: EventBus) -> None:
        from eventsource.bus.kafka import KafkaEventBus

        assert isinstance(bus, KafkaEventBus)
        bus.start_consuming_in_background()
        await asyncio.sleep(0.5)  # let the consumer join the group

    async def stop_delivery(self, bus: EventBus) -> None:
        from eventsource.bus.kafka import KafkaEventBus

        assert isinstance(bus, KafkaEventBus)
        if bus.is_consuming:
            await bus.stop_consuming()

    async def destroy(self, resource: EventBus) -> None:
        from eventsource.bus.kafka import KafkaEventBus

        assert isinstance(resource, KafkaEventBus)
        await self.stop_delivery(resource)
        if resource.is_connected:
            await resource.disconnect()


class RabbitMQBusAdapter(BusAdapter):
    name = "rabbitmq"

    def __init__(self, url: str | None = None) -> None:
        self._url = url or os.environ.get(
            "BENCH_RABBITMQ_URL", "amqp://guest:guest@localhost:5673/"
        )

    async def available(self) -> str | None:
        try:
            import aio_pika  # noqa: F401
        except ImportError:
            return "rabbitmq extra not installed (aio-pika missing)"
        hostport = self._url.split("@")[-1].split("/")[0]
        host, _, port = hostport.partition(":")
        return await _tcp_probe(host or "localhost", int(port or 5672), "rabbitmq")

    async def create(self) -> EventBus:
        from eventsource.bus.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig

        suffix = uuid4().hex[:8]
        config = RabbitMQEventBusConfig(
            rabbitmq_url=self._url,
            exchange_name=f"bench_{suffix}",
            consumer_group=f"bench_group_{suffix}",
            durable=False,
            auto_delete=True,
            enable_tracing=False,
        )
        bus = RabbitMQEventBus(config=config, event_registry=make_registry())
        await bus.connect()
        return bus

    async def start_delivery(self, bus: EventBus) -> None:
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        assert isinstance(bus, RabbitMQEventBus)
        bus.start_consuming_in_background()
        await asyncio.sleep(0.5)

    async def stop_delivery(self, bus: EventBus) -> None:
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        assert isinstance(bus, RabbitMQEventBus)
        if bus.is_consuming:
            await bus.stop_consuming()

    async def destroy(self, resource: EventBus) -> None:
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        assert isinstance(resource, RabbitMQEventBus)
        await self.stop_delivery(resource)
        if resource.is_connected:
            await resource.disconnect()


BUS_ADAPTERS: dict[str, type[BusAdapter]] = {
    MemoryBusAdapter.name: MemoryBusAdapter,
    RedisBusAdapter.name: RedisBusAdapter,
    KafkaBusAdapter.name: KafkaBusAdapter,
    RabbitMQBusAdapter.name: RabbitMQBusAdapter,
}
