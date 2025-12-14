"""Event bus implementations for the eventsource library.

This module provides interfaces and implementations for publishing and
subscribing to domain events.

Available Implementations:
- InMemoryEventBus: In-process event distribution (development/testing/single-instance)
- RedisEventBus: Distributed event streaming via Redis Streams (production/multi-instance)
- RabbitMQEventBus: Distributed event messaging via RabbitMQ (production/multi-instance)
- KafkaEventBus: Distributed event streaming via Apache Kafka (production/multi-instance)

Example:
    >>> from eventsource.bus import InMemoryEventBus, EventSubscriber
    >>> from eventsource.events import DomainEvent
    >>>
    >>> class OrderCreated(DomainEvent):
    ...     event_type: str = "OrderCreated"
    ...     aggregate_type: str = "Order"
    ...     order_number: str
    ...
    >>> class OrderProjection:
    ...     def subscribed_to(self) -> list[type[DomainEvent]]:
    ...         return [OrderCreated]
    ...
    ...     async def handle(self, event: DomainEvent) -> None:
    ...         print(f"Order {event.order_number} created")
    ...
    >>> bus = InMemoryEventBus()
    >>> projection = OrderProjection()
    >>> bus.subscribe_all(projection)

For Redis-based distributed event bus:
    >>> from eventsource.bus import RedisEventBus, RedisEventBusConfig
    >>>
    >>> config = RedisEventBusConfig(
    ...     redis_url="redis://localhost:6379",
    ...     stream_prefix="myapp",
    ...     consumer_group="projections",
    ... )
    >>> bus = RedisEventBus(config=config)
    >>> await bus.connect()
    >>> await bus.publish([MyEvent(...)])

For RabbitMQ-based distributed event bus:
    >>> from eventsource.bus import RabbitMQEventBus, RabbitMQEventBusConfig
    >>>
    >>> config = RabbitMQEventBusConfig(
    ...     rabbitmq_url="amqp://guest:guest@localhost:5672/",
    ...     exchange_name="events",
    ...     consumer_group="projections",
    ... )
    >>> bus = RabbitMQEventBus(config=config)
    >>> await bus.connect()
    >>> await bus.publish([MyEvent(...)])

For Kafka-based distributed event bus:
    >>> from eventsource.bus import KafkaEventBus, KafkaEventBusConfig
    >>>
    >>> config = KafkaEventBusConfig(
    ...     bootstrap_servers="localhost:9092",
    ...     topic_prefix="events",
    ...     consumer_group="projections",
    ... )
    >>> bus = KafkaEventBus(config=config)
    >>> await bus.connect()
    >>> await bus.publish([MyEvent(...)])
"""

from eventsource.bus.interface import (
    EventBus,
    EventHandlerFunc,
)

# Kafka event bus - conditionally imported based on aiokafka availability
from eventsource.bus.kafka import (
    KAFKA_AVAILABLE,
    KafkaEventBus,
    KafkaEventBusConfig,
    KafkaEventBusStats,
    KafkaNotAvailableError,
)
from eventsource.bus.memory import InMemoryEventBus

# RabbitMQ event bus - conditionally imported based on aio-pika availability
from eventsource.bus.rabbitmq import (
    RABBITMQ_AVAILABLE,
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
    RabbitMQEventBusStats,
    RabbitMQNotAvailableError,
)

# Redis event bus - conditionally imported based on redis availability
from eventsource.bus.redis import (
    REDIS_AVAILABLE,
    RedisEventBus,
    RedisEventBusConfig,
    RedisEventBusStats,
    RedisNotAvailableError,
)

# Protocols from canonical location
from eventsource.protocols import (
    AsyncEventHandler,
    EventHandler,
    EventSubscriber,
    FlexibleEventHandler,
    FlexibleEventSubscriber,
)

__all__ = [
    # Interface and ABCs
    "EventBus",
    "EventHandlerFunc",
    "AsyncEventHandler",
    # Protocols (from canonical location)
    "EventHandler",
    "EventSubscriber",
    "FlexibleEventHandler",
    "FlexibleEventSubscriber",
    # Implementations
    "InMemoryEventBus",
    # Redis event bus (Task 11)
    "RedisEventBus",
    "RedisEventBusConfig",
    "RedisEventBusStats",
    "RedisNotAvailableError",
    "REDIS_AVAILABLE",
    # RabbitMQ event bus
    "RabbitMQEventBus",
    "RabbitMQEventBusConfig",
    "RabbitMQEventBusStats",
    "RabbitMQNotAvailableError",
    "RABBITMQ_AVAILABLE",
    # Kafka event bus
    "KafkaEventBus",
    "KafkaEventBusConfig",
    "KafkaEventBusStats",
    "KafkaNotAvailableError",
    "KAFKA_AVAILABLE",
]
