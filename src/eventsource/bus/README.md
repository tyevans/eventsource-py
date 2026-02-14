# Event Bus

Event bus implementations for publishing and subscribing to domain events. The event bus decouples event producers from consumers, allowing projections and handlers to react to events independently.

## Key Interfaces

- `EventBus` -- Abstract base class for event bus implementations
- `EventHandlerFunc` -- Type alias for handler functions (sync or async)
- `FlexibleEventHandler` / `FlexibleEventSubscriber` -- Protocols from `eventsource.protocols`

## Module Map

- `interface.py` -- `EventBus` ABC and core protocols
- `memory.py` -- `InMemoryEventBus` for single-process, in-memory pub/sub
- `redis.py` -- `RedisEventBus` using Redis Pub/Sub
- `rabbitmq.py` -- `RabbitMQEventBus` using AMQP (aio-pika)
- `kafka.py` -- `KafkaEventBus` using Kafka (aiokafka)

## Invariants

- **Thread-safe**: All implementations must be thread-safe
- **Async-first**: All bus operations are async
- **Flexible handlers**: Bus must support both sync and async handlers
- **No ordering guarantees**: Distributed buses (Redis, RabbitMQ, Kafka) do not guarantee event order across handlers
- **At-least-once delivery**: Distributed buses may deliver events multiple times (handlers should be idempotent)
- **Optional tracing**: Implementations should use `eventsource.observability.Tracer` for OpenTelemetry integration
