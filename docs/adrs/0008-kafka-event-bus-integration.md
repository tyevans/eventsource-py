# ADR-0008: Kafka Event Bus Integration

## Status

Accepted

## Date

2025-12-08

## Context

The eventsource library already supports two distributed event bus implementations:

1. **RedisEventBus**: Using Redis Streams for event distribution with consumer groups
2. **RabbitMQEventBus**: Using AMQP 0-9-1 for message-based event distribution

Users have requested Apache Kafka support for scenarios requiring:

- Higher throughput (10,000+ events/second)
- Long-term event retention (weeks/months vs. Redis's typical hours/days)
- Integration with existing Kafka infrastructure in enterprise environments
- Cross-datacenter replication using Kafka's built-in mirroring
- Ecosystem integration with Kafka Connect, Kafka Streams, and KSQL

The challenge is implementing Kafka support while maintaining consistency with the existing EventBus interface and established patterns from Redis and RabbitMQ implementations.

## Decision

We will implement a `KafkaEventBus` class that:

1. **Implements the EventBus interface** exactly as RedisEventBus and RabbitMQEventBus do
2. **Uses aiokafka** as the async Kafka client library
3. **Follows established patterns** from existing implementations for configuration, statistics, error handling, and lifecycle management
4. **Provides Kafka-specific features** including partition-based ordering, consumer groups, and DLQ with replay capability

### Key Design Decisions

#### 1. Topic Naming Convention

Topics follow the pattern `{topic_prefix}.stream` for the main topic and `{topic_prefix}.stream.dlq` for the dead letter queue. This mirrors the Redis stream naming convention.

#### 2. Partitioning Strategy

Events are partitioned by `aggregate_id` to ensure ordering within an aggregate while allowing parallel processing across aggregates. This is a critical design choice that maintains event sourcing consistency guarantees.

#### 3. Consumer Group Management

Consumer groups are managed via Kafka's built-in consumer group protocol, similar to Redis consumer groups but with Kafka-native partition assignment and rebalancing.

#### 4. Dead Letter Queue

Failed messages (after configurable retries) are sent to a separate DLQ topic with metadata headers including:
- Original topic
- Error message
- Retry count
- Timestamp

A `replay_dlq_message` method allows republishing DLQ messages to the main topic.

#### 5. Security

Full support for Kafka security protocols:
- PLAINTEXT (development)
- SSL (TLS encryption)
- SASL_PLAINTEXT (authentication without encryption)
- SASL_SSL (authentication with encryption)
- mTLS (mutual TLS)

#### 6. OpenTelemetry Integration

Tracing spans are created for publish and consume operations, with trace context propagated through Kafka message headers.

## Consequences

### Positive

- **Enterprise adoption**: Organizations with existing Kafka infrastructure can now use eventsource-py
- **High throughput**: Kafka's architecture enables significantly higher event throughput than Redis or RabbitMQ
- **Long-term retention**: Events can be retained for weeks/months for replay and audit purposes
- **Consistent API**: Users can switch between Redis, RabbitMQ, and Kafka with minimal code changes
- **Ecosystem integration**: Access to Kafka Connect, Kafka Streams, KSQL, and other Kafka tools

### Negative

- **Operational complexity**: Kafka requires more operational expertise than Redis
- **Topic management**: Topics should be pre-created in production (unlike Redis streams)
- **Latency**: Kafka optimizes for throughput, not latency; sub-millisecond requirements may be better served by Redis
- **Dependency size**: aiokafka adds approximately 500KB to the package

### Neutral

- **Optional dependency**: Kafka support is optional via `pip install eventsource-py[kafka]`
- **Learning curve**: Users familiar with Redis/RabbitMQ patterns will need to learn Kafka-specific concepts (partitions, offsets, rebalancing)

## Alternatives Considered

### 1. confluent-kafka-python

**Pros:**
- Official Confluent Python client
- Full feature parity with librdkafka
- Schema Registry integration

**Cons:**
- Synchronous API requiring complex async wrappers
- Larger dependency footprint
- More complex installation (requires librdkafka)

**Decision:** Rejected in favor of aiokafka for its native async support and simpler installation.

### 2. kafka-python

**Pros:**
- Pure Python implementation
- No native dependencies

**Cons:**
- Synchronous only
- Less actively maintained
- Missing some newer Kafka features

**Decision:** Rejected due to lack of async support.

### 3. Multiple Topic Pattern

**Alternative:** Create separate topics per event type instead of a single stream topic.

**Pros:**
- More granular consumer subscription
- Better alignment with Kafka conventions

**Cons:**
- Topic proliferation
- Deviates from Redis/RabbitMQ patterns
- Complicates subscriber management

**Decision:** Rejected to maintain consistency with existing implementations.

## Implementation Notes

### Test Coverage

The implementation includes:
- 51 unit tests covering all configuration options, subscription patterns, and error handling
- 31 integration tests using testcontainers-python with a real Kafka broker

### Configuration Dataclass

```python
@dataclass
class KafkaEventBusConfig:
    bootstrap_servers: str = "localhost:9092"
    topic_prefix: str = "events"
    consumer_group: str = "default"
    consumer_name: str | None = None
    acks: str = "all"
    compression_type: str | None = "gzip"
    # ... additional fields
```

### Statistics Dataclass

```python
@dataclass
class KafkaEventBusStats:
    events_published: int = 0
    events_consumed: int = 0
    events_processed_success: int = 0
    events_processed_failed: int = 0
    messages_sent_to_dlq: int = 0
    handler_errors: int = 0
    reconnections: int = 0
    rebalance_count: int = 0
```

## Related

- [ADR-0001: Async-First Design](0001-async-first-design.md)
- [ADR-0005: API Design Patterns](0005-api-design-patterns.md)
- [Kafka Event Bus Guide](../guides/kafka-event-bus.md)
- [Event Bus API Reference](../api/bus.md#kafkaeventbus)
