# Installation Guide

This guide covers installing eventsource-py with the right dependencies for your use case.

## Quick Start

```bash
# Basic installation (in-memory stores only)
pip install eventsource

# Most common: with PostgreSQL support
pip install eventsource[postgresql]
```

## Requirements

- Python 3.11 or higher
- pip (Python package manager)

## Core Dependencies

The base installation includes these required packages:

| Package | Version | Purpose |
|---------|---------|---------|
| pydantic | >= 2.0, < 3.0 | Event and state validation |
| redis | >= 5.0 | Core Redis support |
| sqlalchemy | >= 2.0, < 3.0 | Database abstraction layer |

## Optional Dependencies

eventsource-py uses optional dependencies to keep the core package lightweight. Install only what you need for your use case.

### PostgreSQL (`[postgresql]`)

**Enables:**
- `PostgreSQLEventStore` - Production-ready event store with async support
- `PostgreSQLCheckpointRepository` - Durable checkpoint storage for projections
- `PostgreSQLOutboxRepository` - Transactional outbox pattern implementation
- `PostgreSQLDLQRepository` - Dead letter queue for failed events

**Install:**
```bash
pip install eventsource[postgresql]
```

**Dependencies added:**
- `asyncpg >= 0.27.0, < 1.0` - High-performance async PostgreSQL driver

**When to use:**
- Production deployments requiring durable event storage
- Multi-process or multi-server setups
- When you need ACID guarantees for event persistence

**Example:**
```python
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from eventsource.stores.postgresql import PostgreSQLEventStore

# Create async engine with asyncpg driver
engine = create_async_engine(
    "postgresql+asyncpg://user:password@localhost:5432/mydb",
    echo=False,
)
session_factory = async_sessionmaker(engine, expire_on_commit=False)

# Create event store
event_store = PostgreSQLEventStore(session_factory)
```

### Redis (`[redis]`)

**Enables:**
- `RedisEventBus` - Distributed event publishing with Redis Streams
- Consumer groups for load-balanced event processing
- Dead letter queue support for failed messages
- Pending message recovery

**Install:**
```bash
pip install eventsource[redis]
```

**Dependencies added:**
- `redis >= 5.0, < 6.0` - Redis client library with async support

**When to use:**
- Distributed systems with multiple services
- When projections run in separate processes
- Real-time event streaming across services
- Horizontal scaling with consumer groups

**Example:**
```python
from eventsource.bus.redis import RedisEventBus, RedisEventBusConfig

config = RedisEventBusConfig(
    redis_url="redis://localhost:6379",
    stream_prefix="myapp",
    consumer_group="projections",
)
event_bus = RedisEventBus(config=config)
await event_bus.connect()
```

### RabbitMQ (`[rabbitmq]`)

**Enables:**
- `RabbitMQEventBus` - Distributed event messaging with RabbitMQ AMQP
- Multiple exchange types (topic, direct, fanout, headers)
- Consumer groups via queue bindings
- Dead letter queue for failed messages
- Automatic reconnection

**Install:**
```bash
pip install eventsource[rabbitmq]
```

**Dependencies added:**
- `aio-pika >= 9.0.0` - Async RabbitMQ client

**When to use:**
- Distributed systems requiring flexible routing patterns
- When you need topic-based message routing
- Applications requiring message acknowledgments

**Example:**
```python
from eventsource.bus.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig

config = RabbitMQEventBusConfig(
    rabbitmq_url="amqp://guest:guest@localhost:5672/",
    exchange_name="events",
    exchange_type="topic",
    consumer_group="projections",
)
bus = RabbitMQEventBus(config=config)
await bus.connect()
```

### Kafka (`[kafka]`)

**Enables:**
- `KafkaEventBus` - Distributed event streaming with Apache Kafka
- Consumer groups for horizontal scaling
- Partition-based ordering by aggregate_id
- Dead letter queue with replay capability
- Optional OpenTelemetry tracing
- TLS/SSL and SASL authentication support

**Install:**
```bash
pip install eventsource[kafka]
```

**Dependencies added:**
- `aiokafka >= 0.9.0, < 1.0.0` - Async Kafka client

**When to use:**
- High-throughput event streaming scenarios
- Enterprise environments with existing Kafka infrastructure
- When you need long-term event retention
- Multi-datacenter deployments
- When horizontal scaling of consumers is critical

**Example:**
```python
from eventsource.bus.kafka import KafkaEventBus, KafkaEventBusConfig

config = KafkaEventBusConfig(
    bootstrap_servers="localhost:9092",
    topic_prefix="myapp.events",
    consumer_group="projections",
)
bus = KafkaEventBus(config=config)
await bus.connect()
await bus.publish([my_event])
```

### Telemetry (`[telemetry]`)

**Enables:**
- OpenTelemetry tracing for event store operations
- Distributed tracing correlation across services
- Performance monitoring and observability

**Install:**
```bash
pip install eventsource[telemetry]
```

**Dependencies added:**
- `opentelemetry-api >= 1.0, < 2.0` - OpenTelemetry API
- `opentelemetry-sdk >= 1.0, < 2.0` - OpenTelemetry SDK

**When to use:**
- Production observability requirements
- Distributed tracing with Jaeger, Zipkin, or similar backends
- Performance monitoring and debugging

**Example:**
```python
from eventsource.stores.postgresql import PostgreSQLEventStore

# Tracing is automatically enabled when opentelemetry is installed
event_store = PostgreSQLEventStore(
    session_factory,
    enable_tracing=True,  # Default when telemetry is available
)
```

## Combining Extras

Install multiple extras by separating them with commas:

```bash
# PostgreSQL + Redis
pip install eventsource[postgresql,redis]

# All production dependencies
pip install eventsource[postgresql,redis,telemetry]

# Use the 'all' shortcut for all optional dependencies
pip install eventsource[all]
```

## Development Installation

For contributing to eventsource-py:

```bash
# Clone repository
git clone https://github.com/tyevans/eventsource-py.git
cd eventsource-py

# Install with all development dependencies
pip install -e ".[dev,postgresql,redis,telemetry]"

# Or install everything including docs
pip install -e ".[dev,docs,postgresql,redis,telemetry]"

# Run tests
pytest

# Run type checking
mypy src/eventsource

# Run linting
ruff check src/eventsource
```

### Development Extras

| Extra | Purpose |
|-------|---------|
| `dev` | Testing tools (pytest, mypy, ruff, pre-commit) |
| `docs` | Documentation tools (mkdocs, mkdocs-material) |
| `benchmark` | Performance benchmarking (pytest-benchmark) |

## Verifying Installation

Check which optional dependencies are installed:

```python
# Check PostgreSQL support
try:
    from eventsource.stores.postgresql import PostgreSQLEventStore
    print("PostgreSQL support: INSTALLED")
except ImportError as e:
    print(f"PostgreSQL support: NOT INSTALLED")
    print(f"  Install with: pip install eventsource[postgresql]")

# Check Redis support
try:
    from eventsource.bus.redis import RedisEventBus
    print("Redis support: INSTALLED")
except ImportError as e:
    print(f"Redis support: NOT INSTALLED")
    print(f"  Install with: pip install eventsource[redis]")

# Check RabbitMQ support
try:
    from eventsource.bus.rabbitmq import RabbitMQEventBus
    print("RabbitMQ support: INSTALLED")
except ImportError as e:
    print(f"RabbitMQ support: NOT INSTALLED")
    print(f"  Install with: pip install eventsource[rabbitmq]")

# Check Kafka support
try:
    from eventsource.bus.kafka import KafkaEventBus
    print("Kafka support: INSTALLED")
except ImportError as e:
    print(f"Kafka support: NOT INSTALLED")
    print(f"  Install with: pip install eventsource[kafka]")

# Check telemetry support
try:
    from opentelemetry import trace
    print("Telemetry support: INSTALLED")
except ImportError:
    print("Telemetry support: NOT INSTALLED")
    print("  Install with: pip install eventsource[telemetry]")
```

## Common Issues and Solutions

### ImportError: asyncpg not found

**Error:**
```
ImportError: PostgreSQLEventStore requires the 'asyncpg' package.
Install it with: pip install eventsource[postgresql]
```

**Solution:**
```bash
pip install eventsource[postgresql]
```

**Explanation:** The PostgreSQL event store requires the `asyncpg` package for async database operations. This is an optional dependency to keep the core package lightweight.

### ImportError: Redis package is not installed

**Error:**
```
ImportError: Redis package is not installed.
Install it with: pip install eventsource[redis]
```

**Solution:**
```bash
pip install eventsource[redis]
```

**Explanation:** The Redis event bus requires the `redis` package. While `redis` is included in core dependencies for basic support, the full Redis event bus functionality requires explicit installation of the redis extra.

### SSL/TLS Connection Issues with PostgreSQL

**Error:**
```
asyncpg.exceptions.InvalidPasswordError: password authentication failed
```

**Solution:**
Ensure your connection string includes the correct SSL mode:
```python
engine = create_async_engine(
    "postgresql+asyncpg://user:password@host:5432/db?ssl=require"
)
```

### Redis Connection Timeout

**Error:**
```
redis.exceptions.ConnectionError: Error connecting to redis://localhost:6379
```

**Solution:**
1. Verify Redis is running: `redis-cli ping`
2. Check connection parameters in `RedisEventBusConfig`
3. Adjust timeout settings if needed:
```python
config = RedisEventBusConfig(
    redis_url="redis://localhost:6379",
    socket_timeout=10.0,
    socket_connect_timeout=10.0,
)
```

### ImportError: aiokafka not found

**Error:**
```
ImportError: aiokafka package is not installed.
Install it with: pip install eventsource[kafka]
```

**Solution:**
```bash
pip install eventsource[kafka]
```

**Explanation:** The Kafka event bus requires the `aiokafka` package for async Kafka operations. This is an optional dependency to keep the core package lightweight.

### Kafka Connection Refused

**Error:**
```
KafkaConnectionError: Unable to bootstrap from kafka:9092
```

**Solution:**
1. Verify Kafka is running and accessible
2. Check bootstrap servers configuration:
```python
config = KafkaEventBusConfig(
    bootstrap_servers="localhost:9092",  # Single broker
    # Or multiple brokers:
    # bootstrap_servers="broker1:9092,broker2:9092,broker3:9092",
)
```
3. For Docker environments, ensure proper network configuration
4. Check firewall rules allow port 9092

### Kafka SASL Authentication Failed

**Error:**
```
KafkaError: SASL Authentication failed
```

**Solution:**
```python
config = KafkaEventBusConfig(
    bootstrap_servers="kafka:9093",
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-512",
    sasl_username="your-username",
    sasl_password="your-password",
    ssl_cafile="/path/to/ca.crt",
)
```

## Version Compatibility

| Extra | Min Python | Dependency Version |
|-------|------------|-------------------|
| postgresql | 3.11+ | asyncpg >= 0.27.0 |
| sqlite | 3.11+ | aiosqlite >= 0.19.0 |
| redis | 3.11+ | redis >= 5.0 |
| rabbitmq | 3.11+ | aio-pika >= 9.0.0 |
| kafka | 3.11+ | aiokafka >= 0.9.0 |
| telemetry | 3.11+ | opentelemetry-* >= 1.0 |

## Feature Matrix

| Feature | Core | +postgresql | +sqlite | +redis | +rabbitmq | +kafka | +telemetry |
|---------|------|-------------|---------|--------|-----------|--------|------------|
| In-memory event store | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| PostgreSQL event store | - | Yes | - | - | - | - | Yes |
| SQLite event store | - | - | Yes | - | - | - | Yes |
| In-memory event bus | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| Redis event bus | - | - | - | Yes | - | - | Yes |
| RabbitMQ event bus | - | - | - | - | Yes | - | Yes |
| Kafka event bus | - | - | - | - | - | Yes | Yes |
| Consumer groups | - | - | - | Yes | Yes | Yes | - |
| Dead letter queue | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| TLS/SASL security | - | - | - | - | Yes | Yes | - |
| Distributed tracing | - | - | - | - | - | - | Yes |

## Next Steps

- [Getting Started Guide](getting-started.md) - Build your first event-sourced application
- [Architecture Overview](architecture.md) - Understand the system design
- [Event Bus Guide](guides/event-bus.md) - Event distribution with Redis, RabbitMQ, and Kafka
- [Production Deployment](guides/production.md) - Production readiness checklist
