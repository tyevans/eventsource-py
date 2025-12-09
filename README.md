# eventsource-py

[![Python Version](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![PyPI version](https://img.shields.io/pypi/v/eventsource-py.svg)](https://pypi.org/project/eventsource-py/)
[![CI](https://github.com/tyevans/eventsource-py/actions/workflows/ci.yml/badge.svg)](https://github.com/tyevans/eventsource-py/actions)
[![Documentation](https://img.shields.io/badge/docs-GitHub%20Pages-blue.svg)](https://tyevans.github.io/eventsource-py)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

A production-ready event sourcing library for Python 3.11+.

## Features

- **Event Store** - PostgreSQL, SQLite, and In-Memory backends with optimistic locking
- **Domain Events** - Immutable event classes with Pydantic validation and versioning
- **Event Registry** - Thread-safe event type registration for serialization/deserialization
- **Aggregate Pattern** - Base classes for event-sourced aggregates with state reconstruction
- **Repository Pattern** - Clean abstractions for loading and saving aggregates
- **Aggregate Snapshotting** - Optimize load performance for long-lived aggregates
- **Projection System** - Checkpoint tracking, retry logic, and dead letter queue support
- **Event Bus** - In-Memory, Redis Streams, RabbitMQ, and Kafka backends for event distribution
- **Transactional Outbox** - Reliable event publishing pattern
- **Multi-tenancy** - Built-in tenant isolation support
- **Observability** - Optional OpenTelemetry integration

## Installation

### Basic Installation

```bash
pip install eventsource-py
```

### With Optional Dependencies

eventsource uses optional dependencies to keep the core package lightweight. Install only what you need:

```bash
# PostgreSQL support - production event store with asyncpg
pip install eventsource-py[postgresql]

# SQLite support - lightweight deployments
pip install eventsource-py[sqlite]

# Redis support - distributed event bus with Redis Streams
pip install eventsource-py[redis]

# RabbitMQ support - distributed event bus with RabbitMQ
pip install eventsource-py[rabbitmq]

# Kafka support - distributed event bus with Apache Kafka
pip install eventsource-py[kafka]

# Telemetry support - OpenTelemetry tracing integration
pip install eventsource-py[telemetry]

# All optional dependencies
pip install eventsource-py[all]

# Multiple extras
pip install eventsource-py[postgresql,redis,telemetry]
```

| Extra | Enables | Dependencies |
|-------|---------|--------------|
| `postgresql` | `PostgreSQLEventStore`, checkpoint/outbox/DLQ repositories | asyncpg |
| `sqlite` | `SQLiteEventStore` for lightweight deployments | aiosqlite |
| `redis` | `RedisEventBus` with consumer groups and DLQ | redis |
| `rabbitmq` | `RabbitMQEventBus` with exchange routing and DLQ | aio-pika |
| `kafka` | `KafkaEventBus` with consumer groups, DLQ, and tracing | aiokafka |
| `telemetry` | Distributed tracing for event operations | opentelemetry-api, opentelemetry-sdk |
| `all` | All of the above | All of the above |
| `dev` | Development tools | pytest, mypy, ruff, pre-commit |

For detailed installation instructions, troubleshooting, and version compatibility, see the [Installation Guide](docs/installation.md).

## Requirements

- Python 3.11+
- pydantic >= 2.0
- sqlalchemy >= 2.0

## Quick Start

### 1. Define Your Events

```python
from uuid import UUID
from eventsource import DomainEvent, register_event

@register_event
class OrderCreated(DomainEvent):
    """Event emitted when an order is created."""
    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    customer_id: UUID
    total_amount: float

@register_event
class OrderShipped(DomainEvent):
    """Event emitted when an order is shipped."""
    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"
    tracking_number: str
```

### 2. Define Your Aggregate State

```python
from pydantic import BaseModel

class OrderState(BaseModel):
    """State of an Order aggregate."""
    order_id: UUID
    customer_id: UUID | None = None
    total_amount: float = 0.0
    status: str = "draft"
    tracking_number: str | None = None
```

### 3. Create Your Aggregate

```python
from eventsource import AggregateRoot

class OrderAggregate(AggregateRoot[OrderState]):
    """Event-sourced Order aggregate."""
    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            self._state = OrderState(
                order_id=self.aggregate_id,
                customer_id=event.customer_id,
                total_amount=event.total_amount,
                status="created",
            )
        elif isinstance(event, OrderShipped):
            if self._state:
                self._state = self._state.model_copy(
                    update={
                        "status": "shipped",
                        "tracking_number": event.tracking_number,
                    }
                )

    def create(self, customer_id: UUID, total_amount: float) -> None:
        """Command: Create the order."""
        if self.version > 0:
            raise ValueError("Order already created")

        event = OrderCreated(
            aggregate_id=self.aggregate_id,
            customer_id=customer_id,
            total_amount=total_amount,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

    def ship(self, tracking_number: str) -> None:
        """Command: Ship the order."""
        if not self.state or self.state.status != "created":
            raise ValueError("Order must be created before shipping")

        event = OrderShipped(
            aggregate_id=self.aggregate_id,
            tracking_number=tracking_number,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)
```

### 4. Use the Repository

```python
import asyncio
from uuid import uuid4
from eventsource import InMemoryEventStore, AggregateRepository

async def main():
    # Set up event store and repository
    event_store = InMemoryEventStore()
    repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=OrderAggregate,
        aggregate_type="Order",
    )

    # Create and save a new order
    order_id = uuid4()
    order = repo.create_new(order_id)
    order.create(customer_id=uuid4(), total_amount=99.99)
    await repo.save(order)

    # Load the order and ship it
    loaded_order = await repo.load(order_id)
    loaded_order.ship(tracking_number="TRACK-123")
    await repo.save(loaded_order)

    # Verify the state
    final_order = await repo.load(order_id)
    print(f"Order status: {final_order.state.status}")
    print(f"Tracking: {final_order.state.tracking_number}")

asyncio.run(main())
```

## Architecture

```
+-------------------+     +-------------------+     +-------------------+
|                   |     |                   |     |                   |
|    Commands       |---->|    Aggregates     |---->|   Event Store     |
|                   |     |                   |     |                   |
+-------------------+     +-------------------+     +--------+----------+
                                                             |
                                                             v
+-------------------+     +-------------------+     +-------------------+
|                   |     |                   |     |                   |
|   Read Models     |<----|   Projections     |<----|   Event Bus       |
|                   |     |                   |     |                   |
+-------------------+     +-------------------+     +-------------------+
```

### Core Concepts

- **Events** - Immutable facts that capture state changes. Events are never deleted or modified.
- **Event Store** - Persists events with ordering and optimistic locking guarantees.
- **Aggregates** - Consistency boundaries that reconstruct state from event streams.
- **Repository** - Abstracts loading/saving aggregates from/to the event store.
- **Projections** - Build read-optimized views from event streams.
- **Event Bus** - Distributes events to subscribers for async processing.

## Documentation

📚 **[Full Documentation](https://tyevans.github.io/eventsource-py)**

- [Getting Started Guide](https://tyevans.github.io/eventsource-py/getting-started/) - Installation and first steps
- [Architecture Overview](https://tyevans.github.io/eventsource-py/architecture/) - System design and concepts
- [API Reference](https://tyevans.github.io/eventsource-py/api/) - Complete API documentation
- [FAQ](https://tyevans.github.io/eventsource-py/faq/) - Frequently asked questions

## Development

```bash
# Install in development mode
pip install -e ".[dev]"

# Run tests
pytest

# Run tests with coverage
pytest --cov=eventsource --cov-report=html

# Run type checking
mypy src/eventsource

# Run linting
ruff check src/eventsource

# Format code
ruff format src/eventsource
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
