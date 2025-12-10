# eventsource-py Documentation

Welcome to the eventsource-py documentation. This library provides a production-ready event sourcing implementation for Python 3.11+.

## Quick Links

- [Installation Guide](installation.md) - Installation with optional dependencies
- [Getting Started](getting-started.md) - Build your first event-sourced application
- [Architecture Overview](architecture.md) - Understanding the system design

## API Reference

- [Events API](api/events.md) - DomainEvent and EventRegistry
- [Event Stores](api/stores.md) - EventStore interface and implementations
- [Aggregates](api/aggregates.md) - AggregateRoot and Repository
- [Projections](api/projections.md) - Projection system
- [Event Bus](api/bus.md) - Event distribution
- [Snapshots](api/snapshots.md) - Aggregate state caching for performance

## Development

- [Testing Strategy](development/testing.md) - Testing patterns, fixtures, and best practices

## Guides

- [Repository Pattern Guide](guides/repository-pattern.md) - Loading, saving, and managing aggregates
- [Event Bus Guide](guides/event-bus.md) - In-memory, Redis, RabbitMQ, and Kafka event distribution
- [Kafka Event Bus Guide](guides/kafka-event-bus.md) - High-throughput Kafka configuration and best practices
- [Subscription Manager Guide](guides/subscriptions.md) - Catch-up subscriptions, live streaming, and projections
- [Observability Guide](guides/observability.md) - OpenTelemetry tracing and distributed tracing
- [Snapshotting Guide](guides/snapshotting.md) - Enable aggregate state caching for performance
- [Migration Guide](guides/snapshotting-migration.md) - Add snapshotting to existing projects

## Examples

The library includes working examples in the `examples/` directory:

- **basic_usage.py** - Simple bank account example
- **aggregate_example.py** - Shopping cart with declarative aggregates
- **projection_example.py** - Building read models
- [Snapshotting Example](examples/snapshotting.md) - Aggregate state caching

Run examples with:
```bash
python -m eventsource.examples.basic_usage
python -m eventsource.examples.aggregate_example
python -m eventsource.examples.projection_example
```

## Core Concepts

### Event Sourcing

Instead of storing current state, event sourcing stores a sequence of events that led to the current state:

```
Traditional: UPDATE users SET balance = 150 WHERE id = 1
Event Sourcing: [BalanceIncreased(+50), BalanceDecreased(-20), BalanceIncreased(+120)]
```

### CQRS

Command Query Responsibility Segregation separates reads from writes:

- **Commands** (writes) go through aggregates
- **Queries** (reads) go through projections/read models

### Key Components

| Component | Purpose |
|-----------|---------|
| DomainEvent | Immutable record of a state change |
| EventStore | Persists and retrieves events |
| AggregateRoot | Consistency boundary for business logic |
| Repository | Loads/saves aggregates |
| Projection | Builds read models from events |
| EventBus | Distributes events to subscribers |

## Installation

```bash
# Basic installation (in-memory stores only)
pip install eventsource-py

# With PostgreSQL support (production)
pip install eventsource-py[postgresql]

# With SQLite support (development/testing)
pip install eventsource-py[sqlite]

# With Redis support (distributed event bus)
pip install eventsource-py[redis]

# With OpenTelemetry support (observability)
pip install eventsource-py[telemetry]

# All optional dependencies
pip install eventsource-py[all]
```

For detailed installation instructions, troubleshooting, and version compatibility, see the [Installation Guide](installation.md).

## Quick Example

```python
from uuid import uuid4
from eventsource import (
    DomainEvent,
    register_event,
    AggregateRoot,
    InMemoryEventStore,
    AggregateRepository,
)
from pydantic import BaseModel

# Define event
@register_event
class UserCreated(DomainEvent):
    event_type: str = "UserCreated"
    aggregate_type: str = "User"
    email: str

# Define state
class UserState(BaseModel):
    user_id: UUID
    email: str = ""

# Define aggregate
class UserAggregate(AggregateRoot[UserState]):
    aggregate_type = "User"

    def _get_initial_state(self) -> UserState:
        return UserState(user_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, UserCreated):
            self._state = UserState(
                user_id=self.aggregate_id,
                email=event.email,
            )

    def create(self, email: str) -> None:
        event = UserCreated(
            aggregate_id=self.aggregate_id,
            email=email,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

# Use it
async def main():
    store = InMemoryEventStore()
    repo = AggregateRepository(store, UserAggregate, "User")

    user = repo.create_new(uuid4())
    user.create("alice@example.com")
    await repo.save(user)

    loaded = await repo.load(user.aggregate_id)
    print(f"Email: {loaded.state.email}")
```

## Support

- GitHub Issues: For bugs and feature requests
- Documentation: This site for reference
- Examples: The `examples/` directory for working code
