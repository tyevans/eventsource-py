# eventsource Documentation

Welcome to the eventsource documentation. This library provides a production-ready event sourcing implementation for Python 3.11+.

## Quick Links

- [Getting Started](getting-started.md) - Installation and first steps
- [Architecture Overview](architecture.md) - Understanding the system design
- [Migration Guide](migration-guide.md) - For Honeybadger users

## API Reference

- [Events API](api/events.md) - DomainEvent and EventRegistry
- [Event Stores](api/stores.md) - EventStore interface and implementations
- [Aggregates](api/aggregates.md) - AggregateRoot and Repository
- [Projections](api/projections.md) - Projection system
- [Event Bus](api/bus.md) - Event distribution

## Examples

The library includes working examples in the `examples/` directory:

- **basic_usage.py** - Simple bank account example
- **aggregate_example.py** - Shopping cart with declarative aggregates
- **projection_example.py** - Building read models

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
# Basic
pip install eventsource

# With PostgreSQL
pip install eventsource[postgresql]

# With Redis
pip install eventsource[redis]

# With OpenTelemetry
pip install eventsource[telemetry]

# All extras
pip install eventsource[all]
```

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
