# Multi-Tenant Setup Guide

This guide explains how to use eventsource's built-in multi-tenancy support to build SaaS applications with tenant isolation.

## Overview

Multi-tenancy allows multiple customers (tenants) to share the same event store while maintaining strict data isolation. In eventsource, multi-tenancy is implemented at the application level through the `tenant_id` field present on all domain events.

### Key Concepts

- **Tenant**: A logical boundary representing a customer, organization, or workspace
- **Tenant ID**: A UUID that uniquely identifies each tenant
- **Tenant Isolation**: Ensuring data from one tenant is never accessible to another
- **Shared Infrastructure**: All tenants use the same database and application instances

### How It Works

Every `DomainEvent` in eventsource includes an optional `tenant_id` field:

```python
# From eventsource/events/base.py
class DomainEvent(BaseModel):
    # ... other fields ...

    # Multi-tenancy (optional for library)
    tenant_id: UUID | None = Field(
        default=None,
        description="Tenant this event belongs to (optional)",
    )
```

This field is:
- **Persisted** with every event in the database
- **Indexed** for efficient tenant-scoped queries
- **Propagated** through event chains via causation tracking
- **Filterable** via event store query methods

---

## When to Use Multi-Tenancy

Multi-tenancy is ideal when you need to:

- Build SaaS applications serving multiple customers
- Share a single database across multiple organizations
- Filter and query events by tenant
- Ensure logical data isolation between customers
- Implement per-tenant billing or usage tracking

### Multi-Tenancy vs Multi-Database

| Approach | Pros | Cons |
|----------|------|------|
| **Multi-tenancy (single DB)** | Lower cost, simpler ops, efficient | Application-level isolation only |
| **Multi-database** | Stronger isolation, easier compliance | Higher cost, complex management |

This library implements application-level multi-tenancy. For stronger isolation requirements (e.g., regulatory compliance), consider separate databases per tenant.

---

## Setting Up Tenant-Aware Events

### Event Definition

All your domain events automatically inherit the `tenant_id` field from `DomainEvent`. You don't need to add it explicitly:

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
    # tenant_id is inherited from DomainEvent - no need to declare it

@register_event
class OrderShipped(DomainEvent):
    """Event emitted when an order is shipped."""
    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"
    tracking_number: str
```

### Creating Events with Tenant ID

When creating events, simply pass the `tenant_id`:

```python
from uuid import uuid4

# Tenant and entity IDs
tenant_id = uuid4()  # Usually from authentication context
order_id = uuid4()
customer_id = uuid4()

# Create event with tenant ID
event = OrderCreated(
    aggregate_id=order_id,
    tenant_id=tenant_id,  # Set the tenant
    customer_id=customer_id,
    total_amount=99.99,
    aggregate_version=1,
)

print(f"Event tenant: {event.tenant_id}")
```

---

## Tenant-Aware Aggregates

For a clean architecture, encapsulate tenant awareness within your aggregates.

### Basic Pattern

```python
from uuid import UUID
from pydantic import BaseModel
from eventsource import AggregateRoot, DomainEvent

class OrderState(BaseModel):
    """State of an Order aggregate."""
    order_id: UUID
    tenant_id: UUID | None = None
    customer_id: UUID | None = None
    total_amount: float = 0.0
    status: str = "draft"
    tracking_number: str | None = None


class OrderAggregate(AggregateRoot[OrderState]):
    """Tenant-aware Order aggregate."""
    aggregate_type = "Order"

    def __init__(self, aggregate_id: UUID, tenant_id: UUID | None = None):
        """
        Initialize order aggregate.

        Args:
            aggregate_id: Unique order ID
            tenant_id: Tenant this order belongs to
        """
        super().__init__(aggregate_id)
        self._tenant_id = tenant_id

    @property
    def tenant_id(self) -> UUID | None:
        """Get the tenant ID for this aggregate."""
        return self._tenant_id

    def _get_initial_state(self) -> OrderState:
        return OrderState(
            order_id=self.aggregate_id,
            tenant_id=self._tenant_id,
        )

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            self._state = OrderState(
                order_id=self.aggregate_id,
                tenant_id=event.tenant_id,
                customer_id=event.customer_id,
                total_amount=event.total_amount,
                status="created",
            )
            # Capture tenant_id from event during replay
            if event.tenant_id:
                self._tenant_id = event.tenant_id
        elif isinstance(event, OrderShipped):
            if self._state:
                self._state = self._state.model_copy(
                    update={
                        "status": "shipped",
                        "tracking_number": event.tracking_number,
                    }
                )

    def create(self, customer_id: UUID, total_amount: float) -> None:
        """Create the order."""
        if self.version > 0:
            raise ValueError("Order already created")

        event = OrderCreated(
            aggregate_id=self.aggregate_id,
            tenant_id=self._tenant_id,  # Include tenant in event
            customer_id=customer_id,
            total_amount=total_amount,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

    def ship(self, tracking_number: str) -> None:
        """Ship the order."""
        if not self.state or self.state.status != "created":
            raise ValueError("Order must be created before shipping")

        event = OrderShipped(
            aggregate_id=self.aggregate_id,
            tenant_id=self._tenant_id,  # Include tenant in event
            tracking_number=tracking_number,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)
```

### Using Declarative Aggregates

The same pattern works with `DeclarativeAggregate`:

```python
from eventsource import DeclarativeAggregate, handles

class OrderAggregate(DeclarativeAggregate[OrderState]):
    """Declarative tenant-aware Order aggregate."""
    aggregate_type = "Order"

    def __init__(self, aggregate_id: UUID, tenant_id: UUID | None = None):
        super().__init__(aggregate_id)
        self._tenant_id = tenant_id

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id, tenant_id=self._tenant_id)

    @handles(OrderCreated)
    def _on_order_created(self, event: OrderCreated) -> None:
        self._state = OrderState(
            order_id=self.aggregate_id,
            tenant_id=event.tenant_id,
            customer_id=event.customer_id,
            total_amount=event.total_amount,
            status="created",
        )
        if event.tenant_id:
            self._tenant_id = event.tenant_id

    @handles(OrderShipped)
    def _on_order_shipped(self, event: OrderShipped) -> None:
        if self._state:
            self._state = self._state.model_copy(
                update={"status": "shipped", "tracking_number": event.tracking_number}
            )
```

---

## Tenant-Aware Repository

Create a repository that enforces tenant context:

```python
from uuid import UUID
from eventsource import AggregateRepository, EventStore
from eventsource.exceptions import AggregateNotFoundError


class TenantAwareOrderRepository:
    """Repository that enforces tenant isolation for orders."""

    def __init__(
        self,
        event_store: EventStore,
        tenant_id: UUID,
    ):
        """
        Initialize repository with tenant context.

        Args:
            event_store: Event store for persistence
            tenant_id: Tenant context for all operations
        """
        self._event_store = event_store
        self._tenant_id = tenant_id
        self._inner_repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=lambda aid: OrderAggregate(aid, tenant_id),
            aggregate_type="Order",
        )

    async def load(self, order_id: UUID) -> OrderAggregate:
        """
        Load an order, ensuring tenant ownership.

        Raises:
            AggregateNotFoundError: If order doesn't exist
            PermissionError: If order belongs to different tenant
        """
        order = await self._inner_repo.load(order_id)

        # Verify tenant ownership
        if order.tenant_id and order.tenant_id != self._tenant_id:
            raise PermissionError(
                f"Order {order_id} belongs to tenant {order.tenant_id}, "
                f"not {self._tenant_id}"
            )

        return order

    async def save(self, order: OrderAggregate) -> None:
        """
        Save an order, ensuring tenant context.

        Raises:
            PermissionError: If order belongs to different tenant
        """
        # Verify tenant matches
        if order.tenant_id and order.tenant_id != self._tenant_id:
            raise PermissionError(
                f"Cannot save order for tenant {order.tenant_id} "
                f"in context of tenant {self._tenant_id}"
            )

        await self._inner_repo.save(order)

    def create_new(self, order_id: UUID) -> OrderAggregate:
        """Create a new order in this tenant's context."""
        return OrderAggregate(order_id, self._tenant_id)

    async def exists(self, order_id: UUID) -> bool:
        """Check if an order exists (regardless of tenant)."""
        return await self._inner_repo.exists(order_id)
```

---

## Querying Events by Tenant

### Using get_events_by_type()

The event store provides tenant-filtered queries:

```python
from uuid import UUID
from eventsource import PostgreSQLEventStore

async def get_tenant_orders(
    event_store: PostgreSQLEventStore,
    tenant_id: UUID,
) -> list[DomainEvent]:
    """Get all Order events for a specific tenant."""
    events = await event_store.get_events_by_type(
        aggregate_type="Order",
        tenant_id=tenant_id,
    )
    return events


async def get_recent_tenant_orders(
    event_store: PostgreSQLEventStore,
    tenant_id: UUID,
    since_timestamp: float,
) -> list[DomainEvent]:
    """Get recent Order events for a tenant."""
    events = await event_store.get_events_by_type(
        aggregate_type="Order",
        tenant_id=tenant_id,
        from_timestamp=since_timestamp,
    )
    return events
```

### Reading All Events with Tenant Filter

For building projections, you can filter by tenant while streaming:

```python
from eventsource import ReadOptions

async def process_tenant_events(
    event_store: PostgreSQLEventStore,
    tenant_id: UUID,
):
    """Process all events for a specific tenant."""
    async for stored_event in event_store.read_all():
        # Filter by tenant
        if stored_event.event.tenant_id == tenant_id:
            await process_event(stored_event.event)
```

---

## Tenant-Aware Projections

### Simple Tenant-Filtered Projection

```python
from uuid import UUID
from eventsource import DomainEvent
from eventsource.projections.base import Projection


class TenantOrderSummaryProjection(Projection):
    """Projection that filters events by tenant."""

    def __init__(self, tenant_id: UUID):
        self._tenant_id = tenant_id
        self._orders: dict[UUID, dict] = {}  # In-memory read model

    async def handle(self, event: DomainEvent) -> None:
        # Skip events from other tenants
        if event.tenant_id != self._tenant_id:
            return

        if isinstance(event, OrderCreated):
            self._orders[event.aggregate_id] = {
                "order_id": event.aggregate_id,
                "customer_id": event.customer_id,
                "total_amount": event.total_amount,
                "status": "created",
            }
        elif isinstance(event, OrderShipped):
            if event.aggregate_id in self._orders:
                self._orders[event.aggregate_id]["status"] = "shipped"

    async def reset(self) -> None:
        self._orders.clear()

    def get_orders(self) -> list[dict]:
        """Get all orders for this tenant."""
        return list(self._orders.values())
```

### Declarative Tenant-Aware Projection

```python
from eventsource.projections.base import DeclarativeProjection
from eventsource.projections.decorators import handles


class TenantOrderProjection(DeclarativeProjection):
    """Declarative projection with tenant filtering."""

    def __init__(
        self,
        tenant_id: UUID,
        checkpoint_repo=None,
        dlq_repo=None,
    ):
        super().__init__(checkpoint_repo=checkpoint_repo, dlq_repo=dlq_repo)
        self._tenant_id = tenant_id
        self._orders: dict[UUID, dict] = {}

    @handles(OrderCreated)
    async def _handle_order_created(self, event: OrderCreated) -> None:
        # Skip events from other tenants
        if event.tenant_id != self._tenant_id:
            return

        self._orders[event.aggregate_id] = {
            "order_id": event.aggregate_id,
            "tenant_id": event.tenant_id,
            "customer_id": event.customer_id,
            "total_amount": event.total_amount,
            "status": "created",
            "created_at": event.occurred_at,
        }

    @handles(OrderShipped)
    async def _handle_order_shipped(self, event: OrderShipped) -> None:
        # Skip events from other tenants
        if event.tenant_id != self._tenant_id:
            return

        if event.aggregate_id in self._orders:
            self._orders[event.aggregate_id].update({
                "status": "shipped",
                "tracking_number": event.tracking_number,
            })

    async def _truncate_read_models(self) -> None:
        self._orders.clear()
```

### Global Projection with Per-Tenant Views

For analytics that need to track all tenants:

```python
class GlobalOrderStatsProjection(DeclarativeProjection):
    """Projection that tracks stats across all tenants."""

    def __init__(self, checkpoint_repo=None, dlq_repo=None):
        super().__init__(checkpoint_repo=checkpoint_repo, dlq_repo=dlq_repo)
        # Stats keyed by tenant_id
        self._tenant_stats: dict[UUID, dict] = {}

    @handles(OrderCreated)
    async def _handle_order_created(self, event: OrderCreated) -> None:
        tenant_id = event.tenant_id
        if tenant_id not in self._tenant_stats:
            self._tenant_stats[tenant_id] = {
                "order_count": 0,
                "total_revenue": 0.0,
                "shipped_count": 0,
            }

        self._tenant_stats[tenant_id]["order_count"] += 1
        self._tenant_stats[tenant_id]["total_revenue"] += event.total_amount

    @handles(OrderShipped)
    async def _handle_order_shipped(self, event: OrderShipped) -> None:
        tenant_id = event.tenant_id
        if tenant_id in self._tenant_stats:
            self._tenant_stats[tenant_id]["shipped_count"] += 1

    def get_tenant_stats(self, tenant_id: UUID) -> dict | None:
        """Get stats for a specific tenant."""
        return self._tenant_stats.get(tenant_id)

    def get_all_stats(self) -> dict[UUID, dict]:
        """Get stats for all tenants (admin only)."""
        return self._tenant_stats.copy()

    async def _truncate_read_models(self) -> None:
        self._tenant_stats.clear()
```

---

## Tenant Context Management

### Request-Scoped Tenant Context

In web applications, establish tenant context at the request level:

```python
from contextvars import ContextVar
from uuid import UUID

# Context variable for current tenant
_current_tenant: ContextVar[UUID | None] = ContextVar("current_tenant", default=None)


def get_current_tenant() -> UUID | None:
    """Get the current tenant ID from context."""
    return _current_tenant.get()


def set_current_tenant(tenant_id: UUID | None) -> None:
    """Set the current tenant ID in context."""
    _current_tenant.set(tenant_id)


# Middleware example (FastAPI)
from fastapi import Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware


class TenantMiddleware(BaseHTTPMiddleware):
    """Middleware to extract and set tenant context."""

    async def dispatch(self, request: Request, call_next):
        # Extract tenant from header, token, or subdomain
        tenant_header = request.headers.get("X-Tenant-ID")

        if tenant_header:
            try:
                tenant_id = UUID(tenant_header)
                set_current_tenant(tenant_id)
            except ValueError:
                raise HTTPException(400, "Invalid tenant ID format")

        try:
            response = await call_next(request)
            return response
        finally:
            # Clear tenant context after request
            set_current_tenant(None)
```

### Dependency Injection Pattern

```python
from fastapi import Depends, HTTPException
from uuid import UUID


async def get_tenant_id(request: Request) -> UUID:
    """FastAPI dependency to get and validate tenant ID."""
    tenant_id = get_current_tenant()
    if not tenant_id:
        raise HTTPException(401, "Tenant context required")
    return tenant_id


async def get_order_repository(
    tenant_id: UUID = Depends(get_tenant_id),
    event_store: EventStore = Depends(get_event_store),
) -> TenantAwareOrderRepository:
    """Get a tenant-scoped order repository."""
    return TenantAwareOrderRepository(
        event_store=event_store,
        tenant_id=tenant_id,
    )


# Usage in endpoint
@app.post("/orders")
async def create_order(
    request: CreateOrderRequest,
    repo: TenantAwareOrderRepository = Depends(get_order_repository),
):
    order = repo.create_new(uuid4())
    order.create(
        customer_id=request.customer_id,
        total_amount=request.total_amount,
    )
    await repo.save(order)
    return {"order_id": str(order.aggregate_id)}
```

---

## Database Schema for Multi-Tenancy

The eventsource database schema includes tenant support:

```sql
CREATE TABLE events (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    event_type VARCHAR(255) NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL,
    aggregate_id UUID NOT NULL,
    tenant_id UUID,  -- Tenant isolation field
    actor_id VARCHAR(255),
    version INTEGER NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_events_aggregate_version
        UNIQUE (aggregate_id, aggregate_type, version)
);

-- Index for efficient tenant-scoped queries
CREATE INDEX idx_events_tenant_id ON events(tenant_id)
    WHERE tenant_id IS NOT NULL;

-- Composite index for projection queries by tenant
CREATE INDEX idx_events_type_tenant_timestamp
    ON events(aggregate_type, tenant_id, timestamp);
```

The partial index on `tenant_id` ensures efficient queries for multi-tenant deployments without bloating indexes for single-tenant usage.

---

## Security Considerations

### Application-Level Enforcement

Tenant isolation in eventsource is enforced at the application level. This means:

1. **Always validate tenant context** before any operation
2. **Never trust client-provided tenant IDs** without authentication
3. **Use middleware** to establish tenant context from authenticated tokens
4. **Validate aggregate ownership** when loading from event store

### Best Practices

```python
# DO: Validate tenant ownership after loading
async def get_order(order_id: UUID, tenant_id: UUID) -> OrderAggregate:
    order = await repo.load(order_id)
    if order.tenant_id != tenant_id:
        raise PermissionError("Access denied")
    return order

# DON'T: Trust tenant_id from request body
async def create_order_unsafe(request: dict) -> OrderAggregate:
    # WRONG: Client could set any tenant_id
    order = OrderAggregate(uuid4(), request.get("tenant_id"))
    # ...

# DO: Get tenant from authenticated context
async def create_order_safe(
    request: CreateOrderRequest,
    tenant_id: UUID = Depends(get_tenant_id),  # From auth token
) -> OrderAggregate:
    order = OrderAggregate(uuid4(), tenant_id)
    # ...
```

### Audit Trail

The event store provides a complete audit trail per tenant:

```python
async def get_tenant_audit_log(
    event_store: EventStore,
    tenant_id: UUID,
) -> list[dict]:
    """Get complete audit log for a tenant."""
    events = await event_store.get_events_by_type(
        aggregate_type="Order",  # Or iterate all types
        tenant_id=tenant_id,
    )
    return [
        {
            "event_id": str(e.event_id),
            "event_type": e.event_type,
            "occurred_at": e.occurred_at.isoformat(),
            "actor_id": e.actor_id,
            "aggregate_id": str(e.aggregate_id),
        }
        for e in events
    ]
```

---

## Complete Example

Here's a full working example demonstrating multi-tenant event sourcing:

```python
import asyncio
from uuid import uuid4, UUID

from eventsource import (
    DomainEvent,
    register_event,
    AggregateRoot,
    InMemoryEventStore,
    AggregateRepository,
)
from pydantic import BaseModel


# 1. Define Events
@register_event
class TaskCreated(DomainEvent):
    event_type: str = "TaskCreated"
    aggregate_type: str = "Task"
    title: str
    description: str


@register_event
class TaskCompleted(DomainEvent):
    event_type: str = "TaskCompleted"
    aggregate_type: str = "Task"


# 2. Define State
class TaskState(BaseModel):
    task_id: UUID
    tenant_id: UUID | None = None
    title: str = ""
    description: str = ""
    completed: bool = False


# 3. Define Aggregate
class TaskAggregate(AggregateRoot[TaskState]):
    aggregate_type = "Task"

    def __init__(self, aggregate_id: UUID, tenant_id: UUID | None = None):
        super().__init__(aggregate_id)
        self._tenant_id = tenant_id

    @property
    def tenant_id(self) -> UUID | None:
        return self._tenant_id

    def _get_initial_state(self) -> TaskState:
        return TaskState(task_id=self.aggregate_id, tenant_id=self._tenant_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, TaskCreated):
            self._state = TaskState(
                task_id=self.aggregate_id,
                tenant_id=event.tenant_id,
                title=event.title,
                description=event.description,
                completed=False,
            )
            if event.tenant_id:
                self._tenant_id = event.tenant_id
        elif isinstance(event, TaskCompleted):
            if self._state:
                self._state = self._state.model_copy(update={"completed": True})

    def create(self, title: str, description: str) -> None:
        if self.version > 0:
            raise ValueError("Task already exists")
        event = TaskCreated(
            aggregate_id=self.aggregate_id,
            tenant_id=self._tenant_id,
            title=title,
            description=description,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

    def complete(self) -> None:
        if not self.state or self.state.completed:
            raise ValueError("Task cannot be completed")
        event = TaskCompleted(
            aggregate_id=self.aggregate_id,
            tenant_id=self._tenant_id,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)


# 4. Main Example
async def main():
    # Create event store
    event_store = InMemoryEventStore()

    # Simulate two tenants
    tenant_a = uuid4()
    tenant_b = uuid4()

    print(f"Tenant A: {tenant_a}")
    print(f"Tenant B: {tenant_b}")
    print()

    # Create repositories for each tenant
    def make_repo(tenant_id: UUID) -> AggregateRepository:
        return AggregateRepository(
            event_store=event_store,
            aggregate_factory=lambda aid: TaskAggregate(aid, tenant_id),
            aggregate_type="Task",
        )

    repo_a = make_repo(tenant_a)
    repo_b = make_repo(tenant_b)

    # Tenant A creates a task
    task_a_id = uuid4()
    task_a = TaskAggregate(task_a_id, tenant_a)
    task_a.create(title="Fix bug", description="Fix the login bug")
    await repo_a.save(task_a)
    print(f"Tenant A created task: {task_a_id}")

    # Tenant B creates a task
    task_b_id = uuid4()
    task_b = TaskAggregate(task_b_id, tenant_b)
    task_b.create(title="Add feature", description="Add dark mode")
    await repo_b.save(task_b)
    print(f"Tenant B created task: {task_b_id}")

    # Tenant A completes their task
    loaded_task_a = await repo_a.load(task_a_id)
    loaded_task_a.complete()
    await repo_a.save(loaded_task_a)
    print(f"Tenant A completed task: {task_a_id}")
    print()

    # Query events by tenant
    print("Events for Tenant A:")
    tenant_a_events = await event_store.get_events_by_type("Task", tenant_id=tenant_a)
    for event in tenant_a_events:
        print(f"  - {event.event_type}: {event.aggregate_id}")

    print("\nEvents for Tenant B:")
    tenant_b_events = await event_store.get_events_by_type("Task", tenant_id=tenant_b)
    for event in tenant_b_events:
        print(f"  - {event.event_type}: {event.aggregate_id}")

    # Total events in store
    print(f"\nTotal events in store: {event_store.get_event_count()}")

    # Verify tenant isolation
    print("\nVerifying tenant isolation:")
    print(f"  Tenant A tasks: {len(tenant_a_events)} events")
    print(f"  Tenant B tasks: {len(tenant_b_events)} events")


if __name__ == "__main__":
    asyncio.run(main())
```

Expected output:

```
Tenant A: <uuid>
Tenant B: <uuid>

Tenant A created task: <uuid>
Tenant B created task: <uuid>
Tenant A completed task: <uuid>

Events for Tenant A:
  - TaskCreated: <uuid>
  - TaskCompleted: <uuid>

Events for Tenant B:
  - TaskCreated: <uuid>

Total events in store: 3

Verifying tenant isolation:
  Tenant A tasks: 2 events
  Tenant B tasks: 1 events
```

---

## Troubleshooting

### Common Issues

**Events without tenant_id**

If you have events without `tenant_id`, they will be included in queries with `tenant_id=None`:

```python
# This returns events where tenant_id IS NULL
events = await store.get_events_by_type("Order", tenant_id=None)
```

**Cross-tenant data access**

If you're seeing cross-tenant data:
1. Check that tenant_id is set on all events
2. Verify repository is using tenant-aware factory
3. Ensure middleware properly sets tenant context

**Performance with many tenants**

For deployments with many tenants:
1. Ensure `idx_events_tenant_id` index exists
2. Use timestamp filters to enable partition pruning
3. Consider tenant-specific read models instead of filtering at query time

---

## See Also

- [Event Stores API](../api/stores.md) - `get_events_by_type` with tenant filtering
- [Production Guide](../guides/production.md) - Database schema and setup details
- [Architecture Overview](../architecture.md) - Multi-tenancy section
- [Projections](../api/projections.md) - Building read models
