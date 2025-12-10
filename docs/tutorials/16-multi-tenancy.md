# Tutorial 16: Multi-Tenancy Implementation

**Estimated Time:** 60-75 minutes
**Difficulty:** Advanced
**Progress:** Tutorial 16 of 21 | Phase 4: Advanced Patterns

---

## Phase 4 Introduction

Welcome to Phase 4: Advanced Patterns! Having completed Phase 3 and mastered production deployment, you're now ready to tackle sophisticated techniques for building enterprise-grade event-sourced applications.

Phase 4 covers:

- **Multi-tenancy** (this tutorial) - Building SaaS applications with tenant isolation
- **Distributed event buses** - Redis, Kafka, and RabbitMQ integration
- **Observability** - OpenTelemetry and advanced monitoring
- **Advanced aggregate patterns** - Process managers and sagas

This tutorial focuses on multi-tenancy, an essential pattern for SaaS applications where multiple customers share infrastructure while maintaining strict data isolation.

---

## Prerequisites

Before starting this tutorial, ensure you have:

- Completed [Tutorial 15: Production Deployment](15-production.md)
- Understanding of the repository pattern from [Tutorial 5: Repositories](05-repositories.md)
- Understanding of projections from [Tutorial 6: Projections](06-projections.md)
- Python 3.11+ installed
- eventsource-py installed:

```bash
pip install eventsource-py
```

---

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Explain multi-tenancy concepts and why they matter for SaaS applications
2. Create tenant-aware events using the built-in `tenant_id` field
3. Implement tenant-aware aggregates that enforce tenant boundaries
4. Query events filtered by tenant using event store methods
5. Build tenant-scoped projections that maintain data isolation
6. Propagate tenant context through your application using middleware patterns

---

## What is Multi-Tenancy?

Multi-tenancy is an architecture where a single instance of software serves multiple customers (tenants). Each tenant's data is isolated from other tenants, but they share the same application infrastructure.

### Why Multi-Tenancy Matters

For SaaS applications, multi-tenancy provides:

| Benefit | Description |
|---------|-------------|
| **Cost Efficiency** | Single infrastructure serves many customers |
| **Simplified Operations** | One codebase, one database, one deployment |
| **Scalability** | Add tenants without deploying new instances |
| **Easier Updates** | Deploy once, all tenants get updates |

### Multi-Tenancy in Event Sourcing

In traditional CRUD applications, multi-tenancy often uses:
- Separate databases per tenant
- Schema-based isolation within a database
- Row-level filtering with tenant columns

In event sourcing, we use **event-level tenant tagging**. Every event includes a `tenant_id`, enabling:
- Tenant-filtered event queries
- Tenant-scoped aggregate reconstruction
- Tenant-aware projections
- Complete audit trails per tenant

### Single Database vs Multi-Database

| Approach | Pros | Cons |
|----------|------|------|
| **Single database (shared)** | Lower cost, simpler ops, efficient resource usage | Application-level isolation only |
| **Database per tenant** | Stronger isolation, easier compliance, simple tenant deletion | Higher cost, complex management, harder cross-tenant queries |

The eventsource library implements **shared database with application-level isolation**. For regulatory requirements demanding physical separation (HIPAA, GDPR in some contexts), consider database-per-tenant instead.

---

## How eventsource Supports Multi-Tenancy

### The tenant_id Field

Every `DomainEvent` in eventsource includes an optional `tenant_id` field:

```python
from eventsource.events.base import DomainEvent
from pydantic import Field
from uuid import UUID

class DomainEvent(BaseModel):
    # ... other fields ...

    # Multi-tenancy support (built-in)
    tenant_id: UUID | None = Field(
        default=None,
        description="Tenant this event belongs to (optional)",
    )
```

This field is:
- **Inherited** by all your domain events automatically
- **Persisted** with every event in the database
- **Indexed** for efficient tenant-scoped queries
- **Filterable** via event store query methods

### Database Schema Support

When using PostgreSQL, the events table includes tenant support:

```sql
CREATE TABLE events (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    tenant_id UUID,  -- Tenant isolation field
    -- ... other columns ...
);

-- Index for efficient tenant-scoped queries
CREATE INDEX idx_events_tenant_id ON events(tenant_id)
    WHERE tenant_id IS NOT NULL;
```

---

## Creating Tenant-Aware Events

Since all domain events inherit from `DomainEvent`, they automatically have the `tenant_id` field. You just need to set it when creating events.

### Event Definition

Your events don't need any special changes for multi-tenancy:

```python
from uuid import UUID
from eventsource import DomainEvent, register_event

@register_event
class OrderCreated(DomainEvent):
    """Event emitted when an order is created."""
    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    customer_name: str
    total_amount: float
    # tenant_id is inherited from DomainEvent - no declaration needed!


@register_event
class OrderShipped(DomainEvent):
    """Event emitted when an order is shipped."""
    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"
    tracking_number: str
    # tenant_id is also inherited here
```

### Creating Events with Tenant ID

When creating events, include the tenant_id:

```python
from uuid import uuid4

# IDs (tenant_id typically comes from authentication context)
tenant_id = uuid4()
order_id = uuid4()

# Create event with tenant context
event = OrderCreated(
    aggregate_id=order_id,
    tenant_id=tenant_id,  # Set the tenant
    customer_name="Alice",
    total_amount=99.99,
    aggregate_version=1,
)

print(f"Event tenant: {event.tenant_id}")
# Output: Event tenant: <uuid>
```

---

## Tenant-Aware Aggregates

For clean architecture, encapsulate tenant awareness within your aggregates. The aggregate should:
1. Accept `tenant_id` at construction
2. Include `tenant_id` in all events it creates
3. Capture `tenant_id` from events during replay

### Basic Pattern

```python
from uuid import UUID
from pydantic import BaseModel
from eventsource import AggregateRoot, DomainEvent

class OrderState(BaseModel):
    """State of an Order aggregate."""
    order_id: UUID
    tenant_id: UUID | None = None
    customer_name: str = ""
    total_amount: float = 0.0
    status: str = "draft"
    tracking_number: str | None = None


class TenantOrderAggregate(AggregateRoot[OrderState]):
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
                customer_name=event.customer_name,
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

    def create(self, customer_name: str, total_amount: float) -> None:
        """Create the order with tenant context."""
        if self.version > 0:
            raise ValueError("Order already created")
        if self._tenant_id is None:
            raise ValueError("Tenant context required to create order")

        event = OrderCreated(
            aggregate_id=self.aggregate_id,
            tenant_id=self._tenant_id,  # Include tenant in event
            customer_name=customer_name,
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

### Key Points

1. **Constructor accepts tenant_id**: The aggregate knows its tenant from creation
2. **Events include tenant_id**: Every event carries the tenant context
3. **Replay captures tenant_id**: When loading from history, the aggregate restores its tenant
4. **Validation**: The `create()` method requires tenant context

---

## Tenant-Aware Repository

Create a repository wrapper that enforces tenant boundaries:

```python
from uuid import UUID
from eventsource import AggregateRepository, EventStore


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
            aggregate_factory=lambda aid: TenantOrderAggregate(aid, tenant_id),
            aggregate_type="Order",
        )

    def create_new(self, order_id: UUID) -> TenantOrderAggregate:
        """Create a new order in this tenant's context."""
        return TenantOrderAggregate(order_id, self._tenant_id)

    async def save(self, order: TenantOrderAggregate) -> None:
        """
        Save order, ensuring tenant matches.

        Raises:
            PermissionError: If order belongs to different tenant
        """
        if order.tenant_id and order.tenant_id != self._tenant_id:
            raise PermissionError(
                f"Cannot save order for tenant {order.tenant_id} "
                f"in context of tenant {self._tenant_id}"
            )
        await self._inner_repo.save(order)

    async def load(self, order_id: UUID) -> TenantOrderAggregate:
        """
        Load order, verifying tenant ownership.

        Raises:
            AggregateNotFoundError: If order doesn't exist
            PermissionError: If order belongs to different tenant
        """
        order = await self._inner_repo.load(order_id)

        # Verify tenant ownership after loading
        if order.tenant_id and order.tenant_id != self._tenant_id:
            raise PermissionError(
                f"Order {order_id} belongs to tenant {order.tenant_id}, "
                f"not {self._tenant_id}"
            )

        return order

    async def exists(self, order_id: UUID) -> bool:
        """Check if order exists for this tenant."""
        try:
            await self.load(order_id)
            return True
        except Exception:
            return False
```

### Why Repository-Level Enforcement?

The repository pattern provides a clean enforcement point for tenant isolation:

1. **Centralized validation**: All access goes through one point
2. **Prevents accidents**: Developers can't accidentally load wrong tenant's data
3. **Clear API**: The repository's constructor makes tenant context explicit
4. **Consistent behavior**: Load, save, and create all respect tenant boundaries

---

## Querying Events by Tenant

The event store provides built-in tenant filtering capabilities.

### Using get_events_by_type()

Query all events of a specific type for a tenant:

```python
from uuid import UUID
from datetime import datetime, UTC, timedelta
from eventsource import InMemoryEventStore

async def get_tenant_orders(
    event_store: InMemoryEventStore,
    tenant_id: UUID,
) -> list[DomainEvent]:
    """Get all Order events for a specific tenant."""
    events = await event_store.get_events_by_type(
        aggregate_type="Order",
        tenant_id=tenant_id,
    )
    return events


async def get_recent_tenant_orders(
    event_store: InMemoryEventStore,
    tenant_id: UUID,
    hours: int = 24,
) -> list[DomainEvent]:
    """Get recent Order events for a tenant."""
    since = datetime.now(UTC) - timedelta(hours=hours)
    events = await event_store.get_events_by_type(
        aggregate_type="Order",
        tenant_id=tenant_id,
        from_timestamp=since,
    )
    return events
```

### Streaming All Events with Tenant Filter

For projections that need to process all events, filter during streaming:

```python
from eventsource import ReadOptions

async def process_tenant_events(
    event_store: InMemoryEventStore,
    tenant_id: UUID,
):
    """Process all events for a specific tenant."""
    async for stored_event in event_store.read_all():
        # Filter by tenant during iteration
        if stored_event.event.tenant_id == tenant_id:
            await process_event(stored_event.event)
```

### Example: Counting Events by Tenant

```python
async def count_events_by_tenant(event_store: InMemoryEventStore):
    """Count events grouped by tenant."""
    tenant_counts: dict[str, int] = {}

    async for stored_event in event_store.read_all():
        tenant_key = str(stored_event.event.tenant_id or "no-tenant")
        tenant_counts[tenant_key] = tenant_counts.get(tenant_key, 0) + 1

    return tenant_counts
```

---

## Tenant-Scoped Projections

Projections can be scoped to specific tenants or track all tenants globally.

### Tenant-Filtered Projection

A projection that only processes events for one tenant:

```python
from uuid import UUID
from eventsource import DeclarativeProjection, handles


class TenantOrderSummaryProjection(DeclarativeProjection):
    """Projection that tracks orders for a specific tenant only."""

    def __init__(
        self,
        tenant_id: UUID,
        checkpoint_repo=None,
        dlq_repo=None,
    ):
        super().__init__(checkpoint_repo=checkpoint_repo, dlq_repo=dlq_repo)
        self._target_tenant = tenant_id
        self._orders: dict[str, dict] = {}

    @handles(OrderCreated)
    async def _on_order_created(self, event: OrderCreated) -> None:
        # Skip events from other tenants
        if event.tenant_id != self._target_tenant:
            return

        self._orders[str(event.aggregate_id)] = {
            "order_id": str(event.aggregate_id),
            "customer": event.customer_name,
            "total": event.total_amount,
            "status": "created",
        }

    @handles(OrderShipped)
    async def _on_order_shipped(self, event: OrderShipped) -> None:
        # Skip events from other tenants
        if event.tenant_id != self._target_tenant:
            return

        order_id = str(event.aggregate_id)
        if order_id in self._orders:
            self._orders[order_id]["status"] = "shipped"
            self._orders[order_id]["tracking"] = event.tracking_number

    async def _truncate_read_models(self) -> None:
        self._orders.clear()

    def get_orders(self) -> list[dict]:
        """Get all orders for this tenant."""
        return list(self._orders.values())

    def get_order(self, order_id: UUID) -> dict | None:
        """Get a specific order."""
        return self._orders.get(str(order_id))
```

### Global Projection with Per-Tenant Views

For analytics that need to track all tenants:

```python
class GlobalOrderStatsProjection(DeclarativeProjection):
    """Projection that tracks statistics across all tenants."""

    def __init__(self, checkpoint_repo=None, dlq_repo=None):
        super().__init__(checkpoint_repo=checkpoint_repo, dlq_repo=dlq_repo)
        self._tenant_stats: dict[str, dict] = {}

    @handles(OrderCreated)
    async def _on_order_created(self, event: OrderCreated) -> None:
        tenant_key = str(event.tenant_id) if event.tenant_id else "no-tenant"

        if tenant_key not in self._tenant_stats:
            self._tenant_stats[tenant_key] = {
                "order_count": 0,
                "total_revenue": 0.0,
                "shipped_count": 0,
            }

        self._tenant_stats[tenant_key]["order_count"] += 1
        self._tenant_stats[tenant_key]["total_revenue"] += event.total_amount

    @handles(OrderShipped)
    async def _on_order_shipped(self, event: OrderShipped) -> None:
        tenant_key = str(event.tenant_id) if event.tenant_id else "no-tenant"
        if tenant_key in self._tenant_stats:
            self._tenant_stats[tenant_key]["shipped_count"] += 1

    async def _truncate_read_models(self) -> None:
        self._tenant_stats.clear()

    def get_tenant_stats(self, tenant_id: UUID) -> dict | None:
        """Get statistics for a specific tenant."""
        return self._tenant_stats.get(str(tenant_id))

    def get_all_stats(self) -> dict[str, dict]:
        """Get statistics for all tenants (admin only)."""
        return self._tenant_stats.copy()

    def get_total_revenue(self) -> float:
        """Get total revenue across all tenants."""
        return sum(stats["total_revenue"] for stats in self._tenant_stats.values())
```

---

## Tenant Context Propagation

In web applications, tenant context needs to flow from authentication to all operations.

### Using Context Variables

Python's `contextvars` module provides thread-safe, async-aware context:

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


def require_tenant() -> UUID:
    """Get tenant ID or raise if not set."""
    tenant_id = _current_tenant.get()
    if tenant_id is None:
        raise ValueError("Tenant context not set")
    return tenant_id
```

### FastAPI Middleware Example

Extract tenant from authentication and set context:

```python
from fastapi import FastAPI, Request, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from uuid import UUID

class TenantMiddleware(BaseHTTPMiddleware):
    """Middleware to extract and set tenant context from requests."""

    async def dispatch(self, request: Request, call_next):
        # Option 1: From header
        tenant_header = request.headers.get("X-Tenant-ID")

        # Option 2: From JWT token claims (if using auth)
        # tenant_header = request.state.user.tenant_id

        # Option 3: From subdomain
        # host = request.headers.get("host", "")
        # tenant_header = host.split(".")[0]

        if tenant_header:
            try:
                tenant_id = UUID(tenant_header)
                set_current_tenant(tenant_id)
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid tenant ID format"
                )
        else:
            set_current_tenant(None)

        try:
            response = await call_next(request)
            return response
        finally:
            # Clear tenant context after request
            set_current_tenant(None)


# Apply middleware
app = FastAPI()
app.add_middleware(TenantMiddleware)
```

### Dependency Injection Pattern

Create FastAPI dependencies for tenant-scoped resources:

```python
from fastapi import Depends, HTTPException
from uuid import UUID

async def get_tenant_id() -> UUID:
    """FastAPI dependency to get and validate tenant ID."""
    tenant_id = get_current_tenant()
    if not tenant_id:
        raise HTTPException(
            status_code=401,
            detail="Tenant context required"
        )
    return tenant_id


async def get_order_repository(
    tenant_id: UUID = Depends(get_tenant_id),
) -> TenantAwareOrderRepository:
    """Get a tenant-scoped order repository."""
    # Assume event_store is available globally or injected
    return TenantAwareOrderRepository(
        event_store=event_store,
        tenant_id=tenant_id,
    )


# Usage in endpoint
@app.post("/orders")
async def create_order(
    customer_name: str,
    total_amount: float,
    repo: TenantAwareOrderRepository = Depends(get_order_repository),
):
    """Create an order in the current tenant's context."""
    order_id = uuid4()
    order = repo.create_new(order_id)
    order.create(customer_name=customer_name, total_amount=total_amount)
    await repo.save(order)
    return {"order_id": str(order_id)}
```

---

## Testing Multi-Tenant Applications

### Setting Up Multiple Tenant Fixtures

```python
import pytest
from uuid import uuid4
from eventsource import InMemoryEventStore

@pytest.fixture
def tenant_a():
    """Tenant A fixture."""
    return uuid4()


@pytest.fixture
def tenant_b():
    """Tenant B fixture."""
    return uuid4()


@pytest.fixture
def event_store():
    """Shared event store for testing."""
    return InMemoryEventStore()


@pytest.fixture
def repo_a(event_store, tenant_a):
    """Repository for Tenant A."""
    return TenantAwareOrderRepository(event_store, tenant_a)


@pytest.fixture
def repo_b(event_store, tenant_b):
    """Repository for Tenant B."""
    return TenantAwareOrderRepository(event_store, tenant_b)
```

### Testing Tenant Isolation

```python
import pytest

@pytest.mark.asyncio
async def test_tenant_isolation(repo_a, repo_b, tenant_a, tenant_b):
    """Test that tenants cannot access each other's data."""
    # Tenant A creates an order
    order_a = repo_a.create_new(uuid4())
    order_a.create("Alice", 100.0)
    await repo_a.save(order_a)

    # Tenant B creates an order
    order_b = repo_b.create_new(uuid4())
    order_b.create("Bob", 200.0)
    await repo_b.save(order_b)

    # Tenant A can load their own order
    loaded_a = await repo_a.load(order_a.aggregate_id)
    assert loaded_a.state.customer_name == "Alice"

    # Tenant B cannot load Tenant A's order
    with pytest.raises(PermissionError):
        await repo_b.load(order_a.aggregate_id)

    # Tenant A cannot load Tenant B's order
    with pytest.raises(PermissionError):
        await repo_a.load(order_b.aggregate_id)


@pytest.mark.asyncio
async def test_tenant_event_filtering(event_store, tenant_a, tenant_b, repo_a, repo_b):
    """Test that event queries respect tenant boundaries."""
    # Create orders for both tenants
    order_a = repo_a.create_new(uuid4())
    order_a.create("Alice", 100.0)
    await repo_a.save(order_a)

    order_b = repo_b.create_new(uuid4())
    order_b.create("Bob", 200.0)
    await repo_b.save(order_b)

    # Query events by tenant
    events_a = await event_store.get_events_by_type("Order", tenant_id=tenant_a)
    events_b = await event_store.get_events_by_type("Order", tenant_id=tenant_b)

    # Each tenant only sees their own events
    assert len(events_a) == 1
    assert events_a[0].customer_name == "Alice"

    assert len(events_b) == 1
    assert events_b[0].customer_name == "Bob"
```

---

## Security Best Practices

### Never Trust Client-Provided Tenant IDs

```python
# WRONG: Trusting tenant_id from request body
@app.post("/orders/unsafe")
async def create_order_unsafe(request: dict):
    tenant_id = request.get("tenant_id")  # Client could set any tenant!
    # ...

# CORRECT: Get tenant from authenticated context
@app.post("/orders/safe")
async def create_order_safe(
    request: CreateOrderRequest,
    tenant_id: UUID = Depends(get_tenant_id),  # From auth token
):
    # tenant_id is verified and trusted
    # ...
```

### Always Validate After Loading

Even with tenant-aware factories, validate after loading:

```python
async def get_order(order_id: UUID, tenant_id: UUID) -> TenantOrderAggregate:
    """Load order with explicit tenant validation."""
    order = await inner_repo.load(order_id)

    # Defense in depth: verify tenant even after loading
    if order.tenant_id != tenant_id:
        # Log security event
        logger.warning(
            "Cross-tenant access attempt",
            extra={
                "requested_order": str(order_id),
                "order_tenant": str(order.tenant_id),
                "requesting_tenant": str(tenant_id),
            }
        )
        raise PermissionError("Access denied")

    return order
```

### Audit Logging

The event store provides a complete audit trail per tenant:

```python
async def get_tenant_audit_log(
    event_store: InMemoryEventStore,
    tenant_id: UUID,
    aggregate_type: str | None = None,
) -> list[dict]:
    """Get audit log for a tenant."""
    if aggregate_type:
        events = await event_store.get_events_by_type(
            aggregate_type=aggregate_type,
            tenant_id=tenant_id,
        )
    else:
        # Get all events for tenant by iterating
        events = []
        async for stored in event_store.read_all():
            if stored.event.tenant_id == tenant_id:
                events.append(stored.event)

    return [
        {
            "event_id": str(e.event_id),
            "event_type": e.event_type,
            "aggregate_id": str(e.aggregate_id),
            "aggregate_type": e.aggregate_type,
            "occurred_at": e.occurred_at.isoformat(),
            "actor_id": e.actor_id,
        }
        for e in events
    ]
```

---

## Complete Example

Here's a complete working example that demonstrates all multi-tenancy concepts:

```python
"""
Tutorial 16: Multi-Tenancy Implementation

This example demonstrates multi-tenant event sourcing with eventsource-py.
Run with: python tutorial_16_multi_tenancy.py
"""
import asyncio
from uuid import UUID, uuid4

from pydantic import BaseModel
from eventsource import (
    DomainEvent,
    register_event,
    AggregateRoot,
    InMemoryEventStore,
    AggregateRepository,
    DeclarativeProjection,
    handles,
)


# =============================================================================
# Events
# =============================================================================

@register_event
class OrderCreated(DomainEvent):
    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    customer_name: str
    total_amount: float


@register_event
class OrderShipped(DomainEvent):
    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"
    tracking_number: str


# =============================================================================
# Aggregate State
# =============================================================================

class OrderState(BaseModel):
    order_id: UUID
    tenant_id: UUID | None = None
    customer_name: str = ""
    total_amount: float = 0.0
    status: str = "draft"
    tracking_number: str | None = None


# =============================================================================
# Tenant-Aware Aggregate
# =============================================================================

class TenantOrderAggregate(AggregateRoot[OrderState]):
    """Order aggregate with tenant awareness."""
    aggregate_type = "Order"

    def __init__(self, aggregate_id: UUID, tenant_id: UUID | None = None):
        super().__init__(aggregate_id)
        self._tenant_id = tenant_id

    @property
    def tenant_id(self) -> UUID | None:
        return self._tenant_id

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id, tenant_id=self._tenant_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            self._state = OrderState(
                order_id=self.aggregate_id,
                tenant_id=event.tenant_id,
                customer_name=event.customer_name,
                total_amount=event.total_amount,
                status="created",
            )
            if event.tenant_id:
                self._tenant_id = event.tenant_id
        elif isinstance(event, OrderShipped):
            if self._state:
                self._state = self._state.model_copy(update={
                    "status": "shipped",
                    "tracking_number": event.tracking_number,
                })

    def create(self, customer_name: str, total_amount: float) -> None:
        if self.version > 0:
            raise ValueError("Order already exists")
        if not self._tenant_id:
            raise ValueError("Tenant context required")

        self.apply_event(OrderCreated(
            aggregate_id=self.aggregate_id,
            tenant_id=self._tenant_id,
            customer_name=customer_name,
            total_amount=total_amount,
            aggregate_version=self.get_next_version(),
        ))

    def ship(self, tracking_number: str) -> None:
        if not self._state or self._state.status != "created":
            raise ValueError("Cannot ship order in current state")

        self.apply_event(OrderShipped(
            aggregate_id=self.aggregate_id,
            tenant_id=self._tenant_id,
            tracking_number=tracking_number,
            aggregate_version=self.get_next_version(),
        ))


# =============================================================================
# Tenant-Aware Repository
# =============================================================================

class TenantAwareOrderRepository:
    """Repository that enforces tenant boundaries."""

    def __init__(self, event_store: InMemoryEventStore, tenant_id: UUID):
        self._tenant_id = tenant_id
        self._inner_repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=lambda aid: TenantOrderAggregate(aid, tenant_id),
            aggregate_type="Order",
        )

    def create_new(self, order_id: UUID) -> TenantOrderAggregate:
        return TenantOrderAggregate(order_id, self._tenant_id)

    async def save(self, order: TenantOrderAggregate) -> None:
        if order.tenant_id != self._tenant_id:
            raise PermissionError("Cannot save order from different tenant")
        await self._inner_repo.save(order)

    async def load(self, order_id: UUID) -> TenantOrderAggregate:
        order = await self._inner_repo.load(order_id)
        if order.tenant_id != self._tenant_id:
            raise PermissionError("Order belongs to different tenant")
        return order


# =============================================================================
# Tenant-Scoped Projection
# =============================================================================

class TenantOrderProjection(DeclarativeProjection):
    """Projection that tracks orders per tenant."""

    def __init__(self, tenant_id: UUID | None = None):
        super().__init__()
        self._target_tenant = tenant_id
        self.orders_by_tenant: dict[str, list[dict]] = {}

    @handles(OrderCreated)
    async def _on_created(self, event: OrderCreated) -> None:
        if self._target_tenant and event.tenant_id != self._target_tenant:
            return

        tenant_key = str(event.tenant_id) if event.tenant_id else "default"
        if tenant_key not in self.orders_by_tenant:
            self.orders_by_tenant[tenant_key] = []

        self.orders_by_tenant[tenant_key].append({
            "order_id": str(event.aggregate_id),
            "customer": event.customer_name,
            "total": event.total_amount,
            "status": "created",
        })

    @handles(OrderShipped)
    async def _on_shipped(self, event: OrderShipped) -> None:
        if self._target_tenant and event.tenant_id != self._target_tenant:
            return

        tenant_key = str(event.tenant_id) if event.tenant_id else "default"
        if tenant_key in self.orders_by_tenant:
            for order in self.orders_by_tenant[tenant_key]:
                if order["order_id"] == str(event.aggregate_id):
                    order["status"] = "shipped"

    async def _truncate_read_models(self) -> None:
        self.orders_by_tenant.clear()

    def get_tenant_orders(self, tenant_id: UUID) -> list[dict]:
        return self.orders_by_tenant.get(str(tenant_id), [])


# =============================================================================
# Demonstration
# =============================================================================

async def main():
    print("=" * 60)
    print("Tutorial 16: Multi-Tenancy Implementation")
    print("=" * 60)

    # Create shared event store
    event_store = InMemoryEventStore()

    # Create two tenants
    tenant_acme = uuid4()
    tenant_globex = uuid4()
    print(f"\nTenant ACME:   {tenant_acme}")
    print(f"Tenant Globex: {tenant_globex}")

    # Create tenant-specific repositories
    repo_acme = TenantAwareOrderRepository(event_store, tenant_acme)
    repo_globex = TenantAwareOrderRepository(event_store, tenant_globex)

    # =========================================================================
    # Create orders for different tenants
    # =========================================================================
    print("\n--- Creating Orders ---")

    # ACME orders
    order_1 = repo_acme.create_new(uuid4())
    order_1.create("Alice (ACME)", 100.0)
    await repo_acme.save(order_1)
    print(f"ACME Order 1: {order_1.aggregate_id}")

    order_2 = repo_acme.create_new(uuid4())
    order_2.create("Bob (ACME)", 250.0)
    await repo_acme.save(order_2)
    print(f"ACME Order 2: {order_2.aggregate_id}")

    # Globex order
    order_3 = repo_globex.create_new(uuid4())
    order_3.create("Charlie (Globex)", 500.0)
    await repo_globex.save(order_3)
    print(f"Globex Order: {order_3.aggregate_id}")

    # Ship ACME's first order
    loaded = await repo_acme.load(order_1.aggregate_id)
    loaded.ship("TRACK-ACME-001")
    await repo_acme.save(loaded)
    print(f"Shipped ACME Order 1")

    # =========================================================================
    # Query events by tenant
    # =========================================================================
    print("\n--- Event Queries by Tenant ---")

    acme_events = await event_store.get_events_by_type("Order", tenant_id=tenant_acme)
    globex_events = await event_store.get_events_by_type("Order", tenant_id=tenant_globex)

    print(f"ACME events:   {len(acme_events)}")
    print(f"Globex events: {len(globex_events)}")

    # =========================================================================
    # Test tenant isolation
    # =========================================================================
    print("\n--- Testing Tenant Isolation ---")

    try:
        await repo_globex.load(order_1.aggregate_id)
        print("ERROR: Should not have access!")
    except PermissionError as e:
        print(f"Correctly blocked: {e}")

    # =========================================================================
    # Tenant-scoped projection
    # =========================================================================
    print("\n--- Projection Results ---")

    # Global projection (sees all tenants)
    global_projection = TenantOrderProjection()

    # Tenant-specific projection (only ACME)
    acme_projection = TenantOrderProjection(tenant_id=tenant_acme)

    # Process all events through projections
    async for stored in event_store.read_all():
        await global_projection.handle(stored.event)
        await acme_projection.handle(stored.event)

    print(f"Global - ACME orders:   {len(global_projection.get_tenant_orders(tenant_acme))}")
    print(f"Global - Globex orders: {len(global_projection.get_tenant_orders(tenant_globex))}")
    print(f"ACME-only projection:   {len(acme_projection.get_tenant_orders(tenant_acme))}")

    # Show order details
    print("\nACME Order Details:")
    for order in global_projection.get_tenant_orders(tenant_acme):
        print(f"  - {order['customer']}: ${order['total']:.2f} ({order['status']})")

    print("\n" + "=" * 60)
    print("Multi-Tenancy Demonstration Complete!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
```

Save this as `tutorial_16_multi_tenancy.py` and run it:

```bash
python tutorial_16_multi_tenancy.py
```

Expected output:
```
============================================================
Tutorial 16: Multi-Tenancy Implementation
============================================================

Tenant ACME:   <uuid>
Tenant Globex: <uuid>

--- Creating Orders ---
ACME Order 1: <uuid>
ACME Order 2: <uuid>
Globex Order: <uuid>
Shipped ACME Order 1

--- Event Queries by Tenant ---
ACME events:   3
Globex events: 1

--- Testing Tenant Isolation ---
Correctly blocked: Order belongs to different tenant

--- Projection Results ---
Global - ACME orders:   2
Global - Globex orders: 1
ACME-only projection:   2

ACME Order Details:
  - Alice (ACME): $100.00 (shipped)
  - Bob (ACME): $250.00 (created)

============================================================
Multi-Tenancy Demonstration Complete!
============================================================
```

---

## Exercises

### Exercise 1: Multi-Tenant Order Management System

**Objective:** Build a complete multi-tenant order management system with reporting.

**Time:** 25-30 minutes

**Requirements:**

1. Create a tenant-aware `ProductOrder` aggregate with:
   - Products with names, quantities, and prices
   - Order totals calculated from products
   - Order status tracking (pending, confirmed, shipped)

2. Create orders for at least 3 different tenants

3. Build a `TenantSalesReportProjection` that tracks:
   - Total orders per tenant
   - Total revenue per tenant
   - Average order value per tenant

4. Query and display events filtered by each tenant

5. Verify tenant isolation by attempting cross-tenant access

**Starter Code:**

```python
"""
Tutorial 16 - Exercise 1: Multi-Tenant Order Management

Your task: Build a multi-tenant order system with sales reporting.
"""
import asyncio
from uuid import uuid4, UUID

from pydantic import BaseModel
from eventsource import (
    DomainEvent,
    register_event,
    AggregateRoot,
    InMemoryEventStore,
    AggregateRepository,
    DeclarativeProjection,
    handles,
)


# Step 1: Define your events
# TODO: Create ProductOrderCreated and ProductOrderConfirmed events


# Step 2: Define your aggregate state and aggregate
# TODO: Create ProductOrderState and ProductOrderAggregate


# Step 3: Create tenant-aware repository
# TODO: Create TenantOrderRepository


# Step 4: Create sales report projection
# TODO: Create TenantSalesReportProjection


async def main():
    print("=== Exercise 16-1: Multi-Tenant Order Management ===\n")

    event_store = InMemoryEventStore()

    # Step 5: Create at least 3 tenants and their repositories
    # TODO: Create tenant_alpha, tenant_beta, tenant_gamma

    # Step 6: Create orders for each tenant
    # TODO: Create multiple orders with different products

    # Step 7: Query events by tenant
    # TODO: Use get_events_by_type with tenant_id

    # Step 8: Build and display sales report
    # TODO: Process events through TenantSalesReportProjection

    # Step 9: Verify tenant isolation
    # TODO: Try to load one tenant's order through another's repository

    print("\n=== Exercise Complete! ===")


if __name__ == "__main__":
    asyncio.run(main())
```

**Hints:**

- Use `event.quantity * event.price` to calculate order totals
- The projection should track stats by tenant_id string
- Remember to include `tenant_id` in all events
- Test both successful isolation (PermissionError) and proper access

**Solution:** Available in `docs/tutorials/exercises/solutions/16-1.py`

---

## Summary

In this tutorial, you learned:

- **Multi-tenancy fundamentals**: Why it matters for SaaS and how event sourcing supports it
- **tenant_id field**: Built into all DomainEvents for automatic tenant tracking
- **Tenant-aware aggregates**: How to pass tenant context and include it in events
- **Tenant-aware repositories**: Enforcing boundaries at the data access layer
- **Tenant-scoped queries**: Using `get_events_by_type()` with tenant filtering
- **Tenant-scoped projections**: Building read models that respect tenant boundaries
- **Context propagation**: Using middleware and dependency injection for tenant context

---

## Key Takeaways

!!! warning "Multi-Tenancy Best Practices"
    - **Always validate tenant context** before any data operation
    - **Never trust client-provided tenant IDs** - get them from authenticated context
    - **Test tenant isolation thoroughly** - verify cross-tenant access is blocked
    - **Consider compliance requirements** - some regulations require physical separation

!!! tip "Performance Tip"
    The `tenant_id` column is indexed for efficient filtering. For very large deployments with many tenants, consider time-based partitioning in addition to tenant filtering.

!!! note "When to Use Database-per-Tenant"
    Consider separate databases when:
    - Regulatory requirements demand physical isolation
    - Tenants need independent backup/restore capabilities
    - Tenants have vastly different scale requirements
    - Data residency laws require geographic separation

---

## Next Steps

Continue to [Tutorial 17: Using Redis Event Bus](17-redis.md) to learn how to distribute events across services using Redis Streams, including tenant context propagation in distributed systems.

---

## Related Documentation

- [Multi-Tenant Setup Guide](../guides/multi-tenant.md) - Detailed reference guide
- [Tutorial 5: Repositories](05-repositories.md) - Repository pattern foundations
- [Tutorial 6: Projections](06-projections.md) - Projection fundamentals
- [Tutorial 15: Production Deployment](15-production.md) - Production checklist
