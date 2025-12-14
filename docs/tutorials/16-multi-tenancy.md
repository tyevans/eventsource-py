# Tutorial 16: Multi-Tenancy Patterns

**Difficulty:** Advanced

## Prerequisites

- [Tutorial 1: Introduction to Event Sourcing](01-introduction.md)
- [Tutorial 4: Event Stores](04-event-stores.md)
- [Tutorial 5: Repositories - Managing Aggregate Lifecycle](05-repositories.md)
- [Tutorial 6: Projections - Building Read Models](06-projections.md)
- [Tutorial 15: Production Deployment](15-production.md)
- Python 3.10 or higher
- Understanding of async/await
- Understanding of multi-tenant application architecture

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Explain multi-tenancy patterns in event sourcing systems
2. Use the built-in `tenant_id` field on domain events
3. Implement tenant-aware aggregates and repositories
4. Filter events by tenant using event store queries
5. Build tenant-scoped and cross-tenant projections
6. Propagate tenant context through your application
7. Apply security best practices for tenant isolation
8. Test multi-tenant applications effectively

---

## What is Multi-Tenancy?

**Multi-tenancy** allows multiple customers (tenants) to share the same infrastructure while maintaining strict data isolation. Each tenant's data is logically separated, ensuring privacy and security.

### Multi-Tenancy Patterns

| Pattern | Description | Use Cases |
|---------|-------------|-----------|
| **Separate Databases** | Each tenant has own database | Strongest isolation, highest cost |
| **Separate Schemas** | Shared database, tenant schemas | Good isolation, moderate cost |
| **Shared Schema** | Application-level filtering | Best scalability, lowest cost |

eventsource-py uses **shared schema with application-level isolation** via the built-in `tenant_id` field. This provides excellent scalability while maintaining strong isolation guarantees.

### Benefits

- **Cost efficiency**: Share infrastructure across tenants
- **Operational simplicity**: Single deployment, unified monitoring
- **Scalability**: Add tenants without infrastructure changes
- **Data isolation**: Built-in tenant filtering at the event store level

### Trade-offs

- **Security responsibility**: Application must enforce tenant boundaries
- **Cross-tenant queries**: More complex than single-tenant systems
- **Testing complexity**: Must verify isolation between tenants
- **Migration coordination**: Schema changes affect all tenants

---

## The tenant_id Field

Every `DomainEvent` includes an optional `tenant_id` field that's automatically indexed and persisted by all event store implementations.

### Field Definition

```python
class DomainEvent(BaseModel):
    """Base class for all domain events."""

    tenant_id: UUID | None = Field(
        default=None,
        description="Tenant this event belongs to (optional)",
    )

    # ... other fields
```

**Key characteristics:**

- **Optional**: Events without `tenant_id` are not tenant-scoped
- **Immutable**: Once set, cannot be changed (events are frozen)
- **Indexed**: All event stores index `tenant_id` for fast filtering
- **Propagated**: Automatically preserved through event chains

---

## Creating Tenant-Scoped Events

Events inherit `tenant_id` from `DomainEvent`. You just need to provide it when creating events.

### Basic Event Definition

```python
from uuid import UUID, uuid4
from eventsource import DomainEvent, register_event

@register_event
class OrderPlaced(DomainEvent):
    event_type: str = "OrderPlaced"
    aggregate_type: str = "Order"

    customer_id: UUID
    customer_name: str
    total_amount: float
    # tenant_id inherited from DomainEvent

@register_event
class OrderShipped(DomainEvent):
    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"

    tracking_number: str
```

### Creating Events with Tenant Context

```python
# Tenant context (typically from authentication)
tenant_id = uuid4()  # ACME Corp
order_id = uuid4()

# Create event with tenant
event = OrderPlaced(
    aggregate_id=order_id,
    tenant_id=tenant_id,  # Associate with tenant
    customer_id=uuid4(),
    customer_name="Alice Johnson",
    total_amount=299.99,
    aggregate_version=1,
)

print(f"Event belongs to tenant: {event.tenant_id}")
```

**Important:** The `tenant_id` should come from a trusted source (authentication context), never from user input.

---

## Tenant-Aware Aggregates

Aggregates need to track and enforce tenant context throughout their lifecycle.

### Aggregate with Tenant Support

```python
from uuid import UUID
from pydantic import BaseModel
from eventsource import AggregateRoot, DomainEvent

class OrderState(BaseModel):
    order_id: UUID
    tenant_id: UUID | None = None
    customer_id: UUID | None = None
    customer_name: str = ""
    total_amount: float = 0.0
    status: str = "draft"
    tracking_number: str | None = None

class OrderAggregate(AggregateRoot[OrderState]):
    aggregate_type = "Order"

    def __init__(self, aggregate_id: UUID, tenant_id: UUID | None = None):
        """
        Initialize aggregate with tenant context.

        Args:
            aggregate_id: Unique order identifier
            tenant_id: Tenant this order belongs to
        """
        super().__init__(aggregate_id)
        self._tenant_id = tenant_id

    @property
    def tenant_id(self) -> UUID | None:
        """Get the tenant ID for this aggregate."""
        return self._tenant_id

    def _get_initial_state(self) -> OrderState:
        """Initialize state with tenant context."""
        return OrderState(
            order_id=self.aggregate_id,
            tenant_id=self._tenant_id,
        )

    def _apply(self, event: DomainEvent) -> None:
        """Apply events and update state."""
        if isinstance(event, OrderPlaced):
            self._state = OrderState(
                order_id=self.aggregate_id,
                tenant_id=event.tenant_id,
                customer_id=event.customer_id,
                customer_name=event.customer_name,
                total_amount=event.total_amount,
                status="placed",
            )
            # Set tenant from first event
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

    def place_order(self, customer_id: UUID, customer_name: str, total_amount: float) -> None:
        """Place a new order."""
        if self.version > 0:
            raise ValueError("Order already placed")
        if not self._tenant_id:
            raise ValueError("Tenant context required to place order")

        self.apply_event(
            OrderPlaced(
                aggregate_id=self.aggregate_id,
                tenant_id=self._tenant_id,  # Always include tenant
                customer_id=customer_id,
                customer_name=customer_name,
                total_amount=total_amount,
                aggregate_version=self.get_next_version(),
            )
        )

    def ship(self, tracking_number: str) -> None:
        """Ship the order."""
        if not self._state or self._state.status != "placed":
            raise ValueError("Order must be placed before shipping")

        self.apply_event(
            OrderShipped(
                aggregate_id=self.aggregate_id,
                tenant_id=self._tenant_id,  # Always include tenant
                tracking_number=tracking_number,
                aggregate_version=self.get_next_version(),
            )
        )
```

**Key patterns:**

1. **Constructor takes tenant_id**: Aggregate knows its tenant from creation
2. **Property exposes tenant**: Easy to check which tenant owns the aggregate
3. **All events include tenant**: Ensures complete tenant tracking
4. **Tenant validation**: Prevents operations without tenant context

---

## Tenant-Aware Repository

Repositories should enforce tenant boundaries, preventing cross-tenant access.

### Basic Tenant-Scoped Repository

```python
from eventsource import AggregateRepository, EventStore

class TenantOrderRepository:
    """Repository that enforces tenant boundaries."""

    def __init__(self, event_store: EventStore, tenant_id: UUID):
        """
        Create tenant-scoped repository.

        Args:
            event_store: Underlying event store
            tenant_id: Tenant this repository is scoped to
        """
        self._tenant_id = tenant_id
        self._inner_repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=lambda aid: OrderAggregate(aid, tenant_id),
            aggregate_type="Order",
        )

    @property
    def tenant_id(self) -> UUID:
        """Get the tenant ID this repository is scoped to."""
        return self._tenant_id

    def create_new(self, order_id: UUID) -> OrderAggregate:
        """Create new order aggregate for this tenant."""
        return OrderAggregate(order_id, self._tenant_id)

    async def save(self, order: OrderAggregate) -> None:
        """
        Save order aggregate with tenant validation.

        Raises:
            PermissionError: If order belongs to different tenant
        """
        if order.tenant_id != self._tenant_id:
            raise PermissionError(
                f"Cannot save order from tenant {order.tenant_id} "
                f"using repository scoped to {self._tenant_id}"
            )
        await self._inner_repo.save(order)

    async def load(self, order_id: UUID) -> OrderAggregate:
        """
        Load order aggregate with tenant validation.

        Raises:
            PermissionError: If order belongs to different tenant
        """
        order = await self._inner_repo.load(order_id)
        if order.tenant_id != self._tenant_id:
            raise PermissionError(
                f"Order {order_id} belongs to different tenant"
            )
        return order

    async def exists(self, order_id: UUID) -> bool:
        """Check if order exists and belongs to this tenant."""
        try:
            await self.load(order_id)
            return True
        except (PermissionError, Exception):
            return False
```

**Security enforcement:**

1. **Constructor scoping**: Repository is bound to specific tenant
2. **Validation on save**: Prevents saving other tenant's aggregates
3. **Validation on load**: Prevents accessing other tenant's data
4. **Factory injection**: All created aggregates have correct tenant

---

## Querying Events by Tenant

Event stores provide built-in tenant filtering for efficient queries.

### Filter by Tenant and Type

```python
from eventsource import InMemoryEventStore
from datetime import datetime, UTC, timedelta

async def get_tenant_orders(
    event_store: InMemoryEventStore,
    tenant_id: UUID,
) -> list[DomainEvent]:
    """Get all order events for a specific tenant."""
    return await event_store.get_events_by_type(
        aggregate_type="Order",
        tenant_id=tenant_id,  # Filter by tenant
    )

async def get_recent_tenant_orders(
    event_store: InMemoryEventStore,
    tenant_id: UUID,
    hours: int = 24,
) -> list[DomainEvent]:
    """Get recent order events for a specific tenant."""
    since = datetime.now(UTC) - timedelta(hours=hours)
    return await event_store.get_events_by_type(
        aggregate_type="Order",
        tenant_id=tenant_id,
        from_timestamp=since,  # Time filter
    )
```

### Stream All Events for Tenant

```python
from eventsource.stores import ReadOptions

async def stream_tenant_events(
    event_store: InMemoryEventStore,
    tenant_id: UUID,
):
    """Stream all events for a specific tenant."""
    options = ReadOptions(
        tenant_id=tenant_id,  # Filter by tenant
        from_position=0,       # Start from beginning
    )

    async for stored_event in event_store.read_all(options):
        print(f"Event: {stored_event.event_type}")
        print(f"Tenant: {stored_event.event.tenant_id}")
        print(f"Position: {stored_event.global_position}")
```

**Performance note:** All event stores (InMemory, PostgreSQL, SQLite) index `tenant_id`, making these queries efficient even with millions of events.

---

## Tenant-Scoped Projections

Projections can be scoped to a single tenant or process events for all tenants.

### Single-Tenant Projection

```python
from eventsource.projections import DeclarativeProjection, handles

class TenantOrderSummaryProjection(DeclarativeProjection):
    """Projection scoped to a specific tenant."""

    def __init__(self, tenant_id: UUID):
        """
        Create tenant-scoped projection.

        Args:
            tenant_id: Tenant to build summary for
        """
        super().__init__()
        self._target_tenant = tenant_id
        self._orders: dict[UUID, dict] = {}

    @handles(OrderPlaced)
    async def _on_order_placed(self, event: OrderPlaced) -> None:
        """Handle OrderPlaced events for this tenant only."""
        # Skip events from other tenants
        if event.tenant_id != self._target_tenant:
            return

        self._orders[event.aggregate_id] = {
            "order_id": event.aggregate_id,
            "customer_id": event.customer_id,
            "customer_name": event.customer_name,
            "total_amount": event.total_amount,
            "status": "placed",
        }

    @handles(OrderShipped)
    async def _on_order_shipped(self, event: OrderShipped) -> None:
        """Handle OrderShipped events for this tenant only."""
        if event.tenant_id != self._target_tenant:
            return

        if event.aggregate_id in self._orders:
            self._orders[event.aggregate_id]["status"] = "shipped"

    async def _truncate_read_models(self) -> None:
        """Clear projection data."""
        self._orders.clear()

    def get_orders(self) -> list[dict]:
        """Get all orders for this tenant."""
        return list(self._orders.values())

    def get_order_count(self) -> int:
        """Get total order count for this tenant."""
        return len(self._orders)
```

### Cross-Tenant Projection

```python
class GlobalOrderStatsProjection(DeclarativeProjection):
    """Projection that aggregates stats across all tenants."""

    def __init__(self):
        super().__init__()
        self._stats_by_tenant: dict[UUID, dict] = {}

    @handles(OrderPlaced)
    async def _on_order_placed(self, event: OrderPlaced) -> None:
        """Track order statistics per tenant."""
        tenant_id = event.tenant_id or uuid4()  # Handle null tenant

        if tenant_id not in self._stats_by_tenant:
            self._stats_by_tenant[tenant_id] = {
                "tenant_id": tenant_id,
                "order_count": 0,
                "total_revenue": 0.0,
            }

        stats = self._stats_by_tenant[tenant_id]
        stats["order_count"] += 1
        stats["total_revenue"] += event.total_amount

    async def _truncate_read_models(self) -> None:
        """Clear all stats."""
        self._stats_by_tenant.clear()

    def get_tenant_stats(self, tenant_id: UUID) -> dict | None:
        """Get statistics for a specific tenant."""
        return self._stats_by_tenant.get(tenant_id)

    def get_all_stats(self) -> list[dict]:
        """Get statistics for all tenants."""
        return list(self._stats_by_tenant.values())

    def get_top_tenants(self, limit: int = 10) -> list[dict]:
        """Get top tenants by revenue."""
        stats = list(self._stats_by_tenant.values())
        stats.sort(key=lambda x: x["total_revenue"], reverse=True)
        return stats[:limit]
```

**Pattern selection:**

- **Single-tenant**: Use when building tenant-specific dashboards
- **Cross-tenant**: Use for admin dashboards, analytics, billing
- **Hybrid**: Process all events but store per-tenant read models

---

## Tenant Context Propagation

In production applications, tenant context should flow through the request lifecycle automatically.

### Context Variables

```python
from contextvars import ContextVar
from uuid import UUID

# Thread-safe tenant context storage
_current_tenant: ContextVar[UUID | None] = ContextVar(
    "current_tenant",
    default=None,
)

def get_current_tenant() -> UUID | None:
    """Get the current tenant ID from context."""
    return _current_tenant.get()

def set_current_tenant(tenant_id: UUID | None) -> None:
    """Set the current tenant ID in context."""
    _current_tenant.set(tenant_id)

def require_tenant() -> UUID:
    """
    Get the current tenant ID, raising if not set.

    Raises:
        ValueError: If no tenant context is set
    """
    tenant_id = get_current_tenant()
    if tenant_id is None:
        raise ValueError("Tenant context is required but not set")
    return tenant_id
```

**Why ContextVar?**

- **Thread-safe**: Each async task has isolated context
- **Propagates**: Automatically flows through `await` calls
- **Clean**: No need to pass tenant_id through every function

### FastAPI Integration

```python
from fastapi import FastAPI, Request, Depends, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware

class TenantMiddleware(BaseHTTPMiddleware):
    """Extract tenant from request and set in context."""

    async def dispatch(self, request: Request, call_next):
        """Process request with tenant context."""
        # Extract tenant from header
        tenant_header = request.headers.get("X-Tenant-ID")

        if tenant_header:
            try:
                tenant_id = UUID(tenant_header)
                set_current_tenant(tenant_id)
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid X-Tenant-ID header format",
                )

        try:
            response = await call_next(request)
            return response
        finally:
            # Clear context after request
            set_current_tenant(None)

# Application setup
app = FastAPI()
app.add_middleware(TenantMiddleware)

# Dependency for requiring tenant
async def get_tenant_id() -> UUID:
    """Get tenant ID from context or raise 401."""
    tenant_id = get_current_tenant()
    if not tenant_id:
        raise HTTPException(
            status_code=401,
            detail="Tenant context required (missing X-Tenant-ID header)",
        )
    return tenant_id

# Dependency for tenant-scoped repository
async def get_order_repository(
    tenant_id: UUID = Depends(get_tenant_id),
) -> TenantOrderRepository:
    """Get tenant-scoped order repository."""
    event_store = app.state.event_store  # Assume stored in app state
    return TenantOrderRepository(event_store, tenant_id)

# Routes automatically get tenant context
@app.post("/orders")
async def create_order(
    customer_id: UUID,
    customer_name: str,
    total_amount: float,
    repo: TenantOrderRepository = Depends(get_order_repository),
):
    """Create order for authenticated tenant."""
    order_id = uuid4()
    order = repo.create_new(order_id)
    order.place_order(customer_id, customer_name, total_amount)
    await repo.save(order)

    return {
        "order_id": str(order_id),
        "tenant_id": str(repo.tenant_id),
    }

@app.get("/orders/{order_id}")
async def get_order(
    order_id: UUID,
    repo: TenantOrderRepository = Depends(get_order_repository),
):
    """Get order for authenticated tenant."""
    order = await repo.load(order_id)

    return {
        "order_id": str(order.aggregate_id),
        "customer_name": order.state.customer_name,
        "total_amount": order.state.total_amount,
        "status": order.state.status,
    }
```

**Security flow:**

1. Client sends `X-Tenant-ID` header (from auth token)
2. Middleware validates and sets in context
3. Dependencies retrieve tenant from context
4. Repository enforces tenant boundaries
5. Context cleared after request completes

---

## Security Best Practices

Multi-tenancy requires careful security implementation to prevent data leaks.

### Never Trust Client Input

```python
# WRONG - Client controls tenant_id
@app.post("/orders/unsafe")
async def create_order_unsafe(request: dict):
    # Client can specify any tenant!
    tenant_id = UUID(request.get("tenant_id"))  # DANGEROUS!
    repo = TenantOrderRepository(event_store, tenant_id)
    # Client can now access any tenant's data

# CORRECT - Tenant from authentication
@app.post("/orders/safe")
async def create_order_safe(
    customer_name: str,
    total_amount: float,
    tenant_id: UUID = Depends(get_tenant_id),  # From auth token
):
    # Tenant verified by authentication middleware
    repo = TenantOrderRepository(event_store, tenant_id)
    # Can only access authenticated tenant's data
```

### Always Validate After Loading

```python
async def get_order_safe(
    order_id: UUID,
    tenant_id: UUID,
) -> OrderAggregate:
    """Load order with tenant validation."""
    # Load from event store
    order = await base_repo.load(order_id)

    # Verify tenant ownership
    if order.tenant_id != tenant_id:
        logger.warning(
            "Tenant mismatch",
            order_id=str(order_id),
            expected_tenant=str(tenant_id),
            actual_tenant=str(order.tenant_id),
        )
        raise PermissionError("Access denied")

    return order
```

### Log Security Events

```python
import logging

logger = logging.getLogger(__name__)

async def load_with_audit(
    order_id: UUID,
    tenant_id: UUID,
) -> OrderAggregate:
    """Load order with security audit logging."""
    try:
        order = await repo.load(order_id)

        # Log successful access
        logger.info(
            "Order loaded",
            order_id=str(order_id),
            tenant_id=str(tenant_id),
        )

        return order

    except PermissionError:
        # Log security violation
        logger.warning(
            "Cross-tenant access attempt blocked",
            order_id=str(order_id),
            requested_by_tenant=str(tenant_id),
        )
        raise
```

### Defense in Depth

```python
class SecureTenantRepository:
    """Repository with multiple security layers."""

    def __init__(self, event_store: EventStore, tenant_id: UUID):
        self._event_store = event_store
        self._tenant_id = tenant_id
        self._access_log = []

    async def load(self, order_id: UUID) -> OrderAggregate:
        """Load with multi-layer security."""
        # Layer 1: Check access log for suspicious patterns
        if self._is_suspicious_access(order_id):
            logger.warning("Suspicious access pattern detected")
            raise PermissionError("Access denied")

        # Layer 2: Load aggregate
        order = await self._load_from_store(order_id)

        # Layer 3: Verify tenant ownership
        if order.tenant_id != self._tenant_id:
            logger.error("Tenant mismatch after load")
            raise PermissionError("Access denied")

        # Layer 4: Log successful access
        self._record_access(order_id)

        return order

    def _is_suspicious_access(self, order_id: UUID) -> bool:
        """Detect suspicious access patterns."""
        # Example: Too many failed attempts
        recent_failures = [
            access for access in self._access_log[-10:]
            if access["order_id"] == order_id and not access["success"]
        ]
        return len(recent_failures) > 3

    async def _load_from_store(self, order_id: UUID) -> OrderAggregate:
        """Load aggregate from event store."""
        # Implementation details...
        pass

    def _record_access(self, order_id: UUID, success: bool = True):
        """Record access attempt for audit."""
        self._access_log.append({
            "order_id": order_id,
            "tenant_id": self._tenant_id,
            "timestamp": datetime.now(UTC),
            "success": success,
        })
```

---

## Testing Multi-Tenant Applications

Comprehensive testing is essential to verify tenant isolation.

### Basic Isolation Test

```python
import pytest
from uuid import uuid4

@pytest.mark.asyncio
async def test_tenant_isolation():
    """Verify tenants cannot access each other's data."""
    event_store = InMemoryEventStore()

    # Create two tenants
    tenant_acme = uuid4()
    tenant_globex = uuid4()

    repo_acme = TenantOrderRepository(event_store, tenant_acme)
    repo_globex = TenantOrderRepository(event_store, tenant_globex)

    # Tenant A creates order
    order_id = uuid4()
    order = repo_acme.create_new(order_id)
    order.place_order(uuid4(), "Alice", 100.0)
    await repo_acme.save(order)

    # Tenant B cannot access Tenant A's order
    with pytest.raises(PermissionError, match="different tenant"):
        await repo_globex.load(order_id)

@pytest.mark.asyncio
async def test_cross_tenant_save_blocked():
    """Verify cannot save aggregate from different tenant."""
    event_store = InMemoryEventStore()

    tenant_acme = uuid4()
    tenant_globex = uuid4()

    repo_acme = TenantOrderRepository(event_store, tenant_acme)
    repo_globex = TenantOrderRepository(event_store, tenant_globex)

    # Create order with Tenant A
    order = repo_acme.create_new(uuid4())
    order.place_order(uuid4(), "Alice", 100.0)
    await repo_acme.save(order)

    # Load with Tenant A
    loaded = await repo_acme.load(order.aggregate_id)

    # Cannot save via Tenant B's repository
    with pytest.raises(PermissionError, match="Cannot save order"):
        await repo_globex.save(loaded)
```

### Projection Isolation Test

```python
@pytest.mark.asyncio
async def test_projection_tenant_filtering():
    """Verify projections correctly filter by tenant."""
    event_store = InMemoryEventStore()

    tenant_acme = uuid4()
    tenant_globex = uuid4()

    # Create orders for both tenants
    repo_acme = TenantOrderRepository(event_store, tenant_acme)
    repo_globex = TenantOrderRepository(event_store, tenant_globex)

    for i in range(3):
        order = repo_acme.create_new(uuid4())
        order.place_order(uuid4(), f"Customer {i}", 100.0)
        await repo_acme.save(order)

    for i in range(2):
        order = repo_globex.create_new(uuid4())
        order.place_order(uuid4(), f"Customer {i}", 200.0)
        await repo_globex.save(order)

    # ACME projection should only see 3 orders
    projection_acme = TenantOrderSummaryProjection(tenant_acme)
    async for stored in event_store.read_all():
        await projection_acme.handle(stored.event)

    assert projection_acme.get_order_count() == 3

    # Globex projection should only see 2 orders
    projection_globex = TenantOrderSummaryProjection(tenant_globex)
    async for stored in event_store.read_all():
        await projection_globex.handle(stored.event)

    assert projection_globex.get_order_count() == 2
```

### Query Filtering Test

```python
@pytest.mark.asyncio
async def test_event_store_tenant_filtering():
    """Verify event store filters events by tenant."""
    event_store = InMemoryEventStore()

    tenant_acme = uuid4()
    tenant_globex = uuid4()

    # Create events for both tenants
    repo_acme = TenantOrderRepository(event_store, tenant_acme)
    repo_globex = TenantOrderRepository(event_store, tenant_globex)

    # 5 ACME orders
    for _ in range(5):
        order = repo_acme.create_new(uuid4())
        order.place_order(uuid4(), "Customer", 100.0)
        await repo_acme.save(order)

    # 3 Globex orders
    for _ in range(3):
        order = repo_globex.create_new(uuid4())
        order.place_order(uuid4(), "Customer", 100.0)
        await repo_globex.save(order)

    # Query by tenant
    acme_events = await event_store.get_events_by_type(
        "Order",
        tenant_id=tenant_acme,
    )
    globex_events = await event_store.get_events_by_type(
        "Order",
        tenant_id=tenant_globex,
    )

    assert len(acme_events) == 5
    assert len(globex_events) == 3

    # All events have correct tenant
    assert all(e.tenant_id == tenant_acme for e in acme_events)
    assert all(e.tenant_id == tenant_globex for e in globex_events)
```

---

## Complete Working Example

Here's a runnable example demonstrating all multi-tenancy concepts:

```python
"""
Tutorial 16: Multi-Tenancy
Run with: python tutorial_16_multi_tenancy.py
"""

import asyncio
from uuid import UUID, uuid4

from pydantic import BaseModel

from eventsource import (
    AggregateRepository,
    AggregateRoot,
    DomainEvent,
    InMemoryEventStore,
    register_event,
)
from eventsource.projections import DeclarativeProjection, handles


# =============================================================================
# Events
# =============================================================================


@register_event
class OrderPlaced(DomainEvent):
    event_type: str = "OrderPlaced"
    aggregate_type: str = "Order"

    customer_id: UUID
    customer_name: str
    total_amount: float


@register_event
class OrderShipped(DomainEvent):
    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"

    tracking_number: str


# =============================================================================
# State
# =============================================================================


class OrderState(BaseModel):
    order_id: UUID
    tenant_id: UUID | None = None
    customer_id: UUID | None = None
    customer_name: str = ""
    total_amount: float = 0.0
    status: str = "draft"
    tracking_number: str | None = None


# =============================================================================
# Aggregate
# =============================================================================


class OrderAggregate(AggregateRoot[OrderState]):
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
        if isinstance(event, OrderPlaced):
            self._state = OrderState(
                order_id=self.aggregate_id,
                tenant_id=event.tenant_id,
                customer_id=event.customer_id,
                customer_name=event.customer_name,
                total_amount=event.total_amount,
                status="placed",
            )
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

    def place_order(
        self, customer_id: UUID, customer_name: str, total_amount: float
    ) -> None:
        if self.version > 0:
            raise ValueError("Order already placed")
        if not self._tenant_id:
            raise ValueError("Tenant context required")

        self.apply_event(
            OrderPlaced(
                aggregate_id=self.aggregate_id,
                tenant_id=self._tenant_id,
                customer_id=customer_id,
                customer_name=customer_name,
                total_amount=total_amount,
                aggregate_version=self.get_next_version(),
            )
        )

    def ship(self, tracking_number: str) -> None:
        if not self._state or self._state.status != "placed":
            raise ValueError("Order must be placed before shipping")

        self.apply_event(
            OrderShipped(
                aggregate_id=self.aggregate_id,
                tenant_id=self._tenant_id,
                tracking_number=tracking_number,
                aggregate_version=self.get_next_version(),
            )
        )


# =============================================================================
# Repository
# =============================================================================


class TenantOrderRepository:
    """Repository with tenant boundary enforcement."""

    def __init__(self, event_store: InMemoryEventStore, tenant_id: UUID):
        self._tenant_id = tenant_id
        self._inner_repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=lambda aid: OrderAggregate(aid, tenant_id),
            aggregate_type="Order",
        )

    @property
    def tenant_id(self) -> UUID:
        return self._tenant_id

    def create_new(self, order_id: UUID) -> OrderAggregate:
        return OrderAggregate(order_id, self._tenant_id)

    async def save(self, order: OrderAggregate) -> None:
        if order.tenant_id != self._tenant_id:
            raise PermissionError("Cannot save order from different tenant")
        await self._inner_repo.save(order)

    async def load(self, order_id: UUID) -> OrderAggregate:
        order = await self._inner_repo.load(order_id)
        if order.tenant_id != self._tenant_id:
            raise PermissionError("Order belongs to different tenant")
        return order


# =============================================================================
# Projections
# =============================================================================


class TenantOrderProjection(DeclarativeProjection):
    """Single-tenant projection."""

    def __init__(self, tenant_id: UUID):
        super().__init__()
        self._target_tenant = tenant_id
        self._orders: dict[UUID, dict] = {}

    @handles(OrderPlaced)
    async def _on_order_placed(self, event: OrderPlaced) -> None:
        if event.tenant_id != self._target_tenant:
            return

        self._orders[event.aggregate_id] = {
            "order_id": event.aggregate_id,
            "customer_name": event.customer_name,
            "total_amount": event.total_amount,
            "status": "placed",
        }

    @handles(OrderShipped)
    async def _on_order_shipped(self, event: OrderShipped) -> None:
        if event.tenant_id != self._target_tenant:
            return

        if event.aggregate_id in self._orders:
            self._orders[event.aggregate_id]["status"] = "shipped"

    async def _truncate_read_models(self) -> None:
        self._orders.clear()

    def get_orders(self) -> list[dict]:
        return list(self._orders.values())


class GlobalStatsProjection(DeclarativeProjection):
    """Cross-tenant projection for analytics."""

    def __init__(self):
        super().__init__()
        self._stats: dict[UUID, dict] = {}

    @handles(OrderPlaced)
    async def _on_order_placed(self, event: OrderPlaced) -> None:
        tenant_id = event.tenant_id or uuid4()

        if tenant_id not in self._stats:
            self._stats[tenant_id] = {
                "tenant_id": tenant_id,
                "order_count": 0,
                "total_revenue": 0.0,
            }

        self._stats[tenant_id]["order_count"] += 1
        self._stats[tenant_id]["total_revenue"] += event.total_amount

    async def _truncate_read_models(self) -> None:
        self._stats.clear()

    def get_stats(self, tenant_id: UUID) -> dict | None:
        return self._stats.get(tenant_id)

    def get_all_stats(self) -> list[dict]:
        return list(self._stats.values())


# =============================================================================
# Demo
# =============================================================================


async def main():
    print("=" * 70)
    print("Tutorial 16: Multi-Tenancy")
    print("=" * 70)

    # Setup
    event_store = InMemoryEventStore()

    # Create tenants
    print("\n1. Creating tenant contexts")
    print("-" * 70)
    tenant_acme = uuid4()
    tenant_globex = uuid4()
    print(f"   ACME Corp:    {tenant_acme}")
    print(f"   Globex Inc:   {tenant_globex}")

    # Create tenant-scoped repositories
    print("\n2. Creating tenant-scoped repositories")
    print("-" * 70)
    repo_acme = TenantOrderRepository(event_store, tenant_acme)
    repo_globex = TenantOrderRepository(event_store, tenant_globex)
    print("   - ACME repository created")
    print("   - Globex repository created")

    # Create orders for ACME
    print("\n3. Creating orders for ACME Corp")
    print("-" * 70)
    acme_orders = []
    for i in range(3):
        order_id = uuid4()
        order = repo_acme.create_new(order_id)
        order.place_order(
            customer_id=uuid4(),
            customer_name=f"ACME Customer {i+1}",
            total_amount=100.0 * (i + 1),
        )
        await repo_acme.save(order)
        acme_orders.append(order_id)
        print(f"   Order {i+1}: ${order.state.total_amount:.2f}")

    # Ship one ACME order
    order = await repo_acme.load(acme_orders[0])
    order.ship("ACME-TRACK-001")
    await repo_acme.save(order)
    print(f"   Shipped order 1")

    # Create orders for Globex
    print("\n4. Creating orders for Globex Inc")
    print("-" * 70)
    globex_orders = []
    for i in range(2):
        order_id = uuid4()
        order = repo_globex.create_new(order_id)
        order.place_order(
            customer_id=uuid4(),
            customer_name=f"Globex Customer {i+1}",
            total_amount=500.0 * (i + 1),
        )
        await repo_globex.save(order)
        globex_orders.append(order_id)
        print(f"   Order {i+1}: ${order.state.total_amount:.2f}")

    # Test tenant isolation
    print("\n5. Testing tenant isolation")
    print("-" * 70)
    try:
        # Try to load ACME order via Globex repository
        await repo_globex.load(acme_orders[0])
        print("   ERROR: Cross-tenant access allowed!")
    except PermissionError as e:
        print(f"   SUCCESS: Cross-tenant access blocked")
        print(f"   Message: {e}")

    # Query events by tenant
    print("\n6. Querying events by tenant")
    print("-" * 70)
    acme_events = await event_store.get_events_by_type(
        "Order", tenant_id=tenant_acme
    )
    globex_events = await event_store.get_events_by_type(
        "Order", tenant_id=tenant_globex
    )
    print(f"   ACME events:   {len(acme_events)}")
    print(f"   Globex events: {len(globex_events)}")

    # Build tenant-scoped projections
    print("\n7. Building tenant-scoped projections")
    print("-" * 70)
    projection_acme = TenantOrderProjection(tenant_acme)
    projection_globex = TenantOrderProjection(tenant_globex)

    async for stored in event_store.read_all():
        await projection_acme.handle(stored.event)
        await projection_globex.handle(stored.event)

    acme_orders_list = projection_acme.get_orders()
    globex_orders_list = projection_globex.get_orders()

    print(f"   ACME projection:   {len(acme_orders_list)} orders")
    print(f"   Globex projection: {len(globex_orders_list)} orders")

    # Build cross-tenant projection
    print("\n8. Building cross-tenant analytics")
    print("-" * 70)
    global_stats = GlobalStatsProjection()

    async for stored in event_store.read_all():
        await global_stats.handle(stored.event)

    print("\n   Tenant Statistics:")
    for stats in global_stats.get_all_stats():
        tenant_name = "ACME Corp" if stats["tenant_id"] == tenant_acme else "Globex Inc"
        print(f"   {tenant_name}:")
        print(f"      Orders:  {stats['order_count']}")
        print(f"      Revenue: ${stats['total_revenue']:.2f}")

    # Summary
    print("\n9. Summary")
    print("-" * 70)
    print(f"   Total tenants: 2")
    print(f"   Total orders: {len(acme_events) + len(globex_events)}")
    print(f"   Isolation verified: Yes")
    print(f"   Cross-tenant queries: Working")

    print("\n" + "=" * 70)
    print("Tutorial complete!")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
```

**Expected output:**

```
======================================================================
Tutorial 16: Multi-Tenancy
======================================================================

1. Creating tenant contexts
----------------------------------------------------------------------
   ACME Corp:    [UUID]
   Globex Inc:   [UUID]

2. Creating tenant-scoped repositories
----------------------------------------------------------------------
   - ACME repository created
   - Globex repository created

3. Creating orders for ACME Corp
----------------------------------------------------------------------
   Order 1: $100.00
   Order 2: $200.00
   Order 3: $300.00
   Shipped order 1

4. Creating orders for Globex Inc
----------------------------------------------------------------------
   Order 1: $500.00
   Order 2: $1000.00

5. Testing tenant isolation
----------------------------------------------------------------------
   SUCCESS: Cross-tenant access blocked
   Message: Order belongs to different tenant

6. Querying events by tenant
----------------------------------------------------------------------
   ACME events:   4
   Globex events: 2

7. Building tenant-scoped projections
----------------------------------------------------------------------
   ACME projection:   3 orders
   Globex projection: 2 orders

8. Building cross-tenant analytics
----------------------------------------------------------------------

   Tenant Statistics:
   ACME Corp:
      Orders:  3
      Revenue: $600.00
   Globex Inc:
      Orders:  2
      Revenue: $1500.00

9. Summary
----------------------------------------------------------------------
   Total tenants: 2
   Total orders: 6
   Isolation verified: Yes
   Cross-tenant queries: Working

======================================================================
Tutorial complete!
======================================================================
```

---

## Key Takeaways

1. **tenant_id is built-in**: All events have optional `tenant_id` field, automatically indexed
2. **Application-level isolation**: Shared database with filtering, not separate databases
3. **Repository enforcement**: Tenant-scoped repositories prevent cross-tenant access
4. **Event store filtering**: All stores support efficient tenant filtering via `tenant_id`
5. **Projection flexibility**: Build single-tenant or cross-tenant projections as needed
6. **Context propagation**: Use `ContextVar` for clean tenant context flow
7. **Security first**: Never trust client input, always validate after loading
8. **Test thoroughly**: Verify tenant isolation with comprehensive tests
9. **Defense in depth**: Multiple validation layers prevent data leaks
10. **Production ready**: Built-in support in all event stores and projections

---

## Common Patterns

### Admin Operations Across Tenants

```python
async def admin_get_all_tenants(event_store: EventStore) -> set[UUID]:
    """Get list of all tenant IDs (admin only)."""
    tenants = set()
    async for stored in event_store.read_all():
        if stored.event.tenant_id:
            tenants.add(stored.event.tenant_id)
    return tenants
```

### Tenant Migration

```python
async def migrate_tenant(
    old_tenant_id: UUID,
    new_tenant_id: UUID,
    event_store: EventStore,
):
    """Migrate events from one tenant to another (admin only)."""
    # Note: This is conceptual - actual implementation would need
    # special migration tools since events are immutable
    options = ReadOptions(tenant_id=old_tenant_id)
    async for stored in event_store.read_all(options):
        # Create new event with different tenant_id
        # This requires careful handling and is usually done
        # via data migration scripts, not runtime code
        pass
```

### Tenant-Specific Configuration

```python
class TenantConfig(BaseModel):
    tenant_id: UUID
    max_orders_per_day: int
    features_enabled: list[str]

class TenantOrderRepository:
    def __init__(
        self,
        event_store: EventStore,
        tenant_id: UUID,
        config: TenantConfig,
    ):
        self._tenant_id = tenant_id
        self._config = config
        # ... repository setup

    async def save(self, order: OrderAggregate) -> None:
        # Check tenant-specific limits
        if await self._exceeds_daily_limit():
            raise ValueError("Daily order limit exceeded")
        await super().save(order)
```

---

## Next Steps

Now that you understand multi-tenancy patterns, you're ready to learn about distributed event propagation.

Continue to [Tutorial 17: Redis Event Bus](17-redis.md) to learn about:

- Distributing events across services with Redis Streams
- Implementing producer-consumer patterns with tenant context
- Scaling projections horizontally while maintaining tenant isolation
- Building real-time event-driven applications with multi-tenancy

For more examples, see:
- `examples/multi_tenant/` - Complete multi-tenant application
- `tests/integration/test_multi_tenancy.py` - Comprehensive test suite
- `docs/guides/multi-tenancy.md` - Advanced multi-tenancy patterns
