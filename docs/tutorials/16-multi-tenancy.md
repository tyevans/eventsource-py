# Tutorial 16: Multi-Tenancy Implementation

**Difficulty:** Advanced
**Progress:** Tutorial 16 of 21 | Phase 4: Advanced Patterns

---

## Overview

Multi-tenancy enables multiple customers (tenants) to share infrastructure with strict data isolation. Prerequisites: [Tutorial 15](15-production.md), [Tutorial 5](05-repositories.md), [Tutorial 6](06-projections.md).

eventsource uses **shared database with application-level isolation** via the built-in `tenant_id` field.

---

## The tenant_id Field

All events inherit `tenant_id` - automatically persisted and indexed:

```python
class DomainEvent(BaseModel):
    tenant_id: UUID | None = Field(default=None, description="Tenant this event belongs to")
```

---

## Creating Tenant-Aware Events

```python
from uuid import UUID
from eventsource import DomainEvent, register_event

@register_event
class OrderCreated(DomainEvent):
    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    customer_name: str
    total_amount: float
    # tenant_id inherited automatically

@register_event
class OrderShipped(DomainEvent):
    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"
    tracking_number: str
```

```python
from uuid import uuid4

tenant_id = uuid4()  # Typically from authentication context
order_id = uuid4()

event = OrderCreated(
    aggregate_id=order_id,
    tenant_id=tenant_id,
    customer_name="Alice",
    total_amount=99.99,
    aggregate_version=1,
)
```

---

## Tenant-Aware Aggregates

```python
class OrderState(BaseModel):
    order_id: UUID
    tenant_id: UUID | None = None
    customer_name: str = ""
    total_amount: float = 0.0
    status: str = "draft"

class TenantOrderAggregate(AggregateRoot[OrderState]):
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
                order_id=self.aggregate_id, tenant_id=event.tenant_id,
                customer_name=event.customer_name, total_amount=event.total_amount, status="created"
            )
            if event.tenant_id:
                self._tenant_id = event.tenant_id
        elif isinstance(event, OrderShipped) and self._state:
            self._state = self._state.model_copy(update={"status": "shipped"})

    def create(self, customer_name: str, total_amount: float) -> None:
        if self.version > 0 or not self._tenant_id:
            raise ValueError("Order already created or tenant context required")
        self.apply_event(OrderCreated(
            aggregate_id=self.aggregate_id, tenant_id=self._tenant_id,
            customer_name=customer_name, total_amount=total_amount,
            aggregate_version=self.get_next_version(),
        ))

    def ship(self, tracking_number: str) -> None:
        if not self.state or self.state.status != "created":
            raise ValueError("Order must be created before shipping")
        self.apply_event(OrderShipped(
            aggregate_id=self.aggregate_id, tenant_id=self._tenant_id,
            tracking_number=tracking_number, aggregate_version=self.get_next_version(),
        ))
```

---

## Tenant-Aware Repository

```python
class TenantAwareOrderRepository:
    def __init__(self, event_store: EventStore, tenant_id: UUID):
        self._tenant_id = tenant_id
        self._inner_repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=lambda aid: TenantOrderAggregate(aid, tenant_id),
            aggregate_type="Order",
        )

    def create_new(self, order_id: UUID) -> TenantOrderAggregate:
        return TenantOrderAggregate(order_id, self._tenant_id)

    async def save(self, order: TenantOrderAggregate) -> None:
        if order.tenant_id and order.tenant_id != self._tenant_id:
            raise PermissionError(f"Cannot save order for different tenant")
        await self._inner_repo.save(order)

    async def load(self, order_id: UUID) -> TenantOrderAggregate:
        order = await self._inner_repo.load(order_id)
        if order.tenant_id and order.tenant_id != self._tenant_id:
            raise PermissionError(f"Order belongs to different tenant")
        return order
```

---

## Querying Events by Tenant

```python
async def get_tenant_orders(event_store: InMemoryEventStore, tenant_id: UUID):
    return await event_store.get_events_by_type(aggregate_type="Order", tenant_id=tenant_id)

async def get_recent_tenant_orders(event_store: InMemoryEventStore, tenant_id: UUID, hours: int = 24):
    since = datetime.now(UTC) - timedelta(hours=hours)
    return await event_store.get_events_by_type(
        aggregate_type="Order", tenant_id=tenant_id, from_timestamp=since
    )
```

---

## Tenant-Scoped Projections

```python
class TenantOrderSummaryProjection(DeclarativeProjection):
    def __init__(self, tenant_id: UUID):
        super().__init__()
        self._target_tenant = tenant_id
        self._orders: dict[str, dict] = {}

    @handles(OrderCreated)
    async def _on_order_created(self, event: OrderCreated) -> None:
        if event.tenant_id != self._target_tenant:
            return
        self._orders[str(event.aggregate_id)] = {
            "order_id": str(event.aggregate_id), "customer": event.customer_name,
            "total": event.total_amount, "status": "created",
        }

    @handles(OrderShipped)
    async def _on_order_shipped(self, event: OrderShipped) -> None:
        if event.tenant_id != self._target_tenant:
            return
        order_id = str(event.aggregate_id)
        if order_id in self._orders:
            self._orders[order_id]["status"] = "shipped"

    async def _truncate_read_models(self) -> None:
        self._orders.clear()
```

Global projection:

```python
class GlobalOrderStatsProjection(DeclarativeProjection):
    def __init__(self):
        super().__init__()
        self._tenant_stats: dict[str, dict] = {}

    @handles(OrderCreated)
    async def _on_order_created(self, event: OrderCreated) -> None:
        tenant_key = str(event.tenant_id) if event.tenant_id else "no-tenant"
        if tenant_key not in self._tenant_stats:
            self._tenant_stats[tenant_key] = {"order_count": 0, "total_revenue": 0.0}
        self._tenant_stats[tenant_key]["order_count"] += 1
        self._tenant_stats[tenant_key]["total_revenue"] += event.total_amount

    async def _truncate_read_models(self) -> None:
        self._tenant_stats.clear()
```

---

## Tenant Context Propagation

```python
from contextvars import ContextVar

_current_tenant: ContextVar[UUID | None] = ContextVar("current_tenant", default=None)

def get_current_tenant() -> UUID | None:
    return _current_tenant.get()

def set_current_tenant(tenant_id: UUID | None) -> None:
    _current_tenant.set(tenant_id)

def require_tenant() -> UUID:
    tenant_id = _current_tenant.get()
    if not tenant_id:
        raise ValueError("Tenant context not set")
    return tenant_id
```

FastAPI middleware and dependency injection:

```python
class TenantMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        tenant_header = request.headers.get("X-Tenant-ID")
        if tenant_header:
            try:
                set_current_tenant(UUID(tenant_header))
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid tenant ID")
        try:
            return await call_next(request)
        finally:
            set_current_tenant(None)

app = FastAPI()
app.add_middleware(TenantMiddleware)

async def get_tenant_id() -> UUID:
    tenant_id = get_current_tenant()
    if not tenant_id:
        raise HTTPException(status_code=401, detail="Tenant context required")
    return tenant_id

async def get_order_repository(tenant_id: UUID = Depends(get_tenant_id)):
    return TenantAwareOrderRepository(event_store=event_store, tenant_id=tenant_id)

@app.post("/orders")
async def create_order(customer_name: str, total_amount: float,
                       repo: TenantAwareOrderRepository = Depends(get_order_repository)):
    order_id = uuid4()
    order = repo.create_new(order_id)
    order.create(customer_name=customer_name, total_amount=total_amount)
    await repo.save(order)
    return {"order_id": str(order_id)}
```

---

## Testing Multi-Tenant Applications

```python
@pytest.mark.asyncio
async def test_tenant_isolation(repo_a, repo_b):
    order_a = repo_a.create_new(uuid4())
    order_a.create("Alice", 100.0)
    await repo_a.save(order_a)

    with pytest.raises(PermissionError):
        await repo_b.load(order_a.aggregate_id)
```

---

## Security Best Practices

```python
# WRONG - never trust client-provided tenant IDs
@app.post("/orders/unsafe")
async def create_order_unsafe(request: dict):
    tenant_id = request.get("tenant_id")  # Client could set any tenant!

# CORRECT - get from auth token
@app.post("/orders/safe")
async def create_order_safe(request: Request, tenant_id: UUID = Depends(get_tenant_id)):
    pass  # tenant_id verified from auth

# Always validate after loading
async def get_order(order_id: UUID, tenant_id: UUID):
    order = await inner_repo.load(order_id)
    if order.tenant_id != tenant_id:
        logger.warning("Cross-tenant access attempt")
        raise PermissionError("Access denied")
    return order
```

---

## Complete Example

```python
"""Tutorial 16: Multi-Tenancy - Run with: python tutorial_16_multi_tenancy.py"""
import asyncio
from uuid import UUID, uuid4
from pydantic import BaseModel
from eventsource import (
    DomainEvent, register_event, AggregateRoot, InMemoryEventStore,
    AggregateRepository, DeclarativeProjection, handles,
)

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

class OrderState(BaseModel):
    order_id: UUID
    tenant_id: UUID | None = None
    customer_name: str = ""
    total_amount: float = 0.0
    status: str = "draft"
    tracking_number: str | None = None

class TenantOrderAggregate(AggregateRoot[OrderState]):
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
                order_id=self.aggregate_id, tenant_id=event.tenant_id,
                customer_name=event.customer_name, total_amount=event.total_amount, status="created"
            )
            if event.tenant_id:
                self._tenant_id = event.tenant_id
        elif isinstance(event, OrderShipped) and self._state:
            self._state = self._state.model_copy(update={
                "status": "shipped", "tracking_number": event.tracking_number,
            })

    def create(self, customer_name: str, total_amount: float) -> None:
        if self.version > 0:
            raise ValueError("Order already exists")
        if not self._tenant_id:
            raise ValueError("Tenant context required")
        self.apply_event(OrderCreated(
            aggregate_id=self.aggregate_id, tenant_id=self._tenant_id,
            customer_name=customer_name, total_amount=total_amount,
            aggregate_version=self.get_next_version(),
        ))

    def ship(self, tracking_number: str) -> None:
        if not self._state or self._state.status != "created":
            raise ValueError("Cannot ship order in current state")
        self.apply_event(OrderShipped(
            aggregate_id=self.aggregate_id, tenant_id=self._tenant_id,
            tracking_number=tracking_number, aggregate_version=self.get_next_version(),
        ))

class TenantAwareOrderRepository:
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

class TenantOrderProjection(DeclarativeProjection):
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
            "order_id": str(event.aggregate_id), "customer": event.customer_name,
            "total": event.total_amount, "status": "created",
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

async def main():
    event_store = InMemoryEventStore()
    tenant_acme = uuid4()
    tenant_globex = uuid4()

    repo_acme = TenantAwareOrderRepository(event_store, tenant_acme)
    repo_globex = TenantAwareOrderRepository(event_store, tenant_globex)

    # Create orders
    order_1 = repo_acme.create_new(uuid4())
    order_1.create("Alice (ACME)", 100.0)
    await repo_acme.save(order_1)

    order_2 = repo_acme.create_new(uuid4())
    order_2.create("Bob (ACME)", 250.0)
    await repo_acme.save(order_2)

    order_3 = repo_globex.create_new(uuid4())
    order_3.create("Charlie (Globex)", 500.0)
    await repo_globex.save(order_3)

    loaded = await repo_acme.load(order_1.aggregate_id)
    loaded.ship("TRACK-ACME-001")
    await repo_acme.save(loaded)

    # Test isolation
    try:
        await repo_globex.load(order_1.aggregate_id)
    except PermissionError as e:
        print(f"Correctly blocked: {e}")

    # Projections
    global_projection = TenantOrderProjection()
    acme_projection = TenantOrderProjection(tenant_id=tenant_acme)

    async for stored in event_store.read_all():
        await global_projection.handle(stored.event)
        await acme_projection.handle(stored.event)

    print(f"ACME orders: {len(global_projection.get_tenant_orders(tenant_acme))}")
    print(f"Globex orders: {len(global_projection.get_tenant_orders(tenant_globex))}")


if __name__ == "__main__":
    asyncio.run(main())
```

---

## Next Steps

Continue to [Tutorial 17: Using Redis Event Bus](17-redis.md) to learn distributed event propagation with tenant context.
