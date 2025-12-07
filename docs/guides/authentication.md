# Authentication and Audit Trails

This guide explains how to integrate authentication with eventsource and build audit trails using the built-in actor tracking. It covers patterns for capturing user identity in events, integrating with authentication systems, and building comprehensive audit logs.

## Overview

Every domain event in eventsource can track who (or what) triggered it via the `actor_id` field. Combined with `correlation_id`, `causation_id`, and the flexible `metadata` dictionary, you have everything needed to build robust audit trails and integrate with any authentication system.

### Why Actor Tracking Matters

Tracking actors in events enables:

- **Audit trails**: Complete history of who did what, when
- **User activity tracking**: Build activity feeds and usage analytics
- **Security forensics**: Investigate incidents with full context
- **Compliance requirements**: GDPR, SOX, HIPAA audit requirements
- **Debugging**: Understand the full context of any action

### Key Fields for Authentication

```python
# From eventsource/events/base.py
class DomainEvent(BaseModel):
    # Actor (who caused the event)
    actor_id: str | None = Field(
        default=None,
        description="User/system that triggered this event",
    )

    # Correlation for tracing across aggregates
    correlation_id: UUID = Field(
        default_factory=uuid4,
        description="ID linking related events across aggregates",
    )

    # Causation for event chains
    causation_id: UUID | None = Field(
        default=None,
        description="ID of the event that caused this event",
    )

    # Flexible metadata dictionary
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional event metadata",
    )
```

---

## The actor_id Field

The `actor_id` field is a string that identifies the user, system, or process that triggered an event. Using a string (rather than UUID) provides flexibility for different actor formats.

### Recommended Actor ID Formats

Establish consistent naming conventions across your system:

```python
# Human users (from authentication)
actor_id = f"user:{user_id}"        # user:550e8400-e29b-41d4-a716-446655440000
actor_id = f"user:{username}"       # user:jane.doe

# Service accounts
actor_id = f"service:{service_name}"  # service:payment-processor
actor_id = f"api:{api_key_id}"        # api:key_abc123

# System processes
actor_id = f"system:{process_name}"   # system:daily-cleanup-job
actor_id = f"cron:{job_name}"         # cron:expire-sessions

# External integrations
actor_id = f"webhook:{source}"        # webhook:stripe
actor_id = f"import:{source}"         # import:csv-batch-42

# Anonymous/unauthenticated
actor_id = None                       # Optional field
actor_id = "anonymous"                # Or explicit anonymous marker
```

### Setting actor_id on Events

The simplest approach is to pass `actor_id` when creating events:

```python
from uuid import uuid4
from eventsource import DomainEvent, register_event

@register_event
class OrderCreated(DomainEvent):
    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    customer_id: UUID
    total_amount: float

# Create event with actor
event = OrderCreated(
    aggregate_id=uuid4(),
    actor_id="user:jane.doe",  # Who triggered this event
    customer_id=uuid4(),
    total_amount=99.99,
    aggregate_version=1,
)

print(f"Event triggered by: {event.actor_id}")
```

---

## Integration Patterns

There are several patterns for integrating actor tracking with your authentication system, from simple to sophisticated.

### Pattern 1: Direct Passing

Pass `actor_id` explicitly through your domain layer:

```python
from uuid import UUID
from pydantic import BaseModel
from eventsource import AggregateRoot, DomainEvent

class OrderState(BaseModel):
    order_id: UUID
    customer_id: UUID | None = None
    total_amount: float = 0.0
    status: str = "draft"

class OrderAggregate(AggregateRoot[OrderState]):
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

    def create(
        self,
        customer_id: UUID,
        total_amount: float,
        actor_id: str | None = None,  # Accept actor_id
    ) -> None:
        """Create a new order, recording who created it."""
        if self.version > 0:
            raise ValueError("Order already created")

        event = OrderCreated(
            aggregate_id=self.aggregate_id,
            actor_id=actor_id,  # Include in event
            customer_id=customer_id,
            total_amount=total_amount,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)
```

**Pros**: Simple, explicit, easy to understand
**Cons**: Actor must be passed through every layer

### Pattern 2: Context Variables

Use Python's `contextvars` for request-scoped actor tracking:

```python
import contextvars
from uuid import UUID

# Define context variable for current actor
current_actor: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "current_actor", default=None
)

def get_current_actor() -> str | None:
    """Get the current actor from context."""
    return current_actor.get()

def set_current_actor(actor_id: str | None) -> contextvars.Token:
    """Set the current actor in context. Returns token for reset."""
    return current_actor.set(actor_id)

# In your aggregate, read from context
class OrderAggregate(AggregateRoot[OrderState]):
    def create(self, customer_id: UUID, total_amount: float) -> None:
        """Create order, automatically capturing current actor."""
        if self.version > 0:
            raise ValueError("Order already created")

        event = OrderCreated(
            aggregate_id=self.aggregate_id,
            actor_id=get_current_actor(),  # Read from context
            customer_id=customer_id,
            total_amount=total_amount,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)
```

**Pros**: No need to pass actor through every layer
**Cons**: Implicit dependency, requires discipline to set correctly

### Pattern 3: Command Objects

Encapsulate actor in command objects for explicit handling:

```python
from dataclasses import dataclass
from uuid import UUID

@dataclass
class CreateOrderCommand:
    """Command to create a new order."""
    customer_id: UUID
    total_amount: float
    actor_id: str  # Required - who is issuing this command

@dataclass
class ShipOrderCommand:
    """Command to ship an order."""
    order_id: UUID
    tracking_number: str
    actor_id: str

async def handle_create_order(
    cmd: CreateOrderCommand,
    repo: AggregateRepository,
) -> UUID:
    """Handle create order command."""
    order_id = uuid4()
    order = repo.create_new(order_id)
    order.create(
        customer_id=cmd.customer_id,
        total_amount=cmd.total_amount,
        actor_id=cmd.actor_id,  # Pass from command
    )
    await repo.save(order)
    return order_id

async def handle_ship_order(
    cmd: ShipOrderCommand,
    repo: AggregateRepository,
) -> None:
    """Handle ship order command."""
    order = await repo.load(cmd.order_id)
    order.ship(
        tracking_number=cmd.tracking_number,
        actor_id=cmd.actor_id,
    )
    await repo.save(order)
```

**Pros**: Explicit, testable, good for CQRS architectures
**Cons**: More boilerplate

---

## FastAPI Integration

Here's a complete example integrating with FastAPI and JWT authentication.

### Authentication Middleware

```python
from contextvars import ContextVar
from uuid import UUID
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from starlette.middleware.base import BaseHTTPMiddleware
import jwt

# Context variables for current request
current_actor: ContextVar[str | None] = ContextVar("current_actor", default=None)
current_request_id: ContextVar[str | None] = ContextVar("request_id", default=None)

class AuthMiddleware(BaseHTTPMiddleware):
    """Middleware to extract user identity and set actor context."""

    async def dispatch(self, request: Request, call_next):
        # Generate request ID for correlation
        request_id = str(uuid4())
        current_request_id.set(request_id)

        # Try to extract user from JWT token
        auth_header = request.headers.get("Authorization")
        if auth_header and auth_header.startswith("Bearer "):
            try:
                token = auth_header.split(" ")[1]
                payload = jwt.decode(
                    token,
                    "your-secret-key",  # Use proper key management
                    algorithms=["HS256"],
                )
                user_id = payload.get("sub")
                if user_id:
                    current_actor.set(f"user:{user_id}")
            except jwt.PyJWTError:
                pass  # Invalid token - actor remains None

        try:
            response = await call_next(request)
            return response
        finally:
            # Clean up context after request
            current_actor.set(None)
            current_request_id.set(None)


app = FastAPI()
app.add_middleware(AuthMiddleware)
```

### Dependency Injection for User Context

```python
from typing import Optional
from pydantic import BaseModel

class User(BaseModel):
    """Authenticated user model."""
    id: UUID
    username: str
    roles: list[str] = []

security = HTTPBearer(auto_error=False)

async def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
) -> Optional[User]:
    """Get current user from JWT token."""
    if not credentials:
        return None

    try:
        payload = jwt.decode(
            credentials.credentials,
            "your-secret-key",
            algorithms=["HS256"],
        )
        return User(
            id=UUID(payload["sub"]),
            username=payload["username"],
            roles=payload.get("roles", []),
        )
    except (jwt.PyJWTError, KeyError, ValueError):
        return None

async def require_user(
    user: Optional[User] = Depends(get_current_user),
) -> User:
    """Require authenticated user."""
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
    return user

def get_actor_id(user: Optional[User]) -> str | None:
    """Convert user to actor_id string."""
    return f"user:{user.id}" if user else None
```

### Endpoint with Actor Tracking

```python
from pydantic import BaseModel

class CreateOrderRequest(BaseModel):
    customer_id: UUID
    total_amount: float

class OrderResponse(BaseModel):
    order_id: UUID
    status: str

@app.post("/orders", response_model=OrderResponse)
async def create_order(
    request: CreateOrderRequest,
    user: User = Depends(require_user),
    repo: AggregateRepository = Depends(get_order_repository),
):
    """Create a new order, tracking the user who created it."""
    order_id = uuid4()
    order = repo.create_new(order_id)

    # Create order with actor tracking
    order.create(
        customer_id=request.customer_id,
        total_amount=request.total_amount,
        actor_id=f"user:{user.id}",  # Track who created it
    )
    await repo.save(order)

    return OrderResponse(
        order_id=order_id,
        status="created",
    )

@app.post("/orders/{order_id}/ship")
async def ship_order(
    order_id: UUID,
    tracking_number: str,
    user: User = Depends(require_user),
    repo: AggregateRepository = Depends(get_order_repository),
):
    """Ship an order, tracking the user who shipped it."""
    order = await repo.load(order_id)
    order.ship(
        tracking_number=tracking_number,
        actor_id=f"user:{user.id}",
    )
    await repo.save(order)

    return {"status": "shipped"}
```

---

## Using Event Metadata

The `metadata` field provides a flexible dictionary for additional context that doesn't fit in the event's core fields. Use `with_metadata()` to add context after event creation.

### Common Metadata Fields

```python
# Request context
event = event.with_metadata(
    request_id="req_abc123",
    ip_address="192.168.1.100",
    user_agent="Mozilla/5.0...",
)

# Trace context (OpenTelemetry, etc.)
event = event.with_metadata(
    trace_id="4bf92f3577b34da6a3ce929d0e0e4736",
    span_id="00f067aa0ba902b7",
    trace_flags="01",
)

# Business context
event = event.with_metadata(
    promotion_code="SUMMER20",
    channel="mobile-app",
    referrer="partner:acme",
)

# Authentication context
event = event.with_metadata(
    auth_method="oauth2",
    token_id="tok_xyz789",
    session_id="sess_abc123",
    mfa_verified=True,
)
```

### Middleware to Enrich Events

Create middleware that automatically adds metadata to all events:

```python
from functools import wraps
from typing import Callable, TypeVar
from eventsource import DomainEvent

T = TypeVar("T", bound=DomainEvent)

def enrich_event(event: T) -> T:
    """Add request context to event metadata."""
    return event.with_metadata(
        request_id=current_request_id.get(),
        actor_id=current_actor.get(),  # Also in metadata for queries
    )

# Use in aggregate methods
class OrderAggregate(AggregateRoot[OrderState]):
    def create(self, customer_id: UUID, total_amount: float) -> None:
        event = OrderCreated(
            aggregate_id=self.aggregate_id,
            actor_id=get_current_actor(),
            customer_id=customer_id,
            total_amount=total_amount,
            aggregate_version=self.get_next_version(),
        )
        # Enrich with additional context
        event = enrich_event(event)
        self.apply_event(event)
```

---

## Building Audit Trails

With actor tracking in place, you can build comprehensive audit trails using projections.

### Simple Audit Log Projection

```python
from datetime import datetime
from uuid import UUID
from eventsource import DomainEvent
from eventsource.projections import DeclarativeProjection, handles

@register_event
class AuditEntry(BaseModel):
    """A single audit log entry."""
    id: UUID
    timestamp: datetime
    actor_id: str | None
    event_type: str
    aggregate_type: str
    aggregate_id: UUID
    description: str
    metadata: dict

class AuditLogProjection(DeclarativeProjection):
    """Projection that builds a comprehensive audit log."""

    def __init__(self, checkpoint_repo=None, dlq_repo=None):
        super().__init__(checkpoint_repo=checkpoint_repo, dlq_repo=dlq_repo)
        self.entries: list[AuditEntry] = []

    # Generic handler that logs all events
    async def handle(self, event: DomainEvent) -> None:
        """Log every event to the audit trail."""
        entry = AuditEntry(
            id=event.event_id,
            timestamp=event.occurred_at,
            actor_id=event.actor_id,
            event_type=event.event_type,
            aggregate_type=event.aggregate_type,
            aggregate_id=event.aggregate_id,
            description=self._describe_event(event),
            metadata=event.metadata,
        )
        self.entries.append(entry)

        # Update checkpoint
        await super().handle(event)

    def _describe_event(self, event: DomainEvent) -> str:
        """Generate human-readable description."""
        actor = event.actor_id or "System"
        return f"{actor} triggered {event.event_type} on {event.aggregate_type}"

    def get_entries_by_actor(self, actor_id: str) -> list[AuditEntry]:
        """Get all audit entries for a specific actor."""
        return [e for e in self.entries if e.actor_id == actor_id]

    def get_entries_by_aggregate(self, aggregate_id: UUID) -> list[AuditEntry]:
        """Get all audit entries for a specific aggregate."""
        return [e for e in self.entries if e.aggregate_id == aggregate_id]

    def get_recent_entries(self, limit: int = 100) -> list[AuditEntry]:
        """Get most recent audit entries."""
        return sorted(
            self.entries,
            key=lambda e: e.timestamp,
            reverse=True,
        )[:limit]

    async def _truncate_read_models(self) -> None:
        self.entries.clear()
```

### Database-Backed Audit Projection

For production use, store audit entries in a database:

```python
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

class DatabaseAuditProjection(DeclarativeProjection):
    """Production audit projection with PostgreSQL storage."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        checkpoint_repo=None,
        dlq_repo=None,
    ):
        super().__init__(checkpoint_repo=checkpoint_repo, dlq_repo=dlq_repo)
        self._session_factory = session_factory

    async def handle(self, event: DomainEvent) -> None:
        """Store audit entry in database."""
        async with self._session_factory() as session:
            await session.execute(
                text("""
                    INSERT INTO audit_log (
                        event_id, occurred_at, actor_id, event_type,
                        aggregate_type, aggregate_id, correlation_id,
                        causation_id, metadata, payload
                    ) VALUES (
                        :event_id, :occurred_at, :actor_id, :event_type,
                        :aggregate_type, :aggregate_id, :correlation_id,
                        :causation_id, :metadata, :payload
                    )
                    ON CONFLICT (event_id) DO NOTHING
                """),
                {
                    "event_id": event.event_id,
                    "occurred_at": event.occurred_at,
                    "actor_id": event.actor_id,
                    "event_type": event.event_type,
                    "aggregate_type": event.aggregate_type,
                    "aggregate_id": event.aggregate_id,
                    "correlation_id": event.correlation_id,
                    "causation_id": event.causation_id,
                    "metadata": event.metadata,
                    "payload": event.to_dict(),
                }
            )
            await session.commit()

        await super().handle(event)

    async def _truncate_read_models(self) -> None:
        async with self._session_factory() as session:
            await session.execute(text("TRUNCATE TABLE audit_log"))
            await session.commit()
```

### User Activity Projection

Build user-centric activity views:

```python
class UserActivityProjection(DeclarativeProjection):
    """Track activity per user for dashboards and analytics."""

    def __init__(self, checkpoint_repo=None, dlq_repo=None):
        super().__init__(checkpoint_repo=checkpoint_repo, dlq_repo=dlq_repo)
        # Activity indexed by actor_id
        self.activities: dict[str, list[dict]] = {}

    async def handle(self, event: DomainEvent) -> None:
        """Record activity for the actor."""
        actor = event.actor_id or "anonymous"

        if actor not in self.activities:
            self.activities[actor] = []

        self.activities[actor].append({
            "event_id": str(event.event_id),
            "event_type": event.event_type,
            "aggregate_type": event.aggregate_type,
            "aggregate_id": str(event.aggregate_id),
            "timestamp": event.occurred_at.isoformat(),
            "correlation_id": str(event.correlation_id),
        })

        await super().handle(event)

    def get_user_activity(
        self,
        actor_id: str,
        limit: int = 50,
    ) -> list[dict]:
        """Get recent activity for a user."""
        activities = self.activities.get(actor_id, [])
        return sorted(
            activities,
            key=lambda a: a["timestamp"],
            reverse=True,
        )[:limit]

    def get_active_users(self, since: datetime) -> list[str]:
        """Get users active since a timestamp."""
        active = []
        since_iso = since.isoformat()
        for actor_id, activities in self.activities.items():
            for activity in activities:
                if activity["timestamp"] > since_iso:
                    active.append(actor_id)
                    break
        return active

    async def _truncate_read_models(self) -> None:
        self.activities.clear()
```

---

## Correlation Tracking

Use `correlation_id` and `causation_id` to trace event chains across aggregates.

### How Correlation Works

```
User action creates first event:
    OrderCreated (correlation_id=A, causation_id=None)
        |
        | triggers
        v
    PaymentRequested (correlation_id=A, causation_id=OrderCreated.event_id)
        |
        | triggers
        v
    PaymentProcessed (correlation_id=A, causation_id=PaymentRequested.event_id)
        |
        | triggers
        v
    InventoryReserved (correlation_id=A, causation_id=PaymentProcessed.event_id)

All events share correlation_id=A, enabling full trace reconstruction.
```

### Using with_causation()

```python
# Original event (e.g., from command)
order_created = OrderCreated(
    aggregate_id=order_id,
    actor_id="user:123",
    customer_id=customer_id,
    total_amount=99.99,
    aggregate_version=1,
)

# Subsequent event carries causation chain
payment_requested = PaymentRequested(
    aggregate_id=payment_id,
    actor_id="system:payment-service",  # Service actor
    order_id=order_id,
    amount=99.99,
    aggregate_version=1,
).with_causation(order_created)

# Verify chain
assert payment_requested.causation_id == order_created.event_id
assert payment_requested.correlation_id == order_created.correlation_id

# Continue the chain
payment_processed = PaymentProcessed(
    aggregate_id=payment_id,
    actor_id="webhook:stripe",  # External system
    transaction_id="txn_abc123",
    aggregate_version=2,
).with_causation(payment_requested)

# All share the same correlation_id
assert payment_processed.correlation_id == order_created.correlation_id
```

### Querying by Correlation

```python
async def get_event_chain(
    event_store: EventStore,
    correlation_id: UUID,
) -> list[DomainEvent]:
    """Get all events in a correlation chain."""
    all_events = []

    # Read all events and filter by correlation
    # (In production, add correlation_id index and query directly)
    async for stored_event in event_store.read_all():
        if stored_event.event.correlation_id == correlation_id:
            all_events.append(stored_event.event)

    # Sort by timestamp to show chain order
    return sorted(all_events, key=lambda e: e.occurred_at)

# Usage
chain = await get_event_chain(event_store, order_created.correlation_id)
for event in chain:
    print(f"{event.event_type} by {event.actor_id} at {event.occurred_at}")
```

---

## Authorization Patterns

While eventsource handles authentication tracking (who), authorization (can they?) is typically handled at the application layer.

### Command-Level Authorization

Check permissions before executing commands:

```python
from enum import Enum

class Permission(Enum):
    CREATE_ORDER = "create_order"
    SHIP_ORDER = "ship_order"
    CANCEL_ORDER = "cancel_order"
    VIEW_ORDER = "view_order"

def require_permission(permission: Permission):
    """Decorator to check permissions before command execution."""
    def decorator(func):
        @wraps(func)
        async def wrapper(user: User, *args, **kwargs):
            if permission.value not in user.roles:
                raise HTTPException(
                    status_code=403,
                    detail=f"Permission denied: {permission.value}",
                )
            return await func(user, *args, **kwargs)
        return wrapper
    return decorator

@app.post("/orders/{order_id}/ship")
@require_permission(Permission.SHIP_ORDER)
async def ship_order(
    order_id: UUID,
    tracking_number: str,
    user: User = Depends(require_user),
    repo: AggregateRepository = Depends(get_order_repository),
):
    order = await repo.load(order_id)
    order.ship(tracking_number=tracking_number, actor_id=f"user:{user.id}")
    await repo.save(order)
    return {"status": "shipped"}
```

### Aggregate-Level Authorization

Check ownership or access rights on aggregates:

```python
class AuthorizedOrderRepository:
    """Repository wrapper that enforces authorization."""

    def __init__(
        self,
        inner_repo: AggregateRepository,
        user: User,
    ):
        self._inner = inner_repo
        self._user = user

    async def load(self, order_id: UUID) -> OrderAggregate:
        """Load order, checking access rights."""
        order = await self._inner.load(order_id)

        # Check if user can access this order
        if not self._can_access(order):
            raise HTTPException(
                status_code=403,
                detail="You don't have access to this order",
            )

        return order

    def _can_access(self, order: OrderAggregate) -> bool:
        """Check if user can access the order."""
        # Admin can access all orders
        if "admin" in self._user.roles:
            return True

        # User can access their own orders
        if order.state and order.state.customer_id == self._user.id:
            return True

        return False

# Dependency injection
async def get_authorized_order_repo(
    user: User = Depends(require_user),
    inner_repo: AggregateRepository = Depends(get_order_repository),
) -> AuthorizedOrderRepository:
    return AuthorizedOrderRepository(inner_repo, user)
```

### Combined with Multi-Tenancy

When using multi-tenancy, combine tenant isolation with actor authorization:

```python
class TenantAuthorizedRepository:
    """Repository with both tenant isolation and user authorization."""

    def __init__(
        self,
        event_store: EventStore,
        tenant_id: UUID,
        user: User,
    ):
        self._event_store = event_store
        self._tenant_id = tenant_id
        self._user = user

    async def load(self, order_id: UUID) -> OrderAggregate:
        # Load order
        order = await self._load_order(order_id)

        # Verify tenant ownership
        if order.tenant_id != self._tenant_id:
            raise HTTPException(status_code=404, detail="Order not found")

        # Verify user authorization
        if not self._user_can_access(order):
            raise HTTPException(status_code=403, detail="Access denied")

        return order

    def _user_can_access(self, order: OrderAggregate) -> bool:
        # Implement role-based or ownership-based access
        return "admin" in self._user.roles or order.state.customer_id == self._user.id
```

---

## Security Considerations

### Validate Actor IDs

Never trust client-provided actor IDs:

```python
# BAD: Trust client input
@app.post("/orders")
async def create_order_unsafe(request: dict):
    # Client could claim to be anyone!
    actor_id = request.get("actor_id")
    order.create(..., actor_id=actor_id)

# GOOD: Extract actor from verified authentication
@app.post("/orders")
async def create_order_safe(
    request: CreateOrderRequest,
    user: User = Depends(require_user),  # Verified by JWT
):
    actor_id = f"user:{user.id}"  # From verified token
    order.create(..., actor_id=actor_id)
```

### Set Actors in Trusted Code

Set `actor_id` in server-side code, not from client input:

```python
# Middleware sets actor from verified token
class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        # Verify token and extract user
        user = await verify_jwt_token(request)
        if user:
            current_actor.set(f"user:{user.id}")
        # ...
```

### Use Opaque Identifiers

Don't expose sensitive information in actor IDs:

```python
# BAD: Contains PII
actor_id = f"user:{email}"  # jane@example.com

# GOOD: Opaque identifier
actor_id = f"user:{user_id}"  # user:550e8400-e29b-41d4-a716-...
```

### Consider GDPR Implications

Actor IDs may constitute personal data under GDPR:

- **Include in data exports**: When users request their data
- **Handle in deletion requests**: Consider pseudonymization
- **Document retention policies**: How long audit logs are kept

```python
async def export_user_data(user_id: UUID) -> dict:
    """Export all data for a user (GDPR Article 15)."""
    actor_id = f"user:{user_id}"

    # Get all events triggered by this user
    user_events = []
    async for stored_event in event_store.read_all():
        if stored_event.event.actor_id == actor_id:
            user_events.append(stored_event.event.to_dict())

    return {
        "actor_id": actor_id,
        "events": user_events,
        "export_date": datetime.now(UTC).isoformat(),
    }
```

---

## Best Practices Summary

### Always Set actor_id for User Actions

```python
# Every user-initiated event should have an actor
event = OrderCreated(
    aggregate_id=order_id,
    actor_id=f"user:{user.id}",  # Always include
    ...
)
```

### Use Consistent Formats

Establish and document actor ID conventions:

```python
# Document your conventions
# user:{uuid}      - Human users
# service:{name}   - Service accounts
# system:{process} - Background jobs
# webhook:{source} - External integrations
```

### Include Actor in Logging Context

Add actor to structured logs for correlation:

```python
import structlog

logger = structlog.get_logger()

@app.middleware("http")
async def add_actor_to_logs(request, call_next):
    actor = current_actor.get()
    with structlog.contextvars.bound_contextvars(actor_id=actor):
        return await call_next(request)
```

### Build Dedicated Audit Projections

Create purpose-built projections for compliance:

```python
# Separate projections for different audit needs
audit_log_projection = AuditLogProjection()        # General audit
user_activity_projection = UserActivityProjection() # User-centric
compliance_projection = ComplianceProjection()      # Regulatory
```

### Consider Actor in Retention Policies

Plan for data lifecycle:

```python
async def archive_old_events(
    event_store: EventStore,
    archive_store: ArchiveStore,
    older_than: datetime,
) -> None:
    """Archive old events while preserving audit trail."""
    async for stored_event in event_store.read_all():
        if stored_event.event.occurred_at < older_than:
            # Archive with full context including actor
            await archive_store.store(stored_event.event)
```

---

## Complete Example

Here's a full working example demonstrating authentication integration:

```python
import asyncio
from uuid import uuid4, UUID
from contextvars import ContextVar
from datetime import datetime, UTC
from pydantic import BaseModel

from eventsource import (
    DomainEvent,
    register_event,
    AggregateRoot,
    InMemoryEventStore,
    AggregateRepository,
)
from eventsource.projections import DeclarativeProjection

# Context for current actor
current_actor: ContextVar[str | None] = ContextVar("current_actor", default=None)

# Events
@register_event
class DocumentCreated(DomainEvent):
    event_type: str = "DocumentCreated"
    aggregate_type: str = "Document"
    title: str
    content: str

@register_event
class DocumentEdited(DomainEvent):
    event_type: str = "DocumentEdited"
    aggregate_type: str = "Document"
    new_content: str

# State
class DocumentState(BaseModel):
    document_id: UUID
    title: str = ""
    content: str = ""
    created_by: str | None = None
    last_edited_by: str | None = None

# Aggregate
class DocumentAggregate(AggregateRoot[DocumentState]):
    aggregate_type = "Document"

    def _get_initial_state(self) -> DocumentState:
        return DocumentState(document_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, DocumentCreated):
            self._state = DocumentState(
                document_id=self.aggregate_id,
                title=event.title,
                content=event.content,
                created_by=event.actor_id,
            )
        elif isinstance(event, DocumentEdited):
            if self._state:
                self._state = self._state.model_copy(update={
                    "content": event.new_content,
                    "last_edited_by": event.actor_id,
                })

    def create(self, title: str, content: str) -> None:
        if self.version > 0:
            raise ValueError("Document already exists")
        event = DocumentCreated(
            aggregate_id=self.aggregate_id,
            actor_id=current_actor.get(),  # From context
            title=title,
            content=content,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

    def edit(self, new_content: str) -> None:
        if self.version == 0:
            raise ValueError("Document must be created first")
        event = DocumentEdited(
            aggregate_id=self.aggregate_id,
            actor_id=current_actor.get(),
            new_content=new_content,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

# Audit projection
class DocumentAuditProjection(DeclarativeProjection):
    def __init__(self):
        super().__init__()
        self.log: list[dict] = []

    async def handle(self, event: DomainEvent) -> None:
        self.log.append({
            "timestamp": event.occurred_at.isoformat(),
            "actor": event.actor_id,
            "action": event.event_type,
            "document_id": str(event.aggregate_id),
        })
        await super().handle(event)

    async def _truncate_read_models(self) -> None:
        self.log.clear()

# Demo
async def main():
    # Setup
    event_store = InMemoryEventStore()
    repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=DocumentAggregate,
        aggregate_type="Document",
    )
    audit = DocumentAuditProjection()

    # Simulate user sessions
    print("=== User 'alice' creates a document ===")
    current_actor.set("user:alice")
    doc_id = uuid4()
    doc = repo.create_new(doc_id)
    doc.create(title="Meeting Notes", content="Initial draft...")
    await repo.save(doc)

    # Process event in audit projection
    for event in doc.uncommitted_events:
        pass  # Events already saved
    events = (await event_store.get_events(doc_id, "Document")).events
    for event in events:
        await audit.handle(event)

    print("=== User 'bob' edits the document ===")
    current_actor.set("user:bob")
    doc = await repo.load(doc_id)
    doc.edit(new_content="Updated content by Bob...")
    await repo.save(doc)

    events = (await event_store.get_events(doc_id, "Document")).events
    await audit.handle(events[-1])  # Just the new event

    print("\n=== Audit Log ===")
    for entry in audit.log:
        print(f"  {entry['timestamp']}: {entry['actor']} -> {entry['action']}")

    print(f"\n=== Document State ===")
    print(f"  Title: {doc.state.title}")
    print(f"  Created by: {doc.state.created_by}")
    print(f"  Last edited by: {doc.state.last_edited_by}")

if __name__ == "__main__":
    asyncio.run(main())
```

Expected output:

```
=== User 'alice' creates a document ===
=== User 'bob' edits the document ===

=== Audit Log ===
  2024-01-15T10:30:00: user:alice -> DocumentCreated
  2024-01-15T10:31:00: user:bob -> DocumentEdited

=== Document State ===
  Title: Meeting Notes
  Created by: user:alice
  Last edited by: user:bob
```

---

## See Also

- [Events API](../api/events.md) - `actor_id`, `correlation_id`, and `metadata` fields
- [Multi-Tenant Setup](./multi-tenant.md) - Combining tenant isolation with actor tracking
- [Projections Guide](../examples/projections.md) - Building read models and audit projections
- [Error Handling Guide](./error-handling.md) - Handling failures in projections
