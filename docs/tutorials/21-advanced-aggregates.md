# Tutorial 21: Advanced Aggregate Patterns

**Difficulty:** Expert
**Progress:** Tutorial 21 of 21 | Phase 4: Advanced Patterns (Complete!)

---

## Prerequisites

Before starting this tutorial, ensure you have:

- Completed [Tutorial 20: Observability with OpenTelemetry](20-observability.md)
- Strong understanding of aggregates from [Tutorial 3: Building Your First Aggregate](03-first-aggregate.md)
- Understanding of snapshotting from [Tutorial 14: Optimizing with Snapshotting](14-snapshotting.md)
- Python 3.11+ installed
- eventsource-py installed:

```bash
pip install eventsource-py
```

This capstone tutorial synthesizes concepts from the entire series and introduces advanced patterns for building production-grade event-sourced aggregates in complex domains.

---

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Implement schema versioning with migration strategies for evolving aggregates
2. Handle unregistered events gracefully using different handling modes
3. Compose aggregates with value objects and child entities
4. Build process managers (sagas) for cross-aggregate workflows
5. Design event upcasting strategies for schema evolution
6. Implement robust state validation with complex invariants
7. Create domain services for cross-aggregate operations
8. Use aggregate factories for consistent instantiation
9. Test advanced aggregate patterns effectively

---

## Overview

As event-sourced systems mature, they encounter challenges that require sophisticated solutions. This tutorial covers patterns that address:

- **Evolution**: How do you change aggregate state schemas without breaking existing snapshots?
- **Compatibility**: How do you handle events from older code versions?
- **Complexity**: How do you model domains with rich entity hierarchies?
- **Coordination**: How do you orchestrate operations across multiple aggregates?
- **Integrity**: How do you ensure business rules are consistently enforced?

Let us explore each pattern in depth.

---

## Schema Versioning

When aggregates use snapshotting (Tutorial 14), their state is serialized and stored. Over time, your state models will evolve - fields get added, removed, or renamed. Schema versioning ensures old snapshots can be migrated to new formats.

### Why Schema Versions Matter

Consider an `AccountAggregate` that originally stored only `balance`:

```python
# Version 1 state
class AccountState(BaseModel):
    account_id: UUID
    balance: float
```

Later, you add transaction tracking:

```python
# Version 2 state - added transaction_count
class AccountState(BaseModel):
    account_id: UUID
    balance: float
    transaction_count: int = 0  # New field
```

Without versioning, loading a V1 snapshot with V2 code fails because `transaction_count` is missing from the stored data.

### The schema_version Attribute

Every aggregate has a `schema_version` class attribute:

```python
from eventsource import AggregateRoot

class AccountAggregate(AggregateRoot[AccountState]):
    aggregate_type = "Account"
    schema_version = 2  # Increment when state structure changes
```

When loading snapshots, the repository compares the snapshot's schema version with the aggregate's expected version. Mismatches trigger automatic recovery:

1. Snapshot is ignored
2. Full event replay is performed
3. A new snapshot is created at the current schema version

### Implementing Version Migration

For complex migrations, override `_restore_from_snapshot()` to transform old state:

```python
from uuid import UUID
from pydantic import BaseModel
from eventsource import AggregateRoot, DomainEvent, register_event


@register_event
class AccountOpened(DomainEvent):
    event_type: str = "AccountOpened"
    aggregate_type: str = "Account"
    owner_name: str
    initial_balance: float


@register_event
class MoneyDeposited(DomainEvent):
    event_type: str = "MoneyDeposited"
    aggregate_type: str = "Account"
    amount: float


class AccountStateV2(BaseModel):
    """Current state - Version 2 with transaction tracking."""
    account_id: UUID
    owner_name: str = ""
    balance: float = 0.0
    transaction_count: int = 0  # New in V2


class AccountAggregate(AggregateRoot[AccountStateV2]):
    """Account aggregate with schema migration support."""
    aggregate_type = "Account"
    schema_version = 2  # Current version

    def _get_initial_state(self) -> AccountStateV2:
        return AccountStateV2(account_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, AccountOpened):
            self._state = AccountStateV2(
                account_id=self.aggregate_id,
                owner_name=event.owner_name,
                balance=event.initial_balance,
                transaction_count=0,
            )
        elif isinstance(event, MoneyDeposited):
            if self._state:
                self._state = self._state.model_copy(update={
                    "balance": self._state.balance + event.amount,
                    "transaction_count": self._state.transaction_count + 1,
                })

    def _restore_from_snapshot(
        self,
        state_dict: dict,
        schema_version: int,
    ) -> None:
        """
        Restore state from snapshot with migration support.

        This method handles older snapshot versions by migrating
        the state to the current schema before deserialization.
        """
        # Apply migrations sequentially
        if schema_version < 2:
            state_dict = self._migrate_v1_to_v2(state_dict)

        # Future migrations would go here:
        # if schema_version < 3:
        #     state_dict = self._migrate_v2_to_v3(state_dict)

        # Deserialize the migrated state
        self._state = AccountStateV2.model_validate(state_dict)

    def _migrate_v1_to_v2(self, state: dict) -> dict:
        """Migrate V1 state to V2 by adding transaction_count."""
        if "transaction_count" not in state:
            # V1 did not track transactions - default to 0
            # In production, you might calculate this from events
            state["transaction_count"] = 0
        return state

    # Command methods
    def open(self, owner_name: str, initial_balance: float = 0.0) -> None:
        if self.version > 0:
            raise ValueError("Account already exists")
        self.apply_event(AccountOpened(
            aggregate_id=self.aggregate_id,
            owner_name=owner_name,
            initial_balance=initial_balance,
            aggregate_version=self.get_next_version(),
        ))

    def deposit(self, amount: float) -> None:
        if amount <= 0:
            raise ValueError("Amount must be positive")
        self.apply_event(MoneyDeposited(
            aggregate_id=self.aggregate_id,
            amount=amount,
            aggregate_version=self.get_next_version(),
        ))
```

### Migration Best Practices

| Practice | Description |
|----------|-------------|
| Sequential migrations | Apply migrations one version at a time (V1 -> V2 -> V3) |
| Non-destructive changes | Prefer adding optional fields over removing required ones |
| Test migrations | Write tests that load V1 snapshots with V2 code |
| Document changes | Keep a changelog of schema changes per version |
| Batch invalidation | After deploying, invalidate old snapshots proactively |

### When to Increment schema_version

**DO increment** when:
- Adding required fields without defaults
- Removing existing fields
- Renaming fields
- Changing field types incompatibly

**DO NOT increment** when:
- Adding optional fields with defaults (Pydantic handles this)
- Adding new methods to the aggregate
- Changing validation rules only

---

## Unregistered Event Handling

In production systems, your aggregate may encounter events it does not recognize. This happens when:

- Deploying new code that adds events before all services update
- Rolling back to older code after events were created
- Processing events from a different version of the aggregate

### The Problem

Consider an aggregate that handles `OrderCreated` and `OrderShipped`. A new release adds `OrderCancelled`, but during rollback, the old code sees `OrderCancelled` events it cannot handle.

### DeclarativeAggregate Handling Modes

The `DeclarativeAggregate` class provides three modes via the `unregistered_event_handling` attribute:

```python
from eventsource import DeclarativeAggregate, handles

class OrderAggregate(DeclarativeAggregate[OrderState]):
    aggregate_type = "Order"

    # Options: "ignore" (default), "warn", "error"
    unregistered_event_handling = "warn"

    @handles(OrderCreated)
    def _on_created(self, event: OrderCreated) -> None:
        # Handle OrderCreated
        pass

    @handles(OrderShipped)
    def _on_shipped(self, event: OrderShipped) -> None:
        # Handle OrderShipped
        pass

    # If OrderCancelled is applied but has no handler:
    # - "ignore": Silent skip (default, forward-compatible)
    # - "warn": Log warning, continue processing
    # - "error": Raise UnhandledEventError (strict mode)
```

### Choosing a Mode

| Mode | Use Case | Behavior |
|------|----------|----------|
| `"ignore"` | Forward compatibility | Silently skip unknown events; aggregate continues |
| `"warn"` | Development, debugging | Log warning with event details; aggregate continues |
| `"error"` | Strict validation | Raise `UnhandledEventError`; fail fast |

### Example: Strict Mode for Development

```python
from eventsource import DeclarativeAggregate, handles
from eventsource.exceptions import UnhandledEventError


class StrictOrderAggregate(DeclarativeAggregate[OrderState]):
    """Aggregate that fails on unhandled events - good for catching bugs."""
    aggregate_type = "Order"
    unregistered_event_handling = "error"

    @handles(OrderCreated)
    def _on_created(self, event: OrderCreated) -> None:
        self._state = OrderState(
            order_id=self.aggregate_id,
            status="created",
        )

    # If any other event type is applied, UnhandledEventError is raised
```

### Handling Mode in AggregateRoot

If using `AggregateRoot` directly (not `DeclarativeAggregate`), handle unknown events in your `_apply()` method:

```python
class OrderAggregate(AggregateRoot[OrderState]):
    aggregate_type = "Order"

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            self._state = OrderState(...)
        elif isinstance(event, OrderShipped):
            # Handle shipping
            pass
        else:
            # Unknown event - log and skip for forward compatibility
            import logging
            logging.warning(
                "Unhandled event type %s in OrderAggregate",
                type(event).__name__,
            )
```

---

## Aggregate Composition

Complex domains require aggregates that contain value objects and child entities. Understanding when to use each is crucial for good domain modeling.

### Value Objects vs Entities

| Concept | Identity | Mutability | Example |
|---------|----------|------------|---------|
| **Value Object** | Defined by attributes | Immutable | Money, Address, DateRange |
| **Entity** | Has unique ID | Mutable through aggregate | OrderLine, CartItem |

### Implementing Value Objects

Value objects are immutable and compared by value:

```python
from pydantic import BaseModel


class Money(BaseModel):
    """Immutable money value object."""
    amount: float
    currency: str = "USD"

    class Config:
        frozen = True  # Makes the model immutable

    def add(self, other: "Money") -> "Money":
        """Add two money values, returning a new Money instance."""
        if self.currency != other.currency:
            raise ValueError(f"Cannot add {self.currency} and {other.currency}")
        return Money(amount=self.amount + other.amount, currency=self.currency)

    def subtract(self, other: "Money") -> "Money":
        """Subtract money values, returning a new Money instance."""
        if self.currency != other.currency:
            raise ValueError(f"Cannot subtract {self.currency} and {other.currency}")
        return Money(amount=self.amount - other.amount, currency=self.currency)

    def multiply(self, factor: float) -> "Money":
        """Multiply by a factor, returning a new Money instance."""
        return Money(amount=self.amount * factor, currency=self.currency)


class Address(BaseModel):
    """Immutable address value object."""
    street: str
    city: str
    state: str
    postal_code: str
    country: str = "USA"

    class Config:
        frozen = True
```

### Implementing Child Entities

Child entities have identity within their parent aggregate:

```python
from uuid import UUID, uuid4
from pydantic import BaseModel, Field


class OrderLine(BaseModel):
    """Order line entity - has identity within the Order aggregate."""
    line_id: UUID = Field(default_factory=uuid4)
    product_id: str
    product_name: str
    quantity: int
    unit_price: float

    @property
    def line_total(self) -> float:
        """Calculate total for this line."""
        return self.quantity * self.unit_price
```

### Composing the Aggregate

```python
from uuid import UUID, uuid4
from pydantic import BaseModel, Field
from eventsource import AggregateRoot, DomainEvent, register_event


# Events
@register_event
class OrderCreated(DomainEvent):
    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    customer_id: UUID
    shipping_address: dict  # Serialized Address


@register_event
class OrderLineAdded(DomainEvent):
    event_type: str = "OrderLineAdded"
    aggregate_type: str = "Order"
    line_id: UUID
    product_id: str
    product_name: str
    quantity: int
    unit_price: float


@register_event
class OrderLineRemoved(DomainEvent):
    event_type: str = "OrderLineRemoved"
    aggregate_type: str = "Order"
    line_id: UUID


# State with composition
class OrderState(BaseModel):
    order_id: UUID
    customer_id: UUID | None = None
    shipping_address: Address | None = None
    lines: list[OrderLine] = Field(default_factory=list)
    status: str = "draft"

    @property
    def total(self) -> Money:
        """Calculate order total from all lines."""
        total_amount = sum(line.line_total for line in self.lines)
        return Money(amount=total_amount)

    @property
    def line_count(self) -> int:
        """Number of lines in the order."""
        return len(self.lines)


class OrderAggregate(AggregateRoot[OrderState]):
    """Order aggregate with value objects and child entities."""
    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            self._state = OrderState(
                order_id=self.aggregate_id,
                customer_id=event.customer_id,
                shipping_address=Address(**event.shipping_address),
                status="created",
            )
        elif isinstance(event, OrderLineAdded):
            if self._state:
                new_line = OrderLine(
                    line_id=event.line_id,
                    product_id=event.product_id,
                    product_name=event.product_name,
                    quantity=event.quantity,
                    unit_price=event.unit_price,
                )
                self._state = self._state.model_copy(update={
                    "lines": [*self._state.lines, new_line]
                })
        elif isinstance(event, OrderLineRemoved):
            if self._state:
                updated_lines = [
                    line for line in self._state.lines
                    if line.line_id != event.line_id
                ]
                self._state = self._state.model_copy(update={
                    "lines": updated_lines
                })

    def create(self, customer_id: UUID, shipping_address: Address) -> None:
        """Create a new order."""
        if self.version > 0:
            raise ValueError("Order already exists")

        self.apply_event(OrderCreated(
            aggregate_id=self.aggregate_id,
            customer_id=customer_id,
            shipping_address=shipping_address.model_dump(),
            aggregate_version=self.get_next_version(),
        ))

    def add_line(
        self,
        product_id: str,
        product_name: str,
        quantity: int,
        unit_price: float,
    ) -> UUID:
        """Add a line item to the order. Returns the line ID."""
        if not self._state:
            raise ValueError("Order does not exist")
        if self._state.status != "created":
            raise ValueError("Cannot modify order in current status")

        line_id = uuid4()
        self.apply_event(OrderLineAdded(
            aggregate_id=self.aggregate_id,
            line_id=line_id,
            product_id=product_id,
            product_name=product_name,
            quantity=quantity,
            unit_price=unit_price,
            aggregate_version=self.get_next_version(),
        ))
        return line_id

    def remove_line(self, line_id: UUID) -> None:
        """Remove a line item from the order."""
        if not self._state:
            raise ValueError("Order does not exist")
        if self._state.status != "created":
            raise ValueError("Cannot modify order in current status")

        # Verify line exists
        if not any(line.line_id == line_id for line in self._state.lines):
            raise ValueError(f"Line {line_id} not found in order")

        self.apply_event(OrderLineRemoved(
            aggregate_id=self.aggregate_id,
            line_id=line_id,
            aggregate_version=self.get_next_version(),
        ))
```

### Design Guidelines

1. **Aggregate boundaries**: Only the root aggregate can be referenced from outside
2. **Child entity access**: Access child entities through the aggregate root
3. **Identity scope**: Child entity IDs are unique within their parent, not globally
4. **Transactional consistency**: All changes within an aggregate are atomic

---

## Process Managers (Sagas)

Process managers coordinate operations across multiple aggregates. They listen to events and issue commands, orchestrating complex workflows.

### When to Use Process Managers

- Order fulfillment (Order -> Payment -> Inventory -> Shipping)
- User registration (Account -> Email Verification -> Profile Setup)
- Booking systems (Reservation -> Payment -> Confirmation)

### The Saga Pattern

A saga is a sequence of local transactions where each step can be compensated if a later step fails.

```
Happy Path:
  OrderPlaced -> PaymentAuthorized -> InventoryReserved -> OrderConfirmed

Compensation Path:
  OrderPlaced -> PaymentAuthorized -> InventoryFailed -> PaymentRefunded -> OrderCancelled
```

### Implementing a Process Manager

Process managers are themselves aggregates that track workflow state:

```python
from enum import Enum
from uuid import UUID, uuid4
from pydantic import BaseModel
from eventsource import AggregateRoot, DomainEvent, register_event


class FulfillmentStatus(Enum):
    STARTED = "started"
    PAYMENT_PENDING = "payment_pending"
    PAYMENT_CONFIRMED = "payment_confirmed"
    INVENTORY_RESERVED = "inventory_reserved"
    SHIPPING_PENDING = "shipping_pending"
    COMPLETED = "completed"
    FAILED = "failed"
    COMPENSATING = "compensating"


# Process Manager Events
@register_event
class FulfillmentStarted(DomainEvent):
    event_type: str = "FulfillmentStarted"
    aggregate_type: str = "Fulfillment"
    order_id: UUID
    total_amount: float


@register_event
class PaymentRequested(DomainEvent):
    event_type: str = "PaymentRequested"
    aggregate_type: str = "Fulfillment"
    payment_id: UUID
    amount: float


@register_event
class PaymentConfirmed(DomainEvent):
    event_type: str = "PaymentConfirmed"
    aggregate_type: str = "Fulfillment"
    payment_id: UUID


@register_event
class PaymentFailed(DomainEvent):
    event_type: str = "PaymentFailed"
    aggregate_type: str = "Fulfillment"
    payment_id: UUID
    reason: str


@register_event
class InventoryReserved(DomainEvent):
    event_type: str = "InventoryReserved"
    aggregate_type: str = "Fulfillment"
    reservation_id: UUID


@register_event
class FulfillmentCompleted(DomainEvent):
    event_type: str = "FulfillmentCompleted"
    aggregate_type: str = "Fulfillment"


@register_event
class FulfillmentFailed(DomainEvent):
    event_type: str = "FulfillmentFailed"
    aggregate_type: str = "Fulfillment"
    reason: str


class FulfillmentState(BaseModel):
    fulfillment_id: UUID
    order_id: UUID | None = None
    payment_id: UUID | None = None
    reservation_id: UUID | None = None
    status: FulfillmentStatus = FulfillmentStatus.STARTED
    total_amount: float = 0.0
    failure_reason: str | None = None


class OrderFulfillmentProcess(AggregateRoot[FulfillmentState]):
    """
    Process Manager for order fulfillment.

    Coordinates between Order, Payment, and Inventory aggregates.

    Workflow:
    1. Start fulfillment for an order
    2. Request payment authorization
    3. Wait for payment confirmation
    4. Reserve inventory
    5. Complete fulfillment

    Compensation:
    - If payment fails: Mark fulfillment failed
    - If inventory fails: Request payment refund, mark failed
    """
    aggregate_type = "Fulfillment"

    def _get_initial_state(self) -> FulfillmentState:
        return FulfillmentState(fulfillment_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, FulfillmentStarted):
            self._state = FulfillmentState(
                fulfillment_id=self.aggregate_id,
                order_id=event.order_id,
                total_amount=event.total_amount,
                status=FulfillmentStatus.PAYMENT_PENDING,
            )
        elif isinstance(event, PaymentRequested):
            if self._state:
                self._state = self._state.model_copy(update={
                    "payment_id": event.payment_id,
                })
        elif isinstance(event, PaymentConfirmed):
            if self._state:
                self._state = self._state.model_copy(update={
                    "status": FulfillmentStatus.PAYMENT_CONFIRMED,
                })
        elif isinstance(event, PaymentFailed):
            if self._state:
                self._state = self._state.model_copy(update={
                    "status": FulfillmentStatus.FAILED,
                    "failure_reason": event.reason,
                })
        elif isinstance(event, InventoryReserved):
            if self._state:
                self._state = self._state.model_copy(update={
                    "reservation_id": event.reservation_id,
                    "status": FulfillmentStatus.SHIPPING_PENDING,
                })
        elif isinstance(event, FulfillmentCompleted):
            if self._state:
                self._state = self._state.model_copy(update={
                    "status": FulfillmentStatus.COMPLETED,
                })
        elif isinstance(event, FulfillmentFailed):
            if self._state:
                self._state = self._state.model_copy(update={
                    "status": FulfillmentStatus.FAILED,
                    "failure_reason": event.reason,
                })

    # =========================================================================
    # Workflow Commands
    # =========================================================================

    def start(self, order_id: UUID, total_amount: float) -> None:
        """Start the fulfillment process for an order."""
        if self.version > 0:
            raise ValueError("Fulfillment already started")

        self.apply_event(FulfillmentStarted(
            aggregate_id=self.aggregate_id,
            order_id=order_id,
            total_amount=total_amount,
            aggregate_version=self.get_next_version(),
        ))

    def request_payment(self) -> UUID:
        """Request payment authorization. Returns payment ID."""
        if not self._state:
            raise ValueError("Fulfillment not started")
        if self._state.status != FulfillmentStatus.PAYMENT_PENDING:
            raise ValueError("Payment already requested or process in wrong state")

        payment_id = uuid4()
        self.apply_event(PaymentRequested(
            aggregate_id=self.aggregate_id,
            payment_id=payment_id,
            amount=self._state.total_amount,
            aggregate_version=self.get_next_version(),
        ))
        return payment_id

    def confirm_payment(self, payment_id: UUID) -> None:
        """Record that payment was confirmed."""
        if not self._state or self._state.payment_id != payment_id:
            raise ValueError("Invalid payment ID")

        self.apply_event(PaymentConfirmed(
            aggregate_id=self.aggregate_id,
            payment_id=payment_id,
            aggregate_version=self.get_next_version(),
        ))

    def fail_payment(self, payment_id: UUID, reason: str) -> None:
        """Record that payment failed."""
        if not self._state or self._state.payment_id != payment_id:
            raise ValueError("Invalid payment ID")

        self.apply_event(PaymentFailed(
            aggregate_id=self.aggregate_id,
            payment_id=payment_id,
            reason=reason,
            aggregate_version=self.get_next_version(),
        ))

    def reserve_inventory(self, reservation_id: UUID) -> None:
        """Record that inventory was reserved."""
        if not self._state:
            raise ValueError("Fulfillment not started")
        if self._state.status != FulfillmentStatus.PAYMENT_CONFIRMED:
            raise ValueError("Payment must be confirmed before reserving inventory")

        self.apply_event(InventoryReserved(
            aggregate_id=self.aggregate_id,
            reservation_id=reservation_id,
            aggregate_version=self.get_next_version(),
        ))

    def complete(self) -> None:
        """Mark fulfillment as completed."""
        if not self._state:
            raise ValueError("Fulfillment not started")
        if self._state.status != FulfillmentStatus.SHIPPING_PENDING:
            raise ValueError("Cannot complete in current state")

        self.apply_event(FulfillmentCompleted(
            aggregate_id=self.aggregate_id,
            aggregate_version=self.get_next_version(),
        ))

    def fail(self, reason: str) -> None:
        """Mark fulfillment as failed."""
        if not self._state:
            raise ValueError("Fulfillment not started")

        self.apply_event(FulfillmentFailed(
            aggregate_id=self.aggregate_id,
            reason=reason,
            aggregate_version=self.get_next_version(),
        ))
```

### Orchestrating the Process

In a real application, a service or projection would listen to events and drive the process:

```python
async def handle_order_placed(event: OrderPlaced) -> None:
    """Start fulfillment when an order is placed."""
    fulfillment_id = uuid4()
    process = OrderFulfillmentProcess(fulfillment_id)
    process.start(event.aggregate_id, event.total)

    # Request payment
    payment_id = process.request_payment()

    # Save the process
    await fulfillment_repo.save(process)

    # Trigger payment service (external system)
    await payment_service.authorize(payment_id, event.total)


async def handle_payment_authorized(event: PaymentAuthorizedExternal) -> None:
    """Continue fulfillment when payment is authorized."""
    process = await fulfillment_repo.load_by_payment_id(event.payment_id)
    process.confirm_payment(event.payment_id)

    # Reserve inventory
    reservation_id = await inventory_service.reserve(process.state.order_id)
    process.reserve_inventory(reservation_id)

    # Complete fulfillment
    process.complete()
    await fulfillment_repo.save(process)
```

---

## Event Upcasting

Event upcasting transforms old event formats into new formats during replay. Unlike schema versioning (which handles aggregate state), upcasting handles event structure evolution.

### When to Use Upcasting

- Renaming event fields
- Adding required fields with computed defaults
- Changing field types
- Splitting events into multiple events

### Implementing an Upcaster Registry

```python
from typing import Any, Callable


class EventUpcaster:
    """Registry for event upcasters."""

    def __init__(self):
        self._upcasters: dict[str, list[Callable[[dict], dict]]] = {}

    def register(
        self,
        event_type: str,
        from_version: int,
        to_version: int,
        transformer: Callable[[dict], dict],
    ) -> None:
        """
        Register an upcaster for a specific event type and version range.

        Args:
            event_type: The event type to upcast
            from_version: Source version
            to_version: Target version
            transformer: Function that transforms event data
        """
        key = f"{event_type}:{from_version}:{to_version}"
        if key not in self._upcasters:
            self._upcasters[key] = []
        self._upcasters[key].append(transformer)

    def upcast(
        self,
        event_type: str,
        event_data: dict,
        from_version: int,
        to_version: int,
    ) -> dict:
        """
        Upcast event data from one version to another.

        Args:
            event_type: The event type
            event_data: Raw event data dictionary
            from_version: Current version of the data
            to_version: Target version

        Returns:
            Transformed event data
        """
        current_version = from_version
        result = event_data.copy()

        while current_version < to_version:
            key = f"{event_type}:{current_version}:{current_version + 1}"
            if key in self._upcasters:
                for transformer in self._upcasters[key]:
                    result = transformer(result)
            current_version += 1

        return result


# Global upcaster registry
event_upcaster = EventUpcaster()


# Example: Upcast OrderCreated from V1 to V2
def upcast_order_created_v1_to_v2(data: dict) -> dict:
    """
    V1 had 'customer_name' as single field.
    V2 splits it into 'first_name' and 'last_name'.
    """
    if "customer_name" in data:
        parts = data["customer_name"].split(" ", 1)
        data["first_name"] = parts[0]
        data["last_name"] = parts[1] if len(parts) > 1 else ""
        del data["customer_name"]
    return data


# Register the upcaster
event_upcaster.register("OrderCreated", 1, 2, upcast_order_created_v1_to_v2)


# Usage during event loading
def load_and_upcast_event(raw_event: dict, current_schema_version: int) -> dict:
    """Load an event and upcast it to current version."""
    event_type = raw_event.get("event_type")
    event_version = raw_event.get("schema_version", 1)

    if event_version < current_schema_version:
        return event_upcaster.upcast(
            event_type,
            raw_event,
            event_version,
            current_schema_version,
        )
    return raw_event
```

### Upcasting Best Practices

1. **Version events**: Include `schema_version` in event metadata
2. **Chain upcasters**: Each upcaster moves one version (V1->V2, V2->V3)
3. **Test thoroughly**: Ensure old events load correctly after upcasting
4. **Document changes**: Keep clear documentation of what changed per version
5. **Consider rollback**: Plan for how to handle rollback scenarios

---

## State Validation and Invariants

Aggregates enforce business rules through invariants - conditions that must always be true. Robust validation prevents invalid states.

### Validation Strategies

| Strategy | When | Example |
|----------|------|---------|
| **Pre-command** | Before emitting event | Check balance before withdrawal |
| **Post-event** | After applying event | Verify state consistency |
| **Continuous** | Every state change | Ensure invariants always hold |

### Implementing Invariant Validation

```python
from uuid import UUID, uuid4
from pydantic import BaseModel, field_validator
from eventsource import AggregateRoot, DomainEvent, register_event


# Events
@register_event
class InventoryInitialized(DomainEvent):
    event_type: str = "InventoryInitialized"
    aggregate_type: str = "Inventory"
    product_id: str
    initial_quantity: int


@register_event
class InventoryReserved(DomainEvent):
    event_type: str = "InventoryReserved"
    aggregate_type: str = "Inventory"
    quantity: int
    reservation_id: UUID


@register_event
class InventoryReleased(DomainEvent):
    event_type: str = "InventoryReleased"
    aggregate_type: str = "Inventory"
    quantity: int
    reservation_id: UUID


@register_event
class InventoryAdjusted(DomainEvent):
    event_type: str = "InventoryAdjusted"
    aggregate_type: str = "Inventory"
    adjustment: int  # Can be positive or negative
    reason: str


class InventoryState(BaseModel):
    """State with Pydantic validation."""
    product_id: str
    available: int = 0
    reserved: int = 0

    @field_validator("available", "reserved")
    @classmethod
    def must_be_non_negative(cls, v: int) -> int:
        if v < 0:
            raise ValueError("Inventory quantities cannot be negative")
        return v

    @property
    def total(self) -> int:
        """Total inventory (available + reserved)."""
        return self.available + self.reserved


class InvariantViolationError(Exception):
    """Raised when a business invariant is violated."""
    pass


class InventoryAggregate(AggregateRoot[InventoryState]):
    """
    Inventory aggregate with comprehensive validation.

    Invariants:
    - Available quantity is never negative
    - Reserved quantity is never negative
    - Cannot reserve more than available
    - Total quantity is always consistent
    """
    aggregate_type = "Inventory"

    def _get_initial_state(self) -> InventoryState:
        return InventoryState(product_id=str(self.aggregate_id))

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, InventoryInitialized):
            self._state = InventoryState(
                product_id=event.product_id,
                available=event.initial_quantity,
                reserved=0,
            )
        elif isinstance(event, InventoryReserved):
            if self._state:
                self._state = self._state.model_copy(update={
                    "available": self._state.available - event.quantity,
                    "reserved": self._state.reserved + event.quantity,
                })
        elif isinstance(event, InventoryReleased):
            if self._state:
                self._state = self._state.model_copy(update={
                    "available": self._state.available + event.quantity,
                    "reserved": self._state.reserved - event.quantity,
                })
        elif isinstance(event, InventoryAdjusted):
            if self._state:
                self._state = self._state.model_copy(update={
                    "available": self._state.available + event.adjustment,
                })

        # Post-event invariant check
        self._validate_invariants()

    def _validate_invariants(self) -> None:
        """Validate business invariants after every state change."""
        if not self._state:
            return

        # Invariant 1: Quantities are non-negative (Pydantic handles this)
        # Invariant 2: Total must be consistent
        if self._state.available < 0:
            raise InvariantViolationError(
                f"Available inventory cannot be negative: {self._state.available}"
            )
        if self._state.reserved < 0:
            raise InvariantViolationError(
                f"Reserved inventory cannot be negative: {self._state.reserved}"
            )

    # Commands with pre-validation
    def initialize(self, product_id: str, quantity: int) -> None:
        """Initialize inventory for a product."""
        if self.version > 0:
            raise ValueError("Inventory already initialized")
        if quantity < 0:
            raise ValueError("Initial quantity cannot be negative")

        self.apply_event(InventoryInitialized(
            aggregate_id=self.aggregate_id,
            product_id=product_id,
            initial_quantity=quantity,
            aggregate_version=self.get_next_version(),
        ))

    def reserve(self, quantity: int) -> UUID:
        """Reserve inventory. Returns reservation ID."""
        if not self._state:
            raise ValueError("Inventory not initialized")
        if quantity <= 0:
            raise ValueError("Reservation quantity must be positive")

        # Pre-command validation
        if quantity > self._state.available:
            raise ValueError(
                f"Cannot reserve {quantity}: only {self._state.available} available"
            )

        reservation_id = uuid4()
        self.apply_event(InventoryReserved(
            aggregate_id=self.aggregate_id,
            quantity=quantity,
            reservation_id=reservation_id,
            aggregate_version=self.get_next_version(),
        ))
        return reservation_id

    def release(self, quantity: int, reservation_id: UUID) -> None:
        """Release previously reserved inventory."""
        if not self._state:
            raise ValueError("Inventory not initialized")
        if quantity <= 0:
            raise ValueError("Release quantity must be positive")
        if quantity > self._state.reserved:
            raise ValueError(
                f"Cannot release {quantity}: only {self._state.reserved} reserved"
            )

        self.apply_event(InventoryReleased(
            aggregate_id=self.aggregate_id,
            quantity=quantity,
            reservation_id=reservation_id,
            aggregate_version=self.get_next_version(),
        ))

    def adjust(self, adjustment: int, reason: str) -> None:
        """Adjust inventory (positive to add, negative to remove)."""
        if not self._state:
            raise ValueError("Inventory not initialized")

        # Pre-validation: ensure we do not go negative
        new_available = self._state.available + adjustment
        if new_available < 0:
            raise ValueError(
                f"Adjustment would result in negative inventory: "
                f"{self._state.available} + {adjustment} = {new_available}"
            )

        self.apply_event(InventoryAdjusted(
            aggregate_id=self.aggregate_id,
            adjustment=adjustment,
            reason=reason,
            aggregate_version=self.get_next_version(),
        ))
```

---

## Domain Services

Domain services handle operations that span multiple aggregates or require external resources. They do not have state themselves but coordinate actions.

### When to Use Domain Services

- Operations involving multiple aggregates
- Complex calculations requiring data from multiple sources
- Integration with external services
- Operations that do not naturally fit in a single aggregate

### Implementing a Domain Service

```python
from uuid import UUID
from dataclasses import dataclass
from eventsource import AggregateRepository


@dataclass
class TransferResult:
    """Result of a money transfer operation."""
    transfer_id: UUID
    from_account: UUID
    to_account: UUID
    amount: float
    success: bool
    error_message: str | None = None


class MoneyTransferService:
    """
    Domain service for transferring money between accounts.

    This operation spans two aggregates (source and destination accounts)
    and must be coordinated carefully to maintain consistency.
    """

    def __init__(
        self,
        account_repo: AggregateRepository,
    ):
        self._account_repo = account_repo

    async def transfer(
        self,
        from_account_id: UUID,
        to_account_id: UUID,
        amount: float,
    ) -> TransferResult:
        """
        Transfer money between two accounts.

        Note: In a real system, this would need to be wrapped in a
        saga/process manager for proper compensation handling.

        Args:
            from_account_id: Source account
            to_account_id: Destination account
            amount: Amount to transfer

        Returns:
            TransferResult with success/failure information
        """
        transfer_id = UUID(int=0)  # Generate unique transfer ID

        try:
            # Load both accounts
            from_account = await self._account_repo.load(from_account_id)
            to_account = await self._account_repo.load(to_account_id)

            # Validate
            if from_account.state.balance < amount:
                return TransferResult(
                    transfer_id=transfer_id,
                    from_account=from_account_id,
                    to_account=to_account_id,
                    amount=amount,
                    success=False,
                    error_message="Insufficient funds",
                )

            # Execute withdrawal
            from_account.withdraw(amount)
            await self._account_repo.save(from_account)

            # Execute deposit
            try:
                to_account.deposit(amount)
                await self._account_repo.save(to_account)
            except Exception as e:
                # Compensation: reverse the withdrawal
                from_account_reloaded = await self._account_repo.load(from_account_id)
                from_account_reloaded.deposit(amount)
                await self._account_repo.save(from_account_reloaded)
                raise

            return TransferResult(
                transfer_id=transfer_id,
                from_account=from_account_id,
                to_account=to_account_id,
                amount=amount,
                success=True,
            )

        except Exception as e:
            return TransferResult(
                transfer_id=transfer_id,
                from_account=from_account_id,
                to_account=to_account_id,
                amount=amount,
                success=False,
                error_message=str(e),
            )
```

### Domain Service Best Practices

1. **Stateless**: Services do not hold state; they coordinate aggregates
2. **Single responsibility**: One service per cross-aggregate operation
3. **Compensation**: Plan for failures with compensating actions
4. **Idempotency**: Operations should be safe to retry
5. **Logging**: Log all steps for debugging and audit

---

## Aggregate Factories

Factories provide consistent aggregate instantiation, especially when aggregates have complex initialization requirements.

### Why Use Factories

- Centralize aggregate creation logic
- Handle tenant context injection
- Set up default configurations
- Enforce creation constraints

### Implementing an Aggregate Factory

```python
from abc import ABC, abstractmethod
from uuid import UUID, uuid4
from typing import TypeVar, Generic


T = TypeVar("T")


class AggregateFactory(ABC, Generic[T]):
    """Abstract factory for creating aggregates."""

    @abstractmethod
    def create(self, aggregate_id: UUID | None = None) -> T:
        """Create a new aggregate instance."""
        pass


class TenantAwareOrderFactory(AggregateFactory[OrderAggregate]):
    """Factory that creates orders with tenant context."""

    def __init__(self, tenant_id: UUID):
        self._tenant_id = tenant_id

    def create(self, aggregate_id: UUID | None = None) -> OrderAggregate:
        """Create a new order aggregate for this tenant."""
        aid = aggregate_id or uuid4()
        # If OrderAggregate accepts tenant_id in constructor:
        # return OrderAggregate(aid, tenant_id=self._tenant_id)
        return OrderAggregate(aid)


class ValidatedAccountFactory(AggregateFactory["AccountAggregate"]):
    """Factory that validates account creation constraints."""

    def __init__(self, max_accounts_per_user: int = 5):
        self._max_accounts = max_accounts_per_user
        self._account_counts: dict[UUID, int] = {}

    def create(
        self,
        aggregate_id: UUID | None = None,
        owner_id: UUID | None = None,
    ) -> "AccountAggregate":
        """Create a new account, enforcing limits."""
        if owner_id:
            current_count = self._account_counts.get(owner_id, 0)
            if current_count >= self._max_accounts:
                raise ValueError(
                    f"User {owner_id} has reached maximum accounts ({self._max_accounts})"
                )
            self._account_counts[owner_id] = current_count + 1

        return AccountAggregate(aggregate_id or uuid4())


# Using factory with repository
class TenantAwareRepository:
    """Repository that uses a factory for aggregate creation."""

    def __init__(
        self,
        event_store,
        factory: AggregateFactory,
    ):
        self._event_store = event_store
        self._factory = factory

    def create_new(self, aggregate_id: UUID | None = None):
        """Create a new aggregate using the factory."""
        return self._factory.create(aggregate_id)
```

---

## Advanced Testing Patterns

Testing advanced aggregates requires sophisticated techniques to verify complex behaviors.

### Given-When-Then Pattern

Structure tests to clearly express preconditions, actions, and assertions:

```python
import pytest
from uuid import uuid4


class TestOrderFulfillment:
    """Tests for the fulfillment process manager."""

    @pytest.fixture
    def fulfillment(self):
        """Create a fresh fulfillment process."""
        return OrderFulfillmentProcess(uuid4())

    def test_successful_fulfillment_flow(self, fulfillment):
        """Test the happy path through fulfillment."""
        order_id = uuid4()

        # Given: A started fulfillment process
        fulfillment.start(order_id, total_amount=100.0)
        assert fulfillment.state.status == FulfillmentStatus.PAYMENT_PENDING

        # When: Payment is requested and confirmed
        payment_id = fulfillment.request_payment()
        fulfillment.confirm_payment(payment_id)

        # Then: Status moves to payment confirmed
        assert fulfillment.state.status == FulfillmentStatus.PAYMENT_CONFIRMED

        # When: Inventory is reserved
        reservation_id = uuid4()
        fulfillment.reserve_inventory(reservation_id)

        # Then: Status moves to shipping pending
        assert fulfillment.state.status == FulfillmentStatus.SHIPPING_PENDING

        # When: Fulfillment is completed
        fulfillment.complete()

        # Then: Final status is completed
        assert fulfillment.state.status == FulfillmentStatus.COMPLETED

    def test_payment_failure_marks_fulfillment_failed(self, fulfillment):
        """Test that payment failure properly fails the fulfillment."""
        order_id = uuid4()

        # Given: A fulfillment with payment requested
        fulfillment.start(order_id, total_amount=100.0)
        payment_id = fulfillment.request_payment()

        # When: Payment fails
        fulfillment.fail_payment(payment_id, "Card declined")

        # Then: Fulfillment is marked as failed
        assert fulfillment.state.status == FulfillmentStatus.FAILED
        assert fulfillment.state.failure_reason == "Card declined"


class TestInventoryValidation:
    """Tests for inventory invariant enforcement."""

    @pytest.fixture
    def inventory(self):
        """Create an initialized inventory."""
        inv = InventoryAggregate(uuid4())
        inv.initialize("PROD-001", quantity=100)
        return inv

    def test_cannot_reserve_more_than_available(self, inventory):
        """Test that reservation is limited by available quantity."""
        # Given: 100 units available

        # When/Then: Trying to reserve 150 fails
        with pytest.raises(ValueError) as exc:
            inventory.reserve(150)

        assert "only 100 available" in str(exc.value)

    def test_reserve_updates_quantities_correctly(self, inventory):
        """Test that reservation moves units from available to reserved."""
        # Given: 100 available, 0 reserved

        # When: 30 units are reserved
        inventory.reserve(30)

        # Then: Quantities are updated
        assert inventory.state.available == 70
        assert inventory.state.reserved == 30
        assert inventory.state.total == 100  # Total unchanged

    def test_negative_adjustment_cannot_exceed_available(self, inventory):
        """Test that adjustments cannot make available negative."""
        # Given: 100 available

        # When/Then: Negative adjustment beyond available fails
        with pytest.raises(ValueError) as exc:
            inventory.adjust(-150, "Correction")

        assert "negative inventory" in str(exc.value)


class TestSchemaVersioning:
    """Tests for schema migration in aggregates."""

    def test_v1_snapshot_migrates_to_v2(self):
        """Test that V1 snapshots are properly migrated."""
        # Given: A V1 snapshot without transaction_count
        v1_snapshot = {
            "account_id": str(uuid4()),
            "owner_name": "Test User",
            "balance": 1000.0,
            # Note: no transaction_count field
        }

        # When: Restoring from snapshot
        aggregate = AccountAggregate(UUID(v1_snapshot["account_id"]))
        aggregate._restore_from_snapshot(v1_snapshot, schema_version=1)

        # Then: transaction_count is added with default value
        assert aggregate.state.transaction_count == 0
        assert aggregate.state.balance == 1000.0
```

### Testing Event Replay

```python
def test_aggregate_state_from_events():
    """Test that replaying events produces correct state."""
    account_id = uuid4()

    # Build events manually
    events = [
        AccountOpened(
            aggregate_id=account_id,
            owner_name="Test User",
            initial_balance=100.0,
            aggregate_version=1,
        ),
        MoneyDeposited(
            aggregate_id=account_id,
            amount=50.0,
            aggregate_version=2,
        ),
        MoneyDeposited(
            aggregate_id=account_id,
            amount=25.0,
            aggregate_version=3,
        ),
    ]

    # Replay events
    account = AccountAggregate(account_id)
    account.load_from_history(events)

    # Verify state
    assert account.version == 3
    assert account.state.balance == 175.0
    assert account.state.transaction_count == 2  # Two deposits
```

---

## Complete Example

Here is a complete, runnable example that demonstrates all the advanced patterns:

```python
"""
Tutorial 21: Advanced Aggregate Patterns

This capstone example demonstrates advanced aggregate techniques.
Run with: python tutorial_21_advanced.py
"""
import asyncio
from abc import ABC, abstractmethod
from datetime import datetime, UTC
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field
from eventsource import (
    DomainEvent,
    register_event,
    AggregateRoot,
    InMemoryEventStore,
    AggregateRepository,
)


# =============================================================================
# Part 1: Schema Versioning
# =============================================================================

print("=" * 60)
print("Part 1: Schema Versioning")
print("=" * 60)


@register_event
class AccountOpened(DomainEvent):
    event_type: str = "AccountOpened"
    aggregate_type: str = "Account"
    owner_name: str
    initial_balance: float


@register_event
class AccountCredited(DomainEvent):
    event_type: str = "AccountCredited"
    aggregate_type: str = "Account"
    amount: float
    description: str = ""


class AccountStateV2(BaseModel):
    """Version 2 state with transaction tracking."""
    account_id: UUID
    owner_name: str = ""
    balance: float = 0.0
    transaction_count: int = 0  # New in V2
    created_at: datetime | None = None


class AccountAggregate(AggregateRoot[AccountStateV2]):
    """Account with schema versioning support."""
    aggregate_type = "Account"
    schema_version = 2

    def _get_initial_state(self) -> AccountStateV2:
        return AccountStateV2(account_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, AccountOpened):
            self._state = AccountStateV2(
                account_id=self.aggregate_id,
                owner_name=event.owner_name,
                balance=event.initial_balance,
                transaction_count=0,
                created_at=event.occurred_at,
            )
        elif isinstance(event, AccountCredited):
            if self._state:
                self._state = self._state.model_copy(update={
                    "balance": self._state.balance + event.amount,
                    "transaction_count": self._state.transaction_count + 1,
                })

    def _restore_from_snapshot(self, state: dict, schema_version: int) -> None:
        if schema_version < 2:
            state = self._migrate_v1_to_v2(state)
        self._state = AccountStateV2.model_validate(state)

    def _migrate_v1_to_v2(self, state: dict) -> dict:
        if "transaction_count" not in state:
            state["transaction_count"] = 0
        return state

    def open(self, owner_name: str, initial_balance: float = 0.0) -> None:
        if self.version > 0:
            raise ValueError("Account already exists")
        self.apply_event(AccountOpened(
            aggregate_id=self.aggregate_id,
            owner_name=owner_name,
            initial_balance=initial_balance,
            aggregate_version=self.get_next_version(),
        ))

    def credit(self, amount: float, description: str = "") -> None:
        if amount <= 0:
            raise ValueError("Amount must be positive")
        self.apply_event(AccountCredited(
            aggregate_id=self.aggregate_id,
            amount=amount,
            description=description,
            aggregate_version=self.get_next_version(),
        ))


# =============================================================================
# Part 2: Value Objects and Composition
# =============================================================================

print("\n" + "=" * 60)
print("Part 2: Aggregate Composition")
print("=" * 60)


class Money(BaseModel):
    """Immutable money value object."""
    amount: float
    currency: str = "USD"

    class Config:
        frozen = True

    def add(self, other: "Money") -> "Money":
        if self.currency != other.currency:
            raise ValueError("Currency mismatch")
        return Money(amount=self.amount + other.amount, currency=self.currency)


class Address(BaseModel):
    """Immutable address value object."""
    street: str
    city: str
    country: str
    postal_code: str

    class Config:
        frozen = True


class OrderLine(BaseModel):
    """Order line entity."""
    line_id: UUID
    product_id: str
    quantity: int
    unit_price: float

    @property
    def total(self) -> float:
        return self.quantity * self.unit_price


@register_event
class OrderCreated(DomainEvent):
    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    customer_id: UUID
    shipping_address: dict


@register_event
class OrderLineAdded(DomainEvent):
    event_type: str = "OrderLineAdded"
    aggregate_type: str = "Order"
    line_id: UUID
    product_id: str
    quantity: int
    unit_price: float


class OrderState(BaseModel):
    order_id: UUID
    customer_id: UUID | None = None
    shipping_address: Address | None = None
    lines: list[OrderLine] = Field(default_factory=list)
    status: str = "draft"

    @property
    def total(self) -> Money:
        return Money(amount=sum(line.total for line in self.lines))


class OrderAggregate(AggregateRoot[OrderState]):
    """Order aggregate with composition."""
    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            self._state = OrderState(
                order_id=self.aggregate_id,
                customer_id=event.customer_id,
                shipping_address=Address(**event.shipping_address),
                status="created",
            )
        elif isinstance(event, OrderLineAdded):
            if self._state:
                line = OrderLine(
                    line_id=event.line_id,
                    product_id=event.product_id,
                    quantity=event.quantity,
                    unit_price=event.unit_price,
                )
                self._state = self._state.model_copy(update={
                    "lines": [*self._state.lines, line]
                })

    def create(self, customer_id: UUID, address: Address) -> None:
        if self.version > 0:
            raise ValueError("Order already exists")
        self.apply_event(OrderCreated(
            aggregate_id=self.aggregate_id,
            customer_id=customer_id,
            shipping_address=address.model_dump(),
            aggregate_version=self.get_next_version(),
        ))

    def add_line(self, product_id: str, quantity: int, unit_price: float) -> UUID:
        if not self._state or self._state.status != "created":
            raise ValueError("Cannot modify order")
        line_id = uuid4()
        self.apply_event(OrderLineAdded(
            aggregate_id=self.aggregate_id,
            line_id=line_id,
            product_id=product_id,
            quantity=quantity,
            unit_price=unit_price,
            aggregate_version=self.get_next_version(),
        ))
        return line_id


# =============================================================================
# Part 3: Process Manager
# =============================================================================

print("\n" + "=" * 60)
print("Part 3: Process Manager (Saga)")
print("=" * 60)


class FulfillmentStatus(Enum):
    STARTED = "started"
    PAYMENT_PENDING = "payment_pending"
    PAYMENT_CONFIRMED = "payment_confirmed"
    SHIPPING_PENDING = "shipping_pending"
    COMPLETED = "completed"
    FAILED = "failed"


@register_event
class FulfillmentStarted(DomainEvent):
    event_type: str = "FulfillmentStarted"
    aggregate_type: str = "Fulfillment"
    order_id: UUID
    total_amount: float


@register_event
class PaymentRequested(DomainEvent):
    event_type: str = "PaymentRequested"
    aggregate_type: str = "Fulfillment"
    payment_id: UUID


@register_event
class PaymentConfirmed(DomainEvent):
    event_type: str = "PaymentConfirmed"
    aggregate_type: str = "Fulfillment"
    payment_id: UUID


class FulfillmentState(BaseModel):
    fulfillment_id: UUID
    order_id: UUID | None = None
    payment_id: UUID | None = None
    status: FulfillmentStatus = FulfillmentStatus.STARTED
    total_amount: float = 0.0


class OrderFulfillmentProcess(AggregateRoot[FulfillmentState]):
    """Process Manager for order fulfillment."""
    aggregate_type = "Fulfillment"

    def _get_initial_state(self) -> FulfillmentState:
        return FulfillmentState(fulfillment_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, FulfillmentStarted):
            self._state = FulfillmentState(
                fulfillment_id=self.aggregate_id,
                order_id=event.order_id,
                total_amount=event.total_amount,
                status=FulfillmentStatus.PAYMENT_PENDING,
            )
        elif isinstance(event, PaymentRequested):
            if self._state:
                self._state = self._state.model_copy(update={
                    "payment_id": event.payment_id,
                })
        elif isinstance(event, PaymentConfirmed):
            if self._state:
                self._state = self._state.model_copy(update={
                    "status": FulfillmentStatus.SHIPPING_PENDING,
                })

    def start(self, order_id: UUID, total_amount: float) -> None:
        if self.version > 0:
            raise ValueError("Already started")
        self.apply_event(FulfillmentStarted(
            aggregate_id=self.aggregate_id,
            order_id=order_id,
            total_amount=total_amount,
            aggregate_version=self.get_next_version(),
        ))

    def request_payment(self) -> UUID:
        if not self._state:
            raise ValueError("Not started")
        payment_id = uuid4()
        self.apply_event(PaymentRequested(
            aggregate_id=self.aggregate_id,
            payment_id=payment_id,
            aggregate_version=self.get_next_version(),
        ))
        return payment_id

    def confirm_payment(self, payment_id: UUID) -> None:
        if not self._state or self._state.payment_id != payment_id:
            raise ValueError("Invalid payment")
        self.apply_event(PaymentConfirmed(
            aggregate_id=self.aggregate_id,
            payment_id=payment_id,
            aggregate_version=self.get_next_version(),
        ))


# =============================================================================
# Part 4: State Validation
# =============================================================================

print("\n" + "=" * 60)
print("Part 4: State Validation")
print("=" * 60)


@register_event
class InventoryInitialized(DomainEvent):
    event_type: str = "InventoryInitialized"
    aggregate_type: str = "Inventory"
    product_id: str
    initial_quantity: int


@register_event
class InventoryReserved(DomainEvent):
    event_type: str = "InventoryReserved"
    aggregate_type: str = "Inventory"
    quantity: int
    reservation_id: UUID


class InventoryState(BaseModel):
    product_id: str
    available: int = 0
    reserved: int = 0

    @property
    def total(self) -> int:
        return self.available + self.reserved


class InventoryAggregate(AggregateRoot[InventoryState]):
    """Inventory with validation."""
    aggregate_type = "Inventory"

    def _get_initial_state(self) -> InventoryState:
        return InventoryState(product_id=str(self.aggregate_id))

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, InventoryInitialized):
            self._state = InventoryState(
                product_id=event.product_id,
                available=event.initial_quantity,
                reserved=0,
            )
        elif isinstance(event, InventoryReserved):
            if self._state:
                self._state = self._state.model_copy(update={
                    "available": self._state.available - event.quantity,
                    "reserved": self._state.reserved + event.quantity,
                })
        self._validate_invariants()

    def _validate_invariants(self) -> None:
        if self._state:
            if self._state.available < 0:
                raise ValueError("Available cannot be negative")
            if self._state.reserved < 0:
                raise ValueError("Reserved cannot be negative")

    def initialize(self, product_id: str, quantity: int) -> None:
        if quantity < 0:
            raise ValueError("Quantity cannot be negative")
        self.apply_event(InventoryInitialized(
            aggregate_id=self.aggregate_id,
            product_id=product_id,
            initial_quantity=quantity,
            aggregate_version=self.get_next_version(),
        ))

    def reserve(self, quantity: int) -> UUID:
        if not self._state:
            raise ValueError("Not initialized")
        if quantity > self._state.available:
            raise ValueError(f"Insufficient: {self._state.available} available")
        reservation_id = uuid4()
        self.apply_event(InventoryReserved(
            aggregate_id=self.aggregate_id,
            quantity=quantity,
            reservation_id=reservation_id,
            aggregate_version=self.get_next_version(),
        ))
        return reservation_id


# =============================================================================
# Demo
# =============================================================================

async def main():
    print("\n" + "=" * 60)
    print("Running Advanced Pattern Demos")
    print("=" * 60)

    event_store = InMemoryEventStore()

    # Demo 1: Schema Versioning
    print("\n--- Schema Versioning Demo ---")
    account_repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=AccountAggregate,
        aggregate_type="Account",
    )

    account = account_repo.create_new(uuid4())
    account.open("John Doe", 1000.0)
    account.credit(500.0, "Deposit")
    account.credit(250.0, "Transfer")
    await account_repo.save(account)

    loaded = await account_repo.load(account.aggregate_id)
    print(f"Account: {loaded.state.owner_name}")
    print(f"Balance: ${loaded.state.balance:.2f}")
    print(f"Transactions: {loaded.state.transaction_count}")

    # Demo 2: Composition
    print("\n--- Composition Demo ---")
    order_repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=OrderAggregate,
        aggregate_type="Order",
    )

    address = Address(
        street="123 Main St",
        city="Springfield",
        country="USA",
        postal_code="12345",
    )

    order = order_repo.create_new(uuid4())
    order.create(uuid4(), address)
    order.add_line("WIDGET-001", 2, 29.99)
    order.add_line("GADGET-002", 1, 49.99)
    await order_repo.save(order)

    loaded_order = await order_repo.load(order.aggregate_id)
    print(f"Order lines: {len(loaded_order.state.lines)}")
    print(f"Order total: ${loaded_order.state.total.amount:.2f}")

    # Demo 3: Process Manager
    print("\n--- Process Manager Demo ---")
    process_repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=OrderFulfillmentProcess,
        aggregate_type="Fulfillment",
    )

    process = process_repo.create_new(uuid4())
    process.start(order.aggregate_id, loaded_order.state.total.amount)
    payment_id = process.request_payment()
    process.confirm_payment(payment_id)
    await process_repo.save(process)

    loaded_process = await process_repo.load(process.aggregate_id)
    print(f"Fulfillment status: {loaded_process.state.status.value}")

    # Demo 4: Validation
    print("\n--- Validation Demo ---")
    inventory_repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=InventoryAggregate,
        aggregate_type="Inventory",
    )

    inventory = inventory_repo.create_new(uuid4())
    inventory.initialize("WIDGET-001", 100)
    reservation_id = inventory.reserve(25)
    await inventory_repo.save(inventory)

    loaded_inv = await inventory_repo.load(inventory.aggregate_id)
    print(f"Available: {loaded_inv.state.available}")
    print(f"Reserved: {loaded_inv.state.reserved}")

    # Try to over-reserve
    try:
        loaded_inv.reserve(100)
    except ValueError as e:
        print(f"Validation caught: {e}")

    print("\n" + "=" * 60)
    print("Tutorial 21 Complete!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
```

Save this as `tutorial_21_advanced.py` and run it:

```bash
python tutorial_21_advanced.py
```

---

## Exercises

### Exercise 1: Complete E-Commerce Domain Model

**Objective:** Build a complete e-commerce domain with all advanced patterns.

**Time:** 45-60 minutes

**Requirements:**

1. Create an `Order` aggregate with:
   - Schema versioning (version 2 with new `discount_applied` field)
   - Value objects for Money and Address
   - Child entities for order lines
   - Validation preventing negative totals

2. Create a `Payment` aggregate with:
   - State validation ensuring amounts match
   - Status transitions (pending, authorized, captured, refunded)

3. Create an `OrderFulfillmentSaga` process manager that:
   - Coordinates Order, Payment, and Inventory
   - Handles payment authorization
   - Handles inventory reservation
   - Implements compensation for failures

4. Create comprehensive tests using Given-When-Then pattern

**Starter Code:**

```python
"""
Tutorial 21 - Exercise 1: Complete E-Commerce Domain

Your task: Build a complete e-commerce domain model using advanced patterns.
"""
import asyncio
from uuid import uuid4, UUID

from pydantic import BaseModel
from eventsource import (
    AggregateRoot,
    DomainEvent,
    register_event,
    InMemoryEventStore,
    AggregateRepository,
)


# Step 1: Define value objects
# TODO: Create Money, Address value objects


# Step 2: Define Order aggregate with schema versioning
# TODO: Create OrderState with discount_applied field (V2)
# TODO: Create OrderAggregate with migration from V1


# Step 3: Define Payment aggregate
# TODO: Create PaymentState and PaymentAggregate


# Step 4: Create OrderFulfillmentSaga
# TODO: Implement process manager with compensation


# Step 5: Write tests
# TODO: Test schema migration, validation, and saga flow


async def main():
    print("=== Exercise 21-1: E-Commerce Domain ===\n")

    # Your implementation here

    print("\n=== Exercise Complete! ===")


if __name__ == "__main__":
    asyncio.run(main())
```

**Hints:**

- Use `_restore_from_snapshot()` for schema migration
- Include `_validate_invariants()` in `_apply()` for continuous validation
- Process manager should track state transitions explicitly
- Use Given-When-Then structure for clear test organization

**Solution structure available in:** `docs/tutorials/exercises/solutions/21-1/`

---

## Summary

In this tutorial, you learned:

- **Schema versioning** ensures snapshot compatibility as aggregates evolve
- **Unregistered event handling** provides forward compatibility during deployments
- **Aggregate composition** uses value objects and child entities for rich domain models
- **Process managers** coordinate cross-aggregate workflows with compensation
- **Event upcasting** transforms old events to new formats during replay
- **State validation** enforces business invariants at multiple levels
- **Domain services** handle operations spanning multiple aggregates
- **Aggregate factories** centralize creation logic and constraints
- **Advanced testing** uses Given-When-Then patterns for clarity

---

## Phase 4 Complete!

Congratulations on completing Phase 4: Advanced Patterns!

You have mastered:

| Topic | Key Concepts |
|-------|-------------|
| Multi-tenancy | Tenant isolation, context propagation |
| Distributed buses | Redis Streams, Kafka, RabbitMQ |
| Observability | OpenTelemetry tracing, metrics |
| Advanced aggregates | Schema versioning, sagas, validation |

---

## Tutorial Series Complete!

Congratulations! You have completed the entire eventsource-py tutorial series!

### What You Have Learned

| Phase | Key Concepts |
|-------|-------------|
| **Phase 1: Foundations** | Events, Aggregates, Event Stores, Repositories |
| **Phase 2: Core Patterns** | Projections, Event Bus, Testing, Error Handling, Checkpoints |
| **Phase 3: Production** | PostgreSQL, SQLite, Subscriptions, Snapshotting, Deployment |
| **Phase 4: Advanced** | Multi-tenancy, Distributed Buses, Observability, Advanced Aggregates |

### Your Journey

Over the course of 21 tutorials, you have progressed from understanding basic event sourcing concepts to mastering advanced patterns for enterprise applications. You now have the knowledge to:

- Build event-sourced applications from scratch
- Choose appropriate storage backends for your use case
- Implement projections for read models
- Handle errors gracefully with recovery patterns
- Deploy to production with proper monitoring
- Build multi-tenant SaaS applications
- Distribute events across services
- Observe and debug complex event flows
- Design sophisticated aggregate patterns

### Next Steps

Your event sourcing journey continues beyond these tutorials:

1. **Build a real application** - Apply these patterns to a project you care about
2. **Explore the API reference** - Discover capabilities not covered in tutorials
3. **Read the guides** - Deep-dive into specific topics
4. **Join the community** - Share your experiences and learn from others
5. **Contribute** - Help improve the library for everyone

---

## Key Takeaways

!!! success "Mastery Achieved"
    You now have the knowledge to build production-grade
    event-sourced applications with eventsource-py.

!!! tip "Remember the Fundamentals"
    Even with advanced patterns, the core principles remain:
    - Events are the source of truth
    - Aggregates enforce business rules
    - Projections build read models
    - Eventual consistency enables scalability

!!! note "When to Use Advanced Patterns"
    Not every application needs sagas or complex composition.
    Start simple and add complexity only when requirements demand it.

---

## Related Documentation

- [FAQ](../faq.md) - Frequently asked questions

---

**Thank you for completing the eventsource-py tutorial series!**
