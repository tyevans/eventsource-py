# Tutorial 21: Advanced Aggregate Patterns

**Difficulty:** Advanced | **Progress:** Tutorial 21 of 21 | **Phase:** 4: Advanced Patterns

## Prerequisites

- [Tutorial 1: Introduction to Event Sourcing](01-introduction.md)
- [Tutorial 3: Building Your First Aggregate](03-first-aggregate.md)
- [Tutorial 5: Repositories and Aggregate Lifecycle](05-repositories.md)
- [Tutorial 8: Testing Event-Sourced Systems](08-testing.md)
- [Tutorial 14: Optimizing with Aggregate Snapshotting](14-snapshotting.md)
- Python 3.10 or higher
- Strong understanding of aggregates and event sourcing fundamentals

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Design aggregates with complex state management using value objects and child entities
2. Implement aggregate schema versioning for backward-compatible evolution
3. Build sophisticated business rule validation with multiple invariants
4. Handle unregistered events gracefully during deployments and rollbacks
5. Create aggregates that manage collections and hierarchical data
6. Apply advanced patterns for domain modeling with aggregates
7. Test complex aggregate behaviors comprehensively

---

## What Makes an Aggregate "Advanced"?

You've already learned the basics of aggregates in Tutorial 3. Advanced aggregates differ in their complexity:

| Basic Aggregate | Advanced Aggregate |
|----------------|-------------------|
| Simple scalar state | Nested value objects and entities |
| 2-3 events | 10+ events with complex transitions |
| Single invariant | Multiple interrelated invariants |
| Never changes schema | Evolves with versioning strategy |
| Standalone logic | Coordinates with other aggregates |

Advanced aggregates are necessary when:

- **Domain complexity increases**: E-commerce orders, financial accounts, project management
- **State relationships matter**: Parent-child entities, collections, nested structures
- **Evolution is required**: Long-lived systems that must change over time
- **Business rules are intricate**: Multiple conditions, cross-field validation

This tutorial covers patterns for building production-grade aggregates that handle real-world complexity.

---

## Aggregate Composition with Value Objects and Entities

Complex domains require aggregates that contain more than simple fields. Understanding composition is essential for good domain modeling.

### Value Objects vs Entities

| Concept | Identity | Mutability | Comparison | Example |
|---------|----------|------------|------------|---------|
| **Value Object** | Defined by attributes | Immutable | By value | Money, Address, DateRange |
| **Child Entity** | Has unique ID | Mutable via aggregate | By identity | OrderLine, CartItem, Comment |

**Key principle**: Value objects have no identity - two `Money(100, "USD")` instances are interchangeable. Child entities have identity - `OrderLine(id=123)` is distinct from `OrderLine(id=456)` even with identical data.

### Implementing Value Objects

Value objects are immutable domain concepts:

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

    def multiply(self, factor: float) -> "Money":
        """Multiply by a factor, returning a new Money instance."""
        return Money(amount=self.amount * factor, currency=self.currency)

    def __str__(self) -> str:
        return f"{self.currency} {self.amount:.2f}"


class Address(BaseModel):
    """Immutable address value object."""

    street: str
    city: str
    state: str
    postal_code: str
    country: str = "USA"

    class Config:
        frozen = True

    def format_single_line(self) -> str:
        """Format address on a single line."""
        return f"{self.street}, {self.city}, {self.state} {self.postal_code}"
```

**Why immutable?** Value objects are compared by value, not identity. Immutability ensures they can be safely shared and compared.

### Implementing Child Entities

Child entities have identity within their parent aggregate:

```python
from uuid import UUID, uuid4
from pydantic import BaseModel, Field


class OrderLine(BaseModel):
    """Order line entity with identity."""

    line_id: UUID = Field(default_factory=uuid4)
    product_id: str
    product_name: str
    quantity: int
    unit_price: Money

    @property
    def line_total(self) -> Money:
        """Calculate total for this line."""
        return self.unit_price.multiply(float(self.quantity))

    def with_quantity(self, new_quantity: int) -> "OrderLine":
        """Return a copy with updated quantity."""
        return self.model_copy(update={"quantity": new_quantity})
```

**Important**: Child entity IDs are unique within their parent aggregate, not globally. Two different orders can have `OrderLine` entities, but within each order, line IDs must be unique.

### Composing the Aggregate

Now let's build an aggregate that uses both:

```python
from uuid import UUID, uuid4
from datetime import datetime, UTC
from pydantic import BaseModel, Field
from eventsource import AggregateRoot, DomainEvent, register_event


# ============================================================================
# Events
# ============================================================================

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
    unit_price_amount: float
    unit_price_currency: str


@register_event
class OrderLineQuantityChanged(DomainEvent):
    event_type: str = "OrderLineQuantityChanged"
    aggregate_type: str = "Order"

    line_id: UUID
    old_quantity: int
    new_quantity: int


@register_event
class OrderLineRemoved(DomainEvent):
    event_type: str = "OrderLineRemoved"
    aggregate_type: str = "Order"

    line_id: UUID


@register_event
class OrderShipped(DomainEvent):
    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"

    shipped_at: datetime


# ============================================================================
# State with Composition
# ============================================================================

class OrderState(BaseModel):
    """Order state with value objects and child entities."""

    order_id: UUID
    customer_id: UUID | None = None
    shipping_address: Address | None = None
    lines: list[OrderLine] = Field(default_factory=list)
    status: str = "draft"  # draft, placed, shipped
    created_at: datetime | None = None
    shipped_at: datetime | None = None

    @property
    def total(self) -> Money:
        """Calculate order total from all lines."""
        if not self.lines:
            return Money(amount=0.0, currency="USD")

        # Sum all line totals
        total_amount = sum(line.line_total.amount for line in self.lines)
        # Use currency from first line (all should match)
        currency = self.lines[0].unit_price.currency if self.lines else "USD"
        return Money(amount=total_amount, currency=currency)

    @property
    def line_count(self) -> int:
        """Number of lines in the order."""
        return len(self.lines)

    @property
    def item_count(self) -> int:
        """Total number of items across all lines."""
        return sum(line.quantity for line in self.lines)

    def find_line(self, line_id: UUID) -> OrderLine | None:
        """Find a line by ID."""
        for line in self.lines:
            if line.line_id == line_id:
                return line
        return None


# ============================================================================
# Aggregate
# ============================================================================

class OrderAggregate(AggregateRoot[OrderState]):
    """
    Order aggregate demonstrating composition patterns.

    Manages a collection of OrderLine entities and uses Address value objects.
    """

    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            self._state = OrderState(
                order_id=self.aggregate_id,
                customer_id=event.customer_id,
                shipping_address=Address(**event.shipping_address),
                status="draft",
                created_at=event.occurred_at,
            )

        elif isinstance(event, OrderLineAdded):
            if self._state:
                new_line = OrderLine(
                    line_id=event.line_id,
                    product_id=event.product_id,
                    product_name=event.product_name,
                    quantity=event.quantity,
                    unit_price=Money(
                        amount=event.unit_price_amount,
                        currency=event.unit_price_currency,
                    ),
                )
                self._state = self._state.model_copy(
                    update={"lines": [*self._state.lines, new_line]}
                )

        elif isinstance(event, OrderLineQuantityChanged):
            if self._state:
                updated_lines = [
                    line.with_quantity(event.new_quantity)
                    if line.line_id == event.line_id
                    else line
                    for line in self._state.lines
                ]
                self._state = self._state.model_copy(update={"lines": updated_lines})

        elif isinstance(event, OrderLineRemoved):
            if self._state:
                updated_lines = [
                    line for line in self._state.lines if line.line_id != event.line_id
                ]
                self._state = self._state.model_copy(update={"lines": updated_lines})

        elif isinstance(event, OrderShipped):
            if self._state:
                self._state = self._state.model_copy(
                    update={
                        "status": "shipped",
                        "shipped_at": event.shipped_at,
                    }
                )

    # Command methods

    def create(self, customer_id: UUID, shipping_address: Address) -> None:
        """Create a new order."""
        if self.version > 0:
            raise ValueError("Order already exists")

        self.apply_event(
            OrderCreated(
                aggregate_id=self.aggregate_id,
                customer_id=customer_id,
                shipping_address=shipping_address.model_dump(),
                aggregate_version=self.get_next_version(),
            )
        )

    def add_line(
        self, product_id: str, product_name: str, quantity: int, unit_price: Money
    ) -> UUID:
        """Add a line item. Returns the line ID."""
        if not self._state:
            raise ValueError("Order does not exist")
        if self._state.status != "draft":
            raise ValueError(f"Cannot modify order in {self._state.status} status")
        if quantity <= 0:
            raise ValueError("Quantity must be positive")

        line_id = uuid4()
        self.apply_event(
            OrderLineAdded(
                aggregate_id=self.aggregate_id,
                line_id=line_id,
                product_id=product_id,
                product_name=product_name,
                quantity=quantity,
                unit_price_amount=unit_price.amount,
                unit_price_currency=unit_price.currency,
                aggregate_version=self.get_next_version(),
            )
        )
        return line_id

    def change_line_quantity(self, line_id: UUID, new_quantity: int) -> None:
        """Change quantity for a line."""
        if not self._state:
            raise ValueError("Order does not exist")
        if self._state.status != "draft":
            raise ValueError(f"Cannot modify order in {self._state.status} status")

        line = self._state.find_line(line_id)
        if not line:
            raise ValueError(f"Line {line_id} not found")
        if new_quantity <= 0:
            raise ValueError("Quantity must be positive")

        self.apply_event(
            OrderLineQuantityChanged(
                aggregate_id=self.aggregate_id,
                line_id=line_id,
                old_quantity=line.quantity,
                new_quantity=new_quantity,
                aggregate_version=self.get_next_version(),
            )
        )

    def remove_line(self, line_id: UUID) -> None:
        """Remove a line item."""
        if not self._state:
            raise ValueError("Order does not exist")
        if self._state.status != "draft":
            raise ValueError(f"Cannot modify order in {self._state.status} status")

        if not self._state.find_line(line_id):
            raise ValueError(f"Line {line_id} not found")

        self.apply_event(
            OrderLineRemoved(
                aggregate_id=self.aggregate_id,
                line_id=line_id,
                aggregate_version=self.get_next_version(),
            )
        )

    def ship(self) -> None:
        """Ship the order."""
        if not self._state:
            raise ValueError("Order does not exist")
        if self._state.status == "shipped":
            raise ValueError("Order already shipped")
        if not self._state.lines:
            raise ValueError("Cannot ship empty order")

        self.apply_event(
            OrderShipped(
                aggregate_id=self.aggregate_id,
                shipped_at=datetime.now(UTC),
                aggregate_version=self.get_next_version(),
            )
        )
```

### Design Guidelines for Composition

1. **Aggregate boundaries**: Only the root aggregate can be referenced from outside
2. **Child entity access**: Access child entities through the aggregate root only
3. **Identity scope**: Child entity IDs are unique within their parent
4. **Transactional consistency**: All changes within an aggregate are atomic
5. **Value object reuse**: Use value objects across multiple aggregates
6. **Immutability**: Keep value objects immutable to prevent bugs

---

## Schema Versioning for Aggregate Evolution

Production systems evolve. Your aggregates will need new fields, changed structures, or different business rules. Schema versioning ensures old snapshots remain compatible.

### Why Schema Versioning Matters

When using snapshots (Tutorial 14), aggregate state is serialized and stored. If the state model changes, old snapshots may fail to deserialize:

```python
# Version 1 state
class OrderState(BaseModel):
    order_id: UUID
    total: float  # Simple float

# Version 2 state - changed field type!
class OrderState(BaseModel):
    order_id: UUID
    total: Money  # Now a value object!
```

Without versioning, loading a V1 snapshot with V2 code fails because `total` is a float, not a `Money` object.

### The schema_version Attribute

Every aggregate has a `schema_version` class attribute:

```python
from eventsource import AggregateRoot


class OrderAggregate(AggregateRoot[OrderState]):
    aggregate_type = "Order"
    schema_version = 2  # Increment when state structure changes
```

**How it works:**

1. Repository saves snapshots with the aggregate's `schema_version`
2. When loading, repository compares snapshot version with aggregate's version
3. If they don't match, snapshot is discarded and full event replay is performed
4. A new snapshot is created at the current version

### When to Increment schema_version

**DO increment** when:

- Adding required fields without defaults
- Removing existing fields
- Renaming fields
- Changing field types incompatibly (e.g., `float` → `Money`)
- Restructuring nested objects

**DO NOT increment** when:

- Adding optional fields with defaults (Pydantic handles this gracefully)
- Adding new methods to the aggregate
- Changing validation rules only
- Modifying command method logic

### Implementing Schema Migration

For simple cases, just increment `schema_version` and let full replay handle it. For complex migrations, override `_restore_from_snapshot()`:

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
class TransactionRecorded(DomainEvent):
    event_type: str = "TransactionRecorded"
    aggregate_type: str = "Account"

    amount: float
    transaction_type: str  # "credit" or "debit"


class AccountStateV2(BaseModel):
    """Current state - Version 2 with transaction tracking."""

    account_id: UUID
    owner_name: str = ""
    balance: float = 0.0
    transaction_count: int = 0  # New in V2
    last_transaction_at: str | None = None  # New in V2


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
        elif isinstance(event, TransactionRecorded):
            if self._state:
                new_balance = (
                    self._state.balance + event.amount
                    if event.transaction_type == "credit"
                    else self._state.balance - event.amount
                )
                self._state = self._state.model_copy(
                    update={
                        "balance": new_balance,
                        "transaction_count": self._state.transaction_count + 1,
                        "last_transaction_at": event.occurred_at.isoformat(),
                    }
                )

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
        """Migrate V1 state to V2 by adding new fields."""
        # V1 didn't have transaction tracking
        if "transaction_count" not in state:
            state["transaction_count"] = 0
        if "last_transaction_at" not in state:
            state["last_transaction_at"] = None
        return state

    # Command methods

    def open(self, owner_name: str, initial_balance: float = 0.0) -> None:
        """Open a new account."""
        if self.version > 0:
            raise ValueError("Account already exists")

        self.apply_event(
            AccountOpened(
                aggregate_id=self.aggregate_id,
                owner_name=owner_name,
                initial_balance=initial_balance,
                aggregate_version=self.get_next_version(),
            )
        )

    def record_transaction(self, amount: float, transaction_type: str) -> None:
        """Record a transaction."""
        if not self._state:
            raise ValueError("Account does not exist")
        if transaction_type not in ("credit", "debit"):
            raise ValueError("Transaction type must be 'credit' or 'debit'")
        if amount <= 0:
            raise ValueError("Amount must be positive")

        self.apply_event(
            TransactionRecorded(
                aggregate_id=self.aggregate_id,
                amount=amount,
                transaction_type=transaction_type,
                aggregate_version=self.get_next_version(),
            )
        )
```

### Migration Best Practices

| Practice | Description |
|----------|-------------|
| **Sequential migrations** | Apply migrations one version at a time (V1 → V2 → V3) |
| **Non-destructive changes** | Prefer adding optional fields over removing required ones |
| **Test migrations** | Write tests that load V1 snapshots with V2 code |
| **Document changes** | Keep a changelog of schema changes per version |
| **Default values** | Provide sensible defaults for new fields |

---

## Advanced State Validation and Invariants

Aggregates enforce business rules through invariants - conditions that must always be true. Advanced aggregates have multiple interrelated invariants.

### Validation Strategies

| Strategy | When | Example |
|----------|------|---------|
| **Pre-command** | Before emitting event | Check balance before withdrawal |
| **Post-event** | After applying event | Verify state consistency |
| **Continuous** | Every state change | Ensure invariants always hold |

### Multi-Level Validation Example

Let's build an inventory aggregate with comprehensive validation:

```python
from uuid import UUID, uuid4
from pydantic import BaseModel, field_validator
from eventsource import AggregateRoot, DomainEvent, register_event


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
class ReservationReleased(DomainEvent):
    event_type: str = "ReservationReleased"
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
    reservations: dict[str, int] = {}  # reservation_id -> quantity

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
    - Reservation totals match reserved quantity
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
                reservations={},
            )

        elif isinstance(event, InventoryReserved):
            if self._state:
                new_reservations = dict(self._state.reservations)
                new_reservations[str(event.reservation_id)] = event.quantity

                self._state = self._state.model_copy(
                    update={
                        "available": self._state.available - event.quantity,
                        "reserved": self._state.reserved + event.quantity,
                        "reservations": new_reservations,
                    }
                )

        elif isinstance(event, ReservationReleased):
            if self._state:
                new_reservations = dict(self._state.reservations)
                if str(event.reservation_id) in new_reservations:
                    del new_reservations[str(event.reservation_id)]

                self._state = self._state.model_copy(
                    update={
                        "available": self._state.available + event.quantity,
                        "reserved": self._state.reserved - event.quantity,
                        "reservations": new_reservations,
                    }
                )

        elif isinstance(event, InventoryAdjusted):
            if self._state:
                self._state = self._state.model_copy(
                    update={"available": self._state.available + event.adjustment}
                )

        # Post-event invariant check
        self._validate_invariants()

    def _validate_invariants(self) -> None:
        """Validate business invariants after every state change."""
        if not self._state:
            return

        # Invariant 1: Quantities are non-negative (Pydantic validates this)

        # Invariant 2: Reservation totals must match reserved quantity
        reservation_total = sum(self._state.reservations.values())
        if reservation_total != self._state.reserved:
            raise InvariantViolationError(
                f"Reservation total ({reservation_total}) does not match "
                f"reserved quantity ({self._state.reserved})"
            )

    # Commands with pre-validation

    def initialize(self, product_id: str, quantity: int) -> None:
        """Initialize inventory for a product."""
        if self.version > 0:
            raise ValueError("Inventory already initialized")
        if quantity < 0:
            raise ValueError("Initial quantity cannot be negative")

        self.apply_event(
            InventoryInitialized(
                aggregate_id=self.aggregate_id,
                product_id=product_id,
                initial_quantity=quantity,
                aggregate_version=self.get_next_version(),
            )
        )

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
        self.apply_event(
            InventoryReserved(
                aggregate_id=self.aggregate_id,
                quantity=quantity,
                reservation_id=reservation_id,
                aggregate_version=self.get_next_version(),
            )
        )
        return reservation_id

    def release_reservation(self, reservation_id: UUID) -> None:
        """Release a reservation."""
        if not self._state:
            raise ValueError("Inventory not initialized")

        reservation_key = str(reservation_id)
        if reservation_key not in self._state.reservations:
            raise ValueError(f"Reservation {reservation_id} not found")

        quantity = self._state.reservations[reservation_key]
        self.apply_event(
            ReservationReleased(
                aggregate_id=self.aggregate_id,
                quantity=quantity,
                reservation_id=reservation_id,
                aggregate_version=self.get_next_version(),
            )
        )

    def adjust(self, adjustment: int, reason: str) -> None:
        """Adjust inventory (positive to add, negative to remove)."""
        if not self._state:
            raise ValueError("Inventory not initialized")

        # Pre-validation: ensure we don't go negative
        new_available = self._state.available + adjustment
        if new_available < 0:
            raise ValueError(
                f"Adjustment would result in negative inventory: "
                f"{self._state.available} + {adjustment} = {new_available}"
            )

        self.apply_event(
            InventoryAdjusted(
                aggregate_id=self.aggregate_id,
                adjustment=adjustment,
                reason=reason,
                aggregate_version=self.get_next_version(),
            )
        )
```

### Validation Best Practices

1. **Use Pydantic validators** for simple field-level validation
2. **Pre-command validation** for business rules that can fail
3. **Post-event validation** for complex invariants
4. **Clear error messages** that explain what went wrong
5. **Domain exceptions** for business rule violations
6. **Defensive checks** in event handlers

---

## Handling Unregistered Events

In production systems, your aggregate may encounter events it doesn't recognize. This happens during:

- Rolling deployments (new code adds events before all services update)
- Rollbacks (old code sees new event types)
- A/B testing (different versions running simultaneously)

### The Problem

Consider an aggregate with handlers for `OrderCreated` and `OrderShipped`. A new release adds `OrderCancelled`. During rollback, old code encounters `OrderCancelled` events in the event stream but has no handler.

### DeclarativeAggregate Handling Modes

The `DeclarativeAggregate` class provides three modes via `unregistered_event_handling`:

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
| `"ignore"` | Production, forward compatibility | Silently skip unknown events |
| `"warn"` | Development, debugging | Log warning with event details |
| `"error"` | Strict validation, testing | Raise `UnhandledEventError` |

**Recommended approach:**

- Use `"ignore"` in production for resilience
- Use `"warn"` during development to catch missing handlers
- Use `"error"` in tests to ensure all events have handlers

### Example: Development Mode

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

### Handling in AggregateRoot

If using `AggregateRoot` directly (not `DeclarativeAggregate`), handle unknown events in `_apply()`:

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
                "Unhandled event type %s in OrderAggregate version %s",
                type(event).__name__,
                event.aggregate_version,
            )
```

---

## Advanced Testing Patterns

Testing advanced aggregates requires sophisticated techniques to verify complex behaviors.

### Given-When-Then Pattern

Structure tests to clearly express preconditions, actions, and assertions:

```python
import pytest
from uuid import uuid4


class TestInventoryAggregate:
    """Tests for inventory aggregate validation."""

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
        reservation_id = inventory.reserve(30)

        # Then: Quantities are updated
        assert inventory.state.available == 70
        assert inventory.state.reserved == 30
        assert inventory.state.total == 100  # Total unchanged
        assert str(reservation_id) in inventory.state.reservations

    def test_release_reservation_returns_to_available(self, inventory):
        """Test that releasing a reservation returns inventory."""
        # Given: A reservation exists
        reservation_id = inventory.reserve(30)
        assert inventory.state.available == 70
        assert inventory.state.reserved == 30

        # When: Reservation is released
        inventory.release_reservation(reservation_id)

        # Then: Quantities are restored
        assert inventory.state.available == 100
        assert inventory.state.reserved == 0
        assert str(reservation_id) not in inventory.state.reservations

    def test_negative_adjustment_cannot_exceed_available(self, inventory):
        """Test that adjustments cannot make available negative."""
        # Given: 100 available

        # When/Then: Negative adjustment beyond available fails
        with pytest.raises(ValueError) as exc:
            inventory.adjust(-150, "Correction")

        assert "negative inventory" in str(exc.value)

    def test_invariant_validation_on_every_event(self, inventory):
        """Test that invariants are checked after each event."""
        # Given: 100 available
        reservation_id = inventory.reserve(50)

        # State should be valid
        assert inventory.state.available == 50
        assert inventory.state.reserved == 50

        # Internal consistency is maintained
        reservation_total = sum(inventory.state.reservations.values())
        assert reservation_total == inventory.state.reserved
```

### Testing Event Replay

Verify that replaying events produces correct state:

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
        TransactionRecorded(
            aggregate_id=account_id,
            amount=50.0,
            transaction_type="credit",
            aggregate_version=2,
        ),
        TransactionRecorded(
            aggregate_id=account_id,
            amount=25.0,
            transaction_type="debit",
            aggregate_version=3,
        ),
    ]

    # Replay events
    account = AccountAggregate(account_id)
    account.load_from_history(events)

    # Verify state
    assert account.version == 3
    assert account.state.balance == 125.0  # 100 + 50 - 25
    assert account.state.transaction_count == 2
```

### Testing Schema Migration

Ensure old snapshots can be migrated:

```python
def test_v1_snapshot_migrates_to_v2():
    """Test that V1 snapshots are properly migrated."""
    # Given: A V1 snapshot without transaction_count
    v1_snapshot = {
        "account_id": str(uuid4()),
        "owner_name": "Test User",
        "balance": 1000.0,
        # Note: no transaction_count or last_transaction_at fields
    }

    # When: Restoring from snapshot
    aggregate = AccountAggregate(UUID(v1_snapshot["account_id"]))
    aggregate._restore_from_snapshot(v1_snapshot, schema_version=1)

    # Then: Missing fields are added with defaults
    assert aggregate.state.transaction_count == 0
    assert aggregate.state.last_transaction_at is None
    assert aggregate.state.balance == 1000.0
```

### Testing Business Rules

Verify all invariants are enforced:

```python
class TestOrderBusinessRules:
    """Test business rules for Order aggregate."""

    def test_cannot_modify_shipped_order(self):
        """Verify shipped orders cannot be modified."""
        # Given: A shipped order
        order = OrderAggregate(uuid4())
        address = Address(
            street="123 Main St",
            city="Springfield",
            state="IL",
            postal_code="62701",
        )
        order.create(uuid4(), address)
        line_id = order.add_line(
            "PROD-001", "Widget", 2, Money(amount=10.0, currency="USD")
        )
        order.ship()

        # When/Then: Attempts to modify fail
        with pytest.raises(ValueError) as exc:
            order.add_line("PROD-002", "Gadget", 1, Money(amount=5.0, currency="USD"))
        assert "shipped" in str(exc.value).lower()

        with pytest.raises(ValueError) as exc:
            order.change_line_quantity(line_id, 5)
        assert "shipped" in str(exc.value).lower()

    def test_cannot_ship_empty_order(self):
        """Verify empty orders cannot be shipped."""
        # Given: An order with no lines
        order = OrderAggregate(uuid4())
        address = Address(
            street="123 Main St",
            city="Springfield",
            state="IL",
            postal_code="62701",
        )
        order.create(uuid4(), address)

        # When/Then: Shipping fails
        with pytest.raises(ValueError) as exc:
            order.ship()
        assert "empty" in str(exc.value).lower()
```

---

## Complete Working Example

Here's a complete example demonstrating all advanced patterns:

```python
"""
Tutorial 21: Advanced Aggregate Patterns

Complete example demonstrating:
- Aggregate composition with value objects and entities
- Schema versioning and migration
- Multi-level validation and invariants
- Unregistered event handling

Run with: python tutorial_21_advanced.py
"""

import asyncio
from datetime import datetime, UTC
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator

from eventsource import (
    AggregateRepository,
    AggregateRoot,
    DomainEvent,
    InMemoryEventStore,
    register_event,
)


# =============================================================================
# Part 1: Value Objects
# =============================================================================

print("=" * 70)
print("Tutorial 21: Advanced Aggregate Patterns")
print("=" * 70)


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

    def multiply(self, factor: float) -> "Money":
        return Money(amount=self.amount * factor, currency=self.currency)


class Address(BaseModel):
    """Immutable address value object."""

    street: str
    city: str
    state: str
    postal_code: str

    class Config:
        frozen = True


# =============================================================================
# Part 2: Child Entities
# =============================================================================


class OrderLine(BaseModel):
    """Order line entity."""

    line_id: UUID
    product_id: str
    product_name: str
    quantity: int
    unit_price: Money

    @property
    def total(self) -> Money:
        return self.unit_price.multiply(float(self.quantity))

    def with_quantity(self, new_quantity: int) -> "OrderLine":
        return self.model_copy(update={"quantity": new_quantity})


# =============================================================================
# Part 3: Events
# =============================================================================


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
    product_name: str
    quantity: int
    unit_price_amount: float
    unit_price_currency: str


@register_event
class OrderShipped(DomainEvent):
    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"

    shipped_at: datetime


# =============================================================================
# Part 4: Aggregate State (Version 2 with schema versioning)
# =============================================================================


class OrderState(BaseModel):
    """Order state - Version 2 with tracking fields."""

    order_id: UUID
    customer_id: UUID | None = None
    shipping_address: Address | None = None
    lines: list[OrderLine] = Field(default_factory=list)
    status: str = "draft"
    created_at: datetime | None = None
    shipped_at: datetime | None = None
    # New in V2:
    total_line_count: int = 0  # Track total lines added (never decreases)

    @property
    def total(self) -> Money:
        if not self.lines:
            return Money(amount=0.0, currency="USD")
        total_amount = sum(line.total.amount for line in self.lines)
        currency = self.lines[0].unit_price.currency if self.lines else "USD"
        return Money(amount=total_amount, currency=currency)

    @property
    def current_line_count(self) -> int:
        return len(self.lines)

    def find_line(self, line_id: UUID) -> OrderLine | None:
        for line in self.lines:
            if line.line_id == line_id:
                return line
        return None


# =============================================================================
# Part 5: Aggregate with Advanced Patterns
# =============================================================================


class OrderAggregate(AggregateRoot[OrderState]):
    """
    Order aggregate demonstrating:
    - Composition with value objects (Money, Address) and entities (OrderLine)
    - Schema versioning (V2 adds total_line_count)
    - Complex business rules
    """

    aggregate_type = "Order"
    schema_version = 2  # Increment when OrderState changes

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            self._state = OrderState(
                order_id=self.aggregate_id,
                customer_id=event.customer_id,
                shipping_address=Address(**event.shipping_address),
                status="draft",
                created_at=event.occurred_at,
                total_line_count=0,
            )

        elif isinstance(event, OrderLineAdded):
            if self._state:
                new_line = OrderLine(
                    line_id=event.line_id,
                    product_id=event.product_id,
                    product_name=event.product_name,
                    quantity=event.quantity,
                    unit_price=Money(
                        amount=event.unit_price_amount,
                        currency=event.unit_price_currency,
                    ),
                )
                self._state = self._state.model_copy(
                    update={
                        "lines": [*self._state.lines, new_line],
                        "total_line_count": self._state.total_line_count + 1,
                    }
                )

        elif isinstance(event, OrderShipped):
            if self._state:
                self._state = self._state.model_copy(
                    update={
                        "status": "shipped",
                        "shipped_at": event.shipped_at,
                    }
                )

    def _restore_from_snapshot(self, state_dict: dict, schema_version: int) -> None:
        """Handle schema migration from V1 to V2."""
        if schema_version < 2:
            state_dict = self._migrate_v1_to_v2(state_dict)
        self._state = OrderState.model_validate(state_dict)

    def _migrate_v1_to_v2(self, state: dict) -> dict:
        """Migrate V1 state to V2 by adding total_line_count."""
        if "total_line_count" not in state:
            # Calculate from current lines
            state["total_line_count"] = len(state.get("lines", []))
        return state

    # Command methods

    def create(self, customer_id: UUID, shipping_address: Address) -> None:
        if self.version > 0:
            raise ValueError("Order already exists")

        self.apply_event(
            OrderCreated(
                aggregate_id=self.aggregate_id,
                customer_id=customer_id,
                shipping_address=shipping_address.model_dump(),
                aggregate_version=self.get_next_version(),
            )
        )

    def add_line(
        self, product_id: str, product_name: str, quantity: int, unit_price: Money
    ) -> UUID:
        if not self._state:
            raise ValueError("Order does not exist")
        if self._state.status != "draft":
            raise ValueError(f"Cannot modify {self._state.status} order")
        if quantity <= 0:
            raise ValueError("Quantity must be positive")

        line_id = uuid4()
        self.apply_event(
            OrderLineAdded(
                aggregate_id=self.aggregate_id,
                line_id=line_id,
                product_id=product_id,
                product_name=product_name,
                quantity=quantity,
                unit_price_amount=unit_price.amount,
                unit_price_currency=unit_price.currency,
                aggregate_version=self.get_next_version(),
            )
        )
        return line_id

    def ship(self) -> None:
        if not self._state:
            raise ValueError("Order does not exist")
        if self._state.status == "shipped":
            raise ValueError("Order already shipped")
        if not self._state.lines:
            raise ValueError("Cannot ship empty order")

        self.apply_event(
            OrderShipped(
                aggregate_id=self.aggregate_id,
                shipped_at=datetime.now(UTC),
                aggregate_version=self.get_next_version(),
            )
        )


# =============================================================================
# Demo
# =============================================================================


async def main():
    print("\n" + "=" * 70)
    print("Running Advanced Aggregate Demo")
    print("=" * 70)

    event_store = InMemoryEventStore()

    # Demo 1: Composition with value objects and entities
    print("\n--- Demo 1: Aggregate Composition ---")
    repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=OrderAggregate,
        aggregate_type="Order",
    )

    order_id = uuid4()
    customer_id = uuid4()
    address = Address(
        street="123 Main St",
        city="Springfield",
        state="IL",
        postal_code="62701",
    )

    order = repo.create_new(order_id)
    order.create(customer_id, address)

    # Add multiple lines
    line1 = order.add_line("WIDGET-001", "Widget", 2, Money(amount=29.99, currency="USD"))
    line2 = order.add_line("GADGET-002", "Gadget", 1, Money(amount=49.99, currency="USD"))

    await repo.save(order)

    loaded = await repo.load(order_id)
    print(f"Order ID: {loaded.state.order_id}")
    print(f"Lines: {loaded.state.current_line_count}")
    print(f"Total: {loaded.state.total}")
    print(f"Address: {loaded.state.shipping_address.city}, {loaded.state.shipping_address.state}")

    # Demo 2: Business rule enforcement
    print("\n--- Demo 2: Business Rule Validation ---")
    loaded.ship()
    await repo.save(loaded)
    print("Order shipped successfully")

    # Try to add line to shipped order (should fail)
    loaded = await repo.load(order_id)
    try:
        loaded.add_line("PROD-003", "Product", 1, Money(amount=9.99, currency="USD"))
    except ValueError as e:
        print(f"Business rule enforced: {e}")

    # Demo 3: Event replay
    print("\n--- Demo 3: Event Replay ---")
    stream = await event_store.get_events(order_id, "Order")
    print(f"Total events: {len(stream.events)}")
    for i, event in enumerate(stream.events, 1):
        print(f"  [{i}] v{event.aggregate_version}: {event.event_type}")

    # Demo 4: Schema versioning
    print("\n--- Demo 4: Schema Versioning ---")
    print(f"Aggregate schema version: {OrderAggregate.schema_version}")
    print(
        f"Total lines added (V2 field): {loaded.state.total_line_count}"
    )

    print("\n" + "=" * 70)
    print("Tutorial 21 Complete!")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
```

Save this as `tutorial_21_advanced.py` and run it:

```bash
python tutorial_21_advanced.py
```

**Expected output:**
```
======================================================================
Tutorial 21: Advanced Aggregate Patterns
======================================================================

======================================================================
Running Advanced Aggregate Demo
======================================================================

--- Demo 1: Aggregate Composition ---
Order ID: [UUID]
Lines: 2
Total: USD 109.97
Address: Springfield, IL

--- Demo 2: Business Rule Validation ---
Order shipped successfully
Business rule enforced: Cannot modify shipped order

--- Demo 3: Event Replay ---
Total events: 4
  [1] v1: OrderCreated
  [2] v2: OrderLineAdded
  [3] v3: OrderLineAdded
  [4] v4: OrderShipped

--- Demo 4: Schema Versioning ---
Aggregate schema version: 2
Total lines added (V2 field): 2

======================================================================
Tutorial 21 Complete!
======================================================================
```

---

## Key Takeaways

1. **Composition patterns**: Use value objects for immutable concepts, child entities for items with identity
2. **Schema versioning**: Increment `schema_version` when state structure changes incompatibly
3. **Migration strategies**: Override `_restore_from_snapshot()` for complex schema migrations
4. **Multi-level validation**: Combine Pydantic validators, pre-command checks, and post-event invariants
5. **Unregistered events**: Choose handling mode based on environment (ignore/warn/error)
6. **Collection management**: Use lists/dicts for child entities, implement helper methods to find items
7. **Business rules**: Enforce invariants in commands, never in event handlers
8. **Testing**: Use Given-When-Then pattern, test replay, migration, and all business rules

---

## Best Practices

1. **Start simple**: Don't add complexity until you need it
2. **Immutable value objects**: Always use `frozen=True` for value objects
3. **Defensive coding**: Check state existence before accessing fields
4. **Clear boundaries**: Only access child entities through the aggregate root
5. **Test migration**: Always test loading old snapshots with new code
6. **Document invariants**: Clearly document business rules in docstrings
7. **Use type hints**: Leverage Python's type system for clarity
8. **Forward compatibility**: Design for evolution from day one

---

## Next Steps

Congratulations! You've completed the eventsource-py tutorial series!

You now have the knowledge to:

- Build event-sourced applications from scratch
- Model complex domains with advanced aggregates
- Handle schema evolution gracefully
- Validate complex business rules
- Test aggregates comprehensively

### Where to Go From Here

1. **Build a real application**: Apply these patterns to a project you care about
2. **Explore the API reference**: Discover capabilities not covered in tutorials
3. **Read the guides**: Deep-dive into specific topics
4. **Join the community**: Share your experiences and learn from others
5. **Contribute**: Help improve eventsource-py for everyone

---

## Related Documentation

- [Aggregate API Reference](../api/aggregates.md)
- [Snapshot Guide](../guides/snapshotting.md)
- [Repository Pattern Guide](../guides/repository-pattern.md)
- [FAQ](../faq.md)

---

**Thank you for completing the eventsource-py tutorial series!** You now have the skills to build production-grade event-sourced systems with confidence.
