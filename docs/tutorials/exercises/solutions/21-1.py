"""
Tutorial 21 - Exercise 1 Solution: Complete E-Commerce Domain

This solution demonstrates all advanced aggregate patterns in a complete
e-commerce domain model.

Run with: python 21-1.py
"""

import asyncio
from datetime import UTC, datetime
from enum import Enum
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
# VALUE OBJECTS
# =============================================================================


class Money(BaseModel):
    """Immutable money value object."""

    amount: float
    currency: str = "USD"

    class Config:
        frozen = True

    @field_validator("amount")
    @classmethod
    def amount_must_be_non_negative(cls, v: float) -> float:
        if v < 0:
            raise ValueError("Money amount cannot be negative")
        return v

    def add(self, other: "Money") -> "Money":
        if self.currency != other.currency:
            raise ValueError(f"Cannot add {self.currency} and {other.currency}")
        return Money(amount=self.amount + other.amount, currency=self.currency)

    def subtract(self, other: "Money") -> "Money":
        if self.currency != other.currency:
            raise ValueError(f"Cannot subtract {self.currency} and {other.currency}")
        return Money(amount=self.amount - other.amount, currency=self.currency)

    def multiply(self, factor: float) -> "Money":
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


class OrderLine(BaseModel):
    """Order line entity within Order aggregate."""

    line_id: UUID
    product_id: str
    product_name: str
    quantity: int
    unit_price: float

    @property
    def line_total(self) -> float:
        return self.quantity * self.unit_price


# =============================================================================
# ORDER AGGREGATE (with schema versioning)
# =============================================================================


@register_event
class OrderCreatedV2(DomainEvent):
    """Order created event - V2 includes discount tracking."""

    event_type: str = "OrderCreatedV2"
    aggregate_type: str = "Order"
    customer_id: UUID
    shipping_address: dict


@register_event
class OrderLineAddedV2(DomainEvent):
    """Order line added event."""

    event_type: str = "OrderLineAddedV2"
    aggregate_type: str = "Order"
    line_id: UUID
    product_id: str
    product_name: str
    quantity: int
    unit_price: float


@register_event
class DiscountApplied(DomainEvent):
    """Discount applied to order - new in V2."""

    event_type: str = "DiscountApplied"
    aggregate_type: str = "Order"
    discount_code: str
    discount_percentage: float


@register_event
class OrderSubmitted(DomainEvent):
    """Order submitted for processing."""

    event_type: str = "OrderSubmitted"
    aggregate_type: str = "Order"
    submitted_at: datetime


class OrderStateV2(BaseModel):
    """Order state - Version 2 with discount tracking."""

    order_id: UUID
    customer_id: UUID | None = None
    shipping_address: Address | None = None
    lines: list[OrderLine] = Field(default_factory=list)
    status: str = "draft"
    discount_applied: bool = False  # New in V2
    discount_percentage: float = 0.0  # New in V2
    discount_code: str | None = None  # New in V2
    submitted_at: datetime | None = None

    @property
    def subtotal(self) -> Money:
        """Calculate subtotal before discount."""
        total = sum(line.line_total for line in self.lines)
        return Money(amount=total)

    @property
    def discount_amount(self) -> Money:
        """Calculate discount amount."""
        if not self.discount_applied:
            return Money(amount=0.0)
        return Money(amount=self.subtotal.amount * (self.discount_percentage / 100))

    @property
    def total(self) -> Money:
        """Calculate total after discount."""
        return self.subtotal.subtract(self.discount_amount)


class OrderAggregate(AggregateRoot[OrderStateV2]):
    """
    Order aggregate with schema versioning.

    Version 2 adds discount tracking fields.
    """

    aggregate_type = "Order"
    schema_version = 2

    def _get_initial_state(self) -> OrderStateV2:
        return OrderStateV2(order_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreatedV2):
            self._state = OrderStateV2(
                order_id=self.aggregate_id,
                customer_id=event.customer_id,
                shipping_address=Address(**event.shipping_address),
                status="created",
            )
        elif isinstance(event, OrderLineAddedV2):
            if self._state:
                new_line = OrderLine(
                    line_id=event.line_id,
                    product_id=event.product_id,
                    product_name=event.product_name,
                    quantity=event.quantity,
                    unit_price=event.unit_price,
                )
                self._state = self._state.model_copy(
                    update={"lines": [*self._state.lines, new_line]}
                )
        elif isinstance(event, DiscountApplied):
            if self._state:
                self._state = self._state.model_copy(
                    update={
                        "discount_applied": True,
                        "discount_percentage": event.discount_percentage,
                        "discount_code": event.discount_code,
                    }
                )
        elif isinstance(event, OrderSubmitted) and self._state:
            self._state = self._state.model_copy(
                update={
                    "status": "submitted",
                    "submitted_at": event.submitted_at,
                }
            )

        # Validate invariants
        self._validate_invariants()

    def _validate_invariants(self) -> None:
        """Validate business invariants."""
        if self._state:
            # Total must not be negative (after discount)
            if self._state.total.amount < 0:
                raise ValueError("Order total cannot be negative")
            # Discount percentage must be reasonable
            if self._state.discount_percentage > 100:
                raise ValueError("Discount percentage cannot exceed 100%")

    def _restore_from_snapshot(self, state: dict, schema_version: int) -> None:
        """Restore with migration support."""
        if schema_version < 2:
            state = self._migrate_v1_to_v2(state)
        self._state = OrderStateV2.model_validate(state)

    def _migrate_v1_to_v2(self, state: dict) -> dict:
        """Migrate V1 state to V2."""
        if "discount_applied" not in state:
            state["discount_applied"] = False
        if "discount_percentage" not in state:
            state["discount_percentage"] = 0.0
        if "discount_code" not in state:
            state["discount_code"] = None
        return state

    # Commands
    def create(self, customer_id: UUID, shipping_address: Address) -> None:
        if self.version > 0:
            raise ValueError("Order already exists")
        self.apply_event(
            OrderCreatedV2(
                aggregate_id=self.aggregate_id,
                customer_id=customer_id,
                shipping_address=shipping_address.model_dump(),
                aggregate_version=self.get_next_version(),
            )
        )

    def add_line(
        self,
        product_id: str,
        product_name: str,
        quantity: int,
        unit_price: float,
    ) -> UUID:
        if not self._state or self._state.status != "created":
            raise ValueError("Cannot modify order")
        if quantity <= 0:
            raise ValueError("Quantity must be positive")
        if unit_price < 0:
            raise ValueError("Unit price cannot be negative")

        line_id = uuid4()
        self.apply_event(
            OrderLineAddedV2(
                aggregate_id=self.aggregate_id,
                line_id=line_id,
                product_id=product_id,
                product_name=product_name,
                quantity=quantity,
                unit_price=unit_price,
                aggregate_version=self.get_next_version(),
            )
        )
        return line_id

    def apply_discount(self, discount_code: str, percentage: float) -> None:
        if not self._state or self._state.status != "created":
            raise ValueError("Cannot apply discount")
        if self._state.discount_applied:
            raise ValueError("Discount already applied")
        if percentage <= 0 or percentage > 100:
            raise ValueError("Invalid discount percentage")

        self.apply_event(
            DiscountApplied(
                aggregate_id=self.aggregate_id,
                discount_code=discount_code,
                discount_percentage=percentage,
                aggregate_version=self.get_next_version(),
            )
        )

    def submit(self) -> None:
        if not self._state or self._state.status != "created":
            raise ValueError("Cannot submit order")
        if len(self._state.lines) == 0:
            raise ValueError("Cannot submit empty order")

        self.apply_event(
            OrderSubmitted(
                aggregate_id=self.aggregate_id,
                submitted_at=datetime.now(UTC),
                aggregate_version=self.get_next_version(),
            )
        )


# =============================================================================
# PAYMENT AGGREGATE
# =============================================================================


class PaymentStatus(Enum):
    PENDING = "pending"
    AUTHORIZED = "authorized"
    CAPTURED = "captured"
    REFUNDED = "refunded"
    FAILED = "failed"


@register_event
class PaymentCreated(DomainEvent):
    event_type: str = "PaymentCreated"
    aggregate_type: str = "Payment"
    order_id: UUID
    amount: float
    currency: str


@register_event
class PaymentAuthorized(DomainEvent):
    event_type: str = "PaymentAuthorized"
    aggregate_type: str = "Payment"
    authorization_code: str


@register_event
class PaymentCaptured(DomainEvent):
    event_type: str = "PaymentCaptured"
    aggregate_type: str = "Payment"
    captured_amount: float


@register_event
class PaymentRefunded(DomainEvent):
    event_type: str = "PaymentRefunded"
    aggregate_type: str = "Payment"
    refund_amount: float
    reason: str


@register_event
class PaymentFailed(DomainEvent):
    event_type: str = "PaymentFailed"
    aggregate_type: str = "Payment"
    failure_reason: str


class PaymentState(BaseModel):
    payment_id: UUID
    order_id: UUID | None = None
    amount: float = 0.0
    currency: str = "USD"
    status: PaymentStatus = PaymentStatus.PENDING
    authorization_code: str | None = None
    captured_amount: float = 0.0
    refunded_amount: float = 0.0
    failure_reason: str | None = None


class PaymentAggregate(AggregateRoot[PaymentState]):
    """Payment aggregate with state validation."""

    aggregate_type = "Payment"

    def _get_initial_state(self) -> PaymentState:
        return PaymentState(payment_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, PaymentCreated):
            self._state = PaymentState(
                payment_id=self.aggregate_id,
                order_id=event.order_id,
                amount=event.amount,
                currency=event.currency,
                status=PaymentStatus.PENDING,
            )
        elif isinstance(event, PaymentAuthorized):
            if self._state:
                self._state = self._state.model_copy(
                    update={
                        "status": PaymentStatus.AUTHORIZED,
                        "authorization_code": event.authorization_code,
                    }
                )
        elif isinstance(event, PaymentCaptured):
            if self._state:
                self._state = self._state.model_copy(
                    update={
                        "status": PaymentStatus.CAPTURED,
                        "captured_amount": event.captured_amount,
                    }
                )
        elif isinstance(event, PaymentRefunded):
            if self._state:
                self._state = self._state.model_copy(
                    update={
                        "status": PaymentStatus.REFUNDED,
                        "refunded_amount": self._state.refunded_amount + event.refund_amount,
                    }
                )
        elif isinstance(event, PaymentFailed) and self._state:
            self._state = self._state.model_copy(
                update={
                    "status": PaymentStatus.FAILED,
                    "failure_reason": event.failure_reason,
                }
            )

        self._validate_invariants()

    def _validate_invariants(self) -> None:
        if self._state:
            # Captured amount cannot exceed authorized amount
            if self._state.captured_amount > self._state.amount:
                raise ValueError("Captured amount exceeds authorized amount")
            # Refunded amount cannot exceed captured amount
            if self._state.refunded_amount > self._state.captured_amount:
                raise ValueError("Refunded amount exceeds captured amount")

    def create(self, order_id: UUID, amount: float, currency: str = "USD") -> None:
        if self.version > 0:
            raise ValueError("Payment already exists")
        if amount <= 0:
            raise ValueError("Amount must be positive")
        self.apply_event(
            PaymentCreated(
                aggregate_id=self.aggregate_id,
                order_id=order_id,
                amount=amount,
                currency=currency,
                aggregate_version=self.get_next_version(),
            )
        )

    def authorize(self, authorization_code: str) -> None:
        if not self._state or self._state.status != PaymentStatus.PENDING:
            raise ValueError("Cannot authorize payment")
        self.apply_event(
            PaymentAuthorized(
                aggregate_id=self.aggregate_id,
                authorization_code=authorization_code,
                aggregate_version=self.get_next_version(),
            )
        )

    def capture(self, amount: float | None = None) -> None:
        if not self._state or self._state.status != PaymentStatus.AUTHORIZED:
            raise ValueError("Cannot capture payment")
        capture_amount = amount if amount is not None else self._state.amount
        self.apply_event(
            PaymentCaptured(
                aggregate_id=self.aggregate_id,
                captured_amount=capture_amount,
                aggregate_version=self.get_next_version(),
            )
        )

    def refund(self, amount: float, reason: str) -> None:
        if not self._state or self._state.status != PaymentStatus.CAPTURED:
            raise ValueError("Cannot refund payment")
        if amount > self._state.captured_amount - self._state.refunded_amount:
            raise ValueError("Refund amount exceeds available amount")
        self.apply_event(
            PaymentRefunded(
                aggregate_id=self.aggregate_id,
                refund_amount=amount,
                reason=reason,
                aggregate_version=self.get_next_version(),
            )
        )

    def fail(self, reason: str) -> None:
        if not self._state or self._state.status != PaymentStatus.PENDING:
            raise ValueError("Cannot fail payment")
        self.apply_event(
            PaymentFailed(
                aggregate_id=self.aggregate_id,
                failure_reason=reason,
                aggregate_version=self.get_next_version(),
            )
        )


# =============================================================================
# ORDER FULFILLMENT SAGA (Process Manager)
# =============================================================================


class FulfillmentStatus(Enum):
    STARTED = "started"
    PAYMENT_PENDING = "payment_pending"
    PAYMENT_AUTHORIZED = "payment_authorized"
    PAYMENT_CAPTURED = "payment_captured"
    INVENTORY_RESERVED = "inventory_reserved"
    COMPLETED = "completed"
    FAILED = "failed"
    COMPENSATING = "compensating"


@register_event
class FulfillmentStartedV2(DomainEvent):
    event_type: str = "FulfillmentStartedV2"
    aggregate_type: str = "Fulfillment"
    order_id: UUID
    total_amount: float


@register_event
class FulfillmentPaymentRequested(DomainEvent):
    event_type: str = "FulfillmentPaymentRequested"
    aggregate_type: str = "Fulfillment"
    payment_id: UUID


@register_event
class FulfillmentPaymentAuthorized(DomainEvent):
    event_type: str = "FulfillmentPaymentAuthorized"
    aggregate_type: str = "Fulfillment"
    payment_id: UUID


@register_event
class FulfillmentPaymentCaptured(DomainEvent):
    event_type: str = "FulfillmentPaymentCaptured"
    aggregate_type: str = "Fulfillment"
    payment_id: UUID


@register_event
class FulfillmentInventoryReserved(DomainEvent):
    event_type: str = "FulfillmentInventoryReserved"
    aggregate_type: str = "Fulfillment"
    reservation_id: UUID


@register_event
class FulfillmentCompletedV2(DomainEvent):
    event_type: str = "FulfillmentCompletedV2"
    aggregate_type: str = "Fulfillment"


@register_event
class FulfillmentFailedV2(DomainEvent):
    event_type: str = "FulfillmentFailedV2"
    aggregate_type: str = "Fulfillment"
    reason: str
    compensation_required: bool


@register_event
class CompensationStarted(DomainEvent):
    event_type: str = "CompensationStarted"
    aggregate_type: str = "Fulfillment"


@register_event
class CompensationCompleted(DomainEvent):
    event_type: str = "CompensationCompleted"
    aggregate_type: str = "Fulfillment"
    actions_taken: list[str]


class FulfillmentStateV2(BaseModel):
    fulfillment_id: UUID
    order_id: UUID | None = None
    payment_id: UUID | None = None
    reservation_id: UUID | None = None
    status: FulfillmentStatus = FulfillmentStatus.STARTED
    total_amount: float = 0.0
    failure_reason: str | None = None
    compensation_actions: list[str] = Field(default_factory=list)


class OrderFulfillmentSaga(AggregateRoot[FulfillmentStateV2]):
    """
    Order Fulfillment Saga with compensation.

    Workflow:
    1. Start fulfillment
    2. Request payment authorization
    3. Capture payment
    4. Reserve inventory
    5. Complete

    Compensation:
    - If inventory fails after payment: refund payment
    - If any step fails: record failure, trigger compensation
    """

    aggregate_type = "Fulfillment"

    def _get_initial_state(self) -> FulfillmentStateV2:
        return FulfillmentStateV2(fulfillment_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, FulfillmentStartedV2):
            self._state = FulfillmentStateV2(
                fulfillment_id=self.aggregate_id,
                order_id=event.order_id,
                total_amount=event.total_amount,
                status=FulfillmentStatus.PAYMENT_PENDING,
            )
        elif isinstance(event, FulfillmentPaymentRequested):
            if self._state:
                self._state = self._state.model_copy(
                    update={
                        "payment_id": event.payment_id,
                    }
                )
        elif isinstance(event, FulfillmentPaymentAuthorized):
            if self._state:
                self._state = self._state.model_copy(
                    update={
                        "status": FulfillmentStatus.PAYMENT_AUTHORIZED,
                    }
                )
        elif isinstance(event, FulfillmentPaymentCaptured):
            if self._state:
                self._state = self._state.model_copy(
                    update={
                        "status": FulfillmentStatus.PAYMENT_CAPTURED,
                    }
                )
        elif isinstance(event, FulfillmentInventoryReserved):
            if self._state:
                self._state = self._state.model_copy(
                    update={
                        "reservation_id": event.reservation_id,
                        "status": FulfillmentStatus.INVENTORY_RESERVED,
                    }
                )
        elif isinstance(event, FulfillmentCompletedV2):
            if self._state:
                self._state = self._state.model_copy(
                    update={
                        "status": FulfillmentStatus.COMPLETED,
                    }
                )
        elif isinstance(event, FulfillmentFailedV2):
            if self._state:
                new_status = (
                    FulfillmentStatus.COMPENSATING
                    if event.compensation_required
                    else FulfillmentStatus.FAILED
                )
                self._state = self._state.model_copy(
                    update={
                        "status": new_status,
                        "failure_reason": event.reason,
                    }
                )
        elif isinstance(event, CompensationStarted):
            if self._state:
                self._state = self._state.model_copy(
                    update={
                        "status": FulfillmentStatus.COMPENSATING,
                    }
                )
        elif isinstance(event, CompensationCompleted) and self._state:
            self._state = self._state.model_copy(
                update={
                    "status": FulfillmentStatus.FAILED,
                    "compensation_actions": event.actions_taken,
                }
            )

    def start(self, order_id: UUID, total_amount: float) -> None:
        if self.version > 0:
            raise ValueError("Already started")
        self.apply_event(
            FulfillmentStartedV2(
                aggregate_id=self.aggregate_id,
                order_id=order_id,
                total_amount=total_amount,
                aggregate_version=self.get_next_version(),
            )
        )

    def request_payment(self, payment_id: UUID) -> None:
        if not self._state or self._state.status != FulfillmentStatus.PAYMENT_PENDING:
            raise ValueError("Cannot request payment")
        self.apply_event(
            FulfillmentPaymentRequested(
                aggregate_id=self.aggregate_id,
                payment_id=payment_id,
                aggregate_version=self.get_next_version(),
            )
        )

    def record_payment_authorized(self) -> None:
        if not self._state or not self._state.payment_id:
            raise ValueError("No payment to authorize")
        self.apply_event(
            FulfillmentPaymentAuthorized(
                aggregate_id=self.aggregate_id,
                payment_id=self._state.payment_id,
                aggregate_version=self.get_next_version(),
            )
        )

    def record_payment_captured(self) -> None:
        if not self._state or self._state.status != FulfillmentStatus.PAYMENT_AUTHORIZED:
            raise ValueError("Payment not authorized")
        self.apply_event(
            FulfillmentPaymentCaptured(
                aggregate_id=self.aggregate_id,
                payment_id=self._state.payment_id,
                aggregate_version=self.get_next_version(),
            )
        )

    def record_inventory_reserved(self, reservation_id: UUID) -> None:
        if not self._state or self._state.status != FulfillmentStatus.PAYMENT_CAPTURED:
            raise ValueError("Payment not captured")
        self.apply_event(
            FulfillmentInventoryReserved(
                aggregate_id=self.aggregate_id,
                reservation_id=reservation_id,
                aggregate_version=self.get_next_version(),
            )
        )

    def complete(self) -> None:
        if not self._state or self._state.status != FulfillmentStatus.INVENTORY_RESERVED:
            raise ValueError("Not ready to complete")
        self.apply_event(
            FulfillmentCompletedV2(
                aggregate_id=self.aggregate_id,
                aggregate_version=self.get_next_version(),
            )
        )

    def fail(self, reason: str, compensation_required: bool = False) -> None:
        if not self._state:
            raise ValueError("Not started")
        self.apply_event(
            FulfillmentFailedV2(
                aggregate_id=self.aggregate_id,
                reason=reason,
                compensation_required=compensation_required,
                aggregate_version=self.get_next_version(),
            )
        )

    def start_compensation(self) -> None:
        if not self._state:
            raise ValueError("Not started")
        self.apply_event(
            CompensationStarted(
                aggregate_id=self.aggregate_id,
                aggregate_version=self.get_next_version(),
            )
        )

    def complete_compensation(self, actions: list[str]) -> None:
        if not self._state or self._state.status != FulfillmentStatus.COMPENSATING:
            raise ValueError("Not in compensation state")
        self.apply_event(
            CompensationCompleted(
                aggregate_id=self.aggregate_id,
                actions_taken=actions,
                aggregate_version=self.get_next_version(),
            )
        )


# =============================================================================
# TESTS
# =============================================================================


def test_order_with_discount():
    """Test order with discount application."""
    print("Test: Order with discount...")
    order = OrderAggregate(uuid4())

    # Given: An order with items
    address = Address(
        street="123 Main St",
        city="Springfield",
        state="IL",
        postal_code="62701",
    )
    order.create(uuid4(), address)
    order.add_line("PROD-001", "Widget", 2, 50.0)  # $100
    order.add_line("PROD-002", "Gadget", 1, 75.0)  # $75

    # When: Discount is applied
    order.apply_discount("SAVE20", 20.0)

    # Then: Total reflects discount
    assert order.state.subtotal.amount == 175.0
    assert order.state.discount_percentage == 20.0
    assert order.state.discount_amount.amount == 35.0
    assert order.state.total.amount == 140.0
    print("  PASSED: Order with discount works correctly")


def test_order_schema_migration():
    """Test V1 to V2 schema migration."""
    print("Test: Schema migration...")

    # Given: V1 state without discount fields
    v1_state = {
        "order_id": str(uuid4()),
        "customer_id": str(uuid4()),
        "lines": [],
        "status": "created",
        # Note: missing discount fields
    }

    # When: Restoring from V1 snapshot
    order = OrderAggregate(UUID(v1_state["order_id"]))
    order._restore_from_snapshot(v1_state, schema_version=1)

    # Then: V2 fields are added with defaults
    assert order.state.discount_applied is False
    assert order.state.discount_percentage == 0.0
    assert order.state.discount_code is None
    print("  PASSED: Schema migration works correctly")


def test_payment_validation():
    """Test payment amount validation."""
    print("Test: Payment validation...")
    payment = PaymentAggregate(uuid4())

    # Given: A payment
    payment.create(uuid4(), 100.0)
    payment.authorize("AUTH-123")
    payment.capture(100.0)

    # When/Then: Cannot refund more than captured
    try:
        payment.refund(150.0, "Overpayment")
        raise AssertionError("Should have raised ValueError")
    except ValueError as e:
        assert "exceeds available" in str(e)
    print("  PASSED: Payment validation works correctly")


def test_fulfillment_happy_path():
    """Test fulfillment saga happy path."""
    print("Test: Fulfillment happy path...")
    saga = OrderFulfillmentSaga(uuid4())
    order_id = uuid4()
    payment_id = uuid4()
    reservation_id = uuid4()

    # Start
    saga.start(order_id, 175.0)
    assert saga.state.status == FulfillmentStatus.PAYMENT_PENDING

    # Payment
    saga.request_payment(payment_id)
    saga.record_payment_authorized()
    assert saga.state.status == FulfillmentStatus.PAYMENT_AUTHORIZED

    saga.record_payment_captured()
    assert saga.state.status == FulfillmentStatus.PAYMENT_CAPTURED

    # Inventory
    saga.record_inventory_reserved(reservation_id)
    assert saga.state.status == FulfillmentStatus.INVENTORY_RESERVED

    # Complete
    saga.complete()
    assert saga.state.status == FulfillmentStatus.COMPLETED
    print("  PASSED: Fulfillment happy path works correctly")


def test_fulfillment_compensation():
    """Test fulfillment saga with compensation."""
    print("Test: Fulfillment compensation...")
    saga = OrderFulfillmentSaga(uuid4())
    order_id = uuid4()
    payment_id = uuid4()

    # Start and process payment
    saga.start(order_id, 175.0)
    saga.request_payment(payment_id)
    saga.record_payment_authorized()
    saga.record_payment_captured()

    # Fail with compensation required
    saga.fail("Inventory unavailable", compensation_required=True)
    assert saga.state.status == FulfillmentStatus.COMPENSATING

    # Complete compensation
    saga.complete_compensation(["Refunded payment", "Released inventory hold"])
    assert saga.state.status == FulfillmentStatus.FAILED
    assert len(saga.state.compensation_actions) == 2
    print("  PASSED: Fulfillment compensation works correctly")


# =============================================================================
# DEMO
# =============================================================================


async def main():
    print("=" * 60)
    print("Tutorial 21 - Exercise 1 Solution")
    print("Complete E-Commerce Domain Model")
    print("=" * 60)

    # Run tests
    print("\n--- Running Tests ---\n")
    test_order_with_discount()
    test_order_schema_migration()
    test_payment_validation()
    test_fulfillment_happy_path()
    test_fulfillment_compensation()
    print("\nAll tests passed!")

    # Demo the full workflow
    print("\n--- Full E-Commerce Workflow Demo ---\n")

    event_store = InMemoryEventStore()

    # Create repositories
    order_repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=OrderAggregate,
        aggregate_type="Order",
    )
    payment_repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=PaymentAggregate,
        aggregate_type="Payment",
    )
    saga_repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=OrderFulfillmentSaga,
        aggregate_type="Fulfillment",
    )

    # 1. Create order
    print("1. Creating order...")
    order_id = uuid4()
    customer_id = uuid4()
    order = order_repo.create_new(order_id)
    order.create(
        customer_id,
        Address(
            street="123 E-Commerce Way",
            city="Shopville",
            state="CA",
            postal_code="90210",
        ),
    )
    order.add_line("SKU-001", "Premium Widget", 2, 49.99)
    order.add_line("SKU-002", "Deluxe Gadget", 1, 79.99)
    order.apply_discount("WELCOME10", 10.0)
    order.submit()
    await order_repo.save(order)
    print(f"   Order created: {order_id}")
    print(f"   Subtotal: ${order.state.subtotal.amount:.2f}")
    print(f"   Discount: ${order.state.discount_amount.amount:.2f}")
    print(f"   Total: ${order.state.total.amount:.2f}")

    # 2. Create payment
    print("\n2. Processing payment...")
    payment_id = uuid4()
    payment = payment_repo.create_new(payment_id)
    payment.create(order_id, order.state.total.amount)
    payment.authorize("AUTH-" + str(uuid4())[:8].upper())
    payment.capture()
    await payment_repo.save(payment)
    print(f"   Payment captured: ${payment.state.captured_amount:.2f}")

    # 3. Run fulfillment saga
    print("\n3. Running fulfillment saga...")
    saga_id = uuid4()
    saga = saga_repo.create_new(saga_id)
    saga.start(order_id, order.state.total.amount)
    saga.request_payment(payment_id)
    saga.record_payment_authorized()
    saga.record_payment_captured()
    saga.record_inventory_reserved(uuid4())
    saga.complete()
    await saga_repo.save(saga)
    print(f"   Fulfillment status: {saga.state.status.value}")

    # 4. Summary
    print("\n" + "=" * 60)
    print("E-Commerce Workflow Complete!")
    print("=" * 60)
    print(f"\nOrder: {order_id}")
    print(f"  Status: {order.state.status}")
    print(f"  Lines: {len(order.state.lines)}")
    print(f"  Total: ${order.state.total.amount:.2f}")
    print(f"\nPayment: {payment_id}")
    print(f"  Status: {payment.state.status.value}")
    print(f"  Captured: ${payment.state.captured_amount:.2f}")
    print(f"\nFulfillment: {saga_id}")
    print(f"  Status: {saga.state.status.value}")


if __name__ == "__main__":
    asyncio.run(main())
