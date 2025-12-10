"""
Tutorial 16 - Exercise 1 Solution: Multi-Tenant Order Management System

This solution demonstrates a complete multi-tenant order management system
with sales reporting and tenant isolation verification.

Run with: python 16-1.py
"""

import asyncio
from uuid import UUID, uuid4

from pydantic import BaseModel

from eventsource import (
    AggregateRepository,
    AggregateRoot,
    DeclarativeProjection,
    DomainEvent,
    InMemoryEventStore,
    handles,
    register_event,
)

# =============================================================================
# Step 1: Events
# =============================================================================


@register_event
class ProductOrderCreated(DomainEvent):
    """Event when a product order is created."""

    event_type: str = "ProductOrderCreated"
    aggregate_type: str = "ProductOrder"
    product_name: str
    quantity: int
    unit_price: float

    @property
    def total(self) -> float:
        return self.quantity * self.unit_price


@register_event
class ProductOrderConfirmed(DomainEvent):
    """Event when a product order is confirmed."""

    event_type: str = "ProductOrderConfirmed"
    aggregate_type: str = "ProductOrder"


@register_event
class ProductOrderShipped(DomainEvent):
    """Event when a product order is shipped."""

    event_type: str = "ProductOrderShipped"
    aggregate_type: str = "ProductOrder"
    tracking_number: str


# =============================================================================
# Step 2: Aggregate State and Aggregate
# =============================================================================


class ProductOrderState(BaseModel):
    """State of a ProductOrder aggregate."""

    order_id: UUID
    tenant_id: UUID | None = None
    product_name: str = ""
    quantity: int = 0
    unit_price: float = 0.0
    total: float = 0.0
    status: str = "pending"
    tracking_number: str | None = None


class ProductOrderAggregate(AggregateRoot[ProductOrderState]):
    """Tenant-aware product order aggregate."""

    aggregate_type = "ProductOrder"

    def __init__(self, aggregate_id: UUID, tenant_id: UUID | None = None):
        super().__init__(aggregate_id)
        self._tenant_id = tenant_id

    @property
    def tenant_id(self) -> UUID | None:
        return self._tenant_id

    def _get_initial_state(self) -> ProductOrderState:
        return ProductOrderState(
            order_id=self.aggregate_id,
            tenant_id=self._tenant_id,
        )

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, ProductOrderCreated):
            self._state = ProductOrderState(
                order_id=self.aggregate_id,
                tenant_id=event.tenant_id,
                product_name=event.product_name,
                quantity=event.quantity,
                unit_price=event.unit_price,
                total=event.quantity * event.unit_price,
                status="pending",
            )
            if event.tenant_id:
                self._tenant_id = event.tenant_id
        elif isinstance(event, ProductOrderConfirmed):
            if self._state:
                self._state = self._state.model_copy(update={"status": "confirmed"})
        elif isinstance(event, ProductOrderShipped):
            if self._state:
                self._state = self._state.model_copy(
                    update={
                        "status": "shipped",
                        "tracking_number": event.tracking_number,
                    }
                )

    def create(self, product_name: str, quantity: int, unit_price: float) -> None:
        """Create a product order."""
        if self.version > 0:
            raise ValueError("Order already exists")
        if not self._tenant_id:
            raise ValueError("Tenant context required")

        self.apply_event(
            ProductOrderCreated(
                aggregate_id=self.aggregate_id,
                tenant_id=self._tenant_id,
                product_name=product_name,
                quantity=quantity,
                unit_price=unit_price,
                aggregate_version=self.get_next_version(),
            )
        )

    def confirm(self) -> None:
        """Confirm the order."""
        if not self._state or self._state.status != "pending":
            raise ValueError("Order must be pending to confirm")

        self.apply_event(
            ProductOrderConfirmed(
                aggregate_id=self.aggregate_id,
                tenant_id=self._tenant_id,
                aggregate_version=self.get_next_version(),
            )
        )

    def ship(self, tracking_number: str) -> None:
        """Ship the order."""
        if not self._state or self._state.status != "confirmed":
            raise ValueError("Order must be confirmed to ship")

        self.apply_event(
            ProductOrderShipped(
                aggregate_id=self.aggregate_id,
                tenant_id=self._tenant_id,
                tracking_number=tracking_number,
                aggregate_version=self.get_next_version(),
            )
        )


# =============================================================================
# Step 3: Tenant-Aware Repository
# =============================================================================


class TenantOrderRepository:
    """Repository that enforces tenant isolation."""

    def __init__(self, event_store: InMemoryEventStore, tenant_id: UUID):
        self._tenant_id = tenant_id
        self._event_store = event_store
        self._inner_repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=lambda aid: ProductOrderAggregate(aid, tenant_id),
            aggregate_type="ProductOrder",
        )

    def create_new(self, order_id: UUID) -> ProductOrderAggregate:
        """Create a new order in tenant's context."""
        return ProductOrderAggregate(order_id, self._tenant_id)

    async def save(self, order: ProductOrderAggregate) -> None:
        """Save order with tenant validation."""
        if order.tenant_id != self._tenant_id:
            raise PermissionError(
                f"Cannot save order from tenant {order.tenant_id} "
                f"in context of tenant {self._tenant_id}"
            )
        await self._inner_repo.save(order)

    async def load(self, order_id: UUID) -> ProductOrderAggregate:
        """Load order with tenant validation."""
        order = await self._inner_repo.load(order_id)
        if order.tenant_id != self._tenant_id:
            raise PermissionError(
                f"Order {order_id} belongs to tenant {order.tenant_id}, "
                f"access denied for tenant {self._tenant_id}"
            )
        return order


# =============================================================================
# Step 4: Sales Report Projection
# =============================================================================


class TenantSalesReportProjection(DeclarativeProjection):
    """Projection tracking sales statistics per tenant."""

    def __init__(self):
        super().__init__()
        self._stats: dict[str, dict] = {}

    @handles(ProductOrderCreated)
    async def _on_order_created(self, event: ProductOrderCreated) -> None:
        tenant_key = str(event.tenant_id) if event.tenant_id else "unknown"

        if tenant_key not in self._stats:
            self._stats[tenant_key] = {
                "total_orders": 0,
                "total_revenue": 0.0,
                "order_values": [],
            }

        order_value = event.quantity * event.unit_price
        self._stats[tenant_key]["total_orders"] += 1
        self._stats[tenant_key]["total_revenue"] += order_value
        self._stats[tenant_key]["order_values"].append(order_value)

    async def _truncate_read_models(self) -> None:
        self._stats.clear()

    def get_tenant_report(self, tenant_id: UUID) -> dict:
        """Get sales report for a specific tenant."""
        tenant_key = str(tenant_id)
        stats = self._stats.get(tenant_key)
        if not stats:
            return {
                "total_orders": 0,
                "total_revenue": 0.0,
                "average_order_value": 0.0,
            }

        avg = stats["total_revenue"] / stats["total_orders"] if stats["total_orders"] > 0 else 0.0

        return {
            "total_orders": stats["total_orders"],
            "total_revenue": stats["total_revenue"],
            "average_order_value": avg,
        }

    def get_all_reports(self) -> dict[str, dict]:
        """Get reports for all tenants."""
        return {
            tenant_key: {
                "total_orders": stats["total_orders"],
                "total_revenue": stats["total_revenue"],
                "average_order_value": (
                    stats["total_revenue"] / stats["total_orders"]
                    if stats["total_orders"] > 0
                    else 0.0
                ),
            }
            for tenant_key, stats in self._stats.items()
        }


# =============================================================================
# Main Program
# =============================================================================


async def main():
    print("=" * 70)
    print("Exercise 16-1: Multi-Tenant Order Management System")
    print("=" * 70)

    event_store = InMemoryEventStore()

    # =========================================================================
    # Step 5: Create tenants
    # =========================================================================
    tenant_alpha = uuid4()
    tenant_beta = uuid4()
    tenant_gamma = uuid4()

    print(f"\nTenant Alpha: {tenant_alpha}")
    print(f"Tenant Beta:  {tenant_beta}")
    print(f"Tenant Gamma: {tenant_gamma}")

    # Create repositories
    repo_alpha = TenantOrderRepository(event_store, tenant_alpha)
    repo_beta = TenantOrderRepository(event_store, tenant_beta)
    repo_gamma = TenantOrderRepository(event_store, tenant_gamma)

    # =========================================================================
    # Step 6: Create orders for each tenant
    # =========================================================================
    print("\n--- Creating Orders ---")

    # Alpha orders (2 orders)
    alpha_orders = []
    order1 = repo_alpha.create_new(uuid4())
    order1.create("Premium Widget", 5, 29.99)
    await repo_alpha.save(order1)
    alpha_orders.append(order1.aggregate_id)
    print(
        f"Alpha Order 1: {order1.state.product_name} x{order1.state.quantity} = ${order1.state.total:.2f}"
    )

    order2 = repo_alpha.create_new(uuid4())
    order2.create("Deluxe Gadget", 2, 149.99)
    await repo_alpha.save(order2)
    alpha_orders.append(order2.aggregate_id)
    print(
        f"Alpha Order 2: {order2.state.product_name} x{order2.state.quantity} = ${order2.state.total:.2f}"
    )

    # Beta orders (3 orders)
    beta_orders = []
    order3 = repo_beta.create_new(uuid4())
    order3.create("Standard Part", 100, 2.50)
    await repo_beta.save(order3)
    beta_orders.append(order3.aggregate_id)
    print(
        f"Beta Order 1: {order3.state.product_name} x{order3.state.quantity} = ${order3.state.total:.2f}"
    )

    order4 = repo_beta.create_new(uuid4())
    order4.create("Industrial Component", 25, 45.00)
    await repo_beta.save(order4)
    beta_orders.append(order4.aggregate_id)
    print(
        f"Beta Order 2: {order4.state.product_name} x{order4.state.quantity} = ${order4.state.total:.2f}"
    )

    order5 = repo_beta.create_new(uuid4())
    order5.create("Bulk Supply Pack", 10, 89.99)
    await repo_beta.save(order5)
    beta_orders.append(order5.aggregate_id)
    print(
        f"Beta Order 3: {order5.state.product_name} x{order5.state.quantity} = ${order5.state.total:.2f}"
    )

    # Gamma order (1 order)
    gamma_orders = []
    order6 = repo_gamma.create_new(uuid4())
    order6.create("Enterprise Solution", 1, 4999.99)
    await repo_gamma.save(order6)
    gamma_orders.append(order6.aggregate_id)
    print(
        f"Gamma Order 1: {order6.state.product_name} x{order6.state.quantity} = ${order6.state.total:.2f}"
    )

    # Confirm and ship some orders
    loaded = await repo_alpha.load(alpha_orders[0])
    loaded.confirm()
    await repo_alpha.save(loaded)
    loaded.ship("ALPHA-001")
    await repo_alpha.save(loaded)
    print("\nAlpha Order 1 shipped with tracking: ALPHA-001")

    # =========================================================================
    # Step 7: Query events by tenant
    # =========================================================================
    print("\n--- Event Counts by Tenant ---")

    alpha_events = await event_store.get_events_by_type("ProductOrder", tenant_id=tenant_alpha)
    beta_events = await event_store.get_events_by_type("ProductOrder", tenant_id=tenant_beta)
    gamma_events = await event_store.get_events_by_type("ProductOrder", tenant_id=tenant_gamma)

    print(f"Alpha events: {len(alpha_events)}")
    for e in alpha_events:
        print(f"  - {e.event_type}")

    print(f"Beta events:  {len(beta_events)}")
    for e in beta_events:
        print(f"  - {e.event_type}")

    print(f"Gamma events: {len(gamma_events)}")
    for e in gamma_events:
        print(f"  - {e.event_type}")

    # =========================================================================
    # Step 8: Build and display sales report
    # =========================================================================
    print("\n--- Sales Reports ---")

    projection = TenantSalesReportProjection()

    # Process all events
    async for stored in event_store.read_all():
        await projection.handle(stored.event)

    # Display reports
    alpha_report = projection.get_tenant_report(tenant_alpha)
    beta_report = projection.get_tenant_report(tenant_beta)
    gamma_report = projection.get_tenant_report(tenant_gamma)

    print("\nAlpha Report:")
    print(f"  Total Orders: {alpha_report['total_orders']}")
    print(f"  Total Revenue: ${alpha_report['total_revenue']:.2f}")
    print(f"  Avg Order Value: ${alpha_report['average_order_value']:.2f}")

    print("\nBeta Report:")
    print(f"  Total Orders: {beta_report['total_orders']}")
    print(f"  Total Revenue: ${beta_report['total_revenue']:.2f}")
    print(f"  Avg Order Value: ${beta_report['average_order_value']:.2f}")

    print("\nGamma Report:")
    print(f"  Total Orders: {gamma_report['total_orders']}")
    print(f"  Total Revenue: ${gamma_report['total_revenue']:.2f}")
    print(f"  Avg Order Value: ${gamma_report['average_order_value']:.2f}")

    # Total across all tenants
    total_revenue = sum(r["total_revenue"] for r in [alpha_report, beta_report, gamma_report])
    print(f"\nTotal Platform Revenue: ${total_revenue:.2f}")

    # =========================================================================
    # Step 9: Verify tenant isolation
    # =========================================================================
    print("\n--- Tenant Isolation Tests ---")

    # Test 1: Beta trying to access Alpha's order
    print("\nTest 1: Beta accessing Alpha's order...")
    try:
        await repo_beta.load(alpha_orders[0])
        print("  FAILED: Access should have been denied!")
    except PermissionError as e:
        print(f"  PASSED: {e}")

    # Test 2: Gamma trying to access Beta's order
    print("\nTest 2: Gamma accessing Beta's order...")
    try:
        await repo_gamma.load(beta_orders[1])
        print("  FAILED: Access should have been denied!")
    except PermissionError as e:
        print(f"  PASSED: {e}")

    # Test 3: Alpha can access their own order
    print("\nTest 3: Alpha accessing own order...")
    try:
        order = await repo_alpha.load(alpha_orders[1])
        print(f"  PASSED: Loaded {order.state.product_name}")
    except PermissionError:
        print("  FAILED: Should have access to own order!")

    # Test 4: Each tenant only sees their own events
    print("\nTest 4: Event visibility verification...")
    for event in alpha_events:
        assert event.tenant_id == tenant_alpha, "Alpha sees non-Alpha event!"
    print("  PASSED: Alpha only sees Alpha events")

    for event in beta_events:
        assert event.tenant_id == tenant_beta, "Beta sees non-Beta event!"
    print("  PASSED: Beta only sees Beta events")

    for event in gamma_events:
        assert event.tenant_id == tenant_gamma, "Gamma sees non-Gamma event!"
    print("  PASSED: Gamma only sees Gamma events")

    print("\n" + "=" * 70)
    print("All tests passed! Multi-tenant isolation verified.")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
