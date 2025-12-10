"""
Tutorial 20 - Exercise 1 Solution: Full Observability Setup

This solution demonstrates complete observability setup for an event-sourced
shopping cart application using OpenTelemetry.

Prerequisites:
- pip install eventsource-py[telemetry]
- Optional: Jaeger running on localhost:6831

Run with: python 20-1.py
"""

import asyncio
from uuid import uuid4

from pydantic import BaseModel

from eventsource import (
    AggregateRepository,
    AggregateRoot,
    DomainEvent,
    InMemoryEventStore,
    register_event,
)
from eventsource.observability import (
    ATTR_AGGREGATE_ID,
    ATTR_EVENT_COUNT,
    OTEL_AVAILABLE,
    TracingMixin,
)

if not OTEL_AVAILABLE:
    print("OpenTelemetry not available.")
    print("Install with: pip install eventsource-py[telemetry]")
    exit(1)


# =============================================================================
# Step 1: Set up OpenTelemetry tracing
# =============================================================================

from opentelemetry import trace
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

# Create resource with service name
resource = Resource(attributes={SERVICE_NAME: "exercise-20-cart"})

# Create provider with resource
provider = TracerProvider(resource=resource)

# Add console exporter (use JaegerExporter for production)
provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))

# Set as global provider
trace.set_tracer_provider(provider)

print("OpenTelemetry configured with ConsoleSpanExporter")


# =============================================================================
# Step 2: Define cart events
# =============================================================================


@register_event
class ItemAdded(DomainEvent):
    """Event when an item is added to the cart."""

    event_type: str = "ItemAdded"
    aggregate_type: str = "Cart"
    product: str
    quantity: int = 1


@register_event
class ItemRemoved(DomainEvent):
    """Event when an item is removed from the cart."""

    event_type: str = "ItemRemoved"
    aggregate_type: str = "Cart"
    product: str


# =============================================================================
# Step 3: Create CartAggregate
# =============================================================================


class CartState(BaseModel):
    """State of a shopping cart."""

    cart_id: str
    items: dict[str, int] = {}  # product name -> quantity


class CartAggregate(AggregateRoot[CartState]):
    """Shopping cart aggregate with items."""

    aggregate_type = "Cart"

    def _get_initial_state(self) -> CartState:
        return CartState(cart_id=str(self.aggregate_id))

    def _apply(self, event: DomainEvent) -> None:
        if self._state is None:
            self._state = self._get_initial_state()

        if isinstance(event, ItemAdded):
            current = self._state.items.get(event.product, 0)
            new_items = {**self._state.items, event.product: current + event.quantity}
            self._state = self._state.model_copy(update={"items": new_items})
        elif isinstance(event, ItemRemoved):
            new_items = {k: v for k, v in self._state.items.items() if k != event.product}
            self._state = self._state.model_copy(update={"items": new_items})

    def add_item(self, product: str, quantity: int = 1) -> None:
        """Add an item to the cart."""
        self.apply_event(
            ItemAdded(
                aggregate_id=self.aggregate_id,
                product=product,
                quantity=quantity,
                aggregate_version=self.get_next_version(),
            )
        )

    def remove_item(self, product: str) -> None:
        """Remove an item from the cart."""
        if product in (self._state.items if self._state else {}):
            self.apply_event(
                ItemRemoved(
                    aggregate_id=self.aggregate_id,
                    product=product,
                    aggregate_version=self.get_next_version(),
                )
            )


# =============================================================================
# Step 4: Create CartService with TracingMixin
# =============================================================================


class CartService(TracingMixin):
    """
    Shopping cart service with custom tracing.

    Demonstrates how to add observability to your own service layer.
    """

    def __init__(self, repo: AggregateRepository, enable_tracing: bool = True):
        # Initialize tracing via TracingMixin
        self._init_tracing(__name__, enable_tracing)
        self._repo = repo

    async def add_to_cart(
        self,
        cart_id,
        product: str,
        quantity: int = 1,
    ) -> CartAggregate:
        """
        Add an item to a cart.

        Creates custom span with dynamic attributes.
        """
        with self._create_span_context(
            "cart_service.add_to_cart",
            {
                ATTR_AGGREGATE_ID: str(cart_id),
                "product.name": product,
                "product.quantity": quantity,
            },
        ) as span:
            # Try to load existing cart or create new one
            try:
                cart = await self._repo.load(cart_id)
            except Exception:
                cart = self._repo.create_new(cart_id)

            # Add the item
            cart.add_item(product, quantity)
            await self._repo.save(cart)

            # Add additional attributes after operation
            if span:
                span.set_attribute(ATTR_EVENT_COUNT, len(cart.state.items))
                span.set_attribute("cart.total_items", sum(cart.state.items.values()))

            return cart

    async def remove_from_cart(self, cart_id, product: str) -> CartAggregate:
        """
        Remove an item from a cart.

        Creates custom span with dynamic attributes.
        """
        with self._create_span_context(
            "cart_service.remove_from_cart",
            {
                ATTR_AGGREGATE_ID: str(cart_id),
                "product.name": product,
            },
        ) as span:
            cart = await self._repo.load(cart_id)
            cart.remove_item(product)
            await self._repo.save(cart)

            if span:
                span.set_attribute("cart.total_items", sum(cart.state.items.values()))

            return cart

    async def get_cart(self, cart_id) -> CartAggregate:
        """
        Get a cart by ID.

        Creates custom span for read operation.
        """
        with self._create_span_context(
            "cart_service.get_cart",
            {ATTR_AGGREGATE_ID: str(cart_id)},
        ) as span:
            cart = await self._repo.load(cart_id)

            if span:
                span.set_attribute("cart.item_count", len(cart.state.items))
                span.set_attribute("cart.total_items", sum(cart.state.items.values()))

            return cart

    async def clear_cart(self, cart_id) -> CartAggregate:
        """
        Clear all items from a cart.

        Demonstrates multiple events in one span.
        """
        with self._create_span_context(
            "cart_service.clear_cart",
            {ATTR_AGGREGATE_ID: str(cart_id)},
        ) as span:
            cart = await self._repo.load(cart_id)

            # Get products to remove
            products = list(cart.state.items.keys())

            if span:
                span.set_attribute("cart.items_to_remove", len(products))

            # Remove each item
            for product in products:
                cart.remove_item(product)

            await self._repo.save(cart)

            if span:
                span.set_attribute(ATTR_EVENT_COUNT, len(products))

            return cart


# =============================================================================
# Main - Run the example
# =============================================================================


async def main():
    print("\n" + "=" * 60)
    print("Exercise 20-1: Full Observability Setup")
    print("=" * 60)

    # Step 5: Create traced components
    print("\nStep 1: Creating traced components...")
    store = InMemoryEventStore(enable_tracing=True)
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=CartAggregate,
        aggregate_type="Cart",
        enable_tracing=True,
    )
    service = CartService(repo, enable_tracing=True)
    print("   Event store: tracing enabled")
    print("   Repository: tracing enabled")
    print("   CartService: tracing enabled")

    # Step 6: Perform cart operations
    print("\nStep 2: Performing cart operations...")
    cart_id = uuid4()
    print(f"   Cart ID: {cart_id}")

    print("\n   Adding items to cart:")
    cart = await service.add_to_cart(cart_id, "Laptop", 1)
    print("   - Added: Laptop x1")

    cart = await service.add_to_cart(cart_id, "Mouse", 2)
    print("   - Added: Mouse x2")

    cart = await service.add_to_cart(cart_id, "Keyboard", 1)
    print("   - Added: Keyboard x1")

    cart = await service.add_to_cart(cart_id, "USB Cable", 3)
    print("   - Added: USB Cable x3")

    # Get cart and display
    print("\nStep 3: Reading cart state...")
    cart = await service.get_cart(cart_id)
    print("\n   Cart contents:")
    for product, qty in cart.state.items.items():
        print(f"   - {product}: {qty}")
    print(f"   Total unique items: {len(cart.state.items)}")
    print(f"   Total quantity: {sum(cart.state.items.values())}")

    # Remove an item
    print("\nStep 4: Removing an item...")
    cart = await service.remove_from_cart(cart_id, "USB Cable")
    print("   Removed: USB Cable")
    print(f"   Remaining items: {len(cart.state.items)}")

    # Clear cart
    print("\nStep 5: Clearing cart...")
    cart = await service.clear_cart(cart_id)
    print(f"   Cart cleared, items remaining: {len(cart.state.items)}")

    # Step 7: Flush traces
    print("\nStep 6: Flushing traces...")
    trace.get_tracer_provider().force_flush()

    print("\n" + "=" * 60)
    print("Exercise Complete!")
    print("=" * 60)

    print("\nTrace spans created:")
    print("  - cart_service.add_to_cart (x4)")
    print("  - cart_service.get_cart (x1)")
    print("  - cart_service.remove_from_cart (x1)")
    print("  - cart_service.clear_cart (x1)")
    print("  - eventsource.repository.save (x6)")
    print("  - eventsource.repository.load (x4)")
    print("  - in_memory_event_store.append_events (x6)")
    print("  - in_memory_event_store.get_events (x4)")

    print("\nCheck console output above for detailed span information.")
    print("For Jaeger UI: http://localhost:16686 (if Jaeger is running)")


if __name__ == "__main__":
    asyncio.run(main())
