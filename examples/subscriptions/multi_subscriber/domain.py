"""
Product domain events and aggregate for multi-subscriber example.
"""

from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel

from eventsource import (
    AggregateRoot,
    DomainEvent,
    register_event,
)


@register_event
class ProductCreated(DomainEvent):
    """Event emitted when a product is created."""

    aggregate_type: str = "Product"

    name: str
    price: float
    category: str


@register_event
class ProductPriceChanged(DomainEvent):
    """Event emitted when a product price changes."""

    aggregate_type: str = "Product"

    old_price: float
    new_price: float


@register_event
class InventoryAdded(DomainEvent):
    """Event emitted when inventory is added."""

    aggregate_type: str = "Product"

    quantity: int
    warehouse: str


@register_event
class InventoryReserved(DomainEvent):
    """Event emitted when inventory is reserved for an order."""

    aggregate_type: str = "Product"

    quantity: int
    order_id: UUID


@register_event
class InventorySold(DomainEvent):
    """Event emitted when inventory is sold."""

    aggregate_type: str = "Product"

    quantity: int
    order_id: UUID


class ProductState(BaseModel):
    """Product aggregate state."""

    product_id: UUID
    name: str = ""
    price: float = 0.0
    category: str = ""
    inventory: int = 0


class ProductAggregate(AggregateRoot[ProductState]):
    """Event-sourced product aggregate."""

    aggregate_type = "Product"

    def _get_initial_state(self) -> ProductState:
        return ProductState(product_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, ProductCreated):
            self._state = ProductState(
                product_id=self.aggregate_id,
                name=event.name,
                price=event.price,
                category=event.category,
                inventory=0,
            )
        elif isinstance(event, ProductPriceChanged):
            if self._state:
                self._state = self._state.model_copy(update={"price": event.new_price})
        elif isinstance(event, InventoryAdded):
            if self._state:
                new_inventory = self._state.inventory + event.quantity
                self._state = self._state.model_copy(update={"inventory": new_inventory})
        elif isinstance(event, InventoryReserved):
            if self._state:
                new_inventory = self._state.inventory - event.quantity
                self._state = self._state.model_copy(update={"inventory": new_inventory})
        elif isinstance(event, InventorySold):
            # Inventory already reduced during reservation
            pass

    def create(self, name: str, price: float, category: str) -> None:
        """Create a new product."""
        if self.version > 0:
            raise ValueError("Product already created")

        event = ProductCreated(
            aggregate_id=self.aggregate_id,
            name=name,
            price=price,
            category=category,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

    def change_price(self, new_price: float) -> None:
        """Change the product price."""
        if not self.state:
            raise ValueError("Product not created")

        event = ProductPriceChanged(
            aggregate_id=self.aggregate_id,
            old_price=self.state.price,
            new_price=new_price,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

    def add_inventory(self, quantity: int, warehouse: str) -> None:
        """Add inventory for the product."""
        if not self.state:
            raise ValueError("Product not created")

        event = InventoryAdded(
            aggregate_id=self.aggregate_id,
            quantity=quantity,
            warehouse=warehouse,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

    def reserve_inventory(self, quantity: int, order_id: UUID) -> None:
        """Reserve inventory for an order."""
        if not self.state:
            raise ValueError("Product not created")
        if self.state.inventory < quantity:
            raise ValueError("Insufficient inventory")

        event = InventoryReserved(
            aggregate_id=self.aggregate_id,
            quantity=quantity,
            order_id=order_id,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

    def confirm_sale(self, quantity: int, order_id: UUID) -> None:
        """Confirm a sale (after reservation)."""
        event = InventorySold(
            aggregate_id=self.aggregate_id,
            quantity=quantity,
            order_id=order_id,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)


__all__ = [
    "InventoryAdded",
    "InventoryReserved",
    "InventorySold",
    "ProductAggregate",
    "ProductCreated",
    "ProductPriceChanged",
    "ProductState",
]
