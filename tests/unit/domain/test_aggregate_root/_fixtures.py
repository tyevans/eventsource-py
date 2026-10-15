"""Shared fixtures, events, and aggregate implementations for test_aggregate_root."""

from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel, Field

from eventsource.domain.aggregate import AggregateRoot, DeclarativeAggregate
from eventsource.domain.decorators import handles
from eventsource.domain.event import DomainEvent


class CounterState(BaseModel):
    counter_id: UUID
    value: int = 0
    name: str = ""


class OrderState(BaseModel):
    order_id: UUID
    customer_id: UUID | None = None
    status: str = "draft"
    total: float = 0.0
    items: list[str] = Field(default_factory=list)


class CounterIncremented(DomainEvent):
    aggregate_type: str = "Counter"
    increment: int = 1


class CounterDecremented(DomainEvent):
    aggregate_type: str = "Counter"
    decrement: int = 1


class CounterNamed(DomainEvent):
    aggregate_type: str = "Counter"
    name: str


class CounterReset(DomainEvent):
    aggregate_type: str = "Counter"


class OrderCreated(DomainEvent):
    aggregate_type: str = "Order"
    customer_id: UUID


class OrderItemAdded(DomainEvent):
    aggregate_type: str = "Order"
    item_name: str
    price: float


class OrderShipped(DomainEvent):
    aggregate_type: str = "Order"
    tracking_number: str


class CounterAggregate(AggregateRoot[CounterState]):
    aggregate_type = "Counter"

    def _get_initial_state(self) -> CounterState:
        return CounterState(counter_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, CounterIncremented):
            if self._state is None:
                self._state = self._get_initial_state()
            self._state = self._state.model_copy(
                update={"value": self._state.value + event.increment}
            )
        elif isinstance(event, CounterDecremented):
            if self._state is None:
                self._state = self._get_initial_state()
            self._state = self._state.model_copy(
                update={"value": self._state.value - event.decrement}
            )
        elif isinstance(event, CounterNamed):
            if self._state is None:
                self._state = self._get_initial_state()
            self._state = self._state.model_copy(update={"name": event.name})
        elif isinstance(event, CounterReset):
            self._state = self._get_initial_state()

    def increment(self, amount: int = 1) -> None:
        self.apply_event(
            CounterIncremented(
                aggregate_id=self.aggregate_id,
                aggregate_type=self.aggregate_type,
                aggregate_version=self.get_next_version(),
                increment=amount,
            )
        )

    def decrement(self, amount: int = 1) -> None:
        self.apply_event(
            CounterDecremented(
                aggregate_id=self.aggregate_id,
                aggregate_type=self.aggregate_type,
                aggregate_version=self.get_next_version(),
                decrement=amount,
            )
        )

    def set_name(self, name: str) -> None:
        self.apply_event(
            CounterNamed(
                aggregate_id=self.aggregate_id,
                aggregate_type=self.aggregate_type,
                aggregate_version=self.get_next_version(),
                name=name,
            )
        )

    def reset(self) -> None:
        self.apply_event(
            CounterReset(
                aggregate_id=self.aggregate_id,
                aggregate_type=self.aggregate_type,
                aggregate_version=self.get_next_version(),
            )
        )


class OrderAggregate(AggregateRoot[OrderState]):
    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            self._state = OrderState(
                order_id=self.aggregate_id,
                customer_id=event.customer_id,
                status="created",
            )
        elif isinstance(event, OrderItemAdded):
            if self._state:
                self._state = self._state.model_copy(
                    update={
                        "items": [*self._state.items, event.item_name],
                        "total": self._state.total + event.price,
                    }
                )
        elif isinstance(event, OrderShipped) and self._state:
            self._state = self._state.model_copy(update={"status": "shipped"})

    def create(self, customer_id: UUID) -> None:
        if self.version > 0:
            raise ValueError("Order already exists")
        self.apply_event(
            OrderCreated(
                aggregate_id=self.aggregate_id,
                aggregate_type=self.aggregate_type,
                aggregate_version=self.get_next_version(),
                customer_id=customer_id,
            )
        )

    def add_item(self, item_name: str, price: float) -> None:
        if not self._state or self._state.status == "shipped":
            raise ValueError("Cannot add items to this order")
        self.apply_event(
            OrderItemAdded(
                aggregate_id=self.aggregate_id,
                aggregate_type=self.aggregate_type,
                aggregate_version=self.get_next_version(),
                item_name=item_name,
                price=price,
            )
        )

    def ship(self, tracking_number: str) -> None:
        if not self._state or self._state.status != "created":
            raise ValueError("Cannot ship order in current state")
        self.apply_event(
            OrderShipped(
                aggregate_id=self.aggregate_id,
                aggregate_type=self.aggregate_type,
                aggregate_version=self.get_next_version(),
                tracking_number=tracking_number,
            )
        )


class DeclarativeCounterAggregate(DeclarativeAggregate[CounterState]):
    aggregate_type = "Counter"

    def _get_initial_state(self) -> CounterState:
        return CounterState(counter_id=self.aggregate_id)

    @handles(CounterIncremented)
    def _on_counter_incremented(self, event: CounterIncremented) -> None:
        if self._state is None:
            self._state = self._get_initial_state()
        self._state = self._state.model_copy(update={"value": self._state.value + event.increment})

    @handles(CounterDecremented)
    def _on_counter_decremented(self, event: CounterDecremented) -> None:
        if self._state is None:
            self._state = self._get_initial_state()
        self._state = self._state.model_copy(update={"value": self._state.value - event.decrement})

    @handles(CounterNamed)
    def _on_counter_named(self, event: CounterNamed) -> None:
        if self._state is None:
            self._state = self._get_initial_state()
        self._state = self._state.model_copy(update={"name": event.name})

    @handles(CounterReset)
    def _on_counter_reset(self, event: CounterReset) -> None:
        self._state = self._get_initial_state()

    def increment(self, amount: int = 1) -> None:
        self._raise_event(
            CounterIncremented(
                aggregate_id=self.aggregate_id,
                aggregate_type=self.aggregate_type,
                aggregate_version=self.get_next_version(),
                increment=amount,
            )
        )

    def decrement(self, amount: int = 1) -> None:
        self._raise_event(
            CounterDecremented(
                aggregate_id=self.aggregate_id,
                aggregate_type=self.aggregate_type,
                aggregate_version=self.get_next_version(),
                decrement=amount,
            )
        )


class LenientCounterAggregate(CounterAggregate):
    validate_versions = False
