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

    def _ensure_state(self) -> CounterState:
        self._state = self._state or self._get_initial_state()
        return self._state

    def _make_event(self, cls: type[DomainEvent], **kwargs: object) -> DomainEvent:
        v, aid, atype = self.get_next_version(), self.aggregate_id, self.aggregate_type
        return cls(aggregate_id=aid, aggregate_type=atype, aggregate_version=v, **kwargs)  # type: ignore[arg-type]

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, CounterIncremented):
            state = self._ensure_state()
            self._state = state.model_copy(update={"value": state.value + event.increment})
        elif isinstance(event, CounterDecremented):
            state = self._ensure_state()
            self._state = state.model_copy(update={"value": state.value - event.decrement})
        elif isinstance(event, CounterNamed):
            self._state = self._ensure_state().model_copy(update={"name": event.name})
        elif isinstance(event, CounterReset):
            self._state = self._get_initial_state()

    def increment(self, amount: int = 1) -> None:
        self.apply_event(self._make_event(CounterIncremented, increment=amount))

    def decrement(self, amount: int = 1) -> None:
        self.apply_event(self._make_event(CounterDecremented, decrement=amount))

    def set_name(self, name: str) -> None:
        self.apply_event(self._make_event(CounterNamed, name=name))

    def reset(self) -> None:
        self.apply_event(self._make_event(CounterReset))


class OrderAggregate(AggregateRoot[OrderState]):
    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    def _make_event(self, cls: type[DomainEvent], **kwargs: object) -> DomainEvent:
        v, aid, atype = self.get_next_version(), self.aggregate_id, self.aggregate_type
        return cls(aggregate_id=aid, aggregate_type=atype, aggregate_version=v, **kwargs)  # type: ignore[arg-type]

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            self._state = OrderState(
                order_id=self.aggregate_id, customer_id=event.customer_id, status="created"
            )
        elif isinstance(event, OrderItemAdded) and self._state:
            items, total = [*self._state.items, event.item_name], self._state.total + event.price
            self._state = self._state.model_copy(update={"items": items, "total": total})
        elif isinstance(event, OrderShipped) and self._state:
            self._state = self._state.model_copy(update={"status": "shipped"})

    def create(self, customer_id: UUID) -> None:
        if self.version > 0:
            raise ValueError("Order already exists")
        self.apply_event(self._make_event(OrderCreated, customer_id=customer_id))

    def add_item(self, item_name: str, price: float) -> None:
        if not self._state or self._state.status == "shipped":
            raise ValueError("Cannot add items to this order")
        self.apply_event(self._make_event(OrderItemAdded, item_name=item_name, price=price))

    def ship(self, tracking_number: str) -> None:
        if not self._state or self._state.status != "created":
            raise ValueError("Cannot ship order in current state")
        self.apply_event(self._make_event(OrderShipped, tracking_number=tracking_number))


class DeclarativeCounterAggregate(DeclarativeAggregate[CounterState]):
    aggregate_type = "Counter"

    def _get_initial_state(self) -> CounterState:
        return CounterState(counter_id=self.aggregate_id)

    def _ensure_state(self) -> CounterState:
        self._state = self._state or self._get_initial_state()
        return self._state

    @handles(CounterIncremented)
    def _on_counter_incremented(self, event: CounterIncremented) -> None:
        state = self._ensure_state()
        self._state = state.model_copy(update={"value": state.value + event.increment})

    @handles(CounterDecremented)
    def _on_counter_decremented(self, event: CounterDecremented) -> None:
        state = self._ensure_state()
        self._state = state.model_copy(update={"value": state.value - event.decrement})

    @handles(CounterNamed)
    def _on_counter_named(self, event: CounterNamed) -> None:
        self._state = self._ensure_state().model_copy(update={"name": event.name})

    @handles(CounterReset)
    def _on_counter_reset(self, event: CounterReset) -> None:
        self._state = self._get_initial_state()

    def _emit(self, cls: type[DomainEvent], **kwargs: object) -> None:
        v, aid, atype = self.get_next_version(), self.aggregate_id, self.aggregate_type
        self._raise_event(
            cls(aggregate_id=aid, aggregate_type=atype, aggregate_version=v, **kwargs)
        )  # type: ignore[arg-type]

    def increment(self, amount: int = 1) -> None:
        self._emit(CounterIncremented, increment=amount)

    def decrement(self, amount: int = 1) -> None:
        self._emit(CounterDecremented, decrement=amount)


class LenientCounterAggregate(CounterAggregate):
    validate_versions = False
