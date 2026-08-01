# Aggregate styles

`eventsource` supports three ways to write an aggregate: the **decider pattern**
(pure `decide`/`evolve` functions behind `DeciderAggregate`), `DeclarativeAggregate`
(one `@handles`-decorated method per event type), and `AggregateRoot` (a hand-written
`_apply` you dispatch yourself). All three sit on the same base class and are accepted
identically by `AggregateRepository`, snapshotting, and `eventsource.testing` — nothing
downstream knows or cares which style produced the events it's replaying.

The decider is the library's recommended default (ADR-0022) and is what
[Your First Aggregate](../tutorials/03-first-aggregate.md) and
[Getting Started](../getting-started.md) teach first. `DeclarativeAggregate` and
`AggregateRoot` remain fully supported — this page frames them as alternatives with
legitimate niches, not as deprecated paths.

## Style 1: the decider (recommended)

A decider models the aggregate as three pure functions over plain values, with a thin
`DeciderAggregate` adapter wiring them into the repository:

```python
def initial_state(invoice_id: UUID) -> InvoiceState: ...
def decide(command: InvoiceCommand, state: InvoiceState) -> list[DomainEvent]: ...
def evolve(state: InvoiceState, event: DomainEvent) -> InvoiceState: ...
```

`decide` answers "given what has happened, is this allowed, and what facts does it
produce?" — it returns events or raises a rejection. `evolve` answers "given a fact,
what's the next state?" — the same fold that replay and `decide`'s own view of current
state are built from. Neither function touches `self`, versions, or anything async.

### Worked example: Invoice

```python
from __future__ import annotations

from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel

from eventsource import CommandRejectedError, DeciderAggregate, DomainCommand, DomainEvent, register_event


@register_event
class InvoiceDrafted(DomainEvent):
    aggregate_type: str = "Invoice"
    customer_id: UUID
    amount: Decimal


@register_event
class InvoiceSent(DomainEvent):
    aggregate_type: str = "Invoice"


@register_event
class InvoicePaid(DomainEvent):
    aggregate_type: str = "Invoice"
    amount: Decimal


class InvoiceState(BaseModel):
    invoice_id: UUID
    customer_id: UUID | None = None
    amount: Decimal = Decimal("0")
    status: str = "draft"


class Draft(DomainCommand):
    customer_id: UUID
    amount: Decimal


class Send(DomainCommand):
    pass


class Pay(DomainCommand):
    amount: Decimal


def initial_state(invoice_id: UUID) -> InvoiceState:
    return InvoiceState(invoice_id=invoice_id)


def decide(command: object, state: InvoiceState) -> list[DomainEvent]:
    match command, state:
        case Draft(customer_id=cid, amount=amt), InvoiceState(status="draft", customer_id=None):
            return [InvoiceDrafted(aggregate_id=state.invoice_id, customer_id=cid, amount=amt)]
        case Send(), InvoiceState(status="draft", customer_id=not None):
            return [InvoiceSent(aggregate_id=state.invoice_id)]
        case Pay(amount=amt), InvoiceState(status="sent"):
            if amt != state.amount:
                raise CommandRejectedError(f"Expected payment of {state.amount}, got {amt}")
            return [InvoicePaid(aggregate_id=state.invoice_id, amount=amt)]
        case _:
            raise CommandRejectedError(f"Cannot apply {type(command).__name__} to a {state.status} invoice")


def evolve(state: InvoiceState, event: DomainEvent) -> InvoiceState:
    match event:
        case InvoiceDrafted(customer_id=cid, amount=amt):
            return state.model_copy(update={"customer_id": cid, "amount": amt})
        case InvoiceSent():
            return state.model_copy(update={"status": "sent"})
        case InvoicePaid():
            return state.model_copy(update={"status": "paid"})
        case _:
            return state


class Invoice(DeciderAggregate[InvoiceState]):
    aggregate_type = "Invoice"

    @staticmethod
    def initial_state(aggregate_id: UUID) -> InvoiceState:
        return initial_state(aggregate_id)

    @staticmethod
    def decide(command: object, state: InvoiceState) -> list[DomainEvent]:
        return decide(command, state)

    @staticmethod
    def evolve(state: InvoiceState, event: DomainEvent) -> InvoiceState:
        return evolve(state, event)
```

Business rules read as a state-transition table in `decide`, invariants (payment must
match the invoice amount, an invoice can't be paid before it's sent) live entirely in
that one function, and `evolve` is total — every branch a `case _: return state`
fallback catches, so an unhandled event is a silent no-op rather than a crash. Because
neither `decide` nor `evolve` touches infrastructure, both can be unit-tested with
[`DeciderScenario`](../tutorials/08-testing.md#step-1-test-the-decider-with-deciderscenario)
synchronously, no event store or event bus involved. See
[The Decider Pattern on Top of AggregateRoot](decider-pattern.md) for the full pattern
write-up, `DomainCommand` provenance, and benchmarks against the imperative style.

## Choosing a style

| | Decider | `DeclarativeAggregate` | `AggregateRoot` |
| --- | --- | --- | --- |
| **State handling** | Immutable value threaded through pure `evolve`; never touches `self` | `self._state`, replaced via `model_copy` inside `@handles` methods | `self._state`, replaced via `model_copy` inside hand-written `_apply` branches |
| **Invariant placement** | Centralized in `decide`, one `match` arm per legal transition | Scattered across command methods, each checking `self.state` before raising | Same as declarative — invariants live in command methods, not `_apply` |
| **Testability** | `decide`/`evolve` are pure functions — plain asserts, no async, no infrastructure (`DeciderScenario`) | Needs a live aggregate instance and the async BDD helpers (`given_events`/`when_command`) to exercise fully | Same infrastructure requirement as declarative |
| **When it fits** | Default choice for new aggregates; domains with several legal/illegal transitions worth enumerating explicitly | Aggregates past ~4-5 event types where an `if/elif` chain gets unwieldy, and you want `unregistered_event_handling` or `requires_creation_event` | Small aggregates (2-3 events) where a class is being migrated from legacy code, or the `if/elif` chain is genuinely simpler than a registry |

One differentiator worth calling out explicitly: `DeclarativeAggregate` defaults
`unregistered_event_handling = "error"`, so an event with no `@handles` method raises
`UnhandledEventError` instead of silently doing nothing. `AggregateRoot`'s hand-written
`_apply` has no equivalent — an event that falls off the end of your `if/elif` chain is
persisted and version-counted with no exception anywhere. The decider's `evolve` is
explicitly total (`case _: return state`), which is a third, deliberate answer to the
same question: unhandled events are always a documented no-op, never an error.

All three styles share everything except how an event reaches your state-mutation
code: identity and lifecycle, `apply_event()`'s version check, `load_from_history()`,
`create_event()`, commit bookkeeping, and snapshotting are defined once on
`AggregateRoot` and behave identically regardless of which style you pick.
`aggregate_type` is a required class attribute on all three — a subclass that omits it
raises `AggregateTypeNotSetError` at construction.

## Style 2: `DeclarativeAggregate` with `@handles`

Subclassing `DeclarativeAggregate[TState]` means writing one method per event type,
each tagged `@handles(EventType)`, instead of a hand-written `_apply`. The base class
supplies `_apply` as an exact-type lookup into a per-subclass registry built by
`__init_subclass__`, so "what does this event do to state?" is answered by finding the
method named for it rather than reading a branch chain top to bottom.

### Worked reference example: ShoppingCart (`examples/aggregate_example.py`)

```python
class ShoppingCartAggregate(DeclarativeAggregate[ShoppingCartState]):
    aggregate_type = "ShoppingCart"

    def _get_initial_state(self) -> ShoppingCartState:
        return ShoppingCartState(cart_id=self.aggregate_id)

    @handles(CartCreated)
    def _on_cart_created(self, event: CartCreated) -> None:
        self._state = ShoppingCartState(
            cart_id=self.aggregate_id,
            customer_id=event.customer_id,
            status="empty",
        )

    @handles(CartAbandoned)
    def _on_cart_abandoned(self, event: CartAbandoned) -> None:
        if self._state:
            self._state = self._state.model_copy(update={"status": "abandoned"})

    def checkout(self) -> UUID:
        if not self.state:
            raise ValueError("Cart does not exist")
        if self.state.status != "active":
            raise ValueError(f"Cannot checkout {self.state.status} cart")
        order_id = uuid4()
        self.create_event(CartCheckedOut, order_id=order_id, total_amount=self.state.total_amount)
        return order_id
```

`ShoppingCartAggregate` handles six event types total (`CartCreated`,
`ItemAddedToCart`, `ItemRemovedFromCart`, `ItemQuantityChanged`, `CartCheckedOut`,
`CartAbandoned`) — roughly where an `if/elif` chain stops paying for itself and a
per-event method starts being easier to read. Dispatch is registration by exact
`type(event)`, not `isinstance`: a subclass of a registered event type is *not* routed
to the parent's handler, which is the opposite of an `isinstance` chain's fall-through
behavior. Command methods (`checkout`, `add_item`, ...) are ordinary methods that
validate `self.state` and call `create_event(...)`, same shape under all three styles.

The two capabilities `AggregateRoot` has no equivalent for are
`unregistered_event_handling` (`"ignore"` / `"warn"` / `"error"`, default `"error"`)
and `requires_creation_event`, which lets `_get_initial_state()` return `None` instead
of forcing every aggregate to invent a plausible empty state. Both come from the base
class knowing which event types the registry covers — something a hand-written
`_apply` has no way to know about itself.

## Style 3: `AggregateRoot` with a hand-written `_apply` (migrating from legacy)

Subclassing `AggregateRoot[TState]` directly means implementing `_apply(event)` and
`_get_initial_state()` yourself, with no registry between an event arriving and your
code deciding what it means. This is the style you'll meet migrating an older
event-sourced codebase onto the library, or writing a genuinely tiny aggregate where a
three-branch `if/elif` is simpler than a registry.

### Worked reference example: BankAccount (`examples/imperative_example.py`)

```python
class BankAccountAggregate(AggregateRoot[BankAccountState]):
    aggregate_type = "BankAccount"

    def _get_initial_state(self) -> BankAccountState:
        return BankAccountState(account_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, AccountOpened):
            self._state = BankAccountState(
                account_id=self.aggregate_id,
                owner_name=event.owner_name,
                balance=event.initial_balance,
                is_open=True,
            )
        elif isinstance(event, MoneyDeposited):
            if self._state:
                self._state = self._state.model_copy(
                    update={"balance": self._state.balance + event.amount}
                )
        elif isinstance(event, MoneyWithdrawn) and self._state:
            self._state = self._state.model_copy(
                update={"balance": self._state.balance - event.amount}
            )

    def deposit(self, amount: Decimal) -> None:
        if not self.state or not self.state.is_open:
            raise ValueError("Account is not open")
        if amount <= 0:
            raise ValueError("Deposit amount must be positive")
        self.create_event(MoneyDeposited, amount=amount)
```

Everything about matching is yours here, and the failure modes are structural to the
style rather than to this aggregate: an event with no matching branch falls off the
end of the chain and is silently discarded, but `apply_event` has already advanced
`_version` and appended the event to `_uncommitted_events` before `_apply` runs — so
the event is persisted and counted, and only the state reflects nothing. `_state` is
`None` until a branch assigns it, so every branch reading existing state has to guard
first (`if self._state:`), and `isinstance` matching means a parent event class placed
before its subclass in the chain silently swallows the child. None of these are exotic
at three event types, which is exactly why this style keeps a niche: past that point,
the decider or `DeclarativeAggregate` scale better and this page's comparison table
above is the place to decide which.

## Migrating between styles

Because everything except `_apply`/`decide`+`evolve` is shared, moving an aggregate
from one style to another is a change confined to that class's file: the repository,
snapshotting, and `eventsource.testing` accept whichever style you land on without
modification. Migrating `AggregateRoot` → `DeclarativeAggregate` means replacing the
`if/elif` chain with one `@handles` method per branch. Migrating either imperative
style → the decider means extracting the state-mutation logic into a standalone
`evolve` function and the command-validation logic into `decide`, then wrapping both
in a `DeciderAggregate` adapter as shown above.
