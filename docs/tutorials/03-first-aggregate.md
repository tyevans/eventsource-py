# Your First Aggregate

In this tutorial you will build a complete event-sourced aggregate from scratch: an
`Order` that can be created, shipped, and cancelled.

An *aggregate* is the consistency boundary of an event-sourced system. Instead of
storing the current row of an order in a table, you store the sequence of things that
happened to it -- `OrderCreated`, `OrderShipped` -- and rebuild the current state by
folding those events together. In `eventsource`, the recommended way to write that is
the **decider style**: the domain is two pure functions, `decide` and `evolve`, and a
thin subclass of `DeciderAggregate[TState, TCommand]` adapts them to the library's replay,
snapshot, and repository machinery.

You will write the state model, the events, and the commands that request them; write
`decide()` as the one place business rules live, and see it reject a command with
`CommandRejectedError`; write `evolve()` as the fold that turns an event into the next
state; call `execute()` to run a command end to end; watch `state`, `version`, and
`uncommitted_events` change as you go; rebuild the order from its history with
`load_from_history()`; and finally see the same domain sketched in the other two
aggregate styles this library supports.

Everything here runs in plain Python -- no database, no Docker, no event store. You are
working with the aggregate in memory, which is exactly how aggregates are unit tested.
Persisting the events comes later.

By the end you will have a working `OrderAggregate` and a clear picture of the three
things every decider aggregate needs: a state to start from, a function that decides
whether a command is allowed, and a function that turns the events it produces into new
state.

## What you'll build

A single Python file containing an `OrderAggregate` and the pieces it needs:

- **`OrderState`** -- a Pydantic `BaseModel` holding the order's current status, its
  customer, its total, and its tracking number.
- **Three domain events** -- `OrderCreated`, `OrderShipped`, and `OrderCancelled`, each
  a `DomainEvent` subclass carrying only the fields that event needs.
- **Three commands** -- `CreateOrder`, `ShipOrder`, and `CancelOrder`, each a
  `DomainCommand` subclass carrying the intent's payload.
- **`OrderAggregate(DeciderAggregate[OrderState, OrderCommand])`** -- with `aggregate_type = "Order"`,
  a static `initial_state()` that seeds a fresh order, a static `decide()` that turns a
  command plus the current state into the events it produces (or raises
  `CommandRejectedError`), and a static `evolve()` that folds each event type into a new
  `OrderState`.

By Step 8 you will run the file and watch three things move together: `state` (the
folded `OrderState`), `version` (which advances to match each event's
`aggregate_version`), and `uncommitted_events` (the list the repository will later
persist).

The later steps sharpen it. Step 6 makes `decide()` refuse to ship a cancelled order --
`CommandRejectedError` is raised and no event is produced, no version bump, nothing
queued. Step 9 looks at what `execute()` stamped onto the events for free: `causation_id`
linking each event back to the command that caused it, plus `correlation_id`,
`actor_id`, and `tenant_id`. Step 10 throws away the in-memory instance and rebuilds an
identical order from its event list with `load_from_history()` -- the moment event
sourcing pays off.

Then a look at the other two ways to write an aggregate in this library, for when you
run into an existing codebase that uses them.

Here is the shape you are heading toward:

```python
class OrderAggregate(DeciderAggregate[OrderState, OrderCommand]):
    aggregate_type = "Order"

    @staticmethod
    def initial_state() -> OrderState: ...

    @staticmethod
    def decide(command: OrderCommand, state: OrderState) -> list[DomainEvent]: ...

    @staticmethod
    def evolve(state: OrderState, event: DomainEvent) -> OrderState: ...
```

Three static methods, none of them touching `self`. That is the whole domain; everything
else -- version tracking, replay, provenance stamping -- is inherited.

## Prerequisites

Before you start you need:

- **Python 3.13 or newer** -- the package declares `requires-python = ">=3.13"`.
- **`eventsource-py` installed.** From a clone of the repository, `uv sync --all-extras`;
  otherwise `pip install eventsource-py`. Nothing in this tutorial needs an optional
  extra -- the core install (pydantic + sqlalchemy) is enough.
- **Comfort with pydantic v2 models.** `OrderState`, every event, and every command you
  write are `BaseModel` subclasses, and both `DomainEvent` and `DomainCommand` set
  `model_config = ConfigDict(frozen=True)`, so you will update state with
  `model_copy(update={...})` rather than by assignment.
- **[Your First Event](02-first-event.md), or its equivalent.** This tutorial assumes you
  can already declare a `DomainEvent` subclass and know that events are immutable records
  of something that happened.

You do **not** need Docker, a database, or a message broker. An aggregate is plain
Python: it holds state in memory, folds events into it, and collects the events it
raised in `uncommitted_events` for someone else to persist later. Every snippet below can
be pasted into a single file and run top to bottom with `python`, and none of the code is
async -- `DeciderAggregate`'s methods are all synchronous.

Check your install before you begin:

```bash
python -c "import eventsource; print(eventsource.__version__)"
```

If that prints a version, you are ready.

## Step 1: Model the state (a Pydantic `BaseModel`)

Start with the answer to a single question: *what does the code need to know about an
order right now?* Not the history -- the history is the events. Just the current facts
`decide()` will read before it decides whether to allow something.

For our order that is four things: who the customer is, how much it is for, what status
it is in, and (once shipped) the tracking number.

Create a file called `first_aggregate.py` and start it like this:

```python
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel


class OrderState(BaseModel):
    customer_id: UUID | None = None
    total: Decimal = Decimal("0")
    status: str = "pending"
    tracking_number: str | None = None
```

That is the whole state model. A few things about it are deliberate.

**It is a Pydantic `BaseModel`, and that is a hard requirement.** `AggregateRoot` (which
`DeciderAggregate` extends) is declared as `class AggregateRoot[TState: BaseModel](ABC)`,
an inline PEP 695 type parameter rather than a module-level `TypeVar`. Anything you plug
in as `TState` must be a
`BaseModel` subclass. A dataclass or a plain dict will not type check, and the snapshot
machinery would break on it: `_serialize_state()` calls `self._state.model_dump(mode="json")`,
and `_restore_from_snapshot()` calls `state_type.model_validate(state_dict)`. Those two
methods only exist because the state is a pydantic model.

**Every field has a default, and there is no id field.** In Step 4 you will write
`initial_state()`, which constructs an `OrderState` before any event has been applied --
a blank order that exists only so `evolve()` has something to fold the first event into.
Defaults are what make that no-argument constructor possible. The order's identity is
not among them: which order a command is about is carried by the *command* (Step 3),
which is where `decide()` reads it from when it builds an event. Fields that are unknown until
an event arrives (`customer_id`, `tracking_number`) are typed as optional; fields with a
sensible zero value (`total`, `status`) get one.

**`status` is a plain `str` here** to keep the tutorial short. In a real aggregate you
would reach for an enum:

```python
from enum import StrEnum

class OrderStatus(StrEnum):
    PENDING = "pending"
    CREATED = "created"
    SHIPPED = "shipped"
    CANCELLED = "cancelled"
```

Pydantic validates enum members on assignment and serializes them as strings, so
swapping `status: str` for `status: OrderStatus = OrderStatus.PENDING` costs nothing and
buys you a typo-proof invariant check when `decide()` matches on it in Step 6.

**`total` is a `Decimal`, not a `float`.** Money in a `float` accumulates rounding error,
and this value will be reconstructed from JSON on every replay. Pydantic handles
`Decimal` round-tripping for you.

### State is not the event, and it is not the database row

Two habits from other architectures are worth unlearning right now.

*State is not a mirror of the events.* `OrderCreated` will carry `customer_id` and
`total`; `OrderShipped` will carry `tracking_number`. `OrderState` holds all of those
together, flattened, with no notion of which event put them there. If you find yourself
adding a `last_event_type` field, that is the version counter's job, not the state's.

*State is not the read model.* Resist putting anything in `OrderState` that `decide()`
will never read. Display names, formatted currency strings, denormalized customer
addresses -- those belong in a projection (see
[Projections](06-projections.md)). The aggregate's state exists to answer the question
"is this command allowed?", and it is loaded and rebuilt on every single command, so
keep it small.

A useful test: for each field, name the `decide()` branch that reads it. `status` is
read by every branch to reject invalid transitions. `customer_id` and `total` are there because a real system would check them
(a refund limit, an ownership check) and because they make the folded state visible when
you print it in Step 8. `tracking_number` records the shipment. Nothing else earns a
slot.

### Should the state be frozen?

`DomainEvent` (and, as you will see in the next step, `DomainCommand`) sets
`model_config = ConfigDict(frozen=True)` because both are records of an intent or a fact
that must never change after the fact. `OrderState` is different -- it is a value
`evolve()` replaces on every applied event, so freezing it is optional.

Leaving it mutable is fine as long as you keep the discipline of building a *new* state
object in `evolve()` rather than mutating fields in place, which is what the
`model_copy(update={...})` pattern in Step 5 does. If you would rather have pydantic
enforce that discipline for you, add the config:

```python
from pydantic import BaseModel, ConfigDict


class OrderState(BaseModel):
    model_config = ConfigDict(frozen=True)

    customer_id: UUID | None = None
    # ... as above
```

With `frozen=True`, `state.status = "shipped"` raises a `ValidationError` instead of
silently succeeding, and `model_copy(update={...})` remains the only way forward. The
rest of this tutorial works identically either way; the snippets use the unfrozen
version and never mutate in place.

With the state modelled, the next step is the facts that move it from one `OrderState`
to the next.

## Step 2: Define the domain events (`OrderCreated`, `OrderShipped`, `OrderCancelled`)

Now write the facts. Each event is a `DomainEvent` subclass, and it carries only the
fields that *this* thing happening introduces -- not a snapshot of the whole order.

Add to `first_aggregate.py`:

```python
from eventsource import DomainEvent


class OrderCreated(DomainEvent):
    aggregate_type: str = "Order"
    customer_id: UUID
    total: Decimal


class OrderShipped(DomainEvent):
    aggregate_type: str = "Order"
    tracking_number: str


class OrderCancelled(DomainEvent):
    aggregate_type: str = "Order"
    reason: str
```

Three classes, six payload lines. Everything else an event needs -- who it belongs to,
when it happened, what version it is -- comes from `DomainEvent`.

### What you get for free

`DomainEvent` already declares the envelope fields, so you never redeclare them:

| Field | Default | Notes |
| --- | --- | --- |
| `event_id` | `uuid4()` | unique per event instance |
| `event_type` | class name | auto-derived, see below |
| `event_version` | `1` | schema version, for later migrations |
| `occurred_at` | `datetime.now(UTC)` | timezone-aware |
| `aggregate_id` | **required** | filled by `decide()` in Step 6 |
| `aggregate_type` | **required** | we default it to `"Order"` above |
| `aggregate_version` | `1` | filled by `execute()`'s stamping, see Step 9 |
| `tenant_id`, `actor_id`, `causation_id` | `None` | filled from the command by `execute()`, see Step 9 |
| `correlation_id` | `uuid4()` | links events across aggregates and commands |
| `metadata` | `{}` | free-form dict |

Two of those are declared with `...` (required) on the base class: `aggregate_id` and
`aggregate_type`. Giving `aggregate_type` a default of `"Order"` on each event class
means you can construct one in a test without repeating it, and it documents which
aggregate the event belongs to right at the class definition. `aggregate_id` stays
required -- there is no sensible default for "which order" -- and Step 6 shows `decide()`
supplying it from the state it was handed.

### `event_type` derives itself from the class name

You do not write `event_type = "OrderCreated"`. `DomainEvent.__init_subclass__` sets the
field default to the class name at class-definition time, and a
`model_validator(mode="before")` does the same when an event is built from a dict
(`model_validate`, `from_dict`). So:

```python
event = OrderCreated(aggregate_id=uuid4(), customer_id=uuid4(), total=Decimal("42.00"))
assert event.event_type == "OrderCreated"
```

If you *do* set `event_type` explicitly and it differs from the class name -- which is
what you want when renaming a class without rewriting stored history -- the library logs
a warning. Silence it with the class variable:

```python
class OrderCreated(DomainEvent):
    event_type: str = "order.created.v2"
    suppress_event_type_warning = True
    aggregate_type: str = "Order"
    customer_id: UUID
    total: Decimal
```

Leave it auto-derived for this tutorial.

### Events are frozen

`DomainEvent` sets `model_config = ConfigDict(frozen=True)`. An event records something
that already happened, so mutating it is meaningless:

```python
event.total = Decimal("0")   # pydantic ValidationError: instance is frozen
```

This is one of two places the library forces immutability on you (the other is the
command you are about to write), and it is the reason `evolve()` in Step 5 builds a new
`OrderState` instead of editing the event.

### Choosing the payload

Each event answers "what changed?", and nothing more:

- `OrderCreated` introduces the customer and the total -- the two facts that exist only
  once the order does.
- `OrderShipped` introduces the tracking number. It does **not** repeat `customer_id`;
  the fold in Step 5 already has that from `OrderCreated`.
- `OrderCancelled` carries a `reason`, because "why" is a fact worth keeping and there is
  nowhere else to keep it.

Notice what is absent: no `status` field. The status is *derived* -- `evolve()` sets it
to `"shipped"` when it sees an `OrderShipped`. Putting a `status` field on the event
would let the two disagree. The same rule rules out `new_total`-style fields that restate
the whole aggregate: store the delta or the new fact, and let the fold compute the rest.

Also notice the names are past tense. `OrderShipped`, not `ShipOrder`. `ShipOrder` is the
command you write next -- a request that might be refused (Step 6 refuses one). An event
is what remains after the refusal could no longer happen.

Keep the payload types the same as the state types -- `total: Decimal` here matches
`total: Decimal` on `OrderState`, so no conversion happens in `evolve()`, and the value
round-trips through JSON with pydantic's `Decimal` handling intact.

### A note on the registry

Nothing above registers these classes anywhere. Auto-derivation of `event_type` happens
at subclass creation, but adding the class to the global `EventRegistry` is explicit --
you apply the `@register_event` decorator, or call `registry.register(OrderCreated)`.
You need that only when something has to turn a stored `event_type` string back into a
class, which is a concern for the event store, not for the in-memory aggregate you are
building here. Registering a second class under a name already taken raises
`DuplicateEventTypeError`, so it is a deliberate step rather than a silent one.

With the facts defined, the next step is the requests that might produce them.

## Step 3: Define the commands (`CreateOrder`, `ShipOrder`, `CancelOrder`)

An event is a fact: it happened, full stop. A command is a *request* that the domain is
free to refuse. `eventsource` gives requests their own base class, `DomainCommand`, so
the distinction is visible in the type system and not just in a naming convention.

Add to `first_aggregate.py`:

```python
from eventsource import DomainCommand


class CreateOrder(DomainCommand):
    order_id: UUID
    customer_id: UUID
    total: Decimal


class ShipOrder(DomainCommand):
    order_id: UUID
    tracking_number: str


class CancelOrder(DomainCommand):
    order_id: UUID
    reason: str


OrderCommand = CreateOrder | ShipOrder | CancelOrder
```

Three classes, and every one of them names the order it targets. Commands carry no
`aggregate_type` -- a command does not belong to a stream the way an event does -- but
they do carry the aggregate id, because that is the only route by which `decide()`, a
pure function with no `self`, learns which order it is deciding about. `decide()` copies
it onto every event it returns as `aggregate_id`, which is why `initial_state()` in
Step 4 needs no arguments.

`OrderCommand`, the union of all three, is the vocabulary of everything this order can
be asked to do. Step 4 hands it to `DeciderAggregate` as a second type parameter, which
is what lets your type checker reject a command this aggregate was never meant to
receive -- see [Why the second type parameter](#why-the-second-type-parameter).

### What you get for free

Like `DomainEvent`, `DomainCommand` declares an envelope so you only write the payload:

| Field | Default | Notes |
| --- | --- | --- |
| `command_id` | `uuid4()` | becomes the `causation_id` of every event this command produces |
| `issued_at` | `datetime.now(UTC)` | timezone-aware |
| `correlation_id` | `uuid4()` | a fresh chain by default; see `caused_by()` below |
| `actor_id` | `None` | who issued the command, if you track that |
| `tenant_id` | `None` | falls back to the ambient tenant context if unset -- see Step 9 |

### Commands are never persisted

This is the load-bearing difference from events. `CreateOrder` and `ShipOrder` are never
written to an event store, never registered, never serialized for storage. A rejected
command -- Step 6 rejects one -- leaves no trace at all: no version bump, no event, no
row anywhere. Only what `decide()` decided to *allow* becomes a stored fact. That is why
there is no `@register_event`-equivalent for commands and no command bus in this
library: a command's entire lifetime is the one `decide()` call that consumes it.

### Commands are frozen, and have no `causation_id` of their own

```python
command = ShipOrder(order_id=order_id, tracking_number="1Z999")
command.tracking_number = "1Z000"   # pydantic ValidationError: instance is frozen
```

Notice `DomainCommand` has no `causation_id` field, even though events do. That is
deliberate: within one workflow, `event -> command -> event` linkage is expressed
through `correlation_id`, not `causation_id`. If a saga reacts to an event by issuing a
new command, it calls `command.caused_by(event)` to copy that event's `correlation_id`
onto the new command, continuing the chain without needing an event-to-command
`causation_id` field. You will see the other end of that chain -- command to event -- in
Step 9, where `execute()` copies `command_id` onto each event's `causation_id`.

With state, events, and commands all defined, you have every ingredient the domain
needs. Next you connect them with an aggregate class.

## Step 4: Subclass `DeciderAggregate[OrderState, OrderCommand]` and set `aggregate_type`

Now the class itself. Add this to `first_aggregate.py`:

```python
from eventsource import DeciderAggregate


class OrderAggregate(DeciderAggregate[OrderState, OrderCommand]):
    aggregate_type = "Order"

    @staticmethod
    def initial_state() -> OrderState:
        return OrderState()
```

Three lines of substance, and each one carries weight.

### The `[OrderState]` parameter is not decoration

`DeciderAggregate` subclasses `AggregateRoot`, declared `[TState: BaseModel](ABC)`. Writing
`DeciderAggregate[OrderState, OrderCommand]` tells the type checker that `self.state` is an
`OrderState`, so `state.status` in Step 6 is checked rather than guessed at. It also
does real work at runtime: it is how `_get_state_type()` resolves the class snapshots
get rehydrated into. Always write the parameter.

### Why the second type parameter

`TCommand` is what makes `execute()` and `decide()` speak your domain's vocabulary
rather than accepting anything at all. It defaults to `object`, so
`DeciderAggregate[OrderState]` on its own is valid and still type checks -- but the
default is the permissive one:

```python
class Unrelated(DomainCommand):
    note: str


order.execute(Unrelated(note="nope"))
```

With `DeciderAggregate[OrderState]`, that line passes a type check. Nothing in the
signature says an order only accepts order commands, so the mistake survives until
runtime, where it falls through `decide()`'s final `case _` and raises
`CommandRejectedError("unknown command: ...")`.

With `DeciderAggregate[OrderState, OrderCommand]`, your type checker rejects it before
you run anything:

```
error: Argument 1 to "execute" of "DeciderAggregate" has incompatible type
"Unrelated"; expected "CreateOrder | ShipOrder | CancelOrder"  [arg-type]
```

That is the whole benefit, and it costs one union declaration. Write the second
parameter whenever your commands form a closed set -- which is nearly always. The
one-parameter form stays available for the case where an aggregate genuinely accepts
commands it cannot enumerate ahead of time.

### `aggregate_type` is the stream's name

`AggregateRoot` declares `aggregate_type: ClassVar[str]` with no default, and you are
required to set it -- a subclass that doesn't raises `AggregateTypeNotSetError` at
construction. It is a plain string on the class -- not a property, not derived from
the class name -- so `OrderAggregate` becomes `"Order"` only because you said so. That
string ends up in three places: on every event `execute()` stamps (it must agree with
the `aggregate_type` you defaulted on each event class in Step 2), in the event
store's stream identity (`get_events(aggregate_id, aggregate_type)`), and in
`AggregateRepository`, which refuses to build a repository for a class whose
`aggregate_type` is empty.

Because it is baked into stored events, **treat `aggregate_type` as permanent**. Renaming
the Python class from `OrderAggregate` to `PurchaseOrderAggregate` is free; changing the
string from `"Order"` to `"PurchaseOrder"` orphans every event already written under the
old name. Pick a short domain noun -- `"Order"`, not `"OrderAggregate"` and not
`"orders"` -- and leave it alone.

### `initial_state()` is a static method, and it *is* called for you

This is the first real departure from the two-abstract-method `AggregateRoot` you might
have already met (or will meet in the "other styles" section below). There,
`_get_initial_state()` is a factory the base class never invokes on your behalf -- your
own `_apply()` has to call it, and `self.state` is `None` until it does. Here,
`DeciderAggregate.__init__` calls `self.initial_state()` immediately and
assigns the result:

```python
print(OrderAggregate.initial_state())
```

That prints `OrderState(customer_id=None, total=Decimal('0'),
status='pending', tracking_number=None)` -- calling a `@staticmethod` needs no aggregate
instance, which is why this runs even though `OrderAggregate` cannot be instantiated
yet (`decide` and `evolve` are still unimplemented; more on that in Step 6). Once the
class is complete, `DeciderAggregate.__init__` calls exactly this method for you, so
`order.state` is never `None` on a `DeciderAggregate` -- accessing it before any event
has been applied hands back the blank order `initial_state()` built, not an exception
and not `None`. That is what lets `decide()` in Step 6 pattern-match on `state.status`
unconditionally, with no `if state is None` guard anywhere in the domain code.

`initial_state()` is a `@staticmethod` for the same reason `decide()` and `evolve()`
will be: none of the three touch `self`, only their arguments. That is what makes them
independently testable -- `OrderAggregate.initial_state()` is a plain function
call, no aggregate instance required.

### Keep it cheap and deterministic

`initial_state()` runs once per instance, in `__init__`, and (as you will see in Step
10) once more per `load_from_history()` call on a fresh instance. Two rules follow:

- **No I/O.** No database reads, no clock lookups that end up in state, no random values.
  The initial state must be identical every time it is built, or replaying the same
  history twice would produce two different aggregates.
- **It takes no arguments, and that is the point.** "Before anything has happened" is
  one value for the whole aggregate type, not one per order. There is nothing to derive
  from, and nothing you need to: everything else arrives as an event.

If you catch yourself wanting a parameter here -- a tenant, a currency, a customer --
that is a fact about the order, and facts arrive in `OrderCreated`. If you want the
order id, it is on the command that `decide()` is holding.

With a starting state defined, the aggregate needs a way to fold events into it.

## Step 5: Implement `evolve()` to fold events into state

`evolve()` takes the current state and one event, and returns the next state. Add it to
the class:

```python
class OrderAggregate(DeciderAggregate[OrderState, OrderCommand]):
    aggregate_type = "Order"

    @staticmethod
    def initial_state() -> OrderState:
        return OrderState()

    @staticmethod
    def evolve(state: OrderState, event: DomainEvent) -> OrderState:
        match event:
            case OrderCreated(customer_id=customer_id, total=total):
                return state.model_copy(
                    update={"customer_id": customer_id, "total": total, "status": "created"}
                )
            case OrderShipped(tracking_number=tracking_number):
                return state.model_copy(
                    update={"tracking_number": tracking_number, "status": "shipped"}
                )
            case OrderCancelled():
                return state.model_copy(update={"status": "cancelled"})
            case _:
                return state
```

Read it as a fold: one `match` arm per event type, each returning a new state derived
from the old one plus the event's payload, and a fallthrough that returns the state
unchanged.

### `match`/`case`, not `isinstance`

Structural pattern matching does two things an `isinstance` chain cannot: it binds the
fields you need (`case OrderCreated(customer_id=customer_id, total=total):` both checks
the type *and* unpacks the payload in one line) and it makes "no case matched" a single
explicit `case _:` rather than an implicit fallthrough at the end of an `if/elif` chain.
Every event class this aggregate emits gets one `case`; the wildcard exists for events a
future version of the system might append that this class does not yet know about.

### `model_copy(update={...})` replaces the state, it doesn't mutate it

`state.model_copy(update={...})` builds a *new* `OrderState` and returns it; `state`
itself is untouched. This is the discipline Step 1 mentioned, and it is why freezing
`OrderState` costs you nothing: the pattern already never assigns to a field.

Two properties fall out of it. Folding an event is atomic -- either the whole new state
is built and returned, or nothing changes -- and any state object a caller captured
earlier (in a test, in a snapshot, in a log line) stays valid.

One sharp edge to know about: **`model_copy(update=...)` does not run validation.**
Pydantic copies the values in as-is:

```python
state = OrderState()
state.model_copy(update={"total": "not-a-number"}).total
# 'not-a-number' -- a str sitting in a Decimal field, no error
```

In practice this is fine, because the values you pass come off an event that pydantic
*did* validate when it was constructed. That is the real reason Step 2 told you to keep
event field types identical to state field types: `event.total` is already a `Decimal`,
so copying it into `total: Decimal` needs no conversion and no re-validation. If you ever
find yourself computing or coercing a value inside `evolve()`, construct the state
directly (`OrderState(...)`) instead, which does validate.

### `evolve()` must not decide anything

This is the rule that keeps event sourcing honest: **`evolve()` never rejects an event
and never raises.** By the time an event reaches the fold it is a historical fact -- it
either already happened in production, or `decide()` just decided it should. Refusing to
fold it in would make the aggregate's state disagree with its own history.

So no validation, no `raise`, no invariant checks. Those live entirely in `decide()`
(Step 6), which runs *before* an event exists. The division of labour is:

| | Decides whether it may happen | Updates state |
| --- | --- | --- |
| `decide()` (Step 6) | yes -- may raise | no |
| `evolve()` | no -- never raises | yes |

Two more things `evolve()` must not do, for the same reason it must be deterministic:

- **No I/O and no side effects.** No database writes, no HTTP calls, no email. `evolve()`
  runs again for every event on every single load of the aggregate; anything with an
  external effect would fire once per replay. Reacting to events belongs in a projection
  or subscriber, not in the fold.
- **No `datetime.now()`, no `uuid4()`.** If a timestamp matters, the event carries it --
  `event.occurred_at` is right there. Replaying the same history must always produce the
  same state, and a clock read inside the fold breaks that.

Because `evolve()` is a `@staticmethod` that only touches its two arguments, both of
these rules are easy to audit: there is no `self` to smuggle a side effect through.

With a fold in place, the aggregate can turn history into state. Next it needs a way to
decide what history to add.

## Step 6: Implement `decide()` -- the business-rule home

This is where the domain logic lives, and it is the one method allowed to say no. Add
it to the class, alongside `evolve()`:

```python
from eventsource import CommandRejectedError


class OrderAggregate(DeciderAggregate[OrderState, OrderCommand]):
    # ... aggregate_type, initial_state, evolve as above

    @staticmethod
    def decide(command: OrderCommand, state: OrderState) -> list[DomainEvent]:
        match command, state:
            case CreateOrder(
                order_id=order_id, customer_id=customer_id, total=total
            ), OrderState(status="pending"):
                return [
                    OrderCreated(
                        aggregate_id=order_id, customer_id=customer_id, total=total
                    )
                ]
            case CreateOrder(), _:
                raise CommandRejectedError("order already exists", command=command)
            case ShipOrder(order_id=order_id, tracking_number=tracking_number), OrderState(
                status="created"
            ):
                return [
                    OrderShipped(aggregate_id=order_id, tracking_number=tracking_number)
                ]
            case ShipOrder(), OrderState(status="cancelled"):
                raise CommandRejectedError("cannot ship a cancelled order", command=command)
            case ShipOrder(), _:
                raise CommandRejectedError("order is not ready to ship", command=command)
            case CancelOrder(), OrderState(status="cancelled"):
                raise CommandRejectedError("order is already cancelled", command=command)
            case CancelOrder(reason=reason), _:
                return [OrderCancelled(aggregate_id=order_id, reason=reason)]
            case _:
                raise CommandRejectedError(f"unknown command: {command!r}", command=command)
```

The class is now concrete. `initial_state`, `decide`, and `evolve` are all declared
`@staticmethod` and `@abstractmethod` on `DeciderAggregate`, so until this step
`OrderAggregate(uuid4())` would have raised:

```
TypeError: Can't instantiate abstract class OrderAggregate without an
implementation for abstract method 'decide'
```

At this point `initial_state` and `evolve` are already complete from the previous steps,
so only `decide` is outstanding. `DeciderAggregate` asks for one more abstract method
than the base `AggregateRoot` (which only requires `_apply()` and
`_get_initial_state()`), because the decide/evolve split is itself the contract.
With all three written, `OrderAggregate(uuid4())` builds cleanly.

### Matching on `(command, state)` together

Each `case` matches a *pair*: what was asked, and what is true right now. That pairing
is the whole reason `decide()` takes `state` as an argument instead of reading
`self.state` -- the function is pure, so every branch is a plain, testable fact about
"given this command and this state, what happens?" You can call
`OrderAggregate.decide(CreateOrder(...), OrderState())` directly in a
unit test, no aggregate instance, no event store, no repository.

The order of the `case` arms matters, as it does in any `match`: put the arm that should
succeed before the arms that reject the same command type for other reasons. `ShipOrder`
has three arms -- succeed when `status="created"`, a specific rejection message when the
order is cancelled, and a generic rejection for every other status -- so the error
message a caller sees is as precise as the arms you bothered to write.

### Rejecting with `CommandRejectedError`

`raise CommandRejectedError("cannot ship a cancelled order", command=command)` is the
convention this library ships for "the domain said no" -- not a requirement, just one
catchable type your application code can rely on instead of inventing its own per
aggregate. It takes a message and, optionally, the command that was rejected, which
`execute()` (Step 8) never sees because it does not catch the exception -- that is your
caller's job:

```python
order_id = uuid4()
order = OrderAggregate(order_id)
order.execute(CreateOrder(order_id=order_id, customer_id=uuid4(), total=Decimal("10.00")))
order.execute(ShipOrder(order_id=order_id, tracking_number="1Z999"))
try:
    order.execute(ShipOrder(order_id=order_id, tracking_number="1Z000"))
except CommandRejectedError as e:
    print(f"rejected: {e}")
    print(e.command)
```

Running that prints `rejected: order is not ready to ship` followed by the second
`ShipOrder` command's `repr` -- the order shipped once already, so the state's `status`
is `"shipped"`, matching the generic `ShipOrder(), _:` arm rather than the
`OrderState(status="created")` arm that succeeds.

### Rejection is atomic

The reason `decide()` is a separate function from `evolve()`, rather than one method
that both validates and mutates, is what happens when it raises: nothing. `execute()`
(Step 8) calls `decide()` to completion *before* touching the aggregate at all. If it
raises, no `evolve()` call ever happens, `self.version` does not advance, and
`uncommitted_events` gains nothing. A rejected command leaves the aggregate exactly as
it found it -- which is also why, as Step 3 noted, a rejected command is never persisted:
there is nothing to persist.

### `decide()` returns events, it does not apply them

Notice `decide()` never touches `self._state` or calls `evolve()` itself -- it returns a
plain `list[DomainEvent]`, and it is the aggregate's `execute()` method (Step 8) that
takes that list, stamps each event, and folds it in via `evolve()`. That separation is
what makes `decide()` safe to call in a test without an aggregate instance at all, and
it is what makes multi-event commands (a command that produces two or three events)
just as easy to write as single-event ones: return however many events belong together.

With the domain fully specified, the next step is watching it run.

## Step 7: Run it and watch `state`, `version`, and `uncommitted_events`

Add a small driver to the bottom of `first_aggregate.py`:

```python
from uuid import uuid4

order_id = uuid4()
order = OrderAggregate(order_id)
print(order.state)
print(order.version)
print(order.uncommitted_events)

order.execute(CreateOrder(order_id=order_id, customer_id=uuid4(), total=Decimal("42.00")))
print(order.state)
print(order.version)
print(len(order.uncommitted_events))

order.execute(ShipOrder(order_id=order_id, tracking_number="1Z999"))
print(order.state)
print(order.version)
```

Run the file. The first three lines show the aggregate before anything has happened:
`state` is the blank `OrderState` `initial_state()` built (status `"pending"`, not
`None`), `version` is `0`, and `uncommitted_events` is empty. After `execute(CreateOrder(...))`,
`state.status` is `"created"`, `version` is `1`, and there is one uncommitted event.
After `execute(ShipOrder(...))`, `state.status` is `"shipped"` and `version` is `2`.

`state` and `version` move together because `execute()` (which you will look inside in
Step 8) does the same three things every time: run `decide()`, stamp the resulting
events with the next version number, and apply each one -- which both advances
`self._version` and calls `evolve()`. `uncommitted_events` accumulates every stamped
event across every `execute()` call until something clears it -- typically the
repository, after persisting them.

## Step 8: What `execute()` does under the hood

You have been calling `execute()` since Step 6 without looking inside it. It is
inherited from `DeciderAggregate`, and it is worth reading once so "run `decide()`, stamp
the events, fold them in" stops being a black box:

```python
def execute(self, command: TCommand) -> list[DomainEvent]:
    events = self.decide(command, self.state)
    applied: list[DomainEvent] = []
    for event in events:
        stamped = self._stamp(event, command)
        self.apply_event(stamped, is_new=True)
        applied.append(stamped)
    return applied
```

Four things happen, in order: `decide()` runs to completion first (so a raised
`CommandRejectedError` never reaches the loop); each returned event is stamped (Step 9
covers exactly what that fills in); `apply_event(stamped, is_new=True)` -- the same
method every aggregate style shares -- sets `self._version` to the stamped
`aggregate_version`, calls `evolve()` to fold the event in, and appends it to
`uncommitted_events`; and the stamped events are returned, in case a caller wants them
(a test asserting on exactly what was produced, say).

`TCommand` in that signature is the second type parameter you supplied in Step 4, so on
`OrderAggregate` it reads `execute(self, command: OrderCommand)` -- that substitution is
what produces the type error shown in
[Why the second type parameter](#why-the-second-type-parameter).

You never call `evolve()` or touch `self._version` yourself. `execute()` is the one
front door, and it is the only method on `DeciderAggregate` that is not a `@staticmethod`
-- it needs `self` to read the current state, advance the version counter, and mutate
`uncommitted_events`.

## Step 9: What gets stamped onto each event

Look again at what `decide()` builds: `OrderCreated(aggregate_id=order_id,
customer_id=customer_id, total=total)` -- the `order_id` captured off the command. No `aggregate_version`, no `aggregate_type`, no
`causation_id`. Those are exactly the fields `execute()`'s stamping step (`_stamp()`)
fills in before the event is applied, and it is worth seeing what lands where. Add this
after the `ShipOrder` call from Step 7:

```python
created_event, shipped_event = order.uncommitted_events
print(shipped_event.causation_id is not None)
print(shipped_event.causation_id != created_event.causation_id)
```

Both print `True`. Every stamped event gets `aggregate_version` (from
`get_next_version()`) and `aggregate_type` (from the class attribute) if `decide()`
didn't already set them explicitly -- which is why Step 2's `aggregate_type: str =
"Order"` default and Step 4's `aggregate_type = "Order"` class attribute have to agree;
stamping only fills in what `model_fields_set` shows as *not* already set.

Because both `CreateOrder` and `ShipOrder` are `DomainCommand` instances, stamping goes
further: each event's `causation_id` becomes the command's `command_id`, so
`created_event.causation_id` points at the `CreateOrder` that produced it and
`shipped_event.causation_id` points at the separate `ShipOrder` -- two different
commands, two different `causation_id`s, which is exactly what the second `print` above
confirms. `correlation_id`, `actor_id`, and `tenant_id` are copied across the same way,
with `tenant_id` falling back to the ambient tenant context (see
[Multi-Tenancy](16-multi-tenancy.md)) when neither the command nor the event set it.

If `decide()` had built an event with `aggregate_version` already set -- useful in tests
that assert on an exact version number -- stamping would leave it alone; the check is
always "did `decide()` already decide this field," never "overwrite unconditionally."

This is the traceability ADR-0022 introduced `DomainCommand` for: every event can be
traced back to the command that caused it, and every command in a workflow can be traced
back to the event that triggered it, via `caused_by()` (Step 3). Neither the imperative
nor the declarative style (see the end of this tutorial) gets this for free from a plain
method call -- both need an explicit `command=` argument to `create_event()` to opt in.

## Step 10: Rebuild the order with `load_from_history()`

Everything so far has lived in one `OrderAggregate` instance. The payoff of event
sourcing is that the same history replayed into a *different* instance produces
identical state. Add this to the bottom of the file:

```python
history = order.uncommitted_events
replayed = OrderAggregate(order.aggregate_id)
replayed.load_from_history(history)
assert replayed.state == order.state
assert replayed.version == order.version
print("replay matches")
```

`load_from_history()` is unchanged from the base `AggregateRoot` -- it is not part of
the decider contract at all. It loops over the events you hand it and calls
`apply_event(event, is_new=False)` for each: version tracking happens, `evolve()` runs
just as it did live, but nothing is appended to `uncommitted_events` and version
validation is skipped, so a history starting at any version replays cleanly. The `is_new`
flag is the only thing that distinguishes "this event is new, queue it for persistence"
(`execute()`, Step 8) from "this event already happened, just fold it" (`load_from_history()`,
here).

`replayed` starts from `initial_state()` -- the same blank order
`order` itself started from -- and folds the identical two events through the identical
`evolve()`. There is exactly one place that turns events into state on this class, and
you wrote it once in Step 5; that is why the assertion holds.

## Other styles

`DeciderAggregate` is the style this library recommends and the one its own examples and
tests lead with, but it is not the only way to write an aggregate, and you will likely
run into the other two in an existing codebase.

**The imperative style** subclasses `AggregateRoot[TState]` directly and hand-writes an
`_apply()` method as an `isinstance` chain, with command methods that call
`create_event()` instead of returning events from a pure function. It has no `decide()`/
`evolve()` split -- validation and folding both happen inside command methods and
`_apply()` -- and `self.state` is `None` until the first event lands. See
`examples/imperative_example.py` for a complete `BankAccountAggregate` built this way.

**The declarative style** subclasses `DeclarativeAggregate[TState]` and replaces the
`isinstance` chain with one `@handles(EventType)`-decorated method per event type,
dispatched through a per-subclass registry instead of a hand-written chain. It shares
the imperative style's command-method-plus-`create_event()` shape, but scales better once
an aggregate handles half a dozen event types. See `examples/aggregate_example.py` for a
complete `ShoppingCartAggregate` built this way.

Both styles are still `AggregateRoot` subclasses underneath, so everything you learned
here about `aggregate_type`, `load_from_history()`, snapshots, and the repository applies
to them unchanged -- only how state gets mutated differs. For a full comparison,
including how to move an aggregate from one style to another, read
[Aggregate Styles](../explanation/aggregate-styles.md).

## Key Takeaways

- `DeciderAggregate[TState, TCommand]` needs three static methods: `initial_state()`,
  `decide(command, state)`, and `evolve(state, event)`. None of them touch `self`.
- `initial_state()` takes no arguments. The aggregate a command targets is named by the
  command, and `decide()` stamps that id onto the events it returns.
- `state` is never `None` on a `DeciderAggregate` -- `initial_state()` runs eagerly in
  `__init__`, so `decide()` and `evolve()` never need a null check.
- Commands are `DomainCommand` subclasses: frozen, never persisted, and structurally
  distinct from events because they might be refused.
- `decide()` is the only place allowed to raise. Raise `CommandRejectedError` for a
  refusal; a rejected command leaves the aggregate untouched and nothing is persisted.
- `evolve()` must be total and side-effect free: no raising, no I/O, no clocks -- just
  the next state.
- `execute(command)` runs `decide()`, stamps the resulting events (version, type, and --
  for `DomainCommand`s -- `causation_id`/`correlation_id`/`actor_id`/`tenant_id`), and
  folds each one in with `evolve()`.
- `load_from_history()` is shared by every aggregate style unchanged: replay the events,
  `is_new=False`, nothing queued for persistence.
- The imperative (`AggregateRoot` + `_apply()`) and declarative (`DeclarativeAggregate` +
  `@handles`) styles remain available for existing code and for aggregates that don't fit
  the decider shape; see [Aggregate Styles](../explanation/aggregate-styles.md).

## Next Steps

With a working aggregate, the next step is testing it properly. Continue to
[Testing Your Aggregates](08-testing.md), or read ahead to
[Building Projections](06-projections.md) to see these events drive a read model.
