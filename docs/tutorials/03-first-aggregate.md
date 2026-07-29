# Your First Aggregate

In this tutorial you will build a complete event-sourced aggregate from scratch: an
`Order` that can be created, shipped, and cancelled.

An *aggregate* is the consistency boundary of an event-sourced system. Instead of
storing the current row of an order in a table, you store the sequence of things that
happened to it -- `OrderCreated`, `OrderShipped` -- and rebuild the current state by
folding those events together. In `eventsource`, that folding lives in a subclass of
`AggregateRoot[TState]`, where `TState` is a Pydantic model holding the aggregate's
current state.

You will write the state model, the events, and the aggregate class; raise events from
command methods with `create_event()`; watch `state`, `version`, and
`uncommitted_events` change as you go; enforce a business rule before an event is
raised; rebuild the order from its history with `load_from_history()`; and finally see
the same aggregate rewritten in the declarative `@handles` style.

Everything here runs in plain Python -- no database, no Docker, no event store. You are
working with the aggregate in memory, which is exactly how aggregates are unit tested.
Persisting the events comes later.

By the end you will have a working `OrderAggregate` and a clear picture of the two
things every aggregate does: decide whether a command is allowed, and turn the events it
produces into new state.

## What you'll build

A single Python file containing an `OrderAggregate` and the pieces it needs:

- **`OrderState`** -- a Pydantic `BaseModel` holding the order's current status, its
  customer, its total, and its tracking number.
- **Three domain events** -- `OrderCreated`, `OrderShipped`, and `OrderCancelled`, each
  a `DomainEvent` subclass carrying only the fields that event needs.
- **`OrderAggregate(AggregateRoot[OrderState])`** -- with `aggregate_type = "Order"`,
  a `_get_initial_state()` that seeds a fresh order, an `_apply()` that folds each event
  type into a new `OrderState`, and three command methods (`create()`, `ship()`,
  `cancel()`) that call `create_event()`.

By Step 7 you will run the file and watch three things move together: `state` (the
folded `OrderState`), `version` (which advances to match each event's
`aggregate_version`), and `uncommitted_events` (the list the repository will later
persist).

The later steps sharpen it. Step 8 adds an invariant so `ship()` refuses to run on a
cancelled order and no event is raised. Step 9 attaches `metadata`, `actor_id`,
`causation_id`, and an explicit `tenant_id` to the events you emit. Step 10 throws away
the in-memory instance and rebuilds an identical order from its event list with
`load_from_history()` -- the moment event sourcing pays off.

Then two views of the same aggregate. The aside unpacks what `create_event()` does for
you by writing the equivalent `apply_event()` call by hand, so the convenience method
stops being magic. The variation rewrites `OrderAggregate` as a
`DeclarativeAggregate`, replacing the `isinstance` chain in `_apply()` with
`@handles(OrderCreated)`-decorated methods -- the style you will see in most real
codebases.

Here is the shape you are heading toward:

```python
class OrderAggregate(AggregateRoot[OrderState]):
    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState: ...
    def _apply(self, event: DomainEvent) -> None: ...

    def create(self, customer_id: UUID, total: Decimal) -> None: ...
    def ship(self, tracking_number: str) -> None: ...
    def cancel(self, reason: str) -> None: ...
```

Two methods to fold events into state, three methods to decide which events are allowed.
That is the whole aggregate.

## Prerequisites

Before you start you need:

- **Python 3.11 or newer** -- the package declares `requires-python = ">=3.11"`.
- **`eventsource-py` installed.** From a clone of the repository, `uv sync --all-extras`;
  otherwise `pip install eventsource-py`. Nothing in this tutorial needs an optional
  extra -- the core install (pydantic + sqlalchemy) is enough.
- **Comfort with pydantic v2 models.** Both `OrderState` and every event you write are
  `BaseModel` subclasses, and `DomainEvent` sets
  `model_config = ConfigDict(frozen=True)`, so you will update state with
  `model_copy(update={...})` rather than by assignment.
- **[Your First Event](02-first-event.md), or its equivalent.** This tutorial assumes you
  can already declare a `DomainEvent` subclass and know that events are immutable records
  of something that happened.

You do **not** need Docker, a database, or a message broker. An aggregate is plain
Python: it holds state in memory, folds events into it, and collects the events it
raised in `uncommitted_events` for someone else to persist later. Every snippet below can
be pasted into a single file and run top to bottom with `python`, and none of the code is
async -- `AggregateRoot`'s methods are all synchronous.

Check your install before you begin:

```bash
python -c "import eventsource; print(eventsource.__version__)"
```

If that prints a version, you are ready.

## Step 1: Model the state (a Pydantic `BaseModel`)

Start with the answer to a single question: *what does the code need to know about an
order right now?* Not the history -- the history is the events. Just the current facts a
command method will read before it decides whether to allow something.

For our order that is four things: who the customer is, how much it is for, what status
it is in, and (once shipped) the tracking number.

Create a file called `first_aggregate.py` and start it like this:

```python
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel


class OrderState(BaseModel):
    order_id: UUID
    customer_id: UUID | None = None
    total: Decimal = Decimal("0")
    status: str = "pending"
    tracking_number: str | None = None
```

That is the whole state model. A few things about it are deliberate.

**It is a Pydantic `BaseModel`, and that is a hard requirement.** `AggregateRoot` is
declared as `AggregateRoot(Generic[TState], ABC)` where `TState = TypeVar("TState",
bound=BaseModel)`. Anything you plug in as `TState` must be a `BaseModel` subclass. A
dataclass or a plain dict will not type check, and the snapshot machinery would break on
it: `_serialize_state()` calls `self._state.model_dump(mode="json")`, and
`_restore_from_snapshot()` calls `state_type.model_validate(state_dict)`. Those two
methods only exist because the state is a pydantic model.

**Every field except `order_id` has a default.** In Step 4 you will write
`_get_initial_state()`, which has to construct an `OrderState` *before* any event has
been applied -- a blank order that exists only so `_apply()` has something to fold the
first event into. Defaults are what make that one-line constructor possible. Fields that
are unknown until an event arrives (`customer_id`, `tracking_number`) are typed as
optional; fields with a sensible zero value (`total`, `status`) get one.

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
buys you a typo-proof invariant check in Step 8.

**`total` is a `Decimal`, not a `float`.** Money in a `float` accumulates rounding error,
and this value will be reconstructed from JSON on every replay. Pydantic handles
`Decimal` round-tripping for you.

### State is not the event, and it is not the database row

Two habits from other architectures are worth unlearning right now.

*State is not a mirror of the events.* `OrderCreated` will carry `customer_id` and
`total`; `OrderShipped` will carry `tracking_number`. `OrderState` holds all of those
together, flattened, with no notion of which event put them there. If you find yourself
adding a `last_event_type` field, that is the version counter's job, not the state's.

*State is not the read model.* Resist putting anything in `OrderState` that no command
method will read. Display names, formatted currency strings, denormalized customer
addresses -- those belong in a projection (see
[Projections](06-projections.md)). The aggregate's state exists to answer the question
"is this command allowed?", and it is loaded and rebuilt on every single command, so
keep it small.

A useful test: for each field, name the command method that reads it. `status` is read
by `ship()` and `cancel()` to reject invalid transitions. `order_id` identifies the
aggregate. `customer_id` and `total` are there because a real system would check them
(a refund limit, an ownership check) and because they make the folded state visible when
you print it in Step 7. `tracking_number` records the shipment. Nothing else earns a
slot.

### Should the state be frozen?

`DomainEvent` sets `model_config = ConfigDict(frozen=True)` because an event is a record
of something that already happened and must never change. `OrderState` is different --
it is a value the aggregate replaces on every applied event, so freezing it is optional.

Leaving it mutable is fine as long as you keep the discipline of building a *new* state
object in `_apply()` rather than mutating fields in place, which is what the
`model_copy(update={...})` pattern in Step 5 does. If you would rather have pydantic
enforce that discipline for you, add the config:

```python
from pydantic import BaseModel, ConfigDict


class OrderState(BaseModel):
    model_config = ConfigDict(frozen=True)

    order_id: UUID
    # ... as above
```

With `frozen=True`, `self._state.status = "shipped"` raises a
`ValidationError` instead of silently succeeding, and `model_copy(update={...})` remains
the only way forward. The rest of this tutorial works identically either way; the
snippets use the unfrozen version and never mutate in place.

With the state modelled, the next step is the other half of the pair: the events that
move it from one `OrderState` to the next.

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
| `aggregate_id` | **required** | filled by `create_event()` in Step 6 |
| `aggregate_type` | **required** | we default it to `"Order"` above |
| `aggregate_version` | `1` | filled by `create_event()` in Step 6 |
| `tenant_id`, `actor_id`, `causation_id` | `None` | see Step 9 |
| `correlation_id` | `uuid4()` | links events across aggregates |
| `metadata` | `{}` | free-form dict |

Two of those are declared with `...` (required) on the base class: `aggregate_id` and
`aggregate_type`. Giving `aggregate_type` a default of `"Order"` on each event class
means you can construct one in a test without repeating it, and it documents which
aggregate the event belongs to right at the class definition. `aggregate_id` stays
required -- there is no sensible default for "which order" -- and Step 6 shows
`create_event()` supplying it from the aggregate instance.

### `event_type` derives itself from the class name

You do not write `event_type = "OrderCreated"`. `DomainEvent.__init_subclass__` sets the
field default to the class name at class-definition time, and a `model_validator(mode="before")`
does the same when an event is built from a dict (`model_validate`, `from_dict`). So:

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

This is the one place the library forces immutability on you, and it is the reason
`_apply()` in Step 5 builds a new `OrderState` instead of editing the event.

### Choosing the payload

Each event answers "what changed?", and nothing more:

- `OrderCreated` introduces the customer and the total -- the two facts that exist only
  once the order does.
- `OrderShipped` introduces the tracking number. It does **not** repeat `customer_id`;
  the fold in Step 5 already has that from `OrderCreated`.
- `OrderCancelled` carries a `reason`, because "why" is a fact worth keeping and there is
  nowhere else to keep it.

Notice what is absent: no `status` field. The status is *derived* -- `_apply()` sets it
to `"shipped"` when it sees an `OrderShipped`. Putting a `status` field on the event
would let the two disagree. The same rule rules out `new_total`-style fields that restate
the whole aggregate: store the delta or the new fact, and let the fold compute the rest.

Also notice the names are past tense. `OrderShipped`, not `ShipOrder`. `ShipOrder` is a
command -- a request that might be refused (Step 8 refuses one). An event is what
remains after the refusal could no longer happen.

Keep the payload types the same as the state types -- `total: Decimal` here matches
`total: Decimal` on `OrderState`, so no conversion happens in `_apply()`, and the value
round-trips through JSON with pydantic's `Decimal` handling intact.

### A note on the registry

Nothing above registers these classes anywhere. Auto-derivation of `event_type` happens
at subclass creation, but adding the class to the global `EventRegistry` is explicit --
you apply the `@register_event` decorator, or call `registry.register(OrderCreated)`.
You need that only when something has to turn a stored `event_type` string back into a
class, which is a concern for the event store, not for the in-memory aggregate you are
building here. Registering a second class under a name already taken raises
`DuplicateEventTypeError`, so it is a deliberate step rather than a silent one.

With state and events defined, you have both halves of the fold. Next you connect them
with an aggregate class.

## Step 3: Subclass `AggregateRoot[OrderState]` and set `aggregate_type`

Now the class itself. Add this to `first_aggregate.py`:

```python
from eventsource import AggregateRoot


class OrderAggregate(AggregateRoot[OrderState]):
    aggregate_type = "Order"
```

Two lines, and both of them carry weight.

### The `[OrderState]` parameter is not decoration

`AggregateRoot` is declared `class AggregateRoot(Generic[TState], ABC)`. Writing
`AggregateRoot[OrderState]` tells the type checker that `self.state` is an
`OrderState | None`, so `self.state.status` in Step 8 is checked rather than guessed at.

It also does real work at runtime. `_get_state_type()` walks the class's MRO looking at
`__orig_bases__` for a parameterized base whose origin is an `AggregateRoot` subclass,
and returns the first type argument:

```python
aggregate = OrderAggregate(uuid4())
assert aggregate._get_state_type() is OrderState
```

That lookup is how snapshots get rehydrated -- `_restore_from_snapshot()` calls
`state_type.model_validate(state_dict)` with whatever `_get_state_type()` returned. If
you drop the parameter and write `class OrderAggregate(AggregateRoot):`, everything in
this tutorial still runs, but the moment a snapshot is restored you get:

```
RuntimeError: Cannot determine state type for OrderAggregate.
Ensure the class properly inherits from AggregateRoot[StateType].
```

Always write the parameter.

### `aggregate_type` is the stream's name

`AggregateRoot` defines `aggregate_type: str = "Unknown"` as a class attribute, and you
are expected to override it. It is a plain string on the class -- not a property, not
derived from the class name -- so `OrderAggregate` becomes `"Order"` only because you
said so.

That string ends up in three places:

1. **On every event you raise.** `create_event()` builds its kwargs starting with
   `{"aggregate_id": self.aggregate_id, "aggregate_type": self.aggregate_type,
   "aggregate_version": self.get_next_version()}`. In Step 2 you also defaulted
   `aggregate_type: str = "Order"` on each event class; `create_event()` passes the
   aggregate's value explicitly, so the two must agree. Keep them in sync, or the events
   your commands raise will be stamped differently from the ones you build by hand in
   tests.
2. **In the event store's stream identity.** Events are read back with
   `get_events(aggregate_id, aggregate_type)`, and `EventStream` carries an
   `aggregate_type` field. The pair `(aggregate_id, aggregate_type)` is the stream.
3. **In the repository.** `AggregateRepository` infers its type from the class:
   `_infer_aggregate_type()` reads `factory.aggregate_type` and accepts it only if it is
   a non-empty string that is not `"Unknown"`. Leave the default in place and
   constructing the repository raises `ValueError` with instructions to either set the
   class attribute or pass `aggregate_type=` explicitly.

Because it is baked into stored events, **treat `aggregate_type` as permanent**. Renaming
the Python class from `OrderAggregate` to `PurchaseOrderAggregate` is free; changing the
string from `"Order"` to `"PurchaseOrder"` orphans every event already written under the
old name. Pick a short domain noun -- `"Order"`, not `"OrderAggregate"` and not
`"orders"` -- and leave it alone.

### The class does not work yet

Paste the two lines above into a REPL and try to build one:

```python
OrderAggregate(uuid4())
# TypeError: Can't instantiate abstract class OrderAggregate without an
# implementation for abstract methods '_apply', '_get_initial_state'
```

That is the point of the ABC. `_apply()` and `_get_initial_state()` are decorated
`@abstractmethod`, so Python refuses to instantiate until you supply both -- the next two
steps. The constructor you are calling is `AggregateRoot.__init__(self, aggregate_id:
UUID)`: one argument, the identity. It sets `self._version = 0`, `self._state = None`,
and an empty `self._uncommitted_events` list. Nothing else. An aggregate is born empty
and only becomes something when events are applied to it.

Do not write your own `__init__` to accept `customer_id` or a `total`. Creation is an
event (`OrderCreated`, raised by the `create()` command in Step 6), not a constructor
argument. The one-argument constructor is what lets the repository build an instance
from nothing but an ID before replaying history into it.

### Two more class attributes worth knowing about

`AggregateRoot` declares two other overridable class-level settings. You will not change
either in this tutorial, but they are visible right next to `aggregate_type` in the
source and it is worth knowing what they are for:

```python
class OrderAggregate(AggregateRoot[OrderState]):
    aggregate_type = "Order"
    schema_version = 1        # default; bump when OrderState changes incompatibly
    validate_versions = True  # default; see the aside after Step 10
```

`schema_version` guards snapshots: if a stored snapshot's schema version does not match
the class's, the snapshot is discarded and the aggregate is rebuilt from the full event
history. Bump it when you change `OrderState` in a way old snapshots cannot satisfy.

`validate_versions` controls whether `apply_event()` enforces that a new event's
`aggregate_version` equals `self.version + 1`. With the default `True` a mismatch raises
`EventVersionError`; with `False` it only logs a warning. Since `create_event()` computes
the version for you, you will not trip this -- the aside after Step 10 shows the case
where it matters.

With the class declared and named, the aggregate needs a starting state.

## Step 4: Implement `_get_initial_state()`

The first of the two abstract methods. Add it to the class body:

```python
class OrderAggregate(AggregateRoot[OrderState]):
    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)
```

One line. This is where the defaults you gave `OrderState` in Step 1 pay off: every
field except `order_id` has one, and `order_id` comes from `self.aggregate_id`, which
`AggregateRoot.__init__` has already stored. There is nothing else to supply, because a
blank order knows nothing yet -- no customer, no total, status `"pending"`.

### A fresh aggregate's state is `None`, not the initial state

This is the part that surprises people. `__init__` sets `self._state = None` and stops.
It does **not** call `_get_initial_state()`. Try it:

```python
order = OrderAggregate(uuid4())
print(order.state)               # None
print(order.version)             # 0
print(order.uncommitted_events)  # []
```

An aggregate that has had no events applied has no state, and `state` returning `None` is
how you tell "this order does not exist yet" from "this order exists and is pending".
If `__init__` eagerly built an `OrderState`, a brand-new instance would be
indistinguishable from a real order that had been created and left alone -- and the
repository, which constructs an instance from nothing but an ID before replaying
history, would have no way to report that the stream was empty.

`_get_initial_state()` is a *factory you call*, not a hook the base class calls for you.
Search the source and you will find `AggregateRoot` never invokes it: not in `__init__`,
not in `apply_event()`, not in `load_from_history()`. The only caller is your own
`_apply()`.

### Which is why `_apply()` calls it

That makes the seeding step explicit, and Step 5 will open with it:

```python
    def _apply(self, event: DomainEvent) -> None:
        if self._state is None:
            self._state = self._get_initial_state()
        # ... fold the event into self._state
```

Two lines at the top of the fold, and from there on `self._state` is guaranteed
non-`None`, so `self._state.model_copy(update={...})` is safe for every event type.
Without them, the first event applied to a new aggregate hits
`AttributeError: 'NoneType' object has no attribute 'model_copy'`.

You will sometimes see the seeding done per-branch instead -- the library's own test
aggregates do this, calling `self._get_initial_state()` inside each `isinstance` arm.
Same effect, more repetition. Guarding once at the top is the version this tutorial
uses.

There is a second, less obvious use for the method: a *reset*. An event that returns the
aggregate to its blank state is exactly `self._state = self._get_initial_state()`, with
no `model_copy` at all. That is why the method exists as a named factory rather than an
inline `OrderState(order_id=self.aggregate_id)` buried in the fold.

### Return type and the `TState | None` in the base class

The abstract signature is `def _get_initial_state(self) -> TState | None`. Yours narrows
that to `-> OrderState`, which is correct and what you want -- narrowing a return type in
an override is allowed, and it keeps `self._state` typed as `OrderState` after the guard.

The `| None` in the base signature exists for one specific case:
`DeclarativeAggregate` subclasses that set `requires_creation_event = True`. Those
aggregates have no meaningful blank state -- an extraction process, say, that only exists
once it has been requested -- so their inherited `_get_initial_state()` returns `None`
and the first event handler assigns the state outright. Accessing `.state` before that
raises `AggregateNotCreatedError` rather than handing back a half-built object.
`DeclarativeAggregate` also gives those aggregates `state_or_none` and `is_created` to
check existence without the exception.

You are not in that case. `OrderAggregate` extends `AggregateRoot` directly, where
`_get_initial_state()` is a bare `@abstractmethod` -- Python requires you to implement it,
and returning a real `OrderState` is the straightforward thing to do. (For reference:
`DeclarativeAggregate` with the default `requires_creation_event = False` raises
`NotImplementedError` at call time if you forget, rather than blocking instantiation.)

### Keep it cheap and deterministic

`_get_initial_state()` may be called on every command, on every replay, and once per
reset event. Two rules follow:

- **No I/O.** No database reads, no clock lookups that end up in state, no random values.
  The initial state must be identical every time it is built for a given
  `aggregate_id`, or replaying the same history twice would produce two different
  aggregates.
- **Derive nothing but identity.** `self.aggregate_id` is the only thing available and the
  only thing you should use. Everything else arrives as an event.

If you catch yourself wanting a constructor argument here -- a tenant, a currency, a
customer -- that is a fact about the order, and facts arrive in `OrderCreated`.

With a starting state defined, the aggregate has somewhere to fold events into.

## Step 5: Implement `_apply()` to fold events into state

The second abstract method, and the heart of the aggregate. `_apply()` takes one event
and produces the next `OrderState`. Add it to the class:

```python
class OrderAggregate(AggregateRoot[OrderState]):
    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if self._state is None:
            self._state = self._get_initial_state()

        if isinstance(event, OrderCreated):
            self._state = self._state.model_copy(
                update={
                    "customer_id": event.customer_id,
                    "total": event.total,
                    "status": "created",
                }
            )
        elif isinstance(event, OrderShipped):
            self._state = self._state.model_copy(
                update={
                    "tracking_number": event.tracking_number,
                    "status": "shipped",
                }
            )
        elif isinstance(event, OrderCancelled):
            self._state = self._state.model_copy(update={"status": "cancelled"})
```

The class is now concrete -- both abstract methods are implemented, so
`OrderAggregate(uuid4())` no longer raises `TypeError`.

Read the method as a fold: one guard to make sure there is something to fold into, then
one branch per event type, each returning a new state derived from the old one plus the
event's payload.

### The guard, then one branch per event

The first two lines are the seeding you met in Step 4. After them, `self._state` is
non-`None` for the rest of the method, so every branch can safely call
`self._state.model_copy(...)`.

Each branch does exactly two things: copy the fields the event introduces, and set the
derived `status`. Notice where `status` comes from. No event carries it -- Step 2
deliberately left it off the payloads -- so `_apply()` is the single place that decides
`OrderShipped` means `status == "shipped"`. That is what "derived state" means: the
events are the facts, the status is a conclusion the fold draws from them.

Notice also what each branch *doesn't* touch. The `OrderShipped` branch never mentions
`customer_id` or `total`; those were folded in by `OrderCreated` and `model_copy` carries
every un-updated field through unchanged. Only write the fields this event changes.

### `model_copy(update={...})` replaces the state, it doesn't mutate it

`self._state = self._state.model_copy(update={...})` builds a *new* `OrderState` and
rebinds the attribute. The old state object is untouched. This is the discipline Step 1
mentioned, and it is why freezing `OrderState` costs you nothing: the pattern already
never assigns to a field.

Two properties fall out of it. Applying an event is atomic -- either the whole new state
is built and assigned, or the old one remains -- and any state object you captured
earlier (in a test, in a snapshot, in a log line) stays valid.

One sharp edge to know about: **`model_copy(update=...)` does not run validation.**
Pydantic copies the values in as-is:

```python
state = OrderState(order_id=order_id)
state.model_copy(update={"total": "not-a-number"}).total
# 'not-a-number' -- a str sitting in a Decimal field, no error
```

In practice this is fine, because the values you pass come off an event that pydantic
*did* validate when it was constructed. That is the real reason Step 2 told you to keep
event field types identical to state field types: `event.total` is already a `Decimal`,
so copying it into `total: Decimal` needs no conversion and no re-validation. If you ever
find yourself computing or coercing a value inside `_apply()`, construct the state
directly (`OrderState(...)`) instead, which does validate.

### `_apply()` must not decide anything

This is the rule that keeps event sourcing honest: **`_apply()` never rejects an event
and never raises.** By the time an event reaches the fold it is a historical fact -- it
either already happened in production, or your command method just decided it should.
Refusing to fold it in would make the aggregate's state disagree with its own history.

So no validation, no `raise`, no invariant checks. Those live in the command methods
(Step 6) and the guard clauses (Step 8), which run *before* an event exists. The division
of labour is:

| | Decides whether it may happen | Updates state |
| --- | --- | --- |
| `ship()` (Step 6, 8) | yes -- may raise | no |
| `_apply()` | no -- never raises | yes |

Two more things `_apply()` must not do, for the same reason it must be deterministic:

- **No I/O and no side effects.** No database writes, no HTTP calls, no email. `_apply()`
  runs again for every event on every single load of the aggregate; anything with an
  external effect would fire once per replay. Reacting to events belongs in a projection
  or subscriber, not in the fold.
- **No `datetime.now()`, no `uuid4()`.** If a timestamp matters, the event carries it --
  `event.occurred_at` is right there. Replaying the same history must always produce the
  same state, and a clock read inside the fold breaks that.

### Unrecognized events fall through silently

There is no `else: raise` on the chain, and that is deliberate. If a stored history
contains an event type this class doesn't handle, `_apply()` simply leaves the state
alone -- but the version still advances, because `apply_event()` sets
`self._version = event.aggregate_version` *before* calling `_apply()`:

```python
order.apply_event(SomeUnhandledEvent(aggregate_id=order_id, aggregate_version=3))
order.state    # unchanged
order.version  # 3
```

That tolerance is what lets you deploy a new event type before every consumer knows about
it, and lets old code replay newer histories without crashing. The cost is that a typo in
an `isinstance` check fails quietly -- your state just never updates. Step 7 exists partly
so you catch that immediately by printing the state after each command, and the
`@handles` variation at the end of the tutorial removes the failure mode entirely by
registering handlers by type rather than testing them by hand.

### Where `_apply()` gets called from

You never call `_apply()` yourself. It has exactly one caller in the library:
`apply_event()`, which does four things in order.

1. If `is_new=True`, check `event.aggregate_version == self.version + 1` and raise
   `EventVersionError` if not (Step 3's `validate_versions`).
2. Set `self._version = event.aggregate_version`.
3. Call `self._apply(event)`.
4. If `is_new=True`, append the event to `self._uncommitted_events`.

Both paths into the aggregate go through it. `create_event()` in Step 6 builds an event
and calls `apply_event(event, is_new=True)` -- state updates *and* the event is queued for
persistence. `load_from_history()` in Step 10 loops over stored events calling
`apply_event(event, is_new=False)` -- state updates, version tracks, nothing is queued,
and version validation is skipped so a history starting at any version replays cleanly.

Same fold, both times. That single shared path is why a rebuilt aggregate is guaranteed
identical to the live one: there is only one piece of code that turns events into state,
and you just wrote it.

### The whole file so far

```python
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel

from eventsource import AggregateRoot, DomainEvent


class OrderState(BaseModel):
    order_id: UUID
    customer_id: UUID | None = None
    total: Decimal = Decimal("0")
    status: str = "pending"
    tracking_number: str | None = None


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


class OrderAggregate(AggregateRoot[OrderState]):
    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if self._state is None:
            self._state = self._get_initial_state()

        if isinstance(event, OrderCreated):
            self._state = self._state.model_copy(
                update={
                    "customer_id": event.customer_id,
                    "total": event.total,
                    "status": "created",
                }
            )
        elif isinstance(event, OrderShipped):
            self._state = self._state.model_copy(
                update={
                    "tracking_number": event.tracking_number,
                    "status": "shipped",
                }
            )
        elif isinstance(event, OrderCancelled):
            self._state = self._state.model_copy(update={"status": "cancelled"})
```

You can instantiate this and it will work -- but it has no commands yet, so the only way
to get an event into it is to build one by hand and call `apply_event()`. The next step
gives it a proper front door.
