# Tutorial 2: Your First Domain Event

In this tutorial you will define a domain event, create instances of it, see why it
cannot be changed after creation, register it so it can be read back from storage, and
link several events together into a traceable chain. Everything here runs in a plain
Python script -- no database, no message broker.

## Prerequisites

Before you start you need:

- **Python 3.11 or newer** -- the package declares `requires-python = ">=3.11"`.
- **`eventsource-py` installed.** From a clone of the repository, `uv sync --all-extras`;
  otherwise `pip install eventsource-py`. Nothing in this tutorial needs an optional
  extra -- the core install (pydantic + sqlalchemy) is enough.
- **A general idea of what event sourcing is.** If you have worked through the earlier
  material in the tutorial series, you are set; if not, all you need to know is that an
  event-sourced system stores a history of things that happened instead of a single
  mutable row.

You do **not** need Docker, a database, or a message broker. Everything below runs
in-process, and every snippet can be pasted into one file and run top to bottom:

```bash
python -c "import eventsource; print(eventsource.__version__)"
```

If that prints a version, you are ready.

## Learning Objectives

By the end of this tutorial you will be able to:

- Subclass `DomainEvent` and add your own payload fields
- Explain what each inherited metadata field is for, and which ones you must supply
- Predict what `event_type` will be for any event class you write
- Observe that events are frozen, and create modified copies with `with_metadata`,
  `with_causation`, and `with_aggregate_version`
- Register events with `register_event` so a stored type name can be resolved back to a
  class with `get_event_class`
- Model a sequence of state transitions as separate event types
- Link events into a traceable chain using `correlation_id` and `causation_id`, and
  check the links with `is_caused_by` and `is_correlated_with`
- Round-trip an event through `to_dict()` and `from_dict()`
- Rely on Pydantic validation to reject bad payloads before they reach the log
- Name events the way the rest of the ecosystem expects, and override `event_type`
  safely (with `suppress_event_type_warning`) for legacy stored names

## What is a Domain Event?

A domain event is a record of something that *already happened* in your business
domain. "The order was placed." "The payment was captured." "The order shipped."

Three properties make events different from ordinary data objects:

1. **They are facts about the past.** You cannot un-happen them, so you never edit
   them -- you append a new event that corrects or supersedes them.
2. **They are self-contained.** An event carries everything a reader needs to
   understand the change, so a consumer written years later can still interpret it.
3. **They are the source of truth.** Current state is derived by replaying events, not
   the other way around.

It helps to contrast an event with the two things it is most often confused with:

| | Example | Tense | Can it be rejected? |
| --- | --- | --- | --- |
| Command | `PlaceOrder` | Imperative | Yes -- it is a request |
| Event | `OrderPlaced` | Past | No -- it already happened |
| State | `Order(status="placed")` | Present | It is derived, not recorded |

A command is a request that may fail validation. An event is what you write down once
the request succeeded. State is what you compute by replaying the events you wrote.

In this library that distinction is enforced by the type itself. `DomainEvent` is a
frozen Pydantic model (`model_config = ConfigDict(frozen=True)`), so an event genuinely
cannot be mutated after construction -- assigning to a field raises a `ValidationError`.
Every event also carries the identity of the thing it happened to: `aggregate_id` and
`aggregate_type` are the only two fields with no default, so you cannot accidentally
record a fact that belongs to nothing.

Everything else in this library -- aggregates, projections, stores, buses -- exists to
move events around and fold them into state. So events are the first thing to get right.

## The DomainEvent Base Class

All events subclass `DomainEvent`, which lives in `eventsource.domain.event` and is
re-exported from the package root:

```python
from eventsource import DomainEvent
```

`DomainEvent` is a Pydantic v2 `BaseModel` configured with
`model_config = ConfigDict(frozen=True)`. That gives you validation, JSON
serialization, and immutability for free. Subclassing it means you only write the
fields specific to *your* event; the metadata fields below come with the base class.

### Event Metadata Fields

| Field | Type | Default | Purpose |
| --- | --- | --- | --- |
| `event_id` | `UUID` | new `uuid4()` | Unique identity of this event instance |
| `event_type` | `str` | class name (see below) | Name used to look the class back up |
| `event_version` | `int` | `1` (must be >= 1) | Schema version of this event type |
| `occurred_at` | `datetime` | now, UTC | When the event happened |
| `aggregate_id` | `UUID` | **required** | Which aggregate instance it belongs to |
| `aggregate_type` | `str` | **required** | Which kind of aggregate, e.g. `"Order"` |
| `aggregate_version` | `int` | `1` (must be >= 1) | Aggregate version after this event |
| `tenant_id` | `UUID \| None` | `None` | Multi-tenancy scope, if you use it |
| `actor_id` | `str \| None` | `None` | User or system that triggered the event |
| `correlation_id` | `UUID` | new `uuid4()` | Groups events from one logical operation |
| `causation_id` | `UUID \| None` | `None` | The `event_id` of the event that caused this one |
| `metadata` | `dict[str, Any]` | `{}` | Free-form extras (trace IDs, source, ...) |

Only `aggregate_id` and `aggregate_type` are genuinely required at construction time,
and event classes conventionally pin `aggregate_type` as a class-level default so
callers never have to pass it.

The fields group into four jobs: identifying the event (`event_id`, `event_type`,
`event_version`, `occurred_at`), locating it on an aggregate (`aggregate_id`,
`aggregate_type`, `aggregate_version`), attributing it (`tenant_id`, `actor_id`), and
tracing it (`correlation_id`, `causation_id`, `metadata`). You will meet the tracing
fields properly later in this tutorial.

One more class-level knob exists but is *not* a field:
`suppress_event_type_warning` is a `ClassVar[bool]` defaulting to `False`. Because it is
a `ClassVar`, Pydantic ignores it -- it never appears in the model's fields or in
`to_dict()` output.

### Where event_type Comes From

`DomainEvent.__init_subclass__` runs when you define a subclass. If you did not declare
`event_type` yourself, it sets the field default to your class name. So:

```python
class OrderPlaced(DomainEvent):
    aggregate_type: str = "Order"

# -> event_type == "OrderPlaced"
```

A `model_validator(mode="before")` named `_ensure_event_type` covers the other path:
when you build an event from a dictionary whose `event_type` is missing or empty, the
class name is filled in before validation. That means `model_validate`, `from_dict`, and
`OrderPlaced(**data)` all behave identically:

```python
data = {"aggregate_id": order_id}          # no event_type key
print(OrderPlaced.model_validate(data).event_type)  # 'OrderPlaced'
```

Note that "empty" means empty. An explicitly supplied `event_type=""` is replaced with
the class name -- even on a class that pinned its own default -- while any non-empty
value you pass through is kept as-is.

Subclassing is per-class, not inherited: `__init_subclass__` only looks at the class's
*own* `__dict__`, so a subclass of `OrderPlaced` that does not declare `event_type` gets
its own class name, not `"OrderPlaced"`.

You do not need to write `event_type = "OrderPlaced"`. The class name *is* the contract.

If you *do* declare an `event_type` that differs from the class name,
`__init_subclass__` keeps your value and logs a warning at class-definition time. That
is intentional -- see the compatibility section near the end of this tutorial for when
overriding is legitimate and how to silence the warning.

## Creating Your First Event

Create `first_event.py`:

```python
from uuid import UUID, uuid4

from eventsource import DomainEvent


class OrderPlaced(DomainEvent):
    """An order was placed by a customer."""

    aggregate_type: str = "Order"

    order_number: str
    customer_id: UUID
    total: float


order_id = uuid4()

event = OrderPlaced(
    aggregate_id=order_id,
    order_number="ORD-001",
    customer_id=uuid4(),
    total=99.95,
)
```

That is the whole definition. Three payload fields, one pinned `aggregate_type`, and
Pydantic handles the rest. Three things are worth pointing out.

**The docstring is not decoration.** It is where you record what the event means to the
business. Stored events outlive the code that wrote them, and this is the only place a
future reader can find out what "placed" meant.

**Pinning `aggregate_type` is what makes construction pleasant.** `aggregate_type` has
no default on `DomainEvent`, so without the class-level `= "Order"` every caller would
have to pass it on every construction. Pin it once and `aggregate_id` becomes the only
required argument left:

```python
try:
    OrderPlaced(order_number="ORD-001", customer_id=uuid4(), total=99.95)
except Exception as exc:
    print(exc)   # 1 validation error ... aggregate_id: Field required
```

**You did not write `event_type`.** `__init_subclass__` already set its default to
`"OrderPlaced"` when the class body finished executing. Confirm it, along with the
other defaults you inherited:

```python
print(event.event_type)          # 'OrderPlaced'
print(event.aggregate_type)      # 'Order'
print(event.event_version)       # 1
print(event.aggregate_version)   # 1
```

Your own fields sit alongside the inherited ones in a single flat model -- there is no
nested `payload` dict to unwrap:

```python
print(sorted(set(OrderPlaced.model_fields) - set(DomainEvent.model_fields)))
# ['customer_id', 'order_number', 'total']
```

Because the annotations are real Pydantic fields, they are also coerced and checked at
construction. Passing `total="12.5"` yields the float `12.5`; passing `total="free"`
raises a `ValidationError` before the object exists. The next sections put that
behavior to work.

## Using Your Event

You now have an instance. Read from it the same way you would any Pydantic model --
attribute access, no getters, no unwrapping:

```python
print(event.order_number)        # 'ORD-001'
print(event.total)               # 99.95
print(event.aggregate_id)        # the UUID you passed in
```

The interesting part is everything you *did not* pass. Every remaining field was filled
in for you:

```python
print(event.event_type)          # 'OrderPlaced'
print(event.event_version)       # 1
print(event.aggregate_version)   # 1
print(event.occurred_at)         # 2026-07-28 04:33:15.327695+00:00
print(event.occurred_at.tzinfo)  # UTC
print(event.correlation_id)      # a fresh UUID
print(event.causation_id)        # None
print(event.actor_id)            # None
print(event.tenant_id)           # None
print(event.metadata)            # {}
```

Two of those defaults deserve a note now, because they surprise people later.

`occurred_at` is always timezone-aware UTC -- the default factory is
`datetime.now(UTC)`, never a naive local timestamp. And `correlation_id` defaults to a
*new* UUID per event, not to a shared one. Two events you construct independently are
therefore uncorrelated:

```python
other = OrderPlaced(
    aggregate_id=order_id,
    order_number="ORD-001",
    customer_id=uuid4(),
    total=99.95,
)

print(event.correlation_id == other.correlation_id)  # False
print(event.is_correlated_with(other))               # False
print(event.is_caused_by(other))                     # False
```

Linking events into a chain is something you do deliberately, later in this tutorial.

### Printing Events

`DomainEvent` defines both `__str__` and `__repr__`, and they differ on purpose.
`print()` uses the compact form -- the event type plus the three identifiers you usually
need while scanning a log:

```python
print(event)
# OrderPlaced(event_id=d702a2e4-..., aggregate_id=97d5b03a-..., version=1)
```

`repr()` is the diagnostic form, adding `event_type`, `aggregate_type`, `tenant_id`, and
`occurred_at`. It is what shows up in test failure output and in a debugger:

```python
print(repr(event))
# OrderPlaced(event_id=UUID('d702a2e4-...'), event_type='OrderPlaced',
#  aggregate_id=UUID('97d5b03a-...'), aggregate_type='Order', aggregate_version=1,
#  tenant_id=None, occurred_at=datetime.datetime(2026, 7, 28, 4, 33, 15, tzinfo=...))
```

Neither form prints your payload fields. When you need those, print `event.to_dict()`
instead -- covered in the serialization section below.

### Events Compare by Value, Including Identity

Pydantic gives you `__eq__` for free, comparing every field. Since `event_id`,
`correlation_id`, and `occurred_at` all default to fresh values, two separately
constructed events are never equal, even with identical payloads:

```python
print(event == other)   # False -- different event_id, correlation_id, occurred_at
```

That is the behavior you want: each event is a distinct fact, not a value object. When
you *do* want equality -- checking a serialization round-trip, for instance -- compare an
event against a copy derived from it rather than against a freshly constructed one.

### Deriving Instead of Mutating

Reading is unrestricted; writing is not. The next section shows what happens when you
try to assign to a field, and how to produce the variant you actually wanted.

## Event Immutability

Try to change a field:

```python
event.total = 12.00
```

Pydantic raises a `ValidationError` with `type=frozen_instance`. This is deliberate: an
event is a record of the past, and the past does not get edited. Immutability also
means events are safe to share between coroutines, hand to multiple projections, and
cache without defensive copying.

### Creating Modified Copies

When you need a variation, you make a new instance. `DomainEvent` ships three helpers,
each returning a new event and leaving the original untouched:

```python
enriched = event.with_metadata(trace_id="abc123", source="api")
print(enriched.metadata)   # {'trace_id': 'abc123', 'source': 'api'}
print(event.metadata)      # {} -- original unchanged

versioned = event.with_aggregate_version(5)
print(versioned.aggregate_version)  # 5
```

`with_metadata` merges into the existing metadata dict rather than replacing it, so
later keys win over earlier ones. `with_aggregate_version` is normally called for you by
the aggregate when it records an event -- you rarely call it by hand.

The third helper, `with_causation`, gets its own section below. All three are built on
Pydantic's `model_copy(update=...)`, which keeps `event_id` the same; if you want a
genuinely distinct event, construct a new one instead of copying.

## Event Registration

To *read* an event back out of a store you need to turn the stored `event_type` string
back into a class. That mapping lives in an `EventRegistry`.

Defining a subclass does **not** register it. Registration is an explicit step:

```python
from eventsource import get_event_class, is_event_registered, register_event

register_event(OrderPlaced)

print(is_event_registered("OrderPlaced"))          # True
print(get_event_class("OrderPlaced") is OrderPlaced)  # True
```

`register_event` also works as a decorator, which is the form you will see most often:

```python
@register_event
class OrderShipped(DomainEvent):
    aggregate_type: str = "Order"

    carrier: str
    tracking_number: str
```

Looking up an unregistered name raises `EventTypeNotFoundError`, whose message lists
every type that *is* registered -- usually enough to spot the missing import. If you
prefer a soft failure, use `get_event_class_or_none`. `list_registered_events()` returns
all known type names, sorted.

Registering two different classes under the same name raises `DuplicateEventTypeError`.
Registering the same class twice is a harmless no-op.

### Registering Under a Different Name

You can register a class under a name that is not the class name:

```python
@register_event(event_type="order.shipped")
class OrderShipped(DomainEvent):
    aggregate_type: str = "Order"
    carrier: str
```

Be careful: this changes only the *registry key*, not the event's `event_type` field,
which is still `"OrderShipped"`. Events written to the store will carry
`"OrderShipped"` and will not be found under `"order.shipped"`. Use this override only
when you have a deliberate reason -- for stored legacy names, override the field itself
(see the compatibility section below).

The decorator also accepts `registry=` to target a registry other than the module-level
`default_registry`, which is what you want in tests that need isolation:

```python
from eventsource import EventRegistry

test_registry = EventRegistry()
test_registry.register(OrderPlaced)
```

`EventRegistry` is thread-safe (it guards every operation with an `RLock`), supports
`in`, `len()`, and iteration, and offers `clear()` and `unregister()` for test teardown.

## Multiple Events for Different State Transitions

One event type per meaningful state transition. Resist the urge to write a single
`OrderChanged` event with a `change_type` field -- that pushes interpretation onto every
consumer and defeats the purpose of naming the fact.

```python
@register_event
class OrderPlaced(DomainEvent):
    aggregate_type: str = "Order"
    order_number: str
    customer_id: UUID
    total: float


@register_event
class OrderPaid(DomainEvent):
    aggregate_type: str = "Order"
    payment_reference: str
    amount: float


@register_event
class OrderShipped(DomainEvent):
    aggregate_type: str = "Order"
    carrier: str
    tracking_number: str


@register_event
class OrderCancelled(DomainEvent):
    aggregate_type: str = "Order"
    reason: str
```

Each event carries only the data that transition introduces. `OrderShipped` does not
repeat the customer ID; a reader that needs it replays `OrderPlaced` too.

### Event Sequence Example

A single order's life is a sequence sharing one `aggregate_id`, with
`aggregate_version` increasing by one each step:

```python
order_id = uuid4()

history = [
    OrderPlaced(
        aggregate_id=order_id,
        aggregate_version=1,
        order_number="ORD-001",
        customer_id=uuid4(),
        total=99.95,
    ),
    OrderPaid(
        aggregate_id=order_id,
        aggregate_version=2,
        payment_reference="PAY-77",
        amount=99.95,
    ),
    OrderShipped(
        aggregate_id=order_id,
        aggregate_version=3,
        carrier="UPS",
        tracking_number="1Z999",
    ),
]

for e in history:
    print(f"v{e.aggregate_version}: {e.event_type}")
```

In real code an aggregate assigns `aggregate_version` for you; here we set it by hand
to make the sequence visible.

## Correlation and Causation Tracking

These two fields are what let you reconstruct "why did this happen?" months later, and
they are the reason event-sourced systems are pleasant to debug.

### Correlation ID

`correlation_id` groups every event produced by one logical operation, even across
different aggregates. Each event defaults to a fresh `correlation_id`, so you propagate
it deliberately. `is_correlated_with` compares two events:

```python
placed = history[0]
paid = history[1]

print(paid.is_correlated_with(placed))  # False -- independent correlation IDs so far
```

### Causation ID

`causation_id` is narrower: it names the single event that directly caused this one.
`with_causation` sets it, and inherits the causing event's correlation ID at the same
time -- so one call joins both the chain and the group:

```python
placed = OrderPlaced(
    aggregate_id=order_id,
    order_number="ORD-001",
    customer_id=uuid4(),
    total=99.95,
)

paid = OrderPaid(
    aggregate_id=order_id,
    aggregate_version=2,
    payment_reference="PAY-77",
    amount=99.95,
).with_causation(placed)

print(paid.causation_id == placed.event_id)              # True
print(paid.correlation_id == placed.correlation_id)      # True
print(paid.is_caused_by(placed))                         # True
print(paid.is_correlated_with(placed))                   # True
```

Chain a third event off the second and all three end up sharing one correlation ID
while each points at its immediate cause:

```python
shipped = OrderShipped(
    aggregate_id=order_id,
    aggregate_version=3,
    carrier="UPS",
    tracking_number="1Z999",
).with_causation(paid)

print(shipped.is_caused_by(paid))            # True
print(shipped.is_caused_by(placed))          # False -- not the direct cause
print(shipped.is_correlated_with(placed))    # True -- same operation
```

## Event Serialization

`to_dict()` produces a JSON-compatible dictionary using Pydantic's `mode="json"`, so
UUIDs come out as strings and datetimes as ISO-8601 text:

```python
data = placed.to_dict()

print(type(data["event_id"]))    # <class 'str'>
print(data["occurred_at"])       # '2026-07-28T03:43:23.216808Z'
print(data["event_type"])        # 'OrderPlaced'
print(data["order_number"])      # 'ORD-001'
```

The dictionary contains every field -- your payload plus all twelve metadata fields.
`from_dict()` is the inverse and validates as it goes:

```python
restored = OrderPlaced.from_dict(data)
print(restored == placed)  # True
```

Round-tripping through storage combines the registry with `from_dict`:

```python
event_class = get_event_class(data["event_type"])
restored = event_class.from_dict(data)
```

That two-line pattern is exactly what the event stores do internally when they read
events back.

## Pydantic Validation

Because `DomainEvent` is a Pydantic model, your field annotations are enforced at
construction. Bad input fails immediately, at the boundary, rather than being written
to an append-only log you cannot edit:

```python
from pydantic import ValidationError

try:
    OrderPlaced(
        aggregate_id=uuid4(),
        order_number="ORD-002",
        customer_id=uuid4(),
        total="not a number",
    )
except ValidationError as exc:
    print(exc)
```

Missing required fields, malformed UUID strings, and out-of-range values all raise
`ValidationError` too -- `event_version` and `aggregate_version` both carry `ge=1`
constraints, so a zero or negative version is rejected.

You can tighten validation further with normal Pydantic `Field` constraints:

```python
from pydantic import Field


class OrderPlaced(DomainEvent):
    aggregate_type: str = "Order"

    order_number: str = Field(..., min_length=1)
    customer_id: UUID
    total: float = Field(..., ge=0)
```

## Naming Conventions

Event names outlive the code that writes them. They end up in stored data, dashboards,
and other teams' consumers, so pick carefully.

### Use Past Tense

Events describe what happened, so name them in the past tense: `OrderPlaced`, not
`PlaceOrder` (that is a command) and not `OrderPlacement` (that is a noun with no verb).

### Be Specific

`OrderUpdated` tells a consumer nothing. `ShippingAddressCorrected` and
`OrderCancelled` each say precisely what changed, and consumers can subscribe to just
the ones they care about.

### Use Business Language

Name events the way people in the business talk. `InvoiceVoided` beats
`InvoiceStatusSetToThree`. If a domain expert would not recognize the name, it is
leaking implementation detail into your permanent record.

### The Class Name Is the Contract

Since `event_type` defaults to the class name, renaming a class renames the wire format
of every future event and breaks lookups for stored ones. Treat class names of events
as published API: additive changes are cheap, renames are not.

## Compatibility: Overriding event_type for Legacy Stored Names

Sometimes the stored name and the name you want in code cannot match -- typically when
adopting this library over an existing event log. You can pin `event_type` explicitly:

```python
class OrderPlaced(DomainEvent):
    event_type: str = "order_created_v2"
    aggregate_type: str = "Order"
    suppress_event_type_warning = True

    order_number: str
```

An explicit default wins over auto-derivation in both paths: `__init_subclass__` leaves
your value alone, and `_ensure_event_type` only fills in the class name when the field
default is empty. `register_event` follows the same resolution order -- explicit
argument, then the field default, then the class name -- so this class registers under
`"order_created_v2"` automatically.

### When to Override

Override only when an external contract forces your hand:

- Historical events already stored under a different name
- A cross-language event schema with its own naming rules
- A class rename that must not change the wire format

Do not override just because you prefer `snake_case`. Consistency with the class name is
worth more than aesthetic preference.

### Silencing the Mismatch Warning with suppress_event_type_warning

When `event_type` differs from the class name, `__init_subclass__` logs a warning at
class-definition time telling you the two disagree. That default is right: an accidental
mismatch is a real bug and usually silent otherwise.

When the mismatch is intentional, set the class variable:

```python
class OrderPlaced(DomainEvent):
    event_type: str = "order_created_v2"
    aggregate_type: str = "Order"
    suppress_event_type_warning = True
```

`suppress_event_type_warning` is a `ClassVar[bool]`, not a model field -- it never
appears in `to_dict()` output. Set it on the class only where you have consciously
accepted the divergence, never project-wide.

### Renaming a Class Safely

To rename `OrderCreated` to `OrderPlaced` without invalidating stored history, pin the
old name as `event_type`:

```python
class OrderPlaced(DomainEvent):
    event_type: str = "OrderCreated"   # stored name, unchanged
    aggregate_type: str = "Order"
    suppress_event_type_warning = True

    order_number: str
```

Old and new events keep deserializing under `"OrderCreated"`, and your code reads
`OrderPlaced` everywhere. Leave a comment explaining the pin, because the next reader
will otherwise assume it is a mistake.

## Complete Working Example

The whole tutorial as one runnable script:

```python
"""Tutorial 2: your first domain event."""

from uuid import UUID, uuid4

from pydantic import Field, ValidationError

from eventsource import DomainEvent, get_event_class, register_event


@register_event
class OrderPlaced(DomainEvent):
    """A customer placed an order."""

    aggregate_type: str = "Order"

    order_number: str = Field(..., min_length=1)
    customer_id: UUID
    total: float = Field(..., ge=0)


@register_event
class OrderPaid(DomainEvent):
    """Payment was captured for an order."""

    aggregate_type: str = "Order"

    payment_reference: str
    amount: float = Field(..., ge=0)


@register_event
class OrderShipped(DomainEvent):
    """The order left the warehouse."""

    aggregate_type: str = "Order"

    carrier: str
    tracking_number: str


def main() -> None:
    order_id = uuid4()

    placed = OrderPlaced(
        aggregate_id=order_id,
        aggregate_version=1,
        order_number="ORD-001",
        customer_id=uuid4(),
        total=99.95,
    ).with_metadata(source="tutorial")

    paid = OrderPaid(
        aggregate_id=order_id,
        aggregate_version=2,
        payment_reference="PAY-77",
        amount=99.95,
    ).with_causation(placed)

    shipped = OrderShipped(
        aggregate_id=order_id,
        aggregate_version=3,
        carrier="UPS",
        tracking_number="1Z999",
    ).with_causation(paid)

    for event in (placed, paid, shipped):
        print(f"v{event.aggregate_version} {event.event_type} id={event.event_id}")

    assert paid.is_caused_by(placed)
    assert shipped.is_caused_by(paid)
    assert shipped.is_correlated_with(placed)

    # Immutability
    try:
        placed.total = 1.0
    except ValidationError:
        print("events are frozen")

    # Validation
    try:
        OrderPlaced(
            aggregate_id=order_id,
            order_number="",
            customer_id=uuid4(),
            total=-1.0,
        )
    except ValidationError:
        print("invalid payload rejected")

    # Serialize, then rebuild through the registry
    data = placed.to_dict()
    restored = get_event_class(data["event_type"]).from_dict(data)
    assert restored == placed
    print("round-trip ok")


if __name__ == "__main__":
    main()
```

Running it prints the three events, then `events are frozen`, `invalid payload
rejected`, and `round-trip ok`.

## Key Takeaways

- Events subclass `DomainEvent`, a frozen Pydantic v2 model; you write only the payload
  fields and pin `aggregate_type`.
- `event_type` defaults to the class name, filled in by `__init_subclass__` for normal
  construction and by the `_ensure_event_type` validator for dict construction.
- Events are immutable. Use `with_metadata`, `with_causation`, and
  `with_aggregate_version` to derive new instances.
- Subclassing does not register an event. Call `register_event` (as a function or a
  decorator) so `get_event_class` can resolve stored type names.
- `correlation_id` groups an operation; `causation_id` names the direct cause.
  `with_causation` sets both at once.
- `to_dict()` / `from_dict()` are the serialization boundary, and validation happens on
  the way in so bad data never reaches the log.
- Name events in past tense, specifically, in business language -- the class name is a
  published contract. Override `event_type` only for legacy stored names, and pair the
  override with `suppress_event_type_warning = True`.

## Next Steps

With events defined, the next step is folding them into state. Continue to the next
tutorial to build an aggregate that records these events and rebuilds an order from its
history, then persist that history in an event store.
