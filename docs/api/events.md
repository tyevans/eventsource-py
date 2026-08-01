# Events API Reference

Reference documentation for the event primitives defined in `eventsource.domain.event`
and `eventsource.domain.event_registry` (and re-exported from the top-level
`eventsource` package): the `DomainEvent` base class, the `EventRegistry`
type-name-to-class map, the `register_event` decorator, the module-level convenience
functions that operate on the shared `default_registry`, and the two registry
exceptions.

Public names covered here:

| Name | Kind | Purpose |
| --- | --- | --- |
| `DomainEvent` | class | Frozen Pydantic base class for all domain events |
| `EventRegistry` | class | Maps `event_type` strings to event classes for deserialization |
| `default_registry` | instance | Module-level `EventRegistry` used by the convenience API |
| `register_event` | decorator | Registers an event class, bare or parenthesized |
| `get_event_class` | function | Look up a class by type name in `default_registry` |
| `get_event_class_or_none` | function | Non-raising lookup in `default_registry` |
| `is_event_registered` | function | Membership test against `default_registry` |
| `list_registered_events` | function | List registered type names in `default_registry` |
| `EventTypeNotFoundError` | exception | Raised on a failed strict lookup |
| `DuplicateEventTypeError` | exception | Raised on a conflicting registration |

```python
from eventsource import (
    DomainEvent,
    EventRegistry,
    default_registry,
    register_event,
    get_event_class,
    get_event_class_or_none,
    is_event_registered,
    list_registered_events,
    EventTypeNotFoundError,
    DuplicateEventTypeError,
)
```

Or, for the submodule-style import:

```python
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import (
    EventRegistry,
    default_registry,
    register_event,
    get_event_class,
    get_event_class_or_none,
    is_event_registered,
    list_registered_events,
)
from eventsource.domain.exceptions import EventTypeNotFoundError, DuplicateEventTypeError
```

Behavior described below is that of the current source in
`src/eventsource/domain/event.py` and `src/eventsource/domain/event_registry.py`.
Events are immutable: `DomainEvent` sets `model_config = ConfigDict(frozen=True)`,
so every mutation-shaped method returns a new instance rather than modifying in place.

## Overview

These two modules have two separable responsibilities.

**`DomainEvent` (`domain/event.py`) defines the event record.** It is a Pydantic v2
`BaseModel` with `model_config = ConfigDict(frozen=True)` that carries the envelope
fields every event in the system shares — identity (`event_id`, `event_type`,
`event_version`, `occurred_at`), aggregate placement (`aggregate_id`,
`aggregate_type`, `aggregate_version`), tenancy and attribution (`tenant_id`,
`actor_id`), chain tracking (`correlation_id`, `causation_id`), and an open
`metadata` dict. Only `aggregate_id` and `aggregate_type` are required; everything
else has a default or a default factory. Subclasses add their payload fields as
ordinary Pydantic fields.

Because instances are frozen, the `with_*` methods (`with_causation`,
`with_metadata`, `with_aggregate_version`) are implemented with `model_copy(update=...)`
and return a new instance; `is_caused_by` and `is_correlated_with` are pure
comparisons over `causation_id` / `correlation_id`. `to_dict()` is
`model_dump(mode="json")` and `from_dict()` is `model_validate()`, so the pair
round-trips through JSON-safe primitives.

**`EventRegistry` (`domain/event_registry.py`) maps type names back to classes.** Storage
and message-bus payloads carry `event_type` as a string; deserialization needs the
class that string names. The registry is a thread-safe `dict[str, type[DomainEvent]]`
guarded by an `RLock`. A module-level `default_registry` instance backs the
`register_event` decorator and the four convenience functions, and you can construct
additional independent `EventRegistry` instances — the usual reason being test
isolation.

### How the two interact

`event_type` is the join key, and it is derived in two places:

- At class-definition time, `DomainEvent.__init_subclass__` rewrites the `event_type`
  field default from `""` to the subclass's `__name__`, unless the subclass declared
  its own string `event_type` in its class body.
- At validation time, the `_ensure_event_type` before-validator fills `event_type`
  with the class name when dict input omits it (or passes an empty string) and the
  field default is still empty.

`EventRegistry._resolve_event_type` then reads that same field default when no
explicit name is passed to `register`, so a plain `@register_event` on a plain
subclass registers under the class name with no boilerplate. Each of these steps is
detailed in its own section below.

### Registration is explicit

Defining a `DomainEvent` subclass does **not** put it in any registry.
`__init_subclass__` only adjusts the `event_type` default. A class becomes
resolvable by name only after `@register_event` or a `registry.register(...)` call
runs — which means the module defining the event has to be imported before a lookup
of that type name can succeed, otherwise `get_event_class` raises
`EventTypeNotFoundError`.

### Typical lifecycle

```python
from uuid import uuid4

from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import get_event_class, register_event


@register_event
class OrderCreated(DomainEvent):
    aggregate_type: str = "Order"
    order_number: str


event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
assert event.event_type == "OrderCreated"

payload = event.to_dict()                      # JSON-safe dict, event_type included
cls = get_event_class(payload["event_type"])   # "OrderCreated" -> OrderCreated
restored = cls.from_dict(payload)
assert restored == event
```

## `DomainEvent`

```python
from eventsource.domain.event import DomainEvent
```

Frozen Pydantic v2 base class for all domain events. Defined in
`src/eventsource/domain/event.py`.

### Class definition signature

```python
class DomainEvent(BaseModel):
    model_config = ConfigDict(frozen=True)
    suppress_event_type_warning: ClassVar[bool] = False
    ...
```

A plain Pydantic v2 `BaseModel` subclass — not generic, not abstract, and with no
metaclass beyond Pydantic's own. It can be instantiated directly (`aggregate_id` and
`aggregate_type` are the only required fields), but a bare `DomainEvent` has an empty
`event_type` and carries no payload, so in practice you always subclass it.

Subclass it and add payload fields as ordinary Pydantic fields. The only constructor
arguments with no default are `aggregate_id` and `aggregate_type` (plus whatever
required fields the subclass adds), so the minimal subclass looks like:

```python
class OrderCreated(DomainEvent):
    aggregate_type: str = "Order"   # pin the aggregate type as a field default
    order_number: str               # payload
```

Subclassing is what triggers the two behaviors this class exists to provide:

```python
def __init_subclass__(cls, **kwargs: Any) -> None
```

`DomainEvent` defines no class keyword arguments of its own. `__init_subclass__`
calls `super().__init_subclass__(**kwargs)` first, so Pydantic's own class kwargs
(`frozen=`, `extra=`, and so on) continue to work on subclasses; it then rewrites the
`event_type` field default to `cls.__name__` and, when the class declared its own
`event_type`, checks it against the class name. Both steps are covered under
[`event_type` auto-derivation](#event_type-auto-derivation).

Nothing here registers the class. Add `@register_event` (or call
`registry.register(...)`) to make the type name resolvable for deserialization.

### Configuration (`model_config`: frozen/immutable)

```python
model_config = ConfigDict(frozen=True)
```

That is the entire model config — `frozen=True` and nothing else. `extra` is left at
Pydantic's default (`"ignore"`), and no custom serializers or aliases are configured.

**Attribute assignment raises `ValidationError`.** An event records something that
already happened, so it never changes after construction. This applies to every
field, including `metadata`:

```python
event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")

event.order_number = "ORD-002"   # pydantic ValidationError
event.aggregate_id = uuid4()     # pydantic ValidationError
event.metadata = {"new": "dict"} # pydantic ValidationError
```

Consequently every mutation-shaped API on the class returns a *new* instance rather
than modifying in place: `with_causation`, `with_metadata`, and
`with_aggregate_version` are all thin wrappers over `model_copy(update=...)`.

**Equality is by field value.** Pydantic's generated `__eq__` compares the class and
all field values, so two separately constructed events are equal only when every
field matches — and since `event_id`, `correlation_id`, and `occurred_at` all have
per-instance default factories, independently constructed events are normally
*unequal*. Round-tripping preserves equality because the ids come along in the
payload: `OrderCreated.from_dict(event.to_dict()) == event`.

**Events are not hashable.** Frozen Pydantic models are hashable in general, but
`DomainEvent` always carries a `metadata: dict[str, Any]` field, and hashing a model
hashes its field values — so `hash(event)` raises
`TypeError: unhashable type: 'dict'` even when `metadata` is empty. Do not use events
as dict keys or set members; key on `event_id` instead.

```python
seen: set[UUID] = set()
seen.add(event.event_id)   # not `seen.add(event)`
```

**Freezing is shallow.** The `metadata` dict object cannot be swapped out, but
nothing prevents in-place mutation of its contents — `event.metadata["k"] = v`
succeeds and silently mutates a supposedly immutable record (and any other object
holding a reference to that same dict). Always go through
`with_metadata(**kwargs)`, which builds a fresh merged dict on a copy. The same
caveat applies to any mutable value a subclass adds as a field.

**`model_copy(update=...)` bypasses validation.** That is a Pydantic property, not a
choice made here, and it is how the `with_*` helpers construct their copies — so
`with_aggregate_version(0)` yields an instance whose `aggregate_version` violates the
declared `ge=1` constraint instead of raising. Validate untrusted values before
passing them to a `with_*` method, or construct through `model_validate` /
`from_dict` when input is untrusted.

### Class variables

#### `suppress_event_type_warning: ClassVar[bool] = False`

`DomainEvent` declares exactly one class variable.

```python
class DomainEvent(BaseModel):
    suppress_event_type_warning: ClassVar[bool] = False
```

Because it is annotated `ClassVar`, Pydantic excludes it from the model's fields.
It therefore:

- never appears in `model_fields`, `to_dict()` output, or any serialized payload;
- cannot be passed to the constructor;
- is read off the class, not the instance (`TestEvent.suppress_event_type_warning`).

Its only effect is on the class-definition-time mismatch warning. When a subclass
declares its own string `event_type` that differs from the class name,
`__init_subclass__` reads the flag with
`getattr(cls, "suppress_event_type_warning", False)` and skips the
`logger.warning(...)` call on the `eventsource.domain.event` logger when it is truthy.
Setting the flag has no effect on any other behavior — the `event_type` value itself,
validation, and registration are all unchanged.

```python
class OrderCreated(DomainEvent):
    event_type: str = "order_created_v2"
    aggregate_type: str = "Order"
    suppress_event_type_warning = True   # acknowledge the intentional divergence
    order_number: str
```

Note that no annotation is needed at the use site: writing a bare
`suppress_event_type_warning = True` in the subclass body is enough, since the
`ClassVar` annotation on the base already tells Pydantic this name is not a field.

**Inheritance.** The flag is looked up through the normal MRO, so a subclass of an
event that set it to `True` also suppresses its own warning. A child can opt back in
by setting it to `False` in its own body:

```python
class ParentEvent(DomainEvent):
    aggregate_type: str = "Test"
    suppress_event_type_warning = True


class ChildEventWithWarning(ParentEvent):
    event_type: str = "different"
    suppress_event_type_warning = False   # warning is logged for this class
```

Setting the flag on a class whose `event_type` matches its class name is harmless
but pointless — no warning would have been emitted anyway. Prefer leaving it at the
default `False` so an accidental rename that desynchronizes the wire name from the
class name still shows up in the logs.

### Fields

`DomainEvent` declares twelve envelope fields, all as Pydantic `Field(...)`
declarations. Exactly two — `aggregate_id` and `aggregate_type` — are required;
the rest have a default or a `default_factory`. Subclasses add their payload fields
alongside these and may override any default (most commonly `aggregate_type` and
`event_version`).

#### Field table

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `event_id` | `UUID` | `uuid4()` (factory) | Unique identifier for this event instance |
| `event_type` | `str` | `""`, rewritten to the class name by `__init_subclass__` | Type name used as the registry / storage discriminator |
| `event_version` | `int` | `1` (`ge=1`) | Event schema version, for migrations |
| `occurred_at` | `datetime` | `datetime.now(UTC)` (factory) | When the event occurred |
| `aggregate_id` | `UUID` | *required* | Aggregate this event belongs to |
| `aggregate_type` | `str` | *required* | Aggregate type name, e.g. `"Order"` |
| `aggregate_version` | `int` | `1` (`ge=1`) | Aggregate version after this event |
| `tenant_id` | `UUID \| None` | `None` | Tenant this event belongs to |
| `actor_id` | `str \| None` | `None` | User or system that triggered the event |
| `correlation_id` | `UUID` | `uuid4()` (factory) | Links related events across aggregates |
| `causation_id` | `UUID \| None` | `None` | `event_id` of the event that caused this one |
| `metadata` | `dict[str, Any]` | `{}` (factory) | Open extension bag |

Rows are listed in declaration order, which is also the order Pydantic uses for
positional-free keyword construction and for `to_dict()` output. Every row is a real
Pydantic `Field(...)`; `suppress_event_type_warning` is deliberately absent because it
is a `ClassVar`, not a field.

Only two constraints are declared: `event_version` and `aggregate_version` both carry
`ge=1`, so `0` and negatives raise `ValidationError` at construction. Three fields use
a `default_factory` and therefore produce a fresh value per instance — `event_id`,
`correlation_id` (both `uuid4`), and `occurred_at` (`lambda: datetime.now(UTC)`);
`metadata` likewise gets its own new `dict` per instance rather than a shared one.

Subclasses override defaults by redeclaring the field
(`aggregate_type: str = "Order"`, `event_version: int = 2`), which is the normal way
to turn a required field into an implied one.

#### Event metadata fields: `event_id`, `event_type`, `event_version`, `occurred_at`

`event_id` defaults to a fresh `uuid4()` per instance and is the identity another
event points at through `causation_id`. `event_type` is the string discriminator
written to storage and message payloads and looked up in an `EventRegistry` on the
way back; its derivation is detailed in the next section. `event_version` is an
integer schema version constrained to `>= 1` — bump it in a subclass
(`event_version: int = 2`) when the payload shape changes so upcasting code can
branch on it. `occurred_at` defaults to `datetime.now(UTC)` evaluated at
construction time, so it is timezone-aware UTC; pass it explicitly when recording a
historical or externally-timestamped occurrence.

#### Aggregate fields: `aggregate_id`, `aggregate_type`, `aggregate_version`

`aggregate_id` and `aggregate_type` are the two required fields — an event always
belongs to some aggregate instance of some type. In practice subclasses pin
`aggregate_type` with a field default (`aggregate_type: str = "Order"`) so callers
only supply `aggregate_id`.

`aggregate_version` (`>= 1`) is the aggregate's version *after* this event applies.
It defaults to `1`, which is rarely the right value for anything but the first event;
the aggregate assigns the real number when recording, normally through
`with_aggregate_version(...)`. Note that the `ge=1` constraint means `0` is not a
legal version.

#### Multi-tenancy and actor fields: `tenant_id`, `actor_id`

Both optional and both `None` by default. `tenant_id: UUID | None` carries tenant
scoping for multi-tenant deployments; `actor_id: str | None` is a free-form string
identifying the user or system that triggered the event (a subject claim, a service
name, a job identifier). Neither is populated automatically by `DomainEvent` itself
— setting them is the caller's or the command-handler's job.

#### Correlation fields: `correlation_id`, `causation_id`

`correlation_id` groups every event belonging to one logical operation or saga. It
defaults to a fresh `uuid4()`, so an event that starts a new flow gets its own group
identifier for free; events continuing an existing flow must inherit the originating
event's value. `causation_id` is the `event_id` of the single event that directly
caused this one and defaults to `None` for a root event.

`with_causation(causing_event)` sets both correctly in one step, and
`is_caused_by` / `is_correlated_with` read them back.

#### Extension field: `metadata`

`dict[str, Any]`, defaulting to an empty dict. Use it for cross-cutting context that
is not part of the domain payload: trace and span ids, request ids, source system,
client version. Because `to_dict()` serializes with `mode="json"`, values placed here
must be JSON-serializable by Pydantic or the dump will fail. Add entries with
`with_metadata(**kwargs)`.

### `event_type` auto-derivation

#### Default is the empty string, not a required value

The field is declared `event_type: str = Field(default="", ...)`. `DomainEvent`
itself therefore has an empty `event_type`; the value only becomes meaningful for
subclasses, which get it filled in by the two mechanisms below. This is why you never
write `event_type = "OrderCreated"` in an event class.

#### `__init_subclass__`: rewrites the field default to the class name

`DomainEvent.__init_subclass__` runs at class-definition time. It checks whether
`"event_type"` appears in the new class's own `__dict__` with a `str` value — which
catches both `event_type: str = "custom"` and a bare `event_type = "custom"`.

- **Not declared in the class body**: the class's `FieldInfo` default for
  `event_type` is reassigned to `cls.__name__`. Instances then get the class name
  with no validator involvement.
- **Declared in the class body**: the explicit value is left alone, and a mismatch
  check runs (below).

Every subclass in a hierarchy gets its own name, including intermediate base
classes: given `BaseEvent -> MiddleEvent -> FinalEvent` with no explicit
declarations, the three `event_type` values are `"BaseEvent"`, `"MiddleEvent"`, and
`"FinalEvent"`. A subclass of a class with an explicit `event_type` may declare its
own explicit value to override it.

#### `_ensure_event_type` before-validator

`_ensure_event_type` is a `@model_validator(mode="before")` classmethod that runs on
dict input — the `model_validate` / `from_dict` / `OrderCreated(**data)` paths. It
acts only when `data` is a dict, and only when the incoming `event_type` is missing
or falsy. In that case it substitutes `cls.__name__` if either

1. the field default is empty (auto-derivation applies), or
2. an empty string was passed explicitly (the empty value is replaced).

It copies the dict before writing, so the caller's input mapping is not mutated. A
class with an explicit non-empty default keeps that default when `event_type` is
simply absent from the input.

The net effect is that all four construction paths agree:

```python
e1 = OrderCreated(aggregate_id=aid, order_number="ORD-001")
e2 = OrderCreated.model_validate({"aggregate_id": aid, "order_number": "ORD-001"})
e3 = OrderCreated.from_dict({"aggregate_id": aid, "order_number": "ORD-001"})
e4 = OrderCreated(**{"aggregate_id": aid, "order_number": "ORD-001", "event_type": ""})
assert e1.event_type == e2.event_type == e3.event_type == e4.event_type == "OrderCreated"
```

Note that a *non-empty* `event_type` supplied at construction is kept as-is — the
validator only fills in missing or empty values, it does not enforce agreement with
the class name.

#### Overriding `event_type` explicitly and the mismatch warning

Declaring a different name is supported and is the mechanism for keeping a legacy
wire format:

```python
class OrderCreated(DomainEvent):
    event_type: str = "order_created_v2"
    aggregate_type: str = "Order"
    order_number: str
```

Because `"order_created_v2" != "OrderCreated"`, `__init_subclass__` logs a warning
on the `eventsource.domain.event` logger at class-definition time (import time, once
per class — not once per instance):

```
Event class OrderCreated has event_type='order_created_v2' which differs from class
name. This may cause confusion. Set suppress_event_type_warning=True to silence this
warning.
```

An explicit `event_type` that *matches* the class name produces no warning (and no
benefit — it is exactly what auto-derivation would have done).

#### Suppressing the warning with `suppress_event_type_warning = True`

Add the class variable to acknowledge the divergence:

```python
class OrderCreated(DomainEvent):
    event_type: str = "order_created_v2"
    aggregate_type: str = "Order"
    suppress_event_type_warning = True
    order_number: str
```

The flag is read with `getattr` during `__init_subclass__`, so an inherited `True`
from a parent event class also suppresses the warning for children; a child can set
it back to `False` to opt its own definition into the warning again.

### Methods

#### `with_causation(causing_event)`

```python
def with_causation(self, causing_event: DomainEvent) -> Self
```

Returns a copy with `causation_id` set to `causing_event.event_id` and
`correlation_id` set to `causing_event.correlation_id`. This is the correct way to
continue an event chain: it links to the direct cause *and* joins the originating
saga in one call, replacing the fresh `correlation_id` the new event was born with.

```python
payment = PaymentProcessed(aggregate_id=order_id).with_causation(order_created)
assert payment.causation_id == order_created.event_id
assert payment.correlation_id == order_created.correlation_id
```

#### `with_metadata(**kwargs)`

```python
def with_metadata(self, **kwargs: Any) -> Self
```

Returns a copy whose `metadata` is `{**self.metadata, **kwargs}` — a merge, not a
replacement, with the new keys winning. The original event's dict is untouched.

```python
enriched = event.with_metadata(trace_id="abc123", source="api")
```

#### `with_aggregate_version(version)`

```python
def with_aggregate_version(self, version: int) -> Self
```

Returns a copy with `aggregate_version` set to `version`. Normally called by the
aggregate as it records an event and learns the resulting version.

Because it uses `model_copy(update=...)`, the update bypasses validation — the
`ge=1` constraint is *not* re-checked here, so passing `0` or a negative number
produces an instance that would have failed at construction. Pass a real version.

#### `to_dict()`

```python
def to_dict(self) -> dict[str, Any]
```

`model_dump(mode="json")`. Every value is JSON-safe: `UUID` fields become strings,
`occurred_at` becomes an ISO-8601 string, and the `metadata` contents are serialized
by the same rules. `suppress_event_type_warning` is a `ClassVar` and does not appear.
Subclass payload fields are included alongside the envelope fields.

```python
data = event.to_dict()
assert isinstance(data["event_id"], str)
assert isinstance(data["occurred_at"], str)
```

#### `from_dict(data)` (classmethod)

```python
@classmethod
def from_dict(cls, data: dict[str, Any]) -> Self
```

`cls.model_validate(data)`. Coerces the JSON-shaped primitives back to `UUID` and
`datetime`, applies field constraints, and raises Pydantic's `ValidationError` if the
data does not match the schema. Called on a concrete subclass — usually the one an
`EventRegistry` lookup just returned — not on `DomainEvent` itself. Together with
`to_dict()` it round-trips: `OrderCreated.from_dict(event.to_dict()) == event`.

#### `is_caused_by(event)`

```python
def is_caused_by(self, event: DomainEvent) -> bool
```

`self.causation_id == event.event_id`. Tests the direct causal link only — one hop,
not transitive ancestry. Returns `False` for a root event whose `causation_id` is
still `None`.

#### `is_correlated_with(event)`

```python
def is_correlated_with(self, event: DomainEvent) -> bool
```

`self.correlation_id == event.correlation_id`. True when both events belong to the
same logical operation or saga, regardless of aggregate or ordering. Since
`correlation_id` is never `None`, an event is always correlated with itself.

#### `__str__` / `__repr__`

`__str__` is a short operational line built from the event's own type name:

```
OrderCreated(event_id=..., aggregate_id=..., version=1)
```

where `version` is `aggregate_version`. Note it uses the `event_type` *field*, so an
event with a legacy explicit type prints that string rather than the class name.

`__repr__` is longer and starts from `self.__class__.__name__`, then shows
`event_id`, `event_type`, `aggregate_id`, `aggregate_type`, `aggregate_version`,
`tenant_id`, and `occurred_at`. Neither renders subclass payload fields, so neither
is a substitute for `to_dict()` when you need the full record.

### Examples

#### Defining an event (auto-derived `event_type`, no boilerplate)

```python
from uuid import UUID, uuid4

from eventsource.domain.event import DomainEvent


class OrderCreated(DomainEvent):
    aggregate_type: str = "Order"
    order_number: str
    customer_id: UUID


event = OrderCreated(
    aggregate_id=uuid4(),
    order_number="ORD-001",
    customer_id=uuid4(),
)

assert event.event_type == "OrderCreated"   # from __init_subclass__
assert event.aggregate_version == 1         # default; the aggregate sets the real one
assert event.causation_id is None           # root of a chain
assert event.to_dict()["event_type"] == "OrderCreated"
```

#### Defining an event with an explicit legacy `event_type`

Use this only when the wire/storage name must stay stable while the class is named
something else — for instance after renaming a class whose old events are already
persisted.

```python
class OrderCreated(DomainEvent):
    event_type: str = "order_created_v2"
    event_version: int = 2
    aggregate_type: str = "Order"
    suppress_event_type_warning = True   # acknowledge the intentional divergence
    order_number: str


event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
assert event.event_type == "order_created_v2"
```

Without `suppress_event_type_warning = True` this definition logs a mismatch warning
when the module is imported. Whatever `event_type` ends up on the class is also the
name it registers under by default — see `_resolve_event_type` below.
