# ADR-0012: Event Type Auto-Derivation from Class Name

Every persisted event carries an `event_type` string. That string is what the
event store writes to disk and what `EventRegistry` uses to turn a stored row
back into a Python class. This ADR explains why `DomainEvent` derives that
string from the class name instead of asking authors to declare it, why the
derivation happens in *two* places rather than one, and why a mismatch between
class name and declared `event_type` is a warning rather than an error.

## Status

Accepted. Implemented in `src/eventsource/events/base.py` (`DomainEvent.__init_subclass__`
and `DomainEvent._ensure_event_type`), with behaviour pinned by
`tests/unit/test_event_type_auto.py`.

## Context

### The boilerplate problem: `event_type: str = "OrderCreated"` repeated on every event class

`DomainEvent` is a Pydantic model with an `event_type: str` field. Before
auto-derivation, every subclass had to restate its own name:

```python
class OrderCreated(DomainEvent):
    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
```

The line carries no information — it is the class name spelled a second time.
It is also silently wrong-able: renaming the class does not rename the string,
and nothing in the type system notices. A codebase with a hundred event types
has a hundred opportunities for a copy-paste event class to keep its ancestor's
`event_type` and quietly overwrite another type's registry slot.

### Two independent construction paths that must agree (class definition vs. dict/`model_validate`/`from_dict`)

Events are created two ways in this library, and they do not share a code path:

- **Keyword construction** — `OrderCreated(aggregate_id=..., order_number=...)`.
  Pydantic fills `event_type` from the field default.
- **Dict-shaped input** — `OrderCreated.model_validate(row)`, `OrderCreated.from_dict(payload)`,
  or `OrderCreated(**data)`. This is the deserialization path taken whenever
  events come back out of a store or off a bus.

Whatever mechanism supplies the derived name has to cover both, and has to
produce the same answer for both. A design that only fixes the class-definition
path produces events that round-trip to a different `event_type` than they were
born with.

The split is not only about how instances are built — it is about who *reads*
the name. Instances read it from the value Pydantic assigns; the registry reads
it from `model_fields["event_type"].default` without ever building an instance.
The two paths therefore need two mechanisms, and the rest of this ADR is largely
about keeping them in agreement.

### Existing stored streams already contain `event_type` strings that do not match current class names

Event stores are append-only. Rows written years ago carry whatever string the
class declared at the time — `order_created_v2`, `order.created.v2`,
`OrderCreatedEvent`. Those strings cannot be rewritten, so the class that reads
them must be allowed to keep declaring a name that differs from `cls.__name__`.
Any auto-derivation scheme that *forces* the class name breaks replay of
existing history.

Two things follow, and both are visible in the implementation:

- **A declared `event_type` must survive both construction paths.** When a
  subclass sets `event_type: str = "order_created_v2"`, `__init_subclass__`
  leaves the field default alone, and `_ensure_event_type` substitutes the class
  name only when the field default is empty *or* the caller explicitly passed an
  empty string. A stored row that already carries `"order_created_v2"` is
  passed through untouched, because the validator only acts when the incoming
  `event_type` is missing or falsy.
- **A mismatch cannot be fatal.** The library has no way to distinguish "this
  string is deliberately a legacy wire name" from "someone renamed the class and
  forgot the string". Since the first case is legitimate and unavoidable for any
  system with history, the only mechanism left is a diagnostic —
  `__init_subclass__` logs a warning naming the class and the declared value,
  and `suppress_event_type_warning` turns it off for the deliberate case.

The same pressure applies to *versioned* type names. A class named
`OrderCreated` may legitimately emit `order.created.v2` while an older
`order.created.v1` is still resolvable by a different class. Forcing the class
name would make that impossible without renaming Python classes to match wire
formats.

### Relationship to ADR-0006: the registry resolves names via `EventRegistry._resolve_event_type`, which reads the `event_type` field default

Registration and derivation are coupled through one function. `EventRegistry._resolve_event_type`
resolves a name in three steps:

1. the explicit `event_type` argument, if given;
2. `event_class.model_fields["event_type"].default`, if it is a `str`;
3. `event_class.__name__` as a fallback.

Step 2 reads the *field default*, not an instance value — the registry never
instantiates the class. This is the constraint that shapes the decision below:
derivation must be visible in `model_fields`, not merely applied at construction
time.

Three consequences follow from that coupling:

- **Derivation must happen before registration.** `EventRegistry.register` calls
  `_resolve_event_type` immediately and stores the result as the dictionary key.
  Whatever `model_fields["event_type"].default` holds at that moment is the name
  the class is known by forever after. Since `@register_event` runs as a
  decorator on the class statement — after `__init_subclass__` has already
  completed — mutating the field default during class definition is early enough,
  and nothing later can change the key.
- **The class-name fallback in step 3 is defensive, not the mechanism.** Because
  `__init_subclass__` populates the default, step 2 already returns the class
  name for auto-derived events. Step 3 only fires for classes whose
  `event_type` field somehow has a non-`str` default (for example
  `PydanticUndefined`) — it is a safety net, not the path normal events take.
  This matters when reasoning about failures: if a class registers under an
  unexpected name, the field default is where to look, not the fallback.
- **A wrong default is a registry-level failure, not a per-instance one.** If two
  classes end up with the same resolved name — the classic copy-paste case where
  a subclass inherits an explicit `event_type` from a sibling — `register` raises
  `DuplicateEventTypeError` rather than silently overwriting. Auto-derivation
  makes that collision much rarer, because distinct classes have distinct
  `__name__`s by construction.

ADR-0006 kept registration explicit precisely so that this resolution step is a
deliberate, inspectable act. Auto-derivation does not change that: it only makes
the *input* to `_resolve_event_type` correct by default, so the common case needs
no argument at all.

## Decision

### Derive `event_type` from `cls.__name__` by default; keep the field declared with `default=""` as the sentinel for "not explicitly set"

On `DomainEvent` itself the field is declared as:

```python
event_type: str = Field(
    default="",
    description="Type of event (auto-derived from class name if not set)",
)
```

The empty string is load-bearing. It means "nobody has claimed this name", and
both the class-definition hook and the validator test for it before substituting
the class name.

The choice of `""` rather than `None` or `PydanticUndefined` is what keeps the
field a plain, always-present `str`. Nothing downstream has to widen its type or
handle a missing value: `event.event_type` is a string on every instance,
`to_dict()` always emits the key, and `EventRegistry._resolve_event_type` can
apply a single `isinstance(default, str)` test rather than distinguishing
"unset" from "set to a name". The sentinel is a *value* in the field's own
domain, not a separate absence state.

Two rules follow from treating `""` as "unclaimed", and both are visible in
`src/eventsource/events/base.py`:

- **A falsy value is never a claim.** `__init_subclass__` only treats a
  declaration as explicit when it is a non-empty string
  (`if has_explicit_type and explicit_type:`), and `_ensure_event_type` only
  leaves the incoming data alone when the field default is truthy. So a subclass
  written as `event_type: str = ""` is not an override — the field default stays
  empty and the validator fills in `cls.__name__` at construction time. The class
  behaves exactly as if it had said nothing.
- **An empty value passed at construction is replaced, not preserved.**
  `_ensure_event_type` treats `{"event_type": "", ...}` the same as a dict with
  no `event_type` key at all. This matters because "provided but empty" is a
  value Pydantic would otherwise accept verbatim, bypassing the field default
  entirely — it is the specific hole the validator exists to close.

Derivation is `cls.__name__` verbatim: no case conversion, no suffix stripping,
no namespacing by module. `class OrderCreated(DomainEvent)` yields
`"OrderCreated"`, and `class OrderCreatedEvent(DomainEvent)` yields
`"OrderCreatedEvent"` — the `Event` suffix is not removed. The derivation is
deliberately a pure identity mapping so that the wire name is readable straight
off the class statement, and so that the transformation cannot itself become a
source of surprise. Teams that want a different convention (`order.created`,
`snake_case`) declare it explicitly; that is precisely the case the declared
default and the registry override exist for.

`DomainEvent` itself keeps the empty default. It is abstract in practice — it
declares `aggregate_id` and `aggregate_type` as required and is never persisted
directly — and `__init_subclass__` runs on subclasses, not on the class that
defines it. Its own `model_fields["event_type"].default` therefore remains `""`,
which is also what makes the sentinel readable: every concrete subclass either
overwrites it with its class name or replaces it with a declared string.

### Mutate the `event_type` FieldInfo default in `DomainEvent.__init_subclass__` at class-definition time

When a subclass does not declare `event_type` in its own body,
`__init_subclass__` reaches into `cls.model_fields["event_type"]` and assigns
`field_info.default = cls.__name__`. From that moment the derived name is part
of the model's schema, and anything that introspects `model_fields` — the
registry included — sees it.

"Does not declare" is decided by `"event_type" in cls.__dict__` plus an
`isinstance(value, str)` check, so both `event_type: str = "X"` and a bare
`event_type = "X"` count as declarations, while an inherited value does not. A
subclass that declares `event_type = ""` is treated as having declared it: the
field default stays empty and the validator supplies the class name at
construction time instead.

### Add a `mode="before"` `_ensure_event_type` model validator for dict-shaped input

`_ensure_event_type` runs before validation on any `dict` input. If the incoming
data has no `event_type` (or an empty one), it consults
`cls.model_fields["event_type"].default`: when that default is empty, or when the
caller explicitly passed `""`, it writes `cls.__name__` into a *copy* of the dict.
An explicit non-empty class default is left alone, so a versioned type name still
wins on the deserialization path.

### Warn (never raise) when an explicit `event_type` differs from the class name

If the subclass body contains `event_type` and its value is a string that is not
`cls.__name__`, `__init_subclass__` emits a `logger.warning` naming both the class
and the declared string, and suggesting the opt-out. The class is still created
and the declared value is still used.

### Provide `suppress_event_type_warning: ClassVar[bool]` as the opt-out

Declared on `DomainEvent` as `ClassVar[bool] = False`. Setting it to `True` in a
subclass body silences the mismatch warning for that class:

```python
class OrderCreated(DomainEvent):
    event_type: str = "order_created_v2"
    suppress_event_type_warning = True
```

It is a `ClassVar`, so Pydantic treats it as class-level configuration rather
than a model field, and it never appears in serialized events.

## Rationale

### Why derived rather than declared

The class name is already the canonical human name for the event. Deriving from
it makes the common case zero-effort and removes the class of bug where the
declared string drifts from the class it labels. Declaration remains available
for the cases where it carries real information (legacy names, explicit
versioning) — those are exactly the cases worth writing a line for.

### Why `__init_subclass__` alone is insufficient — the dict path bypasses field defaults in `model_validate`/`from_dict`/`**data`

Mutating the field default does cover keyword construction. It does not reliably
cover dict input where the caller supplies an explicit `event_type` key with an
empty value: `{"event_type": "", ...}` is a *provided* value, so Pydantic uses
it and the default never applies. Since serialized payloads and store rows are
dicts, this path is the norm rather than the exception, and it needs its own
normalization step.

### Why the `mode="before"` validator alone is insufficient — the registry and introspection read `model_fields["event_type"].default`, not instance values

A validator only runs when an instance is built. `EventRegistry.register` never
builds one; `_resolve_event_type` reads the field default directly. If derivation
lived only in the validator, `model_fields["event_type"].default` would stay `""`
for every auto-derived class and the registry would fall through to its
class-name fallback — which happens to agree, but only by coincidence. Anything
else that introspects the schema (JSON Schema generation, tooling, documentation)
would see the empty string. Both mechanisms exist because they serve two
different readers: the validator serves instances, the `FieldInfo` mutation
serves the schema.

### Why detection uses `cls.__dict__` rather than `__annotations__` or `model_fields` (catches both annotated and bare assignment, ignores inherited values)

The check is `if "event_type" in cls.__dict__`, followed by an `isinstance(value, str)`
test. This is deliberate on three counts:

- `__annotations__` would miss a bare `event_type = "OrderCreated"` with no
  annotation.
- `model_fields` is inherited and always contains `event_type`, so it cannot
  distinguish "this class declared it" from "an ancestor did".
- `cls.__dict__` is exactly the class's own body, so a subclass of a subclass
  inherits its parent's declared name without being treated as having declared
  it itself — and without re-emitting the parent's mismatch warning.

### Why mismatch is a warning, not an error: backward compatibility with persisted streams and versioned type names (`OrderCreated.v2`)

Raising would make it impossible to read history written under an older naming
scheme, and would forbid the legitimate pattern of a class named `OrderCreated`
that emits and consumes `order.created.v2`. A warning surfaces the accident
(copy-pasted event class, stale string after a rename) while leaving the
intentional case working. The warning fires at import time, where it is loud
enough to notice during development and harmless in production.

### Why the escape hatch is a `ClassVar` on the model rather than a decorator argument or global setting

A decorator argument would only work for classes that opt into a decorator, and
`DomainEvent` subclassing is the mandatory path while `@register_event` is not.
A global setting would silence unrelated classes and lose the per-class intent.
A `ClassVar` sits next to the declaration it justifies, is inherited by
subclasses that share the same legacy naming, is read via a plain `getattr`
during `__init_subclass__`, and is excluded from the model's fields so it never
leaks into serialized events.

### How this reconciles with ADR-0006's rejection of `__init_subclass__`-based auto-*registration*

ADR-0006 declined to auto-register event classes on subclass creation;
registration stays explicit through `register_event` / `EventRegistry.register`.
This ADR uses `__init_subclass__` for something narrower: computing a default for
a field on the class being defined. It has no global side effects, cannot raise
`DuplicateEventTypeError`, and does not make a class visible to the rest of the
system. Naming is local to the class; registration is a global act. Only the
former is safe to do implicitly.

## Consequences

### Positive

- Event classes are declarations of shape, not of their own names. The common
  case is a class body with only domain fields.
- Renaming a class renames its `event_type` automatically, so the two cannot
  drift apart unnoticed for auto-derived events.
- Both construction paths — keyword and dict — converge on the same string, so
  `to_dict()` / `from_dict()` round-trips are stable.
- The registry needs no special case: `_resolve_event_type` reads a field default
  that is already correct.

### Negative and risks (mutating shared `FieldInfo` state, pydantic-internal coupling, warning noise at import time)

- `__init_subclass__` mutates `field_info.default` in place on an object obtained
  from `cls.model_fields`. This relies on each subclass owning its own `FieldInfo`
  rather than sharing the parent's; it is correct under current Pydantic v2
  behaviour but is not a documented contract.
- The design is coupled to Pydantic internals in two places (`model_fields`
  mutability, and the `mode="before"` validator receiving raw dict input). A
  Pydantic upgrade that changes either would need this code revisited — the unit
  suite in `tests/unit/test_event_type_auto.py` is the tripwire.
- Mismatch warnings fire at import time, once per class definition. A legacy
  codebase migrating to this library sees a burst of warnings on first import
  until each intentional mismatch is annotated with `suppress_event_type_warning`.
- The two-mechanism design means a future reader must understand both to reason
  about `event_type`; neither alone tells the whole story.

### Neutral

- `event_type` remains a real, serialized field on every event. Nothing about
  the wire format changes.
- Explicit declarations continue to work exactly as before; this is additive.
- The `default=""` sentinel is observable — a class could in principle be built
  with an empty `event_type` if it bypasses both mechanisms, but no supported
  construction path does so.

## Interaction with the Event Registry (ADR-0006)

### Precedence order: explicit `register_event(event_type=...)` > declared field default > derived class name

`EventRegistry._resolve_event_type` applies these in order:

1. `event_type` passed to `register` / `@register_event(event_type=...)`.
2. `model_fields["event_type"].default`, when it is a `str`. For auto-derived
   classes `__init_subclass__` has already set this to the class name; for
   classes with an explicit declaration it is the declared string.
3. `event_class.__name__`.

Because step 2 is populated by auto-derivation, step 3 is a defensive fallback
rather than the mechanism that makes derivation work.

### Implications for renaming a class after events have been persisted

For an auto-derived event, renaming the class changes the `event_type` it
registers under and the `event_type` new instances carry. Rows already in the
store still hold the old string, and the registry will no longer resolve it —
`EventRegistry.get` raises for the unknown name. Two safe options:

- Keep the class name and change nothing.
- Rename the class and pin the old wire name, either on the class
  (`event_type: str = "OldName"` plus `suppress_event_type_warning = True`) or at
  the registry (`@register_event(event_type="OldName")`), so old rows keep
  resolving.

## Usage

### Default case: no `event_type` declaration

```python
class OrderCreated(DomainEvent):
    aggregate_type: str = "Order"
    order_number: str
    customer_id: UUID


event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001", customer_id=uuid4())
assert event.event_type == "OrderCreated"
```

The same holds for dict input:

```python
restored = OrderCreated.from_dict(event.to_dict())
assert restored.event_type == "OrderCreated"
```

### Explicit override with `suppress_event_type_warning = True`

```python
class OrderCreated(DomainEvent):
    event_type: str = "order_created_v2"
    suppress_event_type_warning = True
    aggregate_type: str = "Order"
    order_number: str


assert OrderCreated(aggregate_id=uuid4(), order_number="ORD-001").event_type == "order_created_v2"
```

The declared value wins on both construction paths, and no warning is logged.
Omitting `suppress_event_type_warning` produces the same behaviour plus a
warning naming the class and the string.

### Registry-level override without touching the class

```python
@register_event(event_type="order.created")
class OrderCreated(DomainEvent):
    aggregate_type: str = "Order"
    order_number: str
```

Here the registry key is `order.created` while instances still carry
`event_type == "OrderCreated"` — the explicit registration argument affects
resolution only, not the field. Use this when the stored key and the in-memory
field genuinely need to differ; prefer the class-level declaration when they
should agree.

## Alternatives Considered

### Require explicit `event_type` on every event (status quo ante)

Simplest to explain and free of Pydantic coupling, but reintroduces the
duplicated-name boilerplate and the rename-drift bug on every class. Rejected:
the failure mode is silent and its blast radius is the whole registry.

### Metaclass instead of `__init_subclass__`

A metaclass could do the same work, but Pydantic's `BaseModel` already has one
(`ModelMetaclass`); adding another means subclassing it and inheriting
responsibility for its ordering guarantees. `__init_subclass__` achieves the same
effect with a documented hook and no metaclass conflict for users who bring their
own. Rejected as a strictly larger commitment for the same result.

### `mode="after"` or field `default_factory` instead of mutating the field default

`mode="after"` runs post-validation on a `frozen=True` model, so setting
`event_type` there means mutating an immutable instance. A `default_factory`
cannot see the class it is defaulting for — factories take no arguments and the
one on `DomainEvent` would be shared by every subclass, yielding the same name
for all of them. Neither reaches `model_fields[...].default`, so neither would
serve the registry. Rejected on all three counts.

### Raising on class-name/`event_type` mismatch

Would catch stale strings immediately, but breaks reading persisted streams
whose names predate the current classes, and outlaws versioned wire names like
`order.created.v2`. Rejected in favour of a warning plus an explicit opt-out,
which keeps the diagnostic without the breakage.

### Deriving the name at registration time only, leaving the field empty

`_resolve_event_type` already falls back to `event_class.__name__`, so the
registry alone would resolve correctly. But instances would serialize with
`event_type == ""`, stored rows would lose their type discriminator, and any
consumer reading `event.event_type` (logging, routing, `__str__`) would see an
empty string. Rejected: the field is part of the wire format, not just a registry
lookup key.

## References

- `src/eventsource/events/base.py` — `DomainEvent.__init_subclass__`,
  `DomainEvent._ensure_event_type`, `suppress_event_type_warning`
- `src/eventsource/events/registry.py` — `EventRegistry._resolve_event_type`,
  `EventRegistry.register`, `register_event`
- `tests/unit/test_event_type_auto.py` — auto-derivation, explicit-override,
  dict-construction, warning, and inheritance behaviour
- ADR-0006 — Event Registry and explicit registration
