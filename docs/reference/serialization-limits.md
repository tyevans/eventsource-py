# Serialization limits

`eventsource.serialization.json_dumps`/`json_loads` are backed by
[orjson](https://github.com/ijl/orjson), a core dependency of this library (not
optional). The constraints below are the limits of **orjson as this library's
encoder** — not a general statement about JSON. They matter because every
value that reaches `json_dumps` is a candidate for persistence: this is the
encoding used to write event payloads and metadata into the `event_outbox` and
`dead_letter_queue` tables. A value that can't be encoded today can't be
written today; a value that encodes *differently* than you expect is a
correctness bug in whatever reads it back.

## Integer range

Integers must be within `[-2**63, 2**64-1]` (`-9223372036854775808` to
`18446744073709551615`). orjson only supports 64-bit integers; outside that
range, `json_dumps` raises:

```
ValueError: Integer out of range for JSON serialization (must be within [-2**63, 2**64-1])
```

Rationale: orjson itself raises a bare `TypeError: Integer exceeds 64-bit
range` for the same input, with no indication of which value or field caused
it, and no way to distinguish "an int was too big" from any other cause.
`json_dumps` catches that specific `TypeError` and re-raises `ValueError` with
the supported range stated, so the failure mode is at least documented rather
than an opaque orjson internal. This is done as **error translation after the
fact**, not a pre-emptive scan of the payload -- see "How this is enforced"
below for why that distinction is load-bearing. (The message does not name
the offending value: orjson's own exception doesn't carry it, so there is
nothing to recover and pass through.)

This check applies everywhere orjson itself traverses the structure -- top
level, nested inside `dict`/`list` (including subclasses of those, since
orjson serializes those subclasses natively too), and to `int` subclasses
holding an out-of-range value (e.g. an `IntEnum` member defined with too
large a value). `bool` is never treated as an out-of-range candidate: it's
technically an `int` subclass in Python, but it always serializes as
`true`/`false`, never as a number, so the question doesn't apply to it.

## Non-finite floats: `json_dumps` does NOT reject these

**This is the one constraint on this page that changed direction, and it is
easy to get wrong if you only skim.** `json_dumps` itself does not check for
`inf`, `-inf`, or `nan` at all. Passed directly, they silently become JSON
`null`:

```python
>>> json_dumps({"v": float("inf")})
'{"v":null}'
```

This is orjson's own behavior, unmodified. An earlier version of this
encoder pre-scanned every payload to catch and reject this before calling
orjson, but that scan made `json_dumps` **14.5x slower than raw
`orjson.dumps`** on a realistic event payload -- slower, in fact, than the
stdlib `json` encoder this library replaced by taking on orjson as a core
dependency in the first place. The scan was deleted.

**Protection moved upstream instead, to `DomainEvent`.**
`DomainEvent.model_config` sets `allow_inf_nan=False`, so pydantic itself
rejects a non-finite float with `ValidationError` at **event construction
time** -- before serialization is ever reached:

```python
>>> class OrderPlaced(DomainEvent):
...     amount: float
...
>>> OrderPlaced(aggregate_id=..., aggregate_type="Order", amount=float("inf"))
ValidationError: ... Input should be a finite number [type=finite_number, ...]
```

This covers every event in the system -- strictly better than the old scan,
since it fires at the earliest possible point instead of the latest, and
applies uniformly regardless of which repository or serialization call site
eventually handles the event.

**Explicit residual risk, not a hidden one: a non-finite float in anything
that is NOT a `DomainEvent` is unprotected.** A hand-built `dict` passed
straight to `json_dumps`, DLQ error metadata, or any other payload
constructed outside pydantic validation will silently serialize a
non-finite float as `null`, with no error anywhere. If you build payloads
by hand and they might contain a non-finite float, validate that yourself
before calling `json_dumps` -- the library will not catch it for you.

## `float` subclasses (e.g. `numpy.float64`)

A `float` subclass -- finite or non-finite -- behaves exactly like a plain
`float`: converted to a plain `float` and serialized (finite), or silently
turned into `null` (non-finite). This needs calling out because it is *not*
orjson's default behavior for the type itself: orjson serializes
`str`/`int`/`dict`/`list` subclasses natively via their base-type behavior,
but not `float` subclasses -- those would otherwise raise `TypeError: Type
is not JSON serializable: <ClassName>` even for an ordinary finite value.
`json_dumps` handles the type-conversion case explicitly, but deliberately
does **not** try to special-case non-finite subclass values to raise:
verified by direct execution that `orjson.dumps` discards any exception
raised inside its `default=` callback and replaces it with a generic
message, so there was never a way to signal "this specific value is
non-finite" back through that hook. See the `_orjson_default` docstring in
`eventsource/serialization/json.py` for the full account of why that design
was tried and abandoned.

## `dict` / `list` subclasses: supported. `tuple` subclasses: NOT supported

A `dict` or `list` **subclass** serializes natively via orjson, same as the
base type -- fully traversed, nothing skipped.

A `tuple` **subclass**, however, is not serializable at all, regardless of
its contents:

```python
>>> class MyTuple(tuple): pass
>>> json_dumps({"a": MyTuple([1, 2, 3])})
TypeError: Object of type MyTuple is not JSON serializable
```

This is unrelated to any of the constraints above -- it's a general orjson
limitation on the `tuple` type specifically (`list` and `dict` subclasses
are fine; `tuple` subclasses are not). A plain `tuple` (not a subclass)
still serializes fine, as a JSON array.

## How this is enforced (for anyone auditing this list against the code)

- **Integer range** is enforced by catching and translating orjson's own
  `TypeError` in `json_dumps`. This costs nothing on the success path --
  it's a `try/except`, not a pre-scan.
- **Non-finite floats** are enforced upstream, at `DomainEvent` construction
  (pydantic's `allow_inf_nan=False`), not inside `json_dumps` at all.
- There is **no pre-serialization walk of the payload** for either
  constraint anymore. If you're reading older code, docs, or commit history
  that describes a scan-based implementation (`_validate_json_safe_values`
  / `_reject_non_finite_floats`), that was deleted for the performance
  reason described above.

## Non-`str` dict keys

An `int` key stringifies (`{1: "a"}` -> `{"1":"a"}`), matching how a plain
`int` key behaves in ordinary `dict`-to-JSON conversion. A `UUID` key also
stringifies -- via `orjson.OPT_NON_STR_KEYS` -- to its string form
(`{uuid4(): "a"}` -> `{"<uuid-string>":"a"}`). Any other non-`str`,
non-`int`, non-`UUID` key type is unsupported and raises.

## Output format

- **Compact.** No whitespace between tokens (`{"a":1}`, not `{"a": 1}`).
- **Raw UTF-8.** Non-ASCII characters (accented Latin, CJK, Cyrillic, emoji
  including those outside the Basic Multilingual Plane) are emitted as their
  literal UTF-8 bytes, never `\uXXXX`-escaped.
- **`bool` as `true`/`false`**, never as `0`/`1` or any other numeric
  representation, regardless of the fact that `bool` is an `int` subclass in
  Python.

## Unsupported types

Anything not covered above and not natively handled by orjson (UUID,
datetime, the types listed above) raises:

```
TypeError: Object of type <ClassName> is not JSON serializable
```

This includes, for example, `set`, `frozenset`, `Decimal`, generators, and
arbitrary custom objects with no special handling. If you need one of these
supported, convert it to a plain `dict`/`list`/`str`/`int`/`float` before
calling `json_dumps`.

## `json_loads`

`json_loads` is a thin wrapper over `orjson.loads` and performs no type
reconstruction: a UUID or datetime that was serialized comes back as a plain
`str`. Rehydrating those types, if needed, is the caller's responsibility.

## See also

- `serialization/` README (`src/eventsource/serialization/README.md`) —
  package overview and usage.
- `eventsource.serialization.json` module docstring — the same constraints,
  documented at the source.
