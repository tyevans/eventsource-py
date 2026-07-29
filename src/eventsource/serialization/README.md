# Serialization

Reference for `eventsource.serialization` — the JSON encoding utilities used
wherever the library needs to write Python values into a text or JSONB column.

> **Note:** `json_dumps`/`json_loads` are now backed by `orjson`, a core
> dependency (not the stdlib-only `EventSourceJSONEncoder` path described
> below). See [`docs/reference/serialization-limits.md`](../../../docs/reference/serialization-limits.md)
> for the current encoder's constraints (integer range, non-finite floats,
> subclass handling, output format). The rest of this document predates that
> change and describes the prior stdlib-based implementation; it has not been
> updated to match.

The package is deliberately small: a single `json.JSONEncoder` subclass and two
thin wrapper functions. It depends only on the standard library (`json`,
`datetime`, `uuid`), so it can be imported from any layer without pulling in
SQLAlchemy, Pydantic, or a database driver.

Its purpose is narrow: make `UUID` and `datetime` values — which appear in
almost every event payload and metadata dict — serializable by `json.dumps`
without each call site registering its own `default=` hook. Internally it backs
the DLQ and outbox repositories, which persist event data as JSON strings.

```python
from uuid import uuid4
from datetime import UTC, datetime

from eventsource.serialization import json_dumps, json_loads

payload = {"event_id": uuid4(), "occurred_at": datetime.now(UTC)}
raw = json_dumps(payload)      # UUID -> str, datetime -> ISO 8601 str
parsed = json_loads(raw)       # plain json.loads: values come back as strings
```

Note that the round trip is lossy by design: `json_loads` performs no type
reconstruction. See *Asymmetry: json_loads Does Not Rehydrate* below.

## Exported Surface

`eventsource.serialization.__all__` names exactly three objects:

| Name | Kind | Signature | Purpose |
| --- | --- | --- | --- |
| `EventSourceJSONEncoder` | class, subclass of `json.JSONEncoder` | `default(self, obj: Any) -> Any` | Encoder hook that maps `UUID` and `datetime` to strings; delegates everything else to `json.JSONEncoder.default`. |
| `json_dumps` | function | `json_dumps(obj: Any) -> str` | `json.dumps(obj, cls=EventSourceJSONEncoder)`. No other keyword arguments are accepted or forwarded. |
| `json_loads` | function | `json_loads(s: str) -> Any` | Unmodified wrapper around `json.loads(s)`. |

Both `eventsource.serialization` and its implementation module
`eventsource.serialization.json` declare the same `__all__`, so
`from eventsource.serialization import ...` and
`from eventsource.serialization.json import ...` expose an identical surface.

### Re-export locations

Only `EventSourceJSONEncoder` is re-exported from the top-level package:

```python
from eventsource import EventSourceJSONEncoder  # listed in eventsource.__all__
```

`json_dumps` and `json_loads` are *not* in the top-level `__all__` — import them
from `eventsource.serialization`.

All three names are additionally re-exported from `eventsource.repositories`,
which is where the DLQ and outbox implementations consume them. See
*Deprecated Import Path* for the compatibility shim behind that re-export.

### Stability notes

- `json_dumps` takes no `indent`, `sort_keys`, `separators`, or `default`
  parameter. Call `json.dumps(obj, cls=EventSourceJSONEncoder, ...)` directly
  when you need those — `outbox.py` does exactly this in two places.
- `EventSourceJSONEncoder` overrides only `default`. All other encoder
  behavior (key coercion, `ensure_ascii`, circular-reference detection, NaN
  handling) is inherited unchanged from the standard library.

## Module Map

The package is two files. Everything of substance lives in `json.py`; the
package `__init__.py` is a pure re-export.

| Path | Lines | Contents |
| --- | --- | --- |
| `src/eventsource/serialization/__init__.py` | 26 | Package docstring plus `from eventsource.serialization.json import (EventSourceJSONEncoder, json_dumps, json_loads)` and the matching `__all__`. No logic. |
| `src/eventsource/serialization/json.py` | 100 | `EventSourceJSONEncoder`, `json_dumps`, `json_loads`, and the module `__all__`. Imports only `json`, `datetime.datetime`, `typing.Any`, and `uuid.UUID`. |

Note the module name shadows the stdlib `json` module within the package
namespace only — `json.py` itself does `import json`, which resolves to the
stdlib because Python 3 uses absolute imports. Inside other eventsource
modules, `import json` is likewise the stdlib; reach the library's encoder via
`from eventsource.serialization import ...`.

### Related modules outside the package

| Path | Relationship |
| --- | --- |
| `src/eventsource/__init__.py` | Re-exports `EventSourceJSONEncoder` only (line ~143, plus an `__all__` entry). |
| `src/eventsource/repositories/__init__.py` | Re-exports all three names from `eventsource.serialization` for repository consumers. |
| `src/eventsource/repositories/_json.py` | 44-line deprecation shim. Contains no implementation — a module-level `__getattr__` that warns and forwards to `eventsource.serialization.json`. See *Deprecated Import Path*. |
| `src/eventsource/repositories/dlq.py` | Imports `json_dumps`; uses it at three call sites to serialize `event_data` before insert. |
| `src/eventsource/repositories/outbox.py` | Imports both `EventSourceJSONEncoder` and `json_dumps`; uses `json_dumps` once and `json.dumps(..., cls=EventSourceJSONEncoder)` twice. |
| `tests/unit/test_json_encoder.py` | The package's entire test suite: encoder cases, `json_dumps`, `json_loads`, and the lossy round trip. |

## Supported Coercions

`EventSourceJSONEncoder.default` is a three-line dispatch. It adds exactly two
coercions on top of the standard library:

| Python type | Test | Output | JSON type |
| --- | --- | --- | --- |
| `uuid.UUID` | `isinstance(obj, UUID)` | `str(obj)` | string |
| `datetime.datetime` | `isinstance(obj, datetime)` | `obj.isoformat()` | string |
| anything else | — | `super().default(obj)` → `TypeError` | — |

Order matters only in the trivial sense that `UUID` is checked first; the two
types are disjoint, so no value can match both.

### UUID

`str(UUID(...))` produces the canonical lowercase hyphenated form
(`"3f2504e0-4f89-11d3-9a0c-0305e82c3301"`) regardless of how the UUID was
constructed — from hex, bytes, int, or a braced/URN string. Version, variant,
and `is_safe` are not preserved; the output is the 36-character text form only.

Because the check is `isinstance`, any `UUID` subclass is coerced the same way.

### datetime

`datetime.isoformat()` is called with no arguments, so the output follows the
stdlib defaults:

- Separator is `T`: `"2024-01-15T10:30:45"`.
- Microseconds are included **only when non-zero**:
  `datetime(2024, 1, 15, 10, 30, 45)` → `"2024-01-15T10:30:45"`, while
  `datetime(2024, 1, 15, 10, 30, 45, 123456)` →
  `"2024-01-15T10:30:45.123456"`. Both cases are covered by
  `tests/unit/test_json_encoder.py`.
- Timezone is appended as a UTC offset when the value is aware:
  `datetime.now(UTC)` → `"...+00:00"`. A naive `datetime` serializes with no
  offset suffix and no marker distinguishing it from an aware one — the
  encoder does not normalize, localize, or reject naive values. Pass aware
  datetimes if consumers need to recover the instant unambiguously.

### Types handled natively (no coercion involved)

The encoder does not touch values the standard library already serializes:
`str`, `int`, `float`, `bool`, `None`, `list`, `tuple`, and `dict`. These pass
through `json.dumps` without `default` ever being consulted, and survive a
`json_dumps` → `json_loads` round trip unchanged (except `tuple`, which becomes
a `list`). `test_encode_regular_types` asserts exactly this for a mixed dict.

Nesting is handled by `json.dumps` itself: `default` is invoked per value at any
depth, so `UUID`s and `datetime`s inside nested dicts and lists are coerced the
same as top-level ones.

### Coercion applies to values, not dict keys

`default` is only consulted for values. Dictionary *keys* go through
`json.JSONEncoder`'s separate key-coercion path, which accepts only `str`,
`int`, `float`, `bool`, and `None`. A `UUID`- or `datetime`-keyed dict therefore
raises `TypeError` despite those types being supported as values. Convert keys
yourself:

```python
json_dumps({str(aggregate_id): payload})  # not {aggregate_id: payload}
```

## Unsupported Types

Anything that is neither a `UUID`, a `datetime`, nor natively serializable falls
through to `super().default(obj)`, which raises `TypeError`. There is no
fallback to `str(obj)`, no `__dict__` introspection, and no registry hook — the
encoder fails loudly rather than writing an approximation into the database.

```python
class CustomClass:
    pass

json_dumps({"custom": CustomClass()})
# TypeError: Object of type CustomClass is not JSON serializable
```

`test_encode_unsupported_type_raises` asserts this behavior for an arbitrary
user-defined class.

### Notable types that are *not* handled

These are common in event payloads and each raises `TypeError`, despite two of
them living in the same stdlib modules as the supported types:

| Type | Why it fails |
| --- | --- |
| `datetime.date` | `date` is not a `datetime` subclass, so `isinstance(obj, datetime)` is `False`. (The reverse holds: `datetime` *is* a `date` subclass — but the encoder tests for `datetime`.) |
| `datetime.time` | Unrelated class; no branch matches. |
| `datetime.timedelta` | Unrelated class; no branch matches. |
| `decimal.Decimal` | Not handled by `json.JSONEncoder` either. |
| `bytes` / `bytearray` | No branch; `json` has no byte encoding. |
| `set` / `frozenset` | Not a `list`; `json` does not coerce iterables. |
| `enum.Enum` | Fails unless the member also subclasses `str` or `int`, in which case the stdlib serializes it as that primitive. |
| `pydantic.BaseModel` | Including `DomainEvent`. Call `model_dump(mode="json")` first — the encoder has no Pydantic awareness (and `serialization/` imports no Pydantic at all). |
| `None`-keyed nesting, dict keys of `UUID`/`datetime` | See *Coercion applies to values, not dict keys* above. |

### Handling them

Convert at the call site before serializing, rather than reaching for a broader
encoder:

```python
json_dumps(
    {
        "on": some_date.isoformat(),        # date  -> str
        "amount": str(some_decimal),        # Decimal -> str (or float, if lossy is ok)
        "tags": sorted(tag_set),            # set   -> list
        "event": event.model_dump(mode="json"),  # pydantic -> plain dict
    }
)
```

If a type genuinely belongs in the library's serialized surface, add a branch to
`EventSourceJSONEncoder.default` rather than pre-converting everywhere — see
*Extending the Encoder*.

### Non-type failures inherited from the stdlib

`default` is not the only source of `TypeError`. `json.dumps` itself raises for
circular references (`ValueError: Circular reference detected`) and, with the
default `allow_nan=True`, silently emits the non-standard literals `NaN`,
`Infinity`, and `-Infinity` for those float values. `json_dumps` does not
override `check_circular` or `allow_nan`, so both behaviors apply unchanged. If
a strict JSON consumer is downstream, call
`json.dumps(obj, cls=EventSourceJSONEncoder, allow_nan=False)` directly.

## Asymmetry: json_loads Does Not Rehydrate

`json_dumps` and `json_loads` are not inverses. The encoder narrows `UUID` and
`datetime` down to strings on the way out; nothing on the way back in widens
them again. `json_loads` is literally `return json.loads(s)` — no `object_hook`,
no `parse_float`/`parse_int` override, no type registry. The docstring states
this outright: reconstruction "is the application's responsibility."

```python
original = {"id": uuid4(), "timestamp": datetime.now(UTC)}
loaded = json_loads(json_dumps(original))

type(loaded["id"])         # <class 'str'>, not UUID
type(loaded["timestamp"])  # <class 'str'>, not datetime
loaded["id"] == str(original["id"])  # True — the assertion test_loads_roundtrip makes
```

`test_loads_roundtrip` in `tests/unit/test_json_encoder.py` pins exactly this:
it compares the loaded value against `str(original["id"])`, not against the
`UUID`.

### Why it is not symmetric

There is no information in the JSON text saying which strings *were* UUIDs or
datetimes. A generic `object_hook` would have to guess by pattern-matching every
string value, which would silently convert legitimate user data — an order
reference that happens to look like a UUID, a version string that parses as a
date — and would cost a regex pass over every field of every payload. The
library declines to guess. The schema lives in the caller's model, not in the
wire format, so the caller does the conversion.

### What round-trips cleanly, and what does not

| Value written | Value read back | Equal? |
| --- | --- | --- |
| `str`, `int`, `bool`, `None`, `dict` | same type | yes |
| `float` | `float` | yes (subject to IEEE-754 repr) |
| `tuple` | `list` | no — type changes |
| `UUID` | `str` (canonical 36-char form) | no — type changes |
| `datetime` | `str` (ISO 8601) | no — type changes |
| naive `datetime` | `str` with no offset | no — and awareness is unrecoverable |

Only the last row is genuinely lossy in content: an aware datetime keeps its
offset in the text and can be recovered exactly, while a naive one comes back
indistinguishable from an aware-in-local-time value that happened to be
formatted without an offset. Prefer aware datetimes for anything persisted.

### Rehydrating at the call site

Convert explicitly against the schema you already have:

```python
from datetime import datetime
from uuid import UUID

raw = json_loads(row["event_data"])
event_id = UUID(raw["event_id"])
occurred_at = datetime.fromisoformat(raw["occurred_at"])
```

`datetime.fromisoformat` accepts everything `datetime.isoformat()` emits, so the
pair is exact for datetimes; `UUID(str)` likewise accepts the canonical form.

In practice, prefer letting Pydantic do it. `DomainEvent` subclasses declare
`UUID` and `datetime` fields, and `model_validate` coerces the strings back:

```python
event_class = registry.get_event_class(record.event_type)
event = event_class.model_validate(json_loads(record.event_data))
```

This is the path the library itself documents in
`eventsource/events/registry.py` — the registry resolves a type name to a class
and Pydantic handles the per-field coercion. Hand-rolled `UUID(...)` /
`fromisoformat(...)` calls are for dicts that have no model behind them, such as
free-form metadata.

### Where this surfaces in the library

The DLQ and outbox repositories store `event_data` / `payload` as JSON produced
by `json_dumps`, but their read methods (`DeadLetterRecord`, outbox fetches)
return the column value as-is — `event_data: str | dict[str, Any]` on
`DeadLetterRecord` — without calling `json_loads` at all. Nothing in
`src/eventsource/` calls `json_loads`; it exists for consumers. Code that
reprocesses a dead-lettered event is responsible for parsing the string and
reconstructing the event.

## Extending the Encoder

There are two ways to widen the set of serializable types: subclass the encoder
locally, or add a branch to `EventSourceJSONEncoder.default` in the library.
Which one is correct depends on whether the type belongs to your application or
to eventsource's own persisted surface.

### Subclassing (application-local types)

`default` is a plain method, so a subclass can handle its own types first and
defer the rest:

```python
from decimal import Decimal

from eventsource.serialization import EventSourceJSONEncoder


class AppJSONEncoder(EventSourceJSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return super().default(obj)  # UUID / datetime, then TypeError
```

The `super().default(obj)` call is what preserves the inherited `UUID` and
`datetime` coercions — drop it and you lose them. Order your branches before the
`super()` call; the base class raises `TypeError` rather than returning, so
nothing after it runs.

The important caveat: **`json_dumps` will not use your subclass.** It hardcodes
`cls=EventSourceJSONEncoder` and accepts no other arguments, so an extended
encoder has to go through `json.dumps` directly:

```python
import json

raw = json.dumps(payload, cls=AppJSONEncoder)
```

If you want a drop-in replacement, write your own one-line wrapper alongside the
subclass rather than trying to reconfigure `json_dumps`.

### Adding a branch to the library encoder

Add a type here only when the library itself needs to persist it — anything
written by the DLQ or outbox repositories, or anything that could appear in an
event payload built from eventsource's own types. Application-specific types
belong in a subclass.

The change is three edits plus tests:

1. **`src/eventsource/serialization/json.py`** — insert an `isinstance` branch in
   `default`, before the `return super().default(obj)` line:

   ```python
   if isinstance(obj, Decimal):
       return str(obj)
   ```

2. **Docstrings** — update the class docstring's bulleted list of supported
   types. It is the only in-code description of the encoder's contract.

3. **This README** — add the row to *Supported Coercions* and remove it from the
   *Notable types that are not handled* table in *Unsupported Types*.

4. **`tests/unit/test_json_encoder.py`** — add a case to
   `TestEventSourceJSONEncoder` following the existing shape (encode via
   `json.dumps(data, cls=EventSourceJSONEncoder)`, assert on the resulting
   string), and a round-trip case in `TestJsonLoads` if the coercion is lossy.
   There is a second, older suite at `tests/unit/serialization/test_json.py`
   covering the backward-compatible import path; new behavior belongs in the
   canonical file.

No change to `__all__` or to the re-exports in `eventsource/__init__.py` and
`eventsource/repositories/__init__.py` is needed — a new branch does not add a
name.

### Choosing an output form

Two rules keep the encoder honest:

- **Emit something `json.loads` can hand back to a constructor.** `UUID` and
  `datetime` both encode to a string that their standard parser
  (`UUID(...)`, `datetime.fromisoformat(...)`) accepts verbatim. Prefer that
  over a bespoke dict or a `repr()`, so callers rehydrating by hand have an
  obvious inverse — see *Rehydrating at the call site*.
- **Do not encode type tags.** The package has no `object_hook` and no plan for
  one; a `{"__type__": "Decimal", "value": "1.5"}` envelope would round-trip only
  through a decoder that does not exist, and would break every consumer reading
  the column as plain JSON.

Lossy coercions are acceptable when the loss is documented — `datetime` already
discards the `tzinfo` object in favor of an offset, and `UUID` discards version
and variant metadata.

### What not to extend

- **Do not add a `str(obj)` catch-all.** The `TypeError` from
  `super().default(obj)` is the mechanism that stops an unintended object from
  being written to the database as `"<myapp.Thing object at 0x...>"`. A fallback
  turns a loud failure at write time into silent corruption discovered at read
  time.
- **Do not add Pydantic handling.** `serialization/` imports only `json`,
  `datetime`, `typing`, and `uuid`, and that stdlib-only footprint is what lets
  every layer import it. Call `model_dump(mode="json")` at the call site instead.
- **Do not add parameters to `json_dumps`.** Its value is being a fixed,
  predictable one-argument call used identically across `dlq.py` and
  `outbox.py`. Call sites needing `indent`, `sort_keys`, or `allow_nan=False`
  should use `json.dumps(obj, cls=EventSourceJSONEncoder, ...)` — which is
  already the pattern at `outbox.py:282` and `outbox.py:854`.

## Invariants

Properties that hold for every use of this package. They are what other layers
are allowed to rely on; a change that breaks one is a breaking change.

### Dependency footprint

1. **Stdlib only.** `json.py` imports exactly `json`, `datetime.datetime`,
   `typing.Any`, and `uuid.UUID`. No Pydantic, no SQLAlchemy, no driver, no
   other eventsource module. The package is therefore importable from any
   layer — including core domain modules that must stay infrastructure-free —
   without creating a cycle or pulling in an optional dependency.
2. **No import-time side effects.** Neither module registers anything, reads
   configuration, or touches global state. Importing `eventsource.serialization`
   defines one class and two functions and nothing else.

### Encoder behavior

3. **`default` handles exactly two types.** `UUID` → `str(obj)` and `datetime`
   → `obj.isoformat()`. Everything else reaches `super().default(obj)`.
4. **Unsupported types raise, never degrade.** There is no `str()` fallback and
   no silent placeholder; `TypeError` propagates to the caller. A value either
   serializes faithfully or the write fails.
5. **Only `default` is overridden.** Key coercion, `ensure_ascii`,
   `check_circular`, `allow_nan`, `indent`, and `sort_keys` behave exactly as
   `json.JSONEncoder` defines them. `EventSourceJSONEncoder` is substitutable
   anywhere a `json.JSONEncoder` is expected.
6. **Coercion is value-only and depth-independent.** `default` is consulted for
   values at any nesting depth and never for dict keys.
7. **`json_dumps` output is always valid input to `json_loads`.** Both use the
   stdlib codec unmodified, so anything the encoder produces parses back.
   (`allow_nan` is the stdlib default, so `NaN`/`Infinity` may appear —
   `json.loads` accepts them, stricter third-party parsers may not.)

### Function contracts

8. **`json_dumps` is total on its accepted domain and fixed in shape.** One
   positional argument, returns `str`, no keyword arguments, always
   `cls=EventSourceJSONEncoder`. Callers needing other `json.dumps` options
   call `json.dumps` directly — `outbox.py:282` and `outbox.py:854` do.
9. **`json_loads` is `json.loads`.** No `object_hook`, no `parse_float`/
   `parse_int`/`parse_constant` override, no type reconstruction. Adding one
   would change the meaning of every stored payload and is out of scope.
10. **The pair is not an isomorphism.** `json_loads(json_dumps(x)) == x` holds
    only for values built from `str`, `int`, `float`, `bool`, `None`, `list`,
    and `dict`. `tuple`, `UUID`, and `datetime` change type on the way back.
    See *Asymmetry: json_loads Does Not Rehydrate*.

### Surface stability

11. **`__all__` is the same three names in both modules.**
    `eventsource.serialization` and `eventsource.serialization.json` export
    `EventSourceJSONEncoder`, `json_dumps`, and `json_loads` — nothing more.
12. **Re-export asymmetry is intentional.** The top-level `eventsource` package
    exports only `EventSourceJSONEncoder` (`__init__.py:143`, `__all__:289`);
    `eventsource.repositories` re-exports all three
    (`repositories/__init__.py:66-68`, `98-100`). Do not "fix" this by widening
    the top-level surface without a deliberate API decision.
13. **Nothing inside `src/eventsource/` calls `json_loads`.** The library only
    writes JSON (`dlq.py` at lines 346, 732, 1104; `outbox.py` at 282, 568,
    854). Read paths return the stored column value as-is, so parsing and
    rehydration are consumer responsibilities by design, not an oversight.
