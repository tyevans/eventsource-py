# SQL Backend Type Handling

When you append an event to the SQLite or PostgreSQL adapters, the event's
payload is serialised with `event.model_dump(mode="json")` and stored as JSON
(`payload JSONB` on PostgreSQL, `payload TEXT` on SQLite). When you read it
back, the adapter looks the event class up in the `EventRegistry`, decodes the
JSON with `json_loads`, and calls `event_class.model_validate(data)` directly
on the result. There is no intermediate reconstruction step: `model_validate`
against the event's own Pydantic annotations is the entire read path.

This document explains why that is enough, what it depends on the event class
providing, and what changes for someone whose events were written against an
earlier design that guessed types from field names at read time.

## The problem: JSON is a lossy encoding for event payloads

A `DomainEvent` is a Pydantic model whose fields carry real Python types:
`event_id: UUID`, `occurred_at: datetime`, `tenant_id: UUID | None`, plus
whatever your subclass adds. The moment that event is written to a SQL
backend those types stop existing. Both adapters serialise with
`event.model_dump(mode="json")`, and JSON has exactly six value kinds —
object, array, string, number, boolean, null. UUIDs and datetimes are not
among them, so both arrive on disk as strings.

That is not a defect; it is what `mode="json"` is for. The consequence is
that the stored row is not a faithful image of the event — it is a
projection into a smaller type system, and reading it back requires
reconstructing the information the projection dropped.

### What SQLite and PostgreSQL actually store

The two backends differ in column type but not in the loss. PostgreSQL
stores the payload in a `JSONB` column; the driver decodes it for you, so
`_deserialize_event` in `src/eventsource/adapters/postgresql/store.py`
accepts either a `dict` (already decoded) or a JSON string:

```python
event_class = self._event_registry.get(event_type)
data = payload if isinstance(payload, dict) else json_loads(payload)
return event_class.model_validate(data)
```

SQLite stores the payload in a `TEXT` column and always parses it with
`json_loads` (`src/eventsource/adapters/sqlite/store.py`):

```python
event_class = self._event_registry.get(event_type)
data = json_loads(payload)
return event_class.model_validate(data)
```

`json_loads` is backed by `orjson`. It decodes JSON's six value kinds
faithfully and fast, but — as its docstring states explicitly — "UUID and
datetime strings are NOT automatically converted back to their original
types." Whatever comes back from `json_loads` is a plain `dict` of strings,
numbers, booleans, lists, nested dicts, and `None`. Reconstructing the real
types is left entirely to the call that follows: `model_validate`.

### Why `model_validate` is enough on its own — when fields are typed

Pydantic v2's coercion at the JSON boundary is doing all the work here, and
it is capable of more than it might look. For any field annotated with a
concrete type, `model_validate` on a `dict` of JSON-shaped values will:

- coerce a hyphenated 36-character string into `uuid.UUID` for a
  `UUID`-annotated field,
- coerce an ISO-8601 string into `datetime` (including a trailing `Z`) for
  a `datetime`-annotated field,
- coerce a numeric string or `str` into `Decimal` for a `Decimal`-annotated
  field,
- recursively validate a nested `dict` into a declared Pydantic sub-model,
  applying the same coercion to *its* fields.

This is why the base `DomainEvent` fields — `event_id: UUID`,
`occurred_at: datetime`, `aggregate_id: UUID`, `tenant_id: UUID | None` —
round-trip through either SQL backend with no special handling anywhere in
the adapters. The annotation on the model *is* the schema; there is nothing
else to consult.

### Where it stops working: untyped payload fields

The gap is fields whose annotation does not tell Pydantic what to coerce
*to*. The base `DomainEvent` ships one such field outright —
`metadata: dict[str, Any]` — and a domain event that adds a bare
`dict[str, Any]` payload bag inherits the same problem: `Any` instructs
Pydantic to accept the value as-is, so a UUID that went into that field as a
`UUID` at write time comes back out as a `str` after the JSON round-trip.
Nothing raises; the event validates cleanly and is simply wrong in a way
that only shows up later — a comparison against a real `UUID` fails, or a
downstream projection writes a string into a `uuid` column.

## The old design: guessing types from field names

An earlier version of this library shipped a small helper, part of the
legacy synchronous event store package now retired, that ran between JSON
decoding and `model_validate`. It walked the decoded `dict` and tried to
turn selected strings back into `UUID` and `datetime` objects, deciding
which strings to touch by *field name* — an `_id` suffix meant "try
`UUID(value)`", an `_at` suffix meant "try `datetime.fromisoformat(value)`"
— with an allowlist and denylist to override the default guess per field.

That design's whole reason to exist was to patch over exactly the gap
described above: `Any`-typed containers that Pydantic itself has no
annotation to coerce against. It worked most of the time, because
event-sourced codebases do converge on `_id`/`_at` naming conventions. But
it was a convention-based patch over a real problem, and it came with real
costs:

- It was blind to the actual value's shape — a UUID-shaped string in a
  field that didn't match the naming convention was silently left as a
  `str`, and a non-UUID string that happened to end in `_id` was tried and
  discarded (best-effort, never raised) — so failures were silent either
  way.
- It operated on raw JSON with no access to the model's annotations, so it
  could not know it was wrong, only guess.
- It only ran on SQL-backed reads. `InMemoryEventStore` never serialised at
  all, so the same fields round-tripped correctly there and only diverged
  once the same events hit a SQL backend — a gap unit tests using the
  in-memory store could not see.

The legacy `EventStore` ABC and its supporting package, including that
field-name-guessing helper, have been retired along with this design. The
adapters in `src/eventsource/adapters/postgresql/` and
`src/eventsource/adapters/sqlite/` do not call anything like it — the
`model_validate` call shown above is the complete read path.

## The current approach: typed sub-models instead of guessing

The fix for the underlying gap is not a smarter guesser — it is giving
Pydantic something to coerce against. If a payload field carries structured
data with real types (UUIDs, timestamps, decimals, nested identifiers),
declare a Pydantic sub-model for it on the event class instead of typing it
`dict[str, Any]`:

```python
from decimal import Decimal
from datetime import datetime
from uuid import UUID

from pydantic import BaseModel

from eventsource.events import DomainEvent


class LineItem(BaseModel):
    sku: str
    quantity: int
    unit_price: Decimal
    added_at: datetime


class OrderPriced(DomainEvent):
    aggregate_type: str = "Order"
    order_number: str
    customer_id: UUID
    line_items: list[LineItem]
    priced_by: UUID
```

With `line_items: list[LineItem]` instead of `line_items:
list[dict[str, Any]]`, `model_validate` recurses into each item and coerces
`unit_price` to `Decimal` and `added_at` to `datetime` on every read, on
both backends, with no adapter-side involvement at all. The type
information lives in one place — the event class — instead of being
re-derived from field-name heuristics at every read.

This also fixes the asymmetry the old design had: serialisation
(`model_dump(mode="json")`) and deserialisation (`model_validate`) are now
mirror images of the same schema, because both are driven by the same
Pydantic annotations. There is no separate convention that write and read
paths can silently drift apart from.

### What changes if you relied on the old guessing behavior

If your events previously used `dict[str, Any]` fields and depended on the
old `_id`/`_at` suffix convention to get UUIDs and datetimes back on read,
that reconstruction no longer happens. Concretely:

1. **Fields that still validate but are silently wrong.** A `dict[str,
   Any]` field holding a `UUID` under a key like `customer_id` will now
   come back as a plain `str` after a round-trip through SQLite or
   PostgreSQL. Nothing raises — the event validates — so this needs to be
   caught by re-checking assumptions about payload field types, not by
   waiting for a validation error.
2. **The fix is to add type annotations**, not to reimplement the
   guesser. Replace the untyped container with either a concrete field
   type (`UUID`, `datetime`, `Decimal`) or a nested Pydantic sub-model, as
   shown above.
3. **`metadata: dict[str, Any]` on the base `DomainEvent` is unaffected**
   by this change and was never covered by field-name guessing for keys
   inside it in a fully reliable way either — treat values placed there as
   opaque, JSON-safe data (strings, numbers, booleans, nested dicts/lists),
   not as a place to round-trip typed Python objects.
4. **`InMemoryEventStore` behavior is unchanged and remains the strictest
   signal.** It never serialises at all, so it returns the exact objects
   you wrote, typed fields or not. It was already the case that the
   in-memory store could not catch type round-trip regressions the SQL
   backends would hit — that is still true, and is still the reason to
   exercise payload-heavy events against a real SQL backend (or the
   `EventStore` conformance suite in `eventsource.testing.conformance`)
   rather than trusting a green in-memory suite alone.

## Summary

- Serialisation is `event.model_dump(mode="json")`; deserialisation is
  `event_class.model_validate(data)` on the `json_loads`-decoded payload.
  That is the whole pipeline on both SQL backends — no intermediate
  type-guessing step exists anymore.
- Pydantic v2 coerces JSON-shaped values (strings, numbers) into `UUID`,
  `datetime`, `Decimal`, and nested models automatically, for any field
  that carries a concrete annotation.
- `Any`-typed or untyped `dict` payload fields are where information is
  still lost: Pydantic has nothing to coerce against, so a UUID or
  datetime placed there comes back as a plain string.
- The fix is structural, not conventional: declare a typed Pydantic
  sub-model (or a concrete field type) for any payload data that needs to
  round-trip as UUIDs, timestamps, or decimals — not a field-name
  convention for the store to guess from.
