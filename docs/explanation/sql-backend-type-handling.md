# SQL Backend Type Handling

When you append an event to `SQLiteEventStore` or `PostgreSQLEventStore`, the
event's payload is serialised with `event.model_dump(mode="json")` and stored as
JSON. When you read it back, the store looks the event class up in the
`EventRegistry`, parses the JSON, and calls
`event_class.model_validate(event_data, strict=False)`. Between those two steps
sits a piece of machinery that is easy to miss and, once you hit its edges, hard
to reason about without knowing it exists: a `TypeConverter` that walks the
decoded payload and turns selected strings back into `uuid.UUID` and `datetime`
objects, guessing which ones by *field name*.

This document explains why that step is there, how the default heuristic
decides, where it gets things wrong, and which of the several escape hatches
fits which situation. It is background reading rather than a task list — if you
just want to configure a store, the constructor arguments are summarised in
[Choosing a strategy for your domain](#choosing-a-strategy-for-your-domain), and
the full signatures live in the API reference for
`eventsource.stores`.

The short version: JSON has no UUID type and no datetime type, so a round-trip
through a SQL backend is lossy in a way the in-memory store is not. The library
recovers the lost type information by convention (`*_id` looks like a UUID,
`*_at` looks like a timestamp) rather than by inspecting your Pydantic model.
That convention is right for most domains and wrong for some, which is why
`DefaultTypeConverter` is configurable and the `TypeConverter` protocol is
replaceable. Everything below follows from that trade-off.

## The problem: JSON is a lossy encoding for event payloads

A `DomainEvent` is a Pydantic model whose fields carry real Python types:
`event_id: UUID`, `occurred_at: datetime`, `tenant_id: UUID | None`, plus
whatever your subclass adds. The moment that event is written to a SQL backend
those types stop existing. Both SQL stores serialise with
`event.model_dump(mode="json")` and then `json.dumps(...)` the result, and JSON
has exactly six value kinds — object, array, string, number, boolean, null.
UUIDs and datetimes are not among them, so both arrive on disk as strings.

That is not a defect of the library; it is what `mode="json"` is for. The
consequence is that the stored row is *not* a faithful image of the event. It is
a projection of the event into a smaller type system, and reading it back
requires reconstructing the information the projection dropped.

### What SQLite and PostgreSQL actually store

The two backends differ in the column type but not in the loss.

PostgreSQL stores the payload in a `JSONB` column (`payload JSONB NOT NULL` in
`src/eventsource/migrations/schemas/all.sql` and the `events.sql` template).
JSONB preserves the JSON type distinctions — object, array, string, number,
boolean, null — and nothing beyond them. A `UUID` field is a JSON string; a
`datetime` field is a JSON string in ISO-8601 form. Because the driver decodes
JSONB for you, `_deserialize_event` in `postgresql.py` accepts either shape:

```python
event_data = payload if isinstance(payload, dict) else json.loads(payload)
```

SQLite stores the payload in a `TEXT` column (`payload TEXT NOT NULL` in
`sqlite_all.sql`) and the store always parses it with `json.loads`. SQLite's own
type affinity system has no JSON, UUID, or timestamp type at all — the class
docstring on `SQLiteEventStore` spells out the mapping it uses: UUIDs as 36-char
hyphenated `TEXT`, timestamps as ISO-8601 `TEXT`, JSON as `TEXT`.

Either way, what comes back from the database before any reconstruction is a
plain `dict` of strings, numbers, booleans, lists, nested dicts, and `None`.

### Why `model_validate(..., strict=False)` is not enough on its own

Both stores finish with `event_class.model_validate(event_data, strict=False)`,
and lax mode does a lot of this work already: Pydantic will happily coerce the
string `"550e8400-…"` into a `UUID` for a field annotated `UUID`, and an
ISO-8601 string into a `datetime` for a field annotated `datetime`. If every
field in every event were annotated with a concrete type, the `TypeConverter`
would be close to redundant.

The gap is fields whose annotation does not tell Pydantic what to coerce *to*.
The base `DomainEvent` ships one such field outright — `metadata: dict[str, Any]`
— and domain events routinely add more: `dict[str, Any]` payload bags, lists of
loosely-typed records, `str | UUID` unions where the string member wins in lax
mode. `Any` instructs Pydantic to accept the value as-is, so a UUID that went
into `metadata` as a `UUID` comes back out as a `str`. Nothing raises; the event
validates cleanly and is simply wrong in a way that only shows up later, when a
comparison against a real `UUID` fails or a downstream projection writes a string
into a `uuid` column.

This is why the conversion pass runs *before* validation rather than relying on
it. `_deserialize_event` calls
`self._type_converter.convert_types(event_data)` on the
decoded dict first, so by the time Pydantic sees the data the UUID-shaped and
timestamp-shaped strings have already become objects — including the ones buried
inside `Any`-typed containers that Pydantic would have passed through untouched.
The trade-off is immediate and unavoidable: the converter is working on raw
JSON, with no access to the model's annotations at that point, so it has to
decide what to convert from the only signal available to it — the field name.

## Why the in-memory store does not need any of this

`InMemoryEventStore` has no `TypeConverter`, no `type_converter` constructor
argument, and no `with_strict_uuid_detection()` classmethod — and it is not
missing a feature. It never serialises anything. `_do_append_events` appends the
`DomainEvent` instance you handed it straight into a `dict[UUID,
list[DomainEvent]]`, and `_do_get_events` hands the same objects back out. There
is no `json.dumps`, no `model_dump`, and no `model_validate` anywhere in
`in_memory.py`; the module does not even import `json`. Nothing is projected
into JSON's six value kinds, so nothing has to be reconstructed afterwards.

The practical consequence is that the object you read is, for UUID and datetime
fields alike, the object you wrote — identical types, and in fact the very same
instance. A `UUID` tucked inside `metadata: dict[str, Any]` comes back a `UUID`,
because it was never anything else. This is exactly the case the field-name
heuristic exists to approximate for the SQL backends, and the in-memory store
gets it right by construction rather than by convention.

Sharing instances is safe here only because `DomainEvent` sets
`model_config = ConfigDict(frozen=True)`. A frozen Pydantic model cannot be
mutated after construction, so aliasing the stored event into a caller's hands
cannot corrupt the store. Without that guarantee, an in-memory store would have
to copy on read, and the copy would reintroduce a serialisation boundary of its
own.

The catch is that this makes `InMemoryEventStore` *more* permissive than the
backends it stands in for during tests. A field that the default converter would
misclassify — an external string id ending in `_id`, or a UUID field with a name
matching no pattern — behaves perfectly in memory and diverges only once the
same events pass through SQLite or PostgreSQL. Unit tests that use the in-memory
store therefore cannot detect type-handling regressions in the read path; the
`EventStore` conformance suite in `eventsource.testing.conformance` run against a
real SQL backend is where those show up. If your domain has fields the heuristic
is likely to get wrong, exercise them against a SQL store rather than trusting a
green in-memory suite.

One thing the in-memory store does share with the SQL backends is timestamp
validation: it calls `validate_timestamp` from `stores/_compat.py` on incoming
query bounds just as the others do. That is a separate concern from payload type
reconstruction — see
[A related but separate concern: timestamp validation in `_compat.py`](#a-related-but-separate-concern-timestamp-validation-in-_compatpy)
below.

## The reconstruction step: `TypeConverter`

The piece that puts the dropped types back is a small, deliberately narrow
abstraction living in `src/eventsource/stores/_type_converter.py` and re-exported
from `eventsource.stores` as `TypeConverter` (the contract) and
`DefaultTypeConverter` (the implementation both SQL stores use unless told
otherwise).

The contract is three methods:

```python
def convert_types(self, data: Any) -> Any: ...
def is_uuid_field(self, key: str) -> bool: ...
def is_datetime_field(self, key: str) -> bool: ...
```

`convert_types` is the one the stores call; the two predicates are the policy it
consults. That split is the whole design. Walking a decoded JSON structure is
mechanical and identical for everyone — recurse into dicts, recurse into lists,
try `UUID(value)` or `datetime.fromisoformat(...)` on strings, leave everything
else alone. *Which* strings to try it on is a domain question with no universal
answer. Keeping the traversal in `convert_types` and the judgement in
`is_uuid_field` / `is_datetime_field` means adapting the converter to your domain
usually costs a set of field names rather than a reimplementation.

Two properties of `convert_types` are worth stating outright because they shape
everything downstream. It does not mutate: it builds a new `dict` (or new
`list`) and returns it, leaving the decoded payload untouched. And it does not
raise: a value that a predicate flags as a UUID but that will not parse is
caught (`ValueError`, `AttributeError`) and written through unchanged. The
converter is a best-effort enrichment pass, not a validation gate — validation is
Pydantic's job a line later, and it is better positioned to do it because it
knows the model's annotations. The consequences of that choice, including the
ways it can quietly do the wrong thing, are taken up in
[Failure modes of the heuristic](#failure-modes-of-the-heuristic).

### Where conversion happens in the read path (`_deserialize_event`)

Both SQL stores funnel every read through a single private row-to-event method,
`_deserialize_event`, and the conversion sits in the middle of it. From
`postgresql.py`:

```python
# Get event class from registry
event_class = self._event_registry.get(event_type)

# Parse payload
event_data = payload if isinstance(payload, dict) else json.loads(payload)

# Convert string fields to proper types for Pydantic strict validation
event_data = self._type_converter.convert_types(event_data)

# Create event instance
return event_class.model_validate(event_data, strict=False)
```

`sqlite.py` is the same four steps with `json.loads(payload)` unconditional,
since its column is `TEXT`. Registry lookup, decode, convert, validate — in that
order, with nothing between convert and validate.

"Every read" is meant literally, and it is worth naming the callers because it
is what makes the converter's reach total. In both stores exactly four methods
call `_deserialize_event`: `_do_get_events` (the aggregate-stream read behind
`get_events`), `get_events_by_type`, `read_stream`, and `read_all`. Aggregate
rehydration, projection replays, and subscription catch-up all bottom out in one
of those four, so there is no path from a SQL row to a `DomainEvent` that skips
the conversion pass.

The ordering matters in both directions. Conversion runs *after* JSON decoding
because it needs Python containers to walk, not a string. It runs *before*
`model_validate` because that is the last moment at which anything can influence
what Pydantic sees; once validation has passed a `str` through an `Any`-typed
field, the information that it was ever meant to be a `UUID` is gone. And
because there is exactly one such choke point per store, the converter has
uniform reach: there is no read path that reconstructs types differently, and no
way to accidentally bypass it.

The corresponding write path has no counterpart. Appending calls
`event.model_dump(mode="json")` and lets Pydantic flatten UUIDs and datetimes to
strings; the converter is never consulted. The asymmetry is real and intentional
— serialisation is total and lossy, deserialisation is partial and
reconstructive — and it is the source of the divergence risk discussed under
[Silent divergence between write-side and read-side types](#silent-divergence-between-write-side-and-read-side-types).

### Why it is a Protocol rather than a base class

`TypeConverter` is declared as a `@runtime_checkable` `Protocol`, not an ABC, and
`DefaultTypeConverter` does not inherit from it — it simply has the three
methods. This follows the mixed convention described in the architecture notes:
Protocols where structural subtyping suffices, ABCs where an implementation needs
inherited behaviour. Nothing here does. There is no shared traversal to inherit,
no template method to override, no state a base class would manage; a converter
is a bag of three functions over field names and payloads.

Structural typing buys two things concretely. A replacement converter can be any
object with the right shape — including one you already have for other purposes,
or a thin adapter over your own schema registry — with no import of
`eventsource.stores` in its definition and no inheritance coupling it to a
library class. And because the protocol is `runtime_checkable`, `isinstance(obj,
TypeConverter)` works for defensive checks at wiring time, which is exactly what
the docstring example demonstrates:

```python
class MyConverter:
    def convert_types(self, data: Any) -> Any:
        return data  # no conversion
    def is_uuid_field(self, key: str) -> bool:
        return key.endswith("_uuid")
    def is_datetime_field(self, key: str) -> bool:
        return key.endswith("_timestamp")

isinstance(MyConverter(), TypeConverter)  # True
```

Be aware of the limit: `runtime_checkable` protocols check method *names* only,
not signatures or return types. `isinstance` will accept an object whose
`convert_types` takes the wrong arguments. It is a wiring smoke test, not a
guarantee — static type checking is what actually verifies the shape, and mypy
does check it at each store's `type_converter: TypeConverter | None` parameter.

## The default heuristic: field names as type hints

`DefaultTypeConverter` has to decide, for each key in a decoded payload, whether
its string value was once a `UUID`, once a `datetime`, or always a string. It
makes that decision from the key alone. The value is not inspected for
UUID-shaped-ness, the model's annotations are not consulted, and no schema is
recorded alongside the row at write time. The field *name* is treated as the
type hint.

That sounds flimsier than it is. Event-sourced Python codebases converge hard on
a small set of naming conventions — identifiers end in `_id`, timestamps end in
`_at` — and the base `DomainEvent` itself is written that way (`event_id`,
`aggregate_id`, `tenant_id`, `occurred_at`). A converter that keys off those
suffixes reconstructs the right types for the overwhelming majority of fields in
the overwhelming majority of domains, at zero configuration cost and with no
per-event bookkeeping in the schema. The alternative designs — storing a type
sidecar per payload, or reflecting over the Pydantic model before conversion —
each cost more than they return for the case the converter actually exists to
serve, which is the `Any`-typed fields Pydantic cannot coerce on its own.

The policy is four rules, split across the two predicates. `is_uuid_field`
consults, in order: an explicit allowlist (`_uuid_fields`) that always wins, a
denylist (`_string_id_fields`) that vetoes, and then the `_id`-suffix rule if
`_auto_detect_uuid` is on — falling through to `False`.

```python
if key in self._uuid_fields:
    return True

if key in self._string_id_fields:
    return False

return self._auto_detect_uuid and key.endswith("_id")
```

`is_datetime_field` is one line and has no configuration at all:

```python
return key == "occurred_at" or key.endswith("_at")
```

Both predicates are pure functions of a string. They do not see the value they
are about to gate, which is what makes the failure modes in the next section
possible — and also what makes the whole thing cheap enough to run over every
key of every payload on every read.

Note the ordering asymmetry between the two lists. The allowlist is checked
*before* the denylist, so a name in both is a UUID; and the denylist only ever
matters for names the suffix rule would otherwise catch. That is why
`DEFAULT_STRING_ID_FIELDS` exists as a companion to auto-detection rather than as
a standalone feature — turning auto-detection off makes most of it moot.

The four subsections below walk the pieces in the order the predicates consult
them: the always-UUID allowlist, the suffix rule and why it defaults on, the
denylist that keeps the suffix rule honest, and the separate datetime rule with
its ISO-8601 `Z` fix-up — followed by how `convert_types` carries all of this
into nested dicts and lists.
