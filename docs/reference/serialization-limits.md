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
ValueError: Integer out of range for JSON serialization (must be within [-2**63, 2**64-1]): <value>
```

Rationale: orjson itself raises a bare `TypeError: Integer exceeds 64-bit
range` for the same input, with no indication of which value or field caused
it. `json_dumps` checks first and raises a message that names the offending
value, so the failure is traceable back to its source instead of surfacing as
an opaque error deep inside serialization.

This check applies everywhere in the structure — top level, nested inside
`dict`/`list`/`tuple` (including subclasses of those), and to `int` subclasses
holding an out-of-range value (e.g. an `IntEnum` member defined with too large
a value). `bool` is never checked against this range: it's technically an
`int` subclass in Python, but it always serializes as `true`/`false`, never as
a number.

## Non-finite floats

`inf`, `-inf`, and `nan` raise:

```
ValueError: Out of range float values are not JSON compliant: <value>
```

Rationale: orjson has no option to reject or specially encode non-finite
floats — it silently converts every one of them to JSON `null`. For a
persisted event payload, that's data corruption: the field still parses as
valid JSON, but the value is now indistinguishable from an intentional
`null`, and the original number is gone with no error raised anywhere.
`json_dumps` checks first so this fails loudly instead of silently.

This check applies everywhere in the structure, including inside `dict`/
`list`/`tuple` subclasses, and to `float` subclasses (e.g. `numpy.float64`)
holding a non-finite value.

## `float` subclasses

A `float` subclass with a finite value (e.g. `numpy.float64(3.14)`)
serializes correctly, converted to a plain `float` first. This needs calling
out because it is *not* orjson's default behavior: orjson serializes
`str`/`int`/`dict`/`list` subclasses natively via their base-type behavior,
but not `float` subclasses — those would otherwise raise
`TypeError: Type is not JSON serializable: <ClassName>` for an ordinary
finite value. `json_dumps` handles this case explicitly so a `float`
subclass behaves exactly like a plain `float`, whether finite (converts and
serializes) or non-finite (raises, per the rule above).

## `dict` / `list` / `tuple` subclasses

Fully supported: traversed and validated the same as the base types, not
skipped. This matters because orjson serializes such a subclass natively via
its base-type behavior — so a non-finite float or out-of-range integer
inside one is just as reachable, and just as much a correctness risk, as one
inside a plain `dict`/`list`/`tuple`.

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

- [`serialization/` README](../../src/eventsource/serialization/README.md) —
  package overview and usage.
- `eventsource.serialization.json` module docstring — the same constraints,
  documented at the source.
