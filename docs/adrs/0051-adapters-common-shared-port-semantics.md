# 0051. `adapters/_common/` Holds Port Semantics No Adapter Should Re-Derive

`check_expected` — the function that decides whether a stream's current
version satisfies the caller's `ExpectedVersion` — existed verbatim in three
store adapters and one testing double. Nothing would have failed if one copy
had drifted.

## Status

**Accepted.**

**ADR Impact**

| ADR | Status |
| --- | --- |
| [0025](0025-legacy-store-retirement.md) | Stands. The store ports and the per-technology adapter split it decided are untouched; this only says where logic shared *across* those adapters lives. |
| [0031](0031-bus-ring-split.md) | Stands, and supplies the pattern. `adapters/_bus/` is the transport-specific case of the same idea; `_common/` is the case where the shared code is specific to nothing. |

## Context

A store adapter's `append` must answer one question before it writes: does the
stream's version satisfy what the caller asserted? The answer depends on
`ExpectedVersion`, `OptimisticLockError`, and the convention that a
non-existent stream is version 0. It depends on nothing about rows, files, or
dicts.

Each adapter nonetheless carried its own copy — `InMemoryEventStore`,
`SQLiteEventStore`, `PostgreSQLEventStore`, and `PartitionedMemoryStore`, the
last with a docstring explaining that the copy was deliberate, because
`testing/` must not depend on a specific adapter. That reasoning was sound and
the conclusion still wrong: the alternative to depending on *one adapter* is
not copying, it is depending on something that belongs to none of them.

The conformance suite asserts the four adapters agree. It does not stop them
from deriving that agreement four times, and the `read_category`
batch-timestamp tie-break had already shown what independent derivation of a
conformance-asserted rule produces. This is recurring defect shape #1 caught
one step before it becomes a defect: not two implementations that disagree,
but four that agree only as long as nobody edits one.

## Decision

Add **`adapters/_common/`**, a third adapters-internal package alongside
`_sql/` (dialect-specific) and `_bus/` (transport-specific). What lands here is
shared by adapters with no technology in common, because it is port semantics
rather than storage mechanics. `check_expected` and `describe_expected` move
there; all four call sites import them.

The standing rule this records: **behavior a conformance suite asserts is
implemented once.** A conformance suite proves the adapters agree; it is not a
license for each to derive the agreement separately. When logic depends only on
ports and domain types, it belongs in `_common/`.

`_common/` is adapters-internal, not public API — the leading underscore says
so, and users get these behaviors through the adapters. It sits in the adapters
ring rather than `ports/`, which contains no implementation code, ever.

`testing/partitioned_memory.py` imports it. That does not violate the rule its
docstring invoked: `testing/` must not depend on a *specific adapter*, and
`_common/` is by construction the code that belongs to no adapter.

## Consequences

One definition to change when the semantics change, and one place a reviewer
has to look to know what they are. A new store adapter gets the behavior by
importing it rather than by copying the nearest sibling, which is how three of
the four copies came to exist.

`describe_expected` is used by only the two SQL adapters, whose
unique-constraint violation surfaces after the insert rather than at the
pre-check. It moves anyway: it is the same kind of fact, and leaving it
duplicated across two adapters while consolidating its neighbor would be
arbitrary.

The package is a place to put things, which is a mild invitation to put the
wrong things there. The boundary is dependency-shaped and checkable: if it
imports a driver or a dialect, it belongs in `_sql/`, `_bus/`, or the adapter
itself.
