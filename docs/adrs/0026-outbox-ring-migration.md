# 0026. Outbox Ring Migration

The transactional outbox was the last of the three `repositories/` modules
mixing a Protocol definition with sqlalchemy-backed implementations in the
same file — the defect ADR 0019 fixed for the event store and ADR 0024 fixed
for checkpoints and the dead letter queue. This ADR completes the set: the
outbox Protocol and its value objects move to `ports/outbox.py`, its three
backend implementations move to per-technology modules under `adapters/`,
and `src/eventsource/repositories/` is deleted.

## Status

Accepted. Sibling of [ADR 0024](0024-projection-persistence-ports.md) — the
same Protocol/implementation split, applied to the outbox. Implemented in
`src/eventsource/ports/outbox.py`, `src/eventsource/adapters/memory/outbox.py`,
`src/eventsource/adapters/postgresql/outbox.py`,
`src/eventsource/adapters/sqlite/outbox.py`, and
`src/eventsource/testing/conformance_ports/outbox.py`
(`OutboxRepositoryConformance`).

Extends, but does not amend, [ADR 0024](0024-projection-persistence-ports.md):
the checkpoint/DLQ split it made is completed by this record for the third
`repositories/` module, but nothing about ADR 0024's own decision changes.

No existing ADR is amended by this one. See the ADR Impact table below and
its explanation.

## Context

`docs/core-surface.md` tracked `repositories/outbox.py` as the last module
still carrying the defect ADR 0019 and ADR 0024 had already fixed elsewhere:
one file holding the `OutboxRepository` Protocol, the `OutboxEntry` and
`OutboxStats` dataclasses, `InMemoryOutboxRepository`, and both
`PostgreSQLOutboxRepository` and `SQLiteOutboxRepository`. It also flagged
`repositories/_connection.py`'s unconditional `from sqlalchemy.ext.asyncio
import AsyncConnection, AsyncEngine` as an import every one of those classes
paid for, whether or not it touched sqlalchemy.

Four copies of the same six-key outbox payload existed independently: the
in-memory, PostgreSQL, and SQLite `add_event` methods each built the dict by
hand, and `adapters/postgresql/store.py`'s same-transaction
`_write_to_outbox` — living in the store ring, not the repository ring —
built a fifth, structurally identical dict from scratch. Any future field
addition or rename to that shape was four (or five) edits, silently
divergeable.

`repositories/_connection.py`'s `execute_with_connection(conn, transactional=
...)` was one of two connection-normalization helpers in the codebase after
ADR 0024 introduced `adapters/_sql/connection.py`'s `sql_connection(conn, *,
write=...)` for checkpoints and DLQ — the same job, two names, two modules.

`eventsource` is unreleased, so there is no prior public contract to
preserve and no deprecation shim owed to existing callers.

## Decision

### 1. Split: VOs and Protocol to `ports/outbox.py`, implementations to `adapters/`

`OutboxEntry`, `OutboxStats`, the `OutboxRepository` Protocol, and
`outbox_event_data()` move to `src/eventsource/ports/outbox.py` — a Tier 0
module: stdlib, typing, uuid, datetime, dataclasses, and `DomainEvent` only,
no sqlalchemy and no driver types. The three implementations move to
`src/eventsource/adapters/memory/outbox.py`,
`src/eventsource/adapters/postgresql/outbox.py`, and
`src/eventsource/adapters/sqlite/outbox.py`. This is the same operation ADR
0024 applied to checkpoints and DLQ, completing the set of three
`repositories/` modules that carried the Protocol/implementation mixing
defect.

### 2. Per-backend, not dialect-parameterized

Checkpoints and DLQ landed one sqlalchemy implementation under
`adapters/sql/` serving both PostgreSQL and SQLite through the same
dialect-parameterized code path. The outbox does not follow that shape:
`SQLiteOutboxRepository` is written against a raw `aiosqlite.Connection`,
not a sqlalchemy engine or session, because that is what it already was at
HEAD and no caller asked for it to change. The rule this yields: a
repository belongs in `adapters/sql/` when one sqlalchemy implementation
genuinely serves both dialects, and in `adapters/<technology>/` when the
backends take different drivers. The outbox's three adapters land at
`adapters/memory/outbox.py`, `adapters/postgresql/outbox.py`, and
`adapters/sqlite/outbox.py` — one directory per technology, per the
Adapters section of `.claude/rules/architecture.md`, rather than the
`adapters/sql/outbox.py` destination the pre-existing backlog item
predicted.

**Rejected alternative: rewrite `SQLiteOutboxRepository` onto sqlalchemy to
land it in `adapters/sql/` alongside checkpoints and DLQ.** This would make
the module shape uniform across all three repository families at the cost
of rewriting a working adapter's driver layer with no caller requesting the
change — a behavior-risking refactor in service of directory-naming
symmetry alone. Not built.

### 3. `outbox_event_data()` as the single authority for the payload shape

`outbox_event_data(event: DomainEvent) -> dict[str, Any]` in
`ports/outbox.py` replaces four independent constructions of the same
six-key dict (`event_id`, `aggregate_id`, `aggregate_type`, `tenant_id`,
`occurred_at`, `payload`): the in-memory, PostgreSQL, and SQLite
`add_event` methods, plus `adapters/postgresql/store.py`'s
`_write_to_outbox`, which lived in a different ring from the other three —
the store ring rather than the repository ring — and had drifted into its
own copy for that reason. All four call sites now call the one function.
Adding, renaming, or removing a key is a single-module change instead of a
four-site (three-ring) grep-and-edit.

### 4. Connection-helper consolidation onto `sql_connection`

`repositories/_connection.py`'s `execute_with_connection(conn, *,
transactional: bool)` is retired in favor of ADR 0024's
`adapters/_sql/connection.py:sql_connection(conn, *, write: bool)`, the
helper checkpoints and DLQ already used. `PostgreSQLOutboxRepository`'s
seven call sites move onto it directly.

**Accepted debt:** four `migration/` modules
(`migration/repositories/audit_log.py`, `migration/repositories/
position_mapping.py`, `migration/repositories/migration.py`,
`migration/repositories/routing.py`) and `readmodels/postgresql.py` also
called `execute_with_connection` and now import `sql_connection` from an
`adapters/` module instead. Every one of those five modules is already a
non-Tier-0 backend module, so nothing in Clean Architecture's dependency
rule or the import-linter contracts is violated — application-ring or
domain-ring code importing an adapter would be the actual defect, and none
of these five live in either ring. The debt is naming, not layering: five
modules outside `adapters/` reach into it for a connection helper that
arguably belongs in a shared infrastructure location neither `ports/` nor
any single adapter package. It is recorded here rather than fixed here
because those five modules move to `adapters/` themselves in a future
slice, at which point the import stops crossing a ring boundary by
definition and the debt resolves itself rather than requiring a rename.

### 5. Deletion of the `repositories/` package

`src/eventsource/repositories/` — `outbox.py`, `_connection.py`,
`__init__.py`, and the `EventSourceJSONEncoder`/`json_dumps`/`json_loads`
re-exports it carried — is deleted outright. This completes the retirement
of the package that ADR 0024 began by moving checkpoints and DLQ out of it.
`import eventsource.repositories` raises `ModuleNotFoundError`.

### 6. `OutboxRepositoryProtocol` and `list_pending_events` die, no shims

`OutboxRepositoryProtocol` — a bare `= OutboxRepository` alias kept "for
compatibility" — is not carried forward: the library is unreleased, so
there is nothing to be compatible with, and one name per thing is the
standing rule. `list_pending_events(limit=100)`, an alias forwarding to
`get_pending_events(limit=100)`, is dropped for the same reason: two names
for one method is exactly the surface a pre-release library should not
carry into its first release.

### 7. In-memory `event_data` serialization: orjson to stdlib `json.dumps`

`InMemoryOutboxRepository.add_event` serialized `event_data` with
`eventsource.serialization`'s orjson-backed `json_dumps`; it now uses
stdlib `json.dumps` directly. `outbox_event_data()`'s return value is
JSON-native — `str`, `None`, and JSON-native container/scalar types only —
so it needs no custom encoder, and the in-memory adapter is Tier 0 by the
same rule that makes `ports/outbox.py` Tier 0: no non-stdlib import for an
adapter that has no technology-specific reason to carry one. This buys one
fewer non-stdlib import in a Tier-0 adapter, at the cost of a cosmetic wire
format change described in Consequences.

### 8. ADR 0016's tracing decision stands for the outbox repositories

ADR 0025 amended ADR 0016 to remove per-operation spans from the *store*
adapters. That amendment does not extend to the outbox repositories: the
`postgresql_outbox_repository.*` spans on `PostgreSQLOutboxRepository`'s
seven methods are unchanged by this migration, moved verbatim with the
class. "Does this adapter trace?" has two different answers depending on
which ring of `adapters/` you're in after ADR 0025 — store adapters do
not, outbox (and checkpoint/DLQ) repositories do — and this item states
the outbox side explicitly so a future reader does not assume the store
rule generalizes to everything under `adapters/`.

### ADR Impact

Per `.claude/rules/definition-of-done.md`, this record's own impact
statement over the ADRs it touches, rendered from spec §6's verdicts:

| ADR | Disposition |
|-----|-------------|
| 0015 optional-dependency extras | Stands. `repositories/_connection.py`'s removal changes which module hosts the connection helper (already amended once by ADR 0024 for checkpoints/DLQ); the extras themselves are unchanged. |
| 0016 optional tracing no-op by default | Stands, for the outbox repositories specifically (Decision 8) — unlike the store adapters, which ADR 0025 already amended this record for. |
| 0019 clean-architecture store ports | Stands. `outbox_event_data()`'s single-authority role does not change the store ports; `PostgreSQLEventStore._write_to_outbox` now calls the shared function but keeps its existing signature and position in the store ring. |
| 0024 projection persistence ports | Extended, not amended. This record completes the same Protocol/implementation split for the third `repositories/` module; nothing ADR 0024 decided about checkpoints or DLQ changes. |
| 0025 legacy store retirement | Stands. Unrelated surface. |

**No existing ADR is amended by this one.** No other ADR file gains a
Status pointer as part of this change. This is worth stating explicitly
because a reviewer applying `.claude/rules/definition-of-done.md`'s rule —
amending or superseding an ADR requires updating the amended record's
Status section — will look for such a pointer here and should find the
reason it is absent: every ADR this record touches is *extended* by
completing a pattern that record already established (0024) or is
genuinely unaffected (0015, 0016, 0019, 0025), not changed in what it
itself decided.

## Consequences

### Positive

- The `repositories/` package no longer exists; every module ADR 0019 and
  ADR 0024's Protocol/implementation-mixing finding described in
  `docs/core-surface.md` is gone, not just narrowed.
- `ports/outbox.py` is Tier 0 — importing it does not import sqlalchemy,
  and `uv run python -c "import sys, eventsource.ports.outbox; assert
  'sqlalchemy' not in sys.modules"` exits 0.
- The outbox payload shape has one authority instead of four independent
  copies across two rings.
- One fewer connection-normalization helper: `sql_connection` is now the
  only one, used by checkpoints, DLQ, and the outbox alike.

### Negative / observable

- **Behavior delta (in-memory adapter only):** `event_data`'s JSON
  formatting changes from orjson's compact separators to stdlib
  `json.dumps`'s default spaced separators (`", "` / `": "` instead of
  `","` / `":"`). This is cosmetic for any consumer that parses the field
  back into a dict — round-trip equality is unaffected — and breaking for
  a consumer that compares the stored string byte-for-byte. Only the
  in-memory adapter is affected; PostgreSQL and SQLite serialize through
  their own drivers' JSON handling, unchanged.
- The connection-helper consolidation leaves five modules outside
  `adapters/` (`readmodels/postgresql.py` and four `migration/repositories/`
  modules) importing `sql_connection` from an `adapters/` module — the
  accepted debt in Decision 4. Permitted by every current import-linter
  contract; resolved when those modules themselves move to `adapters/` in
  a future slice, not by this one.
- Conformance testing the real PostgreSQL and SQLite schemas found a
  pre-existing bug the unit-test-only coverage at HEAD had not caught:
  `SQLiteOutboxRepository.add_event` inserts `str(uuid4())` into the
  migration's `id INTEGER PRIMARY KEY AUTOINCREMENT` column, which raises
  `IntegrityError` against the real schema. This is not new behavior — the
  bug existed before this migration — but the conformance suite this slice
  adds is what surfaced it. Backlogged as P1 in `BACKLOG.md`; the
  real-schema conformance binding runs `xfail(strict=False)` until it is
  fixed.
- `OutboxRepositoryProtocol` and `list_pending_events` are gone with no
  compatibility path. Any code outside this repository referencing either
  name breaks; there is none inside it after the docs and test sweep in
  this slice.

## Alternatives Considered

### Land the outbox under `adapters/sql/`, uniform with checkpoints and DLQ

Rejected in Decision 2: `SQLiteOutboxRepository` takes a raw
`aiosqlite.Connection`, and unifying it with the PostgreSQL implementation
under one sqlalchemy-backed, dialect-parameterized module would mean
rewriting a working adapter's driver layer for directory-naming symmetry
alone, with no caller requesting the behavior change. The per-backend rule
in Decision 2 is the honest description of what actually differs between
the outbox and its checkpoint/DLQ siblings.

### Keep `OutboxRepositoryProtocol` and `list_pending_events` as aliases

Rejected in Decision 6, same reasoning as ADR 0025 applied throughout the
store retirement: the library is unreleased, so there is no external
contract a compatibility alias would be protecting, and every alias is a
second name for something that should have exactly one.

### Fix the SQLite real-schema bug in this slice

Considered and rejected: the bug is a data-layer defect independent of the
ring migration this ADR documents, discovered incidentally by the new
conformance suite rather than being the thing this slice set out to fix.
Fixing it here would conflate a structural move with a behavior change in
one commit. Backlogged as P1 instead (see Consequences), to be fixed on
its own with its own test and its own review.

## References

- `docs/superpowers/specs/2026-07-31-outbox-ring-design.md` — the design
  spec this decision implements.
- [ADR 0019](0019-clean-architecture-store-ports.md) — the original
  Protocol/implementation split, for the event store.
- [ADR 0024](0024-projection-persistence-ports.md) — the same split
  applied to checkpoints and DLQ; this ADR completes the set for the
  outbox.
- [ADR 0025](0025-legacy-store-retirement.md) — amended ADR 0016 for store
  adapters only; Decision 8 above states why that amendment does not
  extend to the outbox.
- `src/eventsource/ports/outbox.py` — `OutboxRepository`, `OutboxEntry`,
  `OutboxStats`, `outbox_event_data`.
- `src/eventsource/adapters/memory/outbox.py`,
  `src/eventsource/adapters/postgresql/outbox.py`,
  `src/eventsource/adapters/sqlite/outbox.py` — the three implementations.
- `src/eventsource/testing/conformance_ports/outbox.py` —
  `OutboxRepositoryConformance`, exercised against all three adapters.
- `BACKLOG.md` — the P1 SQLite real-schema outbox bug this slice's
  conformance suite surfaced.
