# 0027. Schema Correctness Fixes

## Status

Proposed. Built across the correctness-fixes slice; parts (b) and (c) land
in the same slice's later tasks. Accepted when part (c) lands.

## Context (part a)

`src/eventsource/migrations/templates/sqlite/outbox.sql` declared
`id INTEGER PRIMARY KEY AUTOINCREMENT` for `event_outbox.id` — SQLite's
strictly-typed rowid alias. `SQLiteOutboxRepository.add_event` inserts
`str(uuid4())` into that column. SQLite rejects a TEXT value in an
`INTEGER PRIMARY KEY` column with `sqlite3.IntegrityError: datatype
mismatch`, so every insert against the shipped schema has raised since the
file first shipped. The same table body was embedded verbatim in
`src/eventsource/migrations/schemas/sqlite_all.sql`, which
`SQLiteEventStore._conn()` applies on first connection — so the combined
schema carried the identical defect.

ADR 0026 backlogged this as a P1 found by the outbox conformance suite,
deliberately not fixed in that slice (see ADR 0026's Consequences and
"Fix the SQLite real-schema bug in this slice" under Alternatives
Considered). This record fixes it.

## Decision (part a)

Replace `id INTEGER PRIMARY KEY AUTOINCREMENT` with `id TEXT PRIMARY KEY`
**in place** in both `migrations/templates/sqlite/outbox.sql` and
`migrations/schemas/sqlite_all.sql`.

`migrations/` is append-only by design (see `CLAUDE.md`'s Do Not Modify
section). This change is sanctioned as a narrow, explicit exception:

> A shipped schema that provably never worked with its only shipped writer
> may be corrected in place; everything else under `migrations/` remains
> append-only by file.

Three facts support treating this as correction rather than migration:

- The table as shipped cannot hold a single row this library's own writer
  produces — `add_event`'s only code path fails against it.
- Any deployment that has successfully written an integer id into this
  column is, by construction, not using `SQLiteOutboxRepository` — no
  first-party writer exists that the fix could break.
- SQLite cannot `ALTER TABLE ... ALTER COLUMN` a primary key's declared
  type. The additive-fragment mechanism the append-only convention exists
  to support (`ALTER TABLE ... ADD COLUMN`, guarded by `PRAGMA
  table_info`) has no way to express this repair; only a `CREATE TABLE`
  rewrite can.

## Rejected Alternatives (part a)

**Adapter surfaces the rowid instead of a UUID.** Would break the
`OutboxEntry.id: UUID` port type and diverge from the PostgreSQL adapter's
`gen_random_uuid()`-backed id, which is a `UUID` on both sides of the port.
Not built.

**Ship a second, corrected schema file alongside the broken one.**
`get_schema("outbox", backend="sqlite")` has one caller-visible return
value; adding a second file does not change what that call returns unless
the broken file is also removed or renamed, which is the same operation as
fixing it in place with extra indirection. Not built.

**WONTFIX, treat the hand-rolled test fixture as the de-facto schema.**
The fixture existed for exactly this reason before this task and is what
let the defect ship undetected — see ADR 0026's Context. The migration
file is the product; a fixture that diverges from it to make tests pass is
the bug, not a workaround for it. Not built.

## Consequences (part a)

Any existing SQLite database provisioned from either file before this fix
carries an empty, unusable `event_outbox` table: `CREATE TABLE IF NOT
EXISTS` will not replace an existing table, so re-running the (now
corrected) migration against such a database is a no-op and the broken
table persists. Operators must run `DROP TABLE event_outbox;` and
re-provision from the corrected schema. No data can be lost by this: the
table has never held a row written by `SQLiteOutboxRepository`, so nothing
of value can have existed in it.

## References

- [ADR 0026](0026-outbox-ring-migration.md) — backlogged this defect as a
  P1 found by the outbox conformance suite; this record fixes it.
- `src/eventsource/migrations/templates/sqlite/outbox.sql`,
  `src/eventsource/migrations/schemas/sqlite_all.sql` — the corrected
  files.
- `src/eventsource/adapters/sqlite/outbox.py` — `SQLiteOutboxRepository`,
  the only writer this schema needs to support.
- `tests/unit/adapters/test_sqlite_conformance.py` —
  `TestSQLiteOutboxRepository`, now run without the `xfail` marker that
  previously institutionalized the defect.
