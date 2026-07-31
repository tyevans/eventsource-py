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

## Context (part b)

The PostgreSQL adapter's global feed applies a safe-horizon predicate to
defer rows whose inserting transaction is not yet definitely-committed —
`global_position` is a `BIGSERIAL`, and under concurrent writers a lower
position can commit *after* a higher one, so reading past an uncommitted
lower position would skip it forever once a reader resumes from a higher
position it already saw. The predicate as shipped compared the `xmin`
system column (a 32-bit xid, textually cast to bigint) against
`pg_snapshot_xmin(pg_current_snapshot())` (a 64-bit, epoch-extended
`xid8`, also textually cast to bigint). `xmin`'s cast value wraps at
2^32; once a cluster's transaction counter crosses its first xid epoch,
the two casts stop being comparable on the same numeric line, and the
predicate degenerates to universally true — the no-skip guard silently
disappears, in exactly the high-write-volume deployments most likely to
have crossed an epoch and most in need of the guard. This is a
fail-open defect: nothing errors, reads simply stop deferring
in-flight rows.

## Decision (part b)

Add an `events.txid xid8` column, populated by `DEFAULT
pg_current_xact_id()` for all new rows, and rewrite the safe-horizon
predicate to filter on it directly:

```sql
(txid IS NULL OR txid < CAST(:txid_horizon AS text)::xid8)
```

`xid8` is PostgreSQL's native 64-bit, epoch-extended transaction id type;
comparing it to itself has no wraparound on any human timescale. The
horizon (`pg_snapshot_xmin(pg_current_snapshot())`, wrapped behind the
`eventsource_feed_horizon()` SQL function so the adapter's Python source
never has to name the underlying system column) is fetched once per read
— a scalar query on the same session — and bound as a parameter, rather
than inlined as a volatile expression evaluated per row.

The column arrives via the additive-fragment mechanism
(`migrations/additive/events_txid.sql`, registered in
`migrations/__init__.py`'s `_ADDITIVE_FRAGMENTS` for the `events`,
`events_partitioned`, and `all` PostgreSQL schemas) plus an operator
script, `migrations/updates/004_add_events_txid.sql`, for existing
deployments. Both apply the `ALTER TABLE` in two statements deliberately:
`ADD COLUMN ... xid8` with no default is a metadata-only catalog change,
while a single-statement `ADD COLUMN ... DEFAULT pg_current_xact_id()`
would force a full table rewrite. The subsequent `ALTER COLUMN ... SET
DEFAULT` applies to future inserts only and rewrites nothing.

Rows left with a NULL `txid` — those inserted before the migration
applied — are always definitely-committed and always safe to read.
`ALTER TABLE` takes `ACCESS EXCLUSIVE`, so any transaction that inserted
a pre-migration row had already finished before every snapshot taken
after the migration committed; there is no window in which a NULL-`txid`
row could still be in flight. This is why the predicate treats `txid IS
NULL` as passing rather than backfilling a value or requiring `NOT NULL`.

A database that has not applied `updates/004` fails loudly with an
undefined-column error on the next feed read, by design: PostgreSQL
deployments **must** apply it before upgrading. A silent fallback to the
old predicate was rejected — see below.

The PostgreSQL 13 floor is unchanged: `pg_current_snapshot()` already
required it, and `xid8` / `pg_current_xact_id()` require nothing newer.

## Rejected Alternatives (part b)

**Modular 32-bit arithmetic on `xmin` directly.** PostgreSQL defines no
ordering operators on the `xid` type — any comparison has to go through a
cast, and a wraparound-aware modular comparison would need to be
hand-rolled as an expression evaluated per row. Even done correctly, it
carries an irreducible ambiguity at the 2^31 boundary (a xid up to 2^31
transactions "behind" is ordered before; further behind wraps to
"ahead" — indistinguishable from the true case without an unbounded
reference point) and remains unindexable, since it is not a plain column
comparison. Not built.

**WONTFIX — accept the epoch-crossing gap.** Rejected because it is
fail-open in exactly the deployments that most need the no-skip
guarantee: long-lived, high-write-volume PostgreSQL clusters are the
ones most likely to cross an xid epoch, and losing the guard silently
there is worse than the operational cost of a required migration. Not
built.

**Add the column `NOT NULL` with a backfill.** Backfilling every existing
row's `txid` (there is no historical transaction id to backfill it with —
only a synthetic placeholder) requires rewriting the entire table, for a
correctness gain of zero: the NULL-is-safe argument above already makes
pre-migration rows unconditionally safe to read without a value. Not
built.

**Runtime column-existence probe with fallback to the old predicate.**
Would let a database that has not applied `updates/004` keep running
against the wraparound-unsafe path indefinitely, silently — the exact
failure mode this record exists to close — and would hide the
operational requirement to migrate rather than surfacing it. Not built.

## Consequences (part b)

Existing PostgreSQL deployments must run `migrations/updates
/004_add_events_txid.sql` before upgrading to a version of this library
that includes this change; the global feed read path fails loudly with
an undefined-column error otherwise. Fresh provisioning via `get_schema`
/ `get_all_schemas` needs no operator action — the column arrives as part
of the composed schema. See the `## [Unreleased]` entry in `CHANGELOG.md`
for the operator-facing notice.

`BACKLOG.md`'s "Re-benchmark pg catch-up horizon predicate at scale (P2)"
entry, which proposed the bound-parameter mitigation this record ships,
has had its premise rewritten: the inline `xmin`-cast predicate it
targeted no longer exists, and the entry now points at the new predicate
shape for its next run.

## References (part b)

- [ADR 0025](0025-legacy-store-retirement.md) — the no-skip global-feed
  guarantee this record depends on stands unchanged; this is a mechanism
  replacement, not a guarantee change.
- [ADR 0019](0019-clean-architecture-store-ports.md) — documents the
  feed's transaction-safe-horizon requirement that both predicates
  implement.
- `src/eventsource/adapters/postgresql/store.py` — `_HORIZON_PREDICATE`,
  `_HORIZON_QUERY`, `_do_read_all`, `current_position`.
- `src/eventsource/migrations/additive/events_txid.sql`,
  `src/eventsource/migrations/updates/004_add_events_txid.sql` — the
  additive fragment and operator script.
- `tests/unit/adapters/test_postgresql_feed_horizon.py` — query-shape
  regression guard; true epoch wraparound is not reproducible in a
  testcontainer.
