# ADR-0066. Read-Model Schema Reconciliation Is Additive and Opt-In

`generate_schema` emits `CREATE TABLE IF NOT EXISTS`, which does nothing to a
table that already exists, so a field added to a `ReadModel` never becomes a
column in a database created before it. `generate_additive_migration` (pure)
and `reconcile_read_model_schema` (against a live connection) close the
additive half of that gap. Neither is called by the library.

## Status

**Accepted.**

**ADR Impact**

| ADR | Status |
| --- | --- |
| [0029](0029-locks-readmodels-and-engine-rings.md) | Stands. It placed read-model schema generation in the adapters ring on the grounds that dialect knowledge belongs to an adapter; reconciliation is more of the same and lands beside it. |
| [0039](0039-schema-ddl-to-adapters.md) | Extended. 0039 settled that the library's *own* tables carry append-only additive update scripts. This offers the equivalent for the tables consumers define, without claiming ownership of their migration process. |

## Context

A read model's table is generated from the model class, and the generated DDL
is `CREATE TABLE IF NOT EXISTS`. That statement is a no-op against an existing
table: it does not compare columns, and it does not report that it did nothing.

So adding a field to a `ReadModel` is silently a no-op against every database
that already has the table. The projection then writes a column that is not
there, and the failure is a database error at write time in the first
environment that has real data — never in tests, which build their tables from
nothing, where the `CREATE` is always complete. The gap opens exactly between
the environments that are cheap to test and the one that is not.

The library already solves this shape for its own tables, with additive update
scripts under `adapters/sql/schemas/`. It offered consumers nothing.

## Decision

Two functions, split so the diff can be inspected without being executed.

`generate_additive_migration(model_class, existing_columns, dialect)` is pure:
it takes the columns the table has and returns `ALTER TABLE ... ADD COLUMN`
statements. `reconcile_read_model_schema(conn, model_class)` introspects a live
connection, calls the generator, executes the result, and returns what it ran.

**Additive only.** Nothing is dropped, retyped, renamed, or reordered, and a
column the model no longer declares is left alone. Everything excluded here has
a data question attached — what fills the new column, what happens to the old
one's contents — and a library that answers those questions on a consumer's
behalf, unattended, at startup, is a library that loses data. Adding a nullable
or defaulted column is the one change with no such question.

**Refused rather than attempted:** a required column with no default cannot be
added to a table that may already have rows. The refusal happens before any
statement executes, so it is never a partial apply, and the error says what to
do — give the field a default, make it optional, or write the migration this
change actually needs.

**Opt-in.** Nothing in the library calls it. It runs when a consumer calls it,
at a point the consumer picks, which is what keeps it from competing with
Alembic: it is a function you may call, not a startup hook that will fight
whatever already owns your schema.

**The dialect comes from the connection.** `reconcile_read_model_schema` takes
no dialect argument. The connection already carries that fact, and a second
place to declare it is a second place for it to disagree.

### Rejected: reconciling automatically when a projection starts

It would close the gap with no consumer action at all, which is its whole
appeal. It also means the library issues DDL at a moment the consumer did not
choose, against a schema something else may own, with no opportunity to review
the statements. A schema change that happens without being asked for is the
thing consumers use migration tools to prevent.

### Rejected: full reconciliation, including drops and type changes

That is a migration tool. Writing a worse one inside this library helps nobody,
and the failure mode of getting it wrong is destroyed data rather than a
`CREATE` that quietly did nothing.

## Consequences

Adding a nullable or defaulted field to a `ReadModel` is now a change a
consumer can deploy by calling one function, and a consumer who calls it on
every startup gets an empty statement list and no side effects once the table
matches.

Indexes on an existing table are not reconciled, and this decision accepts
that rather than deferring it — only table creation carries the generated index
DDL, so a model that gains an `__indexes__` entry needs that index created by
hand. Reconciling them is not the same problem: an index has to be compared by
*definition* rather than by name, because `CREATE INDEX IF NOT EXISTS` will
leave a stale index in place under a matching name and report success. That
comparison is dialect-specific, and getting it wrong silently leaves queries on
the old index — the same class of invisible failure this record exists to
close, reintroduced one level down. It stays out of scope until someone wants
it enough to do the definition-level comparison properly.

The additive-only boundary means a consumer who removes a field is left with a
column that is never written and never dropped. That is the correct outcome for
an unattended tool, and the point at which the consumer should be reaching for
their migration process instead.
