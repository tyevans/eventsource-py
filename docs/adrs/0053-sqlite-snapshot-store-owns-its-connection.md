# 0053. `SQLiteSnapshotStore` Owns One Connection And Closes It

Every operation opened its own `aiosqlite.connect()`. Nothing could release the
store, and `":memory:"` silently did not work.

## Status

**Accepted.**

**ADR Impact**

| ADR | Status |
| --- | --- |
| [0037](0037-store-lifecycle-port.md) | Stands, and this is a new implementer of it. The ownership contract it states — `close()` releases only what the object itself created — is what makes the store's own connection closeable and would forbid closing a caller-injected one. |
| [0036](0036-snapshot-port-composed-protocols.md) | Stands. `SupportsClose` is a third optional capability the store satisfies structurally, alongside `SnapshotStore` and `SnapshotTypeInvalidation`. |
| [0039](0039-schema-ddl-to-adapters.md) | Stands. The store applies the bundled sqlite `snapshots` DDL rather than carrying its own copy. |

## Context

`SQLiteEventStore` opens one connection lazily and keeps it — required for
`":memory:"`, whose contents live only as long as the connection that created
them — and implements `close()`. `SQLiteSnapshotStore`, written earlier, opened
a connection per operation instead.

Two consequences followed. `":memory:"` was accepted by the constructor and
reported by `database_path` but could never work: each call saw a different,
empty database, so saves vanished and every load missed. The guide documented
this as a limitation.

The second is the expensive one. `aiosqlite` backs each connection with a
**non-daemon** thread. A per-operation connection closes its thread on exit, so
nothing leaked — but the store had no `close()`, no ownership of anything, and
therefore no way for a caller to say "I am done with this". A downstream
consumer took the only remaining option: thread a single store instance through
every call site so a second is never constructed, and document at each site why
the parameter exists. Three call sites and roughly thirty lines of justification
existed to work around a missing method.

## Decision

`SQLiteSnapshotStore` opens one connection lazily on first use and reuses it for
its lifetime, guarding setup with double-checked locking on a dedicated
`_init_lock` — without which two concurrent first-callers each open a
connection, and the loser's thread leaks for the process lifetime. Every
statement runs under a separate `_lock`, so no `commit()` lands part-way through
another operation.

The store implements `SupportsClose`: `close()` releases the connection it
opened, is idempotent, and a later operation reopens.

On first connect it applies the bundled sqlite `snapshots` schema, which is
`IF NOT EXISTS` throughout, and sets `busy_timeout` — a snapshot store commonly
shares a file with a `SQLiteEventStore` holding its own connection.

This mirrors `SQLiteEventStore` deliberately. Two SQLite adapters with two
different connection disciplines is the shape defect #1 takes before it becomes
a defect.

## Consequences

`":memory:"` works, which makes the store usable in tests without a temporary
directory — the conformance fixture no longer needs one, nor the hand-applied
schema it used to set up.

Callers must close the store. Nothing in the library closes a snapshot store for
them, and an unclosed one now holds a non-daemon thread that keeps the
interpreter alive at shutdown. This is the trade the ownership contract makes
explicit: the resource is releasable precisely because the store owns it.

Consumers no longer need to route one instance through every call site to avoid
constructing a second, and can drop the parameters and properties that existed
only to do so.

Pointing the store at a database whose `snapshots` table was created elsewhere
still works; the DDL is idempotent.
