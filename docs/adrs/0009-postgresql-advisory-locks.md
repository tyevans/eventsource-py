# ADR-0009: PostgreSQL Advisory Locks for Distributed Coordination

**Status:** Accepted

**Date:** 2026-07-27

**Deciders:** Library maintainers (architecture owner, migration owner)

---

This record explains why `eventsource.locks` provides exactly one distributed
mutual-exclusion primitive — `PostgreSQLLockManager`, built on PostgreSQL
session-scoped advisory locks — instead of a backend-agnostic protocol with
several implementations, and why that primitive looks the way it does: string
keys hashed to a 63-bit lock ID, one dedicated `AsyncSession` per held lock,
crash release delegated to the database's session lifecycle, and a client-side
poll loop standing in for a server-side lock timeout.

The motivating consumer is the live-migration tooling. `MigrationCutover` in
`src/eventsource/migration/cutover.py` wraps its whole cutover sequence in
`lock_manager.acquire(migration_lock_key(tenant_id, "cutover"), timeout=...)`
and uses `try_acquire()` / `release()` as a non-blocking readiness probe before
it commits to the operation. Everything below follows from that single use
case, and the scope limits it implies are stated explicitly in "When Not to Use
This".

The implementation lives in `src/eventsource/locks/postgresql.py` and is
re-exported from `src/eventsource/locks/__init__.py`; its behaviour is pinned by
`tests/locks/test_postgresql_locks.py` (pure-Python units: hashing, key
formatting, error and `LockInfo` shape) and
`tests/integration/locks/test_postgresql_locks_integration.py` (exclusion,
timeout, cancellation-safe release, `release_all`, concurrency — requires a real
PostgreSQL server).

## Status

**Accepted** — and retroactive. This record describes a decision that is
already in the code and shipped: `PostgreSQLLockManager`, `LockInfo`,
`migration_lock_key`, `LockAcquisitionError`, and `LockNotHeldError` are all
exported from `src/eventsource/locks/__init__.py` in the released 0.5.0 line,
and `MigrationCutover` already depends on them. Nothing here is a proposal; the
ADR exists to explain choices that were made incrementally and never written
down.

The record is not frozen, though. Two parts of it have been touched since the
original decision and may be touched again:

- The tracing integration was reworked from inheritance to composition
  (`refactor: migrate tracing from inheritance to composition pattern`), which
  is why the manager takes an injected `Tracer` and an `enable_tracing` flag
  rather than subclassing a traced base. That is the shape documented under
  "Tracing by Composition".
- The PostgreSQL-only scope is the load-bearing constraint. It stands as long
  as there is exactly one consumer with exactly one deployment story. If a
  second backend ever becomes necessary, the right move is a new ADR that
  supersedes this one and introduces a `LockManager` protocol — not an edit to
  this record. See "Future Considerations".

**Supersedes:** nothing. **Superseded by:** nothing.

The nearest neighbour is
[ADR-0009: Multi-Instance Subscription Coordination](0009-multi-instance-subscription-coordination.md),
which answers the same family of question — how do several instances of the
same process agree on who does a once-only job — but resolves it differently,
with a pluggable coordination backend rather than a hardcoded one. Read the two
together; the contrast between them is deliberate and is discussed under
"A Backend-Agnostic LockManager Protocol with SQLite/In-Memory Implementations".
ADR-0003's optimistic-concurrency model is a different layer entirely, and "Why
ADR-0003 Does Not Cover This" says why.

## Context

### The Problem: Cross-Instance Mutual Exclusion During Migration Cutover

The live-migration tooling in `src/eventsource/migration/` moves one tenant's
events from a shared store to a dedicated one while the application keeps
serving traffic. The dangerous moment is *cutover*: the point where routing
flips from the source store to the target store. `CutoverManager.execute_cutover()`
(in `src/eventsource/migration/cutover.py`) pauses writes for the tenant,
checks the sync lag one last time against
`config.cutover_max_lag_events`, moves the tenant's routing state to
`CUTOVER_PAUSED`, switches routing to the target store, and resumes writes —
rolling the whole thing back if any step fails or the pause budget
(`timeout_ms`, 100ms by default) is exceeded. That sequence is only correct if
exactly one actor is running it for a given tenant.

Nothing in the library's normal concurrency machinery prevents two actors from
running it at once. A typical deployment runs several identical application
instances, in separate processes on separate hosts. An operator triggering a
cutover from an admin endpoint hits whichever instance the load balancer picks;
a retry after a slow response can hit a second one while the first is still
mid-sequence. Two concurrent cutovers for the same tenant can interleave their
pauses, lag checks, routing-state transitions, and rollbacks: one actor's
rollback can undo the other's routing switch, or a routing switch can land on
the strength of a lag check the other actor's in-flight sync has already
invalidated. The result is routing that points at a store nobody verified —
silent divergence between where the application believes events are going and
where they actually land.

So the requirement is mutual exclusion across *processes*, keyed by tenant.
`asyncio.Lock` handles the intra-process case only; that is why
`PostgreSQLLockManager` uses one internally (`self._lock`) purely to guard its
own `_held_locks` dict, not to provide the guarantee this ADR is about.

### Requirements

The cutover use case fixes five requirements. Each one is visible in the shape
of the API that came out of it, so they are worth stating precisely before the
decision section explains how advisory locks satisfy them.

**Single-holder guarantee.** For a given key, at most one holder across every
process, host, and event loop that talks to the same database. The scope of the
guarantee is the database, not the process: two instances of the application
that share a PostgreSQL server must contend with each other, and the key is what
they contend on. Keys are strings (`migration_lock_key(tenant_id, "cutover")`
produces `"cutover:{tenant_id}"`), so exclusion is per tenant *and* per
operation — a cutover for tenant A must not block a cutover for tenant B, and a
`"migration:"` lock and a `"cutover:"` lock on the same tenant are distinct.

**Crash release.** If the holder is `SIGKILL`ed, loses its network, or its
container is evicted mid-cutover, the lock must become available again without
operator intervention. This is a hard requirement rather than a nicety, and it
is the one that most constrains the design: it rules out any scheme where the
lock is a durable record that only its owner can clear. A cutover is a rare,
high-attention operation, and a permanently wedged lock on a tenant — which
would block every future cutover attempt for that tenant until someone deleted a
row by hand — is a worse operational outcome than a briefly duplicated attempt.
Crucially, the requirement is for release *without a lease*: no TTL to tune, no
heartbeat to keep alive, no reaper to deploy.

**Non-blocking probe.** `CutoverManager.validate_cutover_readiness()` needs to
answer "is anyone else already doing this?" without waiting at all. It is a
pre-flight check run alongside a sync-lag check and a routing-state check, and
its whole value is that it returns immediately with a reason string
(`"Cutover lock is already held by another process"`) rather than stalling a
readiness endpoint. The code acquires and immediately releases:

```python
lock_info = await self._lock_manager.try_acquire(lock_key)
if lock_info is None:
    return False, "Cutover lock is already held by another process"
# Release the lock immediately - we just wanted to check availability
await self._lock_manager.release(lock_key)
```

That is a probe, not a claim: readiness is advisory, and the answer can be stale
the instant it is returned. The requirement it imposes is that the primitive
offer a try-style acquisition that reports failure as a value (`None`) rather
than as an exception, and a matching manual `release()` — which is why the
module has two acquisition surfaces rather than only the context manager.

**Bounded wait.** The blocking path must not hang forever. `CutoverManager`
defaults `lock_acquisition_timeout` to `0.5` seconds, passes it straight through
as `acquire(lock_key, timeout=self._lock_acquisition_timeout)`, and catches the
resulting `LockAcquisitionError` to return a failed `CutoverResult` with
`success=False`, `duration_ms=0.0`, and `rolled_back=False`. Contention is an
expected outcome to report, not an exception to crash on. Note the scale: the
lock timeout is 500ms while the cutover pause budget itself defaults to 100ms,
so the wait for the lock is deliberately allowed to dominate — waiting briefly
for a competitor to finish is fine, waiting unboundedly is not. The requirement
is therefore a *timeout*, not merely a *try*: the caller wants a short,
tolerant wait, not an instant give-up.

**Async, non-blocking I/O.** Per [ADR-0001](0001-async-first-design.md), every
I/O interface in this library is async-first. The lock primitive has to await
rather than block the event loop — including while it waits out contention,
which is why the timeout path sleeps with `asyncio.sleep(retry_interval)` rather
than occupying the loop. It also has to be cancellation-safe. `acquire()` is an
`@asynccontextmanager` that yields a `LockInfo` and releases in a `finally`, so
the lock is dropped and its session closed whether the body returns, raises, or
is cancelled — the property exercised by the cancellation-safe release case in
`tests/integration/locks/test_postgresql_locks_integration.py`. The failure
paths inside `_acquire_lock()` close the session too, so a timeout or a database
error does not leak a connection.

Two requirements that are conspicuously *absent* are worth naming, because
leaving them out is what makes the simple design sufficient: there is no
requirement for fairness (nothing guarantees the longest waiter wins — the poll
loop is a free-for-all), and no requirement for reentrancy across managers
(`_held_locks` is per-`PostgreSQLLockManager` bookkeeping, and `is_held()`
answers only "does *this* manager hold it", never "is anyone holding it").

### Forces

The requirements above could be met by several very different designs. What
narrowed the field was a set of pressures specific to *where this primitive is
used* — a rare, operator-triggered operation inside a library that tries hard
not to impose infrastructure on its users.

**PostgreSQL is already a dependency of the only consumer.** The migration
tooling is PostgreSQL-shaped from top to bottom: `CutoverManager` is
constructed with a `PostgreSQLLockManager` (the type is imported under
`TYPE_CHECKING` in `src/eventsource/migration/cutover.py` and named directly in
the constructor signature), and the routing state it flips lives in a database
the caller is already talking to over SQLAlchemy. There is no deployment in
which someone runs a live cutover without a PostgreSQL connection in hand.
That makes a PostgreSQL-based lock free in the only sense that matters
operationally — it adds no new runtime requirement. It also makes the
constructor honest: `PostgreSQLLockManager(session_factory)` takes an
`async_sessionmaker[AsyncSession]` the caller already has, and asks for nothing
else.

**No new infrastructure.** The alternative — Redis, etcd, ZooKeeper, a
Kubernetes lease — means a new service to deploy, secure, monitor, upgrade, and
reason about the partition behaviour of. For one operation that a given tenant
goes through exactly once. That trade is bad on its own terms, and it also cuts
against the library's optional-dependency policy
([ADR-0007: Optional Dependency Extras](0007-optional-dependency-extras.md)),
which exists so users are not made to install and run what they do not use.
Making a coordinator *mandatory* for a rare admin operation would be the
sharpest possible violation of that principle. Note that this force is about
the *hard requirement*, not about capability: nothing stops a user from
coordinating cutovers some other way; the library simply declines to require it.

**Connection-pool cost is real, and has to be accepted knowingly.** PostgreSQL
advisory locks are session-scoped, which means a held lock occupies a session —
and therefore a pooled connection — for its entire lifetime. The implementation
does not hide this: `_acquire_lock()` calls `self._session_factory()` itself,
returns the live session, and `acquire()` parks it in
`_held_locks[key] = (session, lock_id)` until release closes it. `try_acquire()`
does the same, which is why its docstring puts the release obligation on the
caller. N concurrent locks means N concurrent connections, and the class
docstring says so outright: *"Each lock uses a dedicated session/connection.
Consider connection pool sizing when using multiple concurrent locks."* For a
handful of tenant cutovers this is negligible. For a lock taken on a
per-request or per-event basis it would exhaust a pool, and "When Not to Use
This" draws that line explicitly.

**Session semantics cut both ways.** Binding the lock to a session is precisely
what buys crash release for free. The server drops every advisory lock held by
a connection when that connection goes away, so there is no lease to renew, no
TTL to tune, no heartbeat task to keep scheduled on a busy event loop, and no
reaper process to deploy and monitor. An entire category of distributed-systems
bookkeeping simply does not exist here. The price is threefold: the dedicated
connection above; a lock lifetime decoupled from transactions, so it neither
participates in nor is released by a `COMMIT` or `ROLLBACK` (which is what makes
it usable across the multi-step cutover sequence in the first place, but also
means a rollback does *not* drop the lock); and the fact that the guarantee is
only as strong as the connection's liveness detection — a wedged TCP connection
that the server has not yet reaped keeps the lock held for as long as it takes
`tcp_keepalives` to notice.

**Cheapness matters more than sophistication.** These forces all point the same
way because the operation is rare and the failure it prevents is catastrophic
but improbable. The design budget for such a feature is small: it should be
easy to read, have no moving parts to operate, and be obviously correct rather
than cleverly correct. That is why the whole module is roughly 500 lines with
four SQL statements in it, and why the timeout is a poll loop rather than
anything more subtle.

### Why ADR-0003 Does Not Cover This

The library already has a concurrency-control story, and it is the first thing
a reader reaches for here: optimistic locking on aggregate streams.
`EventStore.append_events()` takes an `expected_version` alongside the events,
and the store raises `OptimisticLockError(aggregate_id, expected_version,
actual_version)` if another writer got there first. That mechanism is
load-bearing everywhere — it is what makes concurrent writes to the same
aggregate safe without any coordination service at all. So the question is
fair: why is it not enough for cutover?

Because it protects a different unit, in a different style, over a different
span of time:

| | Stream optimistic concurrency (`expected_version`) | Advisory lock (`PostgreSQLLockManager`) |
|---|---|---|
| Unit protected | One aggregate's event stream, identified by `aggregate_id` | An arbitrary named operation, identified by a string key such as `cutover:{tenant_id}` |
| Style | Optimistic — let both actors proceed, detect the conflict at write time | Pessimistic — stop the second actor before it starts |
| Span | A single `append_events()` call | A multi-step sequence spanning many calls, several components, and a deliberate pause |
| On conflict | The loser's write is rejected with `OptimisticLockError`; it re-reads and retries | The loser never enters the critical section; it waits, or reports contention |
| Failure prevented | A lost update within one stream | Two actors interleaving the steps of one operation |

Cutover is not an append. `CutoverManager._execute_cutover_locked()` pauses
writes for the tenant, reads the sync lag from the `SyncLagTracker`, moves the
tenant's routing state to `CUTOVER_PAUSED`, switches routing to the target
store, marks the migration state `MIGRATED`, and resumes writes — rolling all
of it back if any step fails or the pause budget is blown. Its correctness
depends on no one else being *inside that sequence*, not on any single write
landing at an expected version.

Two details make the gap concrete. First, the state the sequence guards is not
an event stream at all: `TenantRoutingRepository` exposes `set_routing()` and
`set_migration_state()` as plain upserts, with no version column and no
compare-and-set. There is no `expected_version` to pass, because routing is not
event-sourced. Second — and this is the part that would remain true even if
routing rows *did* carry versions — a version check would fire too late. Both
actors would already have paused writes for the tenant and validated lag
against a state the other was concurrently mutating; the loser would then find
its final routing write rejected and roll back, potentially undoing a
transition the winner was relying on. The damage is done in the steps *before*
the conflicting write, so detecting it at the write is the wrong instrument.
The conflict has to be prevented up front, which is the definition of a
pessimistic, operation-level mutex.

The two mechanisms are therefore complementary rather than competing, and they
coexist during a migration without interacting: optimistic concurrency guards
per-stream integrity on every append, including the appends still flowing
through dual-write while the migration runs; the advisory lock guards
whole-operation exclusivity for the rare administrative sequence that
reconfigures where those appends go. Neither substitutes for the other, and the
choice between them is settled by the unit you need to protect — a stream, or a
procedure.

**A note on the reference.** ADR-0003 is cited here by number as the record for
the optimistic-locking contract, but that record has not been written: there is
no `0003-*.md` in `docs/adrs/`, and `docs/adrs/index.md` lists the number as
planned rather than published. Until it exists, the authoritative description
of the contract is the `append_events()` docstring in
`src/eventsource/stores/interface.py` and the `OptimisticLockError` definition
in `src/eventsource/exceptions.py`. Whoever writes ADR-0003 should treat this
section as the boundary already drawn from the other side, and stay consistent
with it.
