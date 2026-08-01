# 0037. Store Lifecycle Port and Explicit Engine Ownership

`close()` was not part of any store port. `SyncStoreFacade.close()`
duck-typed it with `getattr(store, "close", None)`, and
`PostgreSQLEventStore.close()` unconditionally disposed the `AsyncEngine`
the caller had injected into its constructor — a documented hazard:
`SyncStoreFacade(PostgreSQLEventStore(shared_engine))` would silently tear
down a connection pool the caller still held and might be sharing with
other stores or consumers. This ADR adds an optional `SupportsClose` port
with a documented ownership contract, and makes engine ownership an
explicit, opt-in constructor flag on the PostgreSQL adapter.

## Status

**Accepted.** Implemented in `src/eventsource/ports/lifecycle.py`
(`SupportsClose`), `src/eventsource/testing/sync_facade.py`
(`SyncStoreFacade.close` uses `isinstance` instead of `getattr`), and
`src/eventsource/adapters/postgresql/store.py`
(`PostgreSQLEventStore.__init__`'s `owns_engine: bool = False` keyword-only
parameter; `close()` disposes the engine only when `owns_engine=True`).

## Decision Table

| Before | After | Rationale |
|---|---|---|
| No lifecycle port; `SyncStoreFacade.close` used `getattr(self._store, "close", None)` | `SupportsClose(Protocol)`, `@runtime_checkable`, single `async def close(self) -> None` method in `ports/lifecycle.py` | Optional capability, same shape as `SnapshotTypeInvalidation` (ADR 0036): a store with nothing to release simply doesn't implement it — no default no-op body, no sentinel return value to check. |
| `SyncStoreFacade.close`: `close = getattr(self._store, "close", None); if close is not None: ...` | `if isinstance(self._store, SupportsClose): await self._store.close()` | Structural, `isinstance`-checkable capability test instead of `getattr` duck-typing — the same pattern this codebase already uses for every other optional capability. |
| `PostgreSQLEventStore.close()`: unconditionally `await self._engine.dispose()` | `close()` disposes only when `self._owns_engine` is `True`; new keyword-only `owns_engine: bool = False` constructor parameter | The engine is always caller-supplied; the store cannot know whether the caller still needs it, or shares it with other stores, unless told explicitly. Defaulting to `False` means `close()` is a safe no-op unless the caller opts in — this is a deliberate **breaking** behavior change from the previous always-dispose default. |

## Context

The team lead's recon before this slice found exactly two adapter `close()`
methods in the whole codebase: `SQLiteEventStore.close` (closes an
internally-created `aiosqlite` connection it owns — already correct, no
change needed) and `PostgreSQLEventStore.close` (the hazard above).
`MemoryEventStore` has no `close()` at all, correctly — it holds no
releasable resource. `SyncStoreFacade.close` was the only consumer
duck-typing the capability, and `PostgreSQLEventStore.close`'s single
`self._engine.dispose()` call was the only call site relying on the old
always-dispose behavior anywhere in `src/`.

`SupportsClose` was named after the `typing.SupportsInt`/`SupportsFloat`/
`SupportsBytes` stdlib convention for a single-method structural-typing
marker, rather than after the noun-per-capability pattern the rest of
`ports/` otherwise follows (`EventAppender`, `StreamReader`,
`DistributedLock`). The noun pattern names a port after the capability it
grants ("a thing that appends events," "a thing that reads streams"); there
is no equally natural noun for "a thing you can close" the way there is for
those, so the stdlib's own precedent for exactly this shape of Protocol was
judged the better fit. No prior `Closeable`-style precedent exists anywhere
in this repository's ports.

A repo-wide grep for sites relying on the old always-dispose behavior found
three genuine ones, all in integration test fixtures constructing a private,
per-test `AsyncEngine` specifically so `store.close()` would dispose it at
teardown (`tests/integration/adapters/test_postgresql_conformance.py`'s
`_fresh_store()` helper, shared by eight test classes; a matching helper in
`tests/integration/adapters/test_postgresql_no_skip.py`; and
`tests/integration/e2e/conftest.py`'s `postgres_event_store` fixture). Each
was updated to pass `owns_engine=True` at construction, preserving the exact
prior behavior explicitly rather than implicitly. Two other fixtures in
`tests/integration/conftest.py` share one session-scoped engine across the
whole test session and already never called `store.close()` at all, with
comments warning against it; those needed no functional change (the default
`owns_engine=False` is correct there), though the comments were updated
since they described the old hazard the new default has closed.
`bench/adapters/stores.py`'s benchmark harness disposes its engine directly
in its own `teardown()`, never through `store.close()`, so it was entirely
unaffected.

## Consequences

### Positive

- The engine-ownership hazard this ADR was written to close —
  `SyncStoreFacade(PostgreSQLEventStore(shared_engine)).close()` silently
  disposing a pool the caller still held — no longer happens by default.
- `SyncStoreFacade.close` gains a proper structural-typing check instead of
  `getattr` duck-typing, matching every other optional-capability check in
  the codebase (`isinstance(store, SnapshotTypeInvalidation)`, etc.).
- Callers that genuinely want a `PostgreSQLEventStore` to own and manage its
  engine's lifetime can opt in explicitly (`owns_engine=True`) and get the
  same behavior the store always had before this ADR, just now documented
  and intentional rather than implicit and surprising.

### Negative

- **Breaking change**: any external caller relying on
  `PostgreSQLEventStore.close()` disposing a caller-injected engine must add
  `owns_engine=True` at construction to preserve the old behavior. No
  external caller is known to exist yet (per the standing "library has no
  external users yet" rationale ADR 0030 and its predecessors already
  applied to similar breaking changes), so no shim or deprecation window
  was added.
- `SupportsClose` follows a different naming convention (`Supports*`) than
  every other port in `ports/`. This is a deliberate, cited departure (the
  stdlib precedent), not an oversight, but it means a contributor scanning
  `ports/` for the naming pattern will find exactly one exception.

## Alternatives Considered

**Stop `PostgreSQLEventStore.close()` from disposing the engine at all,
ever.** Rejected: some callers do want the store to manage the engine's
full lifetime (construct it, hand it to exactly one store, dispose it when
done) — removing disposal entirely would just move the hazard the other
direction, forcing every such caller to dispose the engine manually with no
way to ask the store to do it. An opt-in flag serves both cases; a bare
removal serves only one.

**Name the port `Closeable` instead of `SupportsClose`.** Considered, since
it matches the adjective-suffix pattern of similarly-shaped Java/Kotlin
interfaces some contributors might expect. Rejected in favor of the stdlib
`Supports*` convention already established in `typing` itself, which this
codebase's type-checking tooling (mypy) and its users already recognize
without cross-repository context.

## References

- `src/eventsource/ports/lifecycle.py`
- `src/eventsource/testing/sync_facade.py`
- `src/eventsource/adapters/postgresql/store.py`
- `tests/unit/ports/test_lifecycle.py`
- [ADR 0036](0036-snapshot-port-composed-protocols.md) — the optional-
  capability-as-a-separate-Protocol shape this ADR applies to lifecycle

## Related

- BACKLOG.md's "Define store lifecycle in the ports layer" entry (removed
  by this ADR's implementation; see CHANGELOG.md)
