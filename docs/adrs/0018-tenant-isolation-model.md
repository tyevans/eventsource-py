# ADR-0018: Tenant Isolation Model

## Status

Accepted. Implemented in `src/eventsource/multitenancy/` (`context.py`, `events.py`,
`repository.py`, `exceptions.py`). This ADR describes the model as it exists today,
including the parts that are deliberately incomplete.

## Context

### What multi-tenancy has to solve in an event-sourced system

A multi-tenant event-sourced application has to answer three questions, and they are
not the same question:

1. **Attribution** — which tenant does this event belong to? In an append-only log,
   attribution is permanent: an event written with the wrong `tenant_id` is a
   permanent record of the wrong thing, and correcting it means writing a compensating
   event, not an `UPDATE`.
2. **Write isolation** — can a request acting for tenant A append events attributed to
   tenant B? This is the leak that corrupts the log.
3. **Read isolation** — can a request acting for tenant A replay a stream belonging to
   tenant B? Aggregate ids are UUIDs, so this requires either guessing an id or holding
   a stale reference — but "hard to guess" is not an isolation boundary.

Attribution and write isolation are properties of the event and the write path, and the
library can own them. Read isolation is a property of every query against the store,
including queries the library never sees (projection rebuilds, operational SQL, ad-hoc
reporting). That asymmetry drives most of what follows.

### Constraints we accepted going in (async-first, backend-agnostic stores, optional tenancy on the base `DomainEvent`)

Three pre-existing commitments constrained the design:

- **Async-first.** All store, bus, and repository interfaces are `async`. Any mechanism
  for carrying "who is this request for" has to survive `await` boundaries and stay
  isolated between concurrently running tasks.
- **Backend-agnostic stores.** `EventStore` has PostgreSQL, SQLite, and in-memory
  implementations, plus whatever users write. Any change to the read interface has to be
  implementable — and enforced — by all of them. The one tenant-aware seam that does
  exist, `ReadOptions.tenant_id` on the streaming read paths (`read_stream`, `read_all`)
  and the `tenant_id` parameter on `get_events_by_type()`, is a filter the caller opts
  into, not an isolation guarantee the store enforces.
- **Tenancy is optional.** `DomainEvent.tenant_id` is declared as
  `UUID | None = Field(default=None, ...)` in `events/base.py`. Single-tenant users are
  the majority case and must not pay for multi-tenancy. This was settled before this
  ADR, and it is the constraint that makes several decisions below "silently permissive"
  rather than "strict".

## Decision

### 1. Tenancy is ambient, carried by a `ContextVar`, not passed as a parameter

#### `tenant_context: ContextVar[UUID | None]` with default `None`

`multitenancy/context.py` declares a single module-level context variable:

```python
tenant_context: ContextVar[UUID | None] = ContextVar("tenant_context", default=None)
```

The default of `None` means "no tenant context" is a valid, non-exceptional state.
Nothing about importing the multitenancy package changes the behavior of code that never
sets it.

#### Scoping via `tenant_scope()` / `tenant_scope_sync()` and `Token`-based reset

`set_current_tenant(tenant_id)` returns the `Token[UUID | None]` from
`ContextVar.set()`, and the two context managers — `tenant_scope()` (async) and
`tenant_scope_sync()` (sync) — set the variable on entry and `reset(token)` on exit.
Token-based reset rather than "set back to `None`" is what makes nesting correct: an
inner scope restores the *outer tenant*, not the absence of one. Reset happens in a
`finally`, so an exception inside the scope still restores the previous tenant.

`clear_tenant_context()` exists for manual request-boundary teardown, but it does
`tenant_context.set(None)` — it does not restore a previous value. Prefer the scope
managers.

#### Propagation semantics: inherited by tasks spawned inside a scope, isolated per concurrent task and per thread

This is standard `contextvars` behavior, and the unit tests in
`tests/unit/multitenancy/test_context.py` pin it:

- A task created with `asyncio.create_task()` *inside* a scope copies the context at
  creation time and sees the tenant.
- Concurrently running tasks that each enter their own scope do not observe each other's
  tenant, even when interleaved by `await`.
- A tenant set inside a task does not leak back to the spawning context after the task
  completes.

The same isolation applies across threads: each thread has its own context, so a tenant
set on the event loop thread is *not* visible inside `run_in_executor` work.

#### `get_current_tenant()` (nullable) vs `get_required_tenant()` (raises `TenantContextNotSetError`)

Two accessors, deliberately:

- `get_current_tenant() -> UUID | None` never raises. It is for code that behaves
  differently with and without a tenant — for example a projection's dynamic
  `tenant_filter`.
- `get_required_tenant() -> UUID` raises `TenantContextNotSetError` when unset. It is
  for code that has no correct behavior without a tenant. Every enforcement point in
  this design calls this one.

Having both means "tenancy is optional" and "this operation requires a tenant" are
expressed at the call site rather than by convention.

### 2. Tenancy on events is opt-in at the type level via `TenantDomainEvent`

#### Base `DomainEvent.tenant_id` stays optional; `TenantDomainEvent` narrows it to a required `UUID`

`TenantDomainEvent` subclasses `DomainEvent` and redeclares the field:

```python
tenant_id: UUID = Field(..., description="Tenant this event belongs to (required)")
```

Pydantic field overriding does the work: the type narrows from `UUID | None` to `UUID`
and the default disappears, so constructing the event without a tenant is a
`ValidationError` at the model boundary rather than a runtime surprise three layers
down. Single-tenant users keep the permissive base class; multi-tenant users get the
strict one by inheritance, with no configuration flag involved.

#### `with_tenant_context()` auto-populates from context; an explicit `tenant_id` kwarg always wins

The classmethod is intentionally minimal:

```python
if "tenant_id" not in kwargs:
    kwargs["tenant_id"] = get_required_tenant()
return cls(**kwargs)
```

An explicit `tenant_id` always takes precedence over the ambient scope. This is the
escape hatch for admin and migration tooling that legitimately writes on behalf of a
tenant other than the one bound to the current scope — and it is also the hole that
Decision 4 exists to close on the write path, since an explicit mismatched `tenant_id`
will be rejected by `TenantAwareRepository.save()`.

### 3. Enforcement lives in a composition wrapper, not in the store

#### `TenantAwareRepository` wraps `AggregateRepository` rather than subclassing or modifying `EventStore`

`TenantAwareRepository[TAggregate]` holds an `AggregateRepository[TAggregate]` and
delegates `save`, `load`, `exists`, `load_or_create`, and `create_new`. It is a plain
`Generic`, not a subclass of `AggregateRepository`, and it exposes the wrapped instance
via a `repository` property so callers can still reach snapshot and publishing
configuration.

Composition rather than inheritance means the enforcement layer is opt-in per repository,
is trivially removable, and cannot drift when `AggregateRepository` gains behavior.
It also means it is *not* a drop-in `AggregateRepository` for type-checking purposes —
code annotated against `AggregateRepository` will not accept the wrapper.

#### `validate_on_save=True` by default; `enforce_on_load=False` by default

Both flags are keyword-only. The defaults encode the honest asymmetry of this design:
the write path is enforced because the library controls it, and the read path is not,
because the library cannot enforce it (see Decision 6). Choosing the wrapper at all
gets you write protection; read protection is something you configure elsewhere.

### 4. Validation happens on write only

#### `_validate_tenant_consistency()` compares each uncommitted event's `tenant_id` against `get_required_tenant()` before delegating to `save()`

When `validate_on_save=True`, `save()` calls `_validate_tenant_consistency(aggregate)`
before `await self._repository.save(aggregate)`. That method resolves
`get_required_tenant()` first — so saving with no tenant context raises
`TenantContextNotSetError` even when every event is fine — then iterates
`aggregate.uncommitted_events`, reading each event's tenant with
`getattr(event, "tenant_id", None)`.

Validation is pre-persistence and all-or-nothing: nothing is appended if any event
mismatches.

#### Mismatches raise `TenantMismatchError` carrying `expected`, `actual`, and the offending `event_ids`

The scan collects *all* mismatched `event_id`s but reports only the *first* mismatched
tenant as `actual`. The raised `TenantMismatchError` exposes `.expected`, `.actual`, and
`.event_ids` as attributes, and formats the first five ids into the message with an
"... and N more" suffix. A `logger.warning` is emitted at the same point, so a mismatch
is visible in operational logs even if the caller swallows the exception.

### 5. Events without a `tenant_id` silently pass validation

#### Rationale: mixed tenant/non-tenant event streams, incremental adoption, and migration from single-tenant deployments

The loop skips events whose tenant is `None`:

```python
event_tenant = getattr(event, "tenant_id", None)
if event_tenant is None:
    continue
```

Three cases make this the pragmatic choice. Aggregates may legitimately emit
system-level events with no tenant. Applications adopting multi-tenancy convert event
types one at a time and need the un-converted ones to keep working. And single-tenant
deployments migrating to multi-tenant have an entire history of `tenant_id=None` events
that must remain replayable and re-savable.

#### Consequence: a typo'd or plain `DomainEvent` subclass bypasses isolation entirely — use `TenantDomainEvent` to make the field non-optional

The cost is that *permissiveness is the failure mode*. An event class that inherits
plain `DomainEvent` and is constructed without a `tenant_id` will be written inside any
tenant scope without complaint. There is no warning, no log line, and no counter. The
only defense is the type system: derive multi-tenant event types from
`TenantDomainEvent` so the field cannot be omitted, and treat any multi-tenant event
class inheriting `DomainEvent` directly as a review finding.

### 6. `enforce_on_load` is deliberately incomplete: it asserts context, it does not filter

#### What it actually does on `load()`, `exists()`, and `load_or_create()` — calls `get_required_tenant()` and nothing more

With `enforce_on_load=True`, each of these three methods calls `get_required_tenant()`
(raising `TenantContextNotSetError` when unset), logs at debug in the `load()` case, and
then delegates unchanged. The resolved tenant is not passed to the repository, not
passed to the store, and not compared against the events that come back.

The guarantee is therefore precisely: *this read happened inside a tenant scope*. It is
not: *this read returned only that tenant's events*. Given an aggregate id belonging to
tenant B, a `load()` inside tenant A's scope succeeds and returns B's aggregate.

#### Why filtering was not implemented in the wrapper (would require a `tenant_id` parameter on `EventStore` read paths across every backend)

The wrapper cannot fix this by itself. `AggregateRepository.load()` reads through
`EventStore.get_events(aggregate_id, aggregate_type=..., from_version=...)`, which has
no tenant parameter — tenant filtering exists on the newer streaming paths
(`ReadOptions.tenant_id`, consumed by `read_stream`/`read_all`, and `get_events_by_type`)
but not on the aggregate-load path. Adding it there would mean changing the `EventStore`
interface, implementing and testing the filter in PostgreSQL, SQLite, and in-memory
backends, and breaking every third-party implementation — and doing it in a way that
also covers snapshot loading, which restores state that was itself derived from events.

Filtering after the fact in the wrapper was rejected as worse than nothing: discarding
mismatched events would silently produce a *partially replayed aggregate*, which is a
more dangerous object than a correctly replayed one belonging to another tenant.

#### `create_new()` is unguarded by design (in-memory only, no persistence)

`create_new()` delegates straight through with no tenant check even when
`enforce_on_load=True`. It allocates an in-memory aggregate at version 0 and touches no
storage; the tenant check that matters happens when the resulting events are saved.

### 7. Read isolation is delegated to the database layer

#### PostgreSQL Row-Level Security as the intended mechanism

Because read isolation must hold for every query — including projection rebuilds,
maintenance scripts, and analytics that never pass through `TenantAwareRepository` — it
belongs below the library, at the database. PostgreSQL Row-Level Security enforces the
predicate inside the engine, so a query that forgets the `WHERE` clause returns nothing
rather than everything.

#### What operators must configure themselves; what the library does not do for you

The library ships **no** RLS support: no policy DDL in `migrations/`, no session-variable
wiring, no connection-level `SET` of a tenant GUC from `tenant_context`. Operators who
want read isolation must enable RLS on the events table themselves, author policies
against their own tenant column, ensure the application role is not `BYPASSRLS` (note
that table owners bypass RLS unless `FORCE ROW LEVEL SECURITY` is set), and propagate
the current tenant into each session or transaction. SQLite and in-memory stores have no
equivalent mechanism at all — treat them as development and test backends in
multi-tenant applications.

## Security Consequences

### Threat: cross-tenant read via a shared aggregate id when RLS is not configured

The default configuration (`enforce_on_load=False`) and the strict one
(`enforce_on_load=True`) provide the *same* read isolation: none. Any code path that can
supply an aggregate id belonging to another tenant — an IDOR in a route handler, a stale
id in a webhook payload, a foreign key copied between environments — reads that
aggregate successfully. If the database layer is not enforcing the boundary, no layer is.

### Threat: unset context in background tasks, retries, and subscription runners

`ContextVar` inheritance is by *task creation*, not by logical relation. Work that is
enqueued and executed later — an outbox drain, a retry after a delay, a subscription
runner that consumes from a queue, anything handed to a thread pool — runs with an empty
tenant context unless the tenant is captured with the work item and re-entered via
`tenant_scope()` on the far side. Under `validate_on_save=True` this fails loudly with
`TenantContextNotSetError`, which is the good outcome. The dangerous variant is a
long-lived worker that entered a scope once and processes items for several tenants
inside it: every write then validates against the wrong, stale tenant. Enter the scope
per work item, not per worker.

### Threat: non-tenant events written inside a tenant scope

Following directly from Decision 5: a `DomainEvent` subclass with no `tenant_id` written
inside tenant A's scope produces a permanent, unattributed record. It will pass a
projection's tenant filter as well (`projections/base.py` processes events with no
`tenant_id` regardless of filter, for the same legacy-compatibility reason), so it will
appear in *every* tenant's read model. Because the log is append-only, this cannot be
corrected by an update — only by a data migration or compensating events.

### Non-goals: this model is not a substitute for authorization

The tenant context answers "on whose behalf is this running", not "is this actor allowed
to do this". Nothing here authenticates the tenant id, checks that the caller may act
for it, or distinguishes roles within a tenant. The value in `tenant_context` is exactly
as trustworthy as the code that called `set_current_tenant()` — typically middleware
reading a validated token. Authorization remains entirely the application's
responsibility.

## Alternatives Considered

### Explicit `tenant_id` parameter threaded through every store and repository call

Rejected. It is the most auditable option — the tenant is visible at every call site and
cannot be silently absent — but it changes every public read and write signature, forces
single-tenant users to pass `None` everywhere, and turns every intermediate helper into a
tenant-forwarding function. In an async codebase the ambient mechanism also composes with
middleware in a way parameters do not. The chosen model keeps this option available
locally: `get_required_tenant()` at a boundary converts ambient back to explicit.

### Tenant-per-schema or tenant-per-database physical separation

Rejected as a library-level model. Physical separation gives the strongest isolation and
would have made Decisions 4-7 unnecessary, but it pushes connection routing, per-tenant
migration execution, and cross-tenant querying into the library, and it scales poorly
past a few hundred tenants. It remains a perfectly good *deployment* choice — nothing
here prevents running one event store per tenant, in which case the wrapper is redundant.

### Enforcing filtering inside `EventStore` implementations

Rejected for now, and it is the strongest of the alternatives. Enforcement at the store
would cover every read the library performs, including projection and subscription
paths. The cost is the one described in Decision 6: an interface change across every
backend, a correctness question for snapshots, and a hard break for third-party
implementations. The partial version already exists in `ReadOptions.tenant_id` for the
streaming paths, which is filtering the caller *opts into*, not isolation the store
*enforces*. Should this be revisited, the interface change is the whole of the work.

### Rejecting events that lack a `tenant_id`

Rejected as the default, for the migration and mixed-stream reasons in Decision 5. Note
there is no flag for it: `validate_on_save` toggles between "validate events that have a
tenant" and "validate nothing". Applications wanting strict rejection get it structurally
by deriving from `TenantDomainEvent`, which makes the omission impossible rather than
merely detectable.

## Adoption Guidance

### Recommended configuration for new multi-tenant applications

For a new application with no legacy history, use every layer of the model at once:

- Derive all domain events from `TenantDomainEvent` and construct them with
  `with_tenant_context()`.
- Wrap each `AggregateRepository` in
  `TenantAwareRepository(repo, enforce_on_load=True, validate_on_save=True)`.
- Enter `tenant_scope()` once per request in middleware, and again per work item in every
  background worker, retry path, and subscription handler.
- Configure PostgreSQL RLS on the events table. Without it, `enforce_on_load=True` buys
  you a missing-context assertion and nothing more.

### Relaxed configuration during migration (`validate_on_save=False`)

When converting a single-tenant deployment, `TenantAwareRepository(repo,
validate_on_save=False)` delegates `save()` with no checks at all, letting you install
the wrapper before the events and the context plumbing are ready. Treat it as a
temporary state and sequence out of it: introduce `tenant_scope()` at request
boundaries, convert event types to `TenantDomainEvent` one aggregate at a time, then flip
`validate_on_save` back to `True` — at which point mismatches and missing context become
loud. Turning on `enforce_on_load=True` is the last step, and it is only meaningful once
RLS is in place.

## Related Documents

### Stores, subscriptions, and projections tenant-filter docs (cross-reference this ADR rather than restating the model)

- `docs/guides/multi-tenant.md` — the task-oriented walkthrough of wiring context,
  events, and the repository wrapper together.
- `docs/api/projections.md` and `src/eventsource/projections/README.md` — the
  `tenant_filter` parameter (static `UUID` or callable such as `get_current_tenant`) and
  its own legacy-event pass-through rule.
- `src/eventsource/subscriptions/README.md` and `src/eventsource/stores/` —
  `ReadOptions.tenant_id` on the streaming read paths.

Those documents describe *mechanisms*; the reasoning for why enforcement is where it is,
and why the read path is incomplete, lives here and should be linked rather than
duplicated.

### ADR-0002 (Pydantic Event Models), ADR-0005 (API Design Patterns)

ADR-0002 explains why `DomainEvent` is a frozen Pydantic model — the reason
`TenantDomainEvent` can narrow `tenant_id` from `UUID | None` to `UUID` by redeclaring
the field, and why validation of that field happens at construction rather than at save
time. ADR-0005 covers the API conventions this design follows: keyword-only configuration
flags with conservative defaults, composition wrappers over subclassing, and paired
nullable/raising accessors like `get_current_tenant()` / `get_required_tenant()`.
