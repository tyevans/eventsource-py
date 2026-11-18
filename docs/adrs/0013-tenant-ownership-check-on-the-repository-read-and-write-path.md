# ADR 13: Tenant Ownership Check on the Repository Read and Write Path

Status: Accepted (2026-11-17, TASK-0464). Amends ADR-0110 (Consequences).

## Summary
When a tenant scope is active, `TenantAwareRepository` checks that a stream
belongs to the active tenant before it loads, probes, or appends to that
stream. A stream that belongs to another tenant looks the same as a stream
that does not exist. The stream key does not change in this decision. Tenant
identity in the stream key is deferred to a later major version.

## Context
ADR-0110 says the isolation model gives "bulletproof" cross-tenant defense.
That is not true of the code. The current facts are:

1. The stream key is the aggregate id plus the aggregate type. The stream key
   has no tenant. The unique key on the events table, and the unique key on
   the snapshots table, have no tenant either.
2. The write-side check in `TenantAwareRepository` compares the tenant of each
   uncommitted event with the active scope. It does not check who owns the
   stream. A tenant can therefore append events that carry its own tenant to
   a stream that another tenant created. This needs only the aggregate id.
3. Load does not check the tenant. Load reads the snapshot first and trusts
   it. It then reads only the events after the snapshot version. The state
   before the snapshot is never checked.
4. The existence check and the version check do not check the tenant. They
   reveal that a foreign stream exists and how far it has advanced.
5. Event ids are unique across the whole store. Reusing a foreign event id
   raises `DuplicateEventError`. A caller who knows a foreign event id can
   tell that the event exists.
6. A repository built with `require_tenant_context=False` does not check
   anything when no scope is active. This is intended for system code.

The exploit needs the victim's aggregate id. These ids are UUIDs and are hard
to guess. They often appear in URLs and logs, so the precondition is real.

The forces on this decision are:

- Single-tenant users never set a tenant. Their behavior must not change, and
  they must not need a data migration.
- Multi-tenant users need the read and write leak closed without a rewrite of
  stored events.
- A change to the stream key touches every store backend and the migration
  coordinator. On SQLite it needs a table rebuild.

## Decision
1. **Stream identity does not change in this ADR.** The stream key stays the
   aggregate id and the aggregate type. Tenant identity in the stream key
   (Option A) is deferred. See "Rejected Alternatives".

2. **Ownership.** The owner of a stream is the tenant of its events. A stream
   written in a tenant scope has one tenant on every event. For load, every
   event in the stream must carry the active tenant. For exists and for the
   version check, the owner is the tenant of the first event of the stream.

3. **When a scope is active, every scoped operation checks ownership.** This
   applies to load, get-or-none, get-or-raise, load-many, exists, get-version,
   load-or-create, execute, and save. The check runs whether or not
   `require_tenant_context` is set. It also runs whether or not
   `validate_on_save` is set.

4. **A foreign stream looks like a missing stream.** On a mismatch, the
   repository raises `AggregateNotFoundError` with the same arguments it uses
   for a missing stream. Existence returns `False`. The version returns `0`.
   No separate "foreign" error type exists.

5. **Snapshots carry the owner tenant.** A snapshot is used under a scope only
   when its tenant equals the active tenant. Any other snapshot is ignored, and
   the stream is replayed in full. A snapshot with no tenant is ignored under a
   scope. Unscoped loads use snapshots as they do today.

6. **Fail closed on untenanted events.** A stream that holds an event with no
   tenant, or an event of another tenant, is not owned by the active scope.

7. **Save under a scope checks the owner before append** when the aggregate
   already has a version above zero. A new aggregate (expected version zero)
   relies on the store's concurrency check. A foreign stream always has a
   version above zero, so the append fails with the store's standard
   concurrency error.

8. **`require_tenant_context` keeps its meaning for the no-scope case only.**
   When it is set and no scope is active, the repository raises
   `TenantContextNotSetError`. It does not turn ownership checks on or off.

9. **`create_new` stays unguarded.** It builds an object and does no I/O.

10. **Event ids stay globally unique.** Event ids are the deduplication key for
    the bus, the outbox, and event lookup. Making them unique per tenant would
    break those uses. The `DuplicateEventError` oracle is accepted as a known
    residual risk. It is tracked by a follow-up task.

11. **Scope of the guarantee.** This decision covers `TenantAwareRepository`
    only. It does not cover direct store access, unscoped repositories, the
    global feed, or projections.

12. **Schema.** The events table does not change. The snapshots table gains an
    optional tenant column. Existing snapshot rows get no tenant. Under a
    scope they are ignored, so each aggregate is replayed once and then
    snapshotted again. No data migration is required.

### Amendment to ADR-0110

The Consequences bullet "Bulletproof cross-tenant data leak defense in
multi-tenant SaaS environments" is replaced by this text:

> Write-side tenant validation on `TenantAwareRepository`. Under an active
> scope, the repository also refuses to load, probe, or append to a stream
> that another tenant owns (ADR-0113). This is a repository-level control. It
> does not cover direct store access, unscoped repositories, or the global
> uniqueness of event ids.

The original Decision text of ADR-0110 is not changed. ADR-0110 Status gets an
"Amended by ADR-0113" pointer. The same bullet appears in the copy of ADR-0010
under `docs/adrs/`. That copy gets the same replacement.

## Rejected Alternatives

- **Tenant in the stream key now (Option A).** Option A closes the aggregate-id
  oracle by construction. It also changes public types: the stream identity
  value object and the snapshot store port. On SQLite it needs a table rebuild
  to change the unique key. It changes the migration coordinator and the
  router. It also gives untenanted and tenanted code two different streams for
  the same id. The cost is high for a first fix. Option A is kept as the target
  for a later major version.

- **Per-tenant event id uniqueness.** This would close the event-id oracle. It
  would break the global identity that the bus, the outbox, and event lookup
  depend on.

- **Silent empty results on a foreign load.** A foreign load that returns an
  empty result would hide the mismatch from the caller. The caller would then
  create a new aggregate on top of a foreign id. The not-found error keeps the
  caller's view correct.

## Consequences

Positive:

- Scoped code can no longer read the state of another tenant through the
  repository. It also cannot probe or append to another tenant's stream.
- Single-tenant users see no change. They never set a scope.
- The events table does not change. The snapshot change is additive.
- The migration coordinator needs no change for this decision. It copies
  tenant events by tenant and does not copy snapshots.

Negative, each with an owner:

- Under a scope, `exists` and `get_version` add one indexed read each. `load`
  adds no query. A save with a version above zero adds one indexed read.
  Owner: TASK-0464 follow-up for the repository checks.
- A legacy stream can mix tenants. `exists` and `get_version` look only at the
  first event, so such a stream reports as existing with a version. `load`
  refuses it. Owner: follow-up audit task.
- Pre-tenancy streams with no tenant on any event are invisible to scoped
  code. Adopters need a step that assigns an owner. Owner: follow-up task.
- Event-id reuse across tenants still raises `DuplicateEventError`. The error
  reveals that a known event id exists. Owner: follow-up task that pins the
  behavior and documents it.
- The guarantee is repository-level. Direct store access stays unguarded.
  Owner: documented in the multi-tenant guide, Limitations section.

## ADR Impact

- ADR-0110: amended (Consequences). Status pointer needed.
- ADR-0057: stands. This ADR adds the ownership check after the load
  precondition that ADR-0057 defines.
- ADR-0018: stands. The write-side event validation is unchanged.
- PRD-0002: amended by a new checkable outcome for stream ownership (follow-up).
- US-0005: stands. Acceptance criteria follow-up.
