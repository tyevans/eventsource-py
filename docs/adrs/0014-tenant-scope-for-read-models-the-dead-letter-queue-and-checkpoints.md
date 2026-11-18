# ADR 14: Tenant Scope for Read Models, the Dead Letter Queue, and Checkpoints

Status: Accepted (2026-11-17, TASK-0474). Extends ADR-0113.

## Summary
Read models, the dead letter queue (DLQ), and projection checkpoints have no
tenant predicate in their ports. The framework applies no tenant filter to
them. This decision sets one policy for each surface. Read models get a
repository wrapper that applies the active tenant under a scope. The DLQ gets
an additive owner column, and its reads filter under a scope. Checkpoints keep
their key. Per-tenant projection instances must use a unique projection name.
No surface changes behavior when no scope is active.

## Context
ADR-0110 names storage-level tenant filtering as a goal. ADR-0113 closed the
gap for event streams. Three other surfaces hold tenant data and have no
equivalent control. The current facts are:

1. **Read models.** The `ReadModel` base class has no tenant field. The
   repository port exposes `get`, `find`, `count`, `save`, `delete`, and
   `truncate` by id or by equality filter. Nothing binds these calls to the
   active scope. A save uses `ON CONFLICT (id) DO UPDATE` on the PostgreSQL and
   SQLite adapters. A scoped caller that saves a model with another tenant's id
   therefore overwrites that tenant's row. A read of such an id returns the row.
   `truncate` removes every row, including rows of other tenants. Read models
   are the usual query surface of a SaaS application, so this surface carries
   the highest risk of the three.

2. **The DLQ.** A DLQ entry has an event id, a projection name, and the full
   event data as JSON. The table has no tenant column. The repository port
   reads entries by a sequential id and has no tenant parameter. The event
   data holds the tenant of the event, but no query can use that field without
   parsing JSON. The DLQ is an operator surface. Its exposure is smaller than
   the read model exposure.

3. **Checkpoints.** A checkpoint row is keyed by the projection or subscription
   name. Two projection instances with the same name share one row. A projection
   with a fixed tenant filter skips events of other tenants, and the checkpoint
   still advances past them. Two per-tenant instances that share a name can
   therefore skip each other's events. This is a correctness defect with low
   security weight. The multi-tenant guide refers to a section that tells the
   reader to run one instance per tenant with a unique name. That section does
   not exist.

4. **The existing contract.** ADR-0113 added an ownership check only to
   `TenantAwareRepository`. It applies only when a scope is active. Single-tenant
   users never set a scope, so their behavior does not change. The same rule
   applies here.

The forces on this decision are:

- Single-tenant users must see no change. They must not need a data migration.
- The control must live in one place. A fact stored or enforced in several
  adapters drifts. Each adapter then gives its own answer.
- The read model table shape belongs to the application. The framework must
  not add a column to every read model.
- A change to a port method affects every adapter and every third-party
  implementation of that port.

## Decision
1. **Read models: an application wrapper, not an adapter predicate.** A new
   class, `TenantScopedReadModelRepository`, wraps any `ReadModelRepository`.
   The application declares a read model tenant field by name. The field name
   is `tenant_id`. The wrapper is in the application ring. It uses only the
   public port methods. It adds no column and it changes no adapter.

2. **Read models: the wrapper applies the scope to every method.** When a scope
   is active:
   - Reads (`get`, `get_many`, `get_deleted`, `find_deleted`, `exists`, `find`,
     `find_one`, `count`) add `tenant_id = active tenant` as an equality
     filter. A row with a missing or different tenant is not found.
   - Writes (`save`, `save_many`, `save_with_version_check`) check the owner of
     each id before the write. The check includes soft-deleted rows. A save
     that would change the owner of an id is refused. A model whose tenant is
     not the active tenant is refused.
   - Deletes (`delete`, `soft_delete`, `restore`) check the owner first.
   - `truncate` is refused under a scope.

3. **A foreign row looks like a missing row on reads and on deletes.** The
   reads return `None`, `False`, an empty list, or `0`. Deletes return `False`.
   No "foreign" error type exists for these paths. This follows ADR-0113,
   item 4.

4. **A save to a foreign id raises an error.** The owner check on save needs
   to know whether the id exists. The error reveals that the id exists for the
   other tenant. This is the same class of residual risk as the event-id
   oracle in ADR-0113, item 10. It is accepted. The error is
   `TenantMismatchError`, which already exists in the domain exceptions.

5. **`require_tenant_context` keeps the ADR-0113 meaning.** When it is set and no
   scope is active, the wrapper raises `TenantContextNotSetError`. It does not
   turn the scope checks on or off.

6. **No scope means no change.** With no active scope and the flag unset, every
   method passes straight to the wrapped repository.

7. **Fail closed on rows with no tenant.** A row with a null `tenant_id` is
   invisible under a scope. The wrapper does not treat null as shared data.
   This differs on purpose from the tenant filter on projections, which passes
   untenanted events. Read models are stored state, not event flow. An adopter
   that has pre-tenancy rows must assign them an owner before it adopts the
   wrapper.

8. **DLQ: an additive owner column.** The DLQ table and the `DLQEntry` value
   gain a nullable `tenant_id`. The write path takes the tenant from the event
   that failed. The column is the one stored copy used for queries. The event
   data JSON keeps the event as it is. Under an active scope, the DLQ reads and
   the DLQ state changes (`get_failed_events`, `get_failed_event_by_id`,
   `mark_resolved`, `mark_retrying`, `get_failure_stats`,
   `get_projection_failure_counts`) match only rows of the active tenant. The
   DLQ cleanup (`delete_resolved_events`) is refused under a scope. This
   policy applies to the memory and SQL adapters through one shared rule. The
   conformance suite pins the rule. Rows with a null tenant are invisible
   under a scope, as in item 7.

9. **DLQ: the change is later than the read model change.** The DLQ work is a
   second wave. It is Low severity. The port gains optional parameters. Third-
   party implementers of the DLQ protocol get a new keyword. This is documented
   as a minor-version change.

10. **Checkpoints: no schema change, no key change.** The checkpoint key stays
    the projection or subscription name. The `projection_checkpoints` table
    keeps its primary key. A projection instance that serves one tenant with a
    fixed tenant filter must use a name that includes the tenant. The name is
    the projection name, a separator, and the tenant id. The guide documents
    this rule and gives the example. The name must stay within the 255-character
    column limit. A projection with a callable filter resolves its tenant per
    event, so it cannot have one name per tenant. Its owner must run one
    instance per tenant with a fixed filter, or accept the shared checkpoint
    and document it. The guide states this choice.
    The checkpoint advances over every event that a tenant filter skips. The framework
    keeps this behavior, and the name rule is the only guard against it.

11. **Scope of the guarantee.** This decision covers the wrapper and the DLQ
    repository methods only. It does not cover direct adapter access, unscoped
    repositories, the global event feed, the global uniqueness of event ids, or
    checkpoint rows. The multi-tenant guide states these limits in its
    Limitations section.

12. **Schema.** The read model tables do not change in the framework. The
    adopter adds a `tenant_id` field to its own models and migrates its own
    tables. The DLQ table gains one nullable column through an additive update
    script for PostgreSQL and SQLite. Existing DLQ rows get no tenant. Under a
    scope they are invisible.

### Amendment to ADR-0110

The Consequences bullet on tenant validation is extended by this text:

> Under an active scope, read models through `TenantScopedReadModelRepository`
> and DLQ reads and state changes through the DLQ repository match only rows
> of the active tenant (ADR-0114). Checkpoints are not tenant-scoped. Each
> per-tenant projection instance uses its own name. This is an application-level
> control. It does not cover direct adapter access or the global feed.

The original Decision text of ADR-0110 is not changed. ADR-0110 Status gets an
"Amended by ADR-0114" pointer, in addition to the ADR-0113 pointer.

## Rejected Alternatives

- **Opt-in tenant column with an adapter predicate (option a).** Each adapter
  would read a tenant marker on the model class and add a predicate to every
  query. This moves the control into three adapters. Each adapter would need its
  own proof of the same rule. The rule is also hidden in the adapter code. The
  wrapper gives one implementation and uses only public methods. It is the
  smaller change.

- **Documented responsibility only (option b without the wrapper).** Leaving
  the filter to each call site repeats the current state. The defect class in
  ADR-0113 is that one missed call leaks data. A wrapper makes the missed call
  impossible for code that holds the wrapper.

- **A tenant column on the `ReadModel` base class.** This changes every read
  model table and every model constructor. Single-tenant users would see a new
  column. The change is not needed for the wrapper.

- **A tenant column on the checkpoint table with a composite primary key.**
  This changes the primary key on every backend. It needs a migration on
  large tables. The defect is Low severity. The name rule in item 10 gives the
  same isolation with no schema change.

- **Parse the tenant from the DLQ event data JSON.** This keeps a second copy
  of the tenant and forces each adapter to parse JSON in its own way. The
  owner column is one stored copy.

- **Silent empty results on a foreign save.** A save that does nothing hides
  the mismatch. The caller would believe its write succeeded. The error keeps
  the caller's view correct, as in ADR-0113.

## Consequences

Positive:

- Scoped code cannot read, overwrite, or delete another tenant's read model
  row through the wrapper.
- Scoped code cannot reach another tenant's DLQ rows through the DLQ reads and
  state changes.
- Single-tenant users see no change. They never set a scope.
- The read model tables and the checkpoint table do not change in the framework.
- The DLQ change is additive.
- The DLQ owner is set on the first insert. A retry of the same event and
  projection cannot move the entry to another tenant.

Negative, each with an owner:

- Under a scope, each read adds an equality predicate. Each save adds one
  read of the existing row, which includes soft-deleted rows. Owner: the
  read model follow-up tasks.
- A save to a foreign id reveals that the id exists for another tenant. Owner:
  accepted residual risk. The guide's Limitations section states it.
- Rows with no tenant are invisible under a scope. Adopters with existing
  tables need a backfill. Owner: the multi-tenant guide, adoption section.
- A checkpoint advances over events that a tenant filter skips. Projections
  that share a name share one row, so one can move the row past events that the
  other must handle. A callable filter has the same exposure. The rule is
  documented, not enforced. Owner: the multi-tenant guide. A runtime check is a
  later decision.
- The PostgreSQL DLQ adapter runs without the owner column until the operator
  runs update script 007. Entries written then have no owner, and scoped reads
  return no entries. The adapter logs one warning for each repository instance.
  Owner: the multi-tenant guide, upgrade notes.
- Retention cleanup of the DLQ (`delete_resolved_events`) is refused under a
  scope. An operator runs it outside any scope. Owner: the multi-tenant guide.
- `truncate` under a scope is refused. An operator that needs a full truncate
  must run it outside any scope. Owner: the multi-tenant guide.
- Third-party DLQ implementers must accept the new keyword. Owner: the
  changelog entry for the DLQ wave.

## ADR Impact

- ADR-0110: amended (Consequences). Status pointer added.
- ADR-0113: builds on it. The ownership rule for streams is unchanged.
- ADR-0018: stands. The write-side event validation is unchanged.
- PRD-0002: amended by outcome 9, which covers read model and DLQ scope.
- US-0005: stands. Acceptance criteria follow-up.
- US-0010: acceptance criteria follow-up for read models and checkpoints.
