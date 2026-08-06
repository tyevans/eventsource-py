# 0057. Tenant Load Enforcement Is a Precondition, and Says So

`TenantAwareRepository`'s `enforce_on_load` flag enforced nothing about what
was loaded. It is renamed `require_tenant_context`, and the limitation now
appears at the point of use rather than only in an ADR section.

## Status

**Accepted.**

**ADR Impact**

| ADR | Status |
| --- | --- |
| [0018](0018-tenant-isolation-model.md) | Amended — the flag it describes is renamed, and the reasoning in its §6 for why filtering was not implemented argues from an `EventStore` interface that no longer exists. Its conclusion is unchanged and restated here on current grounds; the isolation model itself stands. |
| [0030](0030-top-level-module-ring-consolidation.md) | Stands. Applied: the rename lands without an alias. |
| [0038](0038-multitenancy-dissolution.md) | Stands. The wrapper stays where that relocation put it. |

## Context

Downstream consumers have repeatedly read `enforce_on_load=True` as read
isolation, discovered it is not, and reported it as a bug. It is not a bug —
ADR 0018 §6 records the incompleteness deliberately — but a setting whose name
promises enforcement of loads, and which enforces only that the caller is
inside *some* tenant scope, is a name that has to be corrected by
documentation the reader only consults after being surprised. This project has
paid for that shape before (ADR 0050).

ADR 0018 §6 justified the absence of filtering by pointing at
`EventStore.get_events(aggregate_id, aggregate_type=..., from_version=...)`
and at `ReadOptions.tenant_id` existing only on the newer streaming paths.
Neither API survives. The aggregate-load path today runs through
`AggregateStore.read_stream(StreamId, StreamReadOptions)`, and the options
objects have since been split by read shape: `FeedReadOptions` and
`CategoryReadOptions` both carry `tenant_id`; `StreamReadOptions` does not.
So the surface facts the old argument rested on are stale, while its
conclusion — no filtering on the aggregate-load path — still describes the
tree.

Restating the case on current ground gives a stronger reason than the original
one. A stream is a single aggregate, and an aggregate belongs to one tenant.
Tenancy is therefore a property of the *stream*, not of the individual events
within it: a per-event tenant predicate on a stream read is not a filter, it is
an ownership check wearing a filter's clothes. Adding `tenant_id` to
`StreamReadOptions` would express the check in the one shape that cannot mean
what it should — in the mismatch case it returns a subset, and the repository
replays that subset into a *partially reconstituted aggregate*. That is the
outcome ADR 0018 rejected as worse than nothing, and pushing the predicate down
into the query does not change it; it only removes the consumer's chance to
notice.

Two further facts close off the cheap alternatives. Nothing carries an
aggregate's tenant outside its events: `AggregateRoot` has no `tenant_id`, and
neither does `Snapshot`. So the wrapper cannot check ownership after the fact,
because by the time `load()` returns, the events are gone and only state
remains. And a snapshot restores state derived from events without reading
them at all, so any events-only filter is bypassed exactly when an aggregate is
large enough for someone to care.

## Decision

**No filtering, and no tenant parameter on the stream-read path.** Read
isolation continues to be delegated to the storage layer, as ADR 0018 §7
decided. An all-or-nothing stream-ownership check is the shape that would be
correct, and it requires a notion of stream ownership the library does not have
and a tenant on the snapshot record; neither is in scope here, and a
half-measure that returns partial aggregates is worse than the documented gap.

**Rename `enforce_on_load` to `require_tenant_context`.** The new name states
the precondition it actually imposes — a tenant scope must be active — and
promises nothing about the result of the read. Per ADR 0030 there is no alias:
the old keyword raises `TypeError`, which is the point, because silent
acceptance would preserve the misreading the rename exists to end.

**State the limitation where it is read.** The class docstring, the `load()`,
`exists()`, and `load_or_create()` docstrings, the multi-tenancy tutorial, and
the multi-tenancy API guide each say that reads are not isolated and that
isolation must come from the database. A reader who never opens an ADR now
learns this before relying on the flag.

**Pin the non-guarantee in tests.** Unit tests assert, against a real store,
that a load inside one tenant's scope returns another tenant's aggregate fully
replayed. Asserting an absence of protection is unusual, and deliberate: it
makes any future change that begins filtering reads announce itself here.

## Consequences

Callers passing `enforce_on_load=` get a `TypeError` and a one-word fix. The
runtime behavior of the wrapper is unchanged — nothing that was prevented
before is now allowed, and nothing that was allowed is now prevented.

The security posture is unchanged and now honestly labelled. What the library
guarantees on reads is: *this read happened inside a tenant scope*. What it
does not guarantee is: *this read returned only that tenant's data*. That
second property must come from PostgreSQL row-level security or physical
separation, exactly as before.

The correct primitive is now recorded rather than merely absent. If read
enforcement is picked up later, this ADR names its shape — an ownership check
on the stream, all-or-nothing, covering the snapshot path — so that the next
attempt does not rediscover the partial-replay trap by building it.
