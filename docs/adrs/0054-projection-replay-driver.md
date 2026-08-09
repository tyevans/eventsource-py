# 0054. Rebuilding a Projection Is a Foreground Driver, Not a Subscription

The library could poll a feed and fan events out to projections forever. It
could not answer "rebuild this projection from the log and tell me how it
went" without the caller writing the loop.

## Status

**Accepted.**

**ADR Impact**

| ADR | Status |
| --- | --- |
| [0019](0019-clean-architecture-store-ports.md) | Stands, and is what makes this possible with one port. `replay` type-hints `GlobalEventFeed` alone — it appends nothing, reads no stream, looks up no event id — and the segregated ports let it say so. |
| [0024](0024-projection-persistence-ports.md) | Stands. `replay` does not checkpoint, does not read a checkpoint, and touches neither the DLQ nor the checkpoint ports. It returns the last position it reached and leaves persisting it to the caller, who is the one who knows whether the rebuild is to be resumed. |
| [0041](0041-infrastructure-exceptions-to-ports.md) | Stands, and decides where the new error goes. A projection refusing an event is a domain-ring `ProjectionError`, not a port failure: no store, bus, lock, or checkpoint failed. The infrastructure taxonomy is the wrong home, and a fresh root would put a caller's `except ProjectionError` in the position of not catching a projection error. |
| [0048](0048-failure-paths-report-and-retain.md) | Extended. Its rule — a failure path reports honestly and retains what it cannot handle — is why a bounded failure list carries a count of what the bound dropped. A cap that truncated silently would satisfy the letter of "retain" while restoring the exact defect the retention exists to prevent. |
| [0052](0052-feed-read-aggregate-type-filter.md) | Stands, and is consumed here. `aggregate_type=` is forwarded rather than reimplemented, which is what lets a rebuild scope to one category without `replay` accepting a narrower port than `GlobalEventFeed`. |

## Context

A live subscription runner polls on a timer and fans new events out to
registered projections via `ProjectionCoordinator`/`ProjectionRegistry`. That
is live catch-up: an ongoing background activity whose correct response to a
failure is to stop and not checkpoint past it, so the event is not skipped on
restart.

A rebuild is the other job, and every property differs. It is a foreground
operation someone is waiting on, over a log that is already written, whose
correct response to a failure is the opposite: the bad event has already been
recorded, and stopping would deny the projection every event after it. One
poison event would leave the read model permanently truncated at the poison,
which is worse than the hole the poison itself makes.

The coordinator's `rebuild_projection` does not close this gap. It takes the
events as a materialized list, so the caller has already read the feed, held
it in memory, and filtered it — which is the entire hard part, and the part
that has to know about positions, scoping, and what to do when a projection
refuses.

So consumers wrote the loop. The version upstreamed here came from one, and
the shape it converged on is what this ADR records.

## Decision

`replay(feed, projections, ...)` reads the global feed from a position and
folds it into every projection, returning a `ReplayReport`.

### A rejection is recorded, not raised

The default catches the exception, records it, and continues. `strict=True`
raises `ReplayFailedError` at the first rejection instead — offered because it
is the common case for a test or a first deployment, where a silent partial
rebuild is most costly and least visible, not because it is hard to write.

### The report names the event, and the count is derived

`ReplayReport.failures` carries the position, event id, event type, the
rejecting projection's name, and the exception object itself. A count alone is
safe and useless in the same breath: an operator told "3 events failed" has no
route from that message to the poison event, and the exception that would have
supplied one was discarded inside the `except`. A caller can always turn
detail into a raise; no caller can turn a count back into detail.

`failed` is a property derived from the distinct event ids in `failures`, not
a field counted alongside it. Two projections can reject one event, and the
two numbers then legitimately differ: `failed` answers "how much of the log did
not reach the read models", which is per event, while `failures` has one entry
per refusal because that is what names the projection to fix. Counting both is
how they drift apart; deriving one means they cannot.

The derivation keys on `event_id` rather than on `position`, which is the
intuitive choice and the wrong one. `position` is optional by contract —
`ReplayFailure` keeps it `Position | None` deliberately, because the rebuild
path's job is not to crash on an adapter that supplies none — so keying on it
would fold every failure of a feedless rebuild into a count of one. A count
that undercounts silently is the same dishonesty `failures_truncated` exists
to prevent, one level down.

The rejecting projection is recorded by class name rather than by reference. A
report is something an operator reads or logs, and holding the live projection
would make the report a handle into the read model.

### The failure list is bounded, and says when it bound

Each retained failure pins a live exception, and through its `__traceback__`
every frame's locals. Over a rebuild that fails at scale, an unbounded list is
a memory hazard rather than a diagnostic. `max_failures` caps what the report
retains and `failures_truncated` counts what the cap dropped.

Reporting the drop is not decoration. A silent truncation reproduces exactly
the defect the `failures` field exists to fix — an operator told "N failed"
who cannot reach the Nth — and would do it while appearing to have fixed it.
An optional `on_failure` hook fires for every failure regardless of the cap,
so a caller who needs all of them streams them somewhere that is not memory.

### Scoping narrows the read, not the delivery

`tenant_id=` and `aggregate_type=` are forwarded as `FeedReadOptions` and
pushed into the adapter's query. Naming neither sends no options object at
all, which is what `read_all` documents as unfiltered.

This is deliberately narrower than filtering after delivery, which the
projection base class already offers: a post-delivery filter is correct and
reads the whole log anyway, discarding most of it in the consumer. In a store
shared across tenants or aggregate types, that is the difference between a
scan and an indexed read on every rebuild. Both filters produce the same
answers, which is why the tests assert on what the adapter was asked for
rather than only on what came back.

### The read is bounded

The feed is adapter-supplied and the loop's termination depends on it. A
cursor that failed to advance would turn a rebuild into a hang, and a hang in
CI reads as infrastructure trouble and gets retried rather than investigated.
`max_events` turns that into a raise naming the last position reached.

## Consequences

A rebuild is now a library capability with a reportable outcome rather than a
loop each consumer writes, and the two jobs — live catch-up and rebuild — have
separate names with opposite failure behaviour, which is the distinction that
was previously left to the caller to know.

`replay` does not checkpoint. A caller who wants the rebuild to resume
persists `last_position` itself. This keeps the driver free of the checkpoint
ports, at the cost of one more step for callers who want resumption; a
`replay` that checkpointed would have to decide what to checkpoint after a
partial failure, which is precisely the question only the caller can answer.

`failed` is exact only while `failures_truncated` is zero, and a lower bound
otherwise. That is the honest reading of a capped list, and callers who need
an exact count past the cap use `on_failure`.

The function is named `replay` with no alias. The consumer it came from called
it `project` and aliased it, because "project" collided with a noun in its own
domain; upstream has no such collision, and a second name for one function is
a second thing to keep true.
