# ADR-0059. Subscription Delivery Is Ordered Per Subscription

A subscription delivers its events one at a time, in feed order, and does not
begin the next event until the current one has been handled and its position
recorded. Nothing in the tree said so. The runners were built around a flow
controller that could bound concurrent delivery, but no caller ever delivered
concurrently, so the bound never engaged: it reported a constant, and the
configuration that tuned it changed nothing. That made an invariant the
correctness of checkpointing depends on look like an unfinished performance
feature — something to be switched on, rather than a decision to be argued with.

This ADR records the invariant, the reasons it holds, the ways throughput may
be increased without breaking it, and what lifting it would actually cost.

## Status

**Accepted.** **Amended by
[ADR 0063](0063-live-batch-delivery-is-a-page-not-a-window.md)**, scoped to the
live half of the batch-delivery sanction below: 0063 supplies the pass this ADR
asked for and records the constraint that made it safe — the live batch is a
page the feed already returned, never an accumulator. Every constraint stated
here holds unchanged.

**ADR Impact**

| ADR | Status |
| --- | --- |
| [0047](0047-live-runner-feed-driven-checkpointing.md) | Amended — extended, not contradicted. 0047 established that the store owns ordering and that a checkpointed position is never re-read from the feed, and on that basis deleted catch-up→live duplicate suppression as unreachable by construction. That argument is load-bearing on sequential delivery: it holds only while position advance and delivery are lockstep. This ADR states the dependency 0047 left implicit and pins it, so the deletion cannot be silently invalidated by a later change to delivery. |
| [0007](0007-event-bus-delivery-semantics.md) | Stands. 0007 decides what the *bus* promises — at-least-once, handler isolation, no cross-aggregate ordering — and it already records that the in-memory bus dispatches an event only after the previous one has settled, while handlers *within* one event run concurrently. This ADR decides what a *subscription* promises. The two agree in shape and neither constrains the other; the bus's weaker promises are exactly why the store, not the bus, is the ordered source. |
| [0054](0054-projection-replay-driver.md) | Amended (Context prose only). 0054's Context opens by attributing timer-based polling to `ProjectionCoordinator`. The coordinator does not poll and holds no timer; that role belongs to a live subscription runner. The contrast 0054 draws — ongoing live catch-up that must stop and not checkpoint past a failure, versus a foreground rebuild that records a rejection and carries on — is correct and is the reason this correction is worth carrying rather than dropping: the live half of that contrast is precisely the subject of this ADR. The Decision, Consequences, and every other section of 0054 stand untouched. |
| [0021](0021-snapshot-policy-scheduler-composition.md) | Stands. Its recorded negative — background snapshot scheduling has no bound on pending tasks — is a real, separately-owned gap in a different mechanism. This ADR neither closes nor worsens it. |
| [0017](0017-snapshot-strategy-pattern.md) | Stands, for the same reason: the unbounded background-task negative it records is about snapshot scheduling, not subscription delivery. |
| [0009](0009-multi-instance-subscription-coordination.md) | Stands. Coordination distributes subscriptions across instances; this ADR governs delivery within one subscription on one instance. If partitioned lanes are adopted later, the relationship becomes worth restating, because a lane and an instance would then both be units of parallelism. |
| [0032](0032-subscriptions-ring-migration.md) | Stands. A ring-location record; it takes no position on delivery semantics. |
| [0013](0013-handler-registry-composition.md) | Stands. Handler discovery and registry composition are orthogonal to delivery order. |

ADR 0047's and ADR 0054's Status sections each carry the reciprocal "Amended by"
pointer to this ADR — 0047 scoped to the checkpoint-lockstep dependency, 0054
scoped to the Context prose.

## Context

A subscription's progress is a single position. It is advanced immediately after
each event is handled, in the same step that handles it, and it is the only
thing a restart consults to decide where to resume. Every other property follows
from that one design choice.

**Concurrent delivery would let progress outrun work.** If several events were
in flight at once and a later one finished first, advancing the position on
completion would move it past an earlier event still being handled. A crash at
that moment loses the earlier event permanently: on restart the subscription
resumes after a position it never actually processed. The failure is silent —
no error, no gap in a counter, just a projection that is missing a row nobody
will look for. Reads of the feed are exclusive of the recorded position, so the
skipped event is never offered again.

**An earlier decision already depends on this.** ADR 0047 made the store the
ordered source and the bus a wake-up signal, then removed the duplicate
suppression between catch-up and live delivery on the grounds that a position
already recorded is never read from the feed again — unreachable by
construction rather than merely untested. That reasoning is only sound while
delivery and position advance move together. Under concurrent delivery a drain
that begins while earlier events are unfinished re-reads positions those events
are still working on, and the deleted case becomes reachable again. The
invariant was doing work for a decision that never named it.

**Catch-up's accounting assumes it too.** Catch-up reconciles what it read
against what it delivered when a batch ends early — on stop, on error, on
reaching the target — so an abandoned tail is not counted as seen. That
reconciliation is a subtraction over a strictly ordered walk. With completions
arriving out of order there is no "tail" to reconcile: read-but-undelivered is
no longer a suffix of the batch, and lag accounting drifts in a direction it
cannot recover from, because it never decreases on its own.

**The rule is already derived independently in more than one place.** The
in-memory bus settles one event before dispatching the next. The projection
registry fans out concurrently within a single event but walks a list of events
sequentially, because event order is the one thing the read side must not
reorder. The subscription runners do the same without saying why. Each of these
was reasoned out on its own, which is the shape this project has repeatedly
paid for: one fact held in several places with nothing failing when the copies
disagree. Stating it once is the point of writing this down.

## Decision

**A subscription delivers events one at a time, in feed order. The next event is
not delivered until the current event has been handled and its position
recorded.** The unit of progress and the unit of work are the same thing, and
advancing the position is how a subscription says the work is done.

This is a correctness constraint, not a performance default, and it may not be
relaxed by configuration. There is no knob that makes a subscription deliver
concurrently, and adding one would be adding a way to corrupt a read model.

**The inert flow-control machinery is deleted rather than repaired.** A
semaphore bounding concurrent delivery, the threshold that signalled pressure
against it, the paused state it could enter, and the statistics reporting its
utilization were all reachable only under concurrency that cannot occur. They
were removed, not wired up. This follows the standing preference for deleting an
unreachable branch over preserving it, and ADR 0047's own disposal of metrics
that could only ever report a constant: a number that cannot vary is not a
measurement, and configuration that cannot change behavior is not configuration.
Repairing them would have meant implementing the concurrency this ADR forbids.

Read volume is bounded separately and remains configurable. How much a
subscription reads from the store in one go is a memory-and-latency decision
with no bearing on delivery order; it is not, and must not be described as, a
bound on concurrent work.

## Sanctioned ways to increase throughput

The constraint is on *interleaving*, not on batch size. Both options below
preserve the property that progress never outruns completed work.

**Batch delivery — sanctioned.** A subscriber that can write in bulk may be
handed a run of events to handle as a unit, with the position advanced once
after the whole run succeeds. This is safe for exactly the reason per-event
concurrency is not: the batch completes before the position moves, so progress
still cannot outrun work. The events within a batch are still applied in order.

**Failure is at-least-once at the batch's grain, and that has a consequence
worth stating.** A crash mid-batch re-delivers the whole batch from the last
recorded position — the same guarantee a single event already carries, just
coarser. A batch handler that *raises* is different: delivery falls back to
handing the batch's events over one at a time, so the ordinary per-event
error handling applies to each rather than the runner having to invent
partial-batch semantics it has no way to determine. Either path can re-deliver
events the failed batch had already applied, because a handler that raises
partway through cannot report how far it got. **A batch handler must therefore
be idempotent over its batch.** That is not a new requirement — at-least-once
delivery already demanded idempotence per event — but the window widens from
one event to one batch, and a handler written to be idempotent per event is
not automatically idempotent per batch.

The interface for this already existed and was already documented before the
runners called it. The catch-up runner now dispatches through `handle_batch()`
when the subscriber supports it, detected once via `supports_batch_handling()`;
the live runner does not yet, tracked separately, because its per-envelope
stop/pause checks were made responsive on the assumption of per-event delivery
and batching it needs its own pass to preserve that. *(That pass was
subsequently done — see [ADR 0063](0063-live-batch-delivery-is-a-page-not-a-window.md);
both runners now dispatch through `handle_batch()`.)* Note the shape of the
finding that motivated closing the catch-up half: the capability was
under-wired, not mis-documented — the guide told the truth about a feature the
runner did not yet reach.

**Partitioned lanes — a future option, not a commitment.** Events could be
distributed across several lanes by aggregate identity, each lane delivering
sequentially, lanes running concurrently. This preserves the ordering that
actually matters: two events for the same aggregate applied out of order corrupt
a read model, while two events for different aggregates generally do not.
Progress would become one position per lane — still a scalar per lane, still
advanced in lockstep with that lane's delivery, so the invariant holds within
each lane rather than being abandoned.

This is recorded as the shape a future decision would take, not as a decision.
It carries real costs that would have to be argued on their own: resumption
requires every lane's position, so the checkpoint schema changes; a subscriber
that relies on cross-aggregate ordering silently breaks; lanes rebalance badly
when one aggregate is much hotter than the rest; and any projection that reads
across aggregates loses the guarantee it was relying on without any signal that
it did. It also interacts with multi-instance coordination, since a lane and an
instance would both become units of parallelism.

## What lifting the constraint would require

Recorded so this is not re-derived from scratch each time throughput comes up.

Per-event concurrency is possible, but it is a redesign of how progress is
recorded, not a tuning change.

- **The scalar position becomes a completion set with a low-water-mark.**
  Resumption would use the highest position below which every event is known
  complete, tracking finished-but-not-contiguous positions above it. That is a
  different persistence shape, a different schema, and a different thing to get
  wrong.
- **Redelivery amplifies.** Today a crash re-delivers approximately one event.
  Under a low-water-mark it re-delivers everything above the mark — a window's
  worth. Handler idempotence stops being a nicety and becomes a requirement the
  library would have to state and users would have to meet.
- **It resurrects the machinery ADR 0047 deleted.** This is the sharp one.
  Because resumption replays the in-flight window, positions above the mark
  *are* read from the feed a second time. Duplicate suppression between catch-up
  and live delivery — removed as unreachable — becomes reachable again, and
  would have to come back along with the metrics that were deleted with it.
  Lifting this constraint therefore amends 0047's central argument rather than
  merely extending it.
- **Head-of-line blocking returns regardless.** One poisoned event stalls the
  low-water-mark no matter how many events complete above it, so the throughput
  win is bounded by the slowest event in the window — the very problem
  concurrency was reached for. Partitioned lanes address this; a completion set
  does not.

The conclusion is not that per-event concurrency is impossible. It is that its
cost lands on checkpoint correctness and on decisions already recorded
elsewhere, so it needs its own ADR and its own argument, and it cannot arrive as
a configuration option.

## Consequences

**Positive.** The property checkpoint correctness rests on is written down and
attributable, so a future change to delivery has something to contradict rather
than something to accidentally invalidate. ADR 0047's deletion of unreachable
duplicate handling is protected by an explicit statement of what makes it
unreachable. The library no longer ships configuration that cannot affect
behavior, or statistics that cannot vary — both of which invited tuning effort
that could produce nothing. Throughput has a sanctioned path (batching) that
does not require weakening any guarantee.

**Negative.** A subscription's throughput is bounded by its slowest handler, and
until batch dispatch is wired there is no supported way to raise it: a slow
handler means a slow subscription, and the answer is to make the handler faster
or to split the work across subscriptions. Deleting the flow-control
configuration is a breaking change for anyone who set those fields, though
setting them never did anything, so the break is in the name only and not in
behavior. Users who reasonably inferred parallelism from the presence of those
knobs will find the library slower than they believed it was — the belief was
wrong, but the disappointment is real and the documentation should meet it
directly rather than quietly dropping the subject.

**Neutral.** Per-aggregate ordering, the guarantee most projections actually
need, is stronger than required here: global sequential delivery provides it as
a side effect. That headroom is what partitioned lanes would spend if throughput
ever justifies the redesign.
