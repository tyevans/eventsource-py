# ADR-0063. Live Batch Delivery Is A Page, Not A Window

ADR 0059 sanctioned batch delivery as the way to raise subscription throughput
without weakening ordering, wired it into the catch-up runner, and left the live
runner explicitly unfinished — because the live runner's per-envelope stop and
pause checks had just been made precise, and batching it naively would have
coarsened them. This ADR closes that half and records the constraint that made
it safe: **the live batch is a page the feed already returned, never an
accumulator.**

## Status

**Accepted.**

**ADR Impact**

| ADR | Status |
| --- | --- |
| [0059](0059-ordered-subscription-delivery.md) | Amended — completed, not contradicted. 0059 sanctions batch delivery and names the live runner as the outstanding half, with a stated reason: its per-envelope stop/pause checks assumed per-event delivery. This ADR supplies the pass 0059 asked for and records why the responsiveness objection does not apply to a page-shaped batch. Every constraint 0059 states — one position, advanced only after the unit of work settles, events applied in order, at-least-once at the batch's grain, fallback to per-event delivery when a batch handler raises — holds unchanged on the live path. |
| [0047](0047-live-runner-feed-driven-checkpointing.md) | Stands. The store remains the ordered source and the bus a wake-up signal; a position recorded is still never re-read. Grouping changes how many envelopes one handler call receives, not how many times a position is read. |
| [0007](0007-event-bus-delivery-semantics.md) | Stands. What the bus promises is untouched; the bus still carries no position and delivers no event to a subscriber. |
| [0060](0060-bounded-background-publishing.md) | Stands. Bounded background publishing is on the write side; this is delivery. |

ADR 0059's Status section carries the reciprocal "Amended by" pointer to this
ADR, scoped to the live half of its batch-delivery sanction.

## Context

Delivery on the live path is not driven by arriving events. A bus notification
is a wake-up signal with no position, and on each wake the runner reads the
global feed forward from its checkpoint, bounded by `batch_size`. Whatever comes
back is, by construction, work that already exists and is already in memory —
feed adapters materialize a result set before yielding its first envelope, which
is why the read is bounded at all.

That fact is the whole argument. The objection 0059 recorded — that batching
costs responsiveness — is an objection to *waiting*: an accumulator that holds
the first event back hoping a second arrives buys throughput with latency, which
is the one thing the live path exists to provide, and it stretches the interval
between stop/pause checks by however long the window is. None of that follows
from grouping events the runner is already holding.

The second force is divergence. `handle_batch()` was dispatched by one runner
and not the other, so one subscriber saw two delivery shapes depending on which
runner happened to be driving it, and a handler idempotent per batch on catch-up
was handed single events live. That is this project's most-repeated defect shape
— one fact, two implementations, nothing failing when they disagree — sitting
across the two halves of a single subscription's lifetime.

## Decision

**The live runner delivers each bounded feed read to a batch-capable subscriber
as one `handle_batch()` call.** The batch is exactly the page the read returned.
Nothing is ever held back waiting for more events, and no timer, window, or
minimum batch size exists or may be added. A single available event is
dispatched as a batch of one, immediately.

**Stop and pause remain per-envelope.** The page is scanned before it is
dispatched, and that scan makes the same `_stop_requested` and
`wait_if_paused()` checks per envelope the per-event path made — a stop or pause
landing before dispatch takes effect at exactly the granularity it did before,
and a drain parked on a pause is still released by `stop()` rather than only by
`resume()`. An envelope the scan declines to include is not counted as seen, so
an abandoned tail cannot inflate lag.

**`processing_timeout` bounds one handler call, which is now sometimes a batch.**
This is the one guarantee whose *grain* changes, and it changes to match the
catch-up runner exactly: a `handle_batch()` of many events gets the same budget
as a single `handle()`, because it is one call. The window in which a stop
request waits is therefore bounded by `processing_timeout` on both runners, as
it already was.

**`handle_batch()` takes precedence over `handle()` on the live path**, matching
`supports_batch_handling()`-based detection on catch-up. A subscriber that
implements both now receives batches from both runners instead of batches from
one and single events from the other.

**`EVERY_BATCH` checkpointing acquires a meaning here.** It previously behaved as
`EVERY_EVENT` on the live path with a note that live events arrive one at a
time. They do not; a page is a batch, and the checkpoint lands once after it
settles.

### Rejected: a time-window accumulator

Holding events for a fixed interval to build larger batches was considered and
rejected. It trades the live path's defining property for throughput on a path
that already has a sanctioned bulk mechanism — catch-up — and it degrades stop
responsiveness by the window length in the state an operator is most likely to
be waiting on. A subscription whose live throughput cannot keep up is behind,
and a subscription that is behind is a catch-up problem.

## Consequences

**Positive.** A bulk-writing subscriber gets bulk writes for its whole lifetime
rather than only while catching up, with no latency cost when the feed is quiet:
one available event is still delivered immediately. The two runners now agree
about what a given subscriber is, removing a divergence that produced no error
when it disagreed. The responsiveness work that preceded this is preserved
rather than traded away, and is pinned by tests that assert it on the grouped
path specifically.

**Negative.** A batch-capable subscriber that also implements `handle()` sees a
behavior change on the live path: it now receives batches. This is a correction
of an inconsistency rather than a new hazard — ADR 0059 already requires a batch
handler to be idempotent over its batch, and catch-up already held it to that —
but a handler written on the assumption that live delivery was per-event has its
redelivery window widened from one event to one page. The observable window in
which a `stop()` waits is now bounded by one batch call rather than one event
call; both are bounded by `processing_timeout`, so the bound is unchanged while
the typical wait grows. Per-event handler timing metrics on the grouped path are
an approximation — the batch's duration divided across it — because
`handle_batch()` reports no per-event timing, the same approximation catch-up
already makes.

**Neutral.** Subscribers with no `handle_batch()` never reach the grouped path;
for them nothing about live delivery changes at all.
