# ADR-0061. The Lease Half of Leader Election Is Not Ours to Declare

`LeaderElectorWithLease` extended `LeaderElector` with `lease_duration_seconds`,
`lease_remaining_seconds`, and `wait_for_leadership()`. Nothing in the tree
implemented it, nothing consumed those three members, and the library drives
none of the lifecycle they describe. It is deleted. `LeaderElector` stays.

## Status

**Accepted.**

**ADR Impact**

| ADR | Status |
| --- | --- |
| [0009](0009-multi-instance-subscription-coordination.md) | Amended. 0009 decided leader election is a *protocol rather than an implementation*, and split lease semantics into a separate extended Protocol so a lease-based backend could be typed precisely. The first half stands and is the reason this deletion is cheap. The second half is withdrawn: the split was made on the expectation of `KubernetesLeaderElector` and `RedisLeaderElector`, and neither arrived. 0009's other decisions — coordination over the event bus on reserved topics, peer liveness by heartbeat staleness, advisory orphan reporting, shipping only `InMemoryLeaderElector` — are untouched. |
| [0032](0032-subscriptions-ring-migration.md) | Stands. It relocated the coordination surface into `ports/coordination.py`; deleting one Protocol from that module does not disturb where the module lives. |

## Context

The criterion this library now applies to a declared-but-undispatched capability
is: **who is contractually obligated to call this — the user, or us?** A port
with no internal caller is not a defect when users are the callers.

Applying it here is decisive. The library touches a `LeaderElector` at exactly
three points: it reads `is_leader` when building a heartbeat, reads it again to
gate a leadership handoff, and calls `release()` during that handoff. It never
calls `try_acquire()` and never calls `renew()`. **Acquisition is the user's,
and renewal travels with acquisition** — whoever takes a lease keeps it alive.

So the lease surface was never something the library was going to honor. What
it actually offered was a *type* for a lease-based elector, on the expectation
of backends that were never written. Three members, zero implementations, zero
consumers, across roughly eight months.

## Decision

Delete `LeaderElectorWithLease` and its exports. Keep `LeaderElector` — the
coordinator genuinely uses it.

A backend that has leases defines its own lease API. That is strictly better
than conforming to a shape this library guessed at without ever calling it:
Kubernetes `Lease`, a Redis TTL lock, and a Consul session do not agree on what
a lease exposes, and a Protocol none of them was written against had no
authority to reconcile them.

Rejected: **building the renewal loop.** The library would take ownership of a
lifecycle it does not start, acquire another background task to manage and
drain, and still not fence anything. Rejected: **keeping it and adding a
conformance suite** — a suite pins agreement between implementations, and there
are none to disagree.

## Consequences

**A breaking removal from the public API**, taken freely pre-1.0 with no
deprecation shim, per standing policy. Code that type-hints
`LeaderElectorWithLease` or calls `wait_for_leadership()` through it must switch
to its own type. No behavior changes: nothing called these members.

**`LeaderElector` is now the whole contract**, and it is the honest one — every
member on it is either implemented by `InMemoryLeaderElector` or called by the
coordinator.

**A known negative, tracked rather than merely recorded:** the coordinator
trusts `is_leader` as a cached flag. With a lease-based elector that flag can be
stale — the lease lapsed, another instance took over, and this one still
believes it leads and will redistribute work it no longer owns. Deleting the
lease Protocol neither causes nor fixes this; the hazard lives in the *read*,
not in the type. It is filed as its own task rather than left as a paragraph
here, because this project has a documented habit of recording negatives that
stay open for a year (ADR 0021's unbounded-pending-tasks note, restated as open
by ADRs 0059 and 0060).
