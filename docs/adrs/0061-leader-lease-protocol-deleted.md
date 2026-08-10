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

**Correction (2026-08-10).** This section originally claimed, as a known
negative, that the coordinator "trusts `is_leader` as a cached flag" and on a
lapsed lease "will redistribute work it no longer owns." **That is wrong**, and
it was wrong when written — the claim was made from the shape of the problem
rather than from the call sites, which is the failure mode this project's own
rules warn about. It is corrected here rather than quietly deleted, because a
false consequence in a permanent record is worse than an admitted one.

What is actually true: leadership is read at exactly two sites. One populates
the `is_leader` field of a `HeartbeatMessage`. The other gates
`initiate_leadership_handoff()`, which calls `release()` — and `LeaderElector`
documents `release()` as *"Safe to call even if not currently the leader"*, so a
stale flag there is handled by the implementation, by contract. **No work
redistribution is gated on leadership anywhere in the library**, and nothing in
the library consumes `HeartbeatMessage.is_leader` at all; per ADR 0009,
redistribution is advisory orphan reporting that a caller acts on.

So there is no split-brain hazard for the library to guard. What remains is a
documentation obligation, not a code one: a heartbeat reports whatever the
elector's `is_leader` returns at the moment it is built, so a lease-based
elector must flip `is_leader` on expiry for that field to mean anything. That
is stated in the coordination guide. The task filed against the original claim
was closed as not-a-defect.
