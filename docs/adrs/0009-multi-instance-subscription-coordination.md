# ADR-0009: Multi-Instance Subscription Coordination

**Status:** Accepted. Amended by [ADR 0032](0032-subscriptions-ring-migration.md).

**Date:** 2026-07-27

**Deciders:** Library maintainers (architecture owner, subscriptions owner)

---

## Context

Running a single subscription process is easy: it owns every subscription, its
checkpoints, and its in-flight work. Running several processes of the same
workload — the normal shape of a Kubernetes Deployment, an autoscaling group, or
a spot-instance worker pool — raises questions the library previously had no
answer for:

- Which instance performs work that must happen exactly once (catch-up
  coordination, global position tracking, migration cutover)?
- When an instance is told to shut down, how do the survivors learn that its
  subscriptions are about to go unattended?
- When an instance dies without warning, how do the survivors notice at all?
- How does any of this work when the deployment's message transport is Redis in
  one shop, RabbitMQ in another, and Kafka in a third?

### Forces

- **No mandatory infrastructure.** The library's core dependencies are pydantic
  and sqlalchemy. Requiring an etcd, Consul, or Kubernetes API client to run
  more than one instance would be a large, opinionated addition.
- **Deployments already have consensus.** Almost every target environment
  already runs something that can arbitrate a lock — the Kubernetes control
  plane, Redis, Consul, ZooKeeper, or the PostgreSQL server that is already
  holding the event store. Reimplementing consensus would be both redundant and
  dangerous.
- **Backends are pluggable everywhere else.** `EventStore`, `EventBus`,
  checkpoint/DLQ/outbox repositories all sit behind shared interfaces with
  multiple implementations. Coordination should not be the one subsystem with a
  hardcoded backend.
- **Crash detection and shutdown are different problems.** A graceful shutdown
  can announce itself and say how long it needs; a crash or a network partition
  cannot. Both need to be observable.
- **Preemption is time-boxed.** A spot-instance termination notice gives tens of
  seconds. Peers need to know not just *that* an instance is leaving but *how
  much time is left*.
- **Async-first.** Every store, bus, and projection interface in the library is
  async; coordination callbacks must be too.

### Existing Primitives in the Repo

Three things already existed and shaped the decision:

- `eventsource.locks.postgresql.PostgreSQLLockManager` — session-scoped
  PostgreSQL advisory locks (`acquire`, `try_acquire`, `release`, `is_held`,
  `release_all`), with `migration_lock_key()` used by the migration cutover
  tooling (`eventsource/migration/cutover.py`). This is mutual exclusion, not
  leader election: it has no identity of the current holder for other
  participants to read, and no notification when the holder changes.
- `EventBus` — an async publish/subscribe interface with in-memory, Redis,
  RabbitMQ, and Kafka implementations. Every deployment that runs multiple
  instances already has one configured.
- The `subscriptions/` package — manager, runners, retry, health, and flow
  control, all of which assume a single owning process.

## Decision

Add `src/eventsource/subscriptions/coordination.py`: a **transport-agnostic
coordination protocol** rather than a coordination implementation. It has four
parts:

1. **`LeaderElector`** — a runtime-checkable `Protocol` for leader election,
   with `LeaderElectorWithLease` extending it for lease-based backends. The
   library defines the contract; the deployment supplies the consensus.
2. **Serializable messages** — `ShutdownNotification`, `HeartbeatMessage`, and
   `WorkAssignment`, frozen dataclasses with `to_dict()` / `from_dict()`, plus a
   `ShutdownIntent` enum describing *why* an instance is leaving.
3. **Reserved bus topics** — three constants under the
   `__eventsource_coordination` prefix, so coordination rides the `EventBus` the
   deployment already runs instead of a second transport.
4. **`WorkRedistributionCoordinator`** — tracks peers, derives their status,
   reports orphaned subscriptions, and dispatches async callbacks. It performs
   no I/O: the application publishes what the coordinator creates and feeds it
   what arrives.

The only bundled `LeaderElector` is `InMemoryLeaderElector`, for tests and
single-instance deployments. The module is self-contained and re-exported from
`eventsource.subscriptions`; `SubscriptionManager` does not yet consume it.

### Key Design Decisions

#### 1. Leader Election as a Protocol, Not an Implementation

`LeaderElector` is a `@runtime_checkable` `Protocol` with abstract members:
`identity`, `is_leader`, `current_leader`, `try_acquire(timeout=10.0)`,
`release()`, `renew()`, `on_leader_change(callback)`, and
`remove_leader_change_callback(callback)`.

The library defines the contract; the deployment supplies the consensus.
`try_acquire` is explicitly non-blocking with respect to leadership — it makes a
single attempt and returns `False` if another instance holds the lock; the
`timeout` argument bounds the *backend call*, not the wait for leadership. The
docstring is deliberate that `is_leader` is a cached read and that leadership
can be lost asynchronously, so critical operations should re-verify or use
fencing.

#### 2. Lease Semantics Split into a Separate `LeaderElectorWithLease` Protocol

Lease-based backends (Kubernetes Leases, Redis locks) can expose
`lease_duration_seconds`, `lease_remaining_seconds`, and
`wait_for_leadership(timeout=None)`. Backends without time-based leases — a
PostgreSQL advisory lock held for the life of a session, for instance — are not
forced to invent values for properties they cannot honour. Callers that need
lease introspection depend on the narrower protocol; callers that only need
"am I the leader" depend on the base one.

#### 3. Coordination Travels over the Event Bus, on Reserved Topics

Rather than opening a second connection to a second system, coordination
messages ride the `EventBus` the deployment already runs, on a reserved
namespace:

```python
COORDINATION_TOPIC_PREFIX = "__eventsource_coordination"
SHUTDOWN_NOTIFICATIONS_TOPIC = f"{COORDINATION_TOPIC_PREFIX}.shutdown"
HEARTBEAT_TOPIC = f"{COORDINATION_TOPIC_PREFIX}.heartbeat"
WORK_ASSIGNMENT_TOPIC = f"{COORDINATION_TOPIC_PREFIX}.work_assignment"
```

The double-underscore prefix marks the namespace as library-internal and makes
accidental collision with a domain topic implausible.

#### 4. The Coordinator Owns Protocol and State, Not Transport

`WorkRedistributionCoordinator` tracks peers, computes status, and dispatches
callbacks. It never publishes or subscribes. The application wires it up:
publish what `create_shutdown_notification()` and `create_heartbeat()` return;
feed what arrives into `handle_peer_shutdown()`, `handle_heartbeat()`, and
`handle_work_assignment()`; call `check_peer_timeouts()` on a timer.

This keeps the coordinator testable without a broker, and keeps the library from
taking a position on serialization format, delivery guarantees, or topic
configuration.

#### 5. Serializable Message Contracts (`to_dict` / `from_dict`)

`ShutdownNotification`, `HeartbeatMessage`, and `WorkAssignment` are frozen
dataclasses with explicit `to_dict()` / `from_dict()` pairs: enums serialize to
their `.value`, datetimes to ISO-8601, tuples to lists, and `from_dict` tolerates
missing optional keys via `data.get(...)` defaults. They are *not* `DomainEvent`
subclasses — coordination traffic is operational, not part of anyone's domain,
and must not land in the global `EventRegistry` or an event store.

`ShutdownNotification` carries `time_remaining_seconds` and `is_expired`
properties so a peer can reason about the sender's remaining drain window rather
than guessing.

#### 6. Peer Liveness via Heartbeat Staleness, Not Membership Service

`HeartbeatMessage.is_stale(max_age_seconds=15.0)` compares the message timestamp
against now. `PeerInfo.status` derives one of five strings from the last
heartbeat and any shutdown notification: `terminated` (shutdown notification
expired), `draining` (shutdown notification live), `unknown` (no heartbeat yet),
`stale`, or `healthy`. `check_peer_timeouts()` walks tracked peers, skips any
that already announced shutdown, and fires `on_peer_timeout` callbacks for the
rest whose heartbeat exceeded `heartbeat_timeout_seconds`.

This detects the case an explicit notification cannot cover — a crash or a
partition — using only messages the peers were already sending.

#### 7. Shutdown Intent as an Explicit Enum

`ShutdownIntent` distinguishes `GRACEFUL`, `PREEMPTION`, `HEALTH_FAILURE`, and
`MAINTENANCE`. The urgency of a takeover differs sharply between a rolling
update and a spot termination, and a boolean "is shutting down" flag erases that
difference. The enum lets a peer's callback branch on intent, and the
`metadata` dict carries provider-specific context (for example a spot
termination timestamp) without widening the schema.

#### 8. Redistribution Is Advisory: Orphan Reporting, Not Automatic Reassignment

`get_orphaned_subscriptions()` returns a mapping of peer ID to that peer's
subscriptions for every peer whose status is `draining`, `terminated`, or
`stale`. The coordinator stops there. It does not start subscriptions, does not
claim checkpoints, and does not resolve races between two survivors that both
see the same orphan.

`WorkAssignment` and `handle_work_assignment()` exist for deployments that want
the leader-directed pattern — the leader computes a distribution and addresses
assignments to a `target_instance_id`, and each coordinator ignores assignments
not addressed to it — but the library ships no assignment algorithm. Choosing
who takes what is a policy decision with real correctness stakes, and the wrong
default (steal-on-sight) causes duplicate processing.

`initiate_leadership_handoff()` is the one action the coordinator will take: if
a `leader_elector` was supplied and this instance is the leader, it calls
`release()` so a survivor can acquire leadership immediately instead of waiting
for a lease to lapse.

#### 9. Ship Only `InMemoryLeaderElector`

The single bundled implementation has two modes. With no `shared_state`, it is
single-instance: `try_acquire()` always succeeds. With a shared
`SharedLeaderState` object passed to several electors, first acquire wins and
the others get `False` — enough to exercise multi-instance logic in unit tests
without a broker. `renew()` is a no-op returning `is_leader`, and
`force_lose_leadership()` exists so tests can simulate revocation.

Shipping Kubernetes/Redis/Consul electors would mean owning their client
dependencies, their failure modes, and their version skew. The protocol is
small enough that a deployment can implement one against infrastructure it
already operates and already knows how to debug.

## Consequences

### Positive

- Multi-instance deployments are supported with no new required dependency.
- Coordination is uniformly pluggable, matching how stores, buses, and
  repositories already work.
- The coordinator is fully unit-testable: no broker, no cluster, no sleeps
  beyond timestamp arithmetic.
- Graceful shutdown, preemption, and crash are all observable, through two
  complementary mechanisms (announcement and staleness).
- Message contracts are explicit and transport-neutral, so a deployment can use
  JSON, msgpack, or protobuf without library changes.
- Callback failures are contained: every dispatch loop wraps the callback in
  `try/except`, logs with `exc_info=True`, and continues to the next callback.

### Negative

- **Wiring is the application's job.** Nothing publishes or subscribes for you.
  A deployment that constructs a `WorkRedistributionCoordinator` and never feeds
  it messages gets an empty peer map and silent success.
- **No production leader elector ships.** Anyone running multiple instances for
  real must write one, and `InMemoryLeaderElector`'s always-win default mode is
  actively wrong if mistaken for a production choice.
- **No automatic takeover.** Orphaned subscriptions stay orphaned until the
  application acts on the callback.
- **Coordination is not yet wired into `SubscriptionManager`.** The module is
  self-contained and re-exported from `eventsource.subscriptions`, but the
  manager and runners do not consume it; there is no `leader_elector` parameter
  on the subscription lifecycle today.
- **Split-brain remains possible.** `is_leader` is a cached boolean; between a
  lost lease and the callback firing, two instances can both believe they lead.
  The protocol documents this rather than solving it — fencing is the
  implementation's responsibility.

### Neutral

- Coordination traffic shares the event bus with domain events, so bus capacity
  planning must account for heartbeats (small, but per-instance and periodic).
- `WorkAssignment` is defined and dispatched but unused by any bundled
  component; it is scaffolding for a pattern the library does not yet implement.
- Peer state lives in process memory only. A restarting instance rediscovers
  peers from the next heartbeat round rather than restoring a view.

## Alternatives Considered

### 1. External Lock Service (Kubernetes Leases, Consul, etcd, Redis)

Pick one and depend on it directly. Rejected: it makes a deployment-topology
choice on the user's behalf and drags a client library into a project whose core
dependencies are pydantic and sqlalchemy. The protocol approach lets a user pick
exactly this and implement it in a few dozen lines — the docstrings on
`LeaderElector` and `LeaderElectorWithLease` sketch precisely those backends.

### 2. Reuse the Existing PostgreSQL Advisory Lock Manager

`PostgreSQLLockManager` is already in the tree and already used for migration
locking. Rejected as the general answer because advisory locks give mutual
exclusion without the rest of leader election: no `current_leader` identity for
peers to observe, no lease remaining, no notification when the holder's session
dies — the lock simply vanishes with the connection. It remains a perfectly good
*implementation target* for a user-written `LeaderElector`, and it is still the
right tool for the single-operation migration locks it already serves.

### 3. Static Partitioning of Subscriptions Across Instances

Assign subscription names to instance ordinals by hash or by configuration.
Rejected: it is simple only while the instance count is fixed. It handles
neither scale events nor failures — a dead instance's partition stays unserved —
and it forces the library to know the cluster size, which it cannot learn
without exactly the membership machinery this ADR introduces.

### 4. Delegate to Broker Consumer Groups (Kafka / RabbitMQ)

Let Kafka consumer groups or RabbitMQ competing consumers do the distribution.
Rejected as the only mechanism: it is unavailable for the in-memory and Redis
buses, gives no leader concept for once-only coordination work, and rebalance
semantics differ enough between brokers that the library could not present one
behaviour. Deployments on Kafka can still use consumer groups for event fan-out;
this ADR addresses the orthogonal problem of who coordinates.

### 5. Dedicated Coordination Channel Outside the Event Bus

Run coordination over its own transport (a gossip mesh, a dedicated Redis
connection). Rejected: a second transport means a second set of connection
failures, a second thing to configure, and a second thing to secure — for
message volumes measured in a handful of small messages per instance per
interval. Reserved topics on the existing bus cost nothing extra to operate.

## Implementation Notes

### Module Layout and Public Surface

Everything lives in `src/eventsource/subscriptions/coordination.py` and is
re-exported from `eventsource.subscriptions`. The module's `__all__` groups the
surface as topic constants, `ShutdownIntent`, the three message types, the five
callback type aliases (`LeaderChangeCallback`, `PeerShutdownCallback`,
`HeartbeatCallback`, `WorkAssignmentCallback`, `PeerTimeoutCallback`), the
election types (`LeaderElector`, `LeaderElectorWithLease`,
`InMemoryLeaderElector`, `SharedLeaderState`), and the redistribution types
(`PeerInfo`, `WorkRedistributionCoordinator`).

Callback aliases are all `Callable[..., Awaitable[None]]`; `PeerTimeoutCallback`
takes the peer's `instance_id` as a bare `str`.

### Reserved Topic Namespace

Applications must not publish domain events under `__eventsource_coordination`.
The three concrete topics map one-to-one onto the coordinator's three inbound
handlers, so a straightforward wiring subscribes each topic to its handler and
publishes each created message to the matching topic.

### Default Timings

| Setting | Default | Where |
| --- | --- | --- |
| Heartbeat staleness threshold | 15.0 s | `HeartbeatMessage.is_stale(max_age_seconds=...)` |
| Peer timeout threshold | 15.0 s | `WorkRedistributionCoordinator.heartbeat_timeout_seconds` |
| Drain window | 30.0 s | `create_shutdown_notification(drain_timeout_seconds=...)` |
| Leadership acquire call timeout | 10.0 s | `LeaderElector.try_acquire(timeout=...)` |

Note that `PeerInfo.status` calls `is_stale()` with its own default, so a
coordinator configured with a non-default `heartbeat_timeout_seconds` will use
that value for `check_peer_timeouts()` while `status` still reports against
15 seconds. Heartbeats should be published well inside the timeout — a third of
it is a common choice — so a single lost message does not read as a failure.

### Test Coverage

`tests/unit/subscriptions/test_coordination.py` holds roughly ninety unit tests
covering protocol conformance (`isinstance` checks against the runtime-checkable
protocols and a mock elector), `InMemoryLeaderElector` in both single-instance
and `SharedLeaderState` modes, leadership callbacks including failure isolation,
topic constants, `ShutdownIntent` values, round-trip `to_dict`/`from_dict` for
all three message types, `PeerInfo` status transitions, and the coordinator's
peer tracking, timeout detection, orphan reporting, callback registration and
removal, and leadership handoff. No broker or Docker service is required.

### Writing a Production `LeaderElector`

An implementation must: expose a stable unique `identity`; make `try_acquire`
attempt once and return promptly; make `release` safe to call when not leader;
keep `is_leader` and `current_leader` consistent under concurrent access; and
invoke every registered `on_leader_change` callback on both acquisition and
loss — including involuntary loss such as a lapsed lease. Backends with
time-based leases should also satisfy `LeaderElectorWithLease`. Because
`is_leader` is advisory, side effects that must not happen twice should carry a
fencing token from the backend.

## References

- `src/eventsource/subscriptions/coordination.py` — the implementation and its
  docstrings
- `src/eventsource/subscriptions/__init__.py` — public re-exports
- `src/eventsource/locks/postgresql.py` — `PostgreSQLLockManager`, the
  pre-existing mutual-exclusion primitive
- `src/eventsource/bus/interface.py` — the `EventBus` contract coordination
  messages ride on
- `tests/unit/subscriptions/test_coordination.py` — behavioural specification

## Related

- `docs/guides/subscription-coordination.md` — how to wire the coordinator to a
  bus and run several instances
- `docs/api/subscriptions.md` — API reference for the coordination surface
- `docs/architecture.md` — subsystem overview
