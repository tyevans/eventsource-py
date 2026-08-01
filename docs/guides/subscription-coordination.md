# Coordinating Multiple Subscription Instances

This guide shows how to run several instances of the same subscription workload
without them duplicating work, losing work when a node dies, or dropping events
during a rolling restart. It covers electing a leader, exchanging heartbeats,
detecting failed peers, assigning and reclaiming work, and coordinating
shutdown.

Most of this lives in `eventsource.application.subscriptions.coordination` and
is re-exported from `eventsource.application.subscriptions`. The `LeaderElector`
Protocol pair lives in `eventsource.ports.coordination` (also re-exported from
`application.subscriptions`), and the in-memory implementation,
`InMemoryLeaderElector` / `SharedLeaderState`, lives in
`eventsource.adapters.memory` — it is an adapter, not part of the
`application.subscriptions` re-export surface:

```python
from eventsource.adapters.memory import InMemoryLeaderElector, SharedLeaderState
from eventsource.application.subscriptions import (
    HeartbeatMessage,
    LeaderElector,
    LeaderElectorWithLease,
    PeerInfo,
    ShutdownIntent,
    ShutdownNotification,
    WorkAssignment,
    WorkRedistributionCoordinator,
)
```

Two pieces do the work:

- `LeaderElector` (and `LeaderElectorWithLease`) — a protocol you implement
  against whatever your cluster already agrees on (Kubernetes Leases, Redis,
  Consul, a Postgres advisory lock). `InMemoryLeaderElector` plus
  `SharedLeaderState` are provided for tests and single-process runs.
- `WorkRedistributionCoordinator` — an in-process state machine that builds and
  interprets the three coordination messages (`HeartbeatMessage`,
  `ShutdownNotification`, `WorkAssignment`), tracks peers as `PeerInfo`, and
  fires callbacks when something changes.

The coordinator does not talk to a network. It produces and consumes messages;
you publish and deliver them over your own `EventBus` on the coordination
topics. Follow the sections below in order to assemble a working coordinated
worker, or jump to
[Full example: a coordinated worker](#full-example-a-coordinated-worker) for the
assembled result.

## When you need coordination

Reach for this module when **more than one process runs the same subscriptions
against the same event store** and you need those processes to agree on who does
what.

Concretely, use it when you need to:

- **Run exactly one instance of a singleton workload.** A projection rebuild, an
  outbox drain, or a scheduled compaction job that must not run twice. Elect a
  leader and gate the work on `elector.is_leader`.
- **Split subscriptions across instances.** The leader publishes a
  `WorkAssignment` naming which `subscriptions` belong to which `instance_id`;
  each instance applies the assignment it receives.
- **Notice that a peer died.** Instances publish `HeartbeatMessage` on an
  interval; `check_peer_timeouts()` marks peers whose last heartbeat is older
  than `heartbeat_timeout_seconds` and reports their ids so someone can take
  over.
- **Hand work off cleanly during a rolling restart or spot preemption.** A
  departing instance broadcasts a `ShutdownNotification` with a
  `ShutdownIntent` and a drain deadline, so peers stop waiting on it and claim
  its subscriptions via `get_orphaned_subscriptions()` instead of waiting for a
  heartbeat timeout to elapse.

You do **not** need any of this when:

- You run a single subscription worker process. `SubscriptionManager` alone is
  enough.
- Your instances already have disjoint work by construction — for example, each
  deployment owns a different set of subscription names, or per-tenant workers
  are sharded upstream.
- Your event bus already provides competing-consumer semantics that satisfy your
  correctness requirement. Kafka consumer groups and RabbitMQ work queues
  distribute *messages*; coordination here distributes *subscriptions* and their
  checkpoints, which the broker does not know about. Use the broker's mechanism
  when at-most-once delivery per message is all you need, and this module when
  instances must agree on ownership of a long-lived subscription.

One caveat before you build on it: this module is coordination *plumbing*, not a
consensus system. Leadership is only as strong as the `LeaderElector` you plug
in, and the coordinator's view of peers is only as fresh as the heartbeats your
bus actually delivers. The next section spells out that division of
responsibility.

## What the library provides (and what it doesn't)

Before wiring anything up, be clear about where the library stops and your
infrastructure starts.

**What the library provides:**

- **A leader election contract, not an election algorithm.** `LeaderElector` is
  a `Protocol` with `identity`, `is_leader`, `current_leader`, `try_acquire()`,
  `release()`, `renew()`, `on_leader_change()`, and
  `remove_leader_change_callback()`. `LeaderElectorWithLease` extends it with
  `lease_duration_seconds`, `lease_remaining_seconds`, and
  `wait_for_leadership()`. The only shipped implementation is
  `InMemoryLeaderElector`.
- **Three serializable message types.** `HeartbeatMessage`,
  `ShutdownNotification`, and `WorkAssignment` are dataclasses with `to_dict()`
  / `from_dict()` and a few derived helpers (`is_stale()`,
  `time_remaining_seconds`, `is_expired`).
- **Three topic name constants.** `HEARTBEAT_TOPIC`,
  `SHUTDOWN_NOTIFICATIONS_TOPIC`, and `WORK_ASSIGNMENT_TOPIC`, all under
  `COORDINATION_TOPIC_PREFIX` (`"__eventsource_coordination"`).
- **In-process peer bookkeeping.** `WorkRedistributionCoordinator` keeps a
  `dict[str, PeerInfo]` of peers it has heard from, exposes `known_peers`,
  `healthy_peer_count`, `draining_peers`, and `get_orphaned_subscriptions()`,
  and fires your callbacks from `handle_heartbeat()`,
  `handle_peer_shutdown()`, `handle_work_assignment()`, and
  `check_peer_timeouts()`.
- **A few safe defaults.** The coordinator ignores heartbeats bearing its own
  `instance_id` and work assignments whose `target_instance_id` is not its own,
  and it swallows-and-logs exceptions raised by your callbacks so one bad
  handler cannot break the coordination loop.

**What the library does not provide:**

- **No production leader elector.** There is no Kubernetes, Redis, or Consul
  implementation in this package — the module docstring lists them as future
  work. `InMemoryLeaderElector` is for tests and single-process runs: with no
  `shared_state` it grants leadership to *every* caller unconditionally, with a
  shared `SharedLeaderState` it only coordinates electors inside one process,
  and its `renew()` is a no-op that just returns `is_leader`. It has no lease,
  so it does not satisfy `LeaderElectorWithLease`.
- **No transport.** The coordinator never publishes or subscribes. It hands you
  message objects from `create_heartbeat()` and
  `create_shutdown_notification()`; publishing them on the coordination topics
  and routing inbound messages back into the `handle_*` methods is your code.
- **No background tasks.** Nothing runs on a timer. You own the heartbeat
  publish loop, the lease renewal loop, and the interval that calls
  `check_peer_timeouts()`.
- **No enforcement of assignments.** `WorkAssignment` is advisory. The
  coordinator invokes `on_work_assignment` callbacks; starting, stopping, and
  checkpointing the named subscriptions is left to your `SubscriptionManager`
  wiring. Nothing prevents two instances from running the same subscription if
  your callbacks disagree.
- **No fencing or split-brain protection.** The coordinator does not check
  lease epochs or reject stale writes. If your elector hands leadership to two
  processes at once, two leaders will happily publish assignments.
- **No persistence.** Peer state lives in memory. A restarted instance starts
  with an empty `known_peers` and re-learns the cluster from the next round of
  heartbeats.

The practical consequence: the strength of your coordination equals the
strength of the `LeaderElector` you supply and the reliability of the bus
carrying the three topics. Everything below assumes you have both.

## The coordination topics

All coordination traffic flows over three topic names exported from
`eventsource.application.subscriptions`. They are plain strings — the library never
subscribes to them for you — and all three share the
`COORDINATION_TOPIC_PREFIX` (`"__eventsource_coordination"`) so they are easy to
filter, ACL, or route onto a dedicated exchange:

```python
from eventsource.application.subscriptions import (
    COORDINATION_TOPIC_PREFIX,        # "__eventsource_coordination"
    HEARTBEAT_TOPIC,                  # "__eventsource_coordination.heartbeat"
    SHUTDOWN_NOTIFICATIONS_TOPIC,     # "__eventsource_coordination.shutdown"
    WORK_ASSIGNMENT_TOPIC,            # "__eventsource_coordination.work_assignment"
)
```

Each topic carries exactly one message type, and each message type maps to one
coordinator entry point:

| Topic | Message | Who publishes | Inbound handler |
|---|---|---|---|
| `HEARTBEAT_TOPIC` | `HeartbeatMessage` | every instance, on an interval | `handle_heartbeat()` |
| `SHUTDOWN_NOTIFICATIONS_TOPIC` | `ShutdownNotification` | a departing instance, once | `handle_peer_shutdown()` |
| `WORK_ASSIGNMENT_TOPIC` | `WorkAssignment` | the leader | `handle_work_assignment()` |

All three are **broadcast** topics: every instance should receive every message.
Do not put them behind a competing-consumer queue — if a heartbeat is delivered
to only one peer, the others will time that instance out even though it is
alive. On RabbitMQ that means a fanout exchange with a per-instance queue; on
Kafka, a distinct consumer group per instance.

**`HEARTBEAT_TOPIC`** carries `HeartbeatMessage`: `instance_id`, `timestamp`,
`subscriptions`, `in_flight_count`, `is_leader`, and `load_factor`. It is the
liveness signal, and it doubles as the cluster's shared view of who owns what —
`get_orphaned_subscriptions()` can only report a dead peer's subscriptions
because its last heartbeat listed them. Publish it even when idle; silence is
interpreted as failure.

**`SHUTDOWN_NOTIFICATIONS_TOPIC`** carries `ShutdownNotification`:
`instance_id`, `intent`, `initiated_at`, `expected_completion_at`,
`subscriptions`, `in_flight_count`, and free-form `metadata`. It is the
fast-path counterpart to heartbeat timeout — a peer that announces its
departure is marked as draining immediately, rather than after
`heartbeat_timeout_seconds` of missed heartbeats. Note that
`check_peer_timeouts()` skips any peer with a recorded shutdown notification, so
an announced departure never also surfaces as a timeout.

**`WORK_ASSIGNMENT_TOPIC`** carries `WorkAssignment`: `target_instance_id`,
`subscriptions`, `source_instance_id`, `assigned_at`, and `priority`. It is
addressed rather than broadcast in intent — it is published to all instances,
but `handle_work_assignment()` returns immediately unless
`target_instance_id` equals the receiving coordinator's `instance_id`. To
reassign work across several instances, publish one `WorkAssignment` per target.

Every message is a frozen dataclass with `to_dict()` and `from_dict()`, so the
wire format is your choice — JSON over an `EventBus`, a Redis pub/sub channel,
whatever you already run. See
[Serialize with `to_dict` / `from_dict`](#serialize-with-to_dict--from_dict) for
the round-trip, and [Ignore your own messages](#ignore-your-own-messages) for
the loopback caveat (the coordinator already drops self-authored heartbeats and
shutdown notifications, but your publish path may not).

## Elect a leader

Some coordination work must happen exactly once per cluster: deciding who runs a
singleton subscription, or publishing `WorkAssignment` messages after a peer
dies. `LeaderElector` is the contract for picking that one instance.

The library ships the contract and a test double. It does **not** ship a
production elector — you implement `LeaderElector` on top of whatever your
cluster already agrees on.

### Implement the `LeaderElector` protocol

`LeaderElector` is a `runtime_checkable` `Protocol`, so any object with the right
members satisfies it — no base class to inherit. It requires three properties
and five methods:

| Member | Kind | Contract |
|---|---|---|
| `identity` | property → `str` | Unique id of this instance within the election |
| `is_leader` | property → `bool` | Cached leadership state; no backend round-trip |
| `current_leader` | property → `str \| None` | Identity of the current leader, `None` if none |
| `try_acquire(timeout=10.0)` | async → `bool` | One attempt; returns `False` immediately if taken |
| `release()` | async → `None` | Give up leadership; safe when not leader |
| `renew()` | async → `bool` | Extend the lease; `False` if not leader or failed |
| `on_leader_change(callback)` | sync | Register an async `(bool) -> None` callback |
| `remove_leader_change_callback(callback)` | sync → `bool` | `True` if the callback was registered |

A minimal implementation backed by a Postgres advisory lock looks like this:

```python
from eventsource.application.subscriptions import LeaderChangeCallback, LeaderElector


class AdvisoryLockElector:
    def __init__(self, identity: str, pool, lock_key: int) -> None:
        self._identity = identity
        self._pool = pool
        self._lock_key = lock_key
        self._is_leader = False
        self._callbacks: list[LeaderChangeCallback] = []

    @property
    def identity(self) -> str:
        return self._identity

    @property
    def is_leader(self) -> bool:
        return self._is_leader

    @property
    def current_leader(self) -> str | None:
        return self._identity if self._is_leader else None

    async def try_acquire(self, timeout: float = 10.0) -> bool:
        async with self._pool.acquire() as conn:
            acquired = await conn.fetchval(
                "SELECT pg_try_advisory_lock($1)", self._lock_key
            )
        await self._set_leadership(bool(acquired))
        return self._is_leader

    async def release(self) -> None:
        if not self._is_leader:
            return
        async with self._pool.acquire() as conn:
            await conn.fetchval("SELECT pg_advisory_unlock($1)", self._lock_key)
        await self._set_leadership(False)

    async def renew(self) -> bool:
        # Session-scoped advisory locks are held for the life of the session.
        return self._is_leader

    def on_leader_change(self, callback: LeaderChangeCallback) -> None:
        self._callbacks.append(callback)

    def remove_leader_change_callback(self, callback: LeaderChangeCallback) -> bool:
        try:
            self._callbacks.remove(callback)
            return True
        except ValueError:
            return False

    async def _set_leadership(self, is_leader: bool) -> None:
        if self._is_leader == is_leader:
            return
        self._is_leader = is_leader
        for callback in self._callbacks:
            await callback(is_leader)


assert isinstance(AdvisoryLockElector("worker-1", pool, 42), LeaderElector)
```

Because the protocol is `runtime_checkable`, that final `isinstance` check works
as a cheap structural test — but note it only verifies member *presence*, not
signatures or async-ness. Keep real behavioural tests as well. If you would
rather have the type checker catch a missing member, subclass the protocol
explicitly (`class AdvisoryLockElector(LeaderElector): ...`) — its members are
declared with `@abstractmethod`, so an incomplete subclass fails at
instantiation too.

Three contract points worth honouring:

- **`try_acquire()` does not block.** It makes a single attempt and returns
  `False` if someone else holds leadership. Its `timeout` argument bounds the
  *backend call*, not the wait for leadership to free up. If you need to wait,
  implement `LeaderElectorWithLease.wait_for_leadership()`.
- **All methods must be safe from concurrent tasks.** `InMemoryLeaderElector`
  guards `try_acquire()` and `release()` with an `asyncio.Lock`; do the same.
  Leadership state must stay consistent under concurrent access.
- **Backend failures surface as `ConnectionError`.** The protocol documents
  `try_acquire()` and `release()` as raising `ConnectionError` when the backend
  is unreachable. Raise it rather than returning `False` on a transport error,
  so callers can distinguish "someone else is leader" from "I could not ask".

### Acquire, renew, and release the lease

The lifecycle is the same regardless of backend: attempt, hold by renewing, give
back on shutdown.

```python
import asyncio

elector = AdvisoryLockElector("worker-1", pool, lock_key=42)

if await elector.try_acquire():
    ...  # leader-only work
```

`try_acquire()` is a poll, not a subscription. Retry it on an interval so a
follower picks up leadership after the current leader dies. Because the protocol
documents `ConnectionError` for backend failures, catch it in the loop — a
transient outage should not kill the campaign task:

```python
async def campaign(elector, poll_interval: float = 5.0) -> None:
    while True:
        if not elector.is_leader:
            try:
                await elector.try_acquire()
            except ConnectionError:
                logger.warning("Election backend unreachable; retrying")
        await asyncio.sleep(poll_interval)
```

Calling `try_acquire()` while already leader is harmless: it returns `True` and,
because leadership state is unchanged, fires no `on_leader_change` callbacks.
The `is_leader` guard above is an optimization, not a correctness requirement.

`renew()` extends the lease and returns `False` when this instance is not (or is
no longer) the leader. Nothing in the library calls it for you — run your own
loop, and treat a `False` return as leadership loss:

```python
async def renew_lease(elector, interval: float = 5.0) -> None:
    while elector.is_leader:
        if not await elector.renew():
            break  # lost the lease; on_leader_change callbacks fire
        await asyncio.sleep(interval)
```

Renew well inside the lease window — an interval of roughly a third of
`lease_duration_seconds` leaves room for two failed renewals before the lease
expires. Not every backend needs the loop: the protocol notes that many backends
renew automatically, and `renew()` exists for explicit renewal when they do not.
A session-scoped Postgres advisory lock, for example, is held for the life of
the connection, so its `renew()` can simply report `is_leader` — as
`InMemoryLeaderElector.renew()` does. Skip the loop only when you can point at
the mechanism that keeps the lease alive instead.

Note what `renew()` returning `False` does *not* do: it does not itself flip
`is_leader` or fire callbacks. That is your elector's job — detect the failed
renewal and drive the same internal state transition `release()` would, so
`on_leader_change` subscribers learn about the loss.

Call `release()` during graceful shutdown. It is a no-op when this instance is
not the leader, so it is always safe in a `finally` block, and it lets a peer
take over immediately instead of waiting out the lease:

```python
try:
    await run_leader_work()
finally:
    await elector.release()
```

`release()` may also raise `ConnectionError`, so if shutdown must proceed
regardless, wrap it — an unreachable backend will expire the lease on its own,
just more slowly:

```python
finally:
    try:
        await elector.release()
    except ConnectionError:
        logger.warning("Could not release leadership; lease will expire")
```

If you are also running a `WorkRedistributionCoordinator`, prefer
`coordinator.initiate_leadership_handoff()` — it calls `release()` on the
configured elector and returns `True` only if leadership was actually held. See
[Hand off leadership with `initiate_leadership_handoff`](#hand-off-leadership-with-initiate_leadership_handoff).

### React to leadership changes with `on_leader_change`

Polling `is_leader` from your business logic is fragile; register a callback
instead. `on_leader_change()` takes an async `Callable[[bool], Awaitable[None]]`
(exported as `LeaderChangeCallback`) that is invoked with `True` on acquisition
and `False` on loss. Multiple callbacks are allowed and are called in
registration order.

```python
async def toggle_singleton_work(is_leader: bool) -> None:
    if is_leader:
        await manager.start_subscription("global-compaction")
    else:
        await manager.stop_subscription("global-compaction")


elector.on_leader_change(toggle_singleton_work)
```

Callbacks fire only on a *transition*. `InMemoryLeaderElector` returns early
from its internal state setter when the new value equals the old one, so a
second successful `try_acquire()` while already leader fires nothing. Implement
the same edge-triggered behaviour in your own elector — callbacks that re-run on
every poll will restart subscriptions in a loop.

Three behaviours of the reference implementation are worth matching, because
callers will assume them:

- **Callbacks are awaited one at a time, in registration order.** They are not
  gathered. A callback that blocks for ten seconds delays every later callback
  *and* the leadership transition itself. Keep them short — flip a flag, set an
  `asyncio.Event`, or spawn a task — rather than doing the start/stop work
  inline if it can be slow.
- **They run inside the elector's lock.** `InMemoryLeaderElector` invokes
  callbacks from `_set_leadership()`, which is reached while holding the
  `asyncio.Lock` that guards `try_acquire()`, `release()`, and
  `force_lose_leadership()`. Calling any of those from inside a callback
  deadlocks. If a callback needs to relinquish leadership, schedule it:
  `asyncio.create_task(elector.release())`.
- **Exceptions are caught and logged, not propagated.** Leadership still
  changes, and one broken handler does not prevent the others from running or
  corrupt the state. Copy that convention — but note it means **a failed
  callback is silent to your code**. If starting the singleton workload fails,
  surface it through your own health check rather than relying on the exception
  reaching the caller of `try_acquire()`.

Loss is delivered through the same channel no matter how it happens: a
successful `release()`, a `force_lose_leadership()` in a test, or your own
elector detecting a failed `renew()` and driving the transition. That is the
reason to prefer callbacks over polling — there is one place to stop the
workload, and it covers the unplanned path as well as the graceful one.

To unregister, pass the same callable object to
`remove_leader_change_callback()`; it returns `True` when the callback was found
and `False` otherwise (so removing twice is harmless). Bound methods and
lambdas need the same reference you registered:

```python
elector.on_leader_change(toggle_singleton_work)
...
assert elector.remove_leader_change_callback(toggle_singleton_work) is True
assert elector.remove_leader_change_callback(toggle_singleton_work) is False
```

### Wait for leadership with `LeaderElectorWithLease`

`LeaderElectorWithLease` extends `LeaderElector` for backends with time-based
leases (Kubernetes `Lease`, Redis locks with TTL, Consul sessions). It adds:

- `lease_duration_seconds` → `float` — how long a granted lease lasts.
- `lease_remaining_seconds` → `float | None` — time left, or `None` when this
  instance is not the leader.
- `wait_for_leadership(timeout: float | None = None)` → `bool` — block until
  leadership is acquired, returning `False` when the timeout expires and `True`
  when it is acquired. `None` waits indefinitely.

This is the blocking counterpart to `try_acquire()`. Use it in a worker whose
whole job is the singleton workload:

```python
if await elector.wait_for_leadership(timeout=30.0):
    remaining = elector.lease_remaining_seconds
    logger.info("Became leader", extra={"lease_remaining": remaining})
    await run_leader_work()
else:
    logger.info("Another instance is leading; standing by")
```

`lease_remaining_seconds` is the value to gate long leader-only operations on:
if less time remains than the operation needs, renew first rather than starting
work you may lose the right to finish. The library does not fence anything for
you — see [Split-brain and lease expiry](#split-brain-and-lease-expiry).

Both protocols are `runtime_checkable`, and the extended one is strictly
narrower: an object satisfying only `LeaderElector` fails
`isinstance(obj, LeaderElectorWithLease)`. In particular
`InMemoryLeaderElector` has no lease, so it satisfies `LeaderElector` but **not**
`LeaderElectorWithLease`. If your code calls `wait_for_leadership()`, type it
against `LeaderElectorWithLease` and pick a different test double.

### Test locally with `InMemoryLeaderElector` and `SharedLeaderState`

`InMemoryLeaderElector` is a dataclass whose identity field is named
`_identity`, so construct it positionally or with that keyword:

```python
from eventsource.adapters.memory import InMemoryLeaderElector, SharedLeaderState

elector = InMemoryLeaderElector("worker-1")
assert await elector.try_acquire() is True   # always True, no contention
assert elector.is_leader is True
assert elector.current_leader == "worker-1"
```

With no `shared_state`, every elector wins unconditionally. That is the
single-instance mode: fine for local development and for tests that just need
leader-gated code to execute, useless for testing contention.

Pass a shared `SharedLeaderState` to simulate a cluster **within one process**:

```python
state = SharedLeaderState()
worker1 = InMemoryLeaderElector("worker-1", shared_state=state)
worker2 = InMemoryLeaderElector("worker-2", shared_state=state)

assert await worker1.try_acquire() is True
assert await worker2.try_acquire() is False
assert state.current_leader == "worker-1"

await worker1.release()
assert await worker2.try_acquire() is True
assert state.current_leader == "worker-2"
```

Acquisition is serialized by a per-elector `asyncio.Lock`, so a concurrent
`asyncio.gather()` over ten electors sharing one state yields exactly one
winner. Calling `try_acquire()` again while already leader returns `True`
without firing callbacks.

Two extras make failure paths testable:

- `force_lose_leadership()` revokes leadership as if the backend had taken it
  away — it clears `shared_state.current_leader` and fires your
  `on_leader_change` callbacks with `False`. Use it to test that your workload
  stops cleanly.
- `renew()` is a no-op that simply returns `is_leader`. It never fails, so it
  cannot exercise a renewal-failure branch; force that with
  `force_lose_leadership()` or a stub elector instead.

```python
losses: list[bool] = []


async def record(is_leader: bool) -> None:
    losses.append(is_leader)


elector = InMemoryLeaderElector("worker-1")
elector.on_leader_change(record)
await elector.try_acquire()
await elector.force_lose_leadership()

assert losses == [True, False]
assert elector.is_leader is False
```

Keep the boundary in mind: `SharedLeaderState` is a plain in-memory dataclass.
It coordinates electors that share the same Python object and nothing else — no
process, container, or pod boundary is crossed. Never ship it as your production
elector; wire in a real one behind the same protocol and swap it at composition
time.
