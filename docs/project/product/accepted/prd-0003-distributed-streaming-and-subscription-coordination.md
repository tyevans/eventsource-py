---
id: '0003'
title: Distributed Streaming and Subscription Coordination
status: Accepted
created: 2026-10-07
target_persona: Jordan
component: subscriptions
governing_adrs:
- ADR-0001
- ADR-0002
- ADR-0003
- ADR-0007
- ADR-0107
- ADR-0108
- ADR-0111
---

# PRD-0003 — Distributed Streaming and Subscription Coordination

## Who this is for

- **Jordan (The Streaming & Distributed Systems Platform Engineer)**: Platform operators managing Kafka, RabbitMQ, Redis, PostgreSQL, and resilient subscription consumers.
- **Chris (The SRE / Resilience & Cutover Operator)**: Site reliability engineers managing distributed advisory locks, connection pools, and graceful lifecycle shutdowns.
- **Alex (The Event-Sourced Domain Architect)**: Architects designing asynchronous read models and downstream event-driven workflows.
- **Morgan (The Autonomous Coding Agent)**: Coding agents verifying blackbox message bus delivery and error isolation semantics.

## What the person cannot do today

- **Broker Concurrency Pitfalls**: Hand-rolled message bus integrations frequently suffer from message loss, out-of-order delivery, or blocking retries.
- **Dual-Delivery and Checkpoint Race Conditions**: Streaming consumers that listen directly to bus notifications risk checkpoint desynchronization when out-of-order messages arrive before append store commits.
- **Poison-Pill Feed Blockages**: Unhandled exceptions in downstream projection handlers stall subscription runners or crash worker loops without isolating faulty messages.
- **Unbounded Memory Spikes**: High-volume background publish operations can outrun broker write throughput, exhausting host memory under burst traffic.
- **Resource Leaks on Termination**: Abrupt process termination drops active tasks, leaves distributed locks orphaned, or prematurely closes shared database connection pools.

## What good looks like

1. **Uniform EventBus Contract & Delivery Semantics**:
   - Consistent `EventBus` port implemented across Kafka, RabbitMQ, Redis, and InMemory transports.
   - At-least-once delivery with `HandlerDispatchError` aggregation; broker acks are withheld on dispatch failure to ensure redelivery.
   - Bounded background publishing degrades automatically to inline execution under backpressure without message drops.

2. **Feed-Driven Live Checkpointing & Ordered Delivery**:
   - `LiveRunner` uses broker messages strictly as wake-up signals, reading events from `GlobalEventFeed` to eliminate position drift and gap races.
   - Strictly ordered sequential event delivery per subscription; checkpoints commit only after the active event completes successfully.
   - Live batch delivery dispatches feed pages directly without artificial accumulator windows.

3. **Resilient Error Isolation & Dead-Letter Queuing**:
   - Individual handler exceptions do not prevent sibling handlers on the same bus topic from executing.
   - Configurable `ProjectionRetryPolicy` with exponential backoff and DLQ persistence isolates persistent poison pills without blocking streaming feeds.

4. **Distributed Advisory Locking & Clean Lifecycle Disposal**:
   - PostgreSQL session-level advisory locks provide mutually exclusive distributed leases that release automatically upon connection drop or process crash.
   - `SupportsClose` protocol and explicit `owns_engine` ownership safeguard shared connection pools from accidental disposal while ensuring proper resource teardown.
   - Universal `EventSourceError` hierarchy provides transparent diagnostic context and wraps driver exceptions into `EventStoreConnectionError`.

## What this does not do

- **Consensus Daemon Implementation**: Distributed lease coordination delegates to proven infrastructure (PostgreSQL advisory locks) rather than embedding a Raft/Paxos consensus daemon.
- **Cross-Broker Distributed Transactions**: Two-phase commit (2PC) or distributed rollbacks across heterogeneous message brokers are caller/infrastructure concerns.
- **Dynamic Broker Topic Administration**: Provisioning broker cluster topologies or managing queue partitions occurs via deployment infrastructure, not runtime library code.

## Checkable Outcomes

1. Publishing events across Kafka, RabbitMQ, Redis, and InMemory buses invokes registered subscribers and aggregates handler exceptions into `HandlerDispatchError`.
2. High-volume background publishing respects task concurrency bounds and degrades cleanly to inline execution without task drops.
3. `LiveRunner` processes feed pages sequentially and checkpoints positions deterministically without duplicate event processing.
4. Poison-pill events failing handler retries route to the Dead-Letter Queue while subsequent feed events continue processing.
5. Multi-instance workers coordinate single-active subscriptions via leader election or distributed PostgreSQL advisory locks without split-brain collisions.
6. Invoking `close()` on store or bus adapters releases adapter-allocated resources while respecting external engine ownership.

## Linked User Stories

- [`US-0003`](../../user_stories/accepted/us-0003-publish-and-subscribe-to-events-via-message-buses.md): Publish and Subscribe to Events via Message Buses
- [`US-0009`](../../user_stories/accepted/us-0009-coordinate-subscriptions-and-delivery-guarantees.md): Coordinate Subscriptions with Delivery Guarantees and DLQ Error Isolation
- [`US-0011`](../../user_stories/accepted/us-0011-distributed-locking-resilient-lifecycle.md): Coordinate Distributed Advisory Locks and Resilient Connection Lifecycle
- [`US-0014`](../../user_stories/accepted/us-0014-stage-and-publish-events-via-transactional-outbox.md): Stage and Publish Events via Transactional Outbox
- [`US-0022`](../../user_stories/accepted/us-0022-inspect-replay-and-purge-broker-dead-letter-queues.md): Inspect, Replay, and Purge Broker Dead-Letter Queues with Loop Protection
- [`US-0023`](../../user_stories/accepted/us-0023-dynamically-pause-resume-and-drain-subscriptions.md): Dynamically Pause, Resume, and Drain Subscriptions During Operational Interventions
- [`US-0024`](../../user_stories/accepted/us-0024-monitor-subscription-health-via-composite-checks-and-probes.md): Monitor Subscription Health via Composite Checks and Kubernetes Probes
- [`US-0025`](../../user_stories/accepted/us-0025-handle-kafka-consumer-group-rebalances-and-partition-lag.md): Handle Kafka Consumer Group Rebalances and Partition Lag Monitoring
- [`US-0026`](../../user_stories/accepted/us-0026-coordinate-peer-health-and-redistribute-subscriptions.md): Coordinate Peer Health and Redistribute Subscriptions on Instance Eviction
