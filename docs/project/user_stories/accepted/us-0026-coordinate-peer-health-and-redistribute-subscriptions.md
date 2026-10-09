---
id: '0026'
title: Coordinate Peer Health and Redistribute Subscriptions on Instance Eviction
status: Accepted
created: 2026-10-09
persona: Jordan (The Streaming & Distributed Systems Platform Engineer)
target_bc: subscriptions
feature: FEAT-SUB-COORDINATION-REDISTRIBUTION
governing_prd: PRD-0003
scenarios:
- Peer nodes broadcast periodic heartbeats to maintain cluster presence
- Missing peer heartbeats trigger timeout eviction and orphan detection
- Planned shutdown broadcasts notification for immediate work handover
- Standby peers claim orphaned subscriptions without duplicate execution
governing_adrs:
- ADR-0001
- ADR-0003
- ADR-0108
- ADR-0111
---

# US-0026: Coordinate Peer Health and Redistribute Subscriptions on Instance Eviction

## Governing PRD
- [`PRD-0003: Distributed Streaming and Subscription Coordination`](../../product/accepted/prd-0003-distributed-streaming-and-subscription-coordination.md)

## User Story

**As a** streaming and distributed systems platform engineer (Jordan),
**I want** distributed subscription managers to monitor peer health via heartbeats and redistribute work when instances crash or scale down,
**So that** subscription workloads maintain high availability across multi-node clusters and orphaned partitions are promptly claimed by surviving peers.

## Acceptance Criteria

```gherkin
Scenario: Peer nodes broadcast periodic heartbeats to maintain cluster presence
  Given a cluster of distributed subscription manager instances
  When nodes are running actively
  Then each node broadcasts periodic HeartbeatMessage payloads over the coordination bus
  And active peer registries are updated with fresh heartbeat timestamps.
```

```gherkin
Scenario: Missing peer heartbeats trigger timeout eviction and orphan detection
  Given a cluster where one node crashes or is partitioned from the network
  When the heartbeat timeout threshold is exceeded
  Then surviving peers evict the unresponsive node from the cluster topology
  And identify all subscriptions previously owned by the evicted node as orphaned.
```

```gherkin
Scenario: Planned shutdown broadcasts notification for immediate work handover
  Given a node initiating graceful shutdown (e.g. during a rolling deployment)
  When the shutdown sequence begins
  Then it broadcasts a ShutdownNotification with intent SCALING_DOWN or PLANNED_STOP
  And peer nodes initiate immediate reallocation without waiting for heartbeat timeout.
```

```gherkin
Scenario: Standby peers claim orphaned subscriptions without duplicate execution
  Given orphaned subscriptions identified following peer eviction or departure
  When standby peers negotiate work assignment
  Then orphaned subscriptions are assigned to surviving peers based on partition topology
  And each subscription is claimed by exactly one active instance.
```

## Implementation Status & Verification

- **Status**: Implemented & Verified (100% test pass rate, 0 backdoor mocks)
- **Implementation Modules**:
  - `src/eventsource/application/subscriptions/coordination.py`: `WorkRedistributionCoordinator`, `HeartbeatMessage`, `ShutdownNotification`, `WorkAssignment`.
  - `src/eventsource/ports/coordination.py`: Coordination interfaces.
- **Verified Test Suites**:
  - `tests/unit/application/subscriptions/test_coordination.py`: Work redistribution, heartbeats, and peer eviction.
  - `tests/unit/adapters/test_memory_coordination_conformance.py`: Multi-node failover simulation.
