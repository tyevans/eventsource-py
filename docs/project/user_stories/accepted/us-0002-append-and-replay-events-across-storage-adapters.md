---
id: '0002'
title: Append and Replay Events Across Storage Adapters
status: Accepted
created: 2026-10-07
persona: Jordan (The Backend Platform Engineer)
target_bc: adapters
feature: FEAT-STORE-ADAPTERS
governing_prd: PRD-0001
scenarios:
  - Append and load event stream in storage adapter
  - Replay events from global position offset
---

# US-0002 — Append and Replay Events Across Storage Adapters

## Governing PRD
- [`PRD-0001: Production-Ready Event Sourcing and Live Migration Framework`](../../product/accepted/prd-0001-production-ready-event-sourcing-framework.md)

## User Story

**As a** backend platform engineer (Jordan),
**I want** to persist events into PostgreSQL, SQLite, or InMemory storage backends through a uniform `EventStore` port,
**So that** streams can be replayed from sequence zero or read from global positions across heterogeneous environments.

## Acceptance Criteria

```gherkin
Scenario: Append and load event stream in storage adapter
  Given a configured "EventStore" adapter instance
  When the caller appends 3 domain events to aggregate stream "order-123"
  Then loading the stream "order-123" returns 3 events ordered by version
  And each event preserves its payload, metadata, and timestamp.
```

```gherkin
Scenario: Replay events from global position offset
  Given an event store with 10 total committed events
  When the caller reads all events from global position 5 with batch limit 5
  Then exactly 5 events starting at global position 5 are returned in strictly ascending position order.
```
