---
id: '0010'
title: Transactional Outbox for SQLite Adapter
status: Complete
governing_adrs:
- ADR-0007
- ADR-0126
- ADR-0153
governing_prds:
- PRD-0001
governing_stories:
- US-0002
target_bc: adapters
---

# TASK-0010: Transactional Outbox for SQLite Adapter

## Summary
The transactional outbox pattern guarantees reliable asynchronous event publication alongside store appends in PostgreSQL. Provide parity for SQLite using ACID transactions and WAL mode to support embedded production workloads.

## Definition of Done
1. `SQLiteOutboxRepository` implements `OutboxRepository` port contract.
2. Concurrent transaction tests verify atomic append-and-outbox consistency.
3. Conformance suite passes with 100% assertions satisfied.
