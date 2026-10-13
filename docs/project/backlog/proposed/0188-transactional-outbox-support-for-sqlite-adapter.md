---
id: "0188"
title: Transactional Outbox Support for SQLite Adapter
status: Proposed
target_bc: adapters
governing_adrs:
  - ADR-0007
  - ADR-0168
priority: P2
---

# TASK-0188: Transactional Outbox Support for SQLite Adapter

## Summary
Implement transactional outbox staging and publishing in the SQLite event store adapter, bringing full feature parity and uniform delivery guarantees with the PostgreSQL adapter.

## Problem Statement & Context
- `src/eventsource/adapters/sqlite/store.py` currently lacks transactional outbox support (`outbox_enabled`), while `adapters/postgresql/store.py` provides it.
- Applications switching between SQLite (local development / edge) and PostgreSQL (production) experience differing delivery guarantees and cannot rely on uniform outbox staging.
- The gap is currently documented as a limitation in the event bus documentation.

## Required Actions
1. Define SQLite outbox schema tables and associated schema migration utilities.
2. Implement transactional outbox publishing and stage table commits in `src/eventsource/adapters/sqlite/store.py`.
3. Add conformance tests to verify consistent outbox behavior across SQLite and PostgreSQL adapters (`OutboxConformance`).

## Definition of Done (Blackbox Frontdoor TDD)
1. Setting `outbox_enabled=True` on `SQLiteEventStore` atomically stages published events in the local outbox table during `append()`.
2. Conformance tests pass across both SQLite and PostgreSQL backends through the public `EventStore` frontdoor.
3. All source files strictly comply with ADR-0002 (<500 lines limit).
