---
id: "0189"
title: Ergonomic Async Context Managers, Resource Leak Finalizers, and Snapshot Store Decomposition
status: Refined
target_bc: adapters
governing_adrs:
  - ADR-0002
  - ADR-0007
  - ADR-0105
  - ADR-0106
governing_prds:
  - PRD-0001
governing_stories:
  - US-0007
  - US-0021
priority: P1
---

# TASK-0189: Ergonomic Async Context Managers, Resource Leak Finalizers, and Snapshot Store Decomposition

## Summary
Add `async with` context manager support (`__aenter__` and `__aexit__`) across `SupportsClose` stores (`SQLiteSnapshotStore`, `SQLiteEventStore`, `PostgreSQLSnapshotStore`, `PostgreSQLEventStore`, `InMemorySnapshotStore`, `InMemoryEventStore`), add `__del__` `ResourceWarning` finalizers to connection-owning SQLite adapters, migrate snapshot stores to `orjson`, and decompose `sqlite/snapshots.py` (463 lines) and `postgresql/snapshots.py` (406 lines) below 400 lines.

## Problem Statement & Context
- Unclosed instances of `SQLiteSnapshotStore` and `SQLiteEventStore` hold `aiosqlite` worker threads without emitting a `ResourceWarning` identifying the leaked database path.
- Store implementations implement `close()` (`SupportsClose`), but lack `async with store:` context manager support (`__aenter__`/`__aexit__`).
- `src/eventsource/adapters/sqlite/snapshots.py` (463 lines) and `src/eventsource/adapters/postgresql/snapshots.py` (406 lines) exceed the 400-line warning threshold and still use stdlib `json` instead of `orjson`.

## Required Actions
1. Add `__aenter__` and `__aexit__` async context manager methods to `SupportsClose` (`src/eventsource/ports/lifecycle.py`) and all store implementations (`SQLiteSnapshotStore`, `SQLiteEventStore`, `PostgreSQLSnapshotStore`, `PostgreSQLEventStore`, `InMemorySnapshotStore`, `InMemoryEventStore`), and verify in `LifecycleConformance`.
2. Implement `__del__` finalizers on `SQLiteSnapshotStore` and `SQLiteEventStore` that emit a `ResourceWarning` naming the database path when garbage-collected with an open connection.
3. Migrate `SQLiteSnapshotStore`, `PostgreSQLSnapshotStore`, and `sqlite/readmodels_mutation.py` from stdlib `json` to `orjson` (`json_dumps`/`json_loads`), and decompose `sqlite/snapshots.py` (463 lines) and `postgresql/snapshots.py` (406 lines) below 400 lines.

## Definition of Done (Blackbox Frontdoor TDD)
1. All event stores and snapshot stores support `async with store:` and close cleanly on exit.
2. Unclosed `SQLiteSnapshotStore` and `SQLiteEventStore` instances emit an informative `ResourceWarning` with the database path.
3. `sqlite/snapshots.py` and `postgresql/snapshots.py` are strictly under 400 lines and use `orjson`.
