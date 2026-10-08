---
id: '0105'
title: Clean Storage Ports, Composed Protocols, and Connection Lifecycle
status: Accepted
target_bc: store
governing_prds:
- PRD-0001
governing_stories:
- US-0002
- US-0011
---

# ADR-0105: Clean Storage Ports, Composed Protocols, and Connection Lifecycle

## Summary
Segregated store ports, explicit connection ownership, and clean shutdown.

## Context
Monolithic storage interfaces force storage adapters to implement methods they don't support and cause resource leaks when database engines are shared across multiple repositories.

## Decision
1. Segregate storage interfaces into focused protocols: `EventAppender`, `EventReader`, `GlobalEventFeed`, and `SnapshotStore`.
2. Introduce explicit `owns_engine: bool` parameter on SQL storage adapters to determine whether the adapter created the pool or borrowed it from the host application.
3. Implement `SupportsClose` lifecycle protocol with graceful `aclose()` methods.
4. Retire legacy monolithic stores in favor of unified Clean Architecture storage ports.

## Consequences
- Storage adapters can be composed cleanly without fat interface pollution.
- Shared database connection pools are never disposed prematurely.
- Zero-leak resource cleanup in tests and production shutdowns.
