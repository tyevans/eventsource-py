---
id: "0189"
title: Diagnosability and Lifecycle Management for Connection-Owning Adapters
status: Proposed
target_bc: adapters
governing_adrs:
  - ADR-0007
  - ADR-0053
  - ADR-0162
priority: P2
---

# TASK-0189: Diagnosability and Lifecycle Management for Connection-Owning Adapters

## Summary
Add asynchronous context managers, informative `ResourceWarning` finalizers, and explicit connection lifecycle auditing to connection-owning adapters (notably `SQLiteSnapshotStore`) to eliminate thread leaks on event loop termination.

## Problem Statement & Context
- Unclosed instances of `SQLiteSnapshotStore` leak worker threads managed by `aiosqlite`.
- When the event loop closes, `aiosqlite` worker threads raise unhandled `RuntimeError: Event loop is closed` errors.
- The error traceback does not identify the leaked store instance, file path, or allocation site.
- Pytest attributes these failures to unrelated subsequent tests as unhandled thread warnings.
- Store classes implement `SupportsClose`, but lack ergonomic async context manager bindings.

## Required Actions
1. Implement a `__del__` finalizer on `SQLiteSnapshotStore` that emits a `ResourceWarning` containing the database file path when destroyed without explicit closure.
2. Implement asynchronous context manager support (`__aenter__` and `__aexit__`) to allow clean `async with` scoping.
3. Document explicit connection lifecycle requirements in adapter class docstrings.
4. Audit test fixtures, factories, and consumer helpers across the test suite to ensure all opened instances are closed cleanly.

## Definition of Done (Blackbox Frontdoor TDD)
1. `SQLiteSnapshotStore` supports `async with` and properly tears down internal `aiosqlite` connections upon exit.
2. Unclosed instances trigger an informative `ResourceWarning` naming the leaked database path.
3. Zero unhandled thread warnings or `RuntimeError: Event loop is closed` across the test suite.
4. All source files strictly comply with ADR-0002 (<500 lines limit).
