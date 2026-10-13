---
id: "0188"
title: Remove InMemoryReadModelRepository Legacy Shim, Decompose Outbox Tests, and Sync Outbox Documentation
status: Refined
target_bc: adapters
governing_adrs:
  - ADR-0002
  - ADR-0007
  - ADR-0105
governing_prds:
  - PRD-0001
governing_stories:
  - US-0002
  - US-0014
priority: P1
---

# TASK-0188: Remove InMemoryReadModelRepository Legacy Shim, Decompose Outbox Tests, and Sync Outbox Documentation

## Summary
Remove the legacy `InMemoryReadModelRepository.clear()` test-compatibility alias in favor of `truncate()`, decompose oversized outbox test suites (`test_memory_outbox.py` at 494 lines and `test_outbox.py` at 485 lines) below 400 lines, and update outbox documentation for SQLite parity.

## Problem Statement & Context
- `InMemoryReadModelRepository.clear()` (`src/eventsource/adapters/memory/readmodels.py` line 261) is a legacy alias for `truncate()` kept only for old tests.
- `tests/unit/adapters/test_memory_outbox.py` (494 lines) and `tests/integration/repositories/test_outbox.py` (485 lines) trigger proactive file-length warnings (>=400 lines).
- `docs/guides/event-bus.md` still notes SQLite outbox support as a limitation even though `SQLiteEventStore(outbox_enabled=True)` and `SQLiteOutboxRepository` provide full transactional outbox parity.

## Required Actions
1. Remove `InMemoryReadModelRepository.clear()` from `src/eventsource/adapters/memory/readmodels.py` and update callers in `tests/` to call `await repo.truncate()`.
2. Decompose `tests/unit/adapters/test_memory_outbox.py` (494 lines) and `tests/integration/repositories/test_outbox.py` (485 lines) into modular packages (<400 lines per file), ensuring `SQLiteOutboxRepository` and `SQLiteEventStore(outbox_enabled=True)` are verified alongside in-memory and PostgreSQL outbox implementations.
3. Update `docs/guides/event-bus.md` to document SQLite transactional outbox support.

## Definition of Done (Blackbox Frontdoor TDD)
1. `InMemoryReadModelRepository.clear()` legacy alias is removed and all tests use `truncate()`.
2. `test_memory_outbox` and `test_outbox` modules are strictly under 400 lines.
3. Documentation accurately reflects SQLite transactional outbox support.
