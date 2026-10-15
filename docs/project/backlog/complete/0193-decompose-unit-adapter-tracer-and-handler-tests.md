---
id: "0193"
title: Decompose Unit Adapter, Tracer, and Handler Test Modules Below 400 Lines
status: Complete
target_bc: testing
governing_adrs:
  - ADR-0002
  - ADR-0003
governing_prds:
  - PRD-0004
governing_stories:
  - US-0012
  - US-0015
priority: P2
---

# TASK-0193: Decompose Unit Adapter, Tracer, and Handler Test Modules Below 400 Lines

## Summary
Decompose `tests/unit/adapters/sync/test_adapter.py` (499 lines), `tests/unit/test_fixtures.py` (471 lines), `tests/unit/readmodels/test_handler_integration.py` (444 lines), `tests/unit/adapters/_bus/test_eventbus_tracing_patterns.py` (442 lines), `tests/unit/observability/test_tracer.py` (439 lines), and `tests/unit/test_additional_coverage.py` (403 lines) below 400 lines.

## Required Actions
1. Decompose the remaining 6 unit test modules >= 400 lines into focused modular test packages strictly under 400 lines per file.
2. Verify 100% test pass rate across the unit test suite.

## Definition of Done (Blackbox Frontdoor TDD)
1. All unit test files across `tests/unit/` are strictly under 400 lines.
2. `uv run spec-ops health` reports 0 proactive file-length warnings across the entire repository.
