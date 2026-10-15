---
id: "0192"
title: Decompose Benchmark Suites Below 400-Line Threshold
status: Complete
target_bc: testing
governing_adrs:
  - ADR-0002
  - ADR-0003
governing_prds:
  - PRD-0004
governing_stories:
  - US-0012
priority: P2
---

# TASK-0192: Decompose Benchmark Suites Below 400-Line Threshold

## Summary
Decompose `tests/benchmarks/test_repositories.py` (468 lines), `tests/benchmarks/test_event_store.py` (454 lines), `tests/benchmarks/test_serialization.py` (433 lines), and `tests/benchmarks/test_projections.py` (419 lines) so all benchmark modules are strictly under 400 lines.

## Required Actions
1. Split oversized benchmark modules in `tests/benchmarks/` into focused benchmark files strictly under 400 lines.
2. Verify all benchmark tests collect and execute cleanly.

## Definition of Done (Blackbox Frontdoor TDD)
1. Zero proactive file-length warnings in `tests/benchmarks/`.
2. Benchmark test collection and execution pass cleanly.
