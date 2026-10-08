---
id: '0004'
title: Re-Benchmark PostgreSQL Catch-Up Horizon Predicate at Scale
status: Refined
created: 2026-10-07
governing_adrs:
  - ADR-0002
  - ADR-0003
governing_prds:
  - PRD-0001
governing_stories:
  - US-0002
target_bc: adapters
---

# TASK-0004: Re-Benchmark PostgreSQL Catch-Up Horizon Predicate at Scale

## Summary
Re-evaluate catch-up subscription horizon filtering on large PostgreSQL event tables under concurrent append load to prevent table scan degradation at high event volumes.

## Definition of Done
1. Benchmark harness verifies index utilization for catch-up range queries.
2. Query performance profile documented in docs/reference/.
3. No regression in existing storage suite tests.
