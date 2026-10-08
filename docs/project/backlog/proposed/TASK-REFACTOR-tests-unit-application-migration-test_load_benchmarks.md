---
id: REFACTOR-tests-unit-application-migration-test_load_benchmarks
title: Refactor and Decompose Legacy File test_load_benchmarks.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-migration-test_load_benchmarks: Refactor Legacy File test_load_benchmarks.py

## Summary
The grandfathered debt file `tests/unit/application/migration/test_load_benchmarks.py` contains 1608 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_load_benchmarks_memory.py, test_load_benchmarks_result.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/migration/test_load_benchmarks/` with submodules:
- `test_load_benchmarks_memory.py`: MemoryResult, InMemoryMigrationRepository, InMemoryPositionMappingRepository, TestMemoryUsage, src_pos, tgt_pos, BenchmarkEvent, RaisingTargetStore, create_test_events, TestBulkCopyThroughput, TestDualWriteOverhead, TestConcurrentMigrations, TestPositionTranslation, TestStatusStreaming, TestPerformanceRegression, TestBenchmarkSummary
- `test_load_benchmarks_result.py`: BenchmarkResult, LatencyResult

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/unit/application/migration/test_load_benchmarks.py (1608 lines):
  Submodule 'test_load_benchmarks_memory.py' (~1367 lines):
    - [class] MemoryResult (lines 174-181)
    - [class] InMemoryMigrationRepository (lines 233-315)
    - [class] InMemoryPositionMappingRepository (lines 318-446)
    - [class] TestMemoryUsage (lines 844-953)
    - [function] src_pos (lines 73-75)
    - [function] tgt_pos (lines 78-80)
    - [class] BenchmarkEvent (lines 92-96)
    - [class] RaisingTargetStore (lines 189-225)
    - [function] create_test_events (lines 454-504)
    - [class] TestBulkCopyThroughput (lines 512-640)
    - [class] TestDualWriteOverhead (lines 648-836)
    - [class] TestConcurrentMigrations (lines 961-1090)
    - [class] TestPositionTranslation (lines 1098-1201)
    - [class] TestStatusStreaming (lines 1209-1353)
    - [class] TestPerformanceRegression (lines 1361-1469)
    - [class] TestBenchmarkSummary (lines 1477-1608)
  Submodule 'test_load_benchmarks_result.py' (~63 lines):
    - [class] BenchmarkResult (lines 105-132)
    - [class] LatencyResult (lines 136-170)
  Suggested barrel exports:
    from .test_load_benchmarks_memory import MemoryResult, InMemoryMigrationRepository, InMemoryPositionMappingRepository, TestMemoryUsage, src_pos, tgt_pos, BenchmarkEvent, RaisingTargetStore, create_test_events, TestBulkCopyThroughput, TestDualWriteOverhead, TestConcurrentMigrations, TestPositionTranslation, TestStatusStreaming, TestPerformanceRegression, TestBenchmarkSummary
    from .test_load_benchmarks_result import BenchmarkResult, LatencyResult

    __all__ = ["MemoryResult", "InMemoryMigrationRepository", "InMemoryPositionMappingRepository", "TestMemoryUsage", "src_pos", "tgt_pos", "BenchmarkEvent", "RaisingTargetStore", "create_test_events", "TestBulkCopyThroughput", "TestDualWriteOverhead", "TestConcurrentMigrations", "TestPositionTranslation", "TestStatusStreaming", "TestPerformanceRegression", "TestBenchmarkSummary", "BenchmarkResult", "LatencyResult"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
