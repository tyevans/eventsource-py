---
id: REFACTOR-tests-integration-subscriptions-test_full_flow
title: Refactor and Decompose Legacy File test_full_flow.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
governing_stories:
  - US-0012
target_bc: core
---

# TASK-REFACTOR-tests-integration-subscriptions-test_full_flow: Refactor Legacy File test_full_flow.py

## Summary
The grandfathered debt file `tests/integration/subscriptions/test_full_flow.py` contains 673 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_full_flow_after.py, test_full_flow_and.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/integration/subscriptions/test_full_flow/` with submodules:
- `test_full_flow_after.py`: position_after, TestResumeAfterRestart, TestFullLifecycle, TestCheckpointPersistence, TestContextManagerUsage, TestMultipleConcurrentSubscribers, TestEdgeCases
- `test_full_flow_and.py`: TestErrorHandlingAndRecovery, TestHealthAndStatus

## AST Decomposition Blueprint
Decomposition Blueprint for tests/integration/subscriptions/test_full_flow.py (673 lines):
  Submodule 'test_full_flow_after.py' (~459 lines):
    - [function] position_after (lines 39-44)
    - [class] TestResumeAfterRestart (lines 167-257)
    - [class] TestFullLifecycle (lines 47-110)
    - [class] TestCheckpointPersistence (lines 113-164)
    - [class] TestContextManagerUsage (lines 260-311)
    - [class] TestMultipleConcurrentSubscribers (lines 314-416)
    - [class] TestEdgeCases (lines 583-673)
  Submodule 'test_full_flow_and.py' (~160 lines):
    - [class] TestErrorHandlingAndRecovery (lines 419-500)
    - [class] TestHealthAndStatus (lines 503-580)
  Suggested barrel exports:
    from .test_full_flow_after import position_after, TestResumeAfterRestart, TestFullLifecycle, TestCheckpointPersistence, TestContextManagerUsage, TestMultipleConcurrentSubscribers, TestEdgeCases
    from .test_full_flow_and import TestErrorHandlingAndRecovery, TestHealthAndStatus

    __all__ = ["position_after", "TestResumeAfterRestart", "TestFullLifecycle", "TestCheckpointPersistence", "TestContextManagerUsage", "TestMultipleConcurrentSubscribers", "TestEdgeCases", "TestErrorHandlingAndRecovery", "TestHealthAndStatus"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
