---
id: REFACTOR-tests-integration-subscriptions-test_resilience
title: Refactor and Decompose Legacy File test_resilience.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
governing_stories:
  - US-0012
target_bc: core
---

# TASK-REFACTOR-tests-integration-subscriptions-test_resilience: Refactor Legacy File test_resilience.py

## Summary
The grandfathered debt file `tests/integration/subscriptions/test_resilience.py` contains 1420 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_resilience_projection.py, test_resilience_chaos.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/integration/subscriptions/test_resilience/` with submodules:
- `test_resilience_projection.py`: ConcurrencyTrackingProjection, PositionalFailingProjection, TransientFailingProjection, VariableLatencyProjection, ChaosProjection, position_after, in_memory_dlq_repo, subscription_manager_with_dlq, TestSequentialDelivery, TestGracefulShutdown, TestRetryAndRecovery, TestCircuitBreaker, TestErrorHandlingIntegration, TestCombinedResilience, TestHealthCheckIntegration
- `test_resilience_chaos.py`: TestChaos

## AST Decomposition Blueprint
Decomposition Blueprint for tests/integration/subscriptions/test_resilience.py (1420 lines):
  Submodule 'test_resilience_projection.py' (~1169 lines):
    - [class] ConcurrencyTrackingProjection (lines 80-117)
    - [class] PositionalFailingProjection (lines 120-152)
    - [class] TransientFailingProjection (lines 155-188)
    - [class] VariableLatencyProjection (lines 191-224)
    - [class] ChaosProjection (lines 227-258)
    - [function] position_after (lines 72-77)
    - [function] in_memory_dlq_repo (lines 267-271)
    - [function] subscription_manager_with_dlq (lines 275-290)
    - [class] TestSequentialDelivery (lines 298-391)
    - [class] TestGracefulShutdown (lines 399-557)
    - [class] TestRetryAndRecovery (lines 565-655)
    - [class] TestCircuitBreaker (lines 663-786)
    - [class] TestErrorHandlingIntegration (lines 794-980)
    - [class] TestCombinedResilience (lines 1098-1339)
    - [class] TestHealthCheckIntegration (lines 1347-1420)
  Submodule 'test_resilience_chaos.py' (~103 lines):
    - [class] TestChaos (lines 988-1090)
  Suggested barrel exports:
    from .test_resilience_projection import ConcurrencyTrackingProjection, PositionalFailingProjection, TransientFailingProjection, VariableLatencyProjection, ChaosProjection, position_after, in_memory_dlq_repo, subscription_manager_with_dlq, TestSequentialDelivery, TestGracefulShutdown, TestRetryAndRecovery, TestCircuitBreaker, TestErrorHandlingIntegration, TestCombinedResilience, TestHealthCheckIntegration
    from .test_resilience_chaos import TestChaos

    __all__ = ["ConcurrencyTrackingProjection", "PositionalFailingProjection", "TransientFailingProjection", "VariableLatencyProjection", "ChaosProjection", "position_after", "in_memory_dlq_repo", "subscription_manager_with_dlq", "TestSequentialDelivery", "TestGracefulShutdown", "TestRetryAndRecovery", "TestCircuitBreaker", "TestErrorHandlingIntegration", "TestCombinedResilience", "TestHealthCheckIntegration", "TestChaos"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
