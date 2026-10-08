---
id: REFACTOR-tests-integration-subscriptions-test_advanced_features
title: Refactor and Decompose Legacy File test_advanced_features.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-integration-subscriptions-test_advanced_features: Refactor Legacy File test_advanced_features.py

## Summary
The grandfathered debt file `tests/integration/subscriptions/test_advanced_features.py` contains 1510 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_advanced_features_user.py, test_advanced_features_projection.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/integration/subscriptions/test_advanced_features/` with submodules:
- `test_advanced_features_user.py`: AdvTestUserRegistered, AdvTestUserUpdated, position_after, AdvTestPaymentReceived, TestEventTypeFiltering, TestMultipleSubscriptions, TestHealthCheckAPI, TestPauseResume, TestCombinedAdvancedFeatures, TestOpenTelemetryMetrics, TestFilteringStatistics
- `test_advanced_features_projection.py`: FilteredProjection, PausableProjection

## AST Decomposition Blueprint
Decomposition Blueprint for tests/integration/subscriptions/test_advanced_features.py (1510 lines):
  Submodule 'test_advanced_features_user.py' (~1309 lines):
    - [class] AdvTestUserRegistered (lines 72-76)
    - [class] AdvTestUserUpdated (lines 80-84)
    - [function] position_after (lines 64-69)
    - [class] AdvTestPaymentReceived (lines 88-92)
    - [class] TestEventTypeFiltering (lines 181-385)
    - [class] TestMultipleSubscriptions (lines 393-571)
    - [class] TestHealthCheckAPI (lines 579-895)
    - [class] TestPauseResume (lines 903-1156)
    - [class] TestCombinedAdvancedFeatures (lines 1164-1332)
    - [class] TestOpenTelemetryMetrics (lines 1340-1451)
    - [class] TestFilteringStatistics (lines 1459-1510)
  Submodule 'test_advanced_features_projection.py' (~72 lines):
    - [class] FilteredProjection (lines 100-129)
    - [class] PausableProjection (lines 132-173)
  Suggested barrel exports:
    from .test_advanced_features_user import AdvTestUserRegistered, AdvTestUserUpdated, position_after, AdvTestPaymentReceived, TestEventTypeFiltering, TestMultipleSubscriptions, TestHealthCheckAPI, TestPauseResume, TestCombinedAdvancedFeatures, TestOpenTelemetryMetrics, TestFilteringStatistics
    from .test_advanced_features_projection import FilteredProjection, PausableProjection

    __all__ = ["AdvTestUserRegistered", "AdvTestUserUpdated", "position_after", "AdvTestPaymentReceived", "TestEventTypeFiltering", "TestMultipleSubscriptions", "TestHealthCheckAPI", "TestPauseResume", "TestCombinedAdvancedFeatures", "TestOpenTelemetryMetrics", "TestFilteringStatistics", "FilteredProjection", "PausableProjection"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
