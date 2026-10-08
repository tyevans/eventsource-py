---
id: REFACTOR-tests-unit-application-subscriptions-test_subscription_config
title: Refactor and Decompose Legacy File test_subscription_config.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-subscriptions-test_subscription_config: Refactor Legacy File test_subscription_config.py

## Summary
The grandfathered debt file `tests/unit/application/subscriptions/test_subscription_config.py` contains 513 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_subscription_config_defaults.py, test_subscription_config_values.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/subscriptions/test_subscription_config/` with submodules:
- `test_subscription_config_defaults.py`: TestSubscriptionConfigDefaults, TestSubscriptionConfigValidation, TestSubscriptionConfigImmutability, TestCheckpointStrategy, TestConvenienceFunctions, TestSubscriptionExceptions, TestExceptionHierarchy, TestModuleImports
- `test_subscription_config_values.py`: TestSubscriptionConfigCustomValues

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/unit/application/subscriptions/test_subscription_config.py (513 lines):
  Submodule 'test_subscription_config_defaults.py' (~404 lines):
    - [class] TestSubscriptionConfigDefaults (lines 27-68)
    - [class] TestSubscriptionConfigValidation (lines 140-194)
    - [class] TestSubscriptionConfigImmutability (lines 197-222)
    - [class] TestCheckpointStrategy (lines 225-246)
    - [class] TestConvenienceFunctions (lines 249-274)
    - [class] TestSubscriptionExceptions (lines 277-372)
    - [class] TestExceptionHierarchy (lines 375-430)
    - [class] TestModuleImports (lines 433-513)
  Submodule 'test_subscription_config_values.py' (~67 lines):
    - [class] TestSubscriptionConfigCustomValues (lines 71-137)
  Suggested barrel exports:
    from .test_subscription_config_defaults import TestSubscriptionConfigDefaults, TestSubscriptionConfigValidation, TestSubscriptionConfigImmutability, TestCheckpointStrategy, TestConvenienceFunctions, TestSubscriptionExceptions, TestExceptionHierarchy, TestModuleImports
    from .test_subscription_config_values import TestSubscriptionConfigCustomValues

    __all__ = ["TestSubscriptionConfigDefaults", "TestSubscriptionConfigValidation", "TestSubscriptionConfigImmutability", "TestCheckpointStrategy", "TestConvenienceFunctions", "TestSubscriptionExceptions", "TestExceptionHierarchy", "TestModuleImports", "TestSubscriptionConfigCustomValues"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
