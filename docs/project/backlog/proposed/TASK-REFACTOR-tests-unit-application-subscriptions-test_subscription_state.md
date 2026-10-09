---
id: REFACTOR-tests-unit-application-subscriptions-test_subscription_state
title: Refactor and Decompose Legacy File test_subscription_state.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
governing_stories:
  - US-0012
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-subscriptions-test_subscription_state: Refactor Legacy File test_subscription_state.py

## Summary
The grandfathered debt file `tests/unit/application/subscriptions/test_subscription_state.py` contains 1119 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_subscription_state_transitions.py, test_subscription_state_subscriber.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/subscriptions/test_subscription_state/` with submodules:
- `test_subscription_state_transitions.py`: TestValidTransitions, TestSubscriptionStateTransitions, TestInvalidStateTransitions, pos, config, subscription, TestSubscriptionStateEnum, TestIsValidTransition, TestSubscriptionCreation, TestPositionTracking, TestStatisticsTracking, TestLagCalculation, TestSubscriptionProperties, TestSubscriptionStatus, TestSubscriptionGetStatus, TestSetError, TestStringRepresentation, TestConcurrency, TestTypeAliases, TestModuleImports
- `test_subscription_state_subscriber.py`: MockSubscriber, mock_subscriber

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/application/subscriptions/test_subscription_state.py (1119 lines):
  Submodule 'test_subscription_state_transitions.py' (~992 lines):
    - [class] TestValidTransitions (lines 115-165)
    - [class] TestSubscriptionStateTransitions (lines 395-486)
    - [class] TestInvalidStateTransitions (lines 489-537)
    - [function] pos (lines 33-35)
    - [function] config (lines 55-57)
    - [function] subscription (lines 61-67)
    - [class] TestSubscriptionStateEnum (lines 73-109)
    - [class] TestIsValidTransition (lines 168-333)
    - [class] TestSubscriptionCreation (lines 339-392)
    - [class] TestPositionTracking (lines 543-612)
    - [class] TestStatisticsTracking (lines 618-645)
    - [class] TestLagCalculation (lines 651-697)
    - [class] TestSubscriptionProperties (lines 703-759)
    - [class] TestSubscriptionStatus (lines 765-848)
    - [class] TestSubscriptionGetStatus (lines 851-927)
    - [class] TestSetError (lines 933-953)
    - [class] TestStringRepresentation (lines 959-981)
    - [class] TestConcurrency (lines 987-1046)
    - [class] TestTypeAliases (lines 1052-1072)
    - [class] TestModuleImports (lines 1078-1119)
  Submodule 'test_subscription_state_subscriber.py' (~11 lines):
    - [class] MockSubscriber (lines 38-45)
    - [function] mock_subscriber (lines 49-51)
  Suggested barrel exports:
    from .test_subscription_state_transitions import TestValidTransitions, TestSubscriptionStateTransitions, TestInvalidStateTransitions, pos, config, subscription, TestSubscriptionStateEnum, TestIsValidTransition, TestSubscriptionCreation, TestPositionTracking, TestStatisticsTracking, TestLagCalculation, TestSubscriptionProperties, TestSubscriptionStatus, TestSubscriptionGetStatus, TestSetError, TestStringRepresentation, TestConcurrency, TestTypeAliases, TestModuleImports
    from .test_subscription_state_subscriber import MockSubscriber, mock_subscriber

    __all__ = ["TestValidTransitions", "TestSubscriptionStateTransitions", "TestInvalidStateTransitions", "pos", "config", "subscription", "TestSubscriptionStateEnum", "TestIsValidTransition", "TestSubscriptionCreation", "TestPositionTracking", "TestStatisticsTracking", "TestLagCalculation", "TestSubscriptionProperties", "TestSubscriptionStatus", "TestSubscriptionGetStatus", "TestSetError", "TestStringRepresentation", "TestConcurrency", "TestTypeAliases", "TestModuleImports", "MockSubscriber", "mock_subscriber"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
