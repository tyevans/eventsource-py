---
id: REFACTOR-tests-unit-application-subscriptions-test_error_handling
title: Refactor and Decompose Legacy File test_error_handling.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-subscriptions-test_error_handling: Refactor Legacy File test_error_handling.py

## Summary
The grandfathered debt file `tests/unit/application/subscriptions/test_error_handling.py` contains 857 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_error_handling_classifier.py, test_error_handling_handler.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/subscriptions/test_error_handling/` with submodules:
- `test_error_handling_classifier.py`: TestErrorClassifier, TestGetDefaultClassifier, pos, TestErrorClassification, TestErrorCategory, TestErrorSeverity, TestErrorInfo, TestErrorStats, TestErrorHandlingConfig, TestErrorHandlingStrategy, TestModuleImports
- `test_error_handling_handler.py`: TestErrorHandlerRegistry, TestSubscriptionErrorHandler

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/application/subscriptions/test_error_handling.py (857 lines):
  Submodule 'test_error_handling_classifier.py' (~396 lines):
    - [class] TestErrorClassifier (lines 95-224)
    - [class] TestGetDefaultClassifier (lines 227-239)
    - [function] pos (lines 38-40)
    - [class] TestErrorClassification (lines 43-69)
    - [class] TestErrorCategory (lines 72-81)
    - [class] TestErrorSeverity (lines 84-92)
    - [class] TestErrorInfo (lines 247-288)
    - [class] TestErrorStats (lines 291-387)
    - [class] TestErrorHandlingConfig (lines 562-586)
    - [class] TestErrorHandlingStrategy (lines 589-598)
    - [class] TestModuleImports (lines 828-857)
  Submodule 'test_error_handling_handler.py' (~375 lines):
    - [class] TestErrorHandlerRegistry (lines 395-554)
    - [class] TestSubscriptionErrorHandler (lines 606-820)
  Suggested barrel exports:
    from .test_error_handling_classifier import TestErrorClassifier, TestGetDefaultClassifier, pos, TestErrorClassification, TestErrorCategory, TestErrorSeverity, TestErrorInfo, TestErrorStats, TestErrorHandlingConfig, TestErrorHandlingStrategy, TestModuleImports
    from .test_error_handling_handler import TestErrorHandlerRegistry, TestSubscriptionErrorHandler

    __all__ = ["TestErrorClassifier", "TestGetDefaultClassifier", "pos", "TestErrorClassification", "TestErrorCategory", "TestErrorSeverity", "TestErrorInfo", "TestErrorStats", "TestErrorHandlingConfig", "TestErrorHandlingStrategy", "TestModuleImports", "TestErrorHandlerRegistry", "TestSubscriptionErrorHandler"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
