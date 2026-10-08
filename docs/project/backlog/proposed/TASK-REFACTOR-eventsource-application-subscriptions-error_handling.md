---
id: REFACTOR-eventsource-application-subscriptions-error_handling
title: Refactor and Decompose Legacy File error_handling.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-eventsource-application-subscriptions-error_handling: Refactor Legacy File error_handling.py

## Summary
The grandfathered debt file `src/eventsource/application/subscriptions/error_handling.py` contains 1105 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (error_handling_classifier.py, error_handling_handler.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/application/subscriptions/error_handling/` with submodules:
- `error_handling_classifier.py`: ErrorClassifier, get_default_classifier, ErrorCategory, ErrorSeverity, ErrorClassification, ErrorInfo, ErrorStats, ErrorHandlingStrategy, ErrorHandlingConfig
- `error_handling_handler.py`: ErrorHandlerRegistry, SubscriptionErrorHandler

## AST Decomposition Blueprint
Decomposition Blueprint for src/eventsource/application/subscriptions/error_handling.py (1105 lines):
  Submodule 'error_handling_classifier.py' (~442 lines):
    - [class] ErrorClassifier (lines 103-286)
    - [function] get_default_classifier (lines 293-295)
    - [class] ErrorCategory (lines 46-66)
    - [class] ErrorSeverity (lines 69-86)
    - [class] ErrorClassification (lines 90-100)
    - [class] ErrorInfo (lines 304-342)
    - [class] ErrorStats (lines 346-466)
    - [class] ErrorHandlingStrategy (lines 637-657)
    - [class] ErrorHandlingConfig (lines 661-684)
  Submodule 'error_handling_handler.py' (~536 lines):
    - [class] ErrorHandlerRegistry (lines 482-629)
    - [class] SubscriptionErrorHandler (lines 692-1079)
  Suggested barrel exports:
    from .error_handling_classifier import ErrorClassifier, get_default_classifier, ErrorCategory, ErrorSeverity, ErrorClassification, ErrorInfo, ErrorStats, ErrorHandlingStrategy, ErrorHandlingConfig
    from .error_handling_handler import ErrorHandlerRegistry, SubscriptionErrorHandler

    __all__ = ["ErrorClassifier", "get_default_classifier", "ErrorCategory", "ErrorSeverity", "ErrorClassification", "ErrorInfo", "ErrorStats", "ErrorHandlingStrategy", "ErrorHandlingConfig", "ErrorHandlerRegistry", "SubscriptionErrorHandler"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
