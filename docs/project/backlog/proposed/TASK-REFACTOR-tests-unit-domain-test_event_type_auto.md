---
id: REFACTOR-tests-unit-domain-test_event_type_auto
title: Refactor and Decompose Legacy File test_event_type_auto.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-domain-test_event_type_auto: Refactor Legacy File test_event_type_auto.py

## Summary
The grandfathered debt file `tests/unit/domain/test_event_type_auto.py` contains 656 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_event_type_auto_warning.py, test_event_type_auto_behavior.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/domain/test_event_type_auto/` with submodules:
- `test_event_type_auto_warning.py`: TestEventTypeMismatchWarning, TestSuppressEventTypeWarningAttribute, TestEventTypeAutoDerivation, TestExplicitEventTypePreservation, TestDictConstruction, TestEdgeCases, TestBackwardCompatibility, TestIntegrationWithEventRegistry, TestSubclassingDoesNotCorruptParent
- `test_event_type_auto_behavior.py`: TestInheritanceBehavior, TestModelValidatorBehavior

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/unit/domain/test_event_type_auto.py (656 lines):
  Submodule 'test_event_type_auto_warning.py' (~491 lines):
    - [class] TestEventTypeMismatchWarning (lines 217-260)
    - [class] TestSuppressEventTypeWarningAttribute (lines 463-512)
    - [class] TestEventTypeAutoDerivation (lines 27-82)
    - [class] TestExplicitEventTypePreservation (lines 85-122)
    - [class] TestDictConstruction (lines 125-214)
    - [class] TestEdgeCases (lines 329-410)
    - [class] TestBackwardCompatibility (lines 413-460)
    - [class] TestIntegrationWithEventRegistry (lines 515-548)
    - [class] TestSubclassingDoesNotCorruptParent (lines 608-656)
  Submodule 'test_event_type_auto_behavior.py' (~119 lines):
    - [class] TestInheritanceBehavior (lines 263-326)
    - [class] TestModelValidatorBehavior (lines 551-605)
  Suggested barrel exports:
    from .test_event_type_auto_warning import TestEventTypeMismatchWarning, TestSuppressEventTypeWarningAttribute, TestEventTypeAutoDerivation, TestExplicitEventTypePreservation, TestDictConstruction, TestEdgeCases, TestBackwardCompatibility, TestIntegrationWithEventRegistry, TestSubclassingDoesNotCorruptParent
    from .test_event_type_auto_behavior import TestInheritanceBehavior, TestModelValidatorBehavior

    __all__ = ["TestEventTypeMismatchWarning", "TestSuppressEventTypeWarningAttribute", "TestEventTypeAutoDerivation", "TestExplicitEventTypePreservation", "TestDictConstruction", "TestEdgeCases", "TestBackwardCompatibility", "TestIntegrationWithEventRegistry", "TestSubclassingDoesNotCorruptParent", "TestInheritanceBehavior", "TestModelValidatorBehavior"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
