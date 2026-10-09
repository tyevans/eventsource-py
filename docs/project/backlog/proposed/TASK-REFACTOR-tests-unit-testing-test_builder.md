---
id: REFACTOR-tests-unit-testing-test_builder
title: Refactor and Decompose Legacy File test_builder.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
governing_stories:
  - US-0012
target_bc: core
---

# TASK-REFACTOR-tests-unit-testing-test_builder: Refactor Legacy File test_builder.py

## Summary
The grandfathered debt file `tests/unit/testing/test_builder.py` contains 722 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_builder_with.py, test_builder_sample.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/testing/test_builder/` with submodules:
- `test_builder_with.py`: TestEventBuilderWithAggregateId, TestEventBuilderWithEventId, TestEventBuilderWithTenantId, TestEventBuilderWithVersion, TestEventBuilderWithOccurredAt, TestEventBuilderWithCorrelationId, TestEventBuilderWithCausationId, TestEventBuilderWithActorId, TestEventBuilderWithMetadata, TestEventBuilderWithField, TestEventBuilderWithFields, MinimalEvent, TestEventBuilderInit, TestEventBuilderMethodChaining, TestEventBuilderBuild, TestEventBuilderRepr, TestEventBuilderTypeSafety, TestEventBuilderEdgeCases, TestEventBuilderIntegrationScenarios
- `test_builder_sample.py`: SampleEvent

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/testing/test_builder.py (722 lines):
  Submodule 'test_builder_with.py' (~662 lines):
    - [class] TestEventBuilderWithAggregateId (lines 73-96)
    - [class] TestEventBuilderWithEventId (lines 99-113)
    - [class] TestEventBuilderWithTenantId (lines 116-130)
    - [class] TestEventBuilderWithVersion (lines 133-153)
    - [class] TestEventBuilderWithOccurredAt (lines 156-170)
    - [class] TestEventBuilderWithCorrelationId (lines 173-187)
    - [class] TestEventBuilderWithCausationId (lines 190-204)
    - [class] TestEventBuilderWithActorId (lines 207-220)
    - [class] TestEventBuilderWithMetadata (lines 223-244)
    - [class] TestEventBuilderWithField (lines 247-277)
    - [class] TestEventBuilderWithFields (lines 280-302)
    - [class] MinimalEvent (lines 25-28)
    - [class] TestEventBuilderInit (lines 31-70)
    - [class] TestEventBuilderMethodChaining (lines 305-362)
    - [class] TestEventBuilderBuild (lines 365-489)
    - [class] TestEventBuilderRepr (lines 492-520)
    - [class] TestEventBuilderTypeSafety (lines 523-544)
    - [class] TestEventBuilderEdgeCases (lines 547-614)
    - [class] TestEventBuilderIntegrationScenarios (lines 617-722)
  Submodule 'test_builder_sample.py' (~6 lines):
    - [class] SampleEvent (lines 17-22)
  Suggested barrel exports:
    from .test_builder_with import TestEventBuilderWithAggregateId, TestEventBuilderWithEventId, TestEventBuilderWithTenantId, TestEventBuilderWithVersion, TestEventBuilderWithOccurredAt, TestEventBuilderWithCorrelationId, TestEventBuilderWithCausationId, TestEventBuilderWithActorId, TestEventBuilderWithMetadata, TestEventBuilderWithField, TestEventBuilderWithFields, MinimalEvent, TestEventBuilderInit, TestEventBuilderMethodChaining, TestEventBuilderBuild, TestEventBuilderRepr, TestEventBuilderTypeSafety, TestEventBuilderEdgeCases, TestEventBuilderIntegrationScenarios
    from .test_builder_sample import SampleEvent

    __all__ = ["TestEventBuilderWithAggregateId", "TestEventBuilderWithEventId", "TestEventBuilderWithTenantId", "TestEventBuilderWithVersion", "TestEventBuilderWithOccurredAt", "TestEventBuilderWithCorrelationId", "TestEventBuilderWithCausationId", "TestEventBuilderWithActorId", "TestEventBuilderWithMetadata", "TestEventBuilderWithField", "TestEventBuilderWithFields", "MinimalEvent", "TestEventBuilderInit", "TestEventBuilderMethodChaining", "TestEventBuilderBuild", "TestEventBuilderRepr", "TestEventBuilderTypeSafety", "TestEventBuilderEdgeCases", "TestEventBuilderIntegrationScenarios", "SampleEvent"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
