---
id: REFACTOR-tests-unit-testing-test_assertions
title: Refactor and Decompose Legacy File test_assertions.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-testing-test_assertions: Refactor Legacy File test_assertions.py

## Summary
The grandfathered debt file `tests/unit/testing/test_assertions.py` contains 625 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_assertions_event.py, test_assertions_sample.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/testing/test_assertions/` with submodules:
- `test_assertions_event.py`: OtherEvent, single_event_assertions, multi_event_assertions, TestEventAssertionsInit, TestAssertEventPublished, TestAssertNoEventPublished, TestAssertEventCount, TestAssertEventSequence, TestAssertEventWithFields, TestAssertEventForAggregate, TestEventAssertionsIntegration, empty_assertions, TestAssertNoEventsPublished, TestGetEventsOfType
- `test_assertions_sample.py`: SampleCreated, SampleUpdated, SampleDeleted

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/unit/testing/test_assertions.py (625 lines):
  Submodule 'test_assertions_event.py' (~501 lines):
    - [class] OtherEvent (lines 41-45)
    - [function] single_event_assertions (lines 60-68)
    - [function] multi_event_assertions (lines 72-94)
    - [class] TestEventAssertionsInit (lines 102-151)
    - [class] TestAssertEventPublished (lines 159-199)
    - [class] TestAssertNoEventPublished (lines 207-228)
    - [class] TestAssertEventCount (lines 236-265)
    - [class] TestAssertEventSequence (lines 273-334)
    - [class] TestAssertEventWithFields (lines 342-427)
    - [class] TestAssertEventForAggregate (lines 457-524)
    - [class] TestEventAssertionsIntegration (lines 582-625)
    - [function] empty_assertions (lines 54-56)
    - [class] TestAssertNoEventsPublished (lines 435-449)
    - [class] TestGetEventsOfType (lines 532-574)
  Submodule 'test_assertions_sample.py' (~14 lines):
    - [class] SampleCreated (lines 21-25)
    - [class] SampleUpdated (lines 28-32)
    - [class] SampleDeleted (lines 35-38)
  Suggested barrel exports:
    from .test_assertions_event import OtherEvent, single_event_assertions, multi_event_assertions, TestEventAssertionsInit, TestAssertEventPublished, TestAssertNoEventPublished, TestAssertEventCount, TestAssertEventSequence, TestAssertEventWithFields, TestAssertEventForAggregate, TestEventAssertionsIntegration, empty_assertions, TestAssertNoEventsPublished, TestGetEventsOfType
    from .test_assertions_sample import SampleCreated, SampleUpdated, SampleDeleted

    __all__ = ["OtherEvent", "single_event_assertions", "multi_event_assertions", "TestEventAssertionsInit", "TestAssertEventPublished", "TestAssertNoEventPublished", "TestAssertEventCount", "TestAssertEventSequence", "TestAssertEventWithFields", "TestAssertEventForAggregate", "TestEventAssertionsIntegration", "empty_assertions", "TestAssertNoEventsPublished", "TestGetEventsOfType", "SampleCreated", "SampleUpdated", "SampleDeleted"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
