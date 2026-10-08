---
id: REFACTOR-tests-unit-testing-test_harness
title: Refactor and Decompose Legacy File test_harness.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-testing-test_harness: Refactor Legacy File test_harness.py

## Summary
The grandfathered debt file `tests/unit/testing/test_harness.py` contains 604 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_harness_events.py, test_harness_event.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/testing/test_harness/` with submodules:
- `test_harness_events.py`: TestHarnessPublishedEvents, TestHarnessClearPublishedEvents, TestHarnessGetEventsOfType, TestHarnessInstantiation, TestHarnessProperties, TestHarnessReset, TestHarnessTracingDisabled, TestHarnessRepr, TestHarnessIntegration
- `test_harness_event.py`: HarnessSampleEvent, HarnessOtherEvent

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/testing/test_harness.py (604 lines):
  Submodule 'test_harness_events.py' (~549 lines):
    - [class] TestHarnessPublishedEvents (lines 99-180)
    - [class] TestHarnessClearPublishedEvents (lines 183-266)
    - [class] TestHarnessGetEventsOfType (lines 380-456)
    - [class] TestHarnessInstantiation (lines 40-55)
    - [class] TestHarnessProperties (lines 58-96)
    - [class] TestHarnessReset (lines 269-358)
    - [class] TestHarnessTracingDisabled (lines 361-377)
    - [class] TestHarnessRepr (lines 459-484)
    - [class] TestHarnessIntegration (lines 487-604)
  Submodule 'test_harness_event.py' (~8 lines):
    - [class] HarnessSampleEvent (lines 27-30)
    - [class] HarnessOtherEvent (lines 34-37)
  Suggested barrel exports:
    from .test_harness_events import TestHarnessPublishedEvents, TestHarnessClearPublishedEvents, TestHarnessGetEventsOfType, TestHarnessInstantiation, TestHarnessProperties, TestHarnessReset, TestHarnessTracingDisabled, TestHarnessRepr, TestHarnessIntegration
    from .test_harness_event import HarnessSampleEvent, HarnessOtherEvent

    __all__ = ["TestHarnessPublishedEvents", "TestHarnessClearPublishedEvents", "TestHarnessGetEventsOfType", "TestHarnessInstantiation", "TestHarnessProperties", "TestHarnessReset", "TestHarnessTracingDisabled", "TestHarnessRepr", "TestHarnessIntegration", "HarnessSampleEvent", "HarnessOtherEvent"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
