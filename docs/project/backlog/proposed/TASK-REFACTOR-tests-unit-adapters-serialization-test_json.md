---
id: REFACTOR-tests-unit-adapters-serialization-test_json
title: Refactor and Decompose Legacy File test_json.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-adapters-serialization-test_json: Refactor Legacy File test_json.py

## Summary
The grandfathered debt file `tests/unit/adapters/serialization/test_json.py` contains 707 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_json_dumps.py, test_json_event.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/adapters/serialization/test_json/` with submodules:
- `test_json_dumps.py`: TestJsonDumps, TestJsonDumpsUnsupportedTypes, TestJsonDumpsPydanticModels, TestJsonLoads, TestJsonEncoderContract, TestNewModuleExports
- `test_json_event.py`: TestEventSourceJSONEncoder, TestDomainEventRejectsNonFiniteFloats

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/unit/adapters/serialization/test_json.py (707 lines):
  Submodule 'test_json_dumps.py' (~506 lines):
    - [class] TestJsonDumps (lines 92-126)
    - [class] TestJsonDumpsUnsupportedTypes (lines 176-186)
    - [class] TestJsonDumpsPydanticModels (lines 189-208)
    - [class] TestJsonLoads (lines 129-173)
    - [class] TestJsonEncoderContract (lines 211-556)
    - [class] TestNewModuleExports (lines 659-707)
  Submodule 'test_json_event.py' (~162 lines):
    - [class] TestEventSourceJSONEncoder (lines 26-89)
    - [class] TestDomainEventRejectsNonFiniteFloats (lines 559-656)
  Suggested barrel exports:
    from .test_json_dumps import TestJsonDumps, TestJsonDumpsUnsupportedTypes, TestJsonDumpsPydanticModels, TestJsonLoads, TestJsonEncoderContract, TestNewModuleExports
    from .test_json_event import TestEventSourceJSONEncoder, TestDomainEventRejectsNonFiniteFloats

    __all__ = ["TestJsonDumps", "TestJsonDumpsUnsupportedTypes", "TestJsonDumpsPydanticModels", "TestJsonLoads", "TestJsonEncoderContract", "TestNewModuleExports", "TestEventSourceJSONEncoder", "TestDomainEventRejectsNonFiniteFloats"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
