---
id: REFACTOR-tests-unit-readmodels-test_schema
title: Refactor and Decompose Legacy File test_schema.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-readmodels-test_schema: Refactor Legacy File test_schema.py

## Summary
The grandfathered debt file `tests/unit/readmodels/test_schema.py` contains 631 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_schema_model.py, test_schema_type.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/readmodels/test_schema/` with submodules:
- `test_schema_model.py`: SimpleModel, ComplexModel, AllTypesModel, CustomTableModel, IndexedModel, ModelWithDefaults, ModelWithCustomSqlType, TestGenerateSchema, TestGenerateIndexes, TestGenerateFullSchema, TestIsOptional, TestFormatDefault, TestIntegration
- `test_schema_type.py`: TestTypeMaps, TestExtractType, TestGetCustomSqlType

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/unit/readmodels/test_schema.py (631 lines):
  Submodule 'test_schema_model.py' (~472 lines):
    - [class] SimpleModel (lines 25-29)
    - [class] ComplexModel (lines 32-40)
    - [class] AllTypesModel (lines 43-56)
    - [class] CustomTableModel (lines 59-63)
    - [class] IndexedModel (lines 66-76)
    - [class] ModelWithDefaults (lines 79-88)
    - [class] ModelWithCustomSqlType (lines 91-97)
    - [class] TestGenerateSchema (lines 160-321)
    - [class] TestGenerateIndexes (lines 324-409)
    - [class] TestGenerateFullSchema (lines 412-455)
    - [class] TestIsOptional (lines 488-500)
    - [class] TestFormatDefault (lines 503-546)
    - [class] TestIntegration (lines 570-631)
  Submodule 'test_schema_type.py' (~105 lines):
    - [class] TestTypeMaps (lines 100-157)
    - [class] TestExtractType (lines 458-485)
    - [class] TestGetCustomSqlType (lines 549-567)
  Suggested barrel exports:
    from .test_schema_model import SimpleModel, ComplexModel, AllTypesModel, CustomTableModel, IndexedModel, ModelWithDefaults, ModelWithCustomSqlType, TestGenerateSchema, TestGenerateIndexes, TestGenerateFullSchema, TestIsOptional, TestFormatDefault, TestIntegration
    from .test_schema_type import TestTypeMaps, TestExtractType, TestGetCustomSqlType

    __all__ = ["SimpleModel", "ComplexModel", "AllTypesModel", "CustomTableModel", "IndexedModel", "ModelWithDefaults", "ModelWithCustomSqlType", "TestGenerateSchema", "TestGenerateIndexes", "TestGenerateFullSchema", "TestIsOptional", "TestFormatDefault", "TestIntegration", "TestTypeMaps", "TestExtractType", "TestGetCustomSqlType"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
