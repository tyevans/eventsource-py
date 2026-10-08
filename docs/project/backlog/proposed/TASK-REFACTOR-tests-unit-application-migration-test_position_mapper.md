---
id: REFACTOR-tests-unit-application-migration-test_position_mapper
title: Refactor and Decompose Legacy File test_position_mapper.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-migration-test_position_mapper: Refactor Legacy File test_position_mapper.py

## Summary
The grandfathered debt file `tests/unit/application/migration/test_position_mapper.py` contains 925 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_position_mapper_translate.py, test_position_mapper_record.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/migration/test_position_mapper/` with submodules:
- `test_position_mapper_translate.py`: TestPositionMapperTranslatePosition, TestPositionMapperTranslatePositionReverse, TestPositionMapperTranslatePositionsBatch, pos, TestPositionMapperInit, TestPositionMapperHelperMethods, TestPositionMapperFindNearest, TestTranslationResultDataclass, TestReverseTranslationResultDataclass, TestPositionMapperWorkflows
- `test_position_mapper_record.py`: TestPositionMapperRecordMapping, TestPositionMapperRecordMappingsBatch

## AST Decomposition Blueprint
Decomposition Blueprint for tests/unit/application/migration/test_position_mapper.py (925 lines):
  Submodule 'test_position_mapper_translate.py' (~713 lines):
    - [class] TestPositionMapperTranslatePosition (lines 222-333)
    - [class] TestPositionMapperTranslatePositionReverse (lines 336-392)
    - [class] TestPositionMapperTranslatePositionsBatch (lines 395-521)
    - [function] pos (lines 32-34)
    - [class] TestPositionMapperInit (lines 37-56)
    - [class] TestPositionMapperHelperMethods (lines 524-620)
    - [class] TestPositionMapperFindNearest (lines 623-753)
    - [class] TestTranslationResultDataclass (lines 756-796)
    - [class] TestReverseTranslationResultDataclass (lines 799-824)
    - [class] TestPositionMapperWorkflows (lines 827-925)
  Submodule 'test_position_mapper_record.py' (~159 lines):
    - [class] TestPositionMapperRecordMapping (lines 59-144)
    - [class] TestPositionMapperRecordMappingsBatch (lines 147-219)
  Suggested barrel exports:
    from .test_position_mapper_translate import TestPositionMapperTranslatePosition, TestPositionMapperTranslatePositionReverse, TestPositionMapperTranslatePositionsBatch, pos, TestPositionMapperInit, TestPositionMapperHelperMethods, TestPositionMapperFindNearest, TestTranslationResultDataclass, TestReverseTranslationResultDataclass, TestPositionMapperWorkflows
    from .test_position_mapper_record import TestPositionMapperRecordMapping, TestPositionMapperRecordMappingsBatch

    __all__ = ["TestPositionMapperTranslatePosition", "TestPositionMapperTranslatePositionReverse", "TestPositionMapperTranslatePositionsBatch", "pos", "TestPositionMapperInit", "TestPositionMapperHelperMethods", "TestPositionMapperFindNearest", "TestTranslationResultDataclass", "TestReverseTranslationResultDataclass", "TestPositionMapperWorkflows", "TestPositionMapperRecordMapping", "TestPositionMapperRecordMappingsBatch"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
