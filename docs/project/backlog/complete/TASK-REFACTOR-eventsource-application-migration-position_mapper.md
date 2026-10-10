---
id: REFACTOR-eventsource-application-migration-position_mapper
title: Refactor and Decompose Legacy File position_mapper.py
status: Complete
governing_adrs:
- ADR-0002
governing_stories:
- US-0012
target_bc: core
---

# TASK-REFACTOR-eventsource-application-migration-position_mapper: Refactor Legacy File position_mapper.py

## Summary
The grandfathered debt file `src/eventsource/application/migration/position_mapper.py` contains 603 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (position_mapper_translation.py, position_mapper_core.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/application/migration/position_mapper/` with submodules:
- `position_mapper_translation.py`: TranslationResult, ReverseTranslationResult
- `position_mapper_core.py`: PositionMapper

## AST Decomposition Blueprint
Decomposition Blueprint for src/eventsource/application/migration/position_mapper.py (603 lines):
  Submodule 'position_mapper_translation.py' (~38 lines):
    - [class] TranslationResult (lines 69-88)
    - [class] ReverseTranslationResult (lines 92-109)
  Submodule 'position_mapper_core.py' (~492 lines):
    - [class] PositionMapper (lines 112-603)
  Suggested barrel exports:
    from .position_mapper_translation import TranslationResult, ReverseTranslationResult
    from .position_mapper_core import PositionMapper

    __all__ = ["TranslationResult", "ReverseTranslationResult", "PositionMapper"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
