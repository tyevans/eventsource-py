---
id: REFACTOR-eventsource-__init__
title: Refactor and Decompose Legacy File __init__.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-eventsource-__init__: Refactor Legacy File __init__.py

## Summary
The grandfathered debt file `src/eventsource/__init__.py` contains 600 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (__init___module.py, __init___getattr.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/__init__/` with submodules:
- `__init___module.py`: _module_installed, __dir__
- `__init___getattr.py`: __getattr__

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/src/eventsource/__init__.py (600 lines):
  Submodule '__init___module.py' (~9 lines):
    - [function] _module_installed (lines 423-429)
    - [function] __dir__ (lines 599-600)
  Submodule '__init___getattr.py' (~12 lines):
    - [function] __getattr__ (lines 585-596)
  Suggested barrel exports:
    from .__init___module import _module_installed, __dir__
    from .__init___getattr import __getattr__

    __all__ = ["_module_installed", "__dir__", "__getattr__"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
