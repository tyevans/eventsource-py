---
id: REFACTOR-eventsource-application-migration-consistency
title: Refactor and Decompose Legacy File consistency.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-eventsource-application-migration-consistency: Refactor Legacy File consistency.py

## Summary
The grandfathered debt file `src/eventsource/application/migration/consistency.py` contains 859 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (consistency_verification.py, consistency_stream.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/application/migration/consistency/` with submodules:
- `consistency_verification.py`: VerificationLevel, VerificationReport, ConsistencyViolation, ConsistencyVerifier
- `consistency_stream.py`: StreamConsistency

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/src/eventsource/application/migration/consistency.py (859 lines):
  Submodule 'consistency_verification.py' (~739 lines):
    - [class] VerificationLevel (lines 64-91)
    - [class] VerificationReport (lines 173-237)
    - [class] ConsistencyViolation (lines 135-169)
    - [class] ConsistencyVerifier (lines 240-850)
  Submodule 'consistency_stream.py' (~37 lines):
    - [class] StreamConsistency (lines 95-131)
  Suggested barrel exports:
    from .consistency_verification import VerificationLevel, VerificationReport, ConsistencyViolation, ConsistencyVerifier
    from .consistency_stream import StreamConsistency

    __all__ = ["VerificationLevel", "VerificationReport", "ConsistencyViolation", "ConsistencyVerifier", "StreamConsistency"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
