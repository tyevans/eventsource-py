---
id: REFACTOR-tests-unit-application-migration-test_consistency_verifier
title: Refactor and Decompose Legacy File test_consistency_verifier.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-unit-application-migration-test_consistency_verifier: Refactor Legacy File test_consistency_verifier.py

## Summary
The grandfathered debt file `tests/unit/application/migration/test_consistency_verifier.py` contains 1085 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (test_consistency_verifier_verification.py, test_consistency_verifier_event.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/unit/application/migration/test_consistency_verifier/` with submodules:
- `test_consistency_verifier_verification.py`: TestVerificationLevel, TestVerificationReport, TestConsistencyVerifierHashVerification, TestConsistencyVerifierFullVerification, TestStreamConsistency, TestConsistencyViolation, TestConsistencyVerifierInit, TestConsistencyVerifierVerifyTenant, TestConsistencyVerifierSampling, TestConsistencyVerifierChecksums, TestConsistencyVerifierAggregateVersions, TestConsistencyVerifierHelperMethods, TestConsistencyVerifierErrorHandling
- `test_consistency_verifier_event.py`: TestEvent

## AST Decomposition Blueprint
Decomposition Blueprint for /home/ty/workspace/eventsource-py/tests/unit/application/migration/test_consistency_verifier.py (1085 lines):
  Submodule 'test_consistency_verifier_verification.py' (~1017 lines):
    - [class] TestVerificationLevel (lines 45-61)
    - [class] TestVerificationReport (lines 160-251)
    - [class] TestConsistencyVerifierHashVerification (lines 534-607)
    - [class] TestConsistencyVerifierFullVerification (lines 610-682)
    - [class] TestStreamConsistency (lines 64-119)
    - [class] TestConsistencyViolation (lines 122-157)
    - [class] TestConsistencyVerifierInit (lines 254-281)
    - [class] TestConsistencyVerifierVerifyTenant (lines 284-531)
    - [class] TestConsistencyVerifierSampling (lines 685-760)
    - [class] TestConsistencyVerifierChecksums (lines 763-818)
    - [class] TestConsistencyVerifierAggregateVersions (lines 821-925)
    - [class] TestConsistencyVerifierHelperMethods (lines 928-1049)
    - [class] TestConsistencyVerifierErrorHandling (lines 1052-1085)
  Submodule 'test_consistency_verifier_event.py' (~5 lines):
    - [class] TestEvent (lines 38-42)
  Suggested barrel exports:
    from .test_consistency_verifier_verification import TestVerificationLevel, TestVerificationReport, TestConsistencyVerifierHashVerification, TestConsistencyVerifierFullVerification, TestStreamConsistency, TestConsistencyViolation, TestConsistencyVerifierInit, TestConsistencyVerifierVerifyTenant, TestConsistencyVerifierSampling, TestConsistencyVerifierChecksums, TestConsistencyVerifierAggregateVersions, TestConsistencyVerifierHelperMethods, TestConsistencyVerifierErrorHandling
    from .test_consistency_verifier_event import TestEvent

    __all__ = ["TestVerificationLevel", "TestVerificationReport", "TestConsistencyVerifierHashVerification", "TestConsistencyVerifierFullVerification", "TestStreamConsistency", "TestConsistencyViolation", "TestConsistencyVerifierInit", "TestConsistencyVerifierVerifyTenant", "TestConsistencyVerifierSampling", "TestConsistencyVerifierChecksums", "TestConsistencyVerifierAggregateVersions", "TestConsistencyVerifierHelperMethods", "TestConsistencyVerifierErrorHandling", "TestEvent"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
