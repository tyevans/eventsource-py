---
id: "0183"
title: Small Ring-Consistency Cleanups across Package Boundaries
status: Proposed
target_bc: core
governing_adrs:
  - ADR-0002
  - ADR-0102
priority: P3
---

# TASK-0183: Small Ring-Consistency Cleanups across Package Boundaries

## Summary
Resolve minor structural inconsistencies across package boundaries and test assertions identified following hexagonal ring architecture transitions.

## Problem Statement & Context
Architectural transitions left minor structural inconsistencies across package boundaries and test assertions:
- Hexagonal architecture rules govern ring boundaries (ADR-0002 and ADR-0102).
- Impacted packages include `testing/conformance_ports/`, `observability/`, `protocols.py`, and `ports/readmodels/`.
- Discovered during structure slice A and engine relocation audits.

## Required Actions
1. Evaluate consolidating `testing/conformance_ports/` into `testing/conformance.py`, or formally document the namespace boundary.
2. Relocate the tracing decorator to a ports helper module so adapters do not import `observability/` directly, upholding ADR-0102 isolation invariants.
3. Move remaining abstract base classes and protocols from root `protocols.py` into their respective ring packages (`ports/`, `domain/`, etc.).
4. Provide temporary deprecation re-exports for moved protocols.
5. Update the AST-based port-purity test for `ports/readmodels/` to validate relative imports cleanly.

## Definition of Done (Blackbox Frontdoor TDD)
1. `domain` and `ports` have zero direct imports of `observability` or `testing`.
2. `protocols.py` cleanly redirects or distributes definitions without breaking backward compatibility.
3. All ring isolation linters (`import-linter`) and unit tests pass cleanly.
4. All source files strictly comply with ADR-0002 (<500 lines limit).
