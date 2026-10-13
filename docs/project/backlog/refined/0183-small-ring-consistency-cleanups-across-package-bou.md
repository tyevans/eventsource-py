---
id: "0183"
title: Consolidate Conformance Suites, Remove Legacy Checkpoint & Scenario Shims, and Decompose ReadModel Ports
status: Refined
target_bc: core
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
  - ADR-0105
governing_prds:
  - PRD-0001
  - PRD-0004
governing_stories:
  - US-0012
  - US-0016
priority: P1
---

# TASK-0183: Consolidate Conformance Suites, Remove Legacy Checkpoint & Scenario Shims, and Decompose ReadModel Ports

## Summary
Consolidate `testing/conformance.py` and `testing/conformance_ports/`, remove legacy bare-integer checkpoint shims and `ScenarioResult._state` private accessor shim, and decompose `ports/readmodels/repository.py` and `tests/unit/ports/test_handlers.py` below 400 lines.

## Problem Statement & Context
- `src/eventsource/testing/conformance.py` sits alongside `src/eventsource/testing/conformance_ports/`; consolidating `EventBusConformanceSuite` into `conformance_ports/bus.py` (with clean re-exports) unifies all port conformance suites in one package.
- `src/eventsource/adapters/_sql/positions.py` and `src/eventsource/testing/conformance_ports/checkpoints.py` retain legacy bare-int checkpoint shims (`if raw.isdigit()` and `write_legacy_int_row` / `test_legacy_int_only_row_reads_none`).
- `src/eventsource/testing/scenario.py` retains a deprecated `_state` backward-compatible property.
- `src/eventsource/ports/readmodels/repository.py` (445 lines) and `tests/unit/ports/test_handlers.py` (479 lines) exceed the 400-line proactive warning threshold.

## Required Actions
1. Move `EventBusConformanceSuite` into `src/eventsource/testing/conformance_ports/bus.py`, re-export it from `conformance_ports/__init__.py` and `testing/conformance.py` (or `testing/__init__.py`), keeping all files <400 lines.
2. Remove the legacy bare-integer fallback (`if raw.isdigit()`) in `src/eventsource/adapters/_sql/positions.py` and remove `write_legacy_int_row` / `test_legacy_int_only_row_reads_none` in `src/eventsource/testing/conformance_ports/checkpoints.py` and `tests/unit/adapters/test_checkpoint_position/`.
3. Remove `ScenarioResult._state` backward-compatible accessor from `src/eventsource/testing/scenario.py`.
4. Decompose `src/eventsource/ports/readmodels/repository.py` and `tests/unit/ports/test_handlers.py` below 400 lines and ensure port-purity tests validate relative imports cleanly.

## Definition of Done (Blackbox Frontdoor TDD)
1. Conformance suites are unified under `eventsource.testing.conformance_ports` with clean public exports.
2. Legacy bare-int checkpoint shims and `ScenarioResult._state` are removed.
3. `ports/readmodels/repository.py` and `tests/unit/ports/test_handlers.py` are strictly under 400 lines.
4. All import-linter contracts and unit tests pass cleanly.
