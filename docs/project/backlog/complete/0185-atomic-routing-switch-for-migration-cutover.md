---
id: "0185"
title: Remove Migration Cutover & Exception Legacy Shims and Decompose Audit Log Modules
status: Complete
target_bc: application
governing_adrs:
  - ADR-0002
  - ADR-0007
  - ADR-0111
governing_prds:
  - PRD-0001
governing_stories:
  - US-0006
  - US-0020
priority: P1
---

# TASK-0185: Remove Migration Cutover & Exception Legacy Shims and Decompose Audit Log Modules

## Summary
Remove the non-atomic fallback shim in `CutoverExecutionMixin` so `TenantRoutingRepository.switch_routing` is called unconditionally, remove the legacy `MigrationError.recoverable` attribute shim, migrate `SQLMigrationAuditLogRepository` to `orjson`, and decompose oversized migration audit log and schema test modules below 400 lines.

## Problem Statement & Context
- `CutoverExecutionMixin._execute_cutover_locked` (`src/eventsource/application/migration/cutover_execution.py` lines 278-291) still contains a legacy `if hasattr(self._routing_repo, "switch_routing"): ... else: ...` fallback to non-atomic two-step writes, even though `switch_routing` is part of the `TenantRoutingRepository` port protocol.
- `MigrationError` (`src/eventsource/application/migration/exceptions_base.py`) still carries a legacy `recoverable` boolean attribute alongside `is_recoverable`.
- `src/eventsource/adapters/sql/migration/audit_log.py` (414 lines), `tests/unit/application/migration/test_migration_audit_log.py` (495 lines), and `tests/unit/adapters/sql/schemas/test_migration_schema.py` (445 lines) exceed the 400-line warning threshold, and `audit_log.py` uses stdlib `json` instead of `orjson`.

## Required Actions
1. Remove the `hasattr(self._routing_repo, "switch_routing")` branch in `src/eventsource/application/migration/cutover_execution.py` and invoke `await self._routing_repo.switch_routing(...)` directly.
2. Remove the legacy `recoverable` attribute from `MigrationError` in `src/eventsource/application/migration/exceptions_base.py`, standardizing on `is_recoverable` across `tests/unit/application/migration/test_exceptions/`.
3. Migrate `src/eventsource/adapters/sql/migration/audit_log.py` to `json_dumps`/`json_loads` (`orjson`) and decompose `audit_log.py` (414 lines), `tests/unit/application/migration/test_migration_audit_log.py` (495 lines), and `tests/unit/adapters/sql/schemas/test_migration_schema.py` (445 lines) so all files are strictly under 400 lines.

## Definition of Done (Blackbox Frontdoor TDD)
1. Cutover unconditionally executes atomic `switch_routing` on `TenantRoutingRepository`.
2. Legacy `MigrationError.recoverable` shim is removed in favor of `is_recoverable`.
3. `audit_log.py`, `test_migration_audit_log`, and `test_migration_schema` are all strictly under 400 lines per file.
