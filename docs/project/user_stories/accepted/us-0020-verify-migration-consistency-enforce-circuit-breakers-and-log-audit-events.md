---
id: '0020'
title: Verify Migration Consistency, Enforce Circuit Breakers, and Log Immutable Audit Events
status: Accepted
created: 2026-10-09
persona: Chris (The SRE / Resilience & Cutover Operator)
target_bc: migration
feature: FEAT-LIVE-MIGRATION-RESILIENCE
governing_prd: PRD-0001
scenarios:
- Consistency verifier validates event counts and cryptographic payload hashes
- DualWriteInterceptor circuit breaker isolates target store outages
- Migration operations record immutable audit trail entries
governing_adrs:
- ADR-0001
- ADR-0003
- ADR-0007
- ADR-0111
---

# US-0020: Verify Migration Consistency, Enforce Circuit Breakers, and Log Immutable Audit Events

## Governing PRD
- [`PRD-0001: Production-Ready Event Sourcing and Live Migration Framework`](../../product/accepted/prd-0001-production-ready-event-sourcing-framework.md)

## User Story

**As an** SRE and resilience operator (Chris),
**I want** live migrations to support multi-tier consistency verification, target store circuit breaker trip protection, and immutable audit logging,
**So that** event stores can be migrated with cryptographic verification before cutover, target store downtime never impairs primary authoritative writes, and a tamper-evident compliance audit trail is preserved.

## Acceptance Criteria

```gherkin
Scenario: Consistency verifier validates event counts and cryptographic payload hashes
  Given a completed bulk copy and dual-write between source and target event stores
  When ConsistencyVerifier executes verification at level "DEEP"
  Then event count parity, sequence positions, and SHA-256 payload checksums are verified
  And a VerificationReport confirms zero discrepancies before cutover proceeds.
```

```gherkin
Scenario: DualWriteInterceptor circuit breaker isolates target store outages
  Given an active migration in DUAL_WRITE phase with the target store becoming unreachable
  When the circuit breaker trips from CLOSED to OPEN after consecutive mirror failures
  Then new domain events commit to the primary authoritative store without delay
  And failed target writes are queued and counted in failure stats without aborting callers.
```

```gherkin
Scenario: Migration operations record immutable audit trail entries
  Given migration operations progressing through phases, cutover, and verification
  When phase changes, cutovers, or rollbacks occur
  Then immutable MigrationAuditEntry records are written to MigrationAuditLogRepository
  And audit logs queryable by migration ID provide an auditable compliance trail.
```

## Implementation Status & Verification

- **Status**: Implemented & Verified (100% test pass rate, 0 backdoor mocks)
- **Implementation Modules**:
  - `src/eventsource/application/migration/consistency.py`: `ConsistencyVerifier`, `VerificationLevel` (`FAST`, `STANDARD`, `DEEP`).
  - `src/eventsource/application/migration/circuit_breaker.py`: `CircuitBreaker`, circuit state transitions.
  - `src/eventsource/adapters/sql/migration/audit_log.py`: `SqlMigrationAuditLogRepository`.
- **Verified Test Suites**:
  - `tests/unit/application/migration/test_consistency_verifier.py`: Verification levels and checksum comparisons.
  - `tests/unit/application/migration/test_circuit_breaker.py`: Circuit tripping and half-open probes.
  - `tests/unit/adapters/sql/migration/test_audit_log_repository.py`: Audit entry append and retrieval.
