# ADR 11: Zero-Downtime 5-State Live Store Migration and Distributed Locking

## Summary
5-state live migration coordinator, bounded pause rollback, and advisory locks.

## Context
Migrating terabyte-scale event stores between databases or cloud regions cannot tolerate hours of maintenance downtime or inconsistent split-brain writes.

## Decision
1. Coordinate zero-downtime migrations via a formal 5-state machine: `PREPARE` -> `DUAL_WRITE` -> `BULK_COPY` -> `CUTOVER` -> `ROUTING`.
2. Source-first dual writing: writes are committed to the primary source store first, with best-effort asynchronous replication to the target.
3. Strict bounded write pause during cutover (500ms default); failure or timeout automatically rolls back to `DUAL_WRITE`.
4. Zero-lag cutover guarantee (`cutover_max_lag_events=0`) with on-demand resync passes.
5. Distributed locking powered by PostgreSQL session-level advisory locks (`PostgreSQLLockManager`).

## Consequences
- Seamless, zero-downtime database upgrades and cloud cross-region migrations.
- Absolute write safety with guaranteed zero-data-loss rollback on cutover failures.
- Active-passive coordination without external cluster orchestration engines.
