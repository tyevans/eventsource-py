# Historical Engineering Backlog Archive

This archive preserves the historical record of closed work items, technical debt sweeps, and legacy audit resolutions originally recorded in `BACKLOG.md`.

---

## Closed in Sweep (2026-08-04)

| Item | Resolution | Verification |
|---|---|---|
| Read model aliasing in memory repository | Updated repository so read operations return copies and write operations store copies. | Verified in `ReadModelRepositoryConformance` across all backends (ADR-0050). |
| Duplicate `OptimisticLockError` naming | Renamed read-model error to `ReadModelVersionConflictError` without compatibility shims. | Verified in exception hierarchy and test suite (ADR-0050). |
| Duplicate `ExpectedVersion` dispatch | Consolidated four separate adapter implementations into `adapters/_common/`. | Verified in `src/eventsource/adapters/_common/` (ADR-0051). |
| Core surface purity verification | Added runtime check and expanded static contract driver list to six drivers. | `test_core_surface_purity.py` and `docs/core-surface.md`. |
| Store identifier uniqueness documentation | Documented uniqueness requirements on `Position` and adapter defaults. Preserved defaults to protect checkpoints. | Verified in `Position` docstring and adapter documentation. |
| Ambiguous logging in `run_resync_pass` | Updated log messages to differentiate empty batch outcomes from full completion. | Verified in migration coordinator logging. |
| Stale Mutmut configuration selectors | Retargeted selectors away from deleted `repositories/` package. | Verified by `test_mutmut_configure.py`. |

---

## Closed Prior to Sweep (2026-08-04)

| Item | Resolution | Verification |
|---|---|---|
| Concurrent SQLite read isolation | Isolated read queries under the append lock to prevent dirty reads. | Verified in `tests/unit/adapters/test_sqlite_read_isolation.py`. |
| Dead letter queue cutoff semantics | Standardized both storage backends on rolling instant timestamps in `delete_resolved_events`. | Specified and verified in `ports/dlq.py`. |
| Catch-up premature termination | Added `envelopes_read` counter alongside `events_delivered` in `_BatchOutcome`. | Verified in catch-up batch processing tests. |
| Position mapper in copier coordinator | Replaced duplicate construction call sites with unified `_build_copier` factory. | Verified in migration coordinator. |
| Missing resync for dual-write mirror failures | Added `run_resync_pass` to reconcile primary and mirror stores. | Verified in dual-write replication tests. |
| Cutover lag tolerance threshold | Set default `cutover_max_lag_events` to 0 (strict zero lag). | Verified in cutover configuration and tests. |
| Relocate subscriptions package | Moved `subscriptions/` package from root namespace to application ring. | Completed under ADR-0032. |
| Remove bus facade compatibility shims | Migrated message bus components to ports and adapters. Removed legacy shims. | Completed under ADR-0031. |
| Remove locks and read models shims | Removed deprecated shim modules for locks and read models. | Completed under ADR-0030. |
| Ring placement for `engine.py` | Assigned `engine.py` to its permanent architectural ring. | Completed under ADR-0029. |
| Audit findings critical defects | Resolved three critical audit defects from 2026-08-02 review across adapters, application, and DX. | Completed in PR #112, PR #113, PR #114, and ADR-0048. |
| PostgreSQL catch-up horizon benchmark at scale | Verified index scans on `events_pkey` up to 1M events under concurrent appends (>40k events/sec throughput). | TASK-0004, `docs/reference/postgresql-catchup-benchmark.md`, `tests/benchmarks/test_postgresql_catchup.py`. |
| Delta compression for stored event payloads | Implemented `DeltaCodec` and `DeltaPayload` with RFC 1950 compression, CRC32 checksums, and SHA-256 verification (>35x compression). | TASK-0002, `docs/reference/delta-compression.md`, `tests/benchmarks/test_delta_compression.py`. |
