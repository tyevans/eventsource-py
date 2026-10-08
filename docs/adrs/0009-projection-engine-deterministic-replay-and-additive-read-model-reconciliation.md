# ADR 9: Projection Engine, Deterministic Replay, and Additive Read-Model Reconciliation

## Summary
StoreProjection base, replay drivers, version conflict protection, and additive schemas.

## Context
Read models need to evolve over time, handle concurrent updates, and support rebuilding from historical event streams without corrupting live production checkpoints.

## Decision
1. Provide typed `StoreProjection[TStore]` base class mapping domain events into read-model stores.
2. Implement dedicated `ProjectionReplayDriver` that runs isolated rehydration runs into staging tables or memory.
3. Optimistic concurrency on read models raises `ReadModelVersionConflictError` on stale writes.
4. Relational read model schema reconciliation is additive-only: automatically adds new nullable columns but strictly refuses destructive column drops or datatype alterations.

## Consequences
- Deterministic projection rebuilds without downtime or checkpoint tampering.
- Zero data loss during rolling deployments of new projection features.
- Strict consistency protection against concurrent projection updates.
