# ADR-0025: Legacy Store Surface Retirement

**Status:** Accepted

**Date:** 2026-07-31

**Deciders:** Library maintainers (architecture owner)

## Context

The `EventStore` ABC in `src/eventsource/stores/interface.py` is the legacy store surface. As of slice (b), a new port-based surface (`EventAppender`, `StreamReader`, `EventLookup`, `GlobalEventFeed`, `CategoryQuery`) has been introduced to replace it. This ADR documents the multi-slice effort to migrate off the legacy surface and retire it from the public API entirely.

The full design is recorded in `docs/superpowers/specs/2026-07-31-legacy-store-retirement-design.md`. The decision and consequences are filled in by slice (d).

## Decision

The library is unreleased, so the standing rule applies without qualification: retirement introduces no deprecation shims and no back-compat aliases. `stores/` is deleted entirely and `eventsource` ends with exactly one blessed set of store names.

1. **The `EventStore` ABC surface is retired outright** — no shim module, no re-exported alias, no `DeprecationWarning` bridge. Every consumer is retyped onto the five segregated ports (`EventAppender`, `StreamReader`, `EventLookup`, `GlobalEventFeed`, `CategoryQuery`) landed by ADR 0019, and `src/eventsource/stores/` no longer exists.
2. **`expected_version` int-to-VO translation is by name, never by numeric coincidence.** `ANY` (-1) maps to `.any_()`, `NO_STREAM` (0) maps to `.no_stream()`, `STREAM_EXISTS` (-2) maps to `.stream_exists()`. The sentinel integers happened to be negative and zero; nothing about the mapping may ever rely on that arithmetic, only on the named constant being matched.
3. **Cross-type `get_events(aggregate_type=None)` is dropped, not ported.** No production caller exercises it — every real call site already carries a concrete `aggregate_type` — and the one test that omitted it asserted callability against a mock, not cross-type semantics. Rejected alternative, named and not built: a narrow `StreamDiscovery.find_streams(aggregate_id) -> list[StreamId]` port. If a genuine cross-type need appears later, that is the honest shape for it.
4. **`AppendResult.position` is the position of the first appended event.** Legacy `global_position` was the position of the *last* appended event. All three adapters return the first-event position; this was already true of `LegacyStoreAdapter` without being documented, and is now the recorded contract.
5. **Duplicate-`event_id` appends raise `DuplicateEventError`.** The legacy in-memory and PostgreSQL stores silently skipped a duplicate append; every ports adapter raises instead, giving migration tooling a race-free idempotency primitive rather than a silent no-op.
6. **Category reads filter and order on storage time, inclusive, with position as tie-break.** Legacy `get_events_by_type` filtered and ordered on the event's own `occurred_at`, exclusive (`>`). `CategoryQuery.read_category` filters and orders on `EventEnvelope.stored_at`, inclusive (`>=`), with position as the deterministic tie-break. Naive datetimes are rejected with `ValueError` rather than silently compared against timezone-aware ones.
7. **`TypeConverter` is removed, not moved.** Field-name guessing inside untyped `dict[str, Any]` fields is replaced by typed pydantic sub-models; a consumer that needs structure declares it.
8. **Store-level tracing spans are removed** (amends ADR 0016). The legacy stores carried per-operation spans (`inmemory_event_store.*`, `postgresql_event_store.*`, `sqlite_event_store.*`); the ports adapters carry none. A ports-level tracing decorator is backlogged, not promised — spans survive at the repository, projection, subscription, and migration layers, which is where this loss is absorbed.
9. **`SubscriptionPositions` is retyped from `position: int` to the opaque `Position` value object** (amends ADR 0024). The integer was the legacy store's global position leaking through a port that has no business knowing about store internals.
10. **Position-delta lag becomes count-behind lag**, completing the amendment ADR 0019 already made to ADR 0014. Subtracting opaque positions was never well-defined; lag is now expressed in count-behind or wall-clock terms wherever it is surfaced.
11. **`OptimisticLockError` keeps its int-typed `expected_version` field, deliberately.** The adapters already preserve the legacy sentinel ints (-1/0/-2) for message fidelity via private adapter-internal constants. Retyping a widely-caught exception to carry the VO is churn with no consumer demand, and the sentinel constants are an adapter-internal message-formatting detail, not part of the port contract.
12. **`MemoryEventStore` is renamed `InMemoryEventStore`**, for sibling-naming consistency with the other adapter classes.
13. **Outbox write support is ported onto the PostgreSQL adapter.** `outbox_enabled` moves to the adapter's constructor so the same-transaction outbox guarantee survives the deletion of its only previous writer (the legacy `PostgreSQLEventStore`).

Two further decisions were surfaced by slice (c)'s self-review and are not enumerated in the design spec's §9 list; they are recorded here as additions to that enumeration:

14. **Nearest-position lookup became a binary search.** `find_nearest_source_position` was previously `ORDER BY source_position DESC LIMIT 1` over a BIGINT column. Opaque `Position` tokens cannot be ordered in SQL — `Position.to_str()` is JSON, and its lexicographic order is not position order — so the lookup became a binary search over the surrogate `id` column, with the ordering comparison performed in Python and resting on a documented monotonicity precondition. Rejected alternative: load all mappings and scan linearly, which is correct but unbounded in memory.
15. **The legacy BIGINT position columns are frozen, not dropped.** `projection_checkpoints.global_position`, `migration_position_mappings.source_position`/`.target_position`, and `tenant_migrations.last_source_position`/`.last_target_position` are neither written nor read by the library after slice (c). They remain in the schema: dropping a column is a destructive operation, `schemas/checkpoints.sql` is under the Do Not Modify rule, and the additive-fragment migration mechanism exists to *add* columns, not remove them. They will die with their own schema revision, not this one.

### ADR Impact

Per `.claude/rules/definition-of-done.md`, this record's own impact statement over the ADRs it touches:

| ADR | Disposition |
|-----|-------------|
| 0001 async-first design | Stands. Every retyped signature stays async; the sync wrapper remains a wrapper. |
| 0014 live-migration cutover semantics | Stands, as amended by 0019. 0019 already abolished position-delta lag; this retirement performs that abolition in the migration sync-lag tracking (Decision 10). No further amendment of its own. |
| 0015 optional-dependency extras | Stands. Extras are unchanged. |
| 0016 optional tracing no-op by default | Amended. Store-level spans are removed with the legacy stores (Decision 8); status pointer added. |
| 0018 tenant isolation model | Stands. Tenancy remains a read-option filter, never a stream-identity component. |
| 0019 clean-architecture store ports | Amended. Its Status condition — the legacy ABC remaining the default surface behind the compatibility wrapper — has ended; status pointer and a Consequences line added. |
| 0021 snapshot policy/scheduler composition | Stands. Snapshot collaborators were already ports-typed. |
| 0024 projection persistence ports | Amended. `SubscriptionPositions` retypes from `int` to `Position` (Decision 9); status pointer added. |

## Consequences

- The public surface loses `eventsource.stores` entirely: `import eventsource.stores` raises `ModuleNotFoundError`, and `MemoryEventStore`, `LegacyStoreAdapter`, `StoredEvent`, `EventStream`, `ReadOptions`, `TypeConverter`, and `EventStoreConformanceSuite` all die without a replacement name. `InMemoryEventStore`, `PostgreSQLEventStore`, and `SQLiteEventStore` continue to exist, sourced from `eventsource.adapters.*` and re-exported from the ports adapters rather than from `stores/`.
- Consumers upgrading past this ADR must re-verify every call site touching `AppendResult.position` (first-vs-last), duplicate-append handling (now raising), category-read time semantics (storage time, inclusive), `stored_at` assertions in tests written against the legacy in-memory store's fabricated timestamp, empty-batch appends (now `ValueError`), `current_position()` on an empty store (now `None`, not `0`), and any use of BACKWARD feed reads or feed-level timestamp filters, neither of which has a ports equivalent.
- `docs/adrs/0016-optional-tracing-no-op-by-default.md`, `docs/adrs/0019-clean-architecture-store-ports.md`, and `docs/adrs/0024-projection-persistence-ports.md` each carry an "Amended by ADR 0025" Status pointer as part of this change, per the ADR Impact table above.
- The migration ring's `PostgreSQLEventStore` outbox write path (Decision 13) is the one piece of legacy-adjacent code that grows rather than shrinks in this retirement; everything else in the deletion list (§7 of the design spec) is net removal.
- The rejected `StreamDiscovery` port (Decision 3) and the rejected linear-scan nearest-position lookup (Decision 14) are recorded here so a future implementer who reaches for either first checks why it was declined.

## Supersedes

Nothing.

## Superseded by

Nothing.
