# ADR 0019: Clean-Architecture Store Ports and Opaque Positions

**Status:** Accepted and implemented — ports, memory/sqlite/postgresql adapters,
conformance suites, and the `LegacyStoreAdapter` compatibility wrapper have
landed on this branch. The legacy `EventStore` ABC remains the default shipped
surface, behind that compatibility wrapper, until the application layer is
retyped onto the new ports (sub-project 2).

**Date:** 2026-07-29

**Deciders:** Library maintainers (architecture owner)

---

## Context

The `EventStore` ABC requires global-ordering methods that wide-column,
document, and log-structured backends cannot honestly provide. The interface
already admits this inconsistently: `read_all` has a non-abstract default that
raises `NotImplementedError`, `get_global_position` is abstract, and the
`read_stream` default fabricates `global_position=0`. The library has no
external users, making a full store-contract redesign uniquely cheap now. The
target backend families are SQL (PostgreSQL, SQLite, MySQL, CockroachDB),
wide-column (Cassandra, ScyllaDB, DynamoDB), document (MongoDB), and
purpose-built ES/log systems (EventStoreDB, single-log Kafka).

Full design: `docs/superpowers/specs/2026-07-29-core-rings-design.md`.

## Decision

1. **Clean Architecture rings with the Dependency Rule.** Entities
   (`domain/`), boundary ports (`ports/`), use cases (`application/`),
   interface adapters (`adapters/`) with framework/driver imports confined to
   the outermost ring. Enforced by import-linter contracts as the layout lands.
2. **The store contract is five segregated output ports**, structural
   `Protocol` classes: `EventAppender`, `StreamReader`, `EventLookup`,
   `GlobalEventFeed`, `CategoryQuery`. A backend implements exactly the ports
   it can honor; unsupported capability = unimplemented port, never
   `NotImplementedError`.
3. **Global feed positions are opaque ordered tokens** (`Position` value
   object): totally ordered within one store, serializable, no arithmetic,
   store-identity-guarded (ordering comparisons across stores raise;
   equality returns False). Produced only by `GlobalEventFeed` implementers.
   Consequence, accepted deliberately: position-delta lag metrics are
   abolished (amends ADR 0014); lag is re-expressed in wall-clock or
   count-behind terms in the application-layer redesign.
4. **Per-stream versions remain 1-based integer event counts** (absent = 0) —
   unchanged from the current contract and schema. Optimistic concurrency via
   an `ExpectedVersion` VO with `any`/`no_stream`/`stream_exists`/`exact`.
5. **The feed contract guarantees exclusive resumption and no-skip delivery**:
   resuming strictly after a feed-produced position must never permanently
   skip a committed event. The PostgreSQL adapter bounds feed reads to a
   transaction-safe horizon to honor this.
6. **Duplicate `event_id` appends raise `DuplicateEventError`**, backed by the
   existing unique constraint — a race-free idempotency primitive for
   migration tooling.

## Alternatives Considered

- **ABC inheritance ladder** (`EventStore` → `GloballyOrderedEventStore`):
  rejected — capabilities are orthogonal, ladders force false hierarchies, and
  default implementations invite the `NotImplementedError` pattern this ADR
  bans.
- **Int positions everywhere, NoSQL backends synthesize them**: rejected —
  pushes an unbounded synthetic-ordering problem into every future adapter.
- **Runtime capability flags** (`supports_global_ordering`): rejected — moves
  wiring errors from mypy to production.
- **Partially-ordered positions** (Kafka offset vectors): rejected — exclusive
  resumption is only well-defined over a total order; multi-partition Kafka
  is out of `GlobalEventFeed` scope rather than weakening the law.

## Consequences

- Catch-up subscriptions and live migration type-require `GlobalEventFeed`;
  stores without it are statically excluded from those roles.
- Existing backends port behind legacy wrapper classes until the application
  layer is retyped; persisted int checkpoints stay decodable via the SQL
  position codec.
- ADR 0014 is amended as described in Decision 3.
- ADR 0018's tenant model stands: tenancy remains a read-option filter, not a
  stream-identity component.
