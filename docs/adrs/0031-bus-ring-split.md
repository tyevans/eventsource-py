# 0031. Bus Ring Split

`bus/` was the last top-level package outside `adapters/` that held more than
one technology's implementation behind a shared interface -- exactly the
shape ADR 0019, ADR 0024, ADR 0025, ADR 0026, and ADR 0029 had already
resolved for stores, projections, outbox, and locks/read models. This ADR
moves `EventBus` onto the ports ring, moves the four backend implementations
(`InMemoryEventBus`, `RedisEventBus`, `KafkaEventBus`, `RabbitMQEventBus`)
and their collaborators onto the adapters ring, and deletes `bus/` and its
facade `__init__.py` outright.

| Old import | New import | Ring |
| --- | --- | --- |
| `eventsource.bus.interface.EventBus` | `eventsource.ports.bus.EventBus` | ports |
| `eventsource.bus.base.BaseEventBus` | `eventsource.adapters._bus.BaseEventBus` | adapters (internal) |
| `eventsource.bus.registry.SubscriptionRegistry` | `eventsource.adapters._bus.SubscriptionRegistry` | adapters (internal) |
| `eventsource.bus.memory.InMemoryEventBus` | `eventsource.adapters.memory.bus.InMemoryEventBus` | adapters |
| `eventsource.bus.redis.RedisEventBus` | `eventsource.adapters.redis.bus.RedisEventBus` | adapters |
| `eventsource.bus.redis.RedisEventBusConfig` | `eventsource.adapters.redis.bus.RedisEventBusConfig` | adapters |
| `eventsource.bus.kafka.*` (`KafkaEventBus` + consumer/publisher/connection/config/dlq/metrics/models collaborators) | `eventsource.adapters.kafka.*` | adapters |
| `eventsource.bus.rabbitmq.*` (`RabbitMQEventBus` + consumer/publisher/connection/config/dlq/topology/serialization/models/death_headers collaborators) | `eventsource.adapters.rabbitmq.*` | adapters |

`REDIS_AVAILABLE`, `KAFKA_AVAILABLE`, and `RABBITMQ_AVAILABLE` -- the guarded
optional-import flags -- are preserved verbatim in their new adapter
packages.

## Status

**Accepted.** Implemented in `src/eventsource/ports/bus.py`,
`src/eventsource/adapters/_bus/`, `src/eventsource/adapters/memory/bus.py`,
`src/eventsource/adapters/redis/`, `src/eventsource/adapters/kafka/`,
`src/eventsource/adapters/rabbitmq/`. `src/eventsource/bus/` -- including its
facade `__init__.py` -- is deleted outright. `import eventsource.bus`, and
every `eventsource.bus.*` submodule import, now raises `ModuleNotFoundError`.
No shim, no deprecation warning: the same pre-1.0, no-external-consumers
standing rule ADR 0025, ADR 0026, ADR 0029, and ADR 0030 already applied to
`stores/`, `repositories/`, `locks/`, `readmodels/`, and the other five
top-level modules applies here without qualification. Top-level
`from eventsource import ...` imports are unaffected -- the barrel
re-exports from the new homes directly and always did.

This same deletion completes the "Remove bus facade compat shims (P2)"
backlog item. That entry asked for the ~90 white-box test call sites that
reached through `bus._connection_manager.*`-style facade properties to be
migrated onto direct collaborator access ahead of a scheduled 0.8.0 shim
removal. Splitting the package onto the ring map retargets every one of
those call sites onto the new adapter-internal collaborator modules in the
same pass -- there is no facade left to shim, so the scheduled removal work
and this migration are the same commit, not two.

**Amended by ADR 0031: [ADR 0007](0007-event-bus-delivery-semantics.md),
[ADR 0010](0010-uniform-event-bus-contract.md),
[ADR 0011](0011-handler-error-isolation-with-no-ack.md), and
[ADR 0020](0020-broker-backend-collaborator-decomposition.md).** All four
ADRs' Decisions stand untouched -- delivery semantics (0007), the uniform
`EventBus` contract (0010), no-ack error isolation (0011), and the
per-backend collaborator decomposition into `ConnectionManager` /
`Publisher` / `Consumer` / `DLQAdmin` / topology objects (0020) are exactly
as designed and exactly as tested. What changes is purely where the classes
those ADRs describe live: `EventBus` moves from `eventsource.bus.interface`
to `eventsource.ports.bus`, and the Rabbit/Kafka collaborator packages ADR
0020 introduced move from `eventsource.bus.{rabbitmq,kafka}` to
`eventsource.adapters.{rabbitmq,kafka}`. Each ADR's body is left as written;
this paragraph is the only pointer added to each.

## Context

`docs/core-surface.md` and `.claude/rules/architecture.md`'s ring map both
carried `bus/` under interface adapters' transitional list -- "`adapters/`
(during transition also `stores/`, `bus/` backend modules)" -- as the last
multi-backend package still sitting outside `adapters/` after ADR 0025
retired `stores/` and ADR 0029 closed out `locks/` and `readmodels/`.
`BACKLOG.md` carried the move as a P2 "campaign residue" entry: "`bus/`
colocates the EventBus interface with InMemory, Redis, RabbitMQ, and Kafka
backends. Ring migration: EventBus port into `ports/`, backends into
`adapters/<backend>/bus.py`... Coordinate with the existing 'Remove bus
facade compat shims (P2)' entry."

`EventBus` is an output port in exactly the sense `EventPublisher`
(`eventsource.ports.bus`, moved there by the ports/value-object surface
work earlier in this cycle) already is: the use-case ring calls it, adapters
implement it. Leaving `EventBus` in a standalone `bus/` package while
`EventPublisher` sat in `ports/bus` split one conceptual port -- publish plus
subscribe/deliver -- across two packages for no reason but history.

`BaseEventBus` (shared retry/backoff/handler-dispatch machinery) and
`SubscriptionRegistry` (the handler-routing table ADR 0013 introduced) are
not ports and not a single technology's adapter -- they are implementation
detail shared by every backend adapter, the same relationship
`adapters/_sql/` already has to `adapters/sql/`, `adapters/postgresql/`, and
`adapters/sqlite/`. They move to `eventsource.adapters._bus`, an
underscore-internal adapters-ring package, rather than into any one
backend's directory.

Redis previously had no per-technology package the way PostgreSQL and
SQLite do for stores; `bus.redis` was the only Redis-shaped module in the
codebase. This ADR gives it one -- `eventsource.adapters.redis/` -- so the
adapters ring's per-technology-directory convention (`adapters/postgresql/`,
`adapters/sqlite/`, `adapters/kafka/`, `adapters/rabbitmq/`, and now
`adapters/redis/`) is uniform across every backend, not four out of five.

## Decision

1. `EventBus` (the ABC) moves from `eventsource.bus.interface` into
   `eventsource.ports.bus`, alongside the `EventPublisher` port already
   there. This is a pure relocation -- the ABC's contract, method
   signatures, and the guarantees ADR 0007/0010/0011 describe are unchanged.
2. `BaseEventBus` and `SubscriptionRegistry` move into
   `eventsource.adapters._bus`, a new adapters-ring internal package (same
   pattern as `adapters/_sql`): shared collaborator code that every backend
   adapter composes, owned by no single technology.
3. `InMemoryEventBus` moves to `eventsource.adapters.memory.bus`, alongside
   the other in-memory adapters already under `adapters/memory/`.
4. `RedisEventBus` and `RedisEventBusConfig` move to a new
   `eventsource.adapters.redis` package (`adapters/redis/bus.py`), Redis's
   first per-technology adapter directory.
5. `KafkaEventBus` and every collaborator ADR 0020 introduced for it
   (consumer, publisher, connection, config, dlq, metrics, models) move to
   `eventsource.adapters.kafka`, unchanged in internal shape.
6. `RabbitMQEventBus` and every collaborator ADR 0020 introduced for it
   (consumer, publisher, connection, config, dlq, topology, serialization,
   models, death_headers) move to `eventsource.adapters.rabbitmq`, unchanged
   in internal shape.
7. `REDIS_AVAILABLE`, `KAFKA_AVAILABLE`, and `RABBITMQ_AVAILABLE` -- the
   guarded-import flags each backend exports so callers can check
   availability at runtime without importing the optional dependency --
   move with their respective packages, unchanged in behavior.
8. `eventsource/bus/` -- all of it, including the facade `__init__.py` that
   re-exported all four backends and their collaborators from one module --
   is deleted. No shim, no deprecation warning, no back-compat alias: the
   library is unreleased, the same standing rule ADR 0025/0026/0029/0030
   already applied without qualification.
9. Top-level `from eventsource import ...` re-exports (`EventBus`,
   `InMemoryEventBus`, `RedisEventBus`, `KafkaEventBus`, `RabbitMQEventBus`,
   and the rest of the twenty-name bus import surface) are unaffected --
   the barrel resolves them from the new homes directly.

## Alternatives Considered

- **Keep `bus/` as a facade over `adapters/*` re-exports**: would preserve
  the ~90 white-box test call sites unchanged, but recreates exactly the
  shim-maintenance cost (a second source of truth for where a name lives,
  plus the guarded-import fallback machinery duplicated at the facade layer)
  that ADR 0025/0026/0029/0030 already rejected for every other retired
  package. Rejected.
- **One `adapters/bus/` package holding all four backends together**: keeps
  a bus-shaped grouping but violates the adapters-ring convention every
  other multi-backend concern already follows (`adapters/memory/`,
  `adapters/postgresql/`, `adapters/sqlite/`, `adapters/kafka/`,
  `adapters/rabbitmq/` are per-technology, not per-concern). Rejected.
- **Give `BaseEventBus`/`SubscriptionRegistry` their own top-level
  `bus_shared/` or similar**: adds a seventh transitional-looking package for
  what is, in every other ring, adapter-internal shared code
  (`adapters/_sql` is the direct precedent). Rejected in favor of
  `adapters/_bus`.

## Consequences

- `bus/` no longer exists as a ring exception; the adapters-ring
  transitional list in `.claude/rules/architecture.md` drops its last
  "`bus/` backend modules" phrase.
- The "Remove bus facade compat shims (P2)" backlog entry is resolved by
  this deletion rather than by a separate 0.8.0 migration -- see Status.
- The "Migrate bus/ interface and backends to ports/adapters (P2)" backlog
  entry is resolved by this ADR.
- Redis gains a per-technology adapter directory
  (`eventsource.adapters.redis`) for the first time, closing the last
  backend-directory asymmetry between Redis and the other four backends.
- Delivery, retry, and error-isolation semantics (ADR 0007, ADR 0010, ADR
  0011) and the collaborator decomposition (ADR 0020) are unchanged; the
  conformance suite passes unmodified against the relocated adapters.
