# ADR-0022: Command Objects and the Decider Aggregate Style

## Status

Accepted (2026-07-30)

## Context

The library shipped two aggregate styles (hand-written `_apply` on
`AggregateRoot`; `@handles` on `DeclarativeAggregate`). The decider pattern —
the domain as pure `decide`/`evolve` functions — was documented in
`docs/explanation/decider-pattern.md` as a userland recipe with two structural
problems: `AggregateRoot._state` is `None` until the first event (a naive
decider rejects its first command), and version stamping required a manual
`model_copy` per event. Separately, `DomainEvent` carries
`correlation_id`/`causation_id`/`actor_id`/`tenant_id` but nothing originated
the chain: `with_causation()` links event→event only. In CQRS the originator
of an event chain is the command. Benchmarks (2026-07-30) showed the decider's
overhead is ~1.5x on the command path (~8 µs/order, dominated by pydantic event
construction paid by all styles) and ~1.06x on replay: maintainability, not
performance, decides.

## Decision

1. **`DomainCommand`** (entities ring, `commands/` beside `events/`): frozen
   pydantic model with `command_id`, `issued_at`, `correlation_id`,
   `actor_id`, `tenant_id`. Commands are **never persisted** — a rejected
   command leaves no trace. No registry, no serialization, no command bus.
   Commands have no `causation_id` field; `caused_by(event)` copies the
   event's `correlation_id` so saga-issued commands continue the workflow
   chain by correlation.
2. **`DeciderAggregate`** subclasses `AggregateRoot`: abstract static
   `initial_state`/`decide`/`evolve`; eager state initialization (state is
   never `None`); `execute(command)` runs `decide` to completion, then stamps
   each event with one `model_copy` — always `aggregate_version` and
   `aggregate_type`; for `DomainCommand`s also `causation_id=command_id`,
   `correlation_id`, `actor_id`, `tenant_id`. Precedence: fields in
   `event.model_fields_set` are never overwritten. Tenant resolution:
   command value → tenant context → untouched. Rejections are atomic: no
   version bump, no uncommitted events.
3. **Structural typing, opt-in provenance**: `decide`/`execute`/`create_event`
   accept any object as a command; `isinstance(command, DomainCommand)` is
   what unlocks provenance. `create_event(command=...)` gives the imperative
   and declarative styles identical stamping.
4. **`CommandRejectedError`** is the conventional (not required) rejection
   type: one catchable exception meaning "the domain said no".
5. **The decider is the primary showcased style**: examples, tutorials, and
   general-purpose fixtures lead with it; imperative and declarative each
   keep one worked reference example.

## Consequences

- Every event can be traced to the command that caused it and the actor who
  issued it; `causation_id` references a `command_id` that is resolvable only
  if the application logs its commands, but it still groups the events of one
  command and marks them command-caused.
- The command path pays one `model_copy` per event (~1–2 µs); replay is
  unaffected. Rejected alternatives: an event-spec `decide` contract (faster
  but non-standard signature, awkward assertions) and version-at-append
  (ADR-scale churn to save ~2 µs/event).
- ADR-0001 (async-first) stands: `decide`/`evolve` are sync pure functions
  like `_apply`; I/O boundaries remain async. ADR-0012 stands: commands have
  no registry by design. ADR-0018 stands: this ADR documents the
  command-then-context tenant resolution order as an extension. ADR-0019
  stands: commands are entities-ring; no port changes.
