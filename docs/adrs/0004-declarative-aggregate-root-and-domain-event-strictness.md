# ADR 4: Declarative Aggregate Root and Domain Event Strictness

## Summary
Declarative @handles decorators, single-source wire names, and strict event validation.

## Context
Event schemas and handler dispatch can rot if event types are decoupled from classes or if unknown attributes are silently accepted. In an event-sourced system, corrupt or ambiguous stored events corrupt the database forever.

## Decision
1. Declarative aggregates support `@handles` decorators with class-definition-time signature and duplicate handler validation.
2. `DomainEvent` enforces Pydantic `extra="forbid"`, mandating explicit attribute declarations.
3. Every event carries non-null UUID `event_id`, integer `version`, and UTC `occurred_at`.
4. Event wire type names are derived deterministically or declared explicitly single-source.
5. All library domain exceptions derive from universal base `EventSourceError`.

## Consequences
- Prevents silent attribute dropping or misspelled payload fields.
- Immediate class loading failures if handler signatures are invalid.
- Guaranteed schema integrity across persistent event streams.
