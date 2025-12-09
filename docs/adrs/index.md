# Architecture Decision Records

This directory contains Architecture Decision Records (ADRs) for the eventsource-py library.

## What is an ADR?

An Architecture Decision Record (ADR) is a document that captures an important architectural decision made along with its context and consequences. ADRs are a lightweight way to document significant choices that affect the structure, behavior, or design of a system.

ADRs help teams:

- **Preserve institutional knowledge** about why decisions were made
- **Onboard new contributors** by explaining the rationale behind the codebase
- **Revisit decisions** when circumstances change
- **Avoid repeating past mistakes** by documenting what was tried and why it did or did not work

## ADR Index

| Number | Title | Status | Date |
|--------|-------|--------|------|
| [ADR-0000](0000-template.md) | ADR Template | Accepted | 2025-12-06 |
| [ADR-0001](0001-async-first-design.md) | Async-First Design | Accepted | 2025-12-06 |
| [ADR-0002](0002-pydantic-event-models.md) | Pydantic Event Models | Accepted | 2025-12-06 |
| [ADR-0003](0003-optimistic-locking.md) | Optimistic Locking | Accepted | 2025-12-06 |
| [ADR-0004](0004-projection-error-handling.md) | Projection Error Handling | Accepted | 2025-12-06 |
| [ADR-0005](0005-api-design-patterns.md) | API Design Patterns | Accepted | 2025-12-06 |
| [ADR-0006](0006-event-registry-serialization.md) | Event Registry and Serialization | Accepted | 2025-12-06 |
| [ADR-0007](0007-remove-sync-event-store.md) | Remove SyncEventStore Interface | Accepted | 2025-12-07 |
| [ADR-0008](0008-kafka-event-bus-integration.md) | Kafka Event Bus Integration | Accepted | 2025-12-08 |

## Status Definitions

| Status | Description |
|--------|-------------|
| **Proposed** | The decision is under discussion and not yet finalized |
| **Accepted** | The decision has been accepted and is being or has been implemented |
| **Deprecated** | The decision is no longer relevant (e.g., the feature was removed) |
| **Superseded** | The decision has been replaced by a newer ADR |
| **Planned** | The ADR is planned but not yet written |

## Creating a New ADR

Follow these steps to create a new Architecture Decision Record:

1. **Copy the template:** Copy `0000-template.md` to a new file with the next sequential number:
   ```bash
   cp docs/adrs/0000-template.md docs/adrs/NNNN-descriptive-slug.md
   ```

2. **Choose a descriptive filename:** Use lowercase, hyphen-separated words that summarize the decision:
   - `0007-caching-strategy.md`
   - `0008-error-handling-approach.md`

3. **Fill in all sections:** Replace the placeholder text with your actual content. Remove the instructional comments when done.

4. **Set the initial status:** New ADRs should start with status `Proposed` unless the decision has already been made.

5. **Submit a pull request:** ADRs should be reviewed like any other code change.

6. **Update this index:** Add your new ADR to the table above when your PR is merged.

## When to Write an ADR

Consider writing an ADR when:

- Making a significant architectural or design decision
- Choosing between multiple viable alternatives
- Introducing a new library or framework dependency
- Changing the public API in a non-obvious way
- Establishing a new pattern or convention for the codebase
- Making a decision that future contributors will likely question

You do not need an ADR for:

- Minor implementation details
- Bug fixes
- Obvious choices with no real alternatives
- Decisions that are easily reversible

## Numbering Convention

ADRs use a 4-digit, zero-padded numbering scheme:

- `0000` is reserved for the template
- Numbers are assigned sequentially: `0001`, `0002`, `0003`, ...
- Numbers are never reused, even if an ADR is superseded or deprecated

## File Naming Convention

ADR files follow the pattern: `NNNN-descriptive-slug.md`

- `NNNN`: 4-digit zero-padded number
- `descriptive-slug`: lowercase, hyphen-separated words summarizing the decision
- `.md`: Markdown extension

Examples:
- `0001-async-first.md`
- `0002-pydantic-events.md`
- `0003-optimistic-locking.md`

## References

- [ADR Template](0000-template.md)
- [Documenting Architecture Decisions - Michael Nygard](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions)
- [ADR GitHub Organization](https://adr.github.io/)
- [Lightweight Architecture Decision Records](https://www.thoughtworks.com/radar/techniques/lightweight-architecture-decision-records)
