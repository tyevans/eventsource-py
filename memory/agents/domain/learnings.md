# Learnings: domain

## Codebase Patterns
- `@handles(EventType)` decorator maps events to handler methods on DeclarativeAggregate and DeclarativeProjection (added: 2026-02-13)
- Aggregates use optimistic locking via expected_version for concurrency control (added: 2026-02-13)
- Multitenancy support via TenantDomainEvent and tenant context/scopes (added: 2026-02-13)

- subscriptions/ package structure: Manager → Lifecycle → Registry → Subscription state machine → Runners (catchup/live) → Supporting (flow control, retry, health, shutdown) (added: 2026-02-14)

## Gotchas
- (none yet)

## Preferences
- (none yet)

## Cross-Agent Notes
- (none yet)
