# Learnings: backend

## Codebase Patterns
- Backend implementations live alongside their interfaces (e.g., stores/postgresql.py, bus/kafka.py) -- the old infrastructure/ directory was deleted as dead code (updated: 2026-02-14)
- Optional dependencies guarded by try/except ImportError at module level (added: 2026-02-13)
- Multiple backends per interface: EventStore has PostgreSQL, SQLite, InMemory; EventBus has InMemory, Redis, RabbitMQ, Kafka (added: 2026-02-13)

## Gotchas
- rabbitmq.py has 29 unused `# type: ignore` comments that fail mypy; kafka.py has 7 -- need cleanup (added: 2026-02-13)

## Preferences
- (none yet)

## Cross-Agent Notes
- (from tester) mypy fails with 38 errors, mostly unused type: ignore in bus/rabbitmq.py and bus/kafka.py (added: 2026-02-13)
