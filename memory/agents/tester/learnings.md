# Learnings: tester

## Codebase Patterns
- `asyncio_mode = "auto"` in pytest config -- all test functions are async by default (added: 2026-02-13)
- Test markers: integration, postgres, sqlite, redis, kafka, rabbitmq, slow, e2e, benchmark (added: 2026-02-13)
- Unit tests run without Docker; integration tests require `docker-compose -f docker-compose.test.yml up -d` (added: 2026-02-13)

## Gotchas
- RabbitMQ and OpenTelemetry tests fail when optional deps aren't installed -- need `uv sync --all-extras` for full coverage (added: 2026-02-13)
- Test helper classes with "Test" prefix (TestState, TestEvent, TestAggregate) trigger pytest collection warnings -- avoid "Test" prefix for non-test classes (added: 2026-02-13)
- Unit test suite is 5,216 tests; ~91% pass rate without optional deps, ~212s runtime (added: 2026-02-13)

- Conformance suites use ABC pattern: backends subclass and implement create_store()/create_bus() + create_test_event() (added: 2026-02-14)
- aggregate_type passed to append_events must match the aggregate_type field on the events themselves, or version tracking fails (added: 2026-02-14)

## Preferences
- (none yet)

## Cross-Agent Notes
- (none yet)
