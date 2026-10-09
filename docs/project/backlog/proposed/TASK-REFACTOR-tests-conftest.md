---
id: REFACTOR-tests-conftest
title: Refactor and Decompose Legacy File conftest.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
governing_stories:
  - US-0012
target_bc: core
---

# TASK-REFACTOR-tests-conftest: Refactor Legacy File conftest.py

## Summary
The grandfathered debt file `tests/conftest.py` contains 726 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (conftest_event.py, conftest_aggregate.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/conftest/` with submodules:
- `conftest_event.py`: event_factory, sample_event, counter_event, event_stream, order_event_stream, mock_event_publisher, MockEventPublisher, pytest_configure, tenant_id, customer_id, in_memory_store, populated_store, checkpoint_repo, dlq_repo, outbox_repo, metric_reader, reset_kafka_meter, sqlite_connection, sqlite_checkpoint_repo, sqlite_outbox_repo, sqlite_dlq_repo
- `conftest_aggregate.py`: aggregate_id, counter_aggregate, declarative_counter_aggregate, populated_counter_aggregate, order_aggregate, populated_order_aggregate

## AST Decomposition Blueprint
Decomposition Blueprint for tests/conftest.py (726 lines):
  Submodule 'conftest_event.py' (~368 lines):
    - [function] event_factory (lines 173-188)
    - [function] sample_event (lines 192-206)
    - [function] counter_event (lines 210-224)
    - [function] event_stream (lines 228-257)
    - [function] order_event_stream (lines 261-297)
    - [function] mock_event_publisher (lines 489-496)
    - [class] MockEventPublisher (lines 499-522)
    - [function] pytest_configure (lines 92-94)
    - [function] tenant_id (lines 146-153)
    - [function] customer_id (lines 157-164)
    - [function] in_memory_store (lines 306-315)
    - [function] populated_store (lines 319-341)
    - [function] checkpoint_repo (lines 350-359)
    - [function] dlq_repo (lines 363-372)
    - [function] outbox_repo (lines 376-385)
    - [function] metric_reader (lines 531-554)
    - [function] reset_kafka_meter (lines 558-577)
    - [function] sqlite_connection (lines 586-604)
    - [function] sqlite_checkpoint_repo (lines 608-633)
    - [function] sqlite_outbox_repo (lines 637-663)
    - [function] sqlite_dlq_repo (lines 667-691)
  Submodule 'conftest_aggregate.py' (~83 lines):
    - [function] aggregate_id (lines 135-142)
    - [function] counter_aggregate (lines 394-404)
    - [function] declarative_counter_aggregate (lines 408-418)
    - [function] populated_counter_aggregate (lines 422-438)
    - [function] order_aggregate (lines 442-452)
    - [function] populated_order_aggregate (lines 456-480)
  Suggested barrel exports:
    from .conftest_event import event_factory, sample_event, counter_event, event_stream, order_event_stream, mock_event_publisher, MockEventPublisher, pytest_configure, tenant_id, customer_id, in_memory_store, populated_store, checkpoint_repo, dlq_repo, outbox_repo, metric_reader, reset_kafka_meter, sqlite_connection, sqlite_checkpoint_repo, sqlite_outbox_repo, sqlite_dlq_repo
    from .conftest_aggregate import aggregate_id, counter_aggregate, declarative_counter_aggregate, populated_counter_aggregate, order_aggregate, populated_order_aggregate

    __all__ = ["event_factory", "sample_event", "counter_event", "event_stream", "order_event_stream", "mock_event_publisher", "MockEventPublisher", "pytest_configure", "tenant_id", "customer_id", "in_memory_store", "populated_store", "checkpoint_repo", "dlq_repo", "outbox_repo", "metric_reader", "reset_kafka_meter", "sqlite_connection", "sqlite_checkpoint_repo", "sqlite_outbox_repo", "sqlite_dlq_repo", "aggregate_id", "counter_aggregate", "declarative_counter_aggregate", "populated_counter_aggregate", "order_aggregate", "populated_order_aggregate"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
