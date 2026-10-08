---
id: REFACTOR-tests-integration-conftest
title: Refactor and Decompose Legacy File conftest.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-tests-integration-conftest: Refactor Legacy File conftest.py

## Summary
The grandfathered debt file `tests/integration/conftest.py` contains 761 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (conftest_postgres.py, conftest_order.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `tests/integration/conftest/` with submodules:
- `conftest_postgres.py`: postgres_container, postgres_connection_url, postgres_engine, postgres_session_factory, clean_postgres_tables, postgres_event_store, postgres_event_store_with_outbox, postgres_checkpoint_repo, postgres_dlq_repo, postgres_outbox_repo, pytest_configure, is_docker_available, TestItemCreated, TestItemUpdated, TestItemDeleted, redis_container, redis_connection_url, redis_client, clean_redis, redis_event_bus_factory, redis_event_bus, sample_aggregate_id, sample_tenant_id, sample_customer_id, sample_item_event
- `conftest_order.py`: TestOrderCreated, TestOrderItemAdded, TestOrderCompleted, TestOrderState, TestOrderAggregate, sample_order_event

## AST Decomposition Blueprint
Decomposition Blueprint for tests/integration/conftest.py (761 lines):
  Submodule 'conftest_postgres.py' (~357 lines):
    - [function] postgres_container (lines 346-361)
    - [function] postgres_connection_url (lines 365-369)
    - [function] postgres_engine (lines 373-413)
    - [function] postgres_session_factory (lines 417-429)
    - [function] clean_postgres_tables (lines 433-446)
    - [function] postgres_event_store (lines 543-569)
    - [function] postgres_event_store_with_outbox (lines 573-598)
    - [function] postgres_checkpoint_repo (lines 678-686)
    - [function] postgres_dlq_repo (lines 690-698)
    - [function] postgres_outbox_repo (lines 702-710)
    - [function] pytest_configure (lines 30-39)
    - [function] is_docker_available (lines 60-72)
    - [class] TestItemCreated (lines 109-114)
    - [class] TestItemUpdated (lines 118-123)
    - [class] TestItemDeleted (lines 127-130)
    - [function] redis_container (lines 455-470)
    - [function] redis_connection_url (lines 474-478)
    - [function] redis_client (lines 482-505)
    - [function] clean_redis (lines 509-534)
    - [function] redis_event_bus_factory (lines 607-648)
    - [function] redis_event_bus (lines 652-669)
    - [function] sample_aggregate_id (lines 719-721)
    - [function] sample_tenant_id (lines 725-727)
    - [function] sample_customer_id (lines 731-733)
    - [function] sample_item_event (lines 737-745)
  Submodule 'conftest_order.py' (~121 lines):
    - [class] TestOrderCreated (lines 134-139)
    - [class] TestOrderItemAdded (lines 143-150)
    - [class] TestOrderCompleted (lines 154-158)
    - [class] TestOrderState (lines 168-176)
    - [class] TestOrderAggregate (lines 179-258)
    - [function] sample_order_event (lines 749-761)
  Suggested barrel exports:
    from .conftest_postgres import postgres_container, postgres_connection_url, postgres_engine, postgres_session_factory, clean_postgres_tables, postgres_event_store, postgres_event_store_with_outbox, postgres_checkpoint_repo, postgres_dlq_repo, postgres_outbox_repo, pytest_configure, is_docker_available, TestItemCreated, TestItemUpdated, TestItemDeleted, redis_container, redis_connection_url, redis_client, clean_redis, redis_event_bus_factory, redis_event_bus, sample_aggregate_id, sample_tenant_id, sample_customer_id, sample_item_event
    from .conftest_order import TestOrderCreated, TestOrderItemAdded, TestOrderCompleted, TestOrderState, TestOrderAggregate, sample_order_event

    __all__ = ["postgres_container", "postgres_connection_url", "postgres_engine", "postgres_session_factory", "clean_postgres_tables", "postgres_event_store", "postgres_event_store_with_outbox", "postgres_checkpoint_repo", "postgres_dlq_repo", "postgres_outbox_repo", "pytest_configure", "is_docker_available", "TestItemCreated", "TestItemUpdated", "TestItemDeleted", "redis_container", "redis_connection_url", "redis_client", "clean_redis", "redis_event_bus_factory", "redis_event_bus", "sample_aggregate_id", "sample_tenant_id", "sample_customer_id", "sample_item_event", "TestOrderCreated", "TestOrderItemAdded", "TestOrderCompleted", "TestOrderState", "TestOrderAggregate", "sample_order_event"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
