"""Shared pytest fixtures for integration tests.

This module provides fixtures for PostgreSQL and Redis test infrastructure
using testcontainers for automatic container management.

If testcontainers or Docker is not available, tests are automatically skipped.
"""

from __future__ import annotations

import pytest

from .conftest_helpers import (
    CHECKPOINTS_SCHEMA_STATEMENTS,
    DLQ_SCHEMA_STATEMENTS,
    DOCKER_AVAILABLE,
    OUTBOX_SCHEMA_STATEMENTS,
    POSTGRES_CONTAINER,
    REDIS_CONTAINER,
    TESTCONTAINERS_AVAILABLE,
    TestItemCreated,
    TestItemDeleted,
    TestItemUpdated,
    TestOrderAggregate,
    TestOrderCompleted,
    TestOrderCreated,
    TestOrderItemAdded,
    TestOrderState,
    clean_postgres_tables,
    clean_redis,
    is_docker_available,
    postgres_checkpoint_repo,
    postgres_connection_url,
    postgres_container,
    postgres_dlq_repo,
    postgres_engine,
    postgres_event_store,
    postgres_event_store_with_outbox,
    postgres_outbox_repo,
    postgres_session_factory,
    redis_client,
    redis_connection_url,
    redis_container,
    redis_event_bus,
    redis_event_bus_factory,
    sample_aggregate_id,
    sample_customer_id,
    sample_item_event,
    sample_order_event,
    sample_tenant_id,
    skip_if_no_docker,
    skip_if_no_postgres_infra,
    skip_if_no_redis_infra,
    skip_if_no_testcontainers,
)


def pytest_configure(config: pytest.Config) -> None:
    """Register custom markers for integration tests."""
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests (may require docker)"
    )
    config.addinivalue_line("markers", "postgres: marks tests that require PostgreSQL")
    config.addinivalue_line("markers", "redis: marks tests that require Redis")
    config.addinivalue_line("markers", "rabbitmq: marks tests that require RabbitMQ")
    config.addinivalue_line("markers", "kafka: marks tests that require Kafka")
    config.addinivalue_line("markers", "e2e: marks tests as end-to-end integration tests")


__all__ = [
    "CHECKPOINTS_SCHEMA_STATEMENTS",
    "DLQ_SCHEMA_STATEMENTS",
    "DOCKER_AVAILABLE",
    "OUTBOX_SCHEMA_STATEMENTS",
    "POSTGRES_CONTAINER",
    "REDIS_CONTAINER",
    "TESTCONTAINERS_AVAILABLE",
    "TestItemCreated",
    "TestItemDeleted",
    "TestItemUpdated",
    "TestOrderAggregate",
    "TestOrderCompleted",
    "TestOrderCreated",
    "TestOrderItemAdded",
    "TestOrderState",
    "clean_postgres_tables",
    "clean_redis",
    "is_docker_available",
    "postgres_checkpoint_repo",
    "postgres_connection_url",
    "postgres_container",
    "postgres_dlq_repo",
    "postgres_engine",
    "postgres_event_store",
    "postgres_event_store_with_outbox",
    "postgres_outbox_repo",
    "postgres_session_factory",
    "pytest_configure",
    "redis_client",
    "redis_connection_url",
    "redis_container",
    "redis_event_bus",
    "redis_event_bus_factory",
    "sample_aggregate_id",
    "sample_customer_id",
    "sample_item_event",
    "sample_order_event",
    "sample_tenant_id",
    "skip_if_no_docker",
    "skip_if_no_postgres_infra",
    "skip_if_no_redis_infra",
    "skip_if_no_testcontainers",
]
