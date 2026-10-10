"""Shared pytest fixtures for the eventsource library tests.

This module provides comprehensive test fixtures including:
- Domain event fixtures (event_factory, sample_event, event_stream)
- Event store fixtures (in_memory_store, populated_store)
- Repository fixtures (checkpoint_repo, dlq_repo, outbox_repo)
- Aggregate fixtures (test_aggregate, populated_aggregate)
- Projection fixtures (test_projection)
- Sample data fixtures (aggregate_id, tenant_id)
- SQLite fixtures (sqlite_connection, sqlite_checkpoint_repo, etc.)
- OpenTelemetry metrics fixtures (metric_reader, reset_kafka_meter)

All fixtures are properly scoped and documented for easy reuse.
"""

from __future__ import annotations

import pytest
from hypothesis import HealthCheck, settings

from tests.conftest_helpers import (
    AIOSQLITE_AVAILABLE,
    OTEL_METRICS_AVAILABLE,
    MockEventPublisher,
    aggregate_id,
    checkpoint_repo,
    counter_aggregate,
    counter_event,
    customer_id,
    declarative_counter_aggregate,
    dlq_repo,
    event_factory,
    event_stream,
    in_memory_store,
    metric_reader,
    mock_event_publisher,
    order_aggregate,
    order_event_stream,
    outbox_repo,
    populated_counter_aggregate,
    populated_order_aggregate,
    populated_store,
    reset_kafka_meter,
    sample_event,
    skip_if_no_aiosqlite,
    skip_if_no_otel_metrics,
    sqlite_checkpoint_repo,
    sqlite_connection,
    sqlite_dlq_repo,
    sqlite_outbox_repo,
    tenant_id,
)
from tests.fixtures import (
    CounterAggregate,
    CounterDecremented,
    CounterIncremented,
    CounterNamed,
    CounterReset,
    CounterState,
    DeclarativeCounterAggregate,
    OrderAggregate,
    OrderCreated,
    OrderItemAdded,
    OrderShipped,
    OrderState,
    SampleEvent,
    create_event,
)

# ============================================================================
# Pytest Configuration
# ============================================================================


def pytest_configure(config: pytest.Config) -> None:
    """Register custom markers for tests."""
    config.addinivalue_line("markers", "sqlite: marks tests that require SQLite (aiosqlite)")


# ============================================================================
# Hypothesis Profiles
# ============================================================================

settings.register_profile("default", max_examples=100)
settings.register_profile("ci", max_examples=500, deadline=None)
settings.register_profile(
    "db",
    max_examples=25,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
settings.load_profile("default")


# =============================================================================
# Re-export fixtures module items for convenience
# =============================================================================

__all__ = [
    # Events
    "CounterIncremented",
    "CounterDecremented",
    "CounterNamed",
    "CounterReset",
    "OrderCreated",
    "OrderItemAdded",
    "OrderShipped",
    "SampleEvent",
    # States
    "CounterState",
    "OrderState",
    # Aggregates
    "CounterAggregate",
    "DeclarativeCounterAggregate",
    "OrderAggregate",
    # Factory
    "create_event",
    # Mock
    "MockEventPublisher",
    # SQLite
    "AIOSQLITE_AVAILABLE",
    "skip_if_no_aiosqlite",
    # OpenTelemetry Metrics
    "OTEL_METRICS_AVAILABLE",
    "skip_if_no_otel_metrics",
    # Fixture functions
    "aggregate_id",
    "checkpoint_repo",
    "counter_aggregate",
    "counter_event",
    "customer_id",
    "declarative_counter_aggregate",
    "dlq_repo",
    "event_factory",
    "event_stream",
    "in_memory_store",
    "metric_reader",
    "mock_event_publisher",
    "order_aggregate",
    "order_event_stream",
    "outbox_repo",
    "populated_counter_aggregate",
    "populated_order_aggregate",
    "populated_store",
    "reset_kafka_meter",
    "sample_event",
    "sqlite_checkpoint_repo",
    "sqlite_connection",
    "sqlite_dlq_repo",
    "sqlite_outbox_repo",
    "tenant_id",
]
