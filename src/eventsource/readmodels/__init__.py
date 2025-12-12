"""
Read model persistence tooling for event-sourced projections.

This module provides infrastructure for persisting and querying read models,
which are denormalized views of aggregate state optimized for query performance.

Key Components:
    ReadModel: Pydantic base class for read model definitions
    ReadModelRepository: Protocol for read model persistence
    Query: Query specification with filters, ordering, pagination
    Filter: Single filter condition (eq, ne, gt, etc.)
    InMemoryReadModelRepository: In-memory implementation for testing

SQL Backends (available after importing):
    PostgreSQLReadModelRepository: Production-ready PostgreSQL implementation
    SQLiteReadModelRepository: Embedded/development SQLite implementation

Exceptions:
    ReadModelError: Base exception for read model operations
    OptimisticLockError: Raised when version conflicts during save
    ReadModelNotFoundError: Raised when a model is not found

Example:
    >>> from uuid import uuid4
    >>> from decimal import Decimal
    >>> from eventsource.readmodels import (
    ...     ReadModel,
    ...     InMemoryReadModelRepository,
    ...     Query,
    ...     Filter,
    ...     OptimisticLockError,
    ... )
    >>>
    >>> # Define a read model
    >>> class OrderSummary(ReadModel):
    ...     order_number: str
    ...     customer_name: str
    ...     status: str
    ...     total_amount: Decimal
    ...
    >>> # Create repository and save models
    >>> repo = InMemoryReadModelRepository(OrderSummary)
    >>> await repo.save(OrderSummary(
    ...     id=uuid4(),
    ...     order_number="ORD-001",
    ...     customer_name="Alice",
    ...     status="shipped",
    ...     total_amount=Decimal("99.99"),
    ... ))
    >>>
    >>> # Query with filters
    >>> shipped = await repo.find(Query(
    ...     filters=[Filter.eq("status", "shipped")],
    ...     order_by="created_at",
    ...     order_direction="desc",
    ... ))
    >>>
    >>> # Optimistic locking for concurrent updates
    >>> summary = await repo.get(order_id)
    >>> summary.status = "shipped"
    >>> try:
    ...     await repo.save_with_version_check(summary)
    ... except OptimisticLockError as e:
    ...     print(f"Conflict: {e}")
"""

from eventsource.readmodels.base import ReadModel
from eventsource.readmodels.exceptions import (
    OptimisticLockError,
    ReadModelError,
    ReadModelNotFoundError,
)
from eventsource.readmodels.in_memory import InMemoryReadModelRepository
from eventsource.readmodels.postgresql import PostgreSQLReadModelRepository
from eventsource.readmodels.projection import ReadModelProjection
from eventsource.readmodels.query import Filter, Query
from eventsource.readmodels.repository import ReadModelRepository
from eventsource.readmodels.schema import (
    POSTGRESQL_TYPE_MAP,
    SQLITE_TYPE_MAP,
    generate_full_schema,
    generate_indexes,
    generate_schema,
)
from eventsource.readmodels.sqlite import SQLiteReadModelRepository

__all__ = [
    # Base class
    "ReadModel",
    # Protocol
    "ReadModelRepository",
    # Projection integration
    "ReadModelProjection",
    # Query building
    "Query",
    "Filter",
    # Exceptions
    "ReadModelError",
    "OptimisticLockError",
    "ReadModelNotFoundError",
    # In-memory implementation
    "InMemoryReadModelRepository",
    # PostgreSQL implementation
    "PostgreSQLReadModelRepository",
    # SQLite implementation
    "SQLiteReadModelRepository",
    # Schema generation
    "generate_schema",
    "generate_indexes",
    "generate_full_schema",
    "POSTGRESQL_TYPE_MAP",
    "SQLITE_TYPE_MAP",
]
