"""
Integration tests for read model repositories.

Tests run against all three backends (InMemory, PostgreSQL, SQLite) to ensure
identical behavior across implementations.

These tests verify:
- Basic CRUD operations (get, save, delete)
- Batch operations (get_many, save_many)
- Soft delete and restore functionality
- Query filtering with all operators
- Ordering and pagination
- Edge cases and error handling
"""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING
from uuid import uuid4

import pytest

from eventsource.readmodels import Filter, Query

from ..conftest import skip_if_no_postgres_infra

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import Any

    from .conftest import OrderSummary


pytestmark = [
    pytest.mark.integration,
]


# ============================================================================
# Basic CRUD Operations
# ============================================================================


class TestReadModelRepositoryBasicOperations:
    """Test basic CRUD operations."""

    @pytest.mark.asyncio
    async def test_save_and_get(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test saving and retrieving a read model."""
        order = order_summary_factory(order_number="ORD-001")

        await repo.save(order)
        result = await repo.get(order.id)

        assert result is not None
        assert result.id == order.id
        assert result.order_number == "ORD-001"
        assert result.customer_name == order.customer_name
        assert result.status == order.status

    @pytest.mark.asyncio
    async def test_get_nonexistent_returns_none(self, repo: Any) -> None:
        """Test that getting a nonexistent ID returns None."""
        result = await repo.get(uuid4())
        assert result is None

    @pytest.mark.asyncio
    async def test_save_updates_existing(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test that save updates existing records (upsert behavior)."""
        order = order_summary_factory(status="pending")

        await repo.save(order)

        # Modify and save again
        order.status = "shipped"
        await repo.save(order)

        result = await repo.get(order.id)
        assert result is not None
        assert result.status == "shipped"
        assert result.version == 2  # Version should be incremented

    @pytest.mark.asyncio
    async def test_get_many(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test batch get operation."""
        orders = [order_summary_factory(order_number=f"ORD-{i:03d}") for i in range(5)]
        for order in orders:
            await repo.save(order)

        # Get first 3
        ids = [o.id for o in orders[:3]]
        results = await repo.get_many(ids)

        assert len(results) == 3
        result_ids = {r.id for r in results}
        assert result_ids == set(ids)

    @pytest.mark.asyncio
    async def test_get_many_with_missing_ids(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test that get_many ignores missing IDs."""
        order = order_summary_factory()
        await repo.save(order)

        results = await repo.get_many([order.id, uuid4(), uuid4()])

        assert len(results) == 1
        assert results[0].id == order.id

    @pytest.mark.asyncio
    async def test_save_many(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test batch save operation."""
        orders = [order_summary_factory(order_number=f"ORD-{i:03d}") for i in range(3)]

        await repo.save_many(orders)

        count = await repo.count()
        assert count == 3

    @pytest.mark.asyncio
    async def test_exists(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test existence check."""
        order = order_summary_factory()

        assert await repo.exists(order.id) is False

        await repo.save(order)
        assert await repo.exists(order.id) is True


# ============================================================================
# Delete Operations
# ============================================================================


class TestReadModelRepositoryDelete:
    """Test delete operations."""

    @pytest.mark.asyncio
    async def test_delete(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test hard delete."""
        order = order_summary_factory()
        await repo.save(order)

        deleted = await repo.delete(order.id)

        assert deleted is True
        assert await repo.get(order.id) is None

    @pytest.mark.asyncio
    async def test_delete_nonexistent(self, repo: Any) -> None:
        """Test deleting nonexistent record returns False."""
        deleted = await repo.delete(uuid4())
        assert deleted is False

    @pytest.mark.asyncio
    async def test_soft_delete(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test soft delete sets deleted_at."""
        order = order_summary_factory()
        await repo.save(order)

        deleted = await repo.soft_delete(order.id)

        assert deleted is True
        assert await repo.get(order.id) is None  # Hidden from normal get
        assert await repo.exists(order.id) is False

    @pytest.mark.asyncio
    async def test_soft_delete_already_deleted(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test soft delete on already deleted record returns False."""
        order = order_summary_factory()
        await repo.save(order)
        await repo.soft_delete(order.id)

        # Try to soft delete again
        deleted = await repo.soft_delete(order.id)
        assert deleted is False

    @pytest.mark.asyncio
    async def test_restore(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test restoring soft-deleted record."""
        order = order_summary_factory()
        await repo.save(order)
        await repo.soft_delete(order.id)

        restored = await repo.restore(order.id)

        assert restored is True
        result = await repo.get(order.id)
        assert result is not None
        assert result.deleted_at is None

    @pytest.mark.asyncio
    async def test_restore_not_deleted(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test restore on non-deleted record returns False."""
        order = order_summary_factory()
        await repo.save(order)

        restored = await repo.restore(order.id)
        assert restored is False

    @pytest.mark.asyncio
    async def test_truncate(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test truncate deletes all records."""
        for i in range(5):
            await repo.save(order_summary_factory(order_number=f"ORD-{i}"))

        count = await repo.truncate()

        assert count == 5
        assert await repo.count() == 0


# ============================================================================
# Query Operations - Basic Find
# ============================================================================


class TestReadModelRepositoryFind:
    """Test find operations."""

    @pytest.mark.asyncio
    async def test_find_all(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test finding all records."""
        for i in range(3):
            await repo.save(order_summary_factory(order_number=f"ORD-{i}"))

        results = await repo.find()

        assert len(results) == 3

    @pytest.mark.asyncio
    async def test_find_empty(self, repo: Any) -> None:
        """Test finding when no records exist."""
        results = await repo.find()
        assert results == []


# ============================================================================
# Query Operations - Filter Operators
# ============================================================================


class TestReadModelRepositoryFilterOperators:
    """Test all filter operators."""

    @pytest.mark.asyncio
    async def test_find_with_eq_filter(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test equality filter (eq)."""
        await repo.save(order_summary_factory(status="pending"))
        await repo.save(order_summary_factory(status="shipped"))
        await repo.save(order_summary_factory(status="pending"))

        results = await repo.find(Query(filters=[Filter.eq("status", "shipped")]))

        assert len(results) == 1
        assert results[0].status == "shipped"

    @pytest.mark.asyncio
    async def test_find_with_ne_filter(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test not-equal filter (ne)."""
        await repo.save(order_summary_factory(status="pending"))
        await repo.save(order_summary_factory(status="shipped"))
        await repo.save(order_summary_factory(status="cancelled"))

        results = await repo.find(Query(filters=[Filter.ne("status", "cancelled")]))

        assert len(results) == 2
        assert all(r.status != "cancelled" for r in results)

    @pytest.mark.asyncio
    async def test_find_with_gt_filter(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test greater-than filter (gt)."""
        await repo.save(order_summary_factory(item_count=1))
        await repo.save(order_summary_factory(item_count=5))
        await repo.save(order_summary_factory(item_count=10))

        results = await repo.find(Query(filters=[Filter.gt("item_count", 3)]))

        assert len(results) == 2
        assert all(r.item_count > 3 for r in results)

    @pytest.mark.asyncio
    async def test_find_with_gte_filter(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test greater-than-or-equal filter (gte)."""
        await repo.save(order_summary_factory(item_count=1))
        await repo.save(order_summary_factory(item_count=5))
        await repo.save(order_summary_factory(item_count=10))

        results = await repo.find(Query(filters=[Filter.gte("item_count", 5)]))

        assert len(results) == 2
        assert all(r.item_count >= 5 for r in results)

    @pytest.mark.asyncio
    async def test_find_with_lt_filter(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test less-than filter (lt)."""
        await repo.save(order_summary_factory(item_count=1))
        await repo.save(order_summary_factory(item_count=5))
        await repo.save(order_summary_factory(item_count=10))

        results = await repo.find(Query(filters=[Filter.lt("item_count", 5)]))

        assert len(results) == 1
        assert all(r.item_count < 5 for r in results)

    @pytest.mark.asyncio
    async def test_find_with_lte_filter(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test less-than-or-equal filter (lte)."""
        await repo.save(order_summary_factory(item_count=1))
        await repo.save(order_summary_factory(item_count=5))
        await repo.save(order_summary_factory(item_count=10))

        results = await repo.find(Query(filters=[Filter.lte("item_count", 5)]))

        assert len(results) == 2
        assert all(r.item_count <= 5 for r in results)

    @pytest.mark.asyncio
    async def test_find_with_in_filter(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test in-list filter (in)."""
        await repo.save(order_summary_factory(status="pending"))
        await repo.save(order_summary_factory(status="shipped"))
        await repo.save(order_summary_factory(status="cancelled"))

        results = await repo.find(Query(filters=[Filter.in_("status", ["pending", "shipped"])]))

        assert len(results) == 2
        assert all(r.status in ["pending", "shipped"] for r in results)

    @pytest.mark.asyncio
    async def test_find_with_not_in_filter(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test not-in-list filter (not_in)."""
        await repo.save(order_summary_factory(status="pending"))
        await repo.save(order_summary_factory(status="shipped"))
        await repo.save(order_summary_factory(status="cancelled"))

        results = await repo.find(
            Query(filters=[Filter.not_in("status", ["cancelled", "refunded"])])
        )

        assert len(results) == 2
        assert all(r.status not in ["cancelled", "refunded"] for r in results)


# ============================================================================
# Query Operations - Ordering
# ============================================================================


class TestReadModelRepositoryOrdering:
    """Test ordering functionality."""

    @pytest.mark.asyncio
    async def test_find_with_ordering_asc(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test ascending order."""
        await repo.save(order_summary_factory(order_number="ORD-003"))
        await repo.save(order_summary_factory(order_number="ORD-001"))
        await repo.save(order_summary_factory(order_number="ORD-002"))

        results = await repo.find(Query(order_by="order_number", order_direction="asc"))

        assert len(results) == 3
        assert results[0].order_number == "ORD-001"
        assert results[1].order_number == "ORD-002"
        assert results[2].order_number == "ORD-003"

    @pytest.mark.asyncio
    async def test_find_with_ordering_desc(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test descending order."""
        await repo.save(order_summary_factory(order_number="ORD-001"))
        await repo.save(order_summary_factory(order_number="ORD-003"))
        await repo.save(order_summary_factory(order_number="ORD-002"))

        results = await repo.find(Query(order_by="order_number", order_direction="desc"))

        assert len(results) == 3
        assert results[0].order_number == "ORD-003"
        assert results[1].order_number == "ORD-002"
        assert results[2].order_number == "ORD-001"

    @pytest.mark.asyncio
    async def test_find_with_numeric_ordering(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test ordering by numeric field."""
        await repo.save(order_summary_factory(item_count=50))
        await repo.save(order_summary_factory(item_count=10))
        await repo.save(order_summary_factory(item_count=100))

        results = await repo.find(Query(order_by="item_count", order_direction="asc"))

        assert len(results) == 3
        assert results[0].item_count == 10
        assert results[1].item_count == 50
        assert results[2].item_count == 100


# ============================================================================
# Query Operations - Pagination
# ============================================================================


class TestReadModelRepositoryPagination:
    """Test pagination functionality."""

    @pytest.mark.asyncio
    async def test_find_with_limit(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test limit clause."""
        for i in range(10):
            await repo.save(order_summary_factory(order_number=f"ORD-{i:03d}"))

        results = await repo.find(Query(limit=3))

        assert len(results) == 3

    @pytest.mark.asyncio
    async def test_find_with_offset(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test offset clause (with limit - required for SQLite compatibility)."""
        for i in range(10):
            await repo.save(order_summary_factory(order_number=f"ORD-{i:03d}"))

        # Note: SQLite requires LIMIT when using OFFSET, so we use a large limit
        results = await repo.find(
            Query(order_by="order_number", order_direction="asc", limit=100, offset=5)
        )

        assert len(results) == 5
        assert results[0].order_number == "ORD-005"

    @pytest.mark.asyncio
    async def test_find_with_limit_and_offset(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test limit and offset together (pagination)."""
        for i in range(10):
            await repo.save(order_summary_factory(order_number=f"ORD-{i:03d}"))

        results = await repo.find(
            Query(
                order_by="order_number",
                order_direction="asc",
                limit=3,
                offset=2,
            )
        )

        assert len(results) == 3
        assert results[0].order_number == "ORD-002"
        assert results[1].order_number == "ORD-003"
        assert results[2].order_number == "ORD-004"


# ============================================================================
# Query Operations - Soft Delete Visibility
# ============================================================================


class TestReadModelRepositorySoftDeleteVisibility:
    """Test soft delete visibility in queries."""

    @pytest.mark.asyncio
    async def test_find_excludes_soft_deleted(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test that find excludes soft-deleted by default."""
        order = order_summary_factory()
        await repo.save(order)
        await repo.soft_delete(order.id)

        results = await repo.find()

        assert len(results) == 0

    @pytest.mark.asyncio
    async def test_find_include_deleted(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test finding soft-deleted records with include_deleted."""
        order = order_summary_factory()
        await repo.save(order)
        await repo.soft_delete(order.id)

        results = await repo.find(Query(include_deleted=True))

        assert len(results) == 1
        assert results[0].id == order.id
        assert results[0].deleted_at is not None

    @pytest.mark.asyncio
    async def test_count_excludes_soft_deleted(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test that count excludes soft-deleted by default."""
        await repo.save(order_summary_factory())
        await repo.save(order_summary_factory())
        order3 = order_summary_factory()
        await repo.save(order3)
        await repo.soft_delete(order3.id)

        count = await repo.count()

        assert count == 2

    @pytest.mark.asyncio
    async def test_count_include_deleted(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test counting soft-deleted records with include_deleted."""
        await repo.save(order_summary_factory())
        await repo.save(order_summary_factory())
        order3 = order_summary_factory()
        await repo.save(order3)
        await repo.soft_delete(order3.id)

        count = await repo.count(Query(include_deleted=True))

        assert count == 3


# ============================================================================
# Query Operations - Count
# ============================================================================


class TestReadModelRepositoryCount:
    """Test count operations."""

    @pytest.mark.asyncio
    async def test_count_all(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test count without filters."""
        for i in range(5):
            await repo.save(order_summary_factory(status="pending" if i % 2 == 0 else "shipped"))

        total = await repo.count()
        assert total == 5

    @pytest.mark.asyncio
    async def test_count_with_filter(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test count with filter."""
        for i in range(5):
            await repo.save(order_summary_factory(status="pending" if i % 2 == 0 else "shipped"))

        pending_count = await repo.count(Query(filters=[Filter.eq("status", "pending")]))

        assert pending_count == 3  # 0, 2, 4 are pending

    @pytest.mark.asyncio
    async def test_count_empty(self, repo: Any) -> None:
        """Test count on empty repository."""
        count = await repo.count()
        assert count == 0


# ============================================================================
# Edge Cases
# ============================================================================


class TestReadModelRepositoryEdgeCases:
    """Test edge cases and error handling."""

    @pytest.mark.asyncio
    async def test_empty_get_many(self, repo: Any) -> None:
        """Test get_many with empty list."""
        results = await repo.get_many([])
        assert results == []

    @pytest.mark.asyncio
    async def test_empty_save_many(self, repo: Any) -> None:
        """Test save_many with empty list (should not raise)."""
        await repo.save_many([])
        assert await repo.count() == 0

    @pytest.mark.asyncio
    async def test_multiple_filters(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test combining multiple filters (AND logic)."""
        await repo.save(order_summary_factory(status="shipped", item_count=10))
        await repo.save(order_summary_factory(status="shipped", item_count=2))
        await repo.save(order_summary_factory(status="pending", item_count=10))

        results = await repo.find(
            Query(
                filters=[
                    Filter.eq("status", "shipped"),
                    Filter.gt("item_count", 5),
                ]
            )
        )

        assert len(results) == 1
        assert results[0].status == "shipped"
        assert results[0].item_count > 5

    @pytest.mark.asyncio
    async def test_save_preserves_id(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test that save preserves the model's ID."""
        specific_id = uuid4()
        order = order_summary_factory(id=specific_id)

        await repo.save(order)
        result = await repo.get(specific_id)

        assert result is not None
        assert result.id == specific_id

    @pytest.mark.asyncio
    async def test_version_increments_on_update(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test that version increments on each update."""
        order = order_summary_factory()

        await repo.save(order)
        result1 = await repo.get(order.id)
        assert result1 is not None
        assert result1.version == 1

        # Update
        order.status = "updated1"
        await repo.save(order)
        result2 = await repo.get(order.id)
        assert result2 is not None
        assert result2.version == 2

        # Update again
        order.status = "updated2"
        await repo.save(order)
        result3 = await repo.get(order.id)
        assert result3 is not None
        assert result3.version == 3

    @pytest.mark.asyncio
    async def test_updated_at_changes_on_save(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test that updated_at timestamp changes on save."""
        import asyncio

        order = order_summary_factory()

        await repo.save(order)
        result1 = await repo.get(order.id)
        assert result1 is not None
        first_updated = result1.updated_at

        # Small delay to ensure timestamp difference
        await asyncio.sleep(0.01)

        order.status = "updated"
        await repo.save(order)
        result2 = await repo.get(order.id)
        assert result2 is not None

        # updated_at should have changed
        assert result2.updated_at > first_updated

    @pytest.mark.asyncio
    async def test_decimal_precision(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test that decimal values maintain reasonable precision."""
        order = order_summary_factory(total_amount=Decimal("1234.56"))

        await repo.save(order)
        result = await repo.get(order.id)

        assert result is not None
        # Check within reasonable precision (accounting for float/decimal conversion)
        assert abs(float(result.total_amount) - 1234.56) < 0.01


# ============================================================================
# PostgreSQL-Specific Tests (marked to skip if not available)
# ============================================================================


@pytest.mark.postgres
class TestPostgreSQLReadModelRepository:
    """Tests specific to PostgreSQL repository."""

    pytestmark = [
        pytest.mark.integration,
        pytest.mark.postgres,
        skip_if_no_postgres_infra,
    ]

    @pytest.mark.asyncio
    async def test_postgresql_basic_save_and_get(
        self,
        postgresql_repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test basic PostgreSQL operations work correctly."""
        order = order_summary_factory(order_number="PG-TEST-001")

        await postgresql_repo.save(order)
        result = await postgresql_repo.get(order.id)

        assert result is not None
        assert result.order_number == "PG-TEST-001"


# ============================================================================
# SQLite-Specific Tests
# ============================================================================


class TestSQLiteReadModelRepository:
    """Tests specific to SQLite repository."""

    @pytest.mark.asyncio
    async def test_sqlite_basic_save_and_get(
        self,
        sqlite_repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test basic SQLite operations work correctly."""
        order = order_summary_factory(order_number="SQLITE-TEST-001")

        await sqlite_repo.save(order)
        result = await sqlite_repo.get(order.id)

        assert result is not None
        assert result.order_number == "SQLITE-TEST-001"

    @pytest.mark.asyncio
    async def test_sqlite_uuid_conversion(
        self,
        sqlite_repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test that UUIDs are properly stored and retrieved in SQLite (as TEXT)."""
        specific_id = uuid4()
        order = order_summary_factory(id=specific_id)

        await sqlite_repo.save(order)
        result = await sqlite_repo.get(specific_id)

        assert result is not None
        assert result.id == specific_id
        assert isinstance(result.id, type(specific_id))


# ============================================================================
# InMemory-Specific Tests
# ============================================================================


class TestInMemoryReadModelRepository:
    """Tests specific to InMemory repository."""

    @pytest.mark.asyncio
    async def test_inmemory_basic_save_and_get(
        self,
        inmemory_repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test basic InMemory operations work correctly."""
        order = order_summary_factory(order_number="MEM-TEST-001")

        await inmemory_repo.save(order)
        result = await inmemory_repo.get(order.id)

        assert result is not None
        assert result.order_number == "MEM-TEST-001"

    @pytest.mark.asyncio
    async def test_inmemory_clear(
        self,
        inmemory_repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test the clear() convenience method."""
        for i in range(3):
            await inmemory_repo.save(order_summary_factory(order_number=f"ORD-{i}"))

        await inmemory_repo.clear()

        assert await inmemory_repo.count() == 0
