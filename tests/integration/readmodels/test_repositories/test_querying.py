"""Integration tests for query, filtering, ordering, pagination, and count operations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from eventsource.ports.readmodels import Filter, Query

if TYPE_CHECKING:
    from collections.abc import Callable

    from .conftest import OrderSummary

pytestmark = [
    pytest.mark.integration,
]


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

        assert pending_count == 3

    @pytest.mark.asyncio
    async def test_count_empty(self, repo: Any) -> None:
        """Test count on empty repository."""
        count = await repo.count()
        assert count == 0
