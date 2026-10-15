"""Integration tests for query, filtering, ordering, pagination, and count operations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from eventsource.ports.readmodels import Filter, Query

if TYPE_CHECKING:
    from collections.abc import Callable

    from .conftest import OrderSummary

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _seed_many(
    repo: Any, factory: Callable[..., OrderSummary], field: str, values: list[Any]
) -> None:
    for v in values:
        await repo.save(factory(**{field: v}))


class TestReadModelRepositoryFind:
    async def test_find_all(
        self, repo: Any, order_summary_factory: Callable[..., OrderSummary]
    ) -> None:
        await _seed_many(
            repo, order_summary_factory, "order_number", [f"ORD-{i}" for i in range(3)]
        )
        assert len(await repo.find()) == 3

    async def test_find_empty(self, repo: Any) -> None:
        assert await repo.find() == []


class TestReadModelRepositoryFilterOperators:
    async def test_find_with_eq_filter(
        self, repo: Any, order_summary_factory: Callable[..., OrderSummary]
    ) -> None:
        await _seed_many(repo, order_summary_factory, "status", ["pending", "shipped", "pending"])
        results = await repo.find(Query(filters=[Filter.eq("status", "shipped")]))
        assert len(results) == 1 and results[0].status == "shipped"

    async def test_find_with_ne_filter(
        self, repo: Any, order_summary_factory: Callable[..., OrderSummary]
    ) -> None:
        await _seed_many(repo, order_summary_factory, "status", ["pending", "shipped", "cancelled"])
        results = await repo.find(Query(filters=[Filter.ne("status", "cancelled")]))
        assert len(results) == 2 and all(r.status != "cancelled" for r in results)

    async def test_find_with_gt_filter(
        self, repo: Any, order_summary_factory: Callable[..., OrderSummary]
    ) -> None:
        await _seed_many(repo, order_summary_factory, "item_count", [1, 5, 10])
        results = await repo.find(Query(filters=[Filter.gt("item_count", 3)]))
        assert len(results) == 2 and all(r.item_count > 3 for r in results)

    async def test_find_with_gte_filter(
        self, repo: Any, order_summary_factory: Callable[..., OrderSummary]
    ) -> None:
        await _seed_many(repo, order_summary_factory, "item_count", [1, 5, 10])
        results = await repo.find(Query(filters=[Filter.gte("item_count", 5)]))
        assert len(results) == 2 and all(r.item_count >= 5 for r in results)

    async def test_find_with_lt_filter(
        self, repo: Any, order_summary_factory: Callable[..., OrderSummary]
    ) -> None:
        await _seed_many(repo, order_summary_factory, "item_count", [1, 5, 10])
        results = await repo.find(Query(filters=[Filter.lt("item_count", 5)]))
        assert len(results) == 1 and all(r.item_count < 5 for r in results)

    async def test_find_with_lte_filter(
        self, repo: Any, order_summary_factory: Callable[..., OrderSummary]
    ) -> None:
        await _seed_many(repo, order_summary_factory, "item_count", [1, 5, 10])
        results = await repo.find(Query(filters=[Filter.lte("item_count", 5)]))
        assert len(results) == 2 and all(r.item_count <= 5 for r in results)

    async def test_find_with_in_filter(
        self, repo: Any, order_summary_factory: Callable[..., OrderSummary]
    ) -> None:
        await _seed_many(repo, order_summary_factory, "status", ["pending", "shipped", "cancelled"])
        results = await repo.find(Query(filters=[Filter.in_("status", ["pending", "shipped"])]))
        assert len(results) == 2 and all(r.status in ["pending", "shipped"] for r in results)

    async def test_find_with_not_in_filter(
        self, repo: Any, order_summary_factory: Callable[..., OrderSummary]
    ) -> None:
        await _seed_many(repo, order_summary_factory, "status", ["pending", "shipped", "cancelled"])
        results = await repo.find(
            Query(filters=[Filter.not_in("status", ["cancelled", "refunded"])])
        )
        assert len(results) == 2 and all(r.status not in ["cancelled", "refunded"] for r in results)


class TestReadModelRepositoryOrdering:
    async def test_find_with_ordering_asc(
        self, repo: Any, order_summary_factory: Callable[..., OrderSummary]
    ) -> None:
        await _seed_many(
            repo, order_summary_factory, "order_number", ["ORD-003", "ORD-001", "ORD-002"]
        )
        results = await repo.find(Query(order_by="order_number", order_direction="asc"))
        assert [r.order_number for r in results] == ["ORD-001", "ORD-002", "ORD-003"]

    async def test_find_with_ordering_desc(
        self, repo: Any, order_summary_factory: Callable[..., OrderSummary]
    ) -> None:
        await _seed_many(
            repo, order_summary_factory, "order_number", ["ORD-001", "ORD-003", "ORD-002"]
        )
        results = await repo.find(Query(order_by="order_number", order_direction="desc"))
        assert [r.order_number for r in results] == ["ORD-003", "ORD-002", "ORD-001"]

    async def test_find_with_numeric_ordering(
        self, repo: Any, order_summary_factory: Callable[..., OrderSummary]
    ) -> None:
        await _seed_many(repo, order_summary_factory, "item_count", [50, 10, 100])
        results = await repo.find(Query(order_by="item_count", order_direction="asc"))
        assert [r.item_count for r in results] == [10, 50, 100]


class TestReadModelRepositoryPagination:
    async def test_find_with_limit(
        self, repo: Any, order_summary_factory: Callable[..., OrderSummary]
    ) -> None:
        await _seed_many(
            repo, order_summary_factory, "order_number", [f"ORD-{i:03d}" for i in range(10)]
        )
        assert len(await repo.find(Query(limit=3))) == 3

    async def test_find_with_offset(
        self, repo: Any, order_summary_factory: Callable[..., OrderSummary]
    ) -> None:
        await _seed_many(
            repo, order_summary_factory, "order_number", [f"ORD-{i:03d}" for i in range(10)]
        )
        results = await repo.find(
            Query(order_by="order_number", order_direction="asc", limit=100, offset=5)
        )
        assert len(results) == 5 and results[0].order_number == "ORD-005"

    async def test_find_with_limit_and_offset(
        self, repo: Any, order_summary_factory: Callable[..., OrderSummary]
    ) -> None:
        await _seed_many(
            repo, order_summary_factory, "order_number", [f"ORD-{i:03d}" for i in range(10)]
        )
        results = await repo.find(
            Query(order_by="order_number", order_direction="asc", limit=3, offset=2)
        )
        assert [r.order_number for r in results] == ["ORD-002", "ORD-003", "ORD-004"]


class TestReadModelRepositoryCount:
    async def test_count_all(
        self, repo: Any, order_summary_factory: Callable[..., OrderSummary]
    ) -> None:
        await _seed_many(
            repo,
            order_summary_factory,
            "status",
            ["pending" if i % 2 == 0 else "shipped" for i in range(5)],
        )
        assert await repo.count() == 5

    async def test_count_with_filter(
        self, repo: Any, order_summary_factory: Callable[..., OrderSummary]
    ) -> None:
        await _seed_many(
            repo,
            order_summary_factory,
            "status",
            ["pending" if i % 2 == 0 else "shipped" for i in range(5)],
        )
        assert await repo.count(Query(filters=[Filter.eq("status", "pending")])) == 3

    async def test_count_empty(self, repo: Any) -> None:
        assert await repo.count() == 0
