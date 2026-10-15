"""Pagination, ordering, and lifecycle unit tests for InMemoryReadModelRepository."""

from decimal import Decimal
from uuid import UUID, uuid4

import pytest

from eventsource.adapters.memory.readmodels import InMemoryReadModelRepository
from eventsource.ports.readmodels.query import Query
from tests.unit.readmodels.test_in_memory._fixtures import OrderSummary


def _order(num: str, amount: str | int, model_id: UUID | None = None) -> OrderSummary:
    return OrderSummary(
        id=model_id or uuid4(),
        order_number=num,
        status="pending",
        total_amount=Decimal(str(amount)),
    )


class TestInMemoryReadModelPagination:
    """Pagination, ordering, and lifecycle tests for InMemoryReadModelRepository."""

    @pytest.mark.asyncio
    async def test_find_with_ordering_asc(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        await repo.save(_order("ORD-002", 20))
        await repo.save(_order("ORD-001", 10))

        results = await repo.find(Query(order_by="order_number", order_direction="asc"))
        assert results[0].order_number == "ORD-001"
        assert results[1].order_number == "ORD-002"

    @pytest.mark.asyncio
    async def test_find_with_ordering_desc(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        await repo.save(_order("ORD-001", 10))
        await repo.save(_order("ORD-002", 20))

        results = await repo.find(Query(order_by="order_number", order_direction="desc"))
        assert results[0].order_number == "ORD-002"
        assert results[1].order_number == "ORD-001"

    @pytest.mark.asyncio
    async def test_find_with_limit(self, repo: InMemoryReadModelRepository[OrderSummary]) -> None:
        for i in range(10):
            await repo.save(_order(f"ORD-{i:03d}", i))

        results = await repo.find(Query(limit=3))
        assert len(results) == 3

    @pytest.mark.asyncio
    async def test_find_with_offset(self, repo: InMemoryReadModelRepository[OrderSummary]) -> None:
        for i in range(10):
            await repo.save(_order(f"ORD-{i:03d}", i))

        results = await repo.find(Query(order_by="order_number", offset=7))
        assert len(results) == 3

    @pytest.mark.asyncio
    async def test_find_with_pagination(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        for i in range(10):
            await repo.save(_order(f"ORD-{i:03d}", i))

        results = await repo.find(Query(order_by="order_number", limit=3, offset=2))
        assert len(results) == 3
        assert results[0].order_number == "ORD-002"

    @pytest.mark.asyncio
    async def test_find_include_deleted(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        model_id = uuid4()
        await repo.save(_order("ORD-001", 10, model_id=model_id))
        await repo.soft_delete(model_id)

        results = await repo.find(Query())
        assert len(results) == 0

        results = await repo.find(Query(include_deleted=True))
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_count_excludes_soft_deleted(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        for i in range(5):
            await repo.save(_order(f"ORD-{i}", i))

        results = await repo.find()
        await repo.soft_delete(results[0].id)

        assert await repo.count() == 4
        assert await repo.count(Query(include_deleted=True)) == 5

    @pytest.mark.asyncio
    async def test_truncate(self, repo: InMemoryReadModelRepository[OrderSummary]) -> None:
        for i in range(5):
            await repo.save(_order(f"ORD-{i}", i))

        count = await repo.truncate()
        assert count == 5
        assert await repo.count(Query(include_deleted=True)) == 0
        assert len(repo) == 0

    @pytest.mark.asyncio
    async def test_truncate_empty(self, repo: InMemoryReadModelRepository[OrderSummary]) -> None:
        count = await repo.truncate()
        assert count == 0
        assert await repo.count() == 0
        assert len(repo) == 0

    @pytest.mark.asyncio
    async def test_len_includes_soft_deleted(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        model_id = uuid4()
        await repo.save(_order("ORD-001", 10, model_id=model_id))
        await repo.soft_delete(model_id)

        assert len(repo) == 1
        assert await repo.count() == 0

    def test_model_class_property(self, repo: InMemoryReadModelRepository[OrderSummary]) -> None:
        assert repo.model_class is OrderSummary

    @pytest.mark.asyncio
    async def test_tracing_disabled(self) -> None:
        repo = InMemoryReadModelRepository(OrderSummary, enable_tracing=False)
        model_id = uuid4()
        await repo.save(_order("ORD-001", 10, model_id=model_id))
        assert await repo.get(model_id) is not None

    @pytest.mark.asyncio
    async def test_tracing_enabled(self) -> None:
        repo = InMemoryReadModelRepository(OrderSummary, enable_tracing=True)
        model_id = uuid4()
        await repo.save(_order("ORD-001", 10, model_id=model_id))
        assert await repo.get(model_id) is not None
