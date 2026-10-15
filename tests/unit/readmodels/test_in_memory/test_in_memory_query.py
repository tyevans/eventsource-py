"""Query and filtering unit tests for InMemoryReadModelRepository."""

from decimal import Decimal
from uuid import uuid4

import pytest

from eventsource.adapters.memory.readmodels import InMemoryReadModelRepository
from eventsource.ports.readmodels.query import Filter, Query
from tests.unit.readmodels.test_in_memory._fixtures import OrderSummary


def _order(number: str, status: str, amount: str) -> OrderSummary:
    return OrderSummary(
        id=uuid4(),
        order_number=number,
        status=status,
        total_amount=Decimal(amount),
    )


class TestInMemoryReadModelQuery:
    @pytest.mark.asyncio
    async def test_find_all(self, repo: InMemoryReadModelRepository[OrderSummary]) -> None:
        for i in range(5):
            await repo.save(_order(f"ORD-{i}", "pending", str(i)))

        results = await repo.find()
        assert len(results) == 5

    @pytest.mark.asyncio
    async def test_find_with_filter_ne(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        await repo.save(_order("ORD-001", "pending", "10"))
        await repo.save(_order("ORD-002", "shipped", "20"))

        results = await repo.find(Query(filters=[Filter.ne("status", "pending")]))
        assert len(results) == 1
        assert results[0].status == "shipped"

    @pytest.mark.asyncio
    async def test_find_with_filter_gt(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        await repo.save(_order("ORD-001", "pending", "10"))
        await repo.save(_order("ORD-002", "shipped", "100"))

        results = await repo.find(Query(filters=[Filter.gt("total_amount", Decimal("50"))]))
        assert len(results) == 1
        assert results[0].total_amount == Decimal("100")

    @pytest.mark.asyncio
    async def test_find_with_filter_gte(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        await repo.save(_order("ORD-001", "pending", "50"))
        await repo.save(_order("ORD-002", "shipped", "100"))

        results = await repo.find(Query(filters=[Filter.gte("total_amount", Decimal("50"))]))
        assert len(results) == 2

    @pytest.mark.asyncio
    async def test_find_with_filter_lt(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        await repo.save(_order("ORD-001", "pending", "10"))
        await repo.save(_order("ORD-002", "shipped", "100"))

        results = await repo.find(Query(filters=[Filter.lt("total_amount", Decimal("50"))]))
        assert len(results) == 1
        assert results[0].total_amount == Decimal("10")

    @pytest.mark.asyncio
    async def test_find_with_filter_lte(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        await repo.save(_order("ORD-001", "pending", "50"))
        await repo.save(_order("ORD-002", "shipped", "100"))

        results = await repo.find(Query(filters=[Filter.lte("total_amount", Decimal("50"))]))
        assert len(results) == 1
        assert results[0].total_amount == Decimal("50")

    @pytest.mark.asyncio
    async def test_find_with_filter_in(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        await repo.save(_order("ORD-001", "pending", "10"))
        await repo.save(_order("ORD-002", "shipped", "20"))
        await repo.save(_order("ORD-003", "cancelled", "30"))

        results = await repo.find(Query(filters=[Filter.in_("status", ["pending", "shipped"])]))
        assert len(results) == 2
        assert {r.status for r in results} == {"pending", "shipped"}

    @pytest.mark.asyncio
    async def test_find_with_filter_not_in(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        await repo.save(_order("ORD-001", "pending", "10"))
        await repo.save(_order("ORD-002", "shipped", "20"))
        await repo.save(_order("ORD-003", "cancelled", "30"))

        results = await repo.find(
            Query(filters=[Filter.not_in("status", ["cancelled", "refunded"])])
        )
        assert len(results) == 2
        assert {r.status for r in results} == {"pending", "shipped"}

    @pytest.mark.asyncio
    async def test_find_with_multiple_filters(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        await repo.save(_order("ORD-001", "shipped", "10"))
        await repo.save(_order("ORD-002", "shipped", "100"))
        await repo.save(_order("ORD-003", "pending", "100"))

        results = await repo.find(
            Query(
                filters=[
                    Filter.eq("status", "shipped"),
                    Filter.gt("total_amount", Decimal("50")),
                ]
            )
        )
        assert len(results) == 1
        assert results[0].order_number == "ORD-002"

    @pytest.mark.asyncio
    async def test_find_count_and_find_one_with_eq_filter_kwargs(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        await repo.save(_order("ORD-001", "shipped", "10"))
        await repo.save(_order("ORD-002", "shipped", "100"))
        await repo.save(_order("ORD-003", "pending", "100"))

        shipped = await repo.find(status="shipped")
        assert {m.order_number for m in shipped} == {"ORD-001", "ORD-002"}
        assert await repo.count(status="shipped") == 2

        combined = await repo.find(
            Query(filters=[Filter.gt("total_amount", Decimal("50"))]),
            status="shipped",
        )
        assert [m.order_number for m in combined] == ["ORD-002"]
        assert (
            await repo.count(
                Query(filters=[Filter.gt("total_amount", Decimal("50"))]),
                status="shipped",
            )
            == 1
        )

    @pytest.mark.asyncio
    async def test_find_one_returns_first_or_none_and_respects_order_and_offset(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        assert await repo.find_one() is None

        await repo.save(_order("ORD-002", "shipped", "20"))
        await repo.save(_order("ORD-001", "shipped", "10"))
        await repo.save(_order("ORD-003", "pending", "30"))

        first_asc = await repo.find_one(
            Query(order_by="order_number", order_direction="asc"), status="shipped"
        )
        assert first_asc is not None and first_asc.order_number == "ORD-001"

        second_asc = await repo.find_one(
            Query(order_by="order_number", order_direction="asc", offset=1), status="shipped"
        )
        assert second_asc is not None and second_asc.order_number == "ORD-002"

        assert await repo.find_one(status="cancelled") is None

    @pytest.mark.asyncio
    async def test_get_many_and_save_many_accept_set_tuple_and_generator(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        orders = [_order(f"ORD-{i}", "pending", str(i * 10)) for i in range(3)]
        await repo.save_many(o for o in orders)
        assert await repo.count() == 3

        by_set = await repo.get_many({orders[0].id, orders[1].id})
        assert {m.id for m in by_set} == {orders[0].id, orders[1].id}

        by_tuple = await repo.get_many((orders[1].id, orders[2].id))
        assert {m.id for m in by_tuple} == {orders[1].id, orders[2].id}

        by_gen = await repo.get_many(o.id for o in orders)
        assert len(by_gen) == 3
