"""Query and filtering unit tests for InMemoryReadModelRepository."""

from decimal import Decimal
from uuid import uuid4

import pytest

from eventsource.adapters.memory.readmodels import InMemoryReadModelRepository
from eventsource.ports.readmodels.query import Filter, Query
from tests.unit.readmodels.test_in_memory._fixtures import OrderSummary


class TestInMemoryReadModelQuery:
    """Query and filtering tests for InMemoryReadModelRepository."""

    @pytest.mark.asyncio
    async def test_find_all(self, repo: InMemoryReadModelRepository[OrderSummary]) -> None:
        """Test find with no query returns all non-deleted records."""
        for i in range(5):
            await repo.save(
                OrderSummary(
                    id=uuid4(),
                    order_number=f"ORD-{i}",
                    status="pending",
                    total_amount=Decimal(str(i)),
                )
            )

        results = await repo.find()
        assert len(results) == 5

    @pytest.mark.asyncio
    async def test_find_with_filter_ne(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test find with not-equal filter."""
        await repo.save(
            OrderSummary(
                id=uuid4(),
                order_number="ORD-001",
                status="pending",
                total_amount=Decimal("10"),
            )
        )
        await repo.save(
            OrderSummary(
                id=uuid4(),
                order_number="ORD-002",
                status="shipped",
                total_amount=Decimal("20"),
            )
        )

        results = await repo.find(Query(filters=[Filter.ne("status", "pending")]))

        assert len(results) == 1
        assert results[0].status == "shipped"

    @pytest.mark.asyncio
    async def test_find_with_filter_gt(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test find with greater-than filter."""
        await repo.save(
            OrderSummary(
                id=uuid4(),
                order_number="ORD-001",
                status="pending",
                total_amount=Decimal("10"),
            )
        )
        await repo.save(
            OrderSummary(
                id=uuid4(),
                order_number="ORD-002",
                status="shipped",
                total_amount=Decimal("100"),
            )
        )

        results = await repo.find(Query(filters=[Filter.gt("total_amount", Decimal("50"))]))

        assert len(results) == 1
        assert results[0].total_amount == Decimal("100")

    @pytest.mark.asyncio
    async def test_find_with_filter_gte(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test find with greater-than-or-equal filter."""
        await repo.save(
            OrderSummary(
                id=uuid4(),
                order_number="ORD-001",
                status="pending",
                total_amount=Decimal("50"),
            )
        )
        await repo.save(
            OrderSummary(
                id=uuid4(),
                order_number="ORD-002",
                status="shipped",
                total_amount=Decimal("100"),
            )
        )

        results = await repo.find(Query(filters=[Filter.gte("total_amount", Decimal("50"))]))

        assert len(results) == 2

    @pytest.mark.asyncio
    async def test_find_with_filter_lt(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test find with less-than filter."""
        await repo.save(
            OrderSummary(
                id=uuid4(),
                order_number="ORD-001",
                status="pending",
                total_amount=Decimal("10"),
            )
        )
        await repo.save(
            OrderSummary(
                id=uuid4(),
                order_number="ORD-002",
                status="shipped",
                total_amount=Decimal("100"),
            )
        )

        results = await repo.find(Query(filters=[Filter.lt("total_amount", Decimal("50"))]))

        assert len(results) == 1
        assert results[0].total_amount == Decimal("10")

    @pytest.mark.asyncio
    async def test_find_with_filter_lte(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test find with less-than-or-equal filter."""
        await repo.save(
            OrderSummary(
                id=uuid4(),
                order_number="ORD-001",
                status="pending",
                total_amount=Decimal("50"),
            )
        )
        await repo.save(
            OrderSummary(
                id=uuid4(),
                order_number="ORD-002",
                status="shipped",
                total_amount=Decimal("100"),
            )
        )

        results = await repo.find(Query(filters=[Filter.lte("total_amount", Decimal("50"))]))

        assert len(results) == 1
        assert results[0].total_amount == Decimal("50")

    @pytest.mark.asyncio
    async def test_find_with_filter_in(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test find with in-list filter."""
        await repo.save(
            OrderSummary(
                id=uuid4(),
                order_number="ORD-001",
                status="pending",
                total_amount=Decimal("10"),
            )
        )
        await repo.save(
            OrderSummary(
                id=uuid4(),
                order_number="ORD-002",
                status="shipped",
                total_amount=Decimal("20"),
            )
        )
        await repo.save(
            OrderSummary(
                id=uuid4(),
                order_number="ORD-003",
                status="cancelled",
                total_amount=Decimal("30"),
            )
        )

        results = await repo.find(Query(filters=[Filter.in_("status", ["pending", "shipped"])]))

        assert len(results) == 2
        assert {r.status for r in results} == {"pending", "shipped"}

    @pytest.mark.asyncio
    async def test_find_with_filter_not_in(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test find with not-in-list filter."""
        await repo.save(
            OrderSummary(
                id=uuid4(),
                order_number="ORD-001",
                status="pending",
                total_amount=Decimal("10"),
            )
        )
        await repo.save(
            OrderSummary(
                id=uuid4(),
                order_number="ORD-002",
                status="shipped",
                total_amount=Decimal("20"),
            )
        )
        await repo.save(
            OrderSummary(
                id=uuid4(),
                order_number="ORD-003",
                status="cancelled",
                total_amount=Decimal("30"),
            )
        )

        results = await repo.find(
            Query(filters=[Filter.not_in("status", ["cancelled", "refunded"])])
        )

        assert len(results) == 2
        assert {r.status for r in results} == {"pending", "shipped"}

    @pytest.mark.asyncio
    async def test_find_with_multiple_filters(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test find with multiple filters (AND logic)."""
        await repo.save(
            OrderSummary(
                id=uuid4(),
                order_number="ORD-001",
                status="shipped",
                total_amount=Decimal("10"),
            )
        )
        await repo.save(
            OrderSummary(
                id=uuid4(),
                order_number="ORD-002",
                status="shipped",
                total_amount=Decimal("100"),
            )
        )
        await repo.save(
            OrderSummary(
                id=uuid4(),
                order_number="ORD-003",
                status="pending",
                total_amount=Decimal("100"),
            )
        )

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
