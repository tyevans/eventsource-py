"""Pagination, ordering, and lifecycle unit tests for InMemoryReadModelRepository."""

from decimal import Decimal
from uuid import uuid4

import pytest

from eventsource.adapters.memory.readmodels import InMemoryReadModelRepository
from eventsource.ports.readmodels.query import Query
from tests.unit.readmodels.test_in_memory._fixtures import OrderSummary


class TestInMemoryReadModelPagination:
    """Pagination, ordering, and lifecycle tests for InMemoryReadModelRepository."""

    @pytest.mark.asyncio
    async def test_find_with_ordering_asc(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test find with ascending ordering."""
        await repo.save(
            OrderSummary(
                id=uuid4(),
                order_number="ORD-002",
                status="pending",
                total_amount=Decimal("20"),
            )
        )
        await repo.save(
            OrderSummary(
                id=uuid4(),
                order_number="ORD-001",
                status="pending",
                total_amount=Decimal("10"),
            )
        )

        results = await repo.find(Query(order_by="order_number", order_direction="asc"))

        assert results[0].order_number == "ORD-001"
        assert results[1].order_number == "ORD-002"

    @pytest.mark.asyncio
    async def test_find_with_ordering_desc(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test find with descending ordering."""
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
                status="pending",
                total_amount=Decimal("20"),
            )
        )

        results = await repo.find(Query(order_by="order_number", order_direction="desc"))

        assert results[0].order_number == "ORD-002"
        assert results[1].order_number == "ORD-001"

    @pytest.mark.asyncio
    async def test_find_with_limit(self, repo: InMemoryReadModelRepository[OrderSummary]) -> None:
        """Test find with limit."""
        for i in range(10):
            await repo.save(
                OrderSummary(
                    id=uuid4(),
                    order_number=f"ORD-{i:03d}",
                    status="pending",
                    total_amount=Decimal(str(i)),
                )
            )

        results = await repo.find(Query(limit=3))
        assert len(results) == 3

    @pytest.mark.asyncio
    async def test_find_with_offset(self, repo: InMemoryReadModelRepository[OrderSummary]) -> None:
        """Test find with offset."""
        for i in range(10):
            await repo.save(
                OrderSummary(
                    id=uuid4(),
                    order_number=f"ORD-{i:03d}",
                    status="pending",
                    total_amount=Decimal(str(i)),
                )
            )

        results = await repo.find(Query(order_by="order_number", offset=7))
        assert len(results) == 3

    @pytest.mark.asyncio
    async def test_find_with_pagination(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test find with pagination."""
        for i in range(10):
            await repo.save(
                OrderSummary(
                    id=uuid4(),
                    order_number=f"ORD-{i:03d}",
                    status="pending",
                    total_amount=Decimal(str(i)),
                )
            )

        results = await repo.find(Query(order_by="order_number", limit=3, offset=2))

        assert len(results) == 3
        assert results[0].order_number == "ORD-002"

    @pytest.mark.asyncio
    async def test_find_include_deleted(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test find with include_deleted flag."""
        model_id = uuid4()
        await repo.save(
            OrderSummary(
                id=model_id,
                order_number="ORD-001",
                status="pending",
                total_amount=Decimal("10"),
            )
        )
        await repo.soft_delete(model_id)

        # Without flag - should not find deleted
        results = await repo.find(Query())
        assert len(results) == 0

        # With flag - should find deleted
        results = await repo.find(Query(include_deleted=True))
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_count_excludes_soft_deleted(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test count excludes soft-deleted by default."""
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
        await repo.soft_delete(results[0].id)

        assert await repo.count() == 4
        assert await repo.count(Query(include_deleted=True)) == 5

    @pytest.mark.asyncio
    async def test_truncate(self, repo: InMemoryReadModelRepository[OrderSummary]) -> None:
        """Test truncate operation."""
        for i in range(5):
            await repo.save(
                OrderSummary(
                    id=uuid4(),
                    order_number=f"ORD-{i}",
                    status="pending",
                    total_amount=Decimal(str(i)),
                )
            )

        count = await repo.truncate()

        assert count == 5
        assert await repo.count(Query(include_deleted=True)) == 0
        assert len(repo) == 0

    @pytest.mark.asyncio
    async def test_clear_alias(self, repo: InMemoryReadModelRepository[OrderSummary]) -> None:
        """Test clear() is alias for truncate()."""
        for i in range(3):
            await repo.save(
                OrderSummary(
                    id=uuid4(),
                    order_number=f"ORD-{i}",
                    status="pending",
                    total_amount=Decimal(str(i)),
                )
            )

        await repo.clear()

        assert await repo.count() == 0
        assert len(repo) == 0

    @pytest.mark.asyncio
    async def test_len_includes_soft_deleted(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test that len() includes soft-deleted records."""
        model_id = uuid4()
        await repo.save(
            OrderSummary(
                id=model_id,
                order_number="ORD-001",
                status="pending",
                total_amount=Decimal("10"),
            )
        )
        await repo.soft_delete(model_id)

        assert len(repo) == 1
        assert await repo.count() == 0

    def test_model_class_property(self, repo: InMemoryReadModelRepository[OrderSummary]) -> None:
        """Test model_class property."""
        assert repo.model_class is OrderSummary

    @pytest.mark.asyncio
    async def test_tracing_disabled(self) -> None:
        """Test repository works with tracing disabled."""
        repo = InMemoryReadModelRepository(OrderSummary, enable_tracing=False)

        model_id = uuid4()
        await repo.save(
            OrderSummary(
                id=model_id,
                order_number="ORD-001",
                status="pending",
                total_amount=Decimal("10"),
            )
        )

        result = await repo.get(model_id)
        assert result is not None

    @pytest.mark.asyncio
    async def test_tracing_enabled(self) -> None:
        """Test repository works with tracing enabled."""
        repo = InMemoryReadModelRepository(OrderSummary, enable_tracing=True)

        model_id = uuid4()
        await repo.save(
            OrderSummary(
                id=model_id,
                order_number="ORD-001",
                status="pending",
                total_amount=Decimal("10"),
            )
        )

        result = await repo.get(model_id)
        assert result is not None
