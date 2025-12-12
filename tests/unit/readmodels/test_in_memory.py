"""Unit tests for InMemoryReadModelRepository."""

from decimal import Decimal
from uuid import uuid4

import pytest

from eventsource.readmodels import ReadModel
from eventsource.readmodels.in_memory import InMemoryReadModelRepository
from eventsource.readmodels.query import Filter, Query


class OrderSummary(ReadModel):
    """Test model for repository tests."""

    order_number: str
    status: str
    total_amount: Decimal


@pytest.fixture
def repo() -> InMemoryReadModelRepository[OrderSummary]:
    """Create a fresh in-memory repository for each test."""
    return InMemoryReadModelRepository(OrderSummary, enable_tracing=False)


class TestInMemoryReadModelRepository:
    """Tests for InMemoryReadModelRepository."""

    @pytest.mark.asyncio
    async def test_save_and_get(self, repo: InMemoryReadModelRepository[OrderSummary]) -> None:
        """Test basic save and get operations."""
        model_id = uuid4()
        model = OrderSummary(
            id=model_id,
            order_number="ORD-001",
            status="pending",
            total_amount=Decimal("99.99"),
        )

        await repo.save(model)
        result = await repo.get(model_id)

        assert result is not None
        assert result.id == model_id
        assert result.order_number == "ORD-001"
        assert result.status == "pending"
        assert result.total_amount == Decimal("99.99")

    @pytest.mark.asyncio
    async def test_get_nonexistent(self, repo: InMemoryReadModelRepository[OrderSummary]) -> None:
        """Test get returns None for missing ID."""
        result = await repo.get(uuid4())
        assert result is None

    @pytest.mark.asyncio
    async def test_save_increments_version(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test that save increments version on update."""
        model_id = uuid4()
        model = OrderSummary(
            id=model_id,
            order_number="ORD-001",
            status="pending",
            total_amount=Decimal("99.99"),
        )

        await repo.save(model)
        result1 = await repo.get(model_id)
        assert result1 is not None
        assert result1.version == 1

        # Update the model
        model.status = "shipped"
        await repo.save(model)

        result2 = await repo.get(model_id)
        assert result2 is not None
        assert result2.version == 2

    @pytest.mark.asyncio
    async def test_save_updates_timestamp(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test that save updates updated_at timestamp."""
        model_id = uuid4()
        model = OrderSummary(
            id=model_id,
            order_number="ORD-001",
            status="pending",
            total_amount=Decimal("99.99"),
        )

        await repo.save(model)
        result1 = await repo.get(model_id)
        assert result1 is not None
        original_updated_at = result1.updated_at

        # Wait a tiny bit and update
        import asyncio

        await asyncio.sleep(0.01)

        model.status = "shipped"
        await repo.save(model)

        result2 = await repo.get(model_id)
        assert result2 is not None
        assert result2.updated_at > original_updated_at

    @pytest.mark.asyncio
    async def test_get_many(self, repo: InMemoryReadModelRepository[OrderSummary]) -> None:
        """Test batch get operation."""
        id1, id2, id3 = uuid4(), uuid4(), uuid4()

        await repo.save(
            OrderSummary(
                id=id1,
                order_number="ORD-001",
                status="pending",
                total_amount=Decimal("10"),
            )
        )
        await repo.save(
            OrderSummary(
                id=id2,
                order_number="ORD-002",
                status="shipped",
                total_amount=Decimal("20"),
            )
        )

        results = await repo.get_many([id1, id2, id3])  # id3 doesn't exist

        assert len(results) == 2
        assert {r.id for r in results} == {id1, id2}

    @pytest.mark.asyncio
    async def test_get_many_empty_list(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test get_many with empty list returns empty."""
        results = await repo.get_many([])
        assert results == []

    @pytest.mark.asyncio
    async def test_get_many_excludes_soft_deleted(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test that get_many excludes soft-deleted records."""
        id1, id2 = uuid4(), uuid4()

        await repo.save(
            OrderSummary(
                id=id1,
                order_number="ORD-001",
                status="pending",
                total_amount=Decimal("10"),
            )
        )
        await repo.save(
            OrderSummary(
                id=id2,
                order_number="ORD-002",
                status="shipped",
                total_amount=Decimal("20"),
            )
        )
        await repo.soft_delete(id1)

        results = await repo.get_many([id1, id2])
        assert len(results) == 1
        assert results[0].id == id2

    @pytest.mark.asyncio
    async def test_save_many(self, repo: InMemoryReadModelRepository[OrderSummary]) -> None:
        """Test batch save operation."""
        models = [
            OrderSummary(
                id=uuid4(),
                order_number=f"ORD-{i}",
                status="pending",
                total_amount=Decimal(str(i * 10)),
            )
            for i in range(3)
        ]

        await repo.save_many(models)

        count = await repo.count()
        assert count == 3

    @pytest.mark.asyncio
    async def test_save_many_empty_list(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test save_many with empty list does nothing."""
        await repo.save_many([])
        count = await repo.count()
        assert count == 0

    @pytest.mark.asyncio
    async def test_save_many_increments_versions(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test that save_many increments version for existing models."""
        model_id = uuid4()
        model = OrderSummary(
            id=model_id,
            order_number="ORD-001",
            status="pending",
            total_amount=Decimal("10"),
        )

        await repo.save(model)
        model.status = "shipped"
        await repo.save_many([model])

        result = await repo.get(model_id)
        assert result is not None
        assert result.version == 2
        assert result.status == "shipped"

    @pytest.mark.asyncio
    async def test_delete(self, repo: InMemoryReadModelRepository[OrderSummary]) -> None:
        """Test hard delete."""
        model_id = uuid4()
        model = OrderSummary(
            id=model_id,
            order_number="ORD-001",
            status="pending",
            total_amount=Decimal("10"),
        )

        await repo.save(model)
        deleted = await repo.delete(model_id)

        assert deleted is True
        assert await repo.get(model_id) is None
        assert len(repo) == 0

    @pytest.mark.asyncio
    async def test_delete_nonexistent(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test delete returns False for missing ID."""
        deleted = await repo.delete(uuid4())
        assert deleted is False

    @pytest.mark.asyncio
    async def test_soft_delete(self, repo: InMemoryReadModelRepository[OrderSummary]) -> None:
        """Test soft delete sets deleted_at."""
        model_id = uuid4()
        model = OrderSummary(
            id=model_id,
            order_number="ORD-001",
            status="pending",
            total_amount=Decimal("10"),
        )

        await repo.save(model)
        result = await repo.soft_delete(model_id)

        assert result is True
        assert await repo.get(model_id) is None  # Hidden from normal get
        assert len(repo) == 1  # Still exists in storage

    @pytest.mark.asyncio
    async def test_soft_delete_nonexistent(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test soft_delete returns False for missing ID."""
        result = await repo.soft_delete(uuid4())
        assert result is False

    @pytest.mark.asyncio
    async def test_soft_delete_already_deleted(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test soft_delete returns False if already deleted."""
        model_id = uuid4()
        model = OrderSummary(
            id=model_id,
            order_number="ORD-001",
            status="pending",
            total_amount=Decimal("10"),
        )

        await repo.save(model)
        await repo.soft_delete(model_id)
        result = await repo.soft_delete(model_id)

        assert result is False

    @pytest.mark.asyncio
    async def test_restore(self, repo: InMemoryReadModelRepository[OrderSummary]) -> None:
        """Test restore clears deleted_at."""
        model_id = uuid4()
        model = OrderSummary(
            id=model_id,
            order_number="ORD-001",
            status="pending",
            total_amount=Decimal("10"),
        )

        await repo.save(model)
        await repo.soft_delete(model_id)
        result = await repo.restore(model_id)

        assert result is True
        restored = await repo.get(model_id)
        assert restored is not None
        assert restored.deleted_at is None

    @pytest.mark.asyncio
    async def test_restore_nonexistent(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test restore returns False for missing ID."""
        result = await repo.restore(uuid4())
        assert result is False

    @pytest.mark.asyncio
    async def test_restore_not_deleted(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test restore returns False if not deleted."""
        model_id = uuid4()
        model = OrderSummary(
            id=model_id,
            order_number="ORD-001",
            status="pending",
            total_amount=Decimal("10"),
        )

        await repo.save(model)
        result = await repo.restore(model_id)

        assert result is False

    @pytest.mark.asyncio
    async def test_exists(self, repo: InMemoryReadModelRepository[OrderSummary]) -> None:
        """Test exists check."""
        model_id = uuid4()
        model = OrderSummary(
            id=model_id,
            order_number="ORD-001",
            status="pending",
            total_amount=Decimal("10"),
        )

        assert await repo.exists(model_id) is False

        await repo.save(model)
        assert await repo.exists(model_id) is True

        await repo.soft_delete(model_id)
        assert await repo.exists(model_id) is False

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
    async def test_find_with_filter_eq(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test find with equality filter."""
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

        results = await repo.find(Query(filters=[Filter.eq("status", "shipped")]))

        assert len(results) == 1
        assert results[0].status == "shipped"

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
    async def test_find_excludes_soft_deleted(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test find excludes soft-deleted by default."""
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

        results = await repo.find(Query())
        assert len(results) == 0

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
    async def test_count(self, repo: InMemoryReadModelRepository[OrderSummary]) -> None:
        """Test count operation."""
        for i in range(5):
            await repo.save(
                OrderSummary(
                    id=uuid4(),
                    order_number=f"ORD-{i}",
                    status="pending",
                    total_amount=Decimal(str(i)),
                )
            )

        count = await repo.count()
        assert count == 5

    @pytest.mark.asyncio
    async def test_count_with_filter(self, repo: InMemoryReadModelRepository[OrderSummary]) -> None:
        """Test count with filter."""
        for i in range(5):
            await repo.save(
                OrderSummary(
                    id=uuid4(),
                    order_number=f"ORD-{i}",
                    status="pending" if i % 2 == 0 else "shipped",
                    total_amount=Decimal(str(i)),
                )
            )

        count = await repo.count(Query(filters=[Filter.eq("status", "pending")]))
        assert count == 3  # i=0,2,4

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
