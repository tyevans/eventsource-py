"""CRUD and lifecycle unit tests for InMemoryReadModelRepository."""

import asyncio
from decimal import Decimal
from uuid import uuid4

import pytest

from eventsource.adapters.memory.readmodels import InMemoryReadModelRepository
from tests.unit.readmodels.test_in_memory._fixtures import OrderSummary


class TestInMemoryReadModelCrud:
    """CRUD operation tests for InMemoryReadModelRepository."""

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
    async def test_exists_reflects_soft_delete(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test exists() returns False for a soft-deleted record (not covered by the suite)."""
        model_id = uuid4()
        model = OrderSummary(
            id=model_id,
            order_number="ORD-001",
            status="pending",
            total_amount=Decimal("10"),
        )

        await repo.save(model)
        await repo.soft_delete(model_id)
        assert await repo.exists(model_id) is False
