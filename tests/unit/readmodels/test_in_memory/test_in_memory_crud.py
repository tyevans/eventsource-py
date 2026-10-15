"""CRUD and lifecycle unit tests for InMemoryReadModelRepository."""

import asyncio
from decimal import Decimal
from uuid import UUID, uuid4

import pytest

from eventsource.adapters.memory.readmodels import InMemoryReadModelRepository
from eventsource.ports.readmodels.exceptions import ReadModelNotFoundError
from tests.unit.readmodels.test_in_memory._fixtures import OrderSummary


def _make_order(
    model_id: UUID | None = None,
    order_number: str = "ORD-001",
    status: str = "pending",
    total_amount: Decimal = Decimal("10"),
) -> OrderSummary:
    return OrderSummary(
        id=model_id or uuid4(),
        order_number=order_number,
        status=status,
        total_amount=total_amount,
    )


class TestInMemoryReadModelCrud:
    """CRUD operation tests for InMemoryReadModelRepository."""

    @pytest.mark.asyncio
    async def test_save_increments_version(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test that save increments version on update."""
        model = _make_order(total_amount=Decimal("99.99"))
        await repo.save(model)
        result1 = await repo.get(model.id)
        assert result1 is not None
        assert result1.version == 1

        model.status = "shipped"
        await repo.save(model)
        result2 = await repo.get(model.id)
        assert result2 is not None
        assert result2.version == 2

    @pytest.mark.asyncio
    async def test_save_updates_timestamp(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test that save updates updated_at timestamp."""
        model = _make_order(total_amount=Decimal("99.99"))
        await repo.save(model)
        result1 = await repo.get(model.id)
        assert result1 is not None
        original_updated_at = result1.updated_at

        await asyncio.sleep(0.01)
        model.status = "shipped"
        await repo.save(model)
        result2 = await repo.get(model.id)
        assert result2 is not None
        assert result2.updated_at > original_updated_at

    @pytest.mark.asyncio
    async def test_get_or_raise_returns_existing_model(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test get_or_raise returns the model when present and not deleted."""
        model = _make_order()
        await repo.save(model)
        result = await repo.get_or_raise(model.id)
        assert result.id == model.id
        assert result.order_number == "ORD-001"

    @pytest.mark.asyncio
    async def test_get_or_raise_raises_when_missing_or_soft_deleted(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test get_or_raise raises ReadModelNotFoundError when missing or soft-deleted."""
        missing_id = uuid4()
        with pytest.raises(ReadModelNotFoundError) as exc_info:
            await repo.get_or_raise(missing_id)
        assert exc_info.value.model_id == missing_id

        model = _make_order()
        await repo.save(model)
        await repo.soft_delete(model.id)
        with pytest.raises(ReadModelNotFoundError) as deleted_exc:
            await repo.get_or_raise(model.id)
        assert deleted_exc.value.model_id == model.id

    @pytest.mark.asyncio
    async def test_clear_removes_all_models_and_returns_count(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test clear() removes all stored models and returns deleted count."""
        await repo.save_many([_make_order(order_number=f"ORD-{i}") for i in range(3)])
        deleted = await repo.clear()
        assert deleted == 3
        assert len(repo) == 0
        assert await repo.clear() == 0

    @pytest.mark.asyncio
    async def test_get_many(self, repo: InMemoryReadModelRepository[OrderSummary]) -> None:
        """Test batch get operation."""
        id1, id2, id3 = uuid4(), uuid4(), uuid4()
        await repo.save(_make_order(id1, "ORD-001", "pending", Decimal("10")))
        await repo.save(_make_order(id2, "ORD-002", "shipped", Decimal("20")))

        results = await repo.get_many([id1, id2, id3])
        assert len(results) == 2
        assert {r.id for r in results} == {id1, id2}

    @pytest.mark.asyncio
    async def test_get_many_empty_list(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test get_many with empty list returns empty."""
        assert await repo.get_many([]) == []

    @pytest.mark.asyncio
    async def test_get_many_excludes_soft_deleted(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test that get_many excludes soft-deleted records."""
        id1, id2 = uuid4(), uuid4()
        await repo.save(_make_order(id1, "ORD-001", "pending", Decimal("10")))
        await repo.save(_make_order(id2, "ORD-002", "shipped", Decimal("20")))
        await repo.soft_delete(id1)

        results = await repo.get_many([id1, id2])
        assert len(results) == 1
        assert results[0].id == id2

    @pytest.mark.asyncio
    async def test_save_many(self, repo: InMemoryReadModelRepository[OrderSummary]) -> None:
        """Test batch save operation."""
        models = [
            _make_order(order_number=f"ORD-{i}", total_amount=Decimal(str(i * 10)))
            for i in range(3)
        ]
        await repo.save_many(models)
        assert await repo.count() == 3

    @pytest.mark.asyncio
    async def test_save_many_empty_list(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test save_many with empty list does nothing."""
        await repo.save_many([])
        assert await repo.count() == 0

    @pytest.mark.asyncio
    async def test_save_many_increments_versions(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test that save_many increments version for existing models."""
        model = _make_order()
        await repo.save(model)
        model.status = "shipped"
        await repo.save_many([model])

        result = await repo.get(model.id)
        assert result is not None
        assert result.version == 2
        assert result.status == "shipped"

    @pytest.mark.asyncio
    async def test_soft_delete_nonexistent(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test soft_delete returns False for missing ID."""
        assert await repo.soft_delete(uuid4()) is False

    @pytest.mark.asyncio
    async def test_soft_delete_already_deleted(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test soft_delete returns False if already deleted."""
        model = _make_order()
        await repo.save(model)
        await repo.soft_delete(model.id)
        assert await repo.soft_delete(model.id) is False

    @pytest.mark.asyncio
    async def test_restore_nonexistent(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test restore returns False for missing ID."""
        assert await repo.restore(uuid4()) is False

    @pytest.mark.asyncio
    async def test_restore_not_deleted(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test restore returns False if not deleted."""
        model = _make_order()
        await repo.save(model)
        assert await repo.restore(model.id) is False

    @pytest.mark.asyncio
    async def test_exists_reflects_soft_delete(
        self, repo: InMemoryReadModelRepository[OrderSummary]
    ) -> None:
        """Test exists() returns False for a soft-deleted record (not covered by the suite)."""
        model = _make_order()
        await repo.save(model)
        await repo.soft_delete(model.id)
        assert await repo.exists(model.id) is False
