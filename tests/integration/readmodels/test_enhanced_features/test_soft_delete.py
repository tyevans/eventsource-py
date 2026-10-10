"""Integration tests for soft delete helper methods."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest

from eventsource.ports.readmodels import Filter, Query

if TYPE_CHECKING:
    from .conftest import EnhancedTestModel

pytestmark = [
    pytest.mark.integration,
]


class TestSoftDeleteHelpers:
    """Tests for soft delete helper methods."""

    @pytest.mark.asyncio
    async def test_get_deleted_returns_deleted_record(
        self,
        enhanced_repo: Any,
        enhanced_model_factory: Callable[..., EnhancedTestModel],
    ) -> None:
        """Test get_deleted returns soft-deleted records."""
        model_id = uuid4()
        model = enhanced_model_factory(id=model_id, name="will_delete")
        await enhanced_repo.save(model)
        await enhanced_repo.soft_delete(model_id)

        deleted = await enhanced_repo.get_deleted(model_id)

        assert deleted is not None
        assert deleted.id == model_id
        assert deleted.name == "will_delete"
        assert deleted.deleted_at is not None

    @pytest.mark.asyncio
    async def test_get_deleted_returns_none_for_active(
        self,
        enhanced_repo: Any,
        enhanced_model_factory: Callable[..., EnhancedTestModel],
    ) -> None:
        """Test get_deleted returns None for active records."""
        model_id = uuid4()
        model = enhanced_model_factory(id=model_id, name="active")
        await enhanced_repo.save(model)

        deleted = await enhanced_repo.get_deleted(model_id)

        assert deleted is None

    @pytest.mark.asyncio
    async def test_get_deleted_returns_none_for_nonexistent(
        self,
        enhanced_repo: Any,
    ) -> None:
        """Test get_deleted returns None for nonexistent records."""
        deleted = await enhanced_repo.get_deleted(uuid4())
        assert deleted is None

    @pytest.mark.asyncio
    async def test_find_deleted_returns_only_deleted(
        self,
        enhanced_repo: Any,
        enhanced_model_factory: Callable[..., EnhancedTestModel],
    ) -> None:
        """Test find_deleted returns only soft-deleted records."""
        active_ids = [uuid4() for _ in range(2)]
        deleted_ids = [uuid4() for _ in range(3)]

        for i, model_id in enumerate(active_ids):
            await enhanced_repo.save(enhanced_model_factory(id=model_id, name=f"active_{i}"))

        for i, model_id in enumerate(deleted_ids):
            await enhanced_repo.save(enhanced_model_factory(id=model_id, name=f"deleted_{i}"))
            await enhanced_repo.soft_delete(model_id)

        results = await enhanced_repo.find_deleted()

        assert len(results) == 3
        result_ids = {r.id for r in results}
        assert result_ids == set(deleted_ids)

    @pytest.mark.asyncio
    async def test_find_deleted_with_filter(
        self,
        enhanced_repo: Any,
        enhanced_model_factory: Callable[..., EnhancedTestModel],
    ) -> None:
        """Test find_deleted with filters."""
        model1_id = uuid4()
        model2_id = uuid4()

        await enhanced_repo.save(enhanced_model_factory(id=model1_id, name="del1", category="A"))
        await enhanced_repo.save(enhanced_model_factory(id=model2_id, name="del2", category="B"))
        await enhanced_repo.soft_delete(model1_id)
        await enhanced_repo.soft_delete(model2_id)

        results = await enhanced_repo.find_deleted(Query(filters=[Filter.eq("category", "A")]))

        assert len(results) == 1
        assert results[0].category == "A"

    @pytest.mark.asyncio
    async def test_find_deleted_with_pagination(
        self,
        enhanced_repo: Any,
        enhanced_model_factory: Callable[..., EnhancedTestModel],
    ) -> None:
        """Test find_deleted with pagination."""
        for i in range(5):
            model_id = uuid4()
            await enhanced_repo.save(enhanced_model_factory(id=model_id, name=f"del_{i}"))
            await enhanced_repo.soft_delete(model_id)

        results = await enhanced_repo.find_deleted(Query(limit=2))

        assert len(results) == 2

    @pytest.mark.asyncio
    async def test_find_deleted_empty_when_none_deleted(
        self,
        enhanced_repo: Any,
        enhanced_model_factory: Callable[..., EnhancedTestModel],
    ) -> None:
        """Test find_deleted returns empty list when no deletions."""
        await enhanced_repo.save(enhanced_model_factory(name="active"))

        results = await enhanced_repo.find_deleted()

        assert results == []

    @pytest.mark.asyncio
    async def test_find_deleted_with_ordering(
        self,
        enhanced_repo: Any,
        enhanced_model_factory: Callable[..., EnhancedTestModel],
    ) -> None:
        """Test find_deleted with ordering."""
        id1 = uuid4()
        id2 = uuid4()
        id3 = uuid4()

        await enhanced_repo.save(enhanced_model_factory(id=id1, name="alpha", value=30))
        await enhanced_repo.save(enhanced_model_factory(id=id2, name="gamma", value=10))
        await enhanced_repo.save(enhanced_model_factory(id=id3, name="beta", value=20))

        await enhanced_repo.soft_delete(id1)
        await enhanced_repo.soft_delete(id2)
        await enhanced_repo.soft_delete(id3)

        # Test ascending order by name
        results_asc = await enhanced_repo.find_deleted(
            Query(order_by="name", order_direction="asc")
        )

        assert len(results_asc) == 3
        assert results_asc[0].name == "alpha"
        assert results_asc[1].name == "beta"
        assert results_asc[2].name == "gamma"

        # Test descending order by value
        results_desc = await enhanced_repo.find_deleted(
            Query(order_by="value", order_direction="desc")
        )

        assert len(results_desc) == 3
        assert results_desc[0].value == 30
        assert results_desc[1].value == 20
        assert results_desc[2].value == 10
