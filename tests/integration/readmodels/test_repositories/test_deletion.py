"""Integration tests for delete and soft delete operations on read model repositories."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest

from eventsource.ports.readmodels import Query

if TYPE_CHECKING:
    from collections.abc import Callable

    from .conftest import OrderSummary

pytestmark = [
    pytest.mark.integration,
]


class TestReadModelRepositoryDelete:
    """Test delete operations."""

    @pytest.mark.asyncio
    async def test_delete(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test hard delete."""
        order = order_summary_factory()
        await repo.save(order)

        deleted = await repo.delete(order.id)

        assert deleted is True
        assert await repo.get(order.id) is None

    @pytest.mark.asyncio
    async def test_delete_nonexistent(self, repo: Any) -> None:
        """Test deleting nonexistent record returns False."""
        deleted = await repo.delete(uuid4())
        assert deleted is False

    @pytest.mark.asyncio
    async def test_soft_delete(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test soft delete sets deleted_at."""
        order = order_summary_factory()
        await repo.save(order)

        deleted = await repo.soft_delete(order.id)

        assert deleted is True
        assert await repo.get(order.id) is None  # Hidden from normal get
        assert await repo.exists(order.id) is False

    @pytest.mark.asyncio
    async def test_soft_delete_already_deleted(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test soft delete on already deleted record returns False."""
        order = order_summary_factory()
        await repo.save(order)
        await repo.soft_delete(order.id)

        # Try to soft delete again
        deleted = await repo.soft_delete(order.id)
        assert deleted is False

    @pytest.mark.asyncio
    async def test_restore(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test restoring soft-deleted record."""
        order = order_summary_factory()
        await repo.save(order)
        await repo.soft_delete(order.id)

        restored = await repo.restore(order.id)

        assert restored is True
        result = await repo.get(order.id)
        assert result is not None
        assert result.deleted_at is None

    @pytest.mark.asyncio
    async def test_restore_not_deleted(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test restore on non-deleted record returns False."""
        order = order_summary_factory()
        await repo.save(order)

        restored = await repo.restore(order.id)
        assert restored is False

    @pytest.mark.asyncio
    async def test_truncate(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test truncate deletes all records."""
        for i in range(5):
            await repo.save(order_summary_factory(order_number=f"ORD-{i}"))

        count = await repo.truncate()

        assert count == 5
        assert await repo.count() == 0


class TestReadModelRepositorySoftDeleteVisibility:
    """Test soft delete visibility in queries."""

    @pytest.mark.asyncio
    async def test_find_excludes_soft_deleted(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test that find excludes soft-deleted by default."""
        order = order_summary_factory()
        await repo.save(order)
        await repo.soft_delete(order.id)

        results = await repo.find()

        assert len(results) == 0

    @pytest.mark.asyncio
    async def test_find_include_deleted(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test finding soft-deleted records with include_deleted."""
        order = order_summary_factory()
        await repo.save(order)
        await repo.soft_delete(order.id)

        results = await repo.find(Query(include_deleted=True))

        assert len(results) == 1
        assert results[0].id == order.id
        assert results[0].deleted_at is not None

    @pytest.mark.asyncio
    async def test_count_excludes_soft_deleted(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test that count excludes soft-deleted by default."""
        await repo.save(order_summary_factory())
        await repo.save(order_summary_factory())
        order3 = order_summary_factory()
        await repo.save(order3)
        await repo.soft_delete(order3.id)

        count = await repo.count()

        assert count == 2

    @pytest.mark.asyncio
    async def test_count_include_deleted(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test counting soft-deleted records with include_deleted."""
        await repo.save(order_summary_factory())
        await repo.save(order_summary_factory())
        order3 = order_summary_factory()
        await repo.save(order3)
        await repo.soft_delete(order3.id)

        count = await repo.count(Query(include_deleted=True))

        assert count == 3
