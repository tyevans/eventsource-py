"""Integration tests for basic CRUD operations on read model repositories."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable

    from .conftest import OrderSummary

pytestmark = [
    pytest.mark.integration,
]


class TestReadModelRepositoryBasicOperations:
    """Test basic CRUD operations."""

    @pytest.mark.asyncio
    async def test_save_and_get(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test saving and retrieving a read model."""
        order = order_summary_factory(order_number="ORD-001")

        await repo.save(order)
        result = await repo.get(order.id)

        assert result is not None
        assert result.id == order.id
        assert result.order_number == "ORD-001"
        assert result.customer_name == order.customer_name
        assert result.status == order.status

    @pytest.mark.asyncio
    async def test_get_nonexistent_returns_none(self, repo: Any) -> None:
        """Test that getting a nonexistent ID returns None."""
        result = await repo.get(uuid4())
        assert result is None

    @pytest.mark.asyncio
    async def test_save_updates_existing(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test that save updates existing records (upsert behavior)."""
        order = order_summary_factory(status="pending")

        await repo.save(order)

        # Modify and save again
        order.status = "shipped"
        await repo.save(order)

        result = await repo.get(order.id)
        assert result is not None
        assert result.status == "shipped"
        assert result.version == 2  # Version should be incremented

    @pytest.mark.asyncio
    async def test_get_many(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test batch get operation."""
        orders = [order_summary_factory(order_number=f"ORD-{i:03d}") for i in range(5)]
        for order in orders:
            await repo.save(order)

        # Get first 3
        ids = [o.id for o in orders[:3]]
        results = await repo.get_many(ids)

        assert len(results) == 3
        result_ids = {r.id for r in results}
        assert result_ids == set(ids)

    @pytest.mark.asyncio
    async def test_get_many_with_missing_ids(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test that get_many ignores missing IDs."""
        order = order_summary_factory()
        await repo.save(order)

        results = await repo.get_many([order.id, uuid4(), uuid4()])

        assert len(results) == 1
        assert results[0].id == order.id

    @pytest.mark.asyncio
    async def test_save_many(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test batch save operation."""
        orders = [order_summary_factory(order_number=f"ORD-{i:03d}") for i in range(3)]

        await repo.save_many(orders)

        count = await repo.count()
        assert count == 3

    @pytest.mark.asyncio
    async def test_exists(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test existence check."""
        order = order_summary_factory()

        assert await repo.exists(order.id) is False

        await repo.save(order)
        assert await repo.exists(order.id) is True
