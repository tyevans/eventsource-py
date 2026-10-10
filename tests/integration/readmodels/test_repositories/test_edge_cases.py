"""Integration tests for edge cases and precision in read model repositories."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest

from eventsource.ports.readmodels import Filter, Query

if TYPE_CHECKING:
    from collections.abc import Callable

    from .conftest import OrderSummary

pytestmark = [
    pytest.mark.integration,
]


class TestReadModelRepositoryEdgeCases:
    """Test edge cases and error handling."""

    @pytest.mark.asyncio
    async def test_empty_get_many(self, repo: Any) -> None:
        """Test get_many with empty list."""
        results = await repo.get_many([])
        assert results == []

    @pytest.mark.asyncio
    async def test_empty_save_many(self, repo: Any) -> None:
        """Test save_many with empty list (should not raise)."""
        await repo.save_many([])
        assert await repo.count() == 0

    @pytest.mark.asyncio
    async def test_multiple_filters(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test combining multiple filters (AND logic)."""
        await repo.save(order_summary_factory(status="shipped", item_count=10))
        await repo.save(order_summary_factory(status="shipped", item_count=2))
        await repo.save(order_summary_factory(status="pending", item_count=10))

        results = await repo.find(
            Query(
                filters=[
                    Filter.eq("status", "shipped"),
                    Filter.gt("item_count", 5),
                ]
            )
        )

        assert len(results) == 1
        assert results[0].status == "shipped"
        assert results[0].item_count > 5

    @pytest.mark.asyncio
    async def test_save_preserves_id(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test that save preserves the model's ID."""
        specific_id = uuid4()
        order = order_summary_factory(id=specific_id)

        await repo.save(order)
        result = await repo.get(specific_id)

        assert result is not None
        assert result.id == specific_id

    @pytest.mark.asyncio
    async def test_version_increments_on_update(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test that version increments on each update."""
        order = order_summary_factory()

        await repo.save(order)
        result1 = await repo.get(order.id)
        assert result1 is not None
        assert result1.version == 1

        # Update
        order.status = "updated1"
        await repo.save(order)
        result2 = await repo.get(order.id)
        assert result2 is not None
        assert result2.version == 2

        # Update again
        order.status = "updated2"
        await repo.save(order)
        result3 = await repo.get(order.id)
        assert result3 is not None
        assert result3.version == 3

    @pytest.mark.asyncio
    async def test_updated_at_changes_on_save(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test that updated_at timestamp changes on save."""
        order = order_summary_factory()

        await repo.save(order)
        result1 = await repo.get(order.id)
        assert result1 is not None
        first_updated = result1.updated_at

        await asyncio.sleep(0.01)

        order.status = "updated"
        await repo.save(order)
        result2 = await repo.get(order.id)
        assert result2 is not None

        assert result2.updated_at > first_updated

    @pytest.mark.asyncio
    async def test_decimal_precision(
        self,
        repo: Any,
        order_summary_factory: Callable[..., OrderSummary],
    ) -> None:
        """Test that decimal values maintain reasonable precision."""
        order = order_summary_factory(total_amount=Decimal("1234.56"))

        await repo.save(order)
        result = await repo.get(order.id)

        assert result is not None
        assert abs(float(result.total_amount) - 1234.56) < 0.01
